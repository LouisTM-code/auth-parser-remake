"""
Pipeline: ParserPipeline — оркестрация логина, пакетной выборки страниц,
парсинга, нормализации и экспорта XLSX.

Требования (суммарно по ТЗ и вашим уточнениям):
- Batch‑подход по URL (по умолчанию 10 на партию): для каждой партии параллельно
  выполняется fetch всех URL; для каждого успешно полученного HTML сразу parse →
  normalize → сохранение результатов (в память), чтобы освобождать ресурсы.
- Прогресс: UIState.progress_done +1 на КАЖДЫЙ URL ПОСЛЕ normalize/завершения
  (даже при ошибке загрузки — считаем URL обработанным).
- Интеграция с LogBus и UIState: события по стадиям/итогам партий;
  не спамить мелкими логами — агрегировать.
- Stop: общий флаг отмены (UIState.stop_requested). На каждом основном этапе —
  проверка. Для идущих fetch‑задач применяем asyncio.wait_for(..., timeout=FETCH_TIMEOUT)
  и ловим asyncio.TimeoutError; новые задачи не создаём; выполняем частичный экспорт;
  перед экспортом ставим статус STOPPED, после успешного экспорта — FINISHED.
- Ошибки:
  * LoginFailedError — немедленная остановка без экспорта (статус ERROR).
  * HTTP != 200 — считаем FETCH_ERR (логируем), URL помечаем выполненным.
- FIELD_SPECS: имена полей с подчёркиваниями (см. core.models_and_specs).
- XlsxWriterService: многолистовой XLSX (по странице/заголовку h1).

Архитектура/зависимости (DI через конструктор, значения по умолчанию создаются внутри):
- SessionManager, BaseAuthAdapter, PageFetcher, ProductExtractor, PriceNormalizer,
  XlsxWriterService, LogBus, UIState.

Python 3.13.5, PEP8, ООП, подробные комментарии.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Iterable, Optional, Iterator

from app.core.errors import (
    LoginFailedError,
    HttpStatusError,
    TimeoutError_,
    NetworkError,
    StopRequestedError,
    ErrorCode,
)
from app.app_logging.logbus import LogBus
from app.ui.state import UIState, UIStatus
from app.export_io.writer import XlsxWriterService
from app.net.session_and_fetcher import SessionManager, PageFetcher, FetchedPage
from app.parsing.extractor import ProductExtractor
from app.parsing.normalizer import PriceNormalizer
from app.core.models_and_specs import ProductRecord


# ================================ Конфиг ================================

@dataclass(slots=True)
class PipelineConfig:
    """Настройки пайплайна.

    Attributes:
        batch_size: Размер партии URL, обрабатываемых одновременно.
        concurrency: Глобальный предел параллелизма PageFetcher (делегируется ему).
        fetch_timeout_s: Таймаут на ПАРАЛЛЕЛЬНУЮ загрузку *одного URL* (обёртка вокруг
            вызова fetcher для одной ссылки). Позволяет не зависнуть на долгих запросах.
    """

    batch_size: int = 10
    concurrency: int = 24
    fetch_timeout_s: float = 25.0


# ============================== Пайплайн ===============================


class ParserPipeline:
    """Высокоуровневый конвейер парсинга.

    Последовательность стадий:
        login → for each batch(urls): fetch (параллельно) → parse → normalize → накопление
        → (после всех батчей или по Stop) экспорт XLSX.

    Экономия памяти:
        - Никаких глобальных HTML‑накоплений; сразу после parse/normalize сохраняем
          только итоговые записи товаров (ProductRecord) и отбрасываем HTML/DOM.

    Безопасность остановки:
        - На Stop: не создаём новые задачи; текущие fetch‑корутины ограничены wait_for.
    """

    def __init__(
        self,
        *,
        session: SessionManager,
        auth_adapter,
        log_bus: LogBus,
        ui_state: UIState,
        writer: Optional[XlsxWriterService] = None,
        fetcher: Optional[PageFetcher] = None,
        extractor: Optional[ProductExtractor] = None,
        normalizer: Optional[PriceNormalizer] = None,
        config: Optional[PipelineConfig] = None,
    ) -> None:
        # Зависимости
        self._session = session
        self._auth_adapter = auth_adapter  # BaseAuthAdapter совместим по duck-typing
        self._log = log_bus
        self._ui = ui_state
        self._writer = writer or XlsxWriterService()
        self._fetcher = fetcher or PageFetcher(session=self._session, concurrency=(config.concurrency if config else 24))
        self._extractor = extractor or ProductExtractor()
        self._normalizer = normalizer or PriceNormalizer()

        # Конфигурация
        self._cfg = config or PipelineConfig()

        # Накопитель результатов для экспорта (многостраничный XLSX)
        # groups: list[{"page_title": str, "data": list[ProductRecord]}]
        self._groups: list[dict] = []

    # ------------------------------ Паблик ------------------------------

    async def run(self, urls: Iterable[str]) -> None:
        """Запускает полный цикл пайплайна.

        Args:
            urls: коллекция исходных URL (могут содержать дубликаты; будет дедуп по точному совпадению).
        """
        # Подготовка входа: нормализация/дедуп и настройка UI
        unique_urls = self._dedupe_keep_order(urls)
        total = len(unique_urls)
        self._ui.begin_task(total=total, task_name="parse")

        if total == 0:
            # Нечего делать — создаём пустой файл с одним пустым листом
            self._log.info("STAGE_START", "No URLs to process; creating empty XLSX")
            xlsx = self._safe_export_partial()
            self._ui.end_task(success=True, xlsx_path=xlsx)
            self._log.info("EXPORT_DONE", f"Exported empty workbook: {xlsx}")
            return

        # 1) Логин (однократный)
        try:
            await self._ensure_not_stopped(stage="login")
            self._log.info("LOGIN", "Starting authentication")
            await self._auth_adapter.login(self._session)
            self._log.info("LOGIN_OK", "Authentication successful")
        except LoginFailedError as e:
            # Жёсткая остановка без экспорта
            self._log.error(ErrorCode.ERR_LOGIN_FAILED, f"Login failed: {e}")
            self._ui.add_error(ErrorCode.ERR_LOGIN_FAILED, critical=True)
            self._ui.end_task(success=False, xlsx_path=None)
            return
        except Exception as e:  # не ожидаем, но фиксируем
            self._log.error(ErrorCode.ERR_UNEXPECTED, f"Unexpected error on login: {e!r}")
            self._ui.add_error(ErrorCode.ERR_UNEXPECTED, critical=True)
            self._ui.end_task(success=False, xlsx_path=None)
            return

        # 2) Обработка URL партиями
        for batch_idx, batch in enumerate(self._batched(unique_urls, self._cfg.batch_size), start=1):
            if await self._is_stop_and_handle_before_export():
                return  # экспорт/статус сделаны внутри

            # Сводка по партии
            ok_pages = 0
            http_err = 0
            timeout_err = 0
            net_err = 0
            unexpected_err = 0
            parsed_products = 0
            parse_issues = 0

            self._log.info(
                "BATCH_START",
                f"Batch {batch_idx}: size={len(batch)}",
                context={"batch": batch_idx, "size": len(batch)},
            )

            # Запускаем параллельные задачи загрузки по одному URL (каждая под своим wait_for)
            tasks: list[asyncio.Task[FetchedPage]] = [
                asyncio.create_task(self._fetch_one_with_timeout(u)) for u in batch
            ]

            # Обрабатываем результаты по мере готовности
            for t in asyncio.as_completed(tasks):
                try:
                    page: FetchedPage = await t
                except asyncio.CancelledError:
                    # Если нас прервали — считаем это как остановку/таймаут конкретного URL
                    timeout_err += 1
                    self._ui.inc_done(1)
                    continue

                # Проверка Stop перед парсингом
                if await self._is_stop_and_cancel_pending(tasks):
                    # После отмены незавершённых задач — выходим к частичному экспорту
                    break

                # 2.1) Разбор результатов загрузки
                if page.text is None:
                    # Ошибка статуса/сети/таймаута
                    if isinstance(page.error, HttpStatusError) or (page.status is not None and page.status != 200):
                        http_err += 1
                    elif isinstance(page.error, (TimeoutError_, asyncio.TimeoutError)):
                        timeout_err += 1
                    elif isinstance(page.error, NetworkError):
                        net_err += 1
                    else:
                        unexpected_err += 1
                    # URL считается выполненным
                    self._ui.inc_done(1)
                    continue

                # 2.2) Парсинг → нормализация → накопление
                products, issues, page_title = self._extractor.extract(page.text, task_id=0)
                if issues:
                    parse_issues += len(issues)
                # Вторая фаза нормализации (на основе FIELD_SPECS)
                products = self._normalizer.normalize(products)

                # Накопление листа (по странице)
                self._groups.append({
                    "page_title": page_title or page.url,
                    "data": products,
                })

                parsed_products += len(products)
                ok_pages += 1

                # Готово для URL → +1 к прогрессу
                self._ui.inc_done(1)

            # Итог партии — один агрегированный лог (не спамим)
            self._log.info(
                "BATCH_SUMMARY",
                (
                    f"Batch {batch_idx} done: ok_pages={ok_pages}, "
                    f"http_err={http_err}, timeout_err={timeout_err}, net_err={net_err}, "
                    f"unexpected_err={unexpected_err}, products={parsed_products}, issues={parse_issues}"
                ),
                context={
                    "batch": batch_idx,
                    "ok_pages": ok_pages,
                    "http_err": http_err,
                    "timeout_err": timeout_err,
                    "net_err": net_err,
                    "unexpected_err": unexpected_err,
                    "products": parsed_products,
                    "issues": parse_issues,
                },
            )

        # 3) Экспорт результатов (если не было Stop на предыдущем шаге)
        if await self._is_stop_and_handle_before_export():
            return

        xlsx_path = self._safe_export_partial()
        self._ui.end_task(success=True, xlsx_path=xlsx_path)
        self._log.info("EXPORT_DONE", f"Exported XLSX: {xlsx_path}")

    # --------------------------- Внутреннее API ---------------------------

    @staticmethod
    def _batched(iterable: Iterable[str], batch_size: int) -> Iterator[list[str]]:
        """
        Разбивает последовательность на батчи фиксированного размера.

        Args:
            iterable: входная последовательность.
            batch_size: размер батча (>0).

        Yields:
            Списки элементов длиной <= batch_size.
        """
        batch: list[str] = []
        for item in iterable:
            batch.append(item)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


    async def _fetch_one_with_timeout(self, url: str) -> FetchedPage:
        """Загружает ОДИН URL с индивидуальным таймаутом.

        Реализация использует существующий PageFetcher, создавая маленькую партию из
        одного URL. Это сохраняет общую семафору/лимиты соединений внутри fetcher и
        позволяет применить asyncio.wait_for на уровне задачи.
        """
        try:
            # Оборачиваем вызов в wait_for; внутри fetch_many вернётся список из 1 элемента
            pages = await asyncio.wait_for(
                self._fetcher.fetch_many([url]), timeout=self._cfg.fetch_timeout_s
            )
            if pages:
                return pages[0]
            return FetchedPage(url=url, status=None, text=None, error=TimeoutError_("empty result"))
        except asyncio.TimeoutError as e:
            return FetchedPage(url=url, status=None, text=None, error=e)
        except Exception as e:  # сетевые и прочие — возвращаем как ошибка страницы
            return FetchedPage(url=url, status=None, text=None, error=e)

    async def _is_stop_and_cancel_pending(self, tasks: list[asyncio.Task]) -> bool:
        """Если запрошен Stop — отменяет незавершённые задачи и возвращает True."""
        if not self._ui.stop_requested:
            return False
        for t in tasks:
            if not t.done():
                t.cancel()
        self._log.warn("STOP_REQUESTED", "Stop requested: cancelling pending tasks")
        return True

    async def _is_stop_and_handle_before_export(self) -> bool:
        """Если запрошен Stop — выполнить частичный экспорт и завершить.

        Последовательность:
          - Логируем и ставим статус STOPPED,
          - Выполняем экспорт того, что уже накоплено,
          - Завершаем задачу success=True (частичный результат), статус FINISHED.
        """
        if not self._ui.stop_requested:
            return False
        # Лог + статус STOPPED
        self._log.warn("STOP_REQUESTED", "Stop requested: performing partial export")
        self._ui.set_status(UIStatus.STOPPED)

        # Частичный экспорт
        xlsx_path = self._safe_export_partial()
        self._ui.end_task(success=True, xlsx_path=xlsx_path)  # FINISHED после экспорта
        self._log.info("EXPORT_PARTIAL_DONE", f"Exported partial XLSX: {xlsx_path}")
        return True

    def _safe_export_partial(self) -> str:
        """Безопасный экспорт накопленных данных в XLSX.

        XlsxWriterService поддерживает пустые листы. Если групп нет вовсе —
        создаём один пустой лист "data".
        """
        groups = self._groups
        if not groups:
            groups = [{"page_title": "data", "data": []}]
        try:
            return self._writer.write(groups)
        except Exception as e:
            # Экспорт сам по себе не должен ронять процесс — лог и пустой путь
            self._log.error("ERR_EXPORT", f"Export failed: {e!r}")
            return ""

    @staticmethod
    def _dedupe_keep_order(urls: Iterable[str]) -> list[str]:
        """Удаляет точные дубликаты URL, сохраняя порядок появления."""
        seen: set[str] = set()
        out: list[str] = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out

    async def _ensure_not_stopped(self, *, stage: str) -> None:
        """Удобный хелпер — проверка запроса остановки с генерацией исключения.
        Сейчас не используем StopRequestedError как фатальную ошибку пайплайна,
        но оставляем для потенциальных расширений.
        """
        if self._ui.stop_requested:
            raise StopRequestedError(f"Stop requested before stage: {stage}")


__all__ = ["ParserPipeline", "PipelineConfig"]
