"""
Pipeline: ParserPipeline — оркестрация логина, загрузки страниц, парсинга и экспорта.

Нововведение:
- extended-режим (листинг -> карточки товаров).

Инварианты:
- shallow-режим сохраняет поведение (листинг -> ProductExtractor -> PriceNormalizer -> export).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Iterable, Optional, Iterator, Any

from app.core.errors import (
    LoginFailedError,
    HttpStatusError,
    TimeoutError_,
    NetworkError,
    StopRequestedError,
    ErrorCode,
)
from app.core.models_and_specs import FIELD_SPECS, ParseIssue
from app.core.parsing_mode import ParsingMode
from app.app_logging.logbus import LogBus
from app.ui.state import UIState, UIStatus
from app.export_io.writer import XlsxWriterService
from app.net.session_and_fetcher import SessionManager, PageFetcher, FetchedPage
from app.parsing.extractor import ProductExtractor, ExtractorConfig
from app.parsing.normalizer import PriceNormalizer
from app.parsing.card_extractor import ProductCardExtractor
from app.parsing.aggregator import ProductRecordAggregator
from app.parsing.mapping_normalizer import MappingFieldNormalizer


@dataclass(slots=True)
class PipelineConfig:
    """
    Настройки пайплайна.

    Новое:
        parsing_mode: shallow/extended
        cards_batch_size: ограничение на размер партии карточек (чтобы не плодить слишком много задач разом).
    """
    batch_size: int = 10
    concurrency: int = 24
    fetch_timeout_s: float = 25.0

    parsing_mode: ParsingMode = ParsingMode.SHALLOW
    cards_batch_size: int = 20
    request_delay_s: float = 0.0
    request_delay_jitter_s: float = 0.0


class ParserPipeline:
    """
    Высокоуровневый конвейер парсинга.

    SHALLOW:
      login -> fetch listing -> parse listing -> normalize -> export

    EXTENDED:
      login -> fetch listing -> parse listing(partials+urls) -> fetch cards -> parse cards ->
      aggregate -> normalize(dict) -> export
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
        self._session = session
        self._auth_adapter = auth_adapter
        self._log = log_bus
        self._ui = ui_state
        self._writer = writer or XlsxWriterService()

        self._cfg = config or PipelineConfig()

        self._fetcher = fetcher or PageFetcher(
            session=self._session,
            concurrency=self._cfg.concurrency,
            request_delay_s=self._cfg.request_delay_s,
            request_delay_jitter_s=self._cfg.request_delay_jitter_s,
            log_bus=self._log,
        )

        # Экстрактор листинга:
        # - в extended включаем сбор product_url
        if extractor is not None:
            self._extractor = extractor
        else:
            if self._cfg.parsing_mode == ParsingMode.EXTENDED:
                self._extractor = ProductExtractor(config=ExtractorConfig(collect_product_url=True))
            else:
                self._extractor = ProductExtractor()

        # Нормализатор shallow (dataclass)
        self._normalizer = normalizer or PriceNormalizer()

        # Новые компоненты extended
        self._card_extractor = ProductCardExtractor(log_bus=self._log)
        self._aggregator = ProductRecordAggregator()
        self._mapping_normalizer = MappingFieldNormalizer()

        # groups: list[{"page_title": str, "data": list[dict|dataclass]}]
        self._groups: list[dict] = []

    def _log_issue_summary(self, *, stage: str, issues: list[ParseIssue], source_url: str) -> None:
        """
        Логирует агрегированную диагностику parse issues для этапа пайплайна.

        Роль и ответственность:
        - Сформировать компактный отчёт по кодам ошибок и количеству issue.
        - Добавить sample деталей первой ошибки для ускорения root-cause анализа.

        Границы:
        - Не модифицирует данные парсинга и не влияет на поведение экспорта.
        - Не занимается фильтрацией, восстановлением или повторным парсингом.

        Взаимодействие с другими ролями:
        - Получает список ParseIssue от extract/aggregate уровней.
        - Публикует результат через LogBus для отображения в UI и журналах.
        """
        if not issues:
            return

        code_counters: dict[str, int] = {}
        for issue in issues:
            code_counters[issue.code] = code_counters.get(issue.code, 0) + 1

        sample_issue = issues[0]
        self._log.warn(
            "PARSE_ISSUES_SUMMARY",
            (
                f"{stage}: parse issues={len(issues)} url={source_url} "
                f"codes={code_counters} sample={sample_issue.code}:{sample_issue.details}"
            ),
            context={
                "stage": stage,
                "url": source_url,
                "issues_total": len(issues),
                "codes": code_counters,
                "sample": {
                    "field_name": sample_issue.field_name,
                    "code": sample_issue.code,
                    "details": sample_issue.details,
                },
            },
        )

    def _log_listing_dom_diagnostics(self, *, html: str, source_url: str, stage: str) -> None:
        """
        Логирует диагностический срез HTML/DOM для ошибок поиска контейнера листинга.

        Роль и ответственность:
        - Зафиксировать метрики, нужные для RCA проблем с селекторами и DOM-парсингом.
        - Показать расхождение между наличием маркера в сыром HTML и результатами CSS-поиска.

        Границы:
        - Не изменяет HTML, селекторы и результат бизнес-парсинга.
        - Не выполняет повторных HTTP-запросов и не инициирует fallback-парсеры.

        Взаимодействие с другими ролями:
        - Вызывается после получения ParseIssue с кодом ERR_CONTAINER_NOT_FOUND.
        - Пишет агрегированную диагностику в LogBus для UI и журналов.
        """
        try:
            try:
                from selectolax.lexbor import LexborHTMLParser as html_parser  # type: ignore
            except Exception:  # pragma: no cover
                from selectolax.parser import HTMLParser as html_parser  # type: ignore

            tree = html_parser(html)
            root = tree.root
            all_nodes = len(root.css("*")) if root is not None else 0
            div_inner_wrapper_count = len(root.css("div.inner_wrapper")) if root is not None else 0
            any_inner_wrapper_count = len(root.css(".inner_wrapper")) if root is not None else 0
            section_inner_wrapper_count = len(root.css("section.inner_wrapper")) if root is not None else 0
            dashed_inner_wrapper_count = len(root.css(".inner-wrapper")) if root is not None else 0

            self._log.warn(
                "LISTING_DOM_DIAGNOSTICS",
                (
                    f"{stage}: listing DOM diagnostics url={source_url} "
                    f"html_bytes={len(html)} html_kb={len(html) / 1024:.2f} "
                    f"has_inner_wrapper_token={'inner_wrapper' in html} "
                    f"nodes_total={all_nodes} "
                    f"div_inner_wrapper={div_inner_wrapper_count} any_inner_wrapper={any_inner_wrapper_count} "
                    f"section_inner_wrapper={section_inner_wrapper_count} dashed_inner_wrapper={dashed_inner_wrapper_count}"
                ),
                context={
                    "stage": stage,
                    "url": source_url,
                    "html_bytes": len(html),
                    "html_kb": round(len(html) / 1024, 2),
                    "has_inner_wrapper_token": "inner_wrapper" in html,
                    "nodes_total": all_nodes,
                    "div_inner_wrapper": div_inner_wrapper_count,
                    "any_inner_wrapper": any_inner_wrapper_count,
                    "section_inner_wrapper": section_inner_wrapper_count,
                    "dashed_inner_wrapper": dashed_inner_wrapper_count,
                },
            )
        except Exception as exc:
            self._log.warn(
                "LISTING_DOM_DIAGNOSTICS_FAILED",
                f"{stage}: listing diagnostics failed for url={source_url} err={exc!r}",
                context={"stage": stage, "url": source_url, "error": repr(exc)},
            )

    async def run(self, urls: Iterable[str]) -> None:
        unique_urls = self._dedupe_keep_order(urls)
        total = len(unique_urls)
        self._ui.begin_task(total=total, task_name="parse")

        if total == 0:
            self._log.info("STAGE_START", "No URLs to process; creating empty XLSX")
            xlsx = self._safe_export_partial()
            self._ui.end_task(success=True, xlsx_path=xlsx)
            self._log.info("EXPORT_DONE", f"Exported empty workbook: {xlsx}")
            return

        # 1) Логин
        try:
            await self._ensure_not_stopped(stage="login")
            self._log.info("LOGIN", "Starting authentication")
            await self._auth_adapter.login(self._session)
            self._log.info("LOGIN_OK", "Authentication successful")
        except LoginFailedError as e:
            self._log.error(ErrorCode.ERR_LOGIN_FAILED, f"Login failed: {e}")
            self._ui.add_error(ErrorCode.ERR_LOGIN_FAILED, critical=True)
            self._ui.end_task(success=False, xlsx_path=None)
            return
        except Exception as e:
            self._log.error(ErrorCode.ERR_UNEXPECTED, f"Unexpected error on login: {e!r}")
            self._ui.add_error(ErrorCode.ERR_UNEXPECTED, critical=True)
            self._ui.end_task(success=False, xlsx_path=None)
            return

        # 2) Режимы
        if self._cfg.parsing_mode == ParsingMode.EXTENDED:
            await self._run_extended(unique_urls)
        else:
            await self._run_shallow(unique_urls)

        # 3) Экспорт (если не stop)
        if await self._is_stop_and_handle_before_export():
            return

        xlsx_path = self._safe_export_partial()
        self._ui.end_task(success=True, xlsx_path=xlsx_path)
        self._log.info("EXPORT_DONE", f"Exported XLSX: {xlsx_path}")

    # ------------------------------------------------------------------
    # SHALLOW
    # ------------------------------------------------------------------

    async def _run_shallow(self, unique_urls: list[str]) -> None:
        for batch_idx, batch in enumerate(self._batched(unique_urls, self._cfg.batch_size), start=1):
            if await self._is_stop_and_handle_before_export():
                return

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

            tasks: list[asyncio.Task[FetchedPage]] = [
                asyncio.create_task(self._fetch_one_with_timeout(u, add_showall_params=True))
                for u in batch
            ]

            for t in asyncio.as_completed(tasks):
                try:
                    page: FetchedPage = await t
                except asyncio.CancelledError:
                    timeout_err += 1
                    self._ui.inc_done(1)
                    continue

                if await self._is_stop_and_cancel_pending(tasks):
                    break

                if page.text is None:
                    if isinstance(page.error, HttpStatusError) or (page.status is not None and page.status != 200):
                        http_err += 1
                    elif isinstance(page.error, (TimeoutError_, asyncio.TimeoutError)):
                        timeout_err += 1
                    elif isinstance(page.error, NetworkError):
                        net_err += 1
                    else:
                        unexpected_err += 1
                    self._ui.inc_done(1)
                    continue

                products, issues, page_title = self._extractor.extract(page.text, task_id=batch_idx)
                if issues:
                    parse_issues += len(issues)
                    self._log_issue_summary(stage="SHALLOW_LISTING", issues=issues, source_url=page.url)
                    if any(issue.code == "ERR_CONTAINER_NOT_FOUND" for issue in issues):
                        self._log_listing_dom_diagnostics(
                            html=page.text,
                            source_url=page.url,
                            stage="SHALLOW_LISTING",
                        )

                products = self._normalizer.normalize(products)

                self._groups.append({
                    "page_title": page_title or page.url,
                    "data": products,
                })

                parsed_products += len(products)
                ok_pages += 1
                self._ui.inc_done(1)

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

    # ------------------------------------------------------------------
    # EXTENDED
    # ------------------------------------------------------------------

    async def _run_extended(self, unique_urls: list[str]) -> None:
        """
        Основной цикл extended-режима.

        Прогресс:
        - каждая страница листинга = 1 единица работы
        - каждая уникальная карточка товара, которую реально грузим = +1 единица работы
        """
        for batch_idx, batch in enumerate(self._batched(unique_urls, self._cfg.batch_size), start=1):
            if await self._is_stop_and_handle_before_export():
                return

            ok_pages = 0
            http_err = 0
            timeout_err = 0
            net_err = 0
            unexpected_err = 0

            products_total = 0
            listing_issues = 0
            card_issues = 0
            merge_issues = 0

            self._log.info(
                "BATCH_START",
                f"[EXTENDED] Batch {batch_idx}: size={len(batch)}",
                context={"batch": batch_idx, "size": len(batch)},
            )

            tasks: list[asyncio.Task[FetchedPage]] = [
                asyncio.create_task(self._fetch_one_with_timeout(u, add_showall_params=True))
                for u in batch
            ]

            for t in asyncio.as_completed(tasks):
                try:
                    listing_page: FetchedPage = await t
                except asyncio.CancelledError:
                    timeout_err += 1
                    self._ui.inc_done(1)
                    continue

                if await self._is_stop_and_cancel_pending(tasks):
                    break

                if listing_page.text is None:
                    if isinstance(listing_page.error, HttpStatusError) or (
                        listing_page.status is not None and listing_page.status != 200
                    ):
                        http_err += 1
                    elif isinstance(listing_page.error, (TimeoutError_, asyncio.TimeoutError)):
                        timeout_err += 1
                    elif isinstance(listing_page.error, NetworkError):
                        net_err += 1
                    else:
                        unexpected_err += 1
                    # листинг URL считаем обработанным
                    self._ui.inc_done(1)
                    continue

                # 1) Парсинг листинга -> PartialProduct[]
                partials, issues, page_title = self._extractor.extract_partials(
                    listing_page.text,
                    task_id=batch_idx,
                    base_url=listing_page.url,
                )
                if issues:
                    listing_issues += len(issues)
                    self._log_issue_summary(stage="EXTENDED_LISTING", issues=issues, source_url=listing_page.url)
                    if any(issue.code == "ERR_CONTAINER_NOT_FOUND" for issue in issues):
                        self._log_listing_dom_diagnostics(
                            html=listing_page.text,
                            source_url=listing_page.url,
                            stage="EXTENDED_LISTING",
                        )

                # Листинг считаем обработанным (единица прогресса)
                self._ui.inc_done(1)

                # 2) Сбор ссылок карточек (дедуп по URL, порядок сохраняем)
                card_urls = self._dedupe_keep_order([p.product_url for p in partials if p.product_url])
                base_field_names = {spec.name for spec in FIELD_SPECS}
                url_to_index: dict[str, int] = {}
                for p in partials:
                    if p.product_url and p.product_url not in url_to_index:
                        url_to_index[p.product_url] = p.product_index

                # Увеличиваем total прогресса на кол-во реально загружаемых карточек
                self._ui.add_total(len(card_urls))

                # 3) Fetch карточек (без SHOWALL_*)
                card_data_by_url: dict[str, Optional[dict[str, Any]]] = {}
                card_data_obj_by_url = {}
                card_fetch_total = 0
                card_fetch_ok = 0
                card_fetch_err = 0

                # Загружаем карточки батчами, чтобы не создавать тысячи задач разом
                for cards_sub_idx, cards_sub in enumerate(
                    self._batched(card_urls, self._cfg.cards_batch_size),
                    start=1,
                ):
                    card_fetch_total += len(cards_sub)
                    self._log.info(
                        "CARD_FETCH_BATCH_START",
                        (
                            f"[EXTENDED] Card batch {batch_idx}.{cards_sub_idx}: "
                            f"size={len(cards_sub)} listing_url={listing_page.url}"
                        ),
                        context={
                            "batch": batch_idx,
                            "cards_sub_batch": cards_sub_idx,
                            "size": len(cards_sub),
                            "listing_url": listing_page.url,
                        },
                    )
                    card_tasks: list[asyncio.Task[FetchedPage]] = [
                        asyncio.create_task(self._fetch_one_with_timeout(u, add_showall_params=False))
                        for u in cards_sub
                    ]

                    for ct in asyncio.as_completed(card_tasks):
                        try:
                            card_page: FetchedPage = await ct
                        except asyncio.CancelledError:
                            timeout_err += 1
                            self._ui.inc_done(1)
                            continue

                        if await self._is_stop_and_cancel_pending(card_tasks):
                            break

                        if card_page.text is None:
                            card_fetch_err += 1
                            self._log.warn(
                                "CARD_FETCH_ERR",
                                (
                                    "[EXTENDED] Card fetch failed "
                                    f"url={card_page.url} status={card_page.status} err={card_page.error!r}"
                                ),
                                context={
                                    "batch": batch_idx,
                                    "url": card_page.url,
                                    "product_index": url_to_index.get(card_page.url),
                                    "status": card_page.status,
                                    "error": repr(card_page.error),
                                },
                            )
                            # Карточка не загрузилась — фиксируем отсутствие данных
                            card_data_by_url[card_page.url] = None
                            # прогресс по карточке считаем выполненным
                            self._ui.inc_done(1)
                            continue

                        card_fetch_ok += 1
                        self._log.info(
                            "CARD_FETCH_OK",
                            f"[EXTENDED] Card fetch ok url={card_page.url}",
                            context={
                                "batch": batch_idx,
                                "url": card_page.url,
                                "product_index": url_to_index.get(card_page.url),
                                "status": card_page.status,
                            },
                        )

                        # 4) Парсинг карточки
                        card_data = self._card_extractor.extract(
                            card_page.text,
                            task_id=batch_idx,
                            product_url=card_page.url,
                        )
                        if card_data.issues:
                            card_issues += len(card_data.issues)
                            self._log_issue_summary(
                                stage="EXTENDED_CARD",
                                issues=card_data.issues,
                                source_url=card_page.url,
                            )

                        card_data_obj_by_url[card_page.url] = card_data
                        card_data_by_url[card_page.url] = card_data.values

                        characteristic_keys = [
                            key for key in card_data.values.keys() if key not in base_field_names
                        ]
                        sku = card_data.values.get("Артикул")
                        characteristics_count = len(characteristic_keys)
                        self._log.info(
                            "CARD_PARSE_DETAILS",
                            (
                                "[EXTENDED] Card parse details "
                                f"url={card_page.url} product_index={url_to_index.get(card_page.url)} "
                                f"sku={sku} characteristics_count={characteristics_count}"
                            ),
                            context={
                                "batch": batch_idx,
                                "url": card_page.url,
                                "listing_url": listing_page.url,
                                "product_index": url_to_index.get(card_page.url),
                                "sku": sku,
                                "characteristics_count": characteristics_count,
                                "characteristics": characteristic_keys,
                            },
                        )

                        # прогресс по карточке
                        self._ui.inc_done(1)

                    # если stop — прекращаем загрузку оставшихся карточек
                    if self._ui.stop_requested:
                        break

                    self._log.info(
                        "CARD_FETCH_BATCH_DONE",
                        (
                            f"[EXTENDED] Card batch {batch_idx}.{cards_sub_idx} done: "
                            f"ok={card_fetch_ok} err={card_fetch_err}"
                        ),
                        context={
                            "batch": batch_idx,
                            "cards_sub_batch": cards_sub_idx,
                            "ok": card_fetch_ok,
                            "err": card_fetch_err,
                        },
                    )

                # 5) Агрегация результатов в плоские dict-записи
                records: list[dict[str, Any]] = []
                for p in partials:
                    card_obj = card_data_obj_by_url.get(p.product_url) if p.product_url else None
                    rec, agg_issues = self._aggregator.aggregate(p, card_obj)
                    if agg_issues:
                        merge_issues += len(agg_issues)
                        self._log_issue_summary(
                            stage="EXTENDED_AGGREGATE",
                            issues=agg_issues,
                            source_url=p.product_url or listing_page.url,
                        )
                    records.append(rec)

                # 6) Нормализация dict-записей
                records = self._mapping_normalizer.normalize(records)

                # 7) Накопление для экспорта
                self._groups.append({
                    "page_title": page_title or listing_page.url,
                    "data": records,
                })

                products_total += len(records)
                ok_pages += 1
                self._log.info(
                    "CARD_FETCH_SUMMARY",
                    (
                        f"[EXTENDED] Card fetch summary for listing: "
                        f"total={card_fetch_total} ok={card_fetch_ok} err={card_fetch_err}"
                    ),
                    context={
                        "batch": batch_idx,
                        "listing_url": listing_page.url,
                        "total": card_fetch_total,
                        "ok": card_fetch_ok,
                        "err": card_fetch_err,
                    },
                )

            self._log.info(
                "BATCH_SUMMARY",
                (
                    f"[EXTENDED] Batch {batch_idx} done: ok_pages={ok_pages}, "
                    f"http_err={http_err}, timeout_err={timeout_err}, net_err={net_err}, "
                    f"unexpected_err={unexpected_err}, products={products_total}, "
                    f"listing_issues={listing_issues}, card_issues={card_issues}, merge_issues={merge_issues}"
                ),
                context={
                    "batch": batch_idx,
                    "ok_pages": ok_pages,
                    "http_err": http_err,
                    "timeout_err": timeout_err,
                    "net_err": net_err,
                    "unexpected_err": unexpected_err,
                    "products": products_total,
                    "listing_issues": listing_issues,
                    "card_issues": card_issues,
                    "merge_issues": merge_issues,
                },
            )

    # ------------------------------------------------------------------
    # Внутренние хелперы
    # ------------------------------------------------------------------

    @staticmethod
    def _batched(iterable: Iterable, batch_size: int) -> Iterator[list]:
        batch: list = []
        for item in iterable:
            batch.append(item)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    async def _fetch_one_with_timeout(self, url: str, *, add_showall_params: bool) -> FetchedPage:
        """
        Загружает один URL с индивидуальным таймаутом.
        Важно для extended-режима, где карточек может быть много.
        """
        try:
            pages = await asyncio.wait_for(
                self._fetcher.fetch_many([url], add_showall_params=add_showall_params),
                timeout=self._cfg.fetch_timeout_s,
            )
            if pages:
                return pages[0]
            return FetchedPage(url=url, status=None, text=None, error=TimeoutError_("empty result"))
        except asyncio.TimeoutError as e:
            return FetchedPage(url=url, status=None, text=None, error=e)
        except Exception as e:
            return FetchedPage(url=url, status=None, text=None, error=e)

    async def _is_stop_and_cancel_pending(self, tasks: list[asyncio.Task]) -> bool:
        if not self._ui.stop_requested:
            return False
        for t in tasks:
            if not t.done():
                t.cancel()
        self._log.warn("STOP_REQUESTED", "Stop requested: cancelling pending tasks")
        return True

    async def _is_stop_and_handle_before_export(self) -> bool:
        if not self._ui.stop_requested:
            return False
        self._log.warn("STOP_REQUESTED", "Stop requested: performing partial export")
        self._ui.set_status(UIStatus.STOPPED)

        xlsx_path = self._safe_export_partial()
        self._ui.end_task(success=True, xlsx_path=xlsx_path)
        self._log.info("EXPORT_PARTIAL_DONE", f"Exported partial XLSX: {xlsx_path}")
        return True

    def _safe_export_partial(self) -> str:
        groups = self._groups
        if not groups:
            groups = [{"page_title": "data", "data": []}]
        try:
            return self._writer.write(groups)
        except Exception as e:
            self._log.error("ERR_EXPORT", f"Export failed: {e!r}")
            return ""

    @staticmethod
    def _dedupe_keep_order(urls: Iterable) -> list:
        seen: set = set()
        out: list = []
        for u in urls:
            if not u:
                continue
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out

    async def _ensure_not_stopped(self, *, stage: str) -> None:
        if self._ui.stop_requested:
            raise StopRequestedError(f"Stop requested before stage: {stage}")


__all__ = ["ParserPipeline", "PipelineConfig"]
