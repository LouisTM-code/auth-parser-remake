"""
Сетевой слой: HTTP-сессия и конкурентная выборка страниц.

Состав:
- SessionManager: единый httpx.AsyncClient (HTTP/2, keep-alive, CookieJar),
  методы get/post/is_authenticated/close, явная отметка успешного логина.
- PageFetcher: конкурентный GET по очереди URL с ограничением параллелизма.

Принципы:
- Ретраи реализованы вручную (экспоненциальная задержка), чтобы не зависеть от
  версии httpx и внешних плагинов.
- Исключения инфраструктуры мэппятся на понятные типы из core.errors при необходимости
  во внешних слоях; здесь возвращаются «сырые» ошибки для гибкости.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import Iterable, Final, Optional
from collections.abc import Mapping

import httpx

from app.core.errors import HttpStatusError, NetworkError, TimeoutError_
from app.core.utils_text import add_showall_params


# Дефолтные константы для сессии/пула
_DEFAULT_UA: Final[str] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@dataclass(slots=True, frozen=True)
class SessionConfig:
    """
    Конфигурация HTTP-сессии.

    Attributes:
        base_url: Базовый URL (можно оставить пустым).
        connect_timeout_s: Таймаут установки соединения.
        read_timeout_s: Таймаут чтения ответа.
        max_connections: Максимум одновременных соединений в пуле.
        max_keepalive_connections: Максимум keep-alive соединений.
        http2: Включение HTTP/2.
        default_headers: Базовые заголовки клиента (User-Agent и т.д.).
    """
    base_url: str = ""
    connect_timeout_s: float = 5.0
    read_timeout_s: float = 10.0
    max_connections: int = 64
    max_keepalive_connections: int = 20
    http2: bool = True
    default_headers: Mapping[str, str] | None = None  # подставим ниже


class SessionManager:
    """
    Обёртка над httpx.AsyncClient: общий клиент, CookieJar, таймауты/пул/HTTP2.

    Задачи:
        - Создаёт и хранит один AsyncClient на процесс парсинга.
        - Предоставляет методы GET/POST.
        - Держит флаг аутентификации (устанавливается адаптером авторизации).

    Потоки/асинхронность:
        - Класс предназначен для использования в асинхронной среде.
    """

    def __init__(self, cfg: Optional[SessionConfig] = None) -> None:
        if cfg is None:
            cfg = SessionConfig()

        # Сформируем финальные заголовки в обычный dict[str, str]
        if cfg.default_headers is None:
            self._default_headers: dict[str, str] = {
                "User-Agent": _DEFAULT_UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "ru-RU,ru;q=0.9",
                "Cache-Control": "no-cache",
            }
        else:
            self._default_headers = dict(cfg.default_headers)

        self._cfg = cfg
        self._client = httpx.AsyncClient(
            base_url=self._cfg.base_url,
            http2=self._cfg.http2,
            headers=self._default_headers,  # используем уже гарантированно dict[str, str]
            timeout=httpx.Timeout(
                connect=self._cfg.connect_timeout_s,
                read=self._cfg.read_timeout_s,
                write=self._cfg.read_timeout_s,
                pool=self._cfg.connect_timeout_s,
            ),
            limits=httpx.Limits(
                max_connections=self._cfg.max_connections,
                max_keepalive_connections=self._cfg.max_keepalive_connections,
            ),
            cookies=httpx.Cookies(),
            follow_redirects=True,
            verify=True,
        )
        self._is_authenticated: bool = False

    # --------- свойства/служебные ---------

    @property
    def default_headers(self) -> dict[str, str]:
        """Базовые заголовки клиента (можно расширять в вызовах)."""
        return self._default_headers.copy()

    def mark_authenticated(self, value: bool = True) -> None:
        """Отмечает состояние аутентификации для текущей сессии."""
        self._is_authenticated = bool(value)

    def is_authenticated(self) -> bool:
        """Возвращает True, если адаптер авторизации отметил сессию как успешную."""
        return self._is_authenticated

    # --------- сетевые операции ---------

    async def get(
        self,
        url: str,
        *,
        headers: Optional[dict[str, str]] = None,
        max_retries: int = 2,
        retry_backoff_base: float = 0.3,
        acceptable_statuses: tuple[int, ...] = (200,),
    ) -> httpx.Response:
        """
        Выполняет GET с ручными ретраями.

        Политика ретраев:
            - Повторы на сетевые ошибки и таймауты.
            - На HTTP-статусы не из acceptable_statuses — без ретраев, сразу HttpStatusError.

        Raises:
            HttpStatusError: Если статус не входит в acceptable_statuses.
            TimeoutError_: При таймаутах после всех попыток.
            NetworkError: При сетевых сбоях после всех попыток.
        """
        last_err: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                resp = await self._client.get(url, headers=headers)
                if acceptable_statuses and resp.status_code not in acceptable_statuses:
                    raise HttpStatusError(resp.status_code, url)
                return resp
            except httpx.ReadTimeout as e:
                last_err = e
                if attempt >= max_retries:
                    raise TimeoutError_(f"GET timeout after {attempt+1} attempts: {url}") from e
            except (httpx.ConnectError, httpx.NetworkError) as e:  # NetworkError базовый для ряда сбоев
                last_err = e
                if attempt >= max_retries:
                    raise NetworkError(f"GET network error after {attempt+1} attempts: {url}") from e
            # экспоненциальная задержка
            await asyncio.sleep(retry_backoff_base * math.pow(2, attempt))

        # страховка; сюда не должны попасть
        assert last_err is not None
        raise last_err  # pragma: no cover

    async def post(
        self,
        url: str,
        *,
        data: dict | None = None,
        headers: Optional[dict[str, str]] = None,
        max_retries: int = 1,
        retry_backoff_base: float = 0.3,
        acceptable_statuses: tuple[int, ...] = (200,),
    ) -> httpx.Response:
        """
        Выполняет POST с ручными ретраями на сетевые сбои/таймауты.

        На неожиданный HTTP-статус — исключение HttpStatusError без ретраев.
        """
        last_err: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                resp = await self._client.post(url, data=data, headers=headers)
                if acceptable_statuses and resp.status_code not in acceptable_statuses:
                    raise HttpStatusError(resp.status_code, url)
                return resp
            except httpx.ReadTimeout as e:
                last_err = e
                if attempt >= max_retries:
                    raise TimeoutError_(f"POST timeout after {attempt+1} attempts: {url}") from e
            except (httpx.ConnectError, httpx.NetworkError) as e:
                last_err = e
                if attempt >= max_retries:
                    raise NetworkError(f"POST network error after {attempt+1} attempts: {url}") from e
            await asyncio.sleep(retry_backoff_base * math.pow(2, attempt))

        assert last_err is not None
        raise last_err  # pragma: no cover

    async def close(self) -> None:
        """Закрывает внутренний AsyncClient."""
        await self._client.aclose()


# ----------------------------- Fetcher ---------------------------------


@dataclass(slots=True, frozen=True)
class FetchedPage:
    """
    Результат загрузки одной страницы.

    Attributes:
        url: Запрашиваемый URL (уже с SHOWALL_*).
        status: HTTP-статус ответа (или None при сетевом исключении).
        text: Текст ответа (None при ошибке статуса или исключении).
        error: Исключение (если было); не выбрасывается наружу при mode='collect'.
    """
    url: str
    status: Optional[int]
    text: Optional[str]
    error: Optional[Exception] = None


class PageFetcher:
    """
    Конкурентная загрузка страниц с контролем параллелизма.

    Использование:
        fetcher = PageFetcher(session, concurrency=24)
        pages = await fetcher.fetch_many(urls)

    Примечания:
        - URL автоматически дополняются SHOWALL_1=1 и SHOWALL_3=1.
        - Дедупликацию лучше делать заранее (см. core.utils_text.normalize_and_dedupe_urls),
          но fetcher всё равно нормализует query для идемпотентности.
    """

    def __init__(self, session: SessionManager, *, concurrency: int = 24) -> None:
        self._session = session
        self._sem = asyncio.Semaphore(max(1, concurrency))

    async def _fetch_one(self, url: str) -> FetchedPage:
        # Гарантируем SHOWALL_* параметры
        url_with_showall = add_showall_params(url)

        async with self._sem:
            try:
                resp = await self._session.get(url_with_showall)
                # Только статус 200 считаем успешным; текст берём целиком
                return FetchedPage(
                    url=url_with_showall,
                    status=resp.status_code,
                    text=resp.text if resp.status_code == 200 else None,
                    error=None if resp.status_code == 200 else HttpStatusError(resp.status_code, url_with_showall),
                )
            except Exception as e:  # сетевые/таймауты и пр. — собираем, не роняем батч
                return FetchedPage(url=url_with_showall, status=None, text=None, error=e)

    async def fetch_many(self, urls: Iterable[str]) -> list[FetchedPage]:
        """
        Загружает набор URL конкурентно.

        Возвращает список FetchedPage в порядке завершения задач (не гарантируется
        исходный порядок). Стабильность порядка не критична для последующего парсинга.
        """
        tasks = [asyncio.create_task(self._fetch_one(u)) for u in urls]
        results: list[FetchedPage] = []
        for t in asyncio.as_completed(tasks):
            results.append(await t)
        return results
