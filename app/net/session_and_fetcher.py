"""
Сетевой слой: HTTP-сессия и конкурентная выборка страниц.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import Iterable, Final, Optional
from collections.abc import Mapping
import random

import httpx

from app.core.errors import HttpStatusError, NetworkError, TimeoutError_
from app.core.utils_text import add_showall_params
from app.app_logging.logbus import LogBus


_DEFAULT_UA: Final[str] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@dataclass(slots=True, frozen=True)
class SessionConfig:
    base_url: str = ""
    connect_timeout_s: float = 5.0
    read_timeout_s: float = 30.0
    max_connections: int = 64
    max_keepalive_connections: int = 20
    http2: bool = True
    default_headers: Mapping[str, str] | None = None
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504)


class SessionManager:

    def __init__(self, cfg: Optional[SessionConfig] = None, *, log_bus: Optional[LogBus] = None) -> None:
        if cfg is None:
            cfg = SessionConfig()

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
        self._log = log_bus
        self._http2_enabled = self._cfg.http2

        self._protocol_error_count = 0
        self._protocol_error_counts_by_url: dict[str, int] = {}
        self._protocol_error_threshold = 2

        # ОБЩИЙ COOKIE STORAGE
        cookies = httpx.Cookies()

        self._client = self._build_client(http2=self._http2_enabled, cookies=cookies)
        self._heavy_client = self._build_client(http2=True, cookies=cookies)

        self._is_authenticated: bool = False

    @property
    def default_headers(self) -> dict[str, str]:
        return self._default_headers.copy()

    def mark_authenticated(self, value: bool = True) -> None:
        self._is_authenticated = bool(value)

    def is_authenticated(self) -> bool:
        return self._is_authenticated

    def _build_client(self, *, http2: bool, cookies: httpx.Cookies) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self._cfg.base_url,
            http2=http2,
            headers=self._default_headers,
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
            cookies=cookies,
            follow_redirects=True,
            verify=True,
        )

    async def reset_client(self, *, http2: bool) -> None:
        cookies = self._client.cookies
        await self._client.aclose()
        self._client = self._build_client(http2=http2, cookies=cookies)
        self._http2_enabled = http2

    async def get(
        self,
        url: str,
        *,
        headers: Optional[dict[str, str]] = None,
        max_retries: int = 2,
        retry_backoff_base: float = 0.3,
        acceptable_statuses: tuple[int, ...] = (200,),
    ) -> httpx.Response:

        last_err: Exception | None = None

        for attempt in range(max_retries + 1):

            try:

                request_headers = self._default_headers.copy()

                if headers:
                    request_headers.update(headers)

                request_headers.setdefault("Referer", url)

                if "SHOWALL" in url:
                    resp = await self._heavy_client.get(url, headers=request_headers)
                    if self._log:
                        self._log.info(
                            "HTTP_PROTO",
                            f"url={url} proto={resp.http_version}"
                        )
                else:
                    resp = await self._client.get(url, headers=request_headers)

                if acceptable_statuses and resp.status_code in acceptable_statuses:
                    self._protocol_error_count = 0
                    self._protocol_error_counts_by_url.pop(url, None)
                    return resp

                if resp.status_code in self._cfg.retry_statuses and attempt < max_retries:

                    if self._log:
                        self._log.warn(
                            "FETCH_RETRY_STATUS",
                            f"Retrying GET after status {resp.status_code}: {url}",
                            context={
                                "url": url,
                                "status": resp.status_code,
                                "attempt": attempt + 1,
                                "max_retries": max_retries,
                            },
                        )

                    await asyncio.sleep(retry_backoff_base * math.pow(2, attempt))
                    continue

                if acceptable_statuses and resp.status_code not in acceptable_statuses:
                    raise HttpStatusError(resp.status_code, url)

                return resp

            except (httpx.RemoteProtocolError, httpx.ProtocolError) as e:

                last_err = e

                self._protocol_error_count += 1
                self._protocol_error_counts_by_url[url] = self._protocol_error_counts_by_url.get(url, 0) + 1

                should_fallback = (
                    self._http2_enabled
                    and (
                        self._protocol_error_count >= self._protocol_error_threshold
                        or self._protocol_error_counts_by_url[url] >= self._protocol_error_threshold
                    )
                )

                if should_fallback:

                    if self._log:
                        self._log.warn(
                            "FETCH_HTTP2_FALLBACK",
                            f"Switching to HTTP/1.1 after protocol errors for {url}",
                        )

                    await self.reset_client(http2=False)

                    self._protocol_error_count = 0
                    self._protocol_error_counts_by_url.pop(url, None)

                    continue

                if attempt >= max_retries:
                    raise NetworkError(f"GET protocol error after {attempt+1} attempts: {url}") from e

            except httpx.ReadTimeout as e:

                last_err = e

                if attempt >= max_retries:
                    raise TimeoutError_(f"GET timeout after {attempt+1} attempts: {url}") from e

            except (httpx.ConnectError, httpx.NetworkError) as e:

                last_err = e

                if attempt >= max_retries:
                    raise NetworkError(f"GET network error after {attempt+1} attempts: {url}") from e

            await asyncio.sleep(retry_backoff_base * math.pow(2, attempt))

        assert last_err is not None
        raise last_err

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

        last_err: Exception | None = None

        for attempt in range(max_retries + 1):

            try:

                resp = await self._client.post(url, data=data, headers=headers)

                if acceptable_statuses and resp.status_code in acceptable_statuses:
                    return resp

                if resp.status_code in self._cfg.retry_statuses and attempt < max_retries:
                    await asyncio.sleep(retry_backoff_base * math.pow(2, attempt))
                    continue

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
        raise last_err

    async def close(self) -> None:
        await self._client.aclose()
        await self._heavy_client.aclose()


@dataclass(slots=True, frozen=True)
class FetchedPage:

    url: str
    status: Optional[int]
    text: Optional[str]
    error: Optional[Exception] = None


class PageFetcher:

    def __init__(
        self,
        session: SessionManager,
        *,
        concurrency: int = 24,
        request_delay_s: float = 0.0,
        request_delay_jitter_s: float = 0.0,
        log_bus: Optional[LogBus] = None,
    ) -> None:

        self._session = session
        self._sem = asyncio.Semaphore(max(1, concurrency))
        self._request_delay_s = max(0.0, request_delay_s)
        self._request_delay_jitter_s = max(0.0, request_delay_jitter_s)
        self._log = log_bus

    async def _fetch_one(self, url: str, *, add_showall_params_flag: bool) -> FetchedPage:

        requested_url = add_showall_params(url) if add_showall_params_flag else url

        async with self._sem:

            delay_s = 0.0

            if self._request_delay_s or self._request_delay_jitter_s:

                delay_s = self._request_delay_s + (
                    random.uniform(0.0, self._request_delay_jitter_s)
                    if self._request_delay_jitter_s else 0.0
                )

                if delay_s > 0:

                    if self._log:
                        self._log.info(
                            "FETCH_DELAY",
                            f"Applying request delay {delay_s:.3f}s before GET: {requested_url}",
                        )

                    await asyncio.sleep(delay_s)

            try:

                resp = await self._session.get(requested_url)

                return FetchedPage(
                    url=requested_url,
                    status=resp.status_code,
                    text=resp.text if resp.status_code == 200 else None,
                    error=None if resp.status_code == 200 else HttpStatusError(resp.status_code, requested_url),
                )

            except Exception as e:

                return FetchedPage(
                    url=requested_url,
                    status=None,
                    text=None,
                    error=e
                )

    async def fetch_many(
        self,
        urls: Iterable[str],
        *,
        add_showall_params: bool = True,
    ) -> list[FetchedPage]:

        tasks = [
            asyncio.create_task(self._fetch_one(u, add_showall_params_flag=add_showall_params))
            for u in urls
        ]

        results: list[FetchedPage] = []

        for t in asyncio.as_completed(tasks):
            results.append(await t)

        return results


__all__ = ["SessionConfig", "SessionManager", "FetchedPage", "PageFetcher"]