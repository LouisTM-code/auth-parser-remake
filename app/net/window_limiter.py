"""
Скользящее оконное ограничение запросов на ключ.

Ключом по умолчанию выступает host, но архитектура допускает
композицию (host + group) через key_builder.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from collections.abc import Callable
from typing import Optional

from app.app_logging.logbus import LogBus


class SlidingWindowLimiter:
    """
    Ограничитель частоты на основе скользящего окна.

    Args:
        max_requests: максимум запросов в окне.
        window_seconds: длительность окна в секундах.
        jitter_ms: максимальный джиттер ожидания (миллисекунды).
        log_bus: опциональная шина логов.
        key_builder: функция для нормализации/расширения ключа.
    """

    def __init__(
        self,
        max_requests: int,
        window_seconds: float,
        *,
        jitter_ms: float = 25.0,
        log_bus: Optional[LogBus] = None,
        key_builder: Optional[Callable[[str], str]] = None,
    ) -> None:
        if max_requests <= 0:
            raise ValueError("max_requests must be positive")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if jitter_ms < 0:
            raise ValueError("jitter_ms must be non-negative")

        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._jitter_ms = jitter_ms
        self._log = log_bus
        self._key_builder = key_builder
        self._timestamps: dict[str, deque[float]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._dict_lock = asyncio.Lock()

    async def acquire(self, key: str) -> None:
        """Ожидает разрешения на использование слота для указанного ключа."""
        final_key = self._key_builder(key) if self._key_builder else key

        timestamps, lock = await self._get_state(final_key)
        while True:
            wait_s = 0.0
            jitter_s = 0.0
            window_count = 0

            async with lock:
                now = time.monotonic()
                window_start = now - self._window_seconds
                while timestamps and timestamps[0] <= window_start:
                    timestamps.popleft()

                window_count = len(timestamps)
                if window_count < self._max_requests:
                    timestamps.append(now)
                    if self._log:
                        self._log.info(
                            "RATE_LIMIT_ALLOW",
                            "Разрешение получено в окне",
                            context={
                                "key": final_key,
                                "count": window_count + 1,
                                "limit": self._max_requests,
                                "window_s": self._window_seconds,
                            },
                        )
                    return

                oldest = timestamps[0]
                wait_s = max(0.0, (oldest + self._window_seconds) - now)
                if self._jitter_ms:
                    jitter_s = random.uniform(0.0, self._jitter_ms) / 1000.0

            total_wait_s = wait_s + jitter_s
            if self._log and total_wait_s > 0.05:
                self._log.info(
                    "RATE_LIMIT_WAIT",
                    "Ожидание слота в окне",
                    context={
                        "key": final_key,
                        "wait_s": round(total_wait_s, 4),
                        "base_wait_s": round(wait_s, 4),
                        "jitter_s": round(jitter_s, 4),
                        "limit": self._max_requests,
                        "window_s": self._window_seconds,
                    },
                )
            if total_wait_s > 0:
                await asyncio.sleep(total_wait_s)
            else:
                await asyncio.sleep(0)

    async def _get_state(self, key: str) -> tuple[deque[float], asyncio.Lock]:
        async with self._dict_lock:
            if key not in self._timestamps:
                self._timestamps[key] = deque()
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._timestamps[key], self._locks[key]


__all__ = ["SlidingWindowLimiter"]
