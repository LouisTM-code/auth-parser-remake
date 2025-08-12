# logging/logbus.py
"""
Неблокирующая шина логов для UI.

Назначение:
- Принимать ключевые события (INFO/WARN/ERROR).
- Хранить события в asyncio.Queue с ограничением размера (drop-oldest).
- Предоставлять батчевую выгрузку для Streamlit UI каждые N мс.

Примечания:
- Очередь неблокирующая: push использует put_nowait(); при переполнении удаляется
  самый старый элемент (drop oldest), чтобы не тормозить пайплайн.
- Прогресс/статусы НЕ хранятся здесь — это зона ui_state.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Optional
import asyncio

LogLevel = Literal["INFO", "WARN", "ERROR"]


@dataclass(slots=True, frozen=True)
class LogEvent:
    """
    Событие лога для отображения в UI.

    Attributes:
        ts: строка времени в формате HH:MM:SS (локальное время).
        level: "INFO" | "WARN" | "ERROR".
        code: короткий код события (например, STAGE_START, FETCH_DONE, ERR_HTTP_STATUS).
        msg: человекочитаемое сообщение.
        context: произвольный контекст (URL, task_id, словарь полей и т.п.).
    """
    ts: str
    level: LogLevel
    code: str
    msg: str
    context: Optional[Any] = None


class LogBus:
    """
    Неблокирующая очередь логов с батч-выгрузкой.

    Публичный API:
        info(code, msg, context=None)  -> None
        warn(code, msg, context=None)  -> None
        error(code, msg, context=None) -> None
        push(event: LogEvent)          -> None           # неблокирующий push
        drain_batch(max_items=None)    -> list[LogEvent] # async, но без await внутри
        drain_batch_nowait(max_items=None) -> list[LogEvent] # sync-вариант
    """

    def __init__(self, max_queue_size: int = 1000) -> None:
        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be positive")
        self._q: asyncio.Queue[LogEvent] = asyncio.Queue(maxsize=max_queue_size)

    # ---------- Паблик-обёртки под уровни ----------

    def info(self, code: str, msg: str, context: Optional[Any] = None) -> None:
        self.push(self._make_event("INFO", code, msg, context))

    def warn(self, code: str, msg: str, context: Optional[Any] = None) -> None:
        self.push(self._make_event("WARN", code, msg, context))

    def error(self, code: str, msg: str, context: Optional[Any] = None) -> None:
        self.push(self._make_event("ERROR", code, msg, context))

    # ---------- Основные операции ----------

    def push(self, event: LogEvent) -> None:
        """
        Неблокирующая публикация события. Если очередь переполнена,
        удаляем самый старый элемент и повторяем попытку (drop-oldest).
        Если повторная попытка всё ещё неудачна (маловероятно) — событие отбрасывается.
        """
        try:
            self._q.put_nowait(event)
            return
        except asyncio.QueueFull:
            # Drop oldest
            try:
                _ = self._q.get_nowait()
                self._q.task_done()
            except asyncio.QueueEmpty:
                # Нечего удалить — редкая; продолжим ниже
                pass

            # Вторая попытка
            try:
                self._q.put_nowait(event)
            except asyncio.QueueFull:
                # По-прежнему переполнено — отбрасываем, чтобы не тормозить пайплайн
                return

    async def drain_batch(self, max_items: Optional[int] = None) -> list[LogEvent]:
        """
        Забирает пачку событий без ожидания (non-blocking).
        Рекомендуется вызывать из UI каждые N мс (например, 500 мс).

        Args:
            max_items: максимум событий за вызов. Если None — выгружаем всё.

        Returns:
            Список LogEvent (может быть пустым).
        """
        items: list[LogEvent] = []
        limit = max_items if (isinstance(max_items, int) and max_items > 0) else None

        while True:
            if limit is not None and len(items) >= limit:
                break
            try:
                item = self._q.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                items.append(item)
                self._q.task_done()
        return items

    def drain_batch_nowait(self, max_items: Optional[int] = None) -> list[LogEvent]:
        """
        Синхронная версия drain_batch, удобна для вызова из синхронного кода UI.
        Никаких ожиданий; поведение идентично drain_batch().
        """
        # Оборачиваем вызов без await, т.к. внутри нет асинхронных операций.
        # Логика полностью дублируется, чтобы избежать зависимости от цикла событий UI.
        items: list[LogEvent] = []
        limit = max_items if (isinstance(max_items, int) and max_items > 0) else None

        while True:
            if limit is not None and len(items) >= limit:
                break
            try:
                item = self._q.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                items.append(item)
                self._q.task_done()
        return items

    # ---------- Вспомогательное ----------

    @staticmethod
    def _make_event(level: LogLevel, code: str, msg: str, context: Optional[Any]) -> LogEvent:
        # Форматируем ts здесь, чтобы не возлагать это на UI
        ts = datetime.now().strftime("%H:%M:%S")
        return LogEvent(ts=ts, level=level, code=code, msg=msg, context=context)


__all__ = ["LogEvent", "LogBus"]
