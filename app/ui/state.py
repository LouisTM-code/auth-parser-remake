# ui/state.py
"""
UIState — централизованное состояние интерфейса Streamlit.

Нововведение (extended-режим):
- add_total(delta): увеличивает progress_total на delta.
  Нужно потому, что количество карточек становится известно только после парсинга листинга.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from enum import StrEnum
from typing import Optional
import time


class UIStatus(StrEnum):
    IDLE = "idle"
    RUNNING = "running"
    STOPPED = "stopped"
    FINISHED = "finished"
    ERROR = "error"


@dataclass(slots=True)
class UIState:
    progress_total: int = 0
    progress_done: int = 0
    status: UIStatus = UIStatus.IDLE
    errors_count: int = 0
    xlsx_path: Optional[str] = None
    stop_requested: bool = False

    task_name: Optional[str] = None
    started_at: float = 0.0
    finished_at: float = 0.0

    def reset(self) -> None:
        self.progress_total = 0
        self.progress_done = 0
        self.status = UIStatus.IDLE
        self.errors_count = 0
        self.xlsx_path = None
        self.stop_requested = False
        self.task_name = None
        self.started_at = 0.0
        self.finished_at = 0.0

    def begin_task(self, total: int = 0, task_name: Optional[str] = None) -> None:
        self.reset()
        self.progress_total = max(0, int(total))
        self.task_name = task_name
        self.status = UIStatus.RUNNING
        self.started_at = time.time()

    def end_task(self, success: bool, xlsx_path: Optional[str] = None) -> None:
        self.status = UIStatus.FINISHED if success else UIStatus.ERROR
        self.xlsx_path = xlsx_path
        self.finished_at = time.time()

    def set_total(self, total: int) -> None:
        self.progress_total = max(0, int(total))
        if self.progress_done > self.progress_total:
            self.progress_done = self.progress_total

    def add_total(self, delta: int) -> None:
        """
        (NEW) Увеличивает progress_total на delta.

        Используется в extended-режиме:
        - сначала total = кол-во URL листинга,
        - после парсинга листинга узнаём кол-во карточек и добавляем его к total.
        """
        if delta <= 0:
            return
        self.set_total(self.progress_total + int(delta))

    def inc_done(self, delta: int = 1) -> None:
        if delta <= 0:
            return
        self.progress_done = min(self.progress_done + delta, self.progress_total)

    def set_done(self, done: int) -> None:
        done = max(0, int(done))
        self.progress_done = min(done, self.progress_total)

    def set_status(self, status: UIStatus) -> None:
        self.status = status

    def request_stop(self) -> None:
        self.stop_requested = True
        if self.status == UIStatus.RUNNING:
            self.status = UIStatus.STOPPED

    def clear_stop(self) -> None:
        self.stop_requested = False
        if self.status == UIStatus.STOPPED:
            self.status = UIStatus.IDLE

    def add_error(self, code: Optional[str] = None, *, critical: bool = True) -> None:
        if critical:
            self.errors_count += 1

    @property
    def progress_ratio(self) -> float:
        if self.progress_total <= 0:
            return 0.0
        return min(1.0, self.progress_done / float(self.progress_total))

    def as_dict(self) -> dict:
        d = asdict(self)
        d["status"] = str(self.status)
        return d


_STATE_KEY = "ui_state"


def ensure_in_session() -> UIState:
    import streamlit as st  # локальный импорт
    if _STATE_KEY not in st.session_state or not isinstance(st.session_state[_STATE_KEY], UIState):
        st.session_state[_STATE_KEY] = UIState()
    return st.session_state[_STATE_KEY]


def get_state() -> UIState:
    return ensure_in_session()


def reset_state() -> UIState:
    state = ensure_in_session()
    state.reset()
    return state


def update_state(fn) -> UIState:
    state = ensure_in_session()
    fn(state)
    return state
