# ui/state.py
"""
UIState — централизованное состояние интерфейса Streamlit.

Назначение:
- Хранить прогресс выполнения, статус, счётчик критических ошибок, путь к XLSX,
  флаг остановки.
- Предоставлять методы атомарного обновления.
- Давать удобные хелперы для хранения в st.session_state.

Принципы:
- Значения по умолчанию: progress_total=0, progress_done=0, status='idle',
  errors_count=0, xlsx_path=None, stop_requested=False.
- Сброс состояния перед стартом НОВОЙ задачи: begin_task() вызывает reset().
- Счётчик ошибок учитывает ТОЛЬКО критические ошибки (минорные пропуски поля — мимо).
- as_dict() — для удобного отображения/логирования в UI.

Замечание:
- Модуль не создает зависимость от Streamlit на уровне импорта. Интеграционные
  функции обращаются к streamlit динамически.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import StrEnum
from typing import Optional
import time


class UIStatus(StrEnum):
    """Допустимые статусы жизненного цикла пайплайна."""
    IDLE = "idle"
    RUNNING = "running"
    STOPPED = "stopped"
    FINISHED = "finished"
    ERROR = "error"


@dataclass(slots=True)
class UIState:
    """
    Состояние UI для Streamlit.

    Поля:
        progress_total: целевое кол-во единиц работы (страниц/шагов).
        progress_done: выполненное кол-во.
        status: текущее состояние пайплайна.
        errors_count: число критических ошибок.
        xlsx_path: путь к результирующему XLSX (или None).
        stop_requested: флаг запроса остановки пользователем.
        task_name: опциональное имя текущей задачи (для UI).
        started_at: unix-время старта текущей задачи (или 0).
        finished_at: unix-время завершения (или 0).
    """
    progress_total: int = 0
    progress_done: int = 0
    status: UIStatus = UIStatus.IDLE
    errors_count: int = 0
    xlsx_path: Optional[str] = None
    stop_requested: bool = False

    # Дополнительные служебные поля (необязательные, но полезные для UI/метрик)
    task_name: Optional[str] = None
    started_at: float = 0.0
    finished_at: float = 0.0

    # ---------------------- Жизненный цикл задачи ----------------------

    def reset(self) -> None:
        """Полный сброс к значениям по умолчанию (перед НОВОЙ задачей)."""
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
        """
        Начало новой задачи: полный сброс + установка стартовых параметров.
        """
        self.reset()
        self.progress_total = max(0, int(total))
        self.task_name = task_name
        self.status = UIStatus.RUNNING
        self.started_at = time.time()

    def end_task(self, success: bool, xlsx_path: Optional[str] = None) -> None:
        """
        Завершение текущей задачи.
        success=True  -> status=finished
        success=False -> status=error
        """
        self.status = UIStatus.FINISHED if success else UIStatus.ERROR
        self.xlsx_path = xlsx_path
        self.finished_at = time.time()

    # ---------------------- Прогресс и остановка ----------------------

    def set_total(self, total: int) -> None:
        """Определяет общее количество единиц работы (неотрицательное)."""
        self.progress_total = max(0, int(total))
        if self.progress_done > self.progress_total:
            self.progress_done = self.progress_total

    def inc_done(self, delta: int = 1) -> None:
        """Инкремент выполненной работы с ограничением сверху total."""
        if delta <= 0:
            return
        self.progress_done = min(self.progress_done + delta, self.progress_total)

    def set_done(self, done: int) -> None:
        """Прямое задание выполненной работы (с ограничениями)."""
        done = max(0, int(done))
        self.progress_done = min(done, self.progress_total)

    def set_status(self, status: UIStatus) -> None:
        """Установка статуса пайплайна."""
        self.status = status

    def request_stop(self) -> None:
        """Запрос остановки пользователем (обрабатывается пайплайном)."""
        self.stop_requested = True
        if self.status == UIStatus.RUNNING:
            self.status = UIStatus.STOPPED

    def clear_stop(self) -> None:
        """Сброс флага остановки (перед новым запуском)."""
        self.stop_requested = False
        if self.status == UIStatus.STOPPED:
            self.status = UIStatus.IDLE

    # ---------------------- Ошибки ----------------------

    def add_error(self, code: Optional[str] = None, *, critical: bool = True) -> None:
        """
        Регистрирует ошибку. В счётчик попадают ТОЛЬКО критические ошибки.
        Пропуски полей/мелкие предупреждения должны передаваться с critical=False.
        """
        if critical:
            self.errors_count += 1

    # ---------------------- Представление ----------------------

    @property
    def progress_ratio(self) -> float:
        """Доля выполненного (0.0..1.0)."""
        if self.progress_total <= 0:
            return 0.0
        return min(1.0, self.progress_done / float(self.progress_total))

    def as_dict(self) -> dict:
        """Сериализация состояния в словарь для UI/логов."""
        d = asdict(self)
        # Преобразуем enum в строку для удобного отображения/JSON
        d["status"] = str(self.status)
        return d


# ===================== Интеграция со Streamlit =====================

_STATE_KEY = "ui_state"

def ensure_in_session() -> UIState:
    """
    Гарантирует наличие UIState в st.session_state и возвращает его.
    Импортируем streamlit лениво, чтобы не тянуть зависимость на этапе импорта модуля.
    """
    import streamlit as st  # локальный импорт
    if _STATE_KEY not in st.session_state or not isinstance(st.session_state[_STATE_KEY], UIState):
        st.session_state[_STATE_KEY] = UIState()
    return st.session_state[_STATE_KEY]


def get_state() -> UIState:
    """
    Возвращает текущий UIState из st.session_state (создаст при отсутствии).
    """
    return ensure_in_session()


def reset_state() -> UIState:
    """
    Полный сброс состояния в st.session_state (удобно перед новым запуском).
    """
    state = ensure_in_session()
    state.reset()
    return state


def update_state(fn) -> UIState:
    """
    Удобный хелпер: применяет функцию-мутацию к состоянию и возвращает его.
    Пример:
        update_state(lambda s: (s.begin_task(total=42), s.set_status(UIStatus.RUNNING)))
    """
    state = ensure_in_session()
    fn(state)
    return state
