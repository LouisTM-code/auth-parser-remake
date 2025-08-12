"""
Streamlit UI (ui.app): минималистичный интерфейс и управление пайплайном.

Функции UI:
- Поле ввода URL (по одному в строке), кнопки Старт/Стоп.
- Прогресс‑бар и статус.
- Живое окно логов (батчевая вставка из LogBus, авто‑скролл).
- По завершении — считывание итогового XLSX и кнопка «Скачать».

Особенности реализации:
- Выполнение ParserPipeline вынесено в отдельный поток, чтобы не блокировать UI.
- Логи/прогресс обновляются небольшими батчами на каждом перерисовывании страницы.
- Stop: кнопка выставляет флаг в UIState; пайплайн ловит и делает частичный экспорт.
- Тёмная тема: базовый CSS поверх выбранной темы Streamlit (для быстрой интеграции).

Зависимости (классический импорт ваших модулей):
- runner.ParserPipeline, runner.PipelineConfig
- app_logging.logbus.LogBus
- ui.state.UIState, ui.state.ensure_in_session
- net.session_and_fetcher.SessionManager
- auth.AuthConfig, auth.FormAuthAdapter

Константы (логин/пароль/настройки сети и параллелизма) задаются в коде.
Python 3.13.5, Streamlit 1.48.0.
"""
from __future__ import annotations

import io
import os
import threading
import time
from pathlib import Path
from typing import Optional

import streamlit as st

# ====== Классические импорты из существующих модулей проекта ======
from pipeline.runner import ParserPipeline, PipelineConfig
from app_logging.logbus import LogBus
from ui.state import UIState, UIStatus, ensure_in_session
from net.session_and_fetcher import SessionManager
from net.auth import AuthConfig, FormAuthAdapter

# ===================== Константы конфигурации =====================
# ВАЖНО: замените на реальные учётные данные.
AUTH_EMAIL = "info@stankoopt.ru"
AUTH_PASSWORD = "cnc1.ru"

# Настройки пайплайна (не выставляются в UI)
BATCH_SIZE = 10
CONCURRENCY = 24
FETCH_TIMEOUT_S = 25.0

# Интервалы обновления интерфейса (мс)
LOG_POLL_INTERVAL_MS = 500

# ===================== Вспомогательные функции =====================

def _init_singletons() -> tuple[UIState, LogBus]:
    """Гарантирует наличие UIState и LogBus в session_state.
    Возвращает (ui_state, log_bus).
    """
    ui_state: UIState = ensure_in_session()
    if "log_bus" not in st.session_state or not isinstance(st.session_state["log_bus"], LogBus):
        st.session_state["log_bus"] = LogBus(max_queue_size=2000)
    return ui_state, st.session_state["log_bus"]


def _get_worker_thread() -> Optional[threading.Thread]:
    t = st.session_state.get("worker_thread")
    return t if isinstance(t, threading.Thread) else None


def _set_worker_thread(t: Optional[threading.Thread]) -> None:
    st.session_state["worker_thread"] = t


def _start_pipeline_in_background(urls: list[str]) -> None:
    """Стартует ParserPipeline в отдельном потоке.

    Все зависимости создаются/берутся из session_state.
    """
    ui_state, log_bus = _init_singletons()

    # Защита от повторного запуска
    t = _get_worker_thread()
    if t is not None and t.is_alive():
        st.toast("Уже выполняется задача", icon="⚠️")
        return

    # Сброс флагов остановки и подготовка состояния
    ui_state.clear_stop()

    # Собираем зависимости пайплайна
    session = SessionManager()
    auth = FormAuthAdapter(AuthConfig(email=AUTH_EMAIL, password=AUTH_PASSWORD))
    cfg = PipelineConfig(batch_size=BATCH_SIZE, concurrency=CONCURRENCY, fetch_timeout_s=FETCH_TIMEOUT_S)

    pipeline = ParserPipeline(
        session=session,
        auth_adapter=auth,
        log_bus=log_bus,
        ui_state=ui_state,
        config=cfg,
    )

    def _worker() -> None:
        """Фоновый поток: запускает asyncio‑пайплайн и корректно завершает сессию."""
        try:
            import asyncio

            async def _run():
                try:
                    await pipeline.run(urls)
                finally:
                    # на всякий случай: закрываем сетевую сессию
                    try:
                        await session.close()
                    except Exception:
                        pass

            asyncio.run(_run())
        except Exception as e:  # финальная страховка; пайплайн сам логирует ошибки
            ui_state.add_error(critical=True)
            ui_state.set_status(UIStatus.ERROR)
            log_bus.error("ERR_UI_THREAD", f"Worker thread exception: {e!r}")
        finally:
            # Сигнал UI об окончании потока
            _set_worker_thread(None)

    t = threading.Thread(target=_worker, name="parser-pipeline-thread", daemon=True)
    _set_worker_thread(t)
    t.start()


def _append_logs_to_buffer() -> None:
    """Забирает батч логов из LogBus и добавляет их в буфер для отрисовки."""
    if "log_lines" not in st.session_state:
        st.session_state["log_lines"] = []
    log_bus: LogBus = st.session_state["log_bus"]
    events = log_bus.drain_batch_nowait(max_items=200)
    for ev in events:
        line = f"{ev.ts} | {ev.level:<5} | {ev.code:<18} | {ev.msg}"
        st.session_state["log_lines"].append(line)


def _render_logs() -> None:
    """Рисует окно логов с авто‑скроллом в конец."""
    lines = st.session_state.get("log_lines", [])
    html = "<br/>".join(l.replace("<", "&lt;").replace(">", "&gt;") for l in lines[-2000:])
    st.markdown(
        f"""
        <div id="logbox" style="height:320px; overflow:auto; background:#0c0f12; color:#e6e6e6; padding:8px; border:1px solid #222; border-radius:8px; font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; font-size:12.5px;">
            {html}
        </div>
        <script>
            const el = document.getElementById('logbox');
            if (el) {{ el.scrollTop = el.scrollHeight; }}
        </script>
        """,
        unsafe_allow_html=True,
    )


def _read_urls_from_text(text: str) -> list[str]:
    """Разбирает URLы (по одному в строке), удаляет пустые, строгая дедупликация с сохранением порядка."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in (text or "").splitlines():
        u = raw.strip()
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


# ============================= Разметка UI =============================

st.set_page_config(page_title="HTML Парсер", layout="wide")

# Небольшая тёмная тема поверх активной темы Streamlit
st.markdown(
    """
    <style>
    body { background: #0e1117; }
    .stApp { background: #0e1117; color: #e6e6e6; }
    .stTextArea textarea { background:#0c0f12 !important; color:#e6e6e6 !important; border:1px solid #222; }
    .stButton>button { background:#1b222c; color:#e6e6e6; border:1px solid #2a3340; }
    .stButton>button:hover { background:#222a35; }
    .stDownloadButton>button { background:#1b222c; color:#e6e6e6; border:1px solid #2a3340; }
    .stDownloadButton>button:hover { background:#222a35; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Быстрый HTML‑парсер с авторизацией")

ui_state, log_bus = _init_singletons()

# ---------- Ввод и управление ----------
with st.container():
    col_left, col_right = st.columns([2, 1], gap="large")

    with col_left:
        st.subheader("Ввод ссылок")
        urls_text = st.text_area(
            "URL (по одному в строке)",
            key="urls_text",
            height=180,
            placeholder="https://example.com/catalog/...",
        )

        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("▶️ Старт", use_container_width=True, type="primary"):
                urls = _read_urls_from_text(urls_text)
                if not urls:
                    st.toast("Добавьте хотя бы один URL", icon="⚠️")
                else:
                    _start_pipeline_in_background(urls)
                    # даём немного времени воркеру стартовать
                    time.sleep(0.1)
                    st.rerun()
        with col_b:
            if st.button("⏹ Остановить", use_container_width=True):
                ui_state.request_stop()
                log_bus.warn("STOP_CLICK", "Stop requested by user")
                st.rerun()

    with col_right:
        st.subheader("Статус и прогресс")
        st.write(f"Статус: **{ui_state.status}**")
        st.progress(ui_state.progress_ratio, text=f"{ui_state.progress_done}/{ui_state.progress_total}")
        st.caption("Прогресс обновляется по факту обработки URL после нормализации.")

# ---------- Логи ----------
st.subheader("Логи")
_append_logs_to_buffer()
_render_logs()

# Если пайплайн запущен — мягко обновляем страницу каждые LOG_POLL_INTERVAL_MS
worker = _get_worker_thread()
if worker and worker.is_alive() and ui_state.status in (UIStatus.RUNNING, UIStatus.STOPPED):
    # Небольшая задержка, затем перерисовка
    time.sleep(LOG_POLL_INTERVAL_MS / 1000.0)
    st.rerun()

# ---------- Результаты ----------
if ui_state.status == UIStatus.FINISHED and ui_state.xlsx_path:
    st.subheader("Результаты")
    st.markdown("Если листов много - Нажмите на вкладку и используйте клавиатуру ← →")
    xlsx_path = Path(ui_state.xlsx_path)

    # Предпросмотр первых строк итогового XLSX (один лист или несколько)
    try:
        import pandas as pd
        with pd.ExcelFile(xlsx_path) as xf:
            sheets = [str(name) for name in xf.sheet_names]
            tabs = st.tabs(sheets)
            for sheet, tab in zip(sheets, tabs):
                with tab:
                    df = xf.parse(sheet)
                    st.dataframe(df, use_container_width=True, height=320)
    except Exception as e:
        st.warning(f"Не удалось показать предпросмотр XLSX: {e}")

    # Кнопка скачивания файла
    try:
        with open(xlsx_path, "rb") as f:
            data = f.read()
        st.download_button(
            label="⬇️ Скачать XLSX",
            data=data,
            file_name=os.path.basename(xlsx_path),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    except Exception as e:
        st.error(f"Ошибка доступа к файлу: {e}")

# ---------- Техническая сводка ----------
with st.expander("Техническая информация", expanded=False):
    st.json(ui_state.as_dict())
    st.write("Лог‑буфер: ", len(st.session_state.get("log_lines", [])), " событий")
