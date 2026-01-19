"""
Streamlit UI (ui.app): интерфейс и управление пайплайном.

Нововведение:
- Выбор режима парсинга:
    * SHALLOW  (листинг)
    * EXTENDED (листинг + карточки)

Важно:
- UI не знает деталей реализации extended-режима.
- UI только передаёт флаг в PipelineConfig.
"""

from __future__ import annotations

import io
import os
import threading
import time
from pathlib import Path
from typing import Optional

import streamlit as st

from app.pipeline.runner import ParserPipeline, PipelineConfig
from app.app_logging.logbus import LogBus
from app.ui.state import UIState, UIStatus, ensure_in_session
from app.net.session_and_fetcher import SessionManager
from app.net.auth import AuthConfig, FormAuthAdapter
from app.core.parsing_mode import ParsingMode


AUTH_EMAIL = "info@stankoopt.ru"
AUTH_PASSWORD = "cnc1.ru"

BATCH_SIZE = 10
CONCURRENCY = 24
FETCH_TIMEOUT_S = 25.0
REQUEST_DELAY_S = 0.0
REQUEST_DELAY_JITTER_S = 0.0
LOG_POLL_INTERVAL_MS = 500


def _init_singletons() -> tuple[UIState, LogBus]:
    ui_state: UIState = ensure_in_session()
    if "log_bus" not in st.session_state or not isinstance(st.session_state["log_bus"], LogBus):
        st.session_state["log_bus"] = LogBus(max_queue_size=2000)
    return ui_state, st.session_state["log_bus"]


def _get_worker_thread() -> Optional[threading.Thread]:
    t = st.session_state.get("worker_thread")
    return t if isinstance(t, threading.Thread) else None


def _set_worker_thread(t: Optional[threading.Thread]) -> None:
    st.session_state["worker_thread"] = t


def _start_pipeline_in_background(urls: list[str], *, mode: ParsingMode) -> None:
    ui_state, log_bus = _init_singletons()

    t = _get_worker_thread()
    if t is not None and t.is_alive():
        st.toast("Уже выполняется задача", icon="⚠️")
        return

    ui_state.clear_stop()

    limiter = None
    limiter_key_builder = None
    session = SessionManager(
        log_bus=log_bus,
        limiter=limiter,
        limiter_key_builder=limiter_key_builder,
    )
    auth = FormAuthAdapter(AuthConfig(email=AUTH_EMAIL, password=AUTH_PASSWORD))
    cfg = PipelineConfig(
        batch_size=BATCH_SIZE,
        concurrency=CONCURRENCY,
        fetch_timeout_s=FETCH_TIMEOUT_S,
        request_delay_s=REQUEST_DELAY_S,
        request_delay_jitter_s=REQUEST_DELAY_JITTER_S,
        parsing_mode=mode,  # NEW
    )

    pipeline = ParserPipeline(
        session=session,
        auth_adapter=auth,
        log_bus=log_bus,
        ui_state=ui_state,
        config=cfg,
    )

    def _worker() -> None:
        try:
            import asyncio

            async def _run():
                try:
                    await pipeline.run(urls)
                finally:
                    try:
                        await session.close()
                    except Exception:
                        pass

            asyncio.run(_run())
        except Exception as e:
            ui_state.add_error(critical=True)
            ui_state.set_status(UIStatus.ERROR)
            log_bus.error("ERR_UI_THREAD", f"Worker thread exception: {e!r}")
        finally:
            _set_worker_thread(None)

    t = threading.Thread(target=_worker, name="parser-pipeline-thread", daemon=True)
    _set_worker_thread(t)
    t.start()


def _append_logs_to_buffer() -> None:
    if "log_lines" not in st.session_state:
        st.session_state["log_lines"] = []
    log_bus: LogBus = st.session_state["log_bus"]
    events = log_bus.drain_batch_nowait(max_items=200)
    for ev in events:
        line = f"{ev.ts} | {ev.level:<5} | {ev.code:<18} | {ev.msg}"
        st.session_state["log_lines"].append(line)


def _render_logs() -> None:
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


st.set_page_config(page_title="HTML Парсер", layout="wide")

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

        # NEW: выбор режима
        mode_label_to_value = {
            "Быстрый (листинг)": ParsingMode.SHALLOW,
            "Расширенный (листинг + карточки)": ParsingMode.EXTENDED,
        }
        selected_label = st.selectbox(
            "Режим парсинга",
            options=list(mode_label_to_value.keys()),
            index=0,
        )
        selected_mode = mode_label_to_value[selected_label]

        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("▶️ Старт", use_container_width=True, type="primary"):
                urls = _read_urls_from_text(urls_text)
                if not urls:
                    st.toast("Добавьте хотя бы один URL", icon="⚠️")
                else:
                    _start_pipeline_in_background(urls, mode=selected_mode)
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
        st.caption(
            "В расширенном режиме прогресс включает листинги и карточки товаров "
            "(total может увеличиваться после парсинга листинга)."
        )

st.subheader("Логи")
_append_logs_to_buffer()
_render_logs()

worker = _get_worker_thread()
if worker and worker.is_alive() and ui_state.status in (UIStatus.RUNNING, UIStatus.STOPPED):
    time.sleep(LOG_POLL_INTERVAL_MS / 1000.0)
    st.rerun()

if ui_state.status == UIStatus.FINISHED and ui_state.xlsx_path:
    st.subheader("Результаты")
    st.markdown("Если листов много - Нажмите на вкладку и используйте клавиатуру ← →")
    xlsx_path = Path(ui_state.xlsx_path)

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

with st.expander("Техническая информация", expanded=False):
    st.json(ui_state.as_dict())
    st.write("Лог‑буфер: ", len(st.session_state.get("log_lines", [])), " событий")
