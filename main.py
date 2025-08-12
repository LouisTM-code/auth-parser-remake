"""
Production entrypoint для Streamlit Cloud.

Локальный запуск:
  streamlit run streamlit_main.py

Поведение:
- Делегирует рендеринг в `ui.app` (оставлен без изменений).
- Перезагружает `ui.app` при каждом запуске для повторного выполнения верхнего уровня кода UI.
- Безопасно переопределяет константы во время выполнения через Streamlit `secrets` или переменные окружения, без изменения `ui/app.py`:
  * AUTH_EMAIL, AUTH_PASSWORD (не логировать значения)
  * BATCH_SIZE, CONCURRENCY, FETCH_TIMEOUT_S

Примечания:
- Нет тяжёлой логики; UI/бизнес‑логика остаётся в модулях.
- Совместим со Streamlit 1.48.0 и Python 3.13.x.
"""
from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import Any

import streamlit as st

# Гарантируем, что корень проекта доступен для импорта (полезно на некоторых деплоях)
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _override_constants(mod: Any) -> None:
    """Переопределяет выбранные константы в `ui.app` из secrets/env.

    Эта функция должна вызываться **после** перезагрузки модуля,
    чтобы присвоения сохранялись для действий пользователя (например, кнопки Старт).
    """
    # Читаем secrets, если доступны; иначе переменные окружения. Секреты не логируем.
    secrets = {}
    try:
        secrets = dict(st.secrets)
    except Exception:
        secrets = {}

    def _get(name: str, cast: type[Any] = str, env: bool = True):
        if name in secrets and secrets[name] not in (None, ""):
            try:
                return cast(secrets[name])
            except Exception:
                return None
        if env:
            val = os.getenv(name)
            if val not in (None, ""):
                try:
                    return cast(val)
                except Exception:
                    return None
        return None

    # Учетные данные (строки)
    auth_email = _get("AUTH_EMAIL", str)
    auth_password = _get("AUTH_PASSWORD", str)

    # Параметры производительности (целые/числа с плавающей точкой)
    batch_size = _get("BATCH_SIZE", int)
    concurrency = _get("CONCURRENCY", int)
    fetch_timeout = _get("FETCH_TIMEOUT_S", float)

    # Применяем, если заданы (не трогаем, если None)
    if auth_email is not None:
        setattr(mod, "AUTH_EMAIL", auth_email)
    if auth_password is not None:
        setattr(mod, "AUTH_PASSWORD", auth_password)
    if batch_size is not None:
        setattr(mod, "BATCH_SIZE", batch_size)
    if concurrency is not None:
        setattr(mod, "CONCURRENCY", concurrency)
    if fetch_timeout is not None:
        setattr(mod, "FETCH_TIMEOUT_S", fetch_timeout)


def _render_app() -> None:
    """Загружает и перезагружает `ui.app` при каждом повторном запуске, 
    а также патчит константы из secrets/env."""
    try:
        mod = importlib.import_module("ui.app")
        mod = importlib.reload(mod)
        _override_constants(mod)
    except ModuleNotFoundError as e:
        st.set_page_config(page_title="HTML Parser — ошибка", layout="wide")
        st.title("Не найден модуль UI")
        st.error(
            "Модуль `ui.app` не найден. Проверьте структуру проекта и PYTHONPATH.\n\n"
            f"Техническая деталь: {e}"
        )
    except Exception as e:
        st.set_page_config(page_title="HTML Parser — ошибка", layout="wide")
        st.title("Ошибка запуска приложения")
        st.exception(e)


# Выполняем сразу (Streamlit выполняет скрипт сверху вниз при каждом перезапуске)
_render_app()


# (ВНИМАНИЕ) НЕ добавляем блок автозапуска через `python`,
# чтобы избежать рекурсивного спавна серверов при выполнении `streamlit run`.
# Запускать ТОЛЬКО так:
#   streamlit run streamlit_main.py
