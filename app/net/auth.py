"""
Авторизация: базовый адаптер и форма-логин.

Назначение:
- Единый интерфейс авторизации (ABC).
- Реализация FormAuthAdapter: POST на /auth/?login=yes с
  полями USER_LOGIN/USER_PASSWORD и «браузерными» заголовками.
- Критерий успеха: HTTP 200 и в тексте ответа нет слова "Ошибка" (регистр нечувствителен).
- При успехе адаптер помечает сеанс как аутентифицированный.

Примечания:
- Данные авторизации могут передаваться извне через AuthConfig.
- Логику ретраев и таймаутов обрабатывает SessionManager.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Final

from app.core.errors import LoginFailedError
from app.net.session_and_fetcher import SessionManager


@dataclass(slots=True, frozen=True)
class AuthConfig:
    """
    Конфигурация для формы логина.

    Attributes:
        email: Логин (email).
        password: Пароль.
        login_url: Абсолютный или относительный URL точки входа.
    """
    email: str
    password: str
    login_url: str = "https://cnc1.ru/auth/?login=yes"


@dataclass(slots=True, frozen=True)
class AuthResult:
    """
    Результат авторизации.

    Attributes:
        ok: Признак успеха.
        message: Краткое текстовое описание итога (для логов/UI).
    """
    ok: bool
    message: str = ""


class BaseAuthAdapter(ABC):
    """
    Абстрактный адаптер авторизации.

    Контракт:
        - вызывает сетевые операции через SessionManager.
        - не хранит Cookie сам — это обязанность SessionManager.
    """

    @abstractmethod
    async def login(self, session: SessionManager) -> AuthResult:
        """
        Выполняет авторизацию.

        Args:
            session: Менеджер HTTP-сеанса, предоставляющий клиент/куки/таймауты.

        Returns:
            AuthResult: ok=True при успешном входе.

        Raises:
            LoginFailedError: если критерий успеха не выполнен.
            Любые сетевые исключения пробрасывает SessionManager.
        """
        raise NotImplementedError


class FormAuthAdapter(BaseAuthAdapter):
    """
    Реализация авторизации через пост-форму.

    Алгоритм:
        1) Собрать форму: USER_LOGIN/USER_PASSWORD.
        2) Отправить POST на login_url с заголовками, имитирующими браузер.
        3) Успех: status_code == 200 и не найдено слово "Ошибка" (case-insensitive).
        4) При успехе: session.mark_authenticated(True).

    Примечания:
        - Допускает абсолютный либо относительный login_url.
        - Заголовки «как у браузера» добавляются поверх дефолтных SessionManager.
    """

    # Набор доп. заголовков, характерных для браузерного POST
    _BROWSER_EXTRAS: Final[dict[str, str]] = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/138.0.0.0 Safari/537.36"
        ),
        "Referer": "https://cnc1.ru/?login=yes",
    }

    def __init__(self, config: AuthConfig) -> None:
        self._cfg = config

    async def login(self, session: SessionManager) -> AuthResult:
        form = {
            "backurl": "/?login=yes",
            "AUTH_FORM": "Y",
            "TYPE": "AUTH",
            "POPUP_AUTH": "Y",
            "AUTH_TYPE": "login",
            "USER_LOGIN": self._cfg.email,
            "USER_PASSWORD": self._cfg.password,
            "Login": "Y"
        }

        # Merge заголовков: приоритет у _BROWSER_EXTRAS
        headers = {**session.default_headers, **self._BROWSER_EXTRAS}

        resp = await session.post(self._cfg.login_url, data=form, headers=headers)
        text = resp.text or ""

        ok = (resp.status_code == 200) and ("ошибка" not in text.lower())
        if not ok:
            # Отладка: покажем фрагмент ответа сервера
            print("=== LOGIN RESPONSE START ===")
            print(text[:500])
            print("=== LOGIN RESPONSE END ===")
            raise LoginFailedError(
                f"Login failed: status={resp.status_code}, contains_error={'ошибка' in text.lower()}"
            )
        
        session.mark_authenticated(True)
        return AuthResult(ok=True, message="Login successful")
