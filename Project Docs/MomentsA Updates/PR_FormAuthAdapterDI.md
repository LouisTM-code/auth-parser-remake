# Цель обновления

Сделать `FormAuthAdapter` переносимым и управляемым через внешний конфиг без жёстких значений в коде, а также исключить вывод в `stdout`, переведя логирование на `LogBus`.

## Проблемы (как есть)

1. Жёстко заданный `login_url` и набор «браузерных» заголовков внутри адаптера привязывают модуль к одному сайту.
2. При неудаче логина адаптер печатает фрагмент ответа в `stdout`, минуя шину логов и усложняя контроль чувствительных данных.

## Область применения

* Модули: `app.net.auth` (`FormAuthAdapter`, базовые типы), `app.pipeline.runner` (инициализация и вызов авторизации), `app.app_logging.logbus` (события), опционально `app.net.session_and_fetcher` (политика заголовков).
* На функциональность парсинга и экспорта изменения не влияют.

## *Объяснить новую концепцию работы через json конфиг*

Адаптер не содержит доменной логики. Все внешние параметры поступают из JSON‑профиля авторизации, который загружается в пайплайне и преобразуется в профиль/DTO, передаваемый в адаптер через DI. Авторизация реализуется как мини‑пайплайн шагов (preflight → prepare\_form → submit), а проверка успеха вынесена в `AuthDetector`. Все события и ошибки идут в `LogBus` с маскированием секретов.

### Пример возможной структуры JSON‑профиля (v1, минимально достаточная)

```json
{
  "version": "auth.v1",
  "method": "POST",
  "login_url": "https://example.com/auth/?login=yes",
  "headers": {
    "User-Agent": "...",
    "Referer": "https://example.com/?login=yes"
  },
  "preflight": {
    "enabled": false,
    "method": "GET",
    "url": "https://example.com/auth/",
    "headers": {}
  },
  "form": {
    "user_field": "USER_LOGIN",
    "pass_field": "USER_PASSWORD",
    "extra": {"AUTH_FORM": "Y", "TYPE": "AUTH"}
  },
  "detector": {
    "strategy": "text_absent",
    "needle": "Ошибка",
    "status_allow": [200],
    "cookie_exists": null
  },
  "log": {
    "mask_fields": ["USER_LOGIN", "USER_PASSWORD"],
    "response_preview_chars": 200
  }
}
```

---

# Пайплайн (требования и кодовые примеры) \[По порядоку внедрения]

## 1. Деконфигурация адаптера (убираем хардкод и stdout)

**Требование**

* Исключить дефолтные доменно‑зависимые значения из адаптера.
* Заменить `print(...)` на события `LogBus`.

**Пример кода**

```python
# app/net/auth.py (фрагмент: интерфейс и базовый конструктор)
from dataclasses import dataclass
from typing import Optional, Mapping

@dataclass(slots=True, frozen=True)
class FormAuthProfile:
    login_url: str
    method: str = "POST"
    headers: Mapping[str, str] | None = None
    preflight: Optional[dict] = None
    form_user_field: str = "USER_LOGIN"
    form_pass_field: str = "USER_PASSWORD"
    form_extra: Mapping[str, str] | None = None
    log_mask_fields: tuple[str, ...] = ("USER_LOGIN", "USER_PASSWORD")
    log_response_preview_chars: int = 200

class FormAuthAdapter(BaseAuthAdapter):
    def __init__(self, profile: FormAuthProfile, *, detector: "BaseAuthDetector", log_bus: LogBus) -> None:
        self._p = profile
        self._detector = detector
        self._log = log_bus

    async def login(self, session: SessionManager) -> AuthResult:
        self._log.info("AUTH_START", "Starting auth")
        # ... (preflight/submit ниже в следующих шагах)
        # никакого print(...)
```

**Мотивация**

* Переносимость и нулевая доменная привязка.
* Единая трассируемость через `LogBus`.

> Описание нового поведения: Адаптер не содержит конкретных URL/заголовков; всё поступает через профиль. Любой вывод осуществляется через `LogBus`.

---

## 2. Чтение JSON‑профиля в пайплайне и DI в адаптер

**Требование**

* Загрузка и валидация JSON‑профиля в `ParserPipeline`.
* Создание `FormAuthProfile` и `AuthDetector` из профиля и передача в `FormAuthAdapter`.

**Пример кода**

```python
# app/pipeline/runner.py (фрагмент: инициализация авторизации)
import json
from app.net.auth import FormAuthAdapter, FormAuthProfile

async def _init_auth(self) -> FormAuthAdapter:
    with open("config/auth_profile.json", "r", encoding="utf-8") as f:
        raw = json.load(f)
    prof = FormAuthProfile(
        login_url=raw["login_url"],
        method=raw.get("method", "POST"),
        headers=raw.get("headers"),
        preflight=raw.get("preflight"),
        form_user_field=raw["form"]["user_field"],
        form_pass_field=raw["form"]["pass_field"],
        form_extra=raw["form"].get("extra", {}),
        log_mask_fields=tuple(raw.get("log", {}).get("mask_fields", ("USER_LOGIN", "USER_PASSWORD"))),
        log_response_preview_chars=int(raw.get("log", {}).get("response_preview_chars", 200)),
    )
    detector = AuthDetector.from_profile(raw.get("detector", {}))
    return FormAuthAdapter(prof, detector=detector, log_bus=self._log)

# вызов в run(...):
# self._auth_adapter = await self._init_auth()
```

**Мотивация**

* Изоляция: адаптер не читает файлы и ничего не знает о JSON.
* Подготовка к версиированиям профилей.

> Описание нового поведения: Пайплайн управляет конфигурацией и передаёт адаптеру ровно то, что нужно.

---

## 3. Авторизация как последовательность шагов (мини‑пайплайн)

**Требование**

* Реализовать шаги: `preflight? → prepare_form → submit` внутри адаптера.
* Каждый шаг логируется и использует заголовки/параметры из профиля.

**Пример кода** (Не прямая реализация, лишь пример, псевдокод)

```python
# app/net/auth.py (фрагмент)
from urllib.parse import urlparse

class FormAuthAdapter(BaseAuthAdapter):
    async def login(self, session: SessionManager) -> AuthResult:
        self._log.info("AUTH_START", "Starting auth", {"url": self._p.login_url})

        # 1) preflight
        if self._p.preflight and self._p.preflight.get("enabled"):
            self._log.info("AUTH_PREFLIGHT", "GET preflight")
            await session.get(self._p.preflight.get("url", self._p.login_url),
                              headers=self._merged_headers(session, self._p.preflight.get("headers", {})))

        # 2) prepare_form
        form = dict(self._p.form_extra or {})
        form[self._p.form_user_field] = "<masked>"  # логируем маску
        form[self._p.form_pass_field] = "<masked>"
        self._log.info("AUTH_PREPARE_FORM", "Prepared form", {"fields": list(form.keys())})

        # 3) submit
        submit_form = dict(self._p.form_extra or {})
        # реальные значения подставляются здесь (из внешнего секрета/переменных среды)
        submit_form[self._p.form_user_field] = os.environ.get("AUTH_USER", "")
        submit_form[self._p.form_pass_field] = os.environ.get("AUTH_PASS", "")

        resp = await session.post(self._p.login_url,
                                  data=submit_form,
                                  headers=self._merged_headers(session, self._p.headers or {}))
        preview = (resp.text or "")[: self._p.log_response_preview_chars]
        self._log.info("AUTH_RESPONSE", "Response received",
                       {"status": resp.status_code, "preview": preview})

        # делегируем решение детектору
        ok, message = self._detector.is_ok(resp)
        if ok:
            session.mark_authenticated(True)
            self._log.info("AUTH_SUCCESS", message)
            return AuthResult(ok=True, message=message)
        else:
            self._log.error("ERR_LOGIN_FAILED", message)
            raise LoginFailedError(message)

    def _merged_headers(self, session: SessionManager, extra: Mapping[str, str]) -> dict[str, str]:
        base = session.default_headers
        out = {**base, **(self._p.headers or {}), **(extra or {})}
        # если Referer не задан, собрать по хосту login_url
        if "Referer" not in out:
            u = urlparse(self._p.login_url)
            out["Referer"] = f"{u.scheme}://{u.netloc}/"
        return out
```

**Мотивация**

* Прозрачные шаги, явная трассируемость, возможность расширять пайплайн.

> Описание нового поведения: Адаптер исполняет сценарий из профиля и логирует каждый этап.

---

## 4. Деление обязанностей: внешний `AuthDetector`

**Требование**

* Вынести критерии успеха логина в отдельный детектор, создаваемый из профиля.

**Пример кода** (Не прямая реализация, лишь пример, псевдокод)

```python
# часть auth.py
from abc import ABC, abstractmethod
import httpx

class BaseAuthDetector(ABC):
    @abstractmethod
    def is_ok(self, response: httpx.Response) -> tuple[bool, str]:
        ...

class TextAbsentDetector(BaseAuthDetector):
    def __init__(self, needle: str, allowed: tuple[int, ...] = (200,)) -> None:
        self._needle = needle.lower()
        self._allowed = allowed

    def is_ok(self, response: httpx.Response) -> tuple[bool, str]:
        if response.status_code not in self._allowed:
            return False, f"Unexpected status {response.status_code}"
        text = (response.text or "").lower()
        if self._needle in text:
            return False, "Error marker found in response"
        return True, "Login successful"

class AuthDetector:
    @staticmethod
    def from_profile(cfg: dict) -> BaseAuthDetector:
        strat = (cfg.get("strategy") or "text_absent").lower()
        if strat == "text_absent":
            return TextAbsentDetector(cfg.get("needle", "ошибка"), tuple(cfg.get("status_allow", [200])))
        # здесь могут быть иные стратегии (cookie_exists, redirect, json_field, ...)
        return TextAbsentDetector("ошибка")
```

**Мотивация**

* Снижение связности: адаптер отправляет запрос, детектор решает успех.
* Расширяемость стратегий без правок адаптера.

> Описание нового поведения: Условия успеха меняются конфигурацией и/или детектором, не трогая адаптер.

---

## 5. Интеграция с `ParserPipeline` и `LogBus`

**Требование**

* Пайплайн инициализирует адаптер из профиля, обрабатывает `LoginFailedError`.
* Все события авторизации видны в UI (через существующий `LogBus`).

**Пример кода**

```python
# app/pipeline/runner.py (фрагмент)
try:
    await self._ensure_not_stopped(stage="login")
    self._auth_adapter = await self._init_auth()  # см. шаг 2
    self._log.info("LOGIN", "Starting authentication")
    await self._auth_adapter.login(self._session)
    self._log.info("LOGIN_OK", "Authentication successful")
except LoginFailedError as e:
    self._log.error("ERR_LOGIN_FAILED", f"Login failed: {e}")
    self._ui.add_error("ERR_LOGIN_FAILED", critical=True)
    self._ui.end_task(success=False, xlsx_path=None)
    return
```

**Мотивация**

* Сохраняем текущий контракт пайплайна, добавляя гибкость на этапе инициализации.

> Описание нового поведения: Пайплайн читает профиль и готовит адаптер/детектор; визуальная трассировка логина доступна в UI.

---

## Нефункциональные требования

* **Стабильные коды событий**: `AUTH_START`, `AUTH_PREFLIGHT`, `AUTH_PREPARE_FORM`, `AUTH_RESPONSE`, `AUTH_SUCCESS`, `ERR_LOGIN_FAILED`.
* **Версионирование конфигов**: поле `version` в JSON; обратная совместимость v1.
* **Тестируемость**: unit‑тесты детектора, интеграционные тесты успешного/неуспешного входа с фиктивным HTTP‑сервисом.
* **Производительность**: без блокировок; использование существующего `httpx.AsyncClient`.
* **Изоляция**: адаптер не читает файлы, не зависит от Streamlit/UI; все данные приходят извне через DI.
