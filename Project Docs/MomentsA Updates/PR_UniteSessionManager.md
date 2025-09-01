# Цель обновления

Устранить дублирование логики ретраев и обработки ошибок в `SessionManager.get`/`SessionManager.post`, обеспечив единый путь исполнения запроса, конфигурируемую политику повторов через JSON-профиль, предсказуемую трассируемость и облегчённое тестирование — **без изменения публичного API** вызывающей стороны.

## Проблемы (как есть)

* `get` и `post` содержат практически идентичный код ретраев, бэк‑оффа и обработки исключений/статусов.
* Любые изменения политики (например, добавление 429/503, изменение задержки, таймаутов) приходится дублировать в двух местах → риск рассинхронизации.
* Логирование попыток и ошибок неоднородно; покрытие тестами усложнено (две копии одного алгоритма).

## Область применения

* Модуль: `app.net.session_and_fetcher.SessionManager` (ядро изменений).
* Смежные потребители: `app.net.session_and_fetcher.PageFetcher`, `app.pipeline.runner.ParserPipeline`, `app.app_logging.logbus.LogBus`.
* Внешние контракты: **сигнатуры** `SessionManager.get(...)` и `SessionManager.post(...)` сохраняются.

## *Объяснить новую концепцию работы через json конфиг*

Вводится конфигурация сетевой политики в JSON (версионируемый профиль). Политика описывает общие параметры повторов и решения, когда повторять/останавливаться.

**Мини‑пример**

```json
{
  "schema_version": "1.0",
  "network": {
    "retry_policy": {
      "max_attempts": 3,
      "timeout_budget_s": 20.0,
      "backoff": { "type": "exponential", "base": 0.3, "max": 5.0, "jitter": "full" },
      "retry_on_status": [429, 500, 502, 503, 504],
      "retry_on_exceptions": [
        "httpx.ReadTimeout",
        "httpx.ConnectTimeout",
        "httpx.RemoteProtocolError",
        "httpx.TransportError"
      ],
      "acceptable_statuses": [200]
    },
    "per_method_override": {
      "POST": { "max_attempts": 2, "timeout_budget_s": 15.0 }
    }
  }
}
```

> Профиль может расширяться без изменения кода. При отсутствии секций — используются дефолты.

# Пайплайн (требования и кодовые примеры) [По порядоку внедрения]

## Изменение 1. Единый исполнитель запросов `_request_with_retries`

**Требование**

* Вынести общий алгоритм ретраев и обработки ошибок в приватный метод `SessionManager._request_with_retries(method, url, **kwargs)`.
* `get`/`post` становятся тонкими фасадами, подставляющими метод и пробрасывающими аргументы.

**Пример кода**

```python
class SessionManager:
    # ...
    async def get(self, url: str, **kwargs) -> httpx.Response:
        return await self._request_with_retries("GET", url, **kwargs)

    async def post(self, url: str, **kwargs) -> httpx.Response:
        return await self._request_with_retries("POST", url, **kwargs)

    async def _request_with_retries(self, method: str, url: str, *,
                                    headers: dict | None = None,
                                    data: dict | None = None,
                                    acceptable_statuses: tuple[int, ...] = (200,),
                                    ) -> httpx.Response:
        policy = self._resolve_policy_for(method)
        budget = policy.timeout_budget_s
        attempt = 0
        last_exc: Exception | None = None
        while attempt < policy.max_attempts and budget > 0:
            attempt += 1
            start = monotonic()
            try:
                resp = await self._client.request(method, url, headers=headers, data=data)
                if resp.status_code in acceptable_statuses:
                    return resp
                if not self._should_retry_status(resp.status_code, policy):
                    return resp
            except Exception as exc:  # конкретные типы определяются в policy
                last_exc = exc
                if not self._should_retry_exception(exc, policy):
                    raise
            # планируем паузу
            delay = self._compute_backoff(attempt, policy)
            delay = min(delay, budget)  # не превышаем общий бюджет
            await asyncio.sleep(delay)
            budget -= (monotonic() - start) + delay
        # исчерпали попытки/бюджет
        if last_exc:
            raise last_exc
        # возвращаем последний ответ (или поднимаем доменную ошибку при None)
        return resp
```

**Мотивация**

* Удаление дублирования; единая точка развития политики.

> Описание нового поведения: `get`/`post` делегируют общий путь: поведение идентично и предсказуемо для всех методов.

## Изменение 2. Введение `RetryPolicy` и биндинг к JSON‑профилю

**Требование**

* Ввести `RetryPolicy` (dataclass) и метод разрешения итоговой политики с учётом `per_method_override`.
* Инициализировать `SessionManager` с объектом политики, полученным из загрузчика профиля.

**Пример кода**

```python
@dataclass(slots=True)
class RetryPolicy:
    max_attempts: int = 2
    timeout_budget_s: float = 20.0
    retry_on_status: tuple[int, ...] = (429, 500, 502, 503, 504)
    retry_on_exceptions: tuple[str, ...] = ("httpx.ConnectTimeout", "httpx.ReadTimeout")
    backoff_type: str = "exponential"  # or fixed
    backoff_base: float = 0.3
    backoff_max: float = 5.0
    jitter: str = "full"  # none|full

class SessionManager:
    def __init__(self, client: httpx.AsyncClient, policy: RetryPolicy | None = None):
        self._client = client
        self._policy = policy or RetryPolicy()

    def _resolve_policy_for(self, method: str) -> RetryPolicy:
        # применить per_method_override из профиля, если задано
        return self._policy
```

**Мотивация**

* Гибкость без правок кода; конфигурация политики внешним файлом.

> Описание нового поведения: Смена параметров ретраев (попытки, паузы, коды) делается правкой профиля.

## Изменение 3. Классификатор «повторять/не повторять»

**Требование**

* Единые функции решения по статусам и исключениям.

**Пример кода**

```python
class SessionManager:
    # ...
    def _should_retry_status(self, status: int, policy: RetryPolicy) -> bool:
        return status in policy.retry_on_status

    def _should_retry_exception(self, exc: Exception, policy: RetryPolicy) -> bool:
        name = exc.__class__.__module__ + "." + exc.__class__.__name__
        return name in policy.retry_on_exceptions
```

**Мотивация**

* Прозрачные правила; легко тестировать таблицу решений.

> Описание нового поведения: Поведение повторов детерминировано одной таблицей, общей для всех методов.

## Изменение 4. Таймаут‑бюджет на весь вызов

**Требование**

* Вместо «таймаут на попытку» использовать общий бюджет времени (`timeout_budget_s`), который расходуется попытками и задержками.

**Пример кода**

```python
budget = policy.timeout_budget_s
while attempt < policy.max_attempts and budget > 0:
    start = monotonic()
    # ... запрос + вычисление delay
    spent = (monotonic() - start) + delay
    budget -= spent
```

**Мотивация**

* Предсказуемая верхняя граница времени одного вызова независимо от числа ретраев.

> Описание нового поведения: Вызов не превысит заданный бюджет; «зависаний» из‑за цепочки ретраев нет.

## *Изменения для каждого зависимого компонента от основного обновления*

### `PageFetcher`

**Требование**

* Без изменений API. Опционально можно прокидывать `acceptable_statuses` явным параметром, если раньше делалось локально.

**Пример кода**

```python
resp = await session.get(url, acceptable_statuses=(200, 204))
```

**Мотивация**

* Совместимость; гибкость контроля статусов на месте вызова.

> Описание нового поведения: Поведение запросов стандартизовано; статусы успеха можно уточнить локально.

### `ParserPipeline`

**Требование**

* Без изменений логики, кроме возможной передачи `acceptable_statuses` при необходимости и чтения политики из профиля при инициализации `SessionManager`.

**Пример кода**

```python
policy = load_retry_policy(profile["network"]["retry_policy"])  # загрузчик профиля
session = SessionManager(client=httpx.AsyncClient(), policy=policy)
```

**Мотивация**

* Прозрачная интеграция с профилем; код пайплайна не знает деталей ретраев.

> Описание нового поведения: Пайплайн меняет поведение сети исключительно через профиль.

## Нефункциональные требования

* **Обратная совместимость:** публичный API `get`/`post` сохранён; потребители не переписываются.
* **Модульность:** один источник логики ретраев; правила вынесены в `RetryPolicy` и профиль.
* **Гибкость:** изменение поведения сетью через JSON‑профиль и пер‑методные overrides.
* **Трассируемость:** единообразные события в `LogBus`; коды событий стандартизованы.
* **Тестируемость:** таблицы решений (`retry_on_status`, `retry_on_exceptions`) и backoff покрываются юнит‑тестами без I/O; `_request_with_retries` проверяется мок‑клиентом.
* **Производительность:** отсутствует дублирование кода; вычисления backoff O(1); общее время вызова ограничено `timeout_budget_s`.
* **Расширяемость:** легко добавить другие методы (`PUT`, `DELETE`) или альтернативные backoff‑стратегии без изменения фасадов.
