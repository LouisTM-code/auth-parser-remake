# Цель обновления

Исправить проблемы 7–10 в `ParserPipeline`, обеспечив сквозную трассируемость `task_id` для каждой страницы, устранение дублирования логики дедупликации URL, улучшение модульности и готовность к конфигурации через внешние JSON‑профили.

## Проблемы (как есть)

**7.** В `ParserPipeline` при вызове `extract` передаётся фиксированный `task_id=0`, что лишает `ParseIssue` информации о конкретной странице.
**8.** Невозможность отследить проблемы по конкретной странице из-за фиксированного `task_id` затрудняет отладку и мониторинг (см. выше).
**9.** В `ParserPipeline` реализована собственная функция `_dedupe_keep_order`, хотя аналогичная функция уже есть в `core.utils_text.normalize_and_dedupe_urls`, что создаёт избыточный код.
**10.** `ParserPipeline` передаёт в `ProductExtractor` константу `task_id=0`, из-за чего диагностические сообщения `ParseIssue` не привязаны к конкретному URL, что уменьшает возможность переиспользования и отладки.

## Область применения

* Модуль `app.pipeline.runner.ParserPipeline` (оркестрация).
* Соприкасающиеся модули: `app.core.utils_text`, `app.core.models_and_specs` (`PageTask`, `ParseIssue`), `app.parsing.extractor.ProductExtractor`, `app.net.session_and_fetcher.PageFetcher`, `app.app_logging.logbus.LogBus`.

## *Объяснить новую концепцию работы через json конфиг*

**Идея.** Параметры парсинга (селекторы контейнеров, спецификации полей, правила нормализации) выносятся в версионируемый JSON‑профиль. Пайплайн загружает профиль, валидирует схему и передаёт конкретные `FieldSpec`/`ContainerSpecs` в конструкторы экстрактора и нормализатора (DI). Это позволяет:

* без правок кода адаптировать парсер к новым сайтам/версиям вёрстки;
* централизованно тестировать и версионировать профили;
* сохранять сквозную трассируемость (`task_id`) независимо от профиля.

**Мини‑пример JSON‑профиля (фрагмент):**

```json
{
  "version": "1.0",
  "containers": { "selectors": ["div.inner_wrapper", "section.products"] },
  "fields": [
    {
      "name": "Товар",
      "selectors": [{ "selector": ".card .title", "extract": "text" }],
      "is_unique": true,
      "normalize": []
    },
    {
      "name": "Розничная_цена",
      "selectors": [{ "selector": ".card .price", "extract": "text" }],
      "normalize": [{ "tools": ["default_clean", "price_to_float"] }]
    }
  ]
}
```

# Пайплайн (требования и кодовые примеры) \[По порядоку внедрения]

### Изменение 1. Единая нормализация и дедупликация URL через `core.utils_text`

**Требование**

* Заменить приватную `_dedupe_keep_order` в пайплайне на вызов `core.utils_text.normalize_and_dedupe_urls`.
* Удалить `_dedupe_keep_order` из кода пайплайна.

**Пример кода**

```python
from app.core.utils_text import normalize_and_dedupe_urls

raw_urls: list[str] = input_urls
norm_urls: list[str] = normalize_and_dedupe_urls(raw_urls)
```

**Мотивация**

* Один источник истины для нормализации URL и дедупликации → меньше расхождений.

> Описание нового поведения: Входные строки URL приводятся к каноническому виду и уникализируются с сохранением порядка строго в утилите `utils_text`.

---

### Изменение 2. Введение `PageTask` и присвоение сквозного `task_id`

**Требование**

* Для каждого нормализованного URL создать `PageTask` с монотонным `id`.
* Сохранить исходный `url` и канонический `normalized_url`.

**Пример кода**

```python
from app.core.models_and_specs import PageTask

page_tasks: list[PageTask] = [
    PageTask(id=i + 1, url=original, normalized_url=normalized)
    for i, (original, normalized) in enumerate(zip(raw_urls, norm_urls))
]
```

**Мотивация**

* `task_id` становится уникальным идентификатором жизненного цикла страницы.

> Описание нового поведения: Любая диагностическая запись (`ParseIssue`) и лог‑событие сопоставимы с конкретной страницей.

---

### Изменение 3. Прокидка `task_id` в экстрактор

**Требование**

* Передавать реальный `task.id` в `ProductExtractor.extract(..., task_id=...)` вместо константы `0`.

**Пример кода**

```python
from app.parsing.extractor import ProductExtractor

extractor = ProductExtractor(field_specs=field_specs, container_specs=container_specs)

for task, page in zip(page_tasks, fetched_pages):
    if page.text is None:
        # тут можно породить ParseIssue об ошибке статуса/контента с привязкой к task.id
        continue
    products, issues, page_title = extractor.extract(page.text, task_id=task.id)
    # issues: list[ParseIssue] уже содержит task_id=task.id
```

**Мотивация**

* Восстанавливает задуманную трассировку `PageTask.id → ParseIssue.task_id`.

> Описание нового поведения: Любая проблема извлечения содержит реальный `task_id`, что открывает точный поиск проблем по страницам.

---

### Изменение 4. Событийная трассировка в `LogBus` с контекстом `{task_id, url}`

**Требование**

* Все ключевые события пайплайна логировать через `LogBus` с обязательным контекстом `task_id` и URL.

**Пример кода**

```python
from app.app_logging.logbus import LogBus

log = LogBus()
for task in page_tasks:
    log.info("FETCH_START", f"Start fetch #{task.id}", {"task_id": task.id, "url": task.normalized_url})

# ... после успешной загрузки
log.info("FETCH_OK", f"Fetched #{task.id}", {"task_id": task.id, "url": task.normalized_url, "status": page.status})

# ... после экстракции
log.info("EXTRACT_OK", f"Extracted #{task.id}", {"task_id": task.id, "count": len(products)})
```

**Мотивация**

* Быстрая локализация проблем в UI и логах.

> Описание нового поведения: В UI можно фильтровать/связывать события по `task_id`, видеть путь каждой страницы.

---

### Изменение 5. Внутренний конверт `PageEnvelope`

**Требование**

* Внутри пайплайна оперировать объектом‑конвертом, который переносит контекст по этапам.

**Пример кода**

```python
from dataclasses import dataclass
from typing import Optional

@dataclass(slots=True)
class PageEnvelope:
    task: PageTask
    status: Optional[int] = None
    html: Optional[str] = None
    title: Optional[str] = None
    issues: list = None

# Пример использования
envelopes = [PageEnvelope(task=t, issues=[]) for t in page_tasks]
# fetch → envelopes[i].status/html; extract → envelopes[i].issues/title
```

**Мотивация**

* Исключает потерю `task_id`/контекста между этапами, удобен для тестов и метрик.

> Описание нового поведения: Каждый этап дополняет один и тот же объект; связь между артефактами гарантирована.

---

## *Изменения для каждого зависимого компонента от основного обновления*

### ParserPipeline

**Требование**

* Удалить `_dedupe_keep_order`; вызывать `normalize_and_dedupe_urls`.
* Формировать `PageTask` для каждого URL; прокидывать `task.id` в экстрактор.
* Логировать ключевые события с `{task_id, url}`.

**Пример кода**

```python
norm_urls = normalize_and_dedupe_urls(raw_urls)
page_tasks = [PageTask(i + 1, url=o, normalized_url=n) for i, (o, n) in enumerate(zip(raw_urls, norm_urls))]

pages = await fetcher.fetch_many(t.normalized_url for t in page_tasks)
for task, page in zip(page_tasks, pages):
    products, issues, title = extractor.extract(page.text or "", task_id=task.id)
    # ...
```

> Мотивация: Согласованность с утилитами, сквозная трассируемость, упрощение кода пайплайна.

---

### ProductExtractor

**Требование**

* Без изменения публичной сигнатуры; обязателен учёт `task_id` в `ParseIssue`.

**Пример кода**

```python
# внутри extractor.extract(..., task_id: int)
issues.append(ParseIssue(task_id=task_id, field_name=name, code="ERR_PARSE_MISSING_FIELD", details="..."))
```

> Мотивация: Сохранение контракта; полная привязка диагностик к странице.

---

### LogBus / UI

**Требование**

* Поддержать отображение/фильтрацию событий по `task_id`.

**Пример кода**

```python
log.info("EXTRACT_FAIL", "Missing field", {"task_id": task.id, "url": task.normalized_url, "field": name})
```

> Мотивация: Улучшение наблюдаемости и UX диагностики.

---

### PageFetcher / SessionManager

**Требование**

* Без изменений API; опционально добавлять `task_id` в контекст логов вызова, если логируются сетевые события.

**Пример кода**

```python
log.info("HTTP_GET", "Request", {"task_id": task.id, "url": task.normalized_url})
```

> Мотивация: Единый формат контекста логов по всему конвейеру.

---

## *Новые модули или утилиты* (если необходимо)

* `PageEnvelope` (внутренний dataclass в `pipeline.runner`), переносит контекст между этапами. Опционально.
* `profile_loader` (модуль загрузки/валидации JSON‑профиля) — при переходе на внешнюю конфигурацию.

## Нефункциональные требования

* **Детерминизм.** Порядок URL после нормализации/дедупликации стабилен; `task_id` присваиваются монотонно от 1.
* **Наблюдаемость.** Все ключевые этапы логируются с контекстом `{task_id, url}`.
* **Тестируемость.** Юнит‑тесты на: дедуп/нормализацию URL, сквозной `task_id` в `ParseIssue`, события логов, корректную работу с пустыми/битым HTML.
* **Производительность.** Введение `PageTask` и лог‑контекста не должно заметно влиять на время обработки; аллокации минимальны (без копирования больших структур).
* **Отсутствие дублирования.** `_dedupe_keep_order` удалён; используется только `core.utils_text.normalize_and_dedupe_urls`.
