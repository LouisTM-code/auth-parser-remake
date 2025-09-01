# Цель обновления

Отвязать сетевой слой (`PageFetcher`) от доменно-специфичных правил и централизовать нормализацию входных URL через конфигурируемый модуль трансформаций. Это устраняет хардкод `SHOWALL_*` в загрузчике страниц, повышает модульность, расширяемость и тестируемость.

## Проблемы (как есть)

* `PageFetcher` добавляет `SHOWALL_*` к URL перед запросом. Это делает модуль специфичным для одного сайта и нарушает SRP.
* Дублирование ответственности: разные места могут изменять один и тот же URL (утилиты/пайплайн vs. фетчер).
* Отсутствует единый способ конфигурировать правила нормализации URL — нельзя быстро адаптироваться к новому сайту без правок кода.
* Трассируемость ухудшена: трудно понять, почему именно URL был изменён и где это произошло.

## Область применения

Внесение изменений затрагивает:

* `app.net.session_and_fetcher.PageFetcher` — **очистка от модификации URL**.
* `app.pipeline.runner.ParserPipeline` — **добавление шага нормализации URL до fetch**.
* **Новый модуль** `app/core/url_transformer.py` — **конфигурируемые трансформации URL** на основе JSON-профиля.

> Важно: в рамках этого обновления **не использовать `app.core.utils_text`** (минимизация зависимостей). Все операции на URL выполняются средствами стандартной библиотеки Python (`urllib.parse`, и т.п.) внутри нового модуля трансформаций.

## *Объяснить новую концепцию работы через json конфиг*

URL-нормализация описывается **JSON-профилем**, который задаёт последовательность детерминированных шагов («инструментов»). Профиль выбирается на уровне пайплайна и передаётся в модуль трансформаций.

**Пример JSON-профиля (набросок):**

```json
{
  "version": 1,
  "profiles": {
    "cnc1.ru": {
      "match": { "host": "cnc1.ru" },
      "steps": [
        { "tool": "ensure_query_params", "args": {"SHOWALL_1": "1", "SHOWALL_3": "1"} },
        { "tool": "strip_fragment" },
        { "tool": "dedupe_query" }
      ]
    }
  },
  "default_profile": { "steps": [ { "tool": "strip_fragment" }, { "tool": "dedupe_query" } ] }
}
```

**Идеи:**

* `version` — версия схемы профиля (для будущих миграций).
* `site_match` — селектор профиля по домену/хостам (для мультисайтовости).
* `steps` — упорядоченный список шагов. Каждый шаг — операция с параметрами.
* Шаги **чистые и идемпотентные**: повторное применение даёт тот же результат.
* Все изменения трассируются (опционально: событие в LogBus на уровень пайплайна).

---

## Пайплайн (требования и кодовые примеры) [По порядоку внедрения]

### `PageFetcher` (PR-12)

#### 1) Очистка `PageFetcher` от доменной логики

**Требование**

* Удалить любые модификации URL внутри `PageFetcher`.
* `PageFetcher` работает только с переданным ему URL, никак его не переписывая.

**Пример кода** (не прямое указание к реализации, псевдокод)

```python
# app/net/session_and_fetcher.py (фрагмент, идеоматически)
class PageFetcher:
    def __init__(self, session: SessionManager, *, concurrency: int = 24) -> None:
        self._session = session
        self._sem = asyncio.Semaphore(max(1, concurrency))

    async def _fetch_one(self, url: str) -> FetchedPage:
        async with self._sem:
            try:
                resp = await self._session.get(url)
                ok = (resp.status_code == 200)
                return FetchedPage(
                    url=url,
                    status=resp.status_code,
                    text=(resp.text if ok else None),
                    error=(None if ok else HttpStatusError(resp.status_code, url)),
                )
            except Exception as e:
                return FetchedPage(url=url, status=None, text=None, error=e)
```

**Мотивация**

* Строгое соблюдение SRP: сеть выполняет запросы, но не знает про доменные
  «покажи всё», пагинацию и т.п.
* Упрощение тестов для фетчера (только сетевые сценарии).

> Описание нового поведения: Любые изменения URL находятся **вне** `PageFetcher`. Он получает уже нормализованный URL и загружает страницу как есть.

#### 2) Введение `UrlTransformer` (JSON‑профиль шагов)

**Требование**

* Создать `app/core/url_transformer.py` с публичным API для трансформации списков URL на основе профиля.
* Без внешних зависимостей; использовать только стандартную библиотеку.
* Не использовать `app.core.utils_text`.

**Пример кода** (не прямое указание к реализации, псевдокод)

```python
# app/core/url_transformer.py (эскиз контракта)
```python
from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Mapping
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

@dataclass(slots=True)
class UrlProfile:
    version: int
    profiles: Mapping[str, dict]
    default_profile: dict

class UrlTransformer:
    """Применяет профиль шагов к списку URL. Профиль выбирается по host, иначе default_profile."""
    def __init__(self, profile: UrlProfile) -> None:
        self._p = profile

    def transform(self, raw_urls: Iterable[str]) -> list[str]:
        out: list[str] = []
        for u in raw_urls:
            u = u.strip()
            if not u:
                continue
            steps = self._select_steps(u)
            out.append(self._apply_steps(u, steps))
        # итоговую дедупликацию по порядку при необходимости можно включить отдельным шагом
        return out

    def _select_steps(self, url: str) -> list[dict]:
        host = urlparse(url).netloc
        for name, prof in self._p.profiles.items():
            match = (prof.get("match") or {}).get("host")
            if match and match == host:
                return list(prof.get("steps", []))
        return list((self._p.default_profile or {}).get("steps", []))

    def _apply_steps(self, url: str, steps: list[dict]) -> str:
        for step in steps:
            tool = step.get("tool")
            args = step.get("args", {})
            if tool == "ensure_query_params":
                url = self._ensure_query_params(url, args)
            elif tool == "strip_fragment":
                url = self._strip_fragment(url)
            elif tool == "dedupe_query":
                url = self._dedupe_query(url)
            else:
                # неизвестный tool — опционально логировать/игнорировать/ошибка
                pass
        return url

    @staticmethod
    def _ensure_query_params(url: str, params: Mapping[str, str]) -> str:
        p = urlparse(url)
        q = dict(parse_qsl(p.query, keep_blank_values=True))
        q.update(dict(params))
        new_q = urlencode(list(q.items()), doseq=True)
        return urlunparse(p._replace(query=new_q))

    @staticmethod
    def _strip_fragment(url: str) -> str:
        p = urlparse(url)
        return urlunparse(p._replace(fragment=""))

    @staticmethod
    def _dedupe_query(url: str) -> str:
        p = urlparse(url)
        # сохраняем порядок первых вхождений ключей
        seen = set()
        items = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            key = (k, v)
            if key in seen:
                continue
            seen.add(key)
            items.append(key)
        new_q = urlencode(items, doseq=True)
        return urlunparse(p._replace(query=new_q))
```

**Мотивация**

* Гибкая конфигурация шагов без правок Python-кода.
* Идемпотентность и детерминированность упрощают тестирование.

> Описание нового поведения: Весь «доменно-зависимый» смысл нормализации URL определяется профилем (JSON) и применяется **до** сетевого слоя.

#### 3) Встраивание трансформаций в `ParserPipeline`

**Требование**

* В `runner.py` до формирования задач загрузки вызвать `UrlTransformer.transform(...)` над исходными URL.
* Зафиксировать соответствие `original_url → normalized_url` при формировании `PageTask`.
* Дальше по конвейеру использовать только `normalized_url`.

**Пример кода** (не прямое указание к реализации, псевдокод)

```python
# app/pipeline/runner.py (фрагменты, идеоматически)
from app.core.url_transformer import UrlTransformer, UrlProfile

class ParserPipeline:
    async def run(self, urls: Iterable[str]) -> None:
        # 0) Нормализация входных URL
        profile = UrlProfile(
    version=1,
    profiles={
        "cnc1.ru": {
            "match": {"host": "cnc1.ru"},
            "steps": [
                {"tool": "ensure_query_params", "args": {"SHOWALL_1": "1", "SHOWALL_3": "1"}},
                {"tool": "strip_fragment"},
                {"tool": "dedupe_query"}
            ],
        }
    },
    default_profile={
        "steps": [
            {"tool": "strip_fragment"},
            {"tool": "dedupe_query"}
        ]
    }
)
normalized = UrlTransformer(profile).transform(urls)

        # 1) Маппинг в PageTask (оригиналы можно хранить при необходимости)
        page_tasks = [
            PageTask(id=i+1, url=src, normalized_url=dst)
            for i, (src, dst) in enumerate(zip(urls, normalized))
        ]

        # 2) Передаём в PageFetcher именно normalized_url
        # ... далее логика батчинга и fetch с использованием task.normalized_url
```

**Мотивация**

* Централизация нормализации в одном месте до сети.
* Повышение трассируемости (при необходимости можно логировать пары original/normalized).

> Описание нового поведения: Пайплайн полностью контролирует URL-политику. `PageFetcher` не меняет адреса.

---

## *Изменения для каждого зависимого компонента от основного обновления*

### `ParserPipeline`

**Требование**

* Добавить шаг трансформации URL на старте `run()`.
* Везде заменить использование исходного URL на `PageTask.normalized_url` для сетевых шагов.

**Пример кода**

```python
# внутри цикла обработки
for task in page_tasks:
    page: FetchedPage = await self._fetch_one_with_timeout(task.normalized_url)
    # ... парсинг/нормализация/накопление результатов
```

**Мотивация**

* Линейная, прогнозируемая схема от нормализации к загрузке, без боковых эффектов.

---

### `PageFetcher`

**Требование**

* Исключить любые модификации URL.

**Пример кода**

```python
# см. раздел 1) — _fetch_one(url: str) использует URL без переписывания
```

**Мотивация**

* Универсальность и повторное использование сетевого слоя.

---

## Нефункциональные требования

* **Идемпотентность трансформаций**: повторное применение профиля к уже нормализованным URL не меняет результат.

* **Детерминированность**: порядок вывода зависит только от входа и профиля.

* **Трассируемость**: возможность логировать применённые шаги и пары original→normalized на уровне пайплайна.

* **Тестируемость**:
  
  * Unit-тесты на каждый шаг (`strip`, `skip_empty`, `dedupe_keep_order`, `ensure_params`).
  * Property-based тест на идемпотентность профиля.

* **Производительность**: операции O(n) на список URL; запрет на сетевые вызовы внутри трансформера.

* **Безопасность**: не выполняем произвольный код из профиля; профиль только описывает допустимые операции.

* **Расширяемость**: новый шаг добавляется без изменения существующих — через расширение набора `tool` и обновление профиля.
