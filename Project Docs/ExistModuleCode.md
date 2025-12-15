# Единый **source of truth** кодовой базы

- Документ сформирован автоматически.
- Корень обхода: `D:/Git/auth-parser-remake/app`
- Время сборки: 2025-12-16 00:48:20 RTZ 2 (зима)
- Всего модулей: 17
- Пустых модулей: 0

# Дерево абсолютных импортов модулей (Python)

> Имена приведены в точечной нотации; соответствуют абсолютным путям импортов.

  - [app_logging.logbus](#app_logging-logbus.py)
  - [core.dto_extended](#core-dto_extended.py)
  - [core.errors](#core-errors.py)
  - [core.models_and_specs](#core-models_and_specs.py)
  - [core.parsing_mode](#core-parsing_mode.py)
  - [core.utils_text](#core-utils_text.py)
  - [export_io.writer](#export_io-writer.py)
  - [net.auth](#net-auth.py)
  - [net.session_and_fetcher](#net-session_and_fetcher.py)
  - [parsing.aggregator](#parsing-aggregator.py)
  - [parsing.card_extractor](#parsing-card_extractor.py)
  - [parsing.extractor](#parsing-extractor.py)
  - [parsing.mapping_normalizer](#parsing-mapping_normalizer.py)
  - [parsing.normalizer](#parsing-normalizer.py)
  - [pipeline.runner](#pipeline-runner.py)
  - [ui.interface](#ui-interface.py)
  - [ui.state](#ui-state.py)

# Актуальный код

## app_logging/
<a id="app_logging"></a>

### logbus.py
<a id="app_logging-logbus.py"></a>

```python
# logging/logbus.py
"""
Неблокирующая шина логов для UI.

Назначение:
- Принимать ключевые события (INFO/WARN/ERROR).
- Хранить события в asyncio.Queue с ограничением размера (drop-oldest).
- Предоставлять батчевую выгрузку для Streamlit UI каждые N мс.

Примечания:
- Очередь неблокирующая: push использует put_nowait(); при переполнении удаляется
  самый старый элемент (drop oldest), чтобы не тормозить пайплайн.
- Прогресс/статусы НЕ хранятся здесь — это зона ui_state.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Optional
import asyncio

LogLevel = Literal["INFO", "WARN", "ERROR"]


@dataclass(slots=True, frozen=True)
class LogEvent:
    """
    Событие лога для отображения в UI.

    Attributes:
        ts: строка времени в формате HH:MM:SS (локальное время).
        level: "INFO" | "WARN" | "ERROR".
        code: короткий код события (например, STAGE_START, FETCH_DONE, ERR_HTTP_STATUS).
        msg: человекочитаемое сообщение.
        context: произвольный контекст (URL, task_id, словарь полей и т.п.).
    """
    ts: str
    level: LogLevel
    code: str
    msg: str
    context: Optional[Any] = None


class LogBus:
    """
    Неблокирующая очередь логов с батч-выгрузкой.

    Публичный API:
        info(code, msg, context=None)  -> None
        warn(code, msg, context=None)  -> None
        error(code, msg, context=None) -> None
        push(event: LogEvent)          -> None           # неблокирующий push
        drain_batch(max_items=None)    -> list[LogEvent] # async, но без await внутри
        drain_batch_nowait(max_items=None) -> list[LogEvent] # sync-вариант
    """

    def __init__(self, max_queue_size: int = 1000) -> None:
        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be positive")
        self._q: asyncio.Queue[LogEvent] = asyncio.Queue(maxsize=max_queue_size)

    # ---------- Паблик-обёртки под уровни ----------

    def info(self, code: str, msg: str, context: Optional[Any] = None) -> None:
        self.push(self._make_event("INFO", code, msg, context))

    def warn(self, code: str, msg: str, context: Optional[Any] = None) -> None:
        self.push(self._make_event("WARN", code, msg, context))

    def error(self, code: str, msg: str, context: Optional[Any] = None) -> None:
        self.push(self._make_event("ERROR", code, msg, context))

    # ---------- Основные операции ----------

    def push(self, event: LogEvent) -> None:
        """
        Неблокирующая публикация события. Если очередь переполнена,
        удаляем самый старый элемент и повторяем попытку (drop-oldest).
        Если повторная попытка всё ещё неудачна (маловероятно) — событие отбрасывается.
        """
        try:
            self._q.put_nowait(event)
            return
        except asyncio.QueueFull:
            # Drop oldest
            try:
                _ = self._q.get_nowait()
                self._q.task_done()
            except asyncio.QueueEmpty:
                # Нечего удалить — редкая; продолжим ниже
                pass

            # Вторая попытка
            try:
                self._q.put_nowait(event)
            except asyncio.QueueFull:
                # По-прежнему переполнено — отбрасываем, чтобы не тормозить пайплайн
                return

    async def drain_batch(self, max_items: Optional[int] = None) -> list[LogEvent]:
        """
        Забирает пачку событий без ожидания (non-blocking).
        Рекомендуется вызывать из UI каждые N мс (например, 500 мс).

        Args:
            max_items: максимум событий за вызов. Если None — выгружаем всё.

        Returns:
            Список LogEvent (может быть пустым).
        """
        items: list[LogEvent] = []
        limit = max_items if (isinstance(max_items, int) and max_items > 0) else None

        while True:
            if limit is not None and len(items) >= limit:
                break
            try:
                item = self._q.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                items.append(item)
                self._q.task_done()
        return items

    def drain_batch_nowait(self, max_items: Optional[int] = None) -> list[LogEvent]:
        """
        Синхронная версия drain_batch, удобна для вызова из синхронного кода UI.
        Никаких ожиданий; поведение идентично drain_batch().
        """
        # Оборачиваем вызов без await, т.к. внутри нет асинхронных операций.
        # Логика полностью дублируется, чтобы избежать зависимости от цикла событий UI.
        items: list[LogEvent] = []
        limit = max_items if (isinstance(max_items, int) and max_items > 0) else None

        while True:
            if limit is not None and len(items) >= limit:
                break
            try:
                item = self._q.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                items.append(item)
                self._q.task_done()
        return items

    # ---------- Вспомогательное ----------

    @staticmethod
    def _make_event(level: LogLevel, code: str, msg: str, context: Optional[Any]) -> LogEvent:
        # Форматируем ts здесь, чтобы не возлагать это на UI
        ts = datetime.now().strftime("%H:%M:%S")
        return LogEvent(ts=ts, level=level, code=code, msg=msg, context=context)


__all__ = ["LogEvent", "LogBus"]
```

## core/
<a id="core"></a>

### dto_extended.py
<a id="core-dto_extended.py"></a>

```python
"""
DTO для расширенного режима парсинга (листинг -> карточка).

Цели:
- Ввести промежуточные модели, не ломая существующий ProductRecord.
- Разорвать связку "листинг == финальный продукт".

Важно:
- DTO не содержит бизнес-логики: только структура данных.
- DTO не зависит от сетевого слоя и парсера HTML.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from app.core.models_and_specs import ParseIssue


@dataclass(slots=True)
class PartialProduct:
    """
    Промежуточный результат после парсинга листинга.

    Attributes:
        task_id: идентификатор PageTask/листинга (для трассировки/ошибок).
        product_index: порядковый номер товара на странице листинга (1..N).
        product_url: ссылка на карточку товара (может отсутствовать).
        values: словарь базовых полей товара, извлечённых с листинга.
                Ключи обычно совпадают с FIELD_SPECS.name.
    """
    task_id: int
    product_index: int
    product_url: Optional[str]
    values: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CardProductData:
    """
    Результат парсинга карточки товара.

    Attributes:
        task_id: идентификатор листинга (или иной связанный id) для трассировки.
        product_url: URL карточки (ключ связи с PartialProduct).
        values: словарь значений, извлечённых с карточки.
                Может включать базовые поля (цены/артикул/наличие) и характеристики.
        issues: список диагностических проблем (не фатальных).
    """
    task_id: int
    product_url: str
    values: dict[str, Any] = field(default_factory=dict)
    issues: list[ParseIssue] = field(default_factory=list)


__all__ = ["PartialProduct", "CardProductData"]
```

### errors.py
<a id="core-errors.py"></a>

```python
"""
Коды и исключения верхнего уровня для конвейера парсинга.

Цели:
- Единый набор стабильных кодов ошибок (для логов/UI).
- Базовый класс PipelineError с полем code для мэппинга в логи.

Заметка:
- Ошибки парсинга отсутствующих полей НЕ должны ронять процесс —
  их лучше фиксировать как ParseIssue; исключения — для критики инфраструктуры.
"""

from __future__ import annotations

from enum import StrEnum


class ErrorCode(StrEnum):
    ERR_LOGIN_FAILED = "ERR_LOGIN_FAILED"
    ERR_HTTP_STATUS = "ERR_HTTP_STATUS"
    ERR_TIMEOUT = "ERR_TIMEOUT"
    ERR_NETWORK = "ERR_NETWORK"
    ERR_ENCODING = "ERR_ENCODING"
    ERR_STOP_REQUESTED = "ERR_STOP_REQUESTED"
    ERR_UNEXPECTED = "ERR_UNEXPECTED"


class PipelineError(Exception):
    """
    Базовое исключение конвейера. Все наследники содержат machine-readable code.
    """

    code: ErrorCode = ErrorCode.ERR_UNEXPECTED

    def __init__(self, message: str = "", *args) -> None:
        super().__init__(message, *args)


class LoginFailedError(PipelineError):
    code = ErrorCode.ERR_LOGIN_FAILED


class HttpStatusError(PipelineError):
    code = ErrorCode.ERR_HTTP_STATUS

    def __init__(self, status: int, url: str, message: str | None = None) -> None:
        msg = message or f"Unexpected HTTP status {status} for {url}"
        super().__init__(msg)
        self.status = status
        self.url = url


class TimeoutError_(PipelineError):  # избегаем конфликта с builtins TimeoutError
    code = ErrorCode.ERR_TIMEOUT


class NetworkError(PipelineError):
    code = ErrorCode.ERR_NETWORK


class EncodingError(PipelineError):
    code = ErrorCode.ERR_ENCODING


class StopRequestedError(PipelineError):
    code = ErrorCode.ERR_STOP_REQUESTED


class UnexpectedError(PipelineError):
    code = ErrorCode.ERR_UNEXPECTED
```

### models_and_specs.py
<a id="core-models_and_specs.py"></a>

```python
"""
Модели (DTO) и спецификации полей парсинга.

Цель файла:
- Единая «истина» по структурам данных (FieldSpec, ProductRecord, PageTask, ParseIssue).
- Константный список FIELD_SPECS из 5 полей по ТЗ (имена колонок строго соответствуют name).

Примечания:
- Избегаем циклических зависимостей: файл не импортирует сетевые/парсинговые подсистемы.
- Значение отсутствующего значения фиксируется как строка "NA" (см. константу NA).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal, Optional


# Единое обозначение отсутствующего значения для полей товара (по ТЗ).
NA: Literal["NA"] = "NA"


class ExtractType(StrEnum):
    """
    Тип извлечения значения из DOM-элемента.
    - text: текстовое содержимое узла
    - attr: значение атрибута (вместе с FieldSpec.attr)
    """
    TEXT = "text"
    ATTR = "attr"


@dataclass(slots=True, frozen=True)
class SelectorVariant:
    """
    Описание одного варианта поиска значения для поля.
    selector: CSS-селектор
    extract: тип извлечения (text/attr)
    attr: атрибут, если extract=attr
    """
    selector: str
    extract: ExtractType
    attr: Optional[str] = None

@dataclass(slots=True, frozen=True)
class NormalizeRules:
    """
    Правила нормализации значения поля.

    Attributes:
        tools: Набор идентификаторов инструментов нормализации. Интерпретируется
               модулем parsing.normalizer для выбора соответствующих функций.
               Примеры идентификаторов могут включать преобразование цены в число и т.п.
        supplier_id: Внешнее условие (идентификатор поставщика), пригодно для
                     условной нормализации (например, специфические правила артикула).
    """
    tools: Optional[list[str]] = None
    supplier_id: Optional[int] = None

@dataclass(slots=True, frozen=True)
class FieldSpec:
    """
    Спецификация поля для извлечения.
    Может содержать несколько альтернативных вариантов (SelectorVariant),
    которые будут проверяться по порядку до первого успеха.

    Attributes:
        name: Заголовок столбца (строго соответствует требованиям ТЗ).
        selectors: Список вариантов селекторов/извлечения для поля.
        is_unique: Участвует ли поле в построении ключа уникальности товара.
        normalize: Список правил нормализации, обрабатываемых parsing.normalizer.
    """
    name: str
    selectors: list[SelectorVariant]
    is_unique: bool = False
    normalize: list[NormalizeRules] = field(default_factory=list)

@dataclass(slots=True, frozen=True)
class ContainerSpecs:
    """
    Спецификация контейнеров карточек товаров.

    selectors:
        Список CSS-селекторов контейнеров карточек. Можно передать несколько
        вариантов для совместимости с разными версиями вёрстки. Приоритет — по порядку.
        Если список пуст — парсер выполняет fallback-логику (определяет границы
        карточки по ближайшим предкам якоря внутри global-контейнера).
    """
    selectors: list[str] = field(default_factory=list)

@dataclass(slots=True)
class ProductRecord:
    """
    Строка результата для одного товара.
    Ровно 5 полей — именованы в точности как в FieldSpec.name.

    Замечание:
        Служебные поля (URL, статусы и т.п.) не входят в итоговую таблицу по ТЗ.
        Их следует хранить отдельно в других структурах, если потребуется.
    """
    # Имена соответствуют ТЗ. Обратите внимание на пробелы в двух последних именах.
    Товар: str | Literal["NA"]
    Оптовая_цена: str | float | Literal["NA"]
    Артикул: str | Literal["NA"]
    Наличие: str | Literal["NA"]
    Розничная_цена: str | float | Literal["NA"]

    def to_ordered_values(self) -> list[str | float]:
        """
        Возвращает значения в фиксированном порядке колонок,
        соответствующем FIELD_SPECS (см. ниже).
        """
        return [
            self.Товар,
            self.Оптовая_цена,
            self.Артикул,
            self.Наличие,
            self.Розничная_цена,
        ]


@dataclass(slots=True, frozen=True)
class PageTask:
    """
    Задача на загрузку листинга.

    Attributes:
        id: Внутренний идентификатор задачи (для логов/трассировки).
        url: Оригинальный URL из ввода пользователя.
        normalized_url: URL с гарантированными параметрами SHOWALL_*.
    """
    id: int
    url: str
    normalized_url: str


@dataclass(slots=True)
class ParseIssue:
    """
    Описание проблемы парсинга (не падение конвейера, а диагностическая запись).

    Attributes:
        task_id: Идентификатор PageTask.
        field_name: Имя поля (как в FieldSpec.name), по которому произошла проблема.
        code: Короткий код проблемы (например, 'ERR_PARSE_MISSING_FIELD').
        details: Текстовое описание/контекст.
    """
    task_id: int
    field_name: str
    code: str
    details: str = field(default_factory=str)


# ---------- Спецификация полей (строго 5, как в ТЗ) ----------

# ВНИМАНИЕ К ИМЕНАМ:
# Ниже имена 'name' должны соответствовать вашим колонкам один-в-один.
# По ТЗ присутствуют пробелы в двух названиях ("Розничная цена", "Оптовая цена").
# Для удобства работы внутри Python-класса ProductRecord применены безопасные имена
# с заменой пробелов на '_', но наружные заголовки колонок берутся из FieldSpec.name.

FIELD_SPECS: list[FieldSpec] = [
    FieldSpec(
        name="Товар",
        selectors=[
            SelectorVariant(
                selector="a.dark_link.js-notice-block__title",
                extract=ExtractType.ATTR,
                attr="title",
            ),
            SelectorVariant(
                selector="div.item-title",
                extract=ExtractType.TEXT,
            ),
        ],
    ),
    FieldSpec(
        name="Оптовая_цена",
        selectors=[
            SelectorVariant(
                selector="div.price_group.min span.price_value",
                extract=ExtractType.TEXT,
            ),
        ],
        normalize=[
            NormalizeRules(
                tools=[
                    "price_to_float",
                    "default_clean",
                ],
            ),
        ],
    ),
    FieldSpec(
        name="Артикул",
        selectors=[
            SelectorVariant(
                selector="span.codeProduct, span.code",
                extract=ExtractType.TEXT,
            ),
        ],
        is_unique=True,  # уникальный ключ строим по артикулу
        normalize=[
            NormalizeRules(
                tools=["mark_supplier"],
                supplier_id=123
            ),
        ],
    ),
    FieldSpec(
        name="Наличие",
        selectors=[
            SelectorVariant(
                selector="div.item-stock",
                extract=ExtractType.TEXT,
            ),
        ],
    ),
    FieldSpec(
        name="Розничная_цена",
        selectors=[
            SelectorVariant(
                selector="div.price.font-bold.font_mxs span.price_value",
                extract=ExtractType.TEXT,
            ),
        ],
        normalize=[
            NormalizeRules(
                tools=[
                    "price_to_float",
                    "default_clean",
                ],
            ),
        ],
    ),
]

# Константа с селекторами контейнеров карточек.
# Значения задаются фактическими классами сайта; по вашему требованию не выдумываю —
# оставляю пустой список для fallback-логики в extractor.
CONTAINER_SPECS: ContainerSpecs = ContainerSpecs(selectors=[
    "tr.table-view__item",
    "div.list_item.item_info",
])

__all__ = [
    "NA",
    "ExtractType",
    "SelectorVariant",
    "NormalizeRules",
    "FieldSpec",
    "ContainerSpecs",
    "CONTAINER_SPECS",
    "ProductRecord",
    "PageTask",
    "ParseIssue",
    "FIELD_SPECS",
]
```

### parsing_mode.py
<a id="core-parsing_mode.py"></a>

```python
"""
Определение режима парсинга.

Назначение:
- Явно описать тип запуска пайплайна (shallow/extended),
  чтобы не разносить магические строки по проекту.

Примечание:
- Модуль умышленно отделён от pipeline/ui, чтобы не создавать циклических зависимостей.
"""

from __future__ import annotations

from enum import StrEnum


class ParsingMode(StrEnum):
    """
    Режим парсинга.

    SHALLOW  - текущий режим: парсинг только листинга.
    EXTENDED - расширенный режим: листинг -> ссылки -> карточки товаров.
    """
    SHALLOW = "shallow"
    EXTENDED = "extended"


__all__ = ["ParsingMode"]
```

### utils_text.py
<a id="core-utils_text.py"></a>

```python
"""
Текстовые и URL-утилиты:
- Очистка текста (trim, collapse whitespace, NBSP → space).
- Нормализация ценовой строки в число.
- Работа с URL и query: корректное добавление SHOWALL_1=1, SHOWALL_3=1.
- Нормализация и дедупликация списка URL (с сохранением порядка).

Принципы:
- Не зависим от парсера/сетевого слоя.
- Не логируем приватные данные.
"""

from __future__ import annotations

import re
from collections import OrderedDict
from typing import Iterable, Literal
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse, urljoin

from app.core.models_and_specs import NA


_WS_RE = re.compile(r"\s+", flags=re.MULTILINE)
# Число: допускаем разделители пробел/не-ASCII пробел, запятую как десятичный разделитель.
_NUMBER_CHARS_RE = re.compile(r"[0-9\s\u00A0,.\-]+")


def clean_text(text: str) -> str:
    """
    Базовая нормализация текстов:
      - перевод NBSP (\u00A0) в обычный пробел
      - трим
      - схлопывание повторных пробелов/табов/переводов строк в один пробел
    """
    if text is None:
        return ""
    s = text.replace("\u00A0", " ")
    s = s.strip()
    s = _WS_RE.sub(" ", s)
    return s


def normalize_price_to_float_or_na(raw: str | None) -> float | Literal["NA"]:
    """
    Нормализует строку цены в число (float) с правилами:
      - убирает валютные символы и любые нечисловые буквы
      - удаляет пробелы-разделители тысяч
      - запятую меняет на точку, поддерживает форматы "1 234,56", "1234.56"
      - пустая/некорректная строка → "NA"

    Важно:
      - Возвращаем float или строковый литерал "NA", чтобы верхний слой мог
        напрямую класть в DataFrame, сохраняя единые типы столбцов.
    """
    if not raw:
        return NA

    candidate = clean_text(raw)

    # Выцепим допустимые символы числа и дальше нормализуем.
    m = _NUMBER_CHARS_RE.findall(candidate)
    if not m:
        return NA

    s = "".join(m)
    # Удаляем пробелы/неразрывные пробелы.
    s = s.replace(" ", "").replace("\u00A0", "")

    # Если обе точки и запятые встречаются — считаем, что десятичный разделитель последний символ из [.,]
    if "," in s and "." in s:
        # Оставляем только последний разделитель как десятичный, остальные удаляем
        last_sep_pos = max(s.rfind(","), s.rfind("."))
        integer = re.sub(r"[.,]", "", s[:last_sep_pos])
        fractional = re.sub(r"[.,]", "", s[last_sep_pos + 1 :])
        s = f"{integer}.{fractional}"
    else:
        # Унифицируем: запятая как десятичный разделитель → точка
        s = s.replace(",", ".")

    # Финальная проверка: допустимое число с опциональным минусом и десятичной частью
    if not re.fullmatch(r"-?\d+(\.\d+)?", s):
        return NA

    try:
        return float(s)
    except ValueError:
        return NA


def add_showall_params(url: str) -> str:
    """
    Добавляет/заменяет в URL параметры SHOWALL_1=1 и SHOWALL_3=1.
    Гарантирует корректную сборку query без двойных '?' и дубликатов параметров.
    """
    parsed = urlparse(url)
    # Раскладываем текущий query и обновляем значениями SHOWALL
    query_pairs = OrderedDict(parse_qsl(parsed.query, keep_blank_values=True))
    query_pairs["SHOWALL_1"] = "1"
    query_pairs["SHOWALL_3"] = "1"

    new_query = urlencode(list(query_pairs.items()), doseq=True)
    new_parsed = parsed._replace(query=new_query)
    return urlunparse(new_parsed)


def normalize_and_dedupe_urls(lines: Iterable[str]) -> list[str]:
    """
    Нормализует ввод пользователя:
      - обрезает пробелы
      - пропускает пустые строки
      - добавляет SHOWALL_* параметры
      - удаляет точные дубликаты (после нормализации), сохраняя порядок.
    """
    seen: set[str] = set()
    out: list[str] = []
    for line in lines:
        if line is None:
            continue
        s = line.strip()
        if not s:
            continue
        norm = add_showall_params(s)
        if norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out

def resolve_url(base_url: str, href: str) -> str:
    """
    (NEW) Приводит ссылку href к абсолютному URL относительно base_url.

    Использование:
    - В листинге карточки товаров часто содержат относительные ссылки.
    - Для сетевого слоя удобнее работать с абсолютными URL.

    Args:
        base_url: URL страницы-источника (листинга), относительно которой нужно резолвить.
        href: значение из атрибута href (может быть относительным).

    Returns:
        Абсолютный URL. Если href пустой — вернёт пустую строку.
    """
    if not href:
        return ""
    return urljoin(base_url, href)

__all__ = [
    "clean_text",
    "normalize_price_to_float_or_na",
    "add_showall_params",
    "normalize_and_dedupe_urls",
    "resolve_url",
]
```

## export_io/
<a id="export_io"></a>

### writer.py
<a id="export_io-writer.py"></a>

```python
# io/writer.py
"""
XlsxWriterService — экспорт результатов парсинга в XLSX по страницам.

Входные данные — универсальны (без привязки к FIELD_SPECS):
    groups: list[dict]
    [
      {"page_title": "title1", "data": [Record1, Record2, ...]},
      {"page_title": "title2", "data": [Record3, Record4, ...]}
    ]
где Record — либо dataclass-объект (любой), либо dict[str, Any].

Поведение:
- Для каждой группы создаётся отдельный лист. Имя листа — из page_title,
  с санитарной обработкой и уникализацией.
- Если список data пуст — создаётся пустой лист и выбрасывается предупреждение.
- Столбцы и порядок определяются динамически:
  * для dataclass — порядок полей как в классе;
  * для dict — ключи первой записи + новые ключи добавляются по мере встречаемости.
- Запись выполняется через xlsxwriter (без pandas).
- Выходной файл: "results/results_YYYYMMDD_HHMM.xlsx".
- Ошибки парсинга не сохраняются (только данные).

Соответствие требованиям:
- Инкапсуляция: модуль принимает уже сгруппированные данные после нормализации.
- Универсальность: структура колонок выводится из фактических записей.
- Производительность: без лишних аллокаций и преобразований; авто-ширина колонок
  вычисляется однократным проходом.
"""

from __future__ import annotations

from dataclasses import is_dataclass, asdict, fields
from typing import cast
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
import re
import warnings

import xlsxwriter


class XlsxWriterService:
    """
    Экспортирует данные в XLSX, создавая по листу на каждую страницу (page_title).

    Публичный API:
        write(groups: list[dict]) -> str
            Возвращает путь к созданному файлу XLSX.
    """

    # Excel: недопустимые символы в имени листа и максимальная длина.
    _RE_INVALID_SHEET_CHARS = re.compile(r"[:\\/?*\[\]]")
    _SHEETNAME_MAX = 31
    # Базовый лимит для "ядра" имени (оставляем запас под '...' и суффикс '_N').
    _BASE_CORE_LIMIT = 28

    def write(self, groups: list[dict[str, Any]]) -> str:
        """
        Записывает XLSX-файл по группам данных.

        Args:
            groups: список словарей с ключами:
                - "page_title": str — заголовок страницы (будет именем листа)
                - "data": list[dict|dataclass] — записи товаров

        Returns:
            Путь к XLSX-файлу (str).
        """
        if not isinstance(groups, list):
            raise TypeError("groups must be a list of dict objects")

        out_dir = Path("results")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / datetime.now().strftime("results_%Y%m%d_%H%M.xlsx")

        # Имя файла может существовать; перезаписываем.
        workbook = xlsxwriter.Workbook(out_path.as_posix())
        try:
            used_names_ci: set[str] = set()  # case-insensitive уникальность листов

            # Общие форматы для заголовков/ячееек
            header_fmt = workbook.add_format(
                {
                    "bold": True,
                    "align": "left",
                    "valign": "vcenter",
                    "bg_color": "#F2F2F2",
                    "border": 1,
                }
            )
            cell_fmt = workbook.add_format({"align": "left", "valign": "vcenter"})

            for idx, group in enumerate(groups):
                title = str(group.get("page_title", f"Sheet{idx+1}"))
                data = group.get("data", [])
                sheet_name = self._make_unique_sheet_name(title, used_names_ci)

                ws = workbook.add_worksheet(sheet_name)
                used_names_ci.add(sheet_name.lower())

                # Пустой лист — предупреждение и переход к следующему
                if not data:
                    warnings.warn(
                        f"[XlsxWriterService] Пустые данные для листа '{sheet_name}'. "
                        f"Создан пустой лист.",
                        RuntimeWarning,
                        stacklevel=2,
                    )
                    continue

                # Преобразуем записи к списку словарей с фиксированным порядком ключей
                rows_dicts = self._normalize_rows(data)

                # Заголовки: из первой строки + дополнения по мере встречаемости
                headers = self._infer_headers(rows_dicts)

                # Запись заголовков
                for col, name in enumerate(headers):
                    ws.write(0, col, name, header_fmt)

                # Вычисление ширины колонок (один проход)
                col_widths = [max(8, len(str(h)[:500])) for h in headers]  # старт от заголовков
                max_width = 60  # разумный предел ширины
                for r, row in enumerate(rows_dicts, start=1):
                    for c, key in enumerate(headers):
                        val = row.get(key, "")
                        # Пишем значение (xlsxwriter сам определит тип)
                        ws.write(r, c, val, cell_fmt)
                        w = len(self._to_str_for_width(val))
                        if w > col_widths[c]:
                            col_widths[c] = min(w, max_width)

                # Применяем ширину колонок
                for c, w in enumerate(col_widths):
                    ws.set_column(c, c, w)

                # Заморозка верхней строки
                ws.freeze_panes(1, 0)

        finally:
            workbook.close()

        return out_path.as_posix()

    # ------------------------- Вспомогательные методы -------------------------

    def _normalize_rows(self, data: Iterable[Any]) -> list[dict[str, Any]]:
        """
        Приводит итерацию записей к списку словарей.
        Поддерживает dataclass и dict. Прочие типы — ошибка.
        """
        out: list[dict[str, Any]] = []
        for i, rec in enumerate(data):
            if is_dataclass(rec) and not isinstance(rec, type):
                # asdict сохраняет порядок согласно объявлению полей dataclass
                out.append({f.name: getattr(rec, f.name) for f in fields(rec)})
            elif isinstance(rec, dict):
                out.append(dict(rec))  # копия для устойчивости
            else:
                raise TypeError(
                    "Each record must be a dataclass or a dict. "
                    f"Got type={type(rec)!r} at index={i}"
                )
        return out

    def _infer_headers(self, rows: list[dict[str, Any]]) -> list[str]:
        """
        Определяет порядок столбцов:
        - базируется на ключах первой записи;
        - неизвестные ключи добавляются в конце по мере встречаемости.
        """
        if not rows:
            return []

        seen: set[str] = set()
        headers: list[str] = []

        # База — ключи первой строки в их порядке
        for k in rows[0].keys():
            headers.append(k)
            seen.add(k)

        # Добавляем новые ключи по мере обнаружения
        for row in rows[1:]:
            for k in row.keys():
                if k not in seen:
                    headers.append(k)
                    seen.add(k)
        return headers

    def _make_unique_sheet_name(
        self, title: str, used_names_ci: set[str]
    ) -> str:
        """
        Санитизирует и уникализирует имя листа под ограничения Excel:
        - удаляет запрещённые символы [: \\ / ? * [ ]];
        - схлопывает пробелы; обрезает по базовому лимиту;
        - при обрезке добавляет '...';
        - при конфликте добавляет суффикс '_N';
        - итоговая длина не превышает 31 символ.
        """
        # Удаляем запрещённые символы
        name = self._RE_INVALID_SHEET_CHARS.sub("", title)
        # Заменяем управляющие символы на пробел, схлопываем, обрезаем
        name = re.sub(r"\s+", " ", name).strip()
        if not name:
            name = "Sheet"

        truncated = False
        core = name
        if len(core) > self._BASE_CORE_LIMIT:
            core = core[: self._BASE_CORE_LIMIT]
            truncated = True

        base = core + ("..." if truncated else "")
        if not base:
            base = "Sheet"

        # Безопасный кандидат
        candidate = self._fit_to_limit(base)
        if candidate.lower() not in used_names_ci:
            return candidate

        # Разрешение коллизий: _1, _2, ...
        n = 1
        while True:
            suffix = f"_{n}"
            # Подгоняем длину с учётом суффикса
            allowed = self._SHEETNAME_MAX - len(suffix)
            # Если '...' присутствует — тоже учитываем
            has_ellipsis = base.endswith("...")
            core_part = core
            # Обрезаем так, чтобы поместились base(с '...' если было) + суффикс
            if has_ellipsis:
                # оставляем место под '...'
                allowed_core = max(1, allowed - 3)
                core_part = core_part[:allowed_core]
                cand = core_part + "..." + suffix
            else:
                core_part = core_part[:allowed]
                cand = core_part + suffix

            cand = self._fit_to_limit(cand)

            if cand.lower() not in used_names_ci:
                return cand
            n += 1

    def _fit_to_limit(self, name: str) -> str:
        """
        Обрезает имя по жёсткому лимиту Excel (31 символ).
        """
        if len(name) <= self._SHEETNAME_MAX:
            return name
        return name[: self._SHEETNAME_MAX]

    @staticmethod
    def _to_str_for_width(value: Any) -> str:
        """
        Строковое представление для оценки ширины колонки.
        Числа не форматируем специально, просто str().
        """
        if value is None:
            return ""
        return str(value)


__all__ = ["XlsxWriterService"]
```

## net/
<a id="net"></a>

### auth.py
<a id="net-auth.py"></a>

```python
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
```

### session_and_fetcher.py
<a id="net-session_and_fetcher.py"></a>

```python
"""
Сетевой слой: HTTP-сессия и конкурентная выборка страниц.

Состав:
- SessionManager: единый httpx.AsyncClient (HTTP/2, keep-alive, CookieJar),
  методы get/post/is_authenticated/close, явная отметка успешного логина.
- PageFetcher: конкурентный GET по очереди URL с ограничением параллелизма.

Принципы:
- Ретраи реализованы вручную (экспоненциальная задержка), чтобы не зависеть от
  версии httpx и внешних плагинов.
- Исключения инфраструктуры мэппятся на понятные типы из core.errors при необходимости
  во внешних слоях; здесь возвращаются «сырые» ошибки для гибкости.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import Iterable, Final, Optional
from collections.abc import Mapping

import httpx

from app.core.errors import HttpStatusError, NetworkError, TimeoutError_
from app.core.utils_text import add_showall_params


# Дефолтные константы для сессии/пула
_DEFAULT_UA: Final[str] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@dataclass(slots=True, frozen=True)
class SessionConfig:
    """
    Конфигурация HTTP-сессии.

    Attributes:
        base_url: Базовый URL (можно оставить пустым).
        connect_timeout_s: Таймаут установки соединения.
        read_timeout_s: Таймаут чтения ответа.
        max_connections: Максимум одновременных соединений в пуле.
        max_keepalive_connections: Максимум keep-alive соединений.
        http2: Включение HTTP/2.
        default_headers: Базовые заголовки клиента (User-Agent и т.д.).
    """
    base_url: str = ""
    connect_timeout_s: float = 5.0
    read_timeout_s: float = 10.0
    max_connections: int = 64
    max_keepalive_connections: int = 20
    http2: bool = True
    default_headers: Mapping[str, str] | None = None  # подставим ниже


class SessionManager:
    """
    Обёртка над httpx.AsyncClient: общий клиент, CookieJar, таймауты/пул/HTTP2.

    Задачи:
        - Создаёт и хранит один AsyncClient на процесс парсинга.
        - Предоставляет методы GET/POST.
        - Держит флаг аутентификации (устанавливается адаптером авторизации).

    Потоки/асинхронность:
        - Класс предназначен для использования в асинхронной среде.
    """

    def __init__(self, cfg: Optional[SessionConfig] = None) -> None:
        if cfg is None:
            cfg = SessionConfig()

        # Сформируем финальные заголовки в обычный dict[str, str]
        if cfg.default_headers is None:
            self._default_headers: dict[str, str] = {
                "User-Agent": _DEFAULT_UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "ru-RU,ru;q=0.9",
                "Cache-Control": "no-cache",
            }
        else:
            self._default_headers = dict(cfg.default_headers)

        self._cfg = cfg
        self._client = httpx.AsyncClient(
            base_url=self._cfg.base_url,
            http2=self._cfg.http2,
            headers=self._default_headers,  # используем уже гарантированно dict[str, str]
            timeout=httpx.Timeout(
                connect=self._cfg.connect_timeout_s,
                read=self._cfg.read_timeout_s,
                write=self._cfg.read_timeout_s,
                pool=self._cfg.connect_timeout_s,
            ),
            limits=httpx.Limits(
                max_connections=self._cfg.max_connections,
                max_keepalive_connections=self._cfg.max_keepalive_connections,
            ),
            cookies=httpx.Cookies(),
            follow_redirects=True,
            verify=True,
        )
        self._is_authenticated: bool = False

    # --------- свойства/служебные ---------

    @property
    def default_headers(self) -> dict[str, str]:
        """Базовые заголовки клиента (можно расширять в вызовах)."""
        return self._default_headers.copy()

    def mark_authenticated(self, value: bool = True) -> None:
        """Отмечает состояние аутентификации для текущей сессии."""
        self._is_authenticated = bool(value)

    def is_authenticated(self) -> bool:
        """Возвращает True, если адаптер авторизации отметил сессию как успешную."""
        return self._is_authenticated

    # --------- сетевые операции ---------

    async def get(
        self,
        url: str,
        *,
        headers: Optional[dict[str, str]] = None,
        max_retries: int = 2,
        retry_backoff_base: float = 0.3,
        acceptable_statuses: tuple[int, ...] = (200,),
    ) -> httpx.Response:
        """
        Выполняет GET с ручными ретраями.

        Политика ретраев:
            - Повторы на сетевые ошибки и таймауты.
            - На HTTP-статусы не из acceptable_statuses — без ретраев, сразу HttpStatusError.

        Raises:
            HttpStatusError: Если статус не входит в acceptable_statuses.
            TimeoutError_: При таймаутах после всех попыток.
            NetworkError: При сетевых сбоях после всех попыток.
        """
        last_err: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                resp = await self._client.get(url, headers=headers)
                if acceptable_statuses and resp.status_code not in acceptable_statuses:
                    raise HttpStatusError(resp.status_code, url)
                return resp
            except httpx.ReadTimeout as e:
                last_err = e
                if attempt >= max_retries:
                    raise TimeoutError_(f"GET timeout after {attempt+1} attempts: {url}") from e
            except (httpx.ConnectError, httpx.NetworkError) as e:  # NetworkError базовый для ряда сбоев
                last_err = e
                if attempt >= max_retries:
                    raise NetworkError(f"GET network error after {attempt+1} attempts: {url}") from e
            # экспоненциальная задержка
            await asyncio.sleep(retry_backoff_base * math.pow(2, attempt))

        # страховка; сюда не должны попасть
        assert last_err is not None
        raise last_err  # pragma: no cover

    async def post(
        self,
        url: str,
        *,
        data: dict | None = None,
        headers: Optional[dict[str, str]] = None,
        max_retries: int = 1,
        retry_backoff_base: float = 0.3,
        acceptable_statuses: tuple[int, ...] = (200,),
    ) -> httpx.Response:
        """
        Выполняет POST с ручными ретраями на сетевые сбои/таймауты.

        На неожиданный HTTP-статус — исключение HttpStatusError без ретраев.
        """
        last_err: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                resp = await self._client.post(url, data=data, headers=headers)
                if acceptable_statuses and resp.status_code not in acceptable_statuses:
                    raise HttpStatusError(resp.status_code, url)
                return resp
            except httpx.ReadTimeout as e:
                last_err = e
                if attempt >= max_retries:
                    raise TimeoutError_(f"POST timeout after {attempt+1} attempts: {url}") from e
            except (httpx.ConnectError, httpx.NetworkError) as e:
                last_err = e
                if attempt >= max_retries:
                    raise NetworkError(f"POST network error after {attempt+1} attempts: {url}") from e
            await asyncio.sleep(retry_backoff_base * math.pow(2, attempt))

        assert last_err is not None
        raise last_err  # pragma: no cover

    async def close(self) -> None:
        """Закрывает внутренний AsyncClient."""
        await self._client.aclose()


# ----------------------------- Fetcher ---------------------------------


@dataclass(slots=True, frozen=True)
class FetchedPage:
    """
    Результат загрузки одной страницы.

    Attributes:
        url: Запрашиваемый URL (уже с SHOWALL_*).
        status: HTTP-статус ответа (или None при сетевом исключении).
        text: Текст ответа (None при ошибке статуса или исключении).
        error: Исключение (если было); не выбрасывается наружу при mode='collect'.
    """
    url: str
    status: Optional[int]
    text: Optional[str]
    error: Optional[Exception] = None


class PageFetcher:
    """
    Конкурентная загрузка страниц с контролем параллелизма.

    Использование:
        fetcher = PageFetcher(session, concurrency=24)
        pages = await fetcher.fetch_many(urls)

    Примечания:
        - URL автоматически дополняются SHOWALL_1=1 и SHOWALL_3=1.
        - Дедупликацию лучше делать заранее (см. core.utils_text.normalize_and_dedupe_urls),
          но fetcher всё равно нормализует query для идемпотентности.
        - fetch_many(..., add_showall_params=False) для карточек товара.
    """

    def __init__(self, session: SessionManager, *, concurrency: int = 24) -> None:
        self._session = session
        self._sem = asyncio.Semaphore(max(1, concurrency))

    async def _fetch_one(self, url: str, *, add_showall_params_flag: bool) -> FetchedPage:
        # Для листинга: гарантируем SHOWALL_* параметры.
        # Для карточек: add_showall_params_flag=False => URL не изменяем.
        requested_url = add_showall_params(url) if add_showall_params_flag else url

        async with self._sem:
            try:
                resp = await self._session.get(requested_url)
                return FetchedPage(
                    url=requested_url,
                    status=resp.status_code,
                    text=resp.text if resp.status_code == 200 else None,
                    error=None if resp.status_code == 200 else HttpStatusError(resp.status_code, requested_url),
                )
            except Exception as e:
                return FetchedPage(url=requested_url, status=None, text=None, error=e)

    async def fetch_many(
        self,
        urls: Iterable[str],
        *,
        add_showall_params: bool = True,
    ) -> list[FetchedPage]:
        """
        Загружает набор URL конкурентно.

        Args:
            urls: набор URL.
            add_showall_params: добавлять ли SHOWALL_* параметры (по умолчанию True для листинга).

        Returns:
            Список FetchedPage в порядке завершения задач.
        """
        tasks = [
            asyncio.create_task(self._fetch_one(u, add_showall_params_flag=add_showall_params))
            for u in urls
        ]
        results: list[FetchedPage] = []
        for t in asyncio.as_completed(tasks):
            results.append(await t)
        return results


__all__ = ["SessionConfig", "SessionManager", "FetchedPage", "PageFetcher"]
```

## parsing/
<a id="parsing"></a>

### aggregator.py
<a id="parsing-aggregator.py"></a>

```python
"""
ProductRecordAggregator — объединение данных листинга и карточки в единый словарь.

Почему словарь:
- По вашему требованию агрегатор "соединяет результаты ... в общий словарь".
- Это даёт возможность динамически добавлять характеристики как новые колонки.
- XlsxWriterService уже умеет писать dict как строки.

Правила объединения:
- Источник по умолчанию: листинг.
- Карточка имеет приоритет, но:
  * пустые значения и "NA" не должны затирать полезные значения из листинга.
- Техническое поле URL добавляется (по умолчанию) как отдельная колонка.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from app.core.dto_extended import PartialProduct, CardProductData
from app.core.models_and_specs import ParseIssue


@dataclass(slots=True, frozen=True)
class AggregatorConfig:
    """
    Конфигурация агрегатора.

    Attributes:
        include_product_url_column: добавлять ли техническую колонку с URL товара.
        product_url_column_name: имя колонки URL в XLSX.
    """
    include_product_url_column: bool = False
    product_url_column_name: str = "URL_товара"


class ProductRecordAggregator:
    """
    Агрегирует PartialProduct и CardProductData в один плоский dict для экспорта.
    """

    def __init__(self, *, config: Optional[AggregatorConfig] = None) -> None:
        self._cfg = config or AggregatorConfig()

    def aggregate(
        self,
        base: PartialProduct,
        card: Optional[CardProductData],
    ) -> tuple[dict[str, Any], list[ParseIssue]]:
        """
        Объединяет данные.

        Args:
            base: данные с листинга.
            card: данные с карточки (может отсутствовать, если URL не найден или карточка не загрузилась).

        Returns:
            record: плоский словарь (готов к экспорту).
            issues: список проблем агрегации (конфликты/несостыковки).
        """
        issues: list[ParseIssue] = []
        record: dict[str, Any] = dict(base.values)

        # Техническая колонка URL
        if self._cfg.include_product_url_column and base.product_url:
            record[self._cfg.product_url_column_name] = base.product_url

        if card is None:
            return record, issues

        # Несоответствие URL (в норме совпадают)
        if base.product_url and card.product_url and base.product_url != card.product_url:
            issues.append(
                ParseIssue(
                    task_id=base.task_id,
                    field_name="__merge__",
                    code="ERR_MERGE_URL_MISMATCH",
                    details=f"base_url={base.product_url} card_url={card.product_url}",
                )
            )

        # Правило мерджа: карточка имеет приоритет, но NA/пустое не затирает.
        for k, v in (card.values or {}).items():
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue

            existing = record.get(k)

            # "NA" не должно затирать полезные значения
            if v == "NA":
                if existing not in (None, "", "NA"):
                    continue

            record[k] = v

        return record, issues


__all__ = ["AggregatorConfig", "ProductRecordAggregator"]
```

### card_extractor.py
<a id="parsing-card_extractor.py"></a>

```python
"""
ProductCardExtractor — разбор HTML карточки товара для extended-режима.

ВАЖНО:
- Парсинг аналогов ПОЛНОСТЬЮ УДАЛЁН.
- Экстрактор занимается ТОЛЬКО:
  * базовыми полями товара,
  * характеристиками (строго из целевого контейнера).

Цель:
- исключить ложные срабатывания,
- упростить модель данных,
- сохранить предсказуемость XLSX.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

try:
    from selectolax.lexbor import LexborHTMLParser as _HTMLParser  # type: ignore
except Exception:  # pragma: no cover
    from selectolax.parser import HTMLParser as _HTMLParser  # type: ignore

from app.app_logging.logbus import LogBus
from app.core.dto_extended import CardProductData
from app.core.models_and_specs import FIELD_SPECS, ExtractType, FieldSpec, ParseIssue, NA
from app.core.utils_text import clean_text, normalize_price_to_float_or_na


@dataclass(slots=True, frozen=True)
class CardExtractorConfig:
    """
    Конфигурация экстрактора карточки товара.
    """

    page_title_selector: str = "h1"

    base_field_names: tuple[str, ...] = (
        "Товар",
        "Артикул",
        "Наличие",
        "Розничная_цена",
        "Оптовая_цена",
    )

    # Контейнеры характеристик
    characteristics_root_selectors: tuple[str, ...] = (
        ".properties-group",
        ".properties-group__list",
        ".detail_props",
        ".props",
    )

    max_characteristics: int = 250

    # Фильтрация "мусорных" ключей
    max_key_length: int = 60
    max_key_words: int = 6
    stopwords_in_key: tuple[str, ...] = (
        "каталог",
        "регион",
        "урал",
        "москва",
        "краснодар",
        "сибирь",
        "дальний",
        "калькулятор",
        "экономьте",
    )

    characteristic_collision_prefix: str = "Характеристика: "
    treat_wholesale_missing_as_error: bool = False
    log_rejected_keys_preview: int = 15


class ProductCardExtractor:
    """
    Экстрактор карточки товара (без аналогов).
    """

    def __init__(
        self,
        *,
        log_bus: Optional[LogBus] = None,
        field_specs: Optional[list[FieldSpec]] = None,
        config: Optional[CardExtractorConfig] = None,
    ) -> None:
        self._log = log_bus
        self._cfg = config or CardExtractorConfig()

        specs = field_specs or FIELD_SPECS
        wanted = set(self._cfg.base_field_names)
        self._base_specs = [s for s in specs if s.name in wanted]

        self._retail_name = self._find_name("Розничная_цена", specs)
        self._wholesale_name = self._find_name("Оптовая_цена", specs)
        self._product_name = self._find_name("Товар", specs)

    def extract(self, html: str, *, task_id: int, product_url: str) -> CardProductData:
        if self._log:
            self._log.info(
                "CARD_PARSE_START",
                "Start parsing product card",
                context={"task_id": task_id, "url": product_url},
            )

        tree = _HTMLParser(html)
        root = tree.root
        if root is None:
            return CardProductData(
                task_id=task_id,
                product_url=product_url,
                values={},
                issues=[
                    ParseIssue(
                        task_id=task_id,
                        field_name="__page__",
                        code="ERR_EMPTY_HTML",
                        details="Empty DOM on card page",
                    )
                ],
            )

        issues: list[ParseIssue] = []
        values: dict[str, Any] = {}

        # ---------- Базовые поля ----------
        extracted_base: dict[str, Any] = {}
        missing_base: list[str] = []

        for spec in self._base_specs:
            val = self._extract_in_scope(root, spec)
            if not val:
                if not (spec.name == self._wholesale_name and not self._cfg.treat_wholesale_missing_as_error):
                    issues.append(
                        ParseIssue(
                            task_id=task_id,
                            field_name=spec.name,
                            code="ERR_PARSE_MISSING_FIELD_CARD",
                            details=f"missing on card; selectors={[v.selector for v in spec.selectors]}",
                        )
                    )
                values[spec.name] = NA
                missing_base.append(spec.name)
            else:
                values[spec.name] = val
                extracted_base[spec.name] = val

        # fallback на h1
        if self._product_name and values.get(self._product_name, NA) in (NA, "", None):
            h1 = root.css_first(self._cfg.page_title_selector)
            if h1 is not None:
                title = clean_text(h1.text() or "")
                if title:
                    values[self._product_name] = title
                    extracted_base[self._product_name] = title

        # нормализация цен
        if self._retail_name and values.get(self._retail_name) not in (None, NA):
            values[self._retail_name] = normalize_price_to_float_or_na(str(values[self._retail_name]))

        if self._wholesale_name and values.get(self._wholesale_name) not in (None, NA):
            values[self._wholesale_name] = normalize_price_to_float_or_na(str(values[self._wholesale_name]))

        if self._log:
            self._log.info(
                "CARD_BASE_FIELDS",
                "Base fields extracted",
                context={
                    "task_id": task_id,
                    "url": product_url,
                    "extracted_count": len(extracted_base),
                    "missing_count": len(missing_base),
                    "missing": missing_base[:30],
                    "extracted_preview": self._preview_dict(extracted_base, limit=8),
                },
            )

        # ---------- Характеристики ----------
        characteristics, char_stats = self._extract_characteristics_scoped(root)

        collisions = 0
        for k, v in characteristics.items():
            key = k
            if key in values:
                collisions += 1
                key = f"{self._cfg.characteristic_collision_prefix}{key}"
            values[key] = v

        if self._log:
            self._log.info(
                "CARD_CHARACTERISTICS",
                "Characteristics extracted",
                context={
                    "task_id": task_id,
                    "url": product_url,
                    "count": len(characteristics),
                    "collisions": collisions,
                    "stats": char_stats,
                    "preview": self._preview_dict(characteristics, limit=10),
                    "rejected_preview": char_stats.get("rejected_preview", []),
                    "matched_root_selector": char_stats.get("matched_root_selector"),
                },
            )

        if self._log:
            self._log.info(
                "CARD_PARSE_DONE",
                "Card parsing finished",
                context={
                    "task_id": task_id,
                    "url": product_url,
                    "total_fields": len(values),
                    "issues": len(issues),
                },
            )

        return CardProductData(
            task_id=task_id,
            product_url=product_url,
            values=values,
            issues=issues,
        )

    # ---------------- internals ----------------

    def _extract_in_scope(self, scope_node, spec: FieldSpec) -> str:
        for var in spec.selectors:
            found = scope_node.css_first(var.selector)
            if found is None:
                continue
            if var.extract == ExtractType.TEXT:
                val = clean_text(found.text() or "")
            else:
                attr = var.attr or ""
                val = clean_text(found.attributes.get(attr, ""))
            if val:
                return val
        return ""

    def _extract_characteristics_scoped(self, root_node) -> tuple[dict[str, str], dict[str, Any]]:
        out: dict[str, str] = {}
        rejected_keys: list[str] = []

        stats: dict[str, Any] = {
            "root_found": 0,
            "added": 0,
            "skipped_empty": 0,
            "duplicates_same": 0,
            "duplicates_renamed": 0,
            "rejected_key": 0,
            "matched_root_selector": None,
            "rejected_preview": [],
        }

        scope = None
        for sel in self._cfg.characteristics_root_selectors:
            node = root_node.css_first(sel)
            if node is not None:
                scope = node
                stats["root_found"] = 1
                stats["matched_root_selector"] = sel
                break

        if scope is None:
            return {}, stats

        def _normalize_key(key: str) -> str:
            k = clean_text(key)
            if k.endswith(":"):
                k = k[:-1].strip()
            return k

        def _is_bad_key(k: str) -> bool:
            kk = k.lower()
            if len(k) > self._cfg.max_key_length:
                return True
            if len(k.split()) > self._cfg.max_key_words:
                return True
            for sw in self._cfg.stopwords_in_key:
                if sw and sw in kk:
                    return True
            return False

        def _add_pair(key_raw: str, val_raw: str) -> None:
            if len(out) >= self._cfg.max_characteristics:
                return
            k = _normalize_key(key_raw)
            v = clean_text(val_raw)
            if not k or not v:
                stats["skipped_empty"] += 1
                return
            if _is_bad_key(k):
                stats["rejected_key"] += 1
                if len(rejected_keys) < self._cfg.log_rejected_keys_preview:
                    rejected_keys.append(k)
                return
            if k not in out:
                out[k] = v
                stats["added"] += 1
                return
            if out[k] == v:
                stats["duplicates_same"] += 1
                return
            n = 2
            while True:
                kk = f"{k} ({n})"
                if kk not in out:
                    out[kk] = v
                    stats["duplicates_renamed"] += 1
                    stats["added"] += 1
                    return
                n += 1

        for item in scope.css(".properties-group__item") or []:
            name_node = item.css_first('[itemprop="name"]') or item.css_first(".properties-group__name")
            value_node = item.css_first('[itemprop="value"]') or item.css_first(".properties-group__value")
            if name_node is None or value_node is None:
                continue
            _add_pair(name_node.text() or "", value_node.text() or "")

        for table in scope.css("table") or []:
            for tr in table.css("tr") or []:
                th = tr.css_first("th")
                td = tr.css_first("td")
                if th is not None and td is not None:
                    _add_pair(th.text() or "", td.text() or "")
                    continue
                tds = tr.css("td") or []
                if len(tds) >= 2:
                    _add_pair(tds[0].text() or "", tds[1].text() or "")

        for dl in scope.css("dl") or []:
            dts = dl.css("dt") or []
            dds = dl.css("dd") or []
            for dt, dd in zip(dts, dds):
                _add_pair(dt.text() or "", dd.text() or "")

        stats["rejected_preview"] = rejected_keys
        return out, stats

    @staticmethod
    def _find_name(name: str, specs: list[FieldSpec]) -> Optional[str]:
        for s in specs:
            if s.name == name:
                return s.name
        return None

    @staticmethod
    def _preview_dict(d: dict[str, Any], *, limit: int = 10) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for i, (k, v) in enumerate(d.items()):
            if i >= limit:
                break
            out[k] = v
        return out


__all__ = ["CardExtractorConfig", "ProductCardExtractor"]
```

### extractor.py
<a id="parsing-extractor.py"></a>

```python
"""
ProductExtractor: извлечение карточек товаров со страницы листинга.

Существующий режим (shallow):
- extract(html, task_id) -> list[ProductRecord], list[ParseIssue], page_title

Нововведение (extended):
- extract_partials(html, task_id, base_url) -> list[PartialProduct], issues, page_title
  где PartialProduct содержит базовые поля + ссылку на карточку товара (если найдено).

Важно:
- extract(...) сохранён и не меняет сигнатуру/поведение для shallow-режима.
- product_url извлекается опционально (через ExtractorConfig.collect_product_url).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any

try:
    from selectolax.lexbor import LexborHTMLParser as _HTMLParser  # type: ignore
except Exception:  # pragma: no cover
    from selectolax.parser import HTMLParser as _HTMLParser  # type: ignore

from app.core.dto_extended import PartialProduct
from app.core.models_and_specs import (
    FIELD_SPECS,
    CONTAINER_SPECS,
    ExtractType,
    FieldSpec,
    ProductRecord,
    ParseIssue,
)
from app.core.utils_text import clean_text, normalize_price_to_float_or_na, resolve_url


@dataclass(slots=True, frozen=True)
class ExtractorConfig:
    """
    Настройки извлечения со страницы листинга.

    Нововведение:
        collect_product_url:
            Если True — экстрактор пытается извлечь URL карточки товара из каждой карточки.
            Если False — URL не извлекаем (чтобы не менять поведение shallow-режима).
    """
    page_container_selector: str = "div.inner_wrapper"
    treat_wholesale_missing_as_error: bool = False
    collect_product_url: bool = False


@dataclass(slots=True)
class _CardParseResult:
    """
    Внутренний результат разбора одной карточки в листинге.

    Attributes:
        values: словарь field_name -> value (строка/float/"NA").
        product_url: ссылка на карточку (может быть None).
    """
    values: dict[str, Any]
    product_url: Optional[str]


class ProductExtractor:
    """
    Извлекает товары со страницы листинга.

    Важные особенности:
    - Работает строго в границах контейнеров карточек (CONTAINER_SPECS).
    - Делает дедуп карточек по полям is_unique=True.
    - Поддерживает расширенный сбор URL карточки, не ломая extract(...).
    """

    def __init__(
        self,
        field_specs: list[FieldSpec] | None = None,
        config: ExtractorConfig | None = None,
    ) -> None:
        self._specs: list[FieldSpec] = field_specs or FIELD_SPECS
        self._cfg = config or ExtractorConfig()

        if not self._specs:
            raise ValueError("FIELD_SPECS must not be empty")

        self._retail_idx = self._find_spec_index_by_name("Розничная_цена")
        self._wholesale_idx = self._find_spec_index_by_name("Оптовая_цена")
        self._unique_specs = [s for s in self._specs if getattr(s, "is_unique", False)]

    # ------------------------------------------------------------------
    # Публичный API (shallow) — НЕ МЕНЯЕМ
    # ------------------------------------------------------------------

    def extract(self, html: str, *, task_id: int) -> tuple[list[ProductRecord], list[ParseIssue], str]:
        """
        Классический (shallow) разбор HTML-страницы листинга.

        Returns:
            products: список уникальных ProductRecord.
            issues: список ParseIssue.
            page_title: заголовок страницы (h1), если есть.
        """
        card_results, issues, page_title = self._extract_cards_common(
            html=html,
            task_id=task_id,
            base_url=None,
            collect_product_url=False,
        )

        products: list[ProductRecord] = []
        for cr in card_results:
            products.append(self._to_product_record(cr.values))
        return products, issues, page_title

    # ------------------------------------------------------------------
    # Публичный API (extended)
    # ------------------------------------------------------------------

    def extract_partials(
        self,
        html: str,
        *,
        task_id: int,
        base_url: str,
    ) -> tuple[list[PartialProduct], list[ParseIssue], str]:
        """
        (NEW) Разбор листинга для extended-режима.

        В отличие от extract(...):
        - возвращает PartialProduct (values + product_url),
        - product_url извлекается только если config.collect_product_url=True.

        Args:
            html: HTML листинга.
            task_id: id задачи листинга.
            base_url: URL листинга (нужен для преобразования относительных ссылок).

        Returns:
            partials: список PartialProduct.
            issues: список ParseIssue.
            page_title: заголовок страницы (h1), если есть.
        """
        card_results, issues, page_title = self._extract_cards_common(
            html=html,
            task_id=task_id,
            base_url=base_url,
            collect_product_url=self._cfg.collect_product_url,
        )

        partials: list[PartialProduct] = []
        for idx, cr in enumerate(card_results, start=1):
            partials.append(
                PartialProduct(
                    task_id=task_id,
                    product_index=idx,
                    product_url=cr.product_url,
                    values=dict(cr.values),
                )
            )
        return partials, issues, page_title

    # ------------------------------------------------------------------
    # Общая логика
    # ------------------------------------------------------------------

    def _extract_cards_common(
        self,
        *,
        html: str,
        task_id: int,
        base_url: Optional[str],
        collect_product_url: bool,
    ) -> tuple[list[_CardParseResult], list[ParseIssue], str]:
        """
        Общая логика разбора листинга (для shallow и extended).

        Returns:
            card_results: список _CardParseResult
            issues: список ParseIssue
            page_title: h1 текст
        """
        tree = _HTMLParser(html)
        h1_node = tree.css_first("h1")
        page_title = h1_node.text(strip=True) if h1_node else ""

        root = tree.root
        if root is None:
            return [], [ParseIssue(task_id=task_id, field_name="__page__", code="ERR_EMPTY_HTML", details="Empty DOM")], ""

        page_container = root.css_first(self._cfg.page_container_selector)
        if page_container is None:
            return [], [
                ParseIssue(
                    task_id=task_id,
                    field_name="__page__",
                    code="ERR_CONTAINER_NOT_FOUND",
                    details=self._cfg.page_container_selector,
                )
            ], ""

        card_nodes = self._find_card_containers(page_container)
        if not card_nodes:
            return [], [
                ParseIssue(
                    task_id=task_id,
                    field_name="__page__",
                    code="ERR_CONTAINER_NOT_FOUND",
                    details=f"no card containers via {CONTAINER_SPECS.selectors!r}",
                )
            ], ""

        # Дедуп карточек по is_unique
        seen_keys: set[tuple[str, ...]] = set()
        unique_cards: list[object] = []
        for card in card_nodes:
            key = self._build_unique_key(card, self._unique_specs)
            if key is None:
                unique_cards.append(card)
                continue
            if key in seen_keys:
                continue
            seen_keys.add(key)
            unique_cards.append(card)

        card_results: list[_CardParseResult] = []
        issues: list[ParseIssue] = []

        for card in unique_cards:
            values_by_name: dict[str, Any] = {}

            # Извлечение полей в рамках карточки
            for idx, spec in enumerate(self._specs):
                val = self._extract_in_container(card, spec)

                if not val:
                    if not (idx == self._wholesale_idx and not self._cfg.treat_wholesale_missing_as_error):
                        issues.append(
                            ParseIssue(
                                task_id=task_id,
                                field_name=spec.name,
                                code="ERR_PARSE_MISSING_FIELD",
                                details=f"missing in card; selectors={[v.selector for v in spec.selectors]}",
                            )
                        )
                    values_by_name[spec.name] = "NA"
                else:
                    values_by_name[spec.name] = val

            # Нормализация цен уже на этапе извлечения (как было раньше)
            if self._retail_idx is not None:
                rn = self._specs[self._retail_idx].name
                rv = values_by_name.get(rn, "NA")
                values_by_name[rn] = normalize_price_to_float_or_na(rv if isinstance(rv, str) else str(rv))

            if self._wholesale_idx is not None:
                wn = self._specs[self._wholesale_idx].name
                wv = values_by_name.get(wn, "NA")
                values_by_name[wn] = normalize_price_to_float_or_na(wv if isinstance(wv, str) else str(wv))

            # (NEW) извлечение ссылки на карточку товара
            product_url: Optional[str] = None
            if collect_product_url:
                href = self._extract_product_url(card, base_url=base_url)
                product_url = href if href else None
                if not product_url:
                    issues.append(
                        ParseIssue(
                            task_id=task_id,
                            field_name="__product_url__",
                            code="ERR_PARSE_MISSING_PRODUCT_URL",
                            details="Could not extract product URL from listing card",
                        )
                    )

            card_results.append(_CardParseResult(values=values_by_name, product_url=product_url))

        return card_results, issues, page_title

    # ------------------------------------------------------------------
    # Внутренние методы
    # ------------------------------------------------------------------

    def _find_card_containers(self, scope_node) -> list[object]:
        for sel in CONTAINER_SPECS.selectors:
            nodes = scope_node.css(sel) or []
            if nodes:
                return list(nodes)
        return []

    def _build_unique_key(self, card_node, unique_specs: list[FieldSpec]) -> tuple[str, ...] | None:
        if not unique_specs:
            return None
        parts: list[str] = []
        for spec in unique_specs:
            v = self._extract_in_container(card_node, spec)
            parts.append(clean_text(v) if v else "")
        if all(p == "" for p in parts):
            return None
        return tuple(parts)

    def _extract_in_container(self, container_node, spec: FieldSpec) -> str:
        for var in spec.selectors:
            found = container_node.css_first(var.selector)
            if not found:
                continue
            if var.extract == ExtractType.TEXT:
                val = clean_text(found.text() or "")
            else:
                attr = var.attr or ""
                val = clean_text(found.attributes.get(attr, ""))
            if val:
                return val
        return ""

    def _extract_product_url(self, card_node, *, base_url: Optional[str]) -> str:
        """
        (NEW) Пытается извлечь URL карточки товара из DOM карточки листинга.

        Логика:
        - Сначала ищем типовой якорь названия товара (используется в FIELD_SPECS для 'Товар').
        - Затем fallback: первая ссылка с href внутри карточки.
        - Если ссылка относительная — резолвим через base_url.

        Важно:
        - Мы не делаем предположений о структуре сайта сверх уже используемых селекторов.
        """
        a = card_node.css_first("a.dark_link.js-notice-block__title")
        if a is None:
            a = card_node.css_first("a[href]")

        if a is None:
            return ""

        href = clean_text(a.attributes.get("href", ""))
        if not href:
            return ""

        if base_url:
            return resolve_url(base_url, href)
        return href

    def _find_spec_index_by_name(self, name: str) -> Optional[int]:
        for i, s in enumerate(self._specs):
            if s.name == name:
                return i
        return None

    def _to_product_record(self, values_by_name: dict[str, Any]) -> ProductRecord:
        """
        Преобразование внутреннего словаря в ProductRecord.
        Оставлено максимально близким к текущему поведению.
        """
        rec_kwargs = {
            self._specs[0].name: str(values_by_name.get(self._specs[0].name, "NA")),
            self._specs[1].name: str(values_by_name.get(self._specs[1].name, "NA")) if len(self._specs) > 1 else "NA",
            self._specs[2].name: str(values_by_name.get(self._specs[2].name, "NA")) if len(self._specs) > 2 else "NA",
            self._specs[3].name: values_by_name.get(self._specs[3].name, "NA") if len(self._specs) > 3 else "NA",
            self._specs[4].name: values_by_name.get(self._specs[4].name, "NA") if len(self._specs) > 4 else "NA",
        }
        return ProductRecord(**rec_kwargs)


__all__ = ["ExtractorConfig", "ProductExtractor"]
```

### mapping_normalizer.py
<a id="parsing-mapping_normalizer.py"></a>

```python
"""
MappingFieldNormalizer — нормализация значений для записей в формате dict.

Зачем:
- В shallow-режиме остаётся существующий PriceNormalizer, который работает с ProductRecord.
- В extended-режиме после агрегации получается dict (динамические колонки),
  поэтому нужен нормализатор, который работает с mapping-структурами.

Нормализация выполняется по FIELD_SPECS.normalize:
- default_clean
- price_to_float
- mark_supplier

Ограничение:
- Нормализатор применяется только к ключам, которые совпадают с FieldSpec.name.
  Новые характеристики ("Характеристика - ...") не трогаем.
"""

from __future__ import annotations

import re
import warnings
from dataclasses import dataclass
from typing import Any, Optional

from app.core.models_and_specs import FIELD_SPECS, NormalizeRules, NA


_RE_NBSP = re.compile(r"(?:&nbsp;|\u00A0|\u202F)")
_RE_SPACES = re.compile(r"\s+")
_RE_CURRENCY = re.compile(
    r"(?:руб(?:\.|ля|лей)?|р\.?|₽|BYN|KZT|USD|EUR|\$|€)",
    flags=re.IGNORECASE,
)
_RE_NOT_NUM_DOT = re.compile(r"[^0-9.]+")  # для цены после замены ',' -> '.'


@dataclass(slots=True, frozen=True)
class MappingNormalizerConfig:
    """
    Конфигурация нормализатора словарей.
    """
    strict_unknown_tools: bool = False  # если True — неизвестные tools считать ошибкой (через warning не ограничимся)


class MappingFieldNormalizer:
    """
    Нормализует список записей dict[str, Any] согласно FIELD_SPECS.normalize.
    """

    T_DEFAULT_CLEAN = "default_clean"
    T_PRICE_TO_FLOAT = "price_to_float"
    T_MARK_SUPPLIER = "mark_supplier"

    def __init__(self, *, config: Optional[MappingNormalizerConfig] = None) -> None:
        self._cfg = config or MappingNormalizerConfig()
        self._rules_by_field: dict[str, list[NormalizeRules]] = {}

        for spec in FIELD_SPECS:
            if spec.normalize:
                self._rules_by_field[spec.name] = list(spec.normalize)

    def normalize(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Нормализует список записей (каждая запись — dict).

        Возвращает новые dict только для тех записей, где реально были изменения.
        """
        out: list[dict[str, Any]] = []
        for rec in records:
            out.append(self.normalize_one(rec))
        return out

    def normalize_one(self, rec: dict[str, Any]) -> dict[str, Any]:
        """
        Нормализует одну запись. При необходимости возвращает копию с изменениями.
        """
        if not self._rules_by_field:
            return rec

        updated: dict[str, Any] = {}
        changed = False

        for field_name, rules in self._rules_by_field.items():
            current = rec.get(field_name)
            if current is None or current == NA:
                continue

            new_val: Any = current
            for rule in rules:
                tools = rule.tools or []
                for tool in tools:
                    if tool == self.T_DEFAULT_CLEAN:
                        new_val = self._t_default_clean(new_val)
                    elif tool == self.T_PRICE_TO_FLOAT:
                        new_val = self._t_price_to_float(new_val)
                    elif tool == self.T_MARK_SUPPLIER:
                        if rule.supplier_id is not None:
                            new_val = self._t_mark_supplier(new_val, rule.supplier_id)
                    else:
                        msg = (
                            f"[MappingFieldNormalizer] Unknown normalize tool '{tool}' "
                            f"for field '{field_name}', rule={rule}"
                        )
                        warnings.warn(msg, RuntimeWarning, stacklevel=2)
                        if self._cfg.strict_unknown_tools:
                            # Под "strict" подразумеваем, что вы хотите быстро увидеть проблему.
                            raise ValueError(msg)

            if new_val is not current:
                updated[field_name] = new_val
                changed = True

        if not changed:
            return rec

        # копия + патч
        new_rec = dict(rec)
        new_rec.update(updated)
        return new_rec

    # ------------------ инструменты ------------------

    @staticmethod
    def _t_default_clean(value: Any) -> Any:
        if isinstance(value, (int, float)):
            return value
        if value is None:
            return value

        text = str(value)
        text = _RE_NBSP.sub(" ", text)
        text = _RE_CURRENCY.sub("", text)
        text = _RE_SPACES.sub(" ", text).strip()
        return text

    @staticmethod
    def _t_price_to_float(value: Any) -> Any:
        if value is None or value == NA:
            return value
        if isinstance(value, float):
            return value

        s = str(value).replace(",", ".")
        s = _RE_NOT_NUM_DOT.sub("", s)

        if not s:
            return value

        if s.count(".") > 1:
            first_dot = s.find(".")
            s = s[: first_dot + 1] + s[first_dot + 1 :].replace(".", "")

        try:
            return float(s)
        except ValueError:
            return value

    @staticmethod
    def _t_mark_supplier(value: Any, supplier_id: int) -> Any:
        if value is None or value == NA:
            return value
        s = str(value).strip()
        if not s:
            return value
        prefix = f"{supplier_id}-"
        if s.startswith(prefix):
            return s
        return prefix + s


__all__ = ["MappingNormalizerConfig", "MappingFieldNormalizer"]
```

### normalizer.py
<a id="parsing-normalizer.py"></a>

```python
# parsing/normalizer.py
"""
PriceNormalizer — модуль нормализации значений полей товара на основе
конфигурации в FIELD_SPECS (NormalizeRules). Модуль независим:
принимает готовые записи товаров и возвращает те же записи с
нормализованными значениями.

Ключевые правила (tools):
- default_clean: удаляет \xa0, узкие неразрывные пробелы, &nbsp; и пробелы-разделители;
                 отбрасывает валютные обозначения (руб/₽/BYN/USD/EUR и т.п.)
- price_to_float: приводит строку цены к float (заменяет ',' на '.', удаляет лишние символы)
- mark_supplier: для артикула добавляет префикс "<supplier_id>-" (если задан в NormalizeRules)

Особенности:
- Значение "NA" пропускается без изменений.
- Обработка идёт по FIELD_SPECS; поля без правил нормализации не трогаются.
- Для снижения аллокаций собираем изменения полей в словарь и применяем разом.
"""

from __future__ import annotations

import warnings
from dataclasses import replace
from typing import Any, Callable, Iterable

import re

from app.core.models_and_specs import (
    FIELD_SPECS,
    FieldSpec,
    NormalizeRules,
    ProductRecord,
    NA,
)

# --- Предкомпилированные паттерны для производительности ---

# HTML-сущность &nbsp;, неразрывные пробелы и узкие неразрывные пробелы
_RE_NBSP = re.compile(r"(?:&nbsp;|\u00A0|\u202F)")
# Любые пробелы/табуляции, встречающиеся как разделители тысяч
_RE_SPACES = re.compile(r"\s+")
# Валютные обозначения (минимальный полезный набор, без избыточности)
_RE_CURRENCY = re.compile(
    r"(?:руб(?:\.|ля|лей)?|р\.?|₽|BYN|KZT|USD|EUR|\$|€)",
    flags=re.IGNORECASE,
)

# Для price_to_float: оставить только цифры и точки, после замены ',' на '.'
_RE_NOT_NUM_DOT = re.compile(r"[^0-9.]+")


class PriceNormalizer:
    """
    Оркестратор нормализации значений полей товаров.

    Использование:
        normalizer = PriceNormalizer()
        records_out = normalizer.normalize(records_in)

    Архитектура:
    - На инициализации подготавливает карту действий по полям из FIELD_SPECS.
    - При normalize(...) проходит по товарам, применяя инструменты к нужным полям.
    - Изменения применяются единоразово на товар (одна аллокация/мутация).
    """

    # Имена инструментов (минимизируем магические строки)
    T_DEFAULT_CLEAN = "default_clean"
    T_PRICE_TO_FLOAT = "price_to_float"
    T_MARK_SUPPLIER = "mark_supplier"

    def __init__(self) -> None:
        # Карта: field_name -> список callable-обработчиков для применения по порядку
        # Каждый обработчик — функция вида f(value, rule) -> new_value
        self._actions_by_field: dict[str, list[Callable[[Any, NormalizeRules], Any]]] = {}
        self._rules_by_field: dict[str, list[NormalizeRules]] = {}

        # Построение правил из FIELD_SPECS (только поля, где есть normalize)
        for spec in FIELD_SPECS:
            if spec.normalize:
                self._rules_by_field[spec.name] = list(spec.normalize)
                self._actions_by_field[spec.name] = self._compile_actions(spec)

    # ------------------ Публичный API ------------------

    def normalize(self, records: list[ProductRecord]) -> list[ProductRecord]:
        """
        Нормализует значения в переданных записях товаров согласно FIELD_SPECS.

        Args:
            records: список ProductRecord после парсинга.

        Returns:
            Новый список ProductRecord (по одному объекту на запись).
            Поля без правил нормализации остаются без изменений.
        """
        out: list[ProductRecord] = []
        for rec in records:
            # Если ни одно поле не требует нормализации — возвращаем как есть (копию не создаём)
            if not self._actions_by_field:
                out.append(rec)
                continue

            updates: dict[str, Any] = {}

            # Проходим только по полям, для которых есть правила
            for field_name, actions in self._actions_by_field.items():
                # Текущее значение
                current = getattr(rec, field_name, None)

                # Пропускаем отсутствующие/NA
                if current is None or current == NA:
                    continue

                new_val = current
                # Применяем правила последовательно (в порядке NormalizeRules и tools)
                for idx, rule in enumerate(self._rules_by_field[field_name]):
                    tools = rule.tools or []
                    if not tools:
                        continue
                    for tool in tools:
                        # Вызов соответствующего инструмента
                        if tool is self.T_DEFAULT_CLEAN or tool == self.T_DEFAULT_CLEAN:
                            new_val = self._t_default_clean(new_val)
                        elif tool is self.T_PRICE_TO_FLOAT or tool == self.T_PRICE_TO_FLOAT:
                            new_val = self._t_price_to_float(new_val)
                        elif tool is self.T_MARK_SUPPLIER or tool == self.T_MARK_SUPPLIER:
                            # supplier_id опционален; без него действие игнорируем
                            if rule.supplier_id is not None:
                                new_val = self._t_mark_supplier(new_val, rule.supplier_id)
                        else:
                            # Неизвестный инструмент — выбрасываем предупреждение (но не прерываем конвейер)
                            warnings.warn(
                                f"[PriceNormalizer] Неизвестный инструмент нормализации '{tool}' "
                                f"для поля '{field_name}'. Правило: {rule}",
                                RuntimeWarning,
                                stacklevel=2,
                            )
                            continue

                # Фиксируем обновление, только если оно реально изменило значение
                if new_val is not current:
                    updates[field_name] = new_val

            if updates:
                # Создаём новый объект с заменой изменённых полей (одна аллокация на запись)
                # dataclasses.replace быстрее и чище, чем ручные setattr по одному
                rec = replace(rec, **updates)

            out.append(rec)

        return out

    # ------------------ Компиляция действий ------------------

    def _compile_actions(self, spec: FieldSpec) -> list[Callable[[Any, NormalizeRules], Any]]:
        """
        Формирует последовательность действий по spec.normalize.
        Каждый элемент spec.normalize может включать несколько tools, которые
        выполняются по порядку. Возвращаем плоский список вызовов (для горячего пути).
        """
        actions: list[Callable[[Any, NormalizeRules], Any]] = []

        # Мы не замыкаем rule в функции — применяем rule на этапе normalize(...) для гибкости
        # и чтобы избежать множества мелких объектов.
        for _ in spec.normalize:
            # Непосредственные функции берём из словаря tool->callable на этапе normalize(...)
            # Здесь оставляем заглушку, чтобы сохранить структуру и упорядоченность.
            # Фактическая маршрутизация производится в normalize(...).
            # (См. комментарии внутри normalize)
            pass

        # Возвращаем пустой список — список фактических callable не нужен,
        # т.к. мы вызываем инструменты напрямую в normalize(...) (минимум аллокаций).
        return actions

    # ------------------ Инструменты нормализации ------------------

    @staticmethod
    def _t_default_clean(value: Any) -> Any:
        """
        Базовая очистка текстов: удаление неразрывных пробелов, &nbsp;,
        схлопывание пробелов, отбрасывание валюты.
        """
        if isinstance(value, (int, float)):
            return value
        if value is None:
            return value

        text = str(value)

        # Удаляем &nbsp; и неразрывные пробелы
        text = _RE_NBSP.sub(" ", text)

        # Удаляем валютные обозначения
        text = _RE_CURRENCY.sub("", text)

        # Схлопываем пробелы до одного, вокруг — обрезаем
        text = _RE_SPACES.sub(" ", text).strip()

        return text

    @staticmethod
    def _t_price_to_float(value: Any) -> Any:
        """
        Конвертация цены в float. Устойчива к наличию валютных символов и разделителей.
        - Заменяет запятую на точку.
        - Удаляет любые символы, кроме цифр и точки.
        - Если точек несколько — сохраняет первую слева (остальные убирает).
        """
        if value is None or value == NA:
            return value
        if isinstance(value, float):
            return value
        # Строковое представление
        s = str(value)

        # Унификация десятичного разделителя
        s = s.replace(",", ".")

        # Удаляем всё, что не цифры и не точки
        s = _RE_NOT_NUM_DOT.sub("", s)

        if not s:
            return value  # не трогаем, если ничего не осталось

        # Если несколько точек — оставляем первую
        if s.count(".") > 1:
            first_dot = s.find(".")
            s = s[: first_dot + 1] + s[first_dot + 1 :].replace(".", "")

        try:
            return float(s)
        except ValueError:
            # Оставляем исходное значение, если не удалось распарсить
            return value

    @staticmethod
    def _t_mark_supplier(value: Any, supplier_id: int) -> Any:
        """
        Добавляет префикс '<supplier_id>-' к артикулу. Не дублирует префикс.
        """
        if value is None or value == NA:
            return value
        s = str(value).strip()
        if not s:
            return value
        prefix = f"{supplier_id}-"
        if s.startswith(prefix):
            return s
        return prefix + s


__all__ = ["PriceNormalizer"]
```

## pipeline/
<a id="pipeline"></a>

### runner.py
<a id="pipeline-runner.py"></a>

```python
"""
Pipeline: ParserPipeline — оркестрация логина, загрузки страниц, парсинга и экспорта.

Нововведение:
- extended-режим (листинг -> карточки товаров).

Инварианты:
- shallow-режим сохраняет поведение (листинг -> ProductExtractor -> PriceNormalizer -> export).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Iterable, Optional, Iterator, Any

from app.core.errors import (
    LoginFailedError,
    HttpStatusError,
    TimeoutError_,
    NetworkError,
    StopRequestedError,
    ErrorCode,
)
from app.core.parsing_mode import ParsingMode
from app.app_logging.logbus import LogBus
from app.ui.state import UIState, UIStatus
from app.export_io.writer import XlsxWriterService
from app.net.session_and_fetcher import SessionManager, PageFetcher, FetchedPage
from app.parsing.extractor import ProductExtractor, ExtractorConfig
from app.parsing.normalizer import PriceNormalizer
from app.parsing.card_extractor import ProductCardExtractor
from app.parsing.aggregator import ProductRecordAggregator
from app.parsing.mapping_normalizer import MappingFieldNormalizer


@dataclass(slots=True)
class PipelineConfig:
    """
    Настройки пайплайна.

    Новое:
        parsing_mode: shallow/extended
        cards_batch_size: ограничение на размер партии карточек (чтобы не плодить слишком много задач разом).
    """
    batch_size: int = 10
    concurrency: int = 24
    fetch_timeout_s: float = 25.0

    parsing_mode: ParsingMode = ParsingMode.SHALLOW
    cards_batch_size: int = 60


class ParserPipeline:
    """
    Высокоуровневый конвейер парсинга.

    SHALLOW:
      login -> fetch listing -> parse listing -> normalize -> export

    EXTENDED:
      login -> fetch listing -> parse listing(partials+urls) -> fetch cards -> parse cards ->
      aggregate -> normalize(dict) -> export
    """

    def __init__(
        self,
        *,
        session: SessionManager,
        auth_adapter,
        log_bus: LogBus,
        ui_state: UIState,
        writer: Optional[XlsxWriterService] = None,
        fetcher: Optional[PageFetcher] = None,
        extractor: Optional[ProductExtractor] = None,
        normalizer: Optional[PriceNormalizer] = None,
        config: Optional[PipelineConfig] = None,
    ) -> None:
        self._session = session
        self._auth_adapter = auth_adapter
        self._log = log_bus
        self._ui = ui_state
        self._writer = writer or XlsxWriterService()

        self._cfg = config or PipelineConfig()

        self._fetcher = fetcher or PageFetcher(session=self._session, concurrency=self._cfg.concurrency)

        # Экстрактор листинга:
        # - в extended включаем сбор product_url
        if extractor is not None:
            self._extractor = extractor
        else:
            if self._cfg.parsing_mode == ParsingMode.EXTENDED:
                self._extractor = ProductExtractor(config=ExtractorConfig(collect_product_url=True))
            else:
                self._extractor = ProductExtractor()

        # Нормализатор shallow (dataclass)
        self._normalizer = normalizer or PriceNormalizer()

        # Новые компоненты extended
        self._card_extractor = ProductCardExtractor(log_bus=self._log)
        self._aggregator = ProductRecordAggregator()
        self._mapping_normalizer = MappingFieldNormalizer()

        # groups: list[{"page_title": str, "data": list[dict|dataclass]}]
        self._groups: list[dict] = []

    async def run(self, urls: Iterable[str]) -> None:
        unique_urls = self._dedupe_keep_order(urls)
        total = len(unique_urls)
        self._ui.begin_task(total=total, task_name="parse")

        if total == 0:
            self._log.info("STAGE_START", "No URLs to process; creating empty XLSX")
            xlsx = self._safe_export_partial()
            self._ui.end_task(success=True, xlsx_path=xlsx)
            self._log.info("EXPORT_DONE", f"Exported empty workbook: {xlsx}")
            return

        # 1) Логин
        try:
            await self._ensure_not_stopped(stage="login")
            self._log.info("LOGIN", "Starting authentication")
            await self._auth_adapter.login(self._session)
            self._log.info("LOGIN_OK", "Authentication successful")
        except LoginFailedError as e:
            self._log.error(ErrorCode.ERR_LOGIN_FAILED, f"Login failed: {e}")
            self._ui.add_error(ErrorCode.ERR_LOGIN_FAILED, critical=True)
            self._ui.end_task(success=False, xlsx_path=None)
            return
        except Exception as e:
            self._log.error(ErrorCode.ERR_UNEXPECTED, f"Unexpected error on login: {e!r}")
            self._ui.add_error(ErrorCode.ERR_UNEXPECTED, critical=True)
            self._ui.end_task(success=False, xlsx_path=None)
            return

        # 2) Режимы
        if self._cfg.parsing_mode == ParsingMode.EXTENDED:
            await self._run_extended(unique_urls)
        else:
            await self._run_shallow(unique_urls)

        # 3) Экспорт (если не stop)
        if await self._is_stop_and_handle_before_export():
            return

        xlsx_path = self._safe_export_partial()
        self._ui.end_task(success=True, xlsx_path=xlsx_path)
        self._log.info("EXPORT_DONE", f"Exported XLSX: {xlsx_path}")

    # ------------------------------------------------------------------
    # SHALLOW
    # ------------------------------------------------------------------

    async def _run_shallow(self, unique_urls: list[str]) -> None:
        for batch_idx, batch in enumerate(self._batched(unique_urls, self._cfg.batch_size), start=1):
            if await self._is_stop_and_handle_before_export():
                return

            ok_pages = 0
            http_err = 0
            timeout_err = 0
            net_err = 0
            unexpected_err = 0
            parsed_products = 0
            parse_issues = 0

            self._log.info(
                "BATCH_START",
                f"Batch {batch_idx}: size={len(batch)}",
                context={"batch": batch_idx, "size": len(batch)},
            )

            tasks: list[asyncio.Task[FetchedPage]] = [
                asyncio.create_task(self._fetch_one_with_timeout(u, add_showall_params=True))
                for u in batch
            ]

            for t in asyncio.as_completed(tasks):
                try:
                    page: FetchedPage = await t
                except asyncio.CancelledError:
                    timeout_err += 1
                    self._ui.inc_done(1)
                    continue

                if await self._is_stop_and_cancel_pending(tasks):
                    break

                if page.text is None:
                    if isinstance(page.error, HttpStatusError) or (page.status is not None and page.status != 200):
                        http_err += 1
                    elif isinstance(page.error, (TimeoutError_, asyncio.TimeoutError)):
                        timeout_err += 1
                    elif isinstance(page.error, NetworkError):
                        net_err += 1
                    else:
                        unexpected_err += 1
                    self._ui.inc_done(1)
                    continue

                products, issues, page_title = self._extractor.extract(page.text, task_id=batch_idx)
                if issues:
                    parse_issues += len(issues)

                products = self._normalizer.normalize(products)

                self._groups.append({
                    "page_title": page_title or page.url,
                    "data": products,
                })

                parsed_products += len(products)
                ok_pages += 1
                self._ui.inc_done(1)

            self._log.info(
                "BATCH_SUMMARY",
                (
                    f"Batch {batch_idx} done: ok_pages={ok_pages}, "
                    f"http_err={http_err}, timeout_err={timeout_err}, net_err={net_err}, "
                    f"unexpected_err={unexpected_err}, products={parsed_products}, issues={parse_issues}"
                ),
                context={
                    "batch": batch_idx,
                    "ok_pages": ok_pages,
                    "http_err": http_err,
                    "timeout_err": timeout_err,
                    "net_err": net_err,
                    "unexpected_err": unexpected_err,
                    "products": parsed_products,
                    "issues": parse_issues,
                },
            )

    # ------------------------------------------------------------------
    # EXTENDED
    # ------------------------------------------------------------------

    async def _run_extended(self, unique_urls: list[str]) -> None:
        """
        Основной цикл extended-режима.

        Прогресс:
        - каждая страница листинга = 1 единица работы
        - каждая уникальная карточка товара, которую реально грузим = +1 единица работы
        """
        for batch_idx, batch in enumerate(self._batched(unique_urls, self._cfg.batch_size), start=1):
            if await self._is_stop_and_handle_before_export():
                return

            ok_pages = 0
            http_err = 0
            timeout_err = 0
            net_err = 0
            unexpected_err = 0

            products_total = 0
            listing_issues = 0
            card_issues = 0
            merge_issues = 0

            self._log.info(
                "BATCH_START",
                f"[EXTENDED] Batch {batch_idx}: size={len(batch)}",
                context={"batch": batch_idx, "size": len(batch)},
            )

            tasks: list[asyncio.Task[FetchedPage]] = [
                asyncio.create_task(self._fetch_one_with_timeout(u, add_showall_params=True))
                for u in batch
            ]

            for t in asyncio.as_completed(tasks):
                try:
                    listing_page: FetchedPage = await t
                except asyncio.CancelledError:
                    timeout_err += 1
                    self._ui.inc_done(1)
                    continue

                if await self._is_stop_and_cancel_pending(tasks):
                    break

                if listing_page.text is None:
                    if isinstance(listing_page.error, HttpStatusError) or (
                        listing_page.status is not None and listing_page.status != 200
                    ):
                        http_err += 1
                    elif isinstance(listing_page.error, (TimeoutError_, asyncio.TimeoutError)):
                        timeout_err += 1
                    elif isinstance(listing_page.error, NetworkError):
                        net_err += 1
                    else:
                        unexpected_err += 1
                    # листинг URL считаем обработанным
                    self._ui.inc_done(1)
                    continue

                # 1) Парсинг листинга -> PartialProduct[]
                partials, issues, page_title = self._extractor.extract_partials(
                    listing_page.text,
                    task_id=batch_idx,
                    base_url=listing_page.url,
                )
                if issues:
                    listing_issues += len(issues)

                # Листинг считаем обработанным (единица прогресса)
                self._ui.inc_done(1)

                # 2) Сбор ссылок карточек (дедуп по URL, порядок сохраняем)
                card_urls = self._dedupe_keep_order([p.product_url for p in partials if p.product_url])

                # Увеличиваем total прогресса на кол-во реально загружаемых карточек
                self._ui.add_total(len(card_urls))

                # 3) Fetch карточек (без SHOWALL_*)
                card_data_by_url: dict[str, Optional[dict[str, Any]]] = {}
                card_data_obj_by_url = {}

                # Загружаем карточки батчами, чтобы не создавать тысячи задач разом
                for cards_sub in self._batched(card_urls, self._cfg.cards_batch_size):
                    card_tasks: list[asyncio.Task[FetchedPage]] = [
                        asyncio.create_task(self._fetch_one_with_timeout(u, add_showall_params=False))
                        for u in cards_sub
                    ]

                    for ct in asyncio.as_completed(card_tasks):
                        try:
                            card_page: FetchedPage = await ct
                        except asyncio.CancelledError:
                            timeout_err += 1
                            self._ui.inc_done(1)
                            continue

                        if await self._is_stop_and_cancel_pending(card_tasks):
                            break

                        if card_page.text is None:
                            # Карточка не загрузилась — фиксируем отсутствие данных
                            card_data_by_url[card_page.url] = None
                            # прогресс по карточке считаем выполненным
                            self._ui.inc_done(1)
                            continue

                        # 4) Парсинг карточки
                        card_data = self._card_extractor.extract(
                            card_page.text,
                            task_id=batch_idx,
                            product_url=card_page.url,
                        )
                        if card_data.issues:
                            card_issues += len(card_data.issues)

                        card_data_obj_by_url[card_page.url] = card_data
                        card_data_by_url[card_page.url] = card_data.values

                        # прогресс по карточке
                        self._ui.inc_done(1)

                    # если stop — прекращаем загрузку оставшихся карточек
                    if self._ui.stop_requested:
                        break

                # 5) Агрегация результатов в плоские dict-записи
                records: list[dict[str, Any]] = []
                for p in partials:
                    card_obj = card_data_obj_by_url.get(p.product_url) if p.product_url else None
                    rec, agg_issues = self._aggregator.aggregate(p, card_obj)
                    if agg_issues:
                        merge_issues += len(agg_issues)
                    records.append(rec)

                # 6) Нормализация dict-записей
                records = self._mapping_normalizer.normalize(records)

                # 7) Накопление для экспорта
                self._groups.append({
                    "page_title": page_title or listing_page.url,
                    "data": records,
                })

                products_total += len(records)
                ok_pages += 1

            self._log.info(
                "BATCH_SUMMARY",
                (
                    f"[EXTENDED] Batch {batch_idx} done: ok_pages={ok_pages}, "
                    f"http_err={http_err}, timeout_err={timeout_err}, net_err={net_err}, "
                    f"unexpected_err={unexpected_err}, products={products_total}, "
                    f"listing_issues={listing_issues}, card_issues={card_issues}, merge_issues={merge_issues}"
                ),
                context={
                    "batch": batch_idx,
                    "ok_pages": ok_pages,
                    "http_err": http_err,
                    "timeout_err": timeout_err,
                    "net_err": net_err,
                    "unexpected_err": unexpected_err,
                    "products": products_total,
                    "listing_issues": listing_issues,
                    "card_issues": card_issues,
                    "merge_issues": merge_issues,
                },
            )

    # ------------------------------------------------------------------
    # Внутренние хелперы
    # ------------------------------------------------------------------

    @staticmethod
    def _batched(iterable: Iterable, batch_size: int) -> Iterator[list]:
        batch: list = []
        for item in iterable:
            batch.append(item)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    async def _fetch_one_with_timeout(self, url: str, *, add_showall_params: bool) -> FetchedPage:
        """
        Загружает один URL с индивидуальным таймаутом.
        Важно для extended-режима, где карточек может быть много.
        """
        try:
            pages = await asyncio.wait_for(
                self._fetcher.fetch_many([url], add_showall_params=add_showall_params),
                timeout=self._cfg.fetch_timeout_s,
            )
            if pages:
                return pages[0]
            return FetchedPage(url=url, status=None, text=None, error=TimeoutError_("empty result"))
        except asyncio.TimeoutError as e:
            return FetchedPage(url=url, status=None, text=None, error=e)
        except Exception as e:
            return FetchedPage(url=url, status=None, text=None, error=e)

    async def _is_stop_and_cancel_pending(self, tasks: list[asyncio.Task]) -> bool:
        if not self._ui.stop_requested:
            return False
        for t in tasks:
            if not t.done():
                t.cancel()
        self._log.warn("STOP_REQUESTED", "Stop requested: cancelling pending tasks")
        return True

    async def _is_stop_and_handle_before_export(self) -> bool:
        if not self._ui.stop_requested:
            return False
        self._log.warn("STOP_REQUESTED", "Stop requested: performing partial export")
        self._ui.set_status(UIStatus.STOPPED)

        xlsx_path = self._safe_export_partial()
        self._ui.end_task(success=True, xlsx_path=xlsx_path)
        self._log.info("EXPORT_PARTIAL_DONE", f"Exported partial XLSX: {xlsx_path}")
        return True

    def _safe_export_partial(self) -> str:
        groups = self._groups
        if not groups:
            groups = [{"page_title": "data", "data": []}]
        try:
            return self._writer.write(groups)
        except Exception as e:
            self._log.error("ERR_EXPORT", f"Export failed: {e!r}")
            return ""

    @staticmethod
    def _dedupe_keep_order(urls: Iterable) -> list:
        seen: set = set()
        out: list = []
        for u in urls:
            if not u:
                continue
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out

    async def _ensure_not_stopped(self, *, stage: str) -> None:
        if self._ui.stop_requested:
            raise StopRequestedError(f"Stop requested before stage: {stage}")


__all__ = ["ParserPipeline", "PipelineConfig"]
```

## ui/
<a id="ui"></a>

### interface.py
<a id="ui-interface.py"></a>

```python
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

    session = SessionManager()
    auth = FormAuthAdapter(AuthConfig(email=AUTH_EMAIL, password=AUTH_PASSWORD))
    cfg = PipelineConfig(
        batch_size=BATCH_SIZE,
        concurrency=CONCURRENCY,
        fetch_timeout_s=FETCH_TIMEOUT_S,
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
```

### state.py
<a id="ui-state.py"></a>

```python
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
```
