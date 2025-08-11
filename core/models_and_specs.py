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
class FieldSpec:
    """
    Спецификация поля для извлечения.
    Может содержать несколько альтернативных вариантов (SelectorVariant),
    которые будут проверяться по порядку до первого успеха.
    """
    name: str
    selectors: list[SelectorVariant]
    # Новое поле: определяет, участвует ли поле в построении уникального ключа товара.
    is_unique: bool = False

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

# Константа с селекторами контейнеров карточек.
# Значения задаются фактическими классами сайта; по вашему требованию не выдумываю —
# оставляю пустой список для fallback-логики в extractor.
CONTAINER_SPECS: ContainerSpecs = ContainerSpecs(selectors=[
    "tr.table-view__item",
    "div.list_item.item_info",
])

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
    Артикул: str | Literal["NA"]
    Наличие: str | Literal["NA"]
    Розничная_цена: str | float | Literal["NA"]
    Оптовая_цена: str | float | Literal["NA"]

    def to_ordered_values(self) -> list[str | float]:
        """
        Возвращает значения в фиксированном порядке колонок,
        соответствующем FIELD_SPECS (см. ниже).
        """
        return [
            self.Товар,
            self.Артикул,
            self.Наличие,
            self.Розничная_цена,
            self.Оптовая_цена,
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
        name="Артикул",
        selectors=[
            SelectorVariant(
                selector="span.codeProduct, span.code",
                extract=ExtractType.TEXT,
            ),
        ],
        is_unique=True,  # уникальный ключ строим по артикулу
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
    ),
    FieldSpec(
        name="Оптовая_цена",
        selectors=[
            SelectorVariant(
                selector="div.price_group.min span.price_value",
                extract=ExtractType.TEXT,
            ),
        ],
    ),
]

__all__ = [
    "NA",
    "ExtractType",
    "SelectorVariant",
    "FieldSpec",
    "ContainerSpecs",
    "CONTAINER_SPECS",
    "ProductRecord",
    "PageTask",
    "ParseIssue",
    "FIELD_SPECS",
]
