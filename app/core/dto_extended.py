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
