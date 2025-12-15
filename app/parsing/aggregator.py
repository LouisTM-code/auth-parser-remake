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
