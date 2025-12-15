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
