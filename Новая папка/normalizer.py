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

from core.models_and_specs import (
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
