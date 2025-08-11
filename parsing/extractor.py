# core/extractor.py
"""
ProductExtractor: извлечение карточек товаров строго внутри контейнеров.

Цель обновления:
- Парсить только в пределах карточки, исключая подтягивание значений из соседних карточек.
- Двухэтапная логика:
  1) Поиск контейнеров карточек по CONTAINER_SPECS.selectors (по приоритету).
     Для каждого контейнера извлекаются поля с is_unique=True для построения ключа.
     Дубликатные карточки отбрасываются ДО полного разбора.
  2) Полный разбор ТОЛЬКО уникальных карточек: извлечение всех полей FIELD_SPECS
     локальными поисками внутри контейнера.

Правила:
- Несколько селекторов в FieldSpec поддерживаются — берётся первый, давший непустое значение.
- Отсутствие значения → "NA" + ParseIssue(code="ERR_PARSE_MISSING_FIELD"),
  КРОМЕ оптовой цены — она может отсутствовать и не считается ошибкой.
- Минимизируем глобальные CSS-поиски: используем container.css_first / container.css.

Зависимости:
- core.models_and_specs: FIELD_SPECS, CONTAINER_SPECS, FieldSpec, SelectorVariant,
  ExtractType, ProductRecord, ParseIssue
- core.utils_text: clean_text, normalize_price_to_float_or_na
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

# Выбираем самый быстрый доступный парсер Selectolax
try:
    from selectolax.lexbor import LexborHTMLParser as _HTMLParser  # type: ignore
except Exception:  # pragma: no cover
    from selectolax.parser import HTMLParser as _HTMLParser  # type: ignore

from core.models_and_specs import (
    FIELD_SPECS,
    CONTAINER_SPECS,
    ExtractType,
    FieldSpec,
    ProductRecord,
    ParseIssue,
)
from core.utils_text import clean_text, normalize_price_to_float_or_na


# ------------------------------ Конфиг ---------------------------------


@dataclass(slots=True, frozen=True)
class ExtractorConfig:
    """
    Настройки извлечения.
    """
    # Глобальный контейнер страницы (фиксировано по ТЗ)
    page_container_selector: str = "div.inner_wrapper"
    # Отсутствие оптовой цены не считать ошибкой
    treat_wholesale_missing_as_error: bool = False


# ----------------------------- Экстрактор -------------------------------


class ProductExtractor:
    """
    Извлекает ProductRecord со страницы листинга, работая строго в границах контейнеров карточек.

    Этапы:
      1) Поиск контейнеров карточек по CONTAINER_SPECS.selectors (по приоритету).
      2) Для каждого контейнера: построение ключа уникальности из полей FieldSpec с is_unique=True.
         Если ключ уже встречался — карточка пропускается.
      3) Для оставшихся контейнеров: извлекаются ВСЕ поля FIELD_SPECS локально внутри контейнера.
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

        # Индексы полей цен для нормализации
        self._retail_idx = self._find_spec_index_by_name("Розничная_цена")
        self._wholesale_idx = self._find_spec_index_by_name("Оптовая_цена")

        # Набор «уникальных» FieldSpec (по флагу is_unique).
        # Если флагов нет, логика дедупликации будет неактивна (все карточки считаются уникальными).
        self._unique_specs = [s for s in self._specs if getattr(s, "is_unique", False)]

    # --------------------- Публичный API ---------------------

    def extract(self, html: str, *, task_id: int) -> tuple[list[ProductRecord], list[ParseIssue], str]:

        """
        Полный разбор HTML-страницы с устранением дублей на уровне карточек.

        Returns:
            products: список уникальных ProductRecord.
            issues: список ParseIssue.
        """
        tree = _HTMLParser(html)

        h1_node = tree.css_first("h1")
        page_title = h1_node.text(strip=True) if h1_node else ""

        root = tree.root
        if root is None:
            return [], [ParseIssue(task_id=task_id, field_name="__page__", code="ERR_EMPTY_HTML", details="Empty DOM")], ""

        # 0) Ограничиваем область поиска глобальным контейнером страницы
        page_container = root.css_first(self._cfg.page_container_selector)
        if page_container is None:
            return [], [ParseIssue(task_id=task_id, field_name="__page__", code="ERR_CONTAINER_NOT_FOUND", details=self._cfg.page_container_selector)], ""

        # 1) Ищем контейнеры карточек по CONTAINER_SPECS (по приоритету селекторов)
        card_nodes = self._find_card_containers(page_container)
        if not card_nodes:
            # Нет карточек — возвращаемся с диагностикой, не «выдумываем» альтернатив
            return [], [ParseIssue(task_id=task_id, field_name="__page__", code="ERR_CONTAINER_NOT_FOUND", details=f"no card containers via {CONTAINER_SPECS.selectors!r}")], ""

        # 2) Дедуп на этапе карточек (по is_unique полям)
        seen_keys: set[tuple[str, ...]] = set()
        unique_cards: list[object] = []
        for card in card_nodes:
            key = self._build_unique_key(card, self._unique_specs)
            if key is None:
                # Ключ не построен — не режем данные, пропускаем дедуп
                unique_cards.append(card)
                continue
            if key in seen_keys:
                continue
            seen_keys.add(key)
            unique_cards.append(card)

        # 3) Полный парс только уникальных карточек
        products: list[ProductRecord] = []
        issues: list[ParseIssue] = []

        for card in unique_cards:
            values_by_name: dict[str, str | float] = {}

            for idx, spec in enumerate(self._specs):
                val = self._extract_in_container(card, spec)

                if not val:
                    # Оптовая цена может отсутствовать — не логируем проблему
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

            # Нормализуем цены в число (если спецификации присутствуют)
            if self._retail_idx is not None:
                rn = self._specs[self._retail_idx].name
                rv = values_by_name.get(rn, "NA")
                values_by_name[rn] = normalize_price_to_float_or_na(rv if isinstance(rv, str) else str(rv))
            if self._wholesale_idx is not None:
                wn = self._specs[self._wholesale_idx].name
                wv = values_by_name.get(wn, "NA")
                values_by_name[wn] = normalize_price_to_float_or_na(wv if isinstance(wv, str) else str(wv))

            # Формируем ProductRecord с именами полей ровно как в FIELD_SPECS
            rec_kwargs = {
                self._specs[0].name: str(values_by_name.get(self._specs[0].name, "NA")),
                self._specs[1].name: str(values_by_name.get(self._specs[1].name, "NA")) if len(self._specs) > 1 else "NA",
                self._specs[2].name: str(values_by_name.get(self._specs[2].name, "NA")) if len(self._specs) > 2 else "NA",
                self._specs[3].name: values_by_name.get(self._specs[3].name, "NA") if len(self._specs) > 3 else "NA",
                self._specs[4].name: values_by_name.get(self._specs[4].name, "NA") if len(self._specs) > 4 else "NA",
            }
            products.append(ProductRecord(**rec_kwargs))

        return products, issues, page_title

    # --------------------- Внутренние методы ---------------------

    def _find_card_containers(self, scope_node) -> list[object]:
        """
        Ищет контейнеры карточек по CONTAINER_SPECS.selectors в пределах scope_node.
        Приоритет — по порядку селекторов; выбирается первый, давший результаты.
        """
        for sel in CONTAINER_SPECS.selectors:
            nodes = scope_node.css(sel) or []
            if nodes:
                return list(nodes)
        return []

    def _build_unique_key(self, card_node, unique_specs: list[FieldSpec]) -> tuple[str, ...] | None:
        """
        Формирует кортеж уникальности по полям is_unique=True, извлекая значения
        ТОЛЬКО внутри card_node. Возвращает None, если нет ни одного непустого значения.
        """
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
        """
        Ищет значение поля внутри container_node:
          - проверяет варианты селекторов spec.selectors по порядку,
          - берёт первый непустой результат,
          - возвращает очищенную строку.
        """
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

    def _find_spec_index_by_name(self, name: str) -> Optional[int]:
        for i, s in enumerate(self._specs):
            if s.name == name:
                return i
        return None
