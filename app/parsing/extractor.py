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
    exclude_field_names: tuple[str, ...] = ("Бренд",)


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
        self._cfg = config or ExtractorConfig()
        specs = field_specs or FIELD_SPECS
        if self._cfg.exclude_field_names:
            self._specs = [s for s in specs if s.name not in self._cfg.exclude_field_names]
        else:
            self._specs = list(specs)

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
