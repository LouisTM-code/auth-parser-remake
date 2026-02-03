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
        "Бренд",
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
