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
