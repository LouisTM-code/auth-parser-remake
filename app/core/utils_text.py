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
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

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
