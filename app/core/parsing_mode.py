"""
Определение режима парсинга.

Назначение:
- Явно описать тип запуска пайплайна (shallow/extended),
  чтобы не разносить магические строки по проекту.

Примечание:
- Модуль умышленно отделён от pipeline/ui, чтобы не создавать циклических зависимостей.
"""

from __future__ import annotations

from enum import StrEnum


class ParsingMode(StrEnum):
    """
    Режим парсинга.

    SHALLOW  - текущий режим: парсинг только листинга.
    EXTENDED - расширенный режим: листинг -> ссылки -> карточки товаров.
    """
    SHALLOW = "shallow"
    EXTENDED = "extended"


__all__ = ["ParsingMode"]
