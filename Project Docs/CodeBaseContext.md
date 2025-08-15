# CODEBASE\_CONTEXT.md

## 1. Структура директорий проекта

> Формат: ASCII tree с комментариями назначения. Исключены временные, кеш и сборочные файлы. Директория app и файлы requirements.txt, main.py - лежат в общей корневой директории.

```Project
app/
  ├── app_logging/     # Модули логирования
  │   └── logbus.py    # Неблокирующая шина логов для UI
  ├── core/                      # Модули конфигураций и настроек
  │   ├── errors.py              # Коды и исключения верхнего уровня для конвейера парсинга
  │   ├── models_and_specs.py    # Модели (DTO) и спецификации полей парсинга
  │   └── utils_text.py          # Текстовые и URL-утилиты
  ├── export_io/       # Модули сохранения/экспорта данных
  │   └── writer.py    # Экспорт результатов парсинга в XLSX
  ├── net/                          # Модули и адаптеры сети
  │   ├── auth.py                   # Авторизация: базовый адаптер и форма-логин
  │   └── session_and_fetcher.py    # Сетевой слой: HTTP-сессия и конкурентная выборка страниц
  ├── parsing/             # Модули парсинг HTML и работы с данными
  │   ├── extractor.py     # `ProductExtractor` — извлечение товаров по селекторам.
  │   └── normalizer.py    # `DataNormalizer` — модуль нормализации данных основе `FIELD_SPECS (NormalizeRules)`
  ├── pipeline/        # Модули абстракции и управдения процессами
  │   └── runner.py    # `ParserPipeline` — оркестрация этапов логина, пакетной выборки, парсинга, нормализации и экспорта
  ├── ui/                 # Модули пользовательского интерфейса
  │   ├── app.py          # минималистичный интерфейс и управление пайплайном
  │   └── state.py        # `UIState` — централизованное состояние интерфейса Streamlit
  └── test/    # Тесты (unit, integration)
requirements.txt   # Python requirements
main.py            # Production entrypoint для Streamlit Cloud
```

## 2. Публичный API (YAML)

```yaml
# Формат: YAML
# Правила:
# - Ключ — модуль Python (package.module)
# - Подключи только публичные классы и функции
# - Для каждого метода/функции укажи сигнатуру и описание
# - Старайся избегать избыточных комментариев, чтобы сохранить краткость

app.auth.adapters:
  classes:
    FormAuthAdapter:
      methods:
        send_login_request:
          signature: "(self) -> Awaitable[Response]"
          description: "Отправка POST-формы для авторизации"
    NoAuthAdapter:
      methods:
        authenticate:
          signature: "(self) -> None"
          description: "Пустая авторизация"

app.parser.extractors:
  functions:
    parse_html:
      signature: "(content: str) -> dict"
      description: "Парсинг HTML и извлечение данных"
```

---

## 3. Ключевые структуры данных (Python с комментариями)

```python

# Формат: Python код с Pydantic-моделями или dataclasses

# Правила:

# - Docstring для каждой модели с назначением, местом использования и примером JSON

# - Комментарии у каждого поля с пояснением значения и ограничений

# - Указывать связи между моделями

from pydantic import BaseModel, HttpUrl
from typing import Optional, Literal, List

class ExtractRule(BaseModel):
    """
    Правило извлечения данных.
    Используется в ParserConfig для указания способа извлечения информации.
    Пример:
    {
        "field": "title",
        "method": "xpath",
        "pattern": "//h1/text()"
    }
    """
    field: str  # Имя поля в результирующем JSON
    method: Literal["xpath", "regex"]  # Метод извлечения
    pattern: str  # XPath-выражение или регулярка

class ParserConfig(BaseModel):
    """
    Конфигурация запуска парсера.
    Определяет URL, авторизацию и правила извлечения данных.
    Пример:
    {
        "url": "https://example.com",
        "extract_rules": [
            {"field": "title", "method": "xpath", "pattern": "//h1/text()"}
        ],
        "auth": null
    }
    """
    url: HttpUrl  # Целевой URL для парсинга
    extract_rules: List[ExtractRule]  # Список правил извлечения
    auth: Optional[dict]  # Конфигурация авторизации

```
