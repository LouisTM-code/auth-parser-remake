# Спецификация модуля `structured_error.py`

## Назначение

Модуль обеспечивает централизованное и расширяемое управление ошибками в системе. Основные цели:

- Единый формат ошибок (DTO + Exception).
- Унифицированный API для генерации ошибок (Facade).
- Расширяемая система нормализации аргументов (Strategy).

---

## Архитектура

### Паттерны

- **Facade** — `ErrorFacade` как единая точка входа для создания ошибок.
- **Strategy** — нормализаторы (`Normalizer`) для приведения любых входных данных к строке.
- **Open/Closed Principle** — фасад не изменяется при добавлении новых типов данных или ошибок.

---

## Компоненты

### DTO

```python
@dataclass(slots=True, frozen=True)
class StructuredError:
    code: str
    message: str
    pointer: str | None = None
    config_id: str | None = None
    location: str | None = None
```

- Чистый объект данных.
- Фиксированные поля.
- Иммутабельность (frozen=True).

### Исключение

```python
class DomainError(Exception):
    def __init__(self, structured: StructuredError):
        super().__init__(structured.message)
        self.structured = structured
```

- Единое исключение для выбрасывания наружу.
- Всегда содержит `StructuredError`.

### Strategy: Normalizer

```python
class Normalizer(Protocol):
    def normalize(self, value: Any) -> str: ...
```

Реализации:

- `StringNormalizer`
- `DictNormalizer`
- `ExceptionNormalizer`
- (расширяемо: JsonPointerNormalizer, SQLNormalizer и др.)

### NormalizerRegistry

```python
class NormalizerRegistry:
    def register(self, typ: type, normalizer: Normalizer) -> None: ...
    def normalize(self, value: Any) -> str: ...
```

- Хранит стратегии.
- При неизвестном типе возвращает `str(value)`.
- Гарантия, что всегда вернётся строка.

### Facade: ErrorFacade

```python
class ErrorFacade:
    def __init__(self, templates: dict[str, str], normalizers: NormalizerRegistry): ...

    def _make_error(self, code: str, **kwargs) -> DomainError: ...

    # Методы API
    def io_error(self, path: str, detail: Any) -> DomainError: ...
    def schema_error(self, config_id: str, pointer: Any, detail: Any) -> DomainError: ...
    def profile_error(self, config_id: str, profile: str, detail: Any) -> DomainError: ...
    def semantic_error(self, config_id: str, rule: str, pointer: Any, detail: Any) -> DomainError: ...
```

#### Обязанности фасада

- Инкапсулирует все детали формирования ошибок.
- Делегирует нормализацию аргументов в `NormalizerRegistry`.
- Строит `StructuredError` и оборачивает в `DomainError`.
- Обеспечивает fail-safe даже при неожиданных данных.

---

## Архитектурные свойства

1. **Несколько ошибок** — модуль stateless, поддерживает одновременное создание множества ошибок.
2. **Fail-safe** — любые аргументы нормализуются; если формат шаблона не подходит, используется fallback.
3. **Минимальные данные** — ошибка может быть создана даже только с `code` и `config_id`.
4. **Расширяемость** — новые типы нормализаторов и новые методы ошибок добавляются без изменения фасада.
5. **Единый контракт** — все ошибки возвращаются как `DomainError` с вложенным `StructuredError`.
6. **Изоляция** — модуль не имеет циклических зависимостей, доступен для любых подсистем.
7. **Consistency** — формат сообщений и структура DTO едины для всех ошибок.

---

## Пример использования

```python
# Инициализация
registry = NormalizerRegistry()
registry.register(Exception, ExceptionNormalizer())
registry.register(dict, DictNormalizer())
registry.register(str, StringNormalizer())

errors = ErrorFacade(templates={
    "RESULT_IO_ERROR": "I/O error at {location}: {detail}",
    "RESULT_SCHEMA_INVALID": "Schema invalid for {config_id} at {pointer}: {detail}",
    "RESULT_PROFILE_INVALID": "Invalid profile in {config_id}{pointer}: {detail}",
    "RESULT_SEMANTIC_RULE_FAIL": "Semantic rule violation in {config_id} at {pointer}: {detail}",
}, normalizers=registry)

# Применение в шаге пайплайна
raise errors.schema_error(config_id="auth_1", pointer="/credentials", detail={"type":"string"})
```

---

## Замечание о `templates`

* В текущей реализации `templates` передаются локально при инициализации фасада.
* Такой подход упрощает тестирование и изоляцию модулей: каждая подсистема может иметь свои шаблоны ошибок.
* Возможна централизация (единый реестр шаблонов) при переходе на много-модульную архитектуру.
* Шаблоны выполняют роль **ресурсного словаря**, который можно хранить в коде, конфиге или внешнем файле локализации.

---

## Вывод

Модуль `structured_error.py` реализует **Facade + Strategy**, обеспечивая:

- Чёткое разделение обязанностей.
- Унифицированный контракт для всех ошибок.
- Гибкость и расширяемость без изменения существующего кода.
- Fail-safe поведение даже при неожиданных данных.


