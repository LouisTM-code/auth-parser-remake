# ConfigError

**Задача класса.** Универсальное бизнес-исключение для всех этапов `ConfigLoader`; переносит код ошибки и диагностические детали (JSON Pointer, профиль). 

**Сигнатура и данные:**

* `__init__(self, code: str, message: str, *, json_pointer: str | None = None, profile_id: str | None = None)`
  Вход: машинный код (`IO_*`, `SCHEMA_*`, `SANITY_*`, `PROFILE_*`, `SEMANTIC_*`, …), текст, опционально `json_pointer` и `profile_id`.
  Выход: исключение. 

**Зона ответственности.** Только транспорт кода/контекста ошибки; бизнес-решений не принимает. 

**Техреализация.**

* Методы: `__init__`.
* Подходы: явные поля для трассировки.
* Допущения: коды соответствуют спецификации проекта.
* Ограничения: формат кода задаётся внешней спецификацией.

**Fail-fast.** Создаётся и выбрасывается на месте нарушения, прерывая выполнение. 

**Дополнительно.** Используется всеми нижестоящими проверками, в т.ч. семантическими. 

---

# StructuredError

**Задача класса.** Единый формат представления ошибки для UI/логов (код, сообщение, `config_id`, тип, `profile_id`, `json_pointer`). 

**Сигнатура и данные:**

* Dataclass поля: `code: str`, `message: str`, `config_id: str`, `type: str`, `profile_id: Optional[str] = None`, `json_pointer: Optional[str] = None`.
* `to_dict(self) -> dict[str, Any]`. 

**Зона ответственности.** Только форма представления ошибки; не генерирует и не интерпретирует коды. 

**Техреализация.**

* Методы: `to_dict`.
* Подходы: `@dataclass(frozen=True, slots=True)` для иммутабельности и компактности.
* Допущения/ограничения: поля соответствуют договорённой схеме логирования.

**Fail-fast.** Не применимо (контейнер данных).

**Дополнительно.** Наполняется через `_ErrorFormatter`. 

---

# _ErrorFormatter

**Задача класса.** Нормализация любых исключений (включая `jsonschema`/`pydantic`) в `StructuredError`; не вмешивается в бизнес-коды. 

**Сигнатуры (публичное API):**

* `format(exc: BaseException, *, config_id: str, type: str | Any, profile_id: str | None = None, code_hint: str | None = None) -> StructuredError`
  Вход: исключение и контекст (`config_id`, `type`, опционально `profile_id`, подсказка кода).
  Выход: `StructuredError`. 

**Зона ответственности.** Извлечь сообщение/указатель (`json_pointer`/`path`) из известных типов исключений и оформить в едином виде. 

**Техреализация.**

* Методы: `format`, приватные `_extract_jsonschema_error`, `_extract_pydantic_error`, `_pointer_from_sequence`, `_type_to_str`. 

* Алгоритм:
  
  1. Вывод типа как строки (`Enum`→`.value`).
  2. Код: `exc.code` → `code_hint` → `IO_INVALID_DOC` (fallback).
  3. Для `JsonSchemaValidationError` вытягивает `path/context` → JSON Pointer и компактное сообщение.
  4. Для `PydanticValidationError` переводит `loc` → JSON Pointer и сообщение.
  5. Прокидывает `profile_id` из исключения при отсутствии явного. 

* Подходы: адаптер сообщений, RFC-6901 Pointer из последовательностей. 

* Допущения: `jsonschema/pydantic` могут отсутствовать (optional deps). 

**Fail-fast.** Не генерирует ошибки сам; обеспечивает детерминированность представления.

**Дополнительно.** Не «решает» бизнес-код — это обязанность места генерации ошибки. 

---

# _ManifestReader

**Задача класса.** Прочесть JSON-манифест, провалидировать по схеме, спроецировать в DTO и выполнить минимальные sanity-инварианты (обязательные типы, уникальные `config_id`). 

**Сигнатуры:**

* `__init__(self, manifest_path: str | Path, *, manifest_schema_path: str | Path)`
* `read(self) -> _ManifestDTO` — полный цикл: I/O → JSON Schema → DTO → sanity. 

**Зона ответственности.** Только манифест; ни выбор профилей, ни семантика конкретных конфигов. 

**Техреализация.**

* Методы: `_load_json`, `_load_manifest_schema`, `_validate_schema`, `_pointer_from_sequence`, `_to_dto`, `_check_required_types`, `_check_unique_config_ids`. 

* Алгоритм:
  
  1. Загрузка файла, точная диагностика `IO_*`.
  2. Загрузка схемы, валидация `SCHEMA_VALIDATION_FAILED` с RFC-6901 pointer.
  3. DTO-проекция к типам `ManifestMeta`, `ConfigEntry`.
  4. Sanity: обязательные типы и уникальность `config_id` (детерминированные сообщения). 

* Подходы: строгая DTO-проекция, явная схема.

* Допущения/ограничения: схема манифеста доступна по явному пути; список обязательных типов фиксирован. 

**Fail-fast.** `IO_NOT_FOUND`, `IO_INVALID_JSON`, `SCHEMA_VALIDATION_FAILED`, `SANITY_MISSING_TYPE`, `SANITY_DUPLICATE_CONFIG_ID`. 

**Дополнительно.** Указывает `json_pointer` даже для ошибок синтаксиса (формат `line:col`). 

---

# _ConfigReader

**Задача класса.** Разрешить путь профиля из манифеста, прочитать JSON, проверить базовую форму документа. 

**Сигнатуры:**

* `__init__(self, *, project_root: str | Path, max_file_size: int = 10*1024)`
* `read(self, entry: ConfigEntry) -> dict[str, Any]` — возвращает «сырой» dict профиля. 

**Зона ответственности.** Только I/O и базовая форма JSON (объект); никакой схемной/семантической логики. 

**Техреализация.**

* Методы: приватные I/O проверки — существование файла, лимит размера, чтение UTF-8-SIG, разбор JSON. 
* Алгоритм: `exists/is_file` → `stat.size` → `open/parse` → ошибки `IO_*` с деталями (включая `line:col`). 
* Подходы: fail-fast, понятные сообщения.
* Допущения/ограничения: путь может быть относительным к `project_root`; размер ограничен `max_file_size`. 

**Fail-fast.** `IO_NOT_FOUND`, `IO_TOO_LARGE`, `IO_INVALID_JSON`. 

**Дополнительно.** Не мутирует данные; отдаёт как есть для дальнейших этапов.

---

# _ConfigSanityCheck

**Задача класса.** Минимальные sanity-проверки профиля ДО JSON Schema: ключи верхнего уровня, согласованность с манифестом, формат и соответствие версии. 

**Сигнатура:**

* `run(self, *, entry: ConfigEntry, data: dict[str, Any]) -> None` — на успех `None`, на нарушение бросает `ConfigError`. 

**Зона ответственности.** Проверка `meta`/`profiles`, `meta.config_id` против манифеста, `meta.version` против `expected_version` с поддержкой масок `x`. 

**Техреализация.**

* Методы: `run`, приватный `_match_expected_version`.

* Алгоритм:
  
  1. `meta` и `profiles` найдены и верных типов.
  2. `meta.config_id` совпадает с `entry.config_id`.
  3. `meta.version` есть и удовлетворяет маске `expected_version` (`MAJOR.MINOR.PATCH`, поддержка `1.x`, `1.2.x`). 

* Подходы: строгий формат SemVer; явные `json_pointer` на проблемное поле.

* Допущения/ограничения: ожидается 2 или 3 сегмента в `expected_version`. 

**Fail-fast.** `SANITY_MISSING_KEY`, `SANITY_TYPE_MISMATCH`, `SANITY_MANIFEST_MISMATCH`, `SANITY_VERSION_MISMATCH`, `SANITY_INVALID_VERSION_FORMAT`. 

**Дополнительно.** Сообщения подсказывают, где исправлять (`/meta/version` или `manifest.expected_version`). 

---

# _ConfigUniversalJsonSchemaValidation

**Задача класса.** Универсальная JSON Schema-валидация профилей по их типу (Draft 2020-12), с локальным реестром `$id` для соседних схем. 

**Сигнатуры:**

* `__init__(self, *, configs_schema_dir: str | Path, schema_index: Mapping[Any, str | Path] | None = None)`
* `validate(self, *, entry: ConfigEntry, data: dict[str, Any]) -> None` — (по коду реализовано в теле класса через последовательность `_resolve_schema_path/_load_schema/_build_validator/validator.validate`). На несоответствие — `SCHEMA_VALIDATION_FAILED`. 

**Зона ответственности.** Только соответствие схеме; никаких нормализаций или семантики. 

**Техреализация.**

* Методы: `_resolve_schema_path`, `_load_schema`, `_build_validator`, блок `validator.validate`. 

* Алгоритм:
  
  1. Определить путь к схеме: `schema_index` (ключ — `ConfigType` или строковое значение) → иначе `<configs_schema_dir>/<type>.schema.json`.
  2. Загрузить схему (строгий JSON).
  3. Построить валидатор с `referencing.Registry`: зарегистрировать все `*.schema.json` по их `$id` + переопределить основной.
  4. Провалидировать документ; на ошибки — выровненное сообщение/указатель. 

* Подходы: локальный `$id`-реестр, Draft202012Validator.

* Допущения/ограничения: работает с относительными путями, без абсолютных URI; наличие `jsonschema`/`referencing`. 

**Fail-fast.** `SCHEMA_NOT_FOUND`, `IO_INVALID_JSON` (схема), `SCHEMA_VALIDATION_FAILED` (включая проблемы `$ref`). 

**Дополнительно.** Формат диагностики синхронизирован с `_ManifestReader`. 

---

# _ConfigActiveProfileSelector

**Задача класса.** Выбрать и нормализовать активные профили по правилам `meta.active_profiles`; вернуть глубокие копии. 

**Сигнатура:**

* `run(self, *, config_id: str, config_type: Any, data: dict[str, Any]) -> list[dict[str, Any]]`
  Выход: список профилей в требуемом порядке либо `[profiles[0]]`, если активные не заданы. 

**Зона ответственности.** Только выбор профилей и базовые проверки массива профилей/идентификаторов; не мутирует вход. 

**Техреализация.**

* Методы: `run`, `_check_duplicates_in_active_ids`, `_check_unique_profile_ids`, `_type_to_str`.

* Алгоритм:
  
  1. Проверить, что `profiles` — непустой список.
  2. Если `active_profiles` пуст — вернуть первый профиль.
  3. Проверить дубликаты в `active_profiles`.
  4. Индексировать `profiles` по `profile_id`.
  5. Сопоставить `active_profiles` → профили (копии), на отсутствие — `PROFILE_NOT_FOUND`. 

* Подходы: fail-fast, детальные `json_pointer` (`/meta/active_profiles/<i>`).

* Допущения/ограничения: после JSON Schema валидации `profiles` содержит `profile_id`. 

**Fail-fast.** `PROFILE_ARRAY_EMPTY`, `PROFILE_DUPLICATE`, `PROFILE_NOT_FOUND`. 

**Дополнительно.** Возвращаются именно глубокие копии, чтобы исключить побочные эффекты. 

---

# _ConfigSemanticInvariantsByType

**Задача класса.** Семантические (вне схемы) инварианты по типам конфигов: `auth`, `export`, `logging`, `network`, `site`. Работает только по активным профилям. 

**Сигнатуры:**

* `__init__(self) -> None` — регистрирует диспетчер по типам.
* `run(self, *, config_id: str, config_type: Any, profiles: list[dict]) -> None` — для каждого активного профиля вызывает профильный чекер. 

**Зона ответственности.** Логические проверки, которые либо неудобно/невозможно выразить в JSON Schema, с немедленным остановом на первом нарушении. 

**Техреализация.**

* Методы: `run` + приватные `_check_auth`, `_check_export`, `_check_logging`, `_check_network`, `_check_site` и общие helper’ы `_type_to_str`, `_ptr`. 

* Алгоритм (фрагменты требований):
  
  * **auth**: при `auth_type="form"` обязательны `request.method`, `request.endpoint`, непустой `form_fields`; при `csrf.enabled=true` — `source_url`, `token_selector`, `token_field_name`; при `api_token.enabled=true` — `token_header`, `token_value`; каждый `success_checks[i]` содержит `params`. 
  * **export**: `sheet_name_core_limit ≤ sheet_name_max_len ≤ 31`. 
  * **logging**: `ui.level ∈ events.levels`. 
  * **network**:
    • `rate_limit.burst_size ≥ requests_per_second`;
    • `retry_policy.timeout_budget_s` ≥ расчётной суммарной задержки по параметрам ретраев;
    • `session.request_timeout_s ≥ retry_policy.timeout_budget_s`. (Расчёт с учётом `FLOAT_TOLERANCE`/`MAX_BACKOFF_DELAY_GUARD` в модуле). 
  * **site**: уникальность `container_id`/`field.name`; непустые массивы `item_containers/fields`; при `value_selector.extract="attr"` обязателен ненулевой `attr`; валидный `page_container_selector`. 

* Подходы: детальные JSON Pointer через `_ptr`, строгая локализация ошибок, fail-fast.

* Допущения/ограничения: запускается после успешной JSON Schema-валидации и выбора активных профилей (глубокие копии). 

**Fail-fast.** Семейство `SEMANTIC_*` (см. примеры: `SEMANTIC_AUTH_FORM_MISSING_FIELDS`, `SEMANTIC_AUTH_CSRF_INCOMPLETE`, `SEMANTIC_LOGGING_UI_LEVEL_INVALID`, `SEMANTIC_EXPORT_SHEET_LIMIT`, `SEMANTIC_NETWORK_*`, `SEMANTIC_SITE_*`). 

**Дополнительно.** Для неизвестных типов (`pipeline`, `url`, `meta`) проверок нет — метод `run` просто возвращает. 

---

# Примечание: _ConfigPlaceholderFilling (исключён)

**Статус.** Зарезервированный ранее этап интерполяции плейсхолдеров `${env:...}`/`${secret:...}` исключён из актуальной реализации как нефункциональный; перенесён на будущее (исключён из пайплайна). 

---

# Контекст о фасаде ConfigLoader (по спецификации)

В текущем файле полноценный внешний фасад `ConfigLoader` отсутствует; его роль и требования зафиксированы в спецификации: центральная точка вызова, сбор DTO по `ConfigType`, fail-fast на дубли `config_id` в результате (`RESULT_DUPLICATE_CONFIG_ID`), детерминированная регистрация по порядку из манифеста. 

---

## Сводные fail-fast требования (по этапам)

* **Манифест:** `IO_NOT_FOUND`, `IO_INVALID_JSON`, `SCHEMA_VALIDATION_FAILED`, `SANITY_MISSING_TYPE`, `SANITY_DUPLICATE_CONFIG_ID`. 
* **Чтение профиля:** `IO_NOT_FOUND`, `IO_TOO_LARGE`, `IO_INVALID_JSON`. 
* **Sanity профиля:** `SANITY_MISSING_KEY`, `SANITY_TYPE_MISMATCH`, `SANITY_MANIFEST_MISMATCH`, `SANITY_VERSION_MISMATCH`, `SANITY_INVALID_VERSION_FORMAT`. 
* **JSON Schema профиля:** `SCHEMA_NOT_FOUND`, `IO_INVALID_JSON` (схема), `SCHEMA_VALIDATION_FAILED` (включая неправильные `$ref`). 
* **Активные профили:** `PROFILE_ARRAY_EMPTY`, `PROFILE_DUPLICATE`, `PROFILE_NOT_FOUND`. 
* **Семантика по типам:** семейство `SEMANTIC_*` (auth/export/logging/network/site). 


