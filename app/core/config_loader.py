# config_loader.py
# Python 3.13.5
'''
Зона ответственности этого файла: загрузка конфигураций (манифест+конфиги),
валидация, нормализация и проекция в DTO.

Дизайн: один файл — несколько внутренних подсистемных классов (см. спецификацию).
Этот подход даёт модульность без физического дробления файла.

ВАЖНО:
- ConfigError создаётся в месте возникновения ошибки (SANITY_*, PROFILE_*, SEMANTIC_* …).
- _ErrorFormatter не знает о бизнес-логике ошибок и не решает, какой код использовать.
  Он лишь нормализует Exception → StructuredError.
'''
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Iterable, Optional, Mapping
from pathlib import Path
import json
import copy

# Опциональные зависимости: jsonschema и pydantic могут отсутствовать
try:
    from jsonschema.exceptions import ValidationError as JsonSchemaValidationError  # type: ignore
    from jsonschema import Draft202012Validator, exceptions as jsonschema_exc  # type: ignore
    from referencing import Registry, Resource
except Exception:  # pragma: no cover - окружение без jsonschema
    JsonSchemaValidationError = None  # type: ignore

try:
    # pydantic v2 реэкспортирует ValidationError (из pydantic_core)
    from pydantic import ValidationError as PydanticValidationError  # type: ignore
except Exception:  # pragma: no cover - окружение без pydantic
    PydanticValidationError = None  # type: ignore

from app.dto.manifest_dto import (
    ConfigType,
    ConfigEntry,
    ManifestMeta,
    ConfigManifestV1 as _ManifestDTO,
)

# Допустимая погрешность сравнения чисел с плавающей точкой (1e-9 секунд ≈ 1 нс)
FLOAT_TOLERANCE = 1e-9
# Защитное ограничение на экспоненциальный рост задержек (чтобы избежать переполнения float)
MAX_BACKOFF_DELAY_GUARD = 1e18

# ------------------------------
# Универсальный класс бизнес-ошибки
# ------------------------------

class ConfigError(Exception):
    """
    Универсальное исключение для бизнес-ошибок ConfigLoader.
    Код должен строго соответствовать спецификации (таблица R-7).
    """
    def __init__(
        self,
        code: str,
        message: str,
        *,
        json_pointer: str | None = None,
        profile_id: str | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.json_pointer = json_pointer
        self.profile_id = profile_id

# ------------------------------
# Структура результата ошибки
# ------------------------------

@dataclass(frozen=True, slots=True)
class StructuredError:
    """
    Унифицированная форма ошибки для всех этапов ConfigLoader.

    Поля:
        code: машинный код ошибки по спецификации (IO_*, SCHEMA_*, DTO_*, SANITY_*, и т.д.)
        message: короткое и воспроизводимое описание
        config_id: ID конфига (например, "network:default")
        type: тип конфига/этап пайплайна (например, "network", "auth", "pipeline")
        profile_id: (опционально) профиль, к которому относится ошибка
        json_pointer: (опционально) RFC 6901 JSON Pointer до проблемного поля
    """

    code: str
    message: str
    config_id: str
    type: str
    profile_id: Optional[str] = None
    json_pointer: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Сериализация в dict (готово к JSON)."""
        return asdict(self)


# --------------------------------------
# Внутренние утилиты и форматтер ошибок
# --------------------------------------

class _ErrorFormatter:
    """
    Преобразует исключения в StructuredError.
    Не знает о бизнес-логике — только нормализует структуру.

    Методы:
        format(exc, *, config_id, type, profile_id=None, code_hint=None) -> StructuredError
    """

    @staticmethod
    def format(
        exc: BaseException,
        *,
        config_id: str,
        type: str | Any,
        profile_id: str | None = None,
        code_hint: str | None = None,
    ) -> StructuredError:
        """
        Преобразует исключение в структурированную ошибку.

        Args:
            exc: исходное исключение
            config_id: идентификатор конфига ("network:default")
            type: тип (строка или Enum) — будет приведён к строке
            profile_id: (опционально) профиль
            code_hint: (опционально) подсказка кода (принудительное значение)

        Returns:
            StructuredError — детерминированная ошибка
        """
        err_type_str = _ErrorFormatter._type_to_str(type)
        # Код: приоритет .code → code_hint → fallback
        code = getattr(exc, "code", None) or code_hint or "IO_INVALID_DOC"
        
        # базовые значения
        message = str(exc).strip() or exc.__class__.__name__
        pointer = getattr(exc, "json_pointer", None)

        # JsonSchema
        if JsonSchemaValidationError is not None and isinstance(exc, JsonSchemaValidationError):
            ctx = _ErrorFormatter._extract_jsonschema_error(exc)
            pointer = ctx.get("pointer") or pointer
            message = ctx.get("msg", message)

        # Pydantic
        elif PydanticValidationError is not None and isinstance(exc, PydanticValidationError):
            ctx = _ErrorFormatter._extract_pydantic_error(exc)
            pointer = _ErrorFormatter._pointer_from_sequence(ctx.get("loc", ())) or pointer
            message = ctx.get("msg", message)
        
        exc_profile_id = getattr(exc, "profile_id", None)
        if profile_id is None and exc_profile_id is not None:
            profile_id = exc_profile_id


        return StructuredError(
            code=code,
            message=message,
            config_id=config_id,
            type=err_type_str,
            profile_id=profile_id,
            json_pointer=pointer,
        )
    
    # --------------------
    # Вспомогательные API
    # --------------------

    @staticmethod
    def _type_to_str(type_value: Any) -> str:
        """Поддержка enum/строки: приводит значение типа к строке (значению enum, если есть)."""
        if type_value is None:
            return ""
        if hasattr(type_value, "value"):  # Enum
            return str(getattr(type_value, "value"))
        return str(type_value)

    @staticmethod
    def _extract_pointer(exc: BaseException) -> Optional[str]:
        """Пробует извлечь JSON Pointer из jsonschema/pydantic ошибок."""
        try:
            if JsonSchemaValidationError is not None and isinstance(exc, JsonSchemaValidationError):
                return _ErrorFormatter._pointer_from_sequence(getattr(exc, "path", []))
        except Exception:
            pass

        try:
            if PydanticValidationError is not None and isinstance(exc, PydanticValidationError):
                errors = exc.errors()
                if errors and isinstance(errors[0], dict):
                    loc = errors[0].get("loc")
                    if isinstance(loc, (list, tuple)):
                        return _ErrorFormatter._pointer_from_sequence(loc)
        except Exception:
            pass

        return None

    @staticmethod
    def _pointer_from_sequence(seq: Iterable[Any]) -> str:
        """
        Собирает JSON Pointer (RFC 6901) из последовательности сегментов (имена/индексы).
        Экранирует '~' -> '~0' и '/' -> '~1' по стандарту.
        """
        parts: list[str] = []
        for seg in seq:
            s = str(seg)
            s = s.replace("~", "~0").replace("/", "~1")
            parts.append(s)
        return "/" + "/".join(parts) if parts else ""
    
    @staticmethod
    def _extract_pydantic_error(exc: BaseException) -> dict[str, Any]:
        """
        Возвращает нормализованные данные из Pydantic ValidationError.

        Правила:
        - Определяется приоритетный тип ошибки.
        - В сообщение попадают все ошибки только этого типа.
        - Возвращается первая loc для указания json_pointer.

        Returns:
            dict: {"loc": tuple, "type": str, "msg": str}
        """
        try:
            errors = getattr(exc, "errors")()
            if not isinstance(errors, list) or not errors:
                return {}

            priority = [
                "missing",
                "enum",
                "not_none",
                "none_is_not_allowed",
                "type_error",
                "extra",
                "extra_forbidden",
            ]

            types = [str(err.get("type", "")).lower() for err in errors]
            main_type = next((p for p in priority if any(t.startswith(p) for t in types)), types[0])

            selected = [err for err in errors if str(err.get("type", "")).lower().startswith(main_type)]

            parts = []
            for err in selected:
                loc = "/".join(str(p) for p in err.get("loc", ()))
                msg = err.get("msg", "")
                parts.append(f"[{loc}] {msg}" if loc else msg)

            return {
                "loc": tuple(selected[0].get("loc", ())),
                "type": main_type,
                "msg": "; ".join(parts),
            }
        except Exception:
            return {}

    @staticmethod
    def _extract_jsonschema_error(exc: BaseException) -> dict[str, Any]:
        """
        Возвращает нормализованные данные из jsonschema.ValidationError.

        Правила:
        - Если есть context (вложенные ошибки), агрегируем их сообщения.
        - pointer = path из основной ошибки.
        - msg = объединённые сообщения.

        Returns:
            dict: {"pointer": str, "msg": str}
        """
        try:
            if not isinstance(exc, JsonSchemaValidationError):
                return {}

            pointer = _ErrorFormatter._pointer_from_sequence(getattr(exc, "path", []))

            messages = []
            if getattr(exc, "context", None):
                for sub in exc.context:
                    sub_pointer = _ErrorFormatter._pointer_from_sequence(getattr(sub, "path", []))
                    messages.append(f"[{sub_pointer}] {sub.message}")
            else:
                messages.append(exc.message or str(exc))

            return {
                "pointer": pointer,
                "msg": "; ".join(messages),
            }
        except Exception:
            return {}

#===============================================================================================================================================
#===============================================================================================================================================
#===============================================================================================================================================

class _ManifestReader:
    """
    Читает JSON-манифест, валидирует по JSON Schema (Draft 2020-12) и строит DTO.

    Ответственность класса:
      - I/O: проверка наличия файла и синтаксиса JSON.
      - Schema: валидация по `manifest/v1/manifest.schema.json`.
      - DTO: проекция в строго типизированные dataclass-структуры.
      - Sanity: минимальные бизнес-инварианты (обязательные типы, уникальность config_id).

    Не делает:
      - выбор активных профилей,
      - интерполяцию плейсхолдеров,
      - семантические проверки разных конфигов.
    """

    def __init__(self, manifest_path: str | Path, *, manifest_schema_path: str | Path) -> None:
        """
        Args:
            manifest_path: Путь к JSON-файлу манифеста.
            manifest_schema_path: Явный путь к manifest.schema.json (DI).
        """
        self._manifest_path = Path(manifest_path)
        self._manifest_schema_path = Path(manifest_schema_path)

    # ------------------------------ API ------------------------------

    def read(self) -> _ManifestDTO:
        """
        Выполняет полный цикл: чтение → валидация → DTO → sanity-проверки.

        Returns:
            Экземпляр DTO манифеста (`ConfigManifest v1`).

        Raises:
            ConfigError:
                - IO_NOT_FOUND — файл отсутствует;
                - IO_INVALID_JSON — синтаксическая ошибка JSON;
                - SCHEMA_VALIDATION_FAILED — несоответствие JSON Schema;
                - SANITY_MISSING_TYPE — не хватает обязательных типов конфигов;
                - SANITY_DUPLICATE_CONFIG_ID — повторяющиеся config_id.
        """
        data = self._load_json(self._manifest_path)
        schema = self._load_manifest_schema()
        self._validate_schema(data, schema)
        dto = self._to_dto(data)
        self._check_required_types(dto)
        self._check_unique_config_ids(dto)
        return dto

    # --------------------------- I/O helpers --------------------------

    def _load_json(self, path: Path) -> dict[str, Any]:
        """Загружает JSON, обеспечивая fail-fast диагностику."""
        if not path.exists() or not path.is_file():
            raise ConfigError("IO_NOT_FOUND", f"Manifest file not found: {str(path)}")
        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            # Указываем место сбоя в сообщении; json_pointer для сырого JSON не применим.
            msg = f"Invalid JSON: {e.msg} at line {e.lineno}, column {e.colno}"
            pointer = f"{e.lineno}:{e.colno}"
            raise ConfigError("IO_INVALID_JSON", msg, json_pointer=pointer)

    def _load_manifest_schema(self) -> dict[str, Any]:
        """
        Загружает schema как dict по явно переданному пути.
        """
        if not self._manifest_schema_path.exists() or not self._manifest_schema_path.is_file():
            raise ConfigError("IO_NOT_FOUND", f"Manifest schema not found: {self._manifest_schema_path}")

        try:
            with self._manifest_schema_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            msg = f"Invalid JSON in manifest schema: {e.msg} at line {e.lineno}, column {e.colno}"
            pointer = f"{e.lineno}:{e.colno}"
            raise ConfigError("IO_INVALID_JSON", msg, json_pointer=pointer)

    # ------------------------- Validation -----------------------------

    def _validate_schema(self, data: dict[str, Any], schema: dict[str, Any]) -> None:
        """
        Валидация по Draft 2020-12. На несоответствие — бросаем ConfigError с указанием JSON Pointer.
        """
        validator = Draft202012Validator(schema)
        try:
            validator.validate(data)
        except jsonschema_exc.ValidationError as e:
            # Сформируем короткое воспроизводимое сообщение и JSON Pointer
            pointer = self._pointer_from_sequence(e.path)
            # Строим компактный текст ошибки (как в jsonschema.message)
            message = e.message
            raise ConfigError(
                "SCHEMA_VALIDATION_FAILED",
                message,
                json_pointer=pointer or None,
            )

    @staticmethod
    def _pointer_from_sequence(seq: Iterable[Any]) -> str:
        """
        RFC 6901 JSON Pointer из последовательности сегментов (имя/индекс).
        Экранируем ~ → ~0 и / → ~1. Пустая последовательность → "".
        """
        parts: list[str] = []
        for seg in seq:
            s = str(seg).replace("~", "~0").replace("/", "~1")
            parts.append(s)
        return "/" + "/".join(parts) if parts else ""

    # --------------------------- DTO build ----------------------------

    def _to_dto(self, data: dict[str, Any]) -> _ManifestDTO:
        """Проекция описанного схемой словаря в строго типизированные dataclass DTO."""
        meta_raw = data["manifest_meta"]
        configs_raw = data["configs"]

        meta = ManifestMeta(
            manifest_id=meta_raw["manifest_id"],
            version=meta_raw["version"],
            created_at=meta_raw["created_at"],
            description=meta_raw.get("description", ""),
            tags = meta_raw["tags"],
        )

        entries: list[ConfigEntry] = []
        for i, c in enumerate(configs_raw):
            try:
                entries.append(
                    ConfigEntry(
                        type=ConfigType(c["type"]),  # Enum из DTO
                        config_id=c["config_id"],
                        path=c["path"],
                        expected_version=c["expected_version"],
                    )
                )
            except Exception as e:
                # На случай нештатной ошибки проекции (теоретически схема уже гарантирует корректность)
                # Конвертируем в контролируемую бизнес-ошибку DTO_* при необходимости в будущем.
                raise ConfigError("SCHEMA_VALIDATION_FAILED", f"Invalid config entry at index {i}: {e}", json_pointer=f"/configs/{i}")

        return _ManifestDTO(manifest_meta=meta, configs=entries)

    # ----------------------- Business invariants ----------------------

    def _check_required_types(self, manifest: _ManifestDTO) -> None:
        """
        Проверяет, что все обязательные типы конфигов представлены хотя бы раз.
        """
        required: set[ConfigType] = {
            ConfigType.pipeline,
            ConfigType.network,
            ConfigType.site,
            ConfigType.url,
            ConfigType.auth,
            ConfigType.logging,
            ConfigType.export,
        }
        present: set[ConfigType] = {e.type for e in manifest.configs}
        missing = [t.value for t in sorted(required - present, key=lambda x: x.value)]
        if missing:
            raise ConfigError(
                "SANITY_MISSING_TYPE",
                f"Missing required config types: {', '.join(missing)}",
            )

    def _check_unique_config_ids(self, manifest: _ManifestDTO) -> None:
        """
        Проверяет уникальность config_id в пределах всего манифеста.
        При повторе — детерминированная ошибка с указанием индексов.
        """
        first_seen: dict[str, int] = {}
        for idx, entry in enumerate(manifest.configs):
            cid = entry.config_id
            if cid in first_seen:
                first_idx = first_seen[cid]
                # Укажем pointer до повторной записи (вторая позиция)
                raise ConfigError(
                    "SANITY_DUPLICATE_CONFIG_ID",
                    f"Duplicate config_id '{cid}' at index {idx} (first at index {first_idx})",
                    json_pointer=f"/configs/{idx}/config_id",
                )
            first_seen[cid] = idx

#===============================================================================================================================================
#===============================================================================================================================================
#===============================================================================================================================================


class _ConfigReader:
    """
    Этап 2.1: Разрешение пути и чтение JSON-конфига.

    Ответственность:
      - Path: резолв относительного пути относительно DI-параметра `project_root`.
      - I/O: проверка существования/доступности файла (UTF-8).
      - JSON: десериализация содержания (draft-neutral).
      - Shape: базовая структурная проверка — документ должен быть JSON-объектом (dict),
               а не None/[]/"".

    Не делает:
      - Sanity/Schema/DTO/семантику/интерполяцию — это последующие этапы пайплайна.
    """

    def __init__(self, *, project_root: str | Path, max_file_size: int = 10 * 1024) -> None:
        """
        Args:
            project_root: Абсолютный путь к корню проекта `auth-parser-remake`.
            max_file_size: Максимальный допустимый размер JSON-файла (байты).
                           По умолчанию 8 МБ. При превышении → IO_TOO_LARGE.
        """
        root = Path(project_root)
        # Разрешаем без strict, чтобы не бросать здесь; проверки — позже в _load_json
        self._project_root = root.resolve(strict=False)
        self._max_file_size = max_file_size

    # ------------------------------ API ------------------------------

    def read(self, entry: ConfigEntry) -> dict[str, Any]:
        """
        Читает и возвращает сырое содержимое конфига как dict.

        Args:
            entry: Запись из манифеста с полями (.path/.config_id/.type).

        Returns:
            dict[str, Any]: JSON-документ верхнего уровня (объект).
        """
        # 1) Разрешаем путь
        cfg_path = self._resolve_path(entry.path)
        # 2) Загружаем JSON
        data = self._load_json(cfg_path)
        # 3) Базовая структурная проверка
        invalid_shapes = (None, "", [])
        if data in invalid_shapes:
            raise ConfigError("IO_INVALID_DOC", "Config must not be null/empty/[]")

        if not isinstance(data, dict):
            raise ConfigError("IO_INVALID_DOC", "Config must be a JSON object (dict)")

        return data

    # --------------------------- Helpers -----------------------------

    def _resolve_path(self, raw_path: str | Path) -> Path:
        """
        Разрешает путь к файлу конфига.
        Проверяет корректность симлинков.
        """
        p = Path(raw_path)
        resolved = p if p.is_absolute() else (self._project_root / p).resolve(strict=False)

        # Дополнительно: проверка битых симлинков
        if resolved.is_symlink() and not resolved.exists():
            raise ConfigError("IO_NOT_FOUND", f"Broken symlink: {str(resolved)}")

        return resolved

    def _load_json(self, path: Path) -> dict[str, Any]:
        """
        Загружает файл в UTF-8 и десериализует JSON.

        Кодировки/безопасность:
          - Всегда открываем с encoding='utf-8'.
          - Только json.load — без исполнения какого-либо кода.

        Ошибки маппятся в коды IO_* согласно спецификации.
        """
        # Существование и тип объекта файловой системы
        try:
            if not path.exists() or not path.is_file():
                raise ConfigError("IO_NOT_FOUND", f"Config file not found or not a file: {str(path)}")
        except OSError as e:
            # Например, отказ в доступе при stat()
            raise ConfigError("IO_NOT_FOUND", f"I/O error accessing config file: {str(path)} ({e.strerror or e})")

        # Проверка размера файла
        try:
            size = path.stat().st_size
            if size > self._max_file_size:
                raise ConfigError("IO_TOO_LARGE", f"Config file too large ({size} bytes): {str(path)}")
        except OSError as e:
            raise ConfigError("IO_NOT_FOUND", f"I/O error stat config file: {str(path)} ({e.strerror or e})")

        # Чтение и парсинг JSON
        try:
            with path.open("r", encoding="utf-8-sig") as f:
                return json.load(f)
        except PermissionError as e:
            raise ConfigError("IO_NOT_FOUND", f"Access denied to config file: {str(path)}") from e
        except OSError as e:
            raise ConfigError("IO_NOT_FOUND", f"I/O error reading config file: {str(path)} ({e.strerror or e})") from e
        except json.JSONDecodeError as e:
            pointer = f"{e.lineno}:{e.colno}"
            msg = f"Invalid JSON: {e.msg} at line {e.lineno}, column {e.colno}"
            raise ConfigError("IO_INVALID_JSON", msg, json_pointer=pointer) from e

#===============================================================================================================================================
#===============================================================================================================================================
#===============================================================================================================================================


class _ConfigSanityCheck:
    """
    Этап 2.2: Минимальные sanity-проверки конфига до JSON Schema.

    Ответственность:
      - Базовая структура верхнего уровня: ключи и типы (`meta`, `profiles`).
      - Согласованность с манифестом:
          * meta.config_id == entry.config_id
          * meta.version удовлетворяет entry.expected_version

    Исключения (fail-fast):
      - SANITY_MISSING_KEY               — нет обязательного ключа
      - SANITY_TYPE_MISMATCH             — тип поля неверный
      - SANITY_MANIFEST_MISMATCH         — расхождение с манифестом (config_id)
      - SANITY_VERSION_MISMATCH          — версия не удовлетворяет маске expected_version
      - SANITY_INVALID_VERSION_FORMAT    — meta.version или expected_version в неожиданном формате
    """

    def run(self, *, entry: "ConfigEntry", data: dict[str, Any]) -> None:
        """
        Выполняет sanity-проверки. На любое нарушение — бросает ConfigError.

        Args:
            entry: запись из манифеста (config_id, expected_version, type).
            data: JSON-конфиг как dict (получен из _ConfigReader).

        Raises:
            ConfigError: с кодом SANITY_* и json_pointer на проблемное поле.
        """
        # 1) Ключи верхнего уровня
        if "meta" not in data:
            raise ConfigError("SANITY_MISSING_KEY", "Missing required key 'meta'", json_pointer="/meta")
        if "profiles" not in data:
            raise ConfigError("SANITY_MISSING_KEY", "Missing required key 'profiles'", json_pointer="/profiles")

        meta_section = data["meta"]
        profiles_section = data["profiles"]

        # 2) Типы верхнего уровня
        if not isinstance(meta_section, dict):
            raise ConfigError("SANITY_TYPE_MISMATCH", "'meta' must be an object (dict)", json_pointer="/meta")
        if not isinstance(profiles_section, list):
            raise ConfigError("SANITY_TYPE_MISMATCH", "'profiles' must be an array (list)", json_pointer="/profiles")

        # 3) Согласованность с манифестом: config_id
        meta_config_id = meta_section.get("config_id")
        if meta_config_id is None:
            raise ConfigError("SANITY_MISSING_KEY", "Missing required 'meta.config_id'", json_pointer="/meta/config_id")
        if str(meta_config_id) != str(entry.config_id):
            raise ConfigError(
                "SANITY_MANIFEST_MISMATCH",
                f"meta.config_id '{meta_config_id}' != manifest config_id '{entry.config_id}'",
                json_pointer="/meta/config_id",
            )

        # 4) Версия против expected_version
        meta_actual_version = meta_section.get("version")
        if meta_actual_version is None:
            raise ConfigError("SANITY_MISSING_KEY", "Missing required 'meta.version'", json_pointer="/meta/version")

        expected_version = str(entry.expected_version)
        if not self._match_expected_version(str(meta_actual_version), expected_version):
            raise ConfigError(
                "SANITY_VERSION_MISMATCH",
                f"Config version '{meta_actual_version}' does not satisfy expected '{expected_version}'"
                f"Check /meta/version (config) or /manifest/expected_version (manifest).",
                json_pointer="/meta/version",
            )

    # --------------------------- Helpers -----------------------------
    @staticmethod
    def _match_expected_version(actual_ver: str, expected_ver: str) -> bool:
        """
        Универсальная проверка версии: поддерживает маски 'x' в expected.
        Формат обоих: 'MAJOR.MINOR.PATCH' (3 сегмента).
        """
        actual_ver_parts = str(actual_ver).strip().split(".")
        expected_ver_parts = str(expected_ver).strip().split(".")

        if len(actual_ver_parts) != 3:
            raise ConfigError(
                "SANITY_INVALID_VERSION_FORMAT", 
                f"Invalid actual='{actual_ver}'", 
                json_pointer="/meta/version"
            )
        
        if len(expected_ver_parts) not in (2, 3):
            raise ConfigError(
                "SANITY_INVALID_VERSION_FORMAT", 
                f"Unsupported expected='{expected_ver}'", 
                json_pointer="/meta/version"
            )

        for a, e in zip(actual_ver_parts, expected_ver_parts):
            if e == "x":
                continue
            if not (a.isdigit() and e.isdigit()):
                raise ConfigError(
                    "SANITY_INVALID_VERSION_FORMAT",
                    f"Invalid segment: actual='{a}', expected='{e}'"
                    f"Check /meta/version (config) or /manifest/expected_version (manifest).",
                    json_pointer="/meta/version",
                )
            if int(a) != int(e):
                return False

        return True

#===============================================================================================================================================
#===============================================================================================================================================
#===============================================================================================================================================

class _ConfigUniversalJsonSchemaValidation:
    """
    Универсальная валидация конфигурации по JSON Schema (Draft 2020-12).

    Ответственность:
      - Нахождение и загрузка схемы по типу конфига (ConfigEntry.type).
      - Подготовка резолвера $ref (относительные пути на соседние *.schema.json).
      - Запуск строгой валидации через Draft202012Validator.
      - Fail-fast: при первой ошибке поднимается ConfigError с JSON Pointer.

    Не делает:
      - Никаких нормализаций/DTO/семантики — только соответствие схеме.
    """

    def __init__(
        self,
        *,
        configs_schema_dir: str | Path,
        schema_index: Mapping[Any, str | Path] | None = None,
    ) -> None:
        """
        Args:
            configs_schema_dir: Абсолютный или относительный путь к директории схем конфигов v1,
                                например: ".../schemas/configs/v1".
            schema_index: Необязательная карта явных путей к схемам.
                          Ключ: ConfigType или его строковое значение.
                          Значение: путь к schema.json для данного типа.

        Примечания по DI:
            - В проде предпочтительно передавать абсолютный путь.
            - При наличии schema_index он имеет приоритет над соглашением имён.
        """
        try:
            self._configs_schema_dir = Path(configs_schema_dir).resolve(strict=True)
        except FileNotFoundError as e:
            raise ConfigError(
                "SCHEMA_NOT_FOUND",
                f"Configs schema directory not found: {configs_schema_dir}"
            ) from e
            
        self._schema_index = dict(schema_index or {})

    # ------------------------------ API ------------------------------

    def run(self, *, entry: "ConfigEntry", data: dict[str, Any]) -> None:
        """
        Валидирует конфиг против соответствующей JSON Schema.

        Args:
            entry: запись манифеста (type/config_id/expected_version/…).
            data:  JSON-конфиг как dict (после _ConfigReader и _ConfigSanityCheck).

        Raises:
            ConfigError:
                - SCHEMA_NOT_FOUND — файл схемы отсутствует или неразрешимые $ref;
                - IO_INVALID_JSON — файл схемы повреждён (невалидный JSON);
                - SCHEMA_VALIDATION_FAILED — несоответствие документа схеме.
        """
        schema_path = self._resolve_schema_path(entry)
        schema = self._load_schema(schema_path)
        validator = self._build_validator(schema, schema_path)

        try:
            validator.validate(data)
        except jsonschema_exc.RefResolutionError as e:
            # Ошибка резолвинга $ref: схема есть, но ссылки внутри неё некорректны.
            pointer = None
            msg = f"Invalid $ref in schema for config_id='{entry.config_id}': {e}"
            raise ConfigError(
                "SCHEMA_VALIDATION_FAILED",
                msg,
                json_pointer=pointer,
            ) from e

        except jsonschema_exc.ValidationError as e:
            # Выровняем формат ошибки с уже применяемой практикой (_ManifestReader).
            pointer = e.json_path
            message = e.message
            raise ConfigError(
                "SCHEMA_VALIDATION_FAILED",
                message,
                json_pointer=pointer or None,
            ) from e

    # --------------------------- Helpers -----------------------------

    def _resolve_schema_path(self, entry: "ConfigEntry") -> Path:
        """
        Возвращает путь к schema.json для данного типа конфига.

        Алгоритм:
          1) Если в schema_index есть ключ по entry.type или entry.type.value → берём оттуда.
          2) Иначе: <configs_schema_dir>/<type>.schema.json (имя по значению Enum).
        """
        type_key = getattr(entry.type, "value", str(entry.type))
        # 1) Явная карта
        if type_key in self._schema_index:
            p = Path(self._schema_index[type_key])
        elif entry.type in self._schema_index:
            p = Path(self._schema_index[entry.type])
        else:
            # 2) Соглашение имени
            filename = f"{type_key}.schema.json"
            p = self._configs_schema_dir / filename

        if not p.exists() or not p.is_file():
            raise ConfigError(
                "SCHEMA_NOT_FOUND",
                f"Schema not found for type '{type_key}': {str(p)}",
            )
        return p

    def _load_schema(self, schema_path: Path) -> dict[str, Any]:
        """
        Загружает JSON Schema с диска.

        На ошибки:
          - не найден файл → SCHEMA_NOT_FOUND (обработано в _resolve_schema_path);
          - синтаксис JSON → IO_INVALID_JSON.
        """
        try:
            with schema_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            pointer = f"{e.lineno}:{e.colno}"
            msg = f"Invalid JSON in schema: {e.msg} at line {e.lineno}, column {e.colno}"
            raise ConfigError("IO_INVALID_JSON", msg, json_pointer=pointer) from e

    def _build_validator(self, schema: dict[str, Any], schema_path: Path) -> Draft202012Validator:
        """
        Готовит валидатор Draft 2020-12.
        Все соседние схемы регистрируются по их $id (если указано).
        Работает только с относительными путями, без абсолютных URI.
        """
        base_dir: Path = schema_path.parent
        registry = Registry()

        # Загружаем все *.schema.json и регистрируем по $id
        for file in base_dir.glob("*.schema.json"):
            try:
                with file.open("r", encoding="utf-8") as f:
                    contents = json.load(f)
                resource = Resource.from_contents(contents)

                schema_id = contents.get("$id")
                if schema_id:  # используем только $id
                    registry = registry.with_resource(schema_id, resource)
            except Exception:
                continue

        # Регистрируем основную схему ещё раз (перекроет при совпадении $id)
        schema_id = schema.get("$id")
        if schema_id:
            registry = registry.with_resource(schema_id, Resource.from_contents(schema))

        return Draft202012Validator(schema, registry=registry)

#===============================================================================================================================================
#===============================================================================================================================================
#===============================================================================================================================================

class _ConfigActiveProfileSelector:
    """
    Выбор и нормализация активных профилей.

    Ответственность:
      - Определить набор активных профилей по правилам:
          1) Если meta.active_profiles пуст → выбрать первый элемент profiles.
          2) Если meta.active_profiles не пуст → выбрать профили строго по порядку id из списка.
      - Fail-fast при нарушениях:
          * profile_id из active_profiles отсутствует в profiles → PROFILE_NOT_FOUND;
          * в active_profiles есть повторяющийся id → PROFILE_DUPLICATE;
          * profiles пуст → PROFILE_ARRAY_EMPTY (дублируем защиту).
      - Не мутировать входной `data`, возвращаются копии профилей, а не ссылки на объекты из `data`.

    Входные гарантии (после JSON Schema валидации):
      - data["meta"]["active_profiles"] — массив строк.
      - data["profiles"] — массив объектов с ключом "profile_id" (гарантируется непустой).

    Args:
      config_id (str): Идентификатор конфига, используется для формирования диагностических сообщений.
      config_type (Any): Тип конфига (enum или строка), используется для сообщений и трассировки.
      data (dict[str, Any]): Объект конфига после этапов `_ConfigReader`, `_ConfigSanityCheck`, `_ConfigUniversalJsonSchemaValidation`.

    Returns:
      list[dict[str, Any]]: Новый список выбранных профилей (копии объектов), в порядке, заданном meta.active_profiles, либо [profiles[0]] если активных не задано.
    """
    def run(self, *, config_id: str, config_type: Any, data: dict[str, Any]) -> list[dict[str, Any]]:
        profiles = data.get("profiles", [])
        if not isinstance(profiles, list) or len(profiles) == 0:
            raise ConfigError(
                "PROFILE_ARRAY_EMPTY",
                f"Profiles array is empty for config '{config_id}' (type '{self._type_to_str(config_type)}')",
                json_pointer="/profiles",
            )

        meta = data.get("meta", {}) or {}
        active_ids: list[str] = meta.get("active_profiles", []) or []
        # Проверка уникальности profile_id в profiles
        self._check_unique_profile_ids(profiles, config_id, config_type)

        # Быстрый путь: если активные не заданы → первый профиль
        if len(active_ids) == 0:
            return [copy.deepcopy(profiles[0])]

        # Проверка дубликатов в active_profiles
        self._check_duplicates_in_active_ids(active_ids, config_id, config_type)
        # Индексация profiles по profile_id
        index: dict[str, dict[str, Any]] = {
            p["profile_id"]: p for p in profiles
        }

        # Сопоставление active_ids → profiles
        selected: list[dict[str, Any]] = []
        for idx, pid in enumerate(active_ids):
            prof = index.get(pid)
            if prof is None:
                raise ConfigError(
                    "PROFILE_NOT_FOUND",
                    (
                        f"Active profile_id '{pid}' not found in profiles "
                        f"for config '{config_id}' (type '{self._type_to_str(config_type)}')"
                    ),
                    json_pointer=f"/meta/active_profiles/{idx}",
                    profile_id=pid,
                )
            # Защита от побочных эффектов
            selected.append(copy.deepcopy(prof))

        return selected

    # --- helpers ---

    @staticmethod
    def _type_to_str(type_value: Any) -> str:
        if hasattr(type_value, "value"):
            return str(getattr(type_value, "value"))
        return str(type_value)

    @staticmethod
    def _check_duplicates_in_active_ids(active_ids: list[str], config_id: str, config_type: Any) -> None:
        """
        Проверяет дублирующиеся profile_id в meta.active_profiles.
        Бросает ConfigError(PROFILE_DUPLICATE) при первом повторе.
        """
        seen: set[str] = set()
        for idx, pid in enumerate(active_ids):
            if pid in seen:
                raise ConfigError(
                    "PROFILE_DUPLICATE",
                    (
                        f"Duplicate profile_id '{pid}' in meta.active_profiles "
                        f"for config '{config_id}' (type '{_ConfigActiveProfileSelector._type_to_str(config_type)}')"
                    ),
                    json_pointer=f"/meta/active_profiles/{idx}",
                    profile_id=pid,
                )
            seen.add(pid)

    @staticmethod
    def _check_unique_profile_ids(profiles: list[dict[str, Any]], config_id: str, config_type: Any) -> None:
        """
        Проверяет уникальность profile_id внутри массива profiles.
        Бросает ConfigError(PROFILE_DUPLICATE) при обнаружении повторяющегося profile_id.
        """
        seen: set[str] = set()
        for idx, profile in enumerate(profiles):
            pid = profile["profile_id"]
            if pid in seen:
                raise ConfigError(
                    "PROFILE_DUPLICATE",
                    (
                        f"Duplicate profile_id '{pid}' in profiles "
                        f"for config '{config_id}' (type '{_ConfigActiveProfileSelector._type_to_str(config_type)}')"
                    ),
                    json_pointer=f"/profiles/{idx}/profile_id",
                    profile_id=pid,
                )
            seen.add(pid)

#===============================================================================================================================================
#===============================================================================================================================================
#===============================================================================================================================================


class _ConfigSemanticInvariantsByType:
    """
    Семантические инварианты по типам конфигов.

    Ответственность:
      - Выполнить логические проверки, которые нецелесообразно кодировать в JSON Schema.
      - Работать только с активными профилями (результат `_ConfigActiveProfileSelector`).
      - Немедленно останавливать процесс при первом нарушении (fail-fast).

    Гарантии на входе:
      - Документ прошёл JSON Schema валидацию (`_ConfigUniversalJsonSchemaValidation`).
      - Профили выбраны и переданы в виде глубоких копий (не мутируем вход).

    Публичный API:
      run(*, config_id: str, config_type: Any, profiles: list[dict]) -> None
        - На успех: None
        - На нарушение: raise ConfigError(<SEMANTIC_*>, message, json_pointer=..., profile_id=...)
    """
    def __init__(self) -> None:
        self._checkers = {
            "auth": self._check_auth,
            "export": self._check_export,
            "logging": self._check_logging,
            "network": self._check_network,
            "site": self._check_site,
            # pipeline, url, meta → нет проверок
        }

    # ------------------------------ API ------------------------------

    def run(self, *, config_id: str, config_type: Any, profiles: list[dict]) -> None:
        """
        Выполняет проверки для всех активных профилей выбранного типа.

        Args:
            config_id: Идентификатор конфига (например, "network:default").
            config_type: Enum или строка ("network", "auth", ...).
            profiles: Активные профили (глубокие копии объектов профилей).

        Raises:
            ConfigError: Любая ошибка из семейства SEMANTIC_* (см. перечень E-7).
        """
        type_str = self._type_to_str(config_type)
        checker = self._checkers.get(type_str)
        if checker is None:
            return  # no rules for this type

        for idx, prof in enumerate(profiles):
            profile_id = prof["profile_id"]
            checker(
                profile=prof,
                profile_index=idx,
                profile_id=profile_id,
                config_id=config_id,
                config_type=type_str,
            )

    # --------------------------- AUTH ---------------------------

    def _check_auth(self, *, profile: dict[str, Any], profile_index: int, profile_id: str, config_id: str, config_type: str) -> None:
        """
        Правила для auth:
        - `_check_form` → обязательные `request.method`, `request.endpoint`, непустой `form_fields`.
        - `_check_csrf` → обязательные `source_url`, `token_selector`, `token_field_name`.
        - `_check_api_token" and api_token.enabled=true` → обязательные `token_header`, `token_value`.
        - `_check_success_checks` — наличие ключа `params` для `success_checks`.
        """
        # --- Вложенные проверки (приватные) ---
        def _check_form() -> None:
            request = profile.get("request", {})
            method = (request or {}).get("method")
            endpoint = (request or {}).get("endpoint")
            form_fields = profile.get("form_fields", [])

            if not method or not endpoint or not isinstance(form_fields, list) or len(form_fields) == 0:
                raise ConfigError(
                    "SEMANTIC_AUTH_FORM_MISSING_FIELDS",
                    "For auth_type='form' required: request.method, request.endpoint, non-empty form_fields.",
                    json_pointer=self._ptr(
                        "/profiles",
                        profile_index,
                        "request" if not method or not endpoint else "form_fields",
                    ),
                    profile_id=profile_id or None,
                )

        def _check_csrf() -> None:
            csrf = profile.get("csrf", {})
            if isinstance(csrf, dict) and bool(csrf.get("enabled", False)):
                source_url = csrf.get("source_url")
                token_selector = csrf.get("token_selector")
                token_field_name = csrf.get("token_field_name")

                if not source_url or not token_selector or not token_field_name:
                    raise ConfigError(
                        "SEMANTIC_AUTH_CSRF_INCOMPLETE",
                        "When csrf.enabled=true, required: source_url, token_selector, token_field_name.",
                        json_pointer=self._ptr("/profiles", profile_index, "csrf"),
                        profile_id=profile_id or None,
                    )

        def _check_api_token() -> None:
            api_token = profile.get("api_token", {})
            if isinstance(api_token, dict) and bool(api_token.get("enabled", False)):
                if not api_token.get("token_header") or not api_token.get("token_value"):
                    raise ConfigError(
                        "SEMANTIC_AUTH_TOKEN_INCOMPLETE",
                        "When api_token.enabled=true, required: token_header and token_value.",
                        json_pointer=self._ptr("/profiles", profile_index, "api_token"),
                        profile_id=profile_id or None,
                    )

        def _check_success_checks() -> None:
            success_checks = profile.get("success_checks")
            if isinstance(success_checks, list):
                for i, chk in enumerate(success_checks):
                    if not isinstance(chk, dict) or "params" not in chk:
                        raise ConfigError(
                            "SEMANTIC_AUTH_SUCCESSCHECK_NO_PARAMS",
                            "Each success_checks item must contain 'params'.",
                            json_pointer=self._ptr("/profiles", profile_index, "success_checks", i),
                            profile_id=profile_id or None,
                        )

        # --- Диспетчеризация правил ---
        auth_type = str(profile.get("auth_type", "")).strip()

        if auth_type == "form":
            _check_form()
        _check_csrf()
        _check_api_token()
        _check_success_checks()

    # -------------------------- EXPORT --------------------------

    def _check_export(self, *, profile: dict[str, Any], profile_index: int, profile_id: str, config_id: str, config_type: str) -> None:
        """
        Правила для export:
        - `_check_sheet_name_limits` - sheet_name_core_limit ≤ sheet_name_max_len ≤ 31.
        """
        # --- Вложенные проверки (приватные) ---
        def _check_sheet_name_limits() -> None:
            core_limit = profile.get("sheet_name_core_limit")
            max_len = profile.get("sheet_name_max_len")

            if core_limit is None or max_len is None or core_limit > max_len or max_len > 31:
                raise ConfigError(
                    "SEMANTIC_EXPORT_SHEET_LIMIT",
                    "sheet_name_core_limit ≤ sheet_name_max_len ≤ 31 is required.",
                    json_pointer=self._ptr("/profiles", profile_index),
                    profile_id=profile_id or None,
                )

        # --- Диспетчеризация правил ---
        _check_sheet_name_limits()

    # -------------------------- LOGGING -------------------------

    def _check_logging(self, *, profile: dict[str, Any], profile_index: int, profile_id: str, config_id: str, config_type: str) -> None:
        """
        Правила для logging:
        - `_check_ui_level_valid` - ui.level ∈ events.levels
        """
        # --- Вложенные проверки (приватные) ---
        def _check_ui_level_valid() -> None:
            ui = profile.get("ui", {})
            events = profile.get("events", {})
            level = (ui or {}).get("level")
            allowed = (events or {}).get("levels", [])

            if not isinstance(allowed, list) or level not in allowed:
                raise ConfigError(
                    "SEMANTIC_LOGGING_UI_LEVEL_INVALID",
                    f"ui.level '{level}' must be one of events.levels.",
                    json_pointer=self._ptr("/profiles", profile_index, "ui", "level"),
                    profile_id=profile_id or None,
                )
            
            if level not in allowed:
                raise ConfigError(
                    "SEMANTIC_LOGGING_UI_LEVEL_INVALID",
                    f"ui.level '{level}' must be one of {allowed}.",
                    json_pointer=self._ptr("/profiles", profile_index, "ui", "level"),
                    profile_id=profile_id or None,
                )

        # --- Диспетчеризация правил ---
        _check_ui_level_valid()

    # -------------------------- NETWORK -------------------------

    def _check_network(self, *, profile: dict[str, Any], profile_index: int, profile_id: str, config_id: str, config_type: str) -> None:
        """
        Правила для network:
        - `_check_rate_limit` - rate_limit.burst_size ≥ requests_per_second.
        - `_check_retry_policy` - retry_policy.timeout_budget_s ≥ сумма задержек с учётом `max_attempts`, `initial_delay_s`, `backoff_multiplier`, `max_delay_s`.
        - `_check_session_timeout` → session.request_timeout_s ≥ retry_policy.timeout_budget_s.
        """
        # --- Вложенные проверки (приватные) ---
        def _check_rate_limit() -> None:
            rl = profile.get("rate_limit", {})
            rps = (rl or {}).get("requests_per_second")
            burst = (rl or {}).get("burst_size")

            if burst is not None and rps is not None and float(burst) < float(rps):
                raise ConfigError(
                    "SEMANTIC_NETWORK_RATE_LIMIT",
                    "rate_limit.burst_size must be ≥ requests_per_second.",
                    json_pointer=self._ptr("/profiles", profile_index, "rate_limit", "burst_size"),
                    profile_id=profile_id or None,
                )

        def _check_retry_policy() -> float:
            rp = profile.get("retry_policy", {})
            if not isinstance(rp, dict):
                return 0.0

            try:
                budget = float(rp.get("timeout_budget_s"))
                attempts = int(rp.get("max_attempts"))
                initial = float(rp.get("initial_delay_s"))
                mult = float(rp.get("backoff_multiplier"))
                max_delay = float(rp.get("max_delay_s"))
            except (TypeError, ValueError):
                raise ConfigError(
                    "SEMANTIC_NETWORK_TIMEOUT_BUDGET",
                    "retry_policy fields must be numeric and present.",
                    json_pointer=self._ptr("/profiles", profile_index, "retry_policy"),
                    profile_id=profile_id or None,
                )

            if attempts <= 0 or initial < 0 or mult < 1 or max_delay < 0:
                raise ConfigError(
                    "SEMANTIC_NETWORK_TIMEOUT_BUDGET",
                    "Invalid retry_policy values (attempts>0, initial≥0, backoff_multiplier≥1, max_delay≥0).",
                    json_pointer=self._ptr("/profiles", profile_index, "retry_policy"),
                    profile_id=profile_id or None,
                )

            # --- прямой расчёт суммы экспоненциального бэкоффа ---
            total_delay = 0.0
            cur = initial
            for _ in range(attempts):
                total_delay += min(cur, max_delay)

                cur *= mult
                if cur > MAX_BACKOFF_DELAY_GUARD:
                    cur = MAX_BACKOFF_DELAY_GUARD

            # Проверка бюджета
            if budget + FLOAT_TOLERANCE < total_delay:
                raise ConfigError(
                    "SEMANTIC_NETWORK_TIMEOUT_BUDGET",
                    f"timeout_budget_s={budget:g} < required={total_delay:g} (sum of delays).",
                    json_pointer=self._ptr("/profiles", profile_index, "retry_policy", "timeout_budget_s"),
                    profile_id=profile_id or None,
                )

            return budget


        def _check_session_timeout(budget: float) -> None:
            session = profile.get("session", {})
            if not isinstance(session, dict) or session.get("request_timeout_s") is None:
                return

            try:
                req_timeout = float(session.get("request_timeout_s"))
            except (TypeError, ValueError):
                req_timeout = -1.0

            if req_timeout + FLOAT_TOLERANCE < budget:
                raise ConfigError(
                    "SEMANTIC_NETWORK_REQUEST_TIMEOUT_CONFLICT",
                    "session.request_timeout_s must be ≥ retry_policy.timeout_budget_s.",
                    json_pointer=self._ptr("/profiles", profile_index, "session", "request_timeout_s"),
                    profile_id=profile_id or None,
                )

        # --- Диспетчеризация правил ---
        _check_rate_limit()
        budget = _check_retry_policy()
        if budget > 0:
            _check_session_timeout(budget)


    # --------------------------- SITE ---------------------------

    def _check_site(self, *, profile: dict[str, Any], profile_index: int, profile_id: str, config_id: str, config_type: str) -> None:
        """
        Правила для site:
        - `_check_value_selector` - `extract="attr"` → поле `attr` обязательно и непустое.
        - `_check_page_container_selector` - `page_container_selector` обязателен и непустой.
        - `_check_item_containers` - `item_containers` и `fields` не пустые.
        - `_check_containers` - Уникальность `container_id` в пределах профиля.
        - `_check_fields` - Уникальность `field.name` в пределах контейнера.
        """
        # --- Вложенные проверки (приватные) ---
        def _check_page_container_selector() -> None:
            pcs = profile.get("page_container_selector")
            if not isinstance(pcs, str) or pcs.strip() == "":
                raise ConfigError(
                    "SEMANTIC_SITE_PAGE_CONTAINER_EMPTY",
                    "page_container_selector must be a non-empty string.",
                    json_pointer=self._ptr("/profiles", profile_index, "page_container_selector"),
                    profile_id=profile_id or None,
                )

        def _check_item_containers() -> list[dict]:
            item_containers = profile.get("item_containers")
            if not isinstance(item_containers, list) or len(item_containers) == 0:
                raise ConfigError(
                    "SEMANTIC_SITE_EMPTY_ARRAYS",
                    "item_containers must be a non-empty array.",
                    json_pointer=self._ptr("/profiles", profile_index, "item_containers"),
                    profile_id=profile_id or None,
                )
            return item_containers

        def _check_containers(item_containers: list[dict]) -> None:
            seen_cont_ids: set[str] = set()
            for c_idx, container in enumerate(item_containers):
                cid = str(container.get("container_id", ""))
                if cid in seen_cont_ids:
                    raise ConfigError(
                        "SEMANTIC_SITE_DUPLICATE_CONTAINER_ID",
                        f"Duplicate container_id '{cid}'.",
                        json_pointer=self._ptr(
                            "/profiles", profile_index, "item_containers", c_idx, "container_id"
                        ),
                        profile_id=profile_id or None,
                    )
                seen_cont_ids.add(cid)

                _check_fields(container, c_idx, cid)

        def _check_fields(container: dict, c_idx: int, cid: str) -> None:
            fields = container.get("fields")
            if not isinstance(fields, list) or len(fields) == 0:
                raise ConfigError(
                    "SEMANTIC_SITE_EMPTY_ARRAYS",
                    f"fields must be a non-empty array for container_id='{cid}'.",
                    json_pointer=self._ptr("/profiles", profile_index, "item_containers", c_idx, "fields"),
                    profile_id=profile_id or None,
                )

            seen_field_names: set[str] = set()
            for f_idx, field in enumerate(fields):
                fname = str(field.get("name", ""))
                if fname in seen_field_names:
                    raise ConfigError(
                        "SEMANTIC_SITE_DUPLICATE_FIELD_NAME",
                        f"Duplicate field.name '{fname}' in container_id='{cid}'.",
                        json_pointer=self._ptr(
                            "/profiles", profile_index, "item_containers", c_idx, "fields", f_idx, "name"
                        ),
                        profile_id=profile_id or None,
                    )
                seen_field_names.add(fname)

                _check_value_selector(field, c_idx, f_idx, cid)

        def _check_value_selector(field: dict, c_idx: int, f_idx: int, cid: str) -> None:
            vs = field.get("value_selector", {})
            if isinstance(vs, dict) and str(vs.get("extract", "")).strip() == "attr":
                attr = vs.get("attr")
                if not isinstance(attr, str) or attr.strip() == "":
                    raise ConfigError(
                        "SEMANTIC_SITE_ATTR_MISSING",
                        "value_selector.extract='attr' requires non-empty 'attr'.",
                        json_pointer=self._ptr(
                            "/profiles", profile_index, "item_containers", c_idx, "fields", f_idx, "value_selector"
                        ),
                        profile_id=profile_id or None,
                    )

        # --- Диспетчеризация подправил ---
        _check_page_container_selector()
        item_containers = _check_item_containers()
        _check_containers(item_containers)


    # -------------------------- Helpers --------------------------

    @staticmethod
    def _type_to_str(type_value: Any) -> str:
        """Enum/str → строковое значение."""
        if type_value is None:
            return ""
        if hasattr(type_value, "value"):
            return str(getattr(type_value, "value"))
        return str(type_value)

    @staticmethod
    def _ptr(base: str, *segments: Any) -> str:
        """
        Сборка JSON Pointer (RFC 6901) с экранированием '~' → '~0', '/' → '~1'.
        """
        def esc(x: Any) -> str:
            s = str(x)
            return s.replace("~", "~0").replace("/", "~1")

        parts: list[str] = []
        if base and base != "/":
            parts.extend([seg for seg in base.split("/") if seg])
        elif base == "/":
            pass
        for seg in segments:
            parts.append(esc(seg))
        return "/" + "/".join(parts)

#===============================================================================================================================================
#===============================================================================================================================================
#===============================================================================================================================================

# NOTE: class _ConfigPlaceholderFilling (интерполяция плейсхолдеров)
"""
 Этап 2.6 пайплайна был зарезервирован для обработки строк вида:
   - ${env:VAR}    → подстановка переменных окружения
   - ${secret:KEY} → подстановка секретов из внешних источников (например, streamlit.secrets)

 Однако, на текущем этапе развития проекта данный функционал признан
 нефункциональным и исключён из рабочей реализации:
   * Для пользовательских конфигов (редактируемых вручную) работа с секретами
     бессмысленна, так как требует дополнительной настройки окружения.
   * В проекте пока отсутствуют сервисы и сценарии, где секреты критичны.
   * Поддержка этого шага в текущем виде вела бы к избыточным зависимостям
     (os.environ, streamlit.secrets) и ухудшала тестируемость.

 Вместо этого принято решение:
   - Исключить класс _ConfigPlaceholderFilling из кода и пайплайна.
   - Отложить реализацию до будущей итерации, когда появится:
       • Разделение пользовательских и системных конфигов;
       • Гибкая логика управления секретами;
       • Отдельный модуль для работы с окружением и секретами,
         чтобы исключить прямые зависимости от внешних провайдеров.

 Таким образом, текущий пайплайн чтения и нормализации конфигов
 НЕ содержит этапа интерполяции плейсхолдеров. Документация по проекту
 отражает этот факт: шаг "2.6. Интерполяция плейсхолдеров" находится
 в состоянии "out of scope" (зарезервирован на будущее).
"""

#===============================================================================================================================================
#===============================================================================================================================================
#===============================================================================================================================================
