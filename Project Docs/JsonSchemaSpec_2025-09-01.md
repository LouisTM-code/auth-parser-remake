# 📑 Спецификация Markdown-документа JSON Schema

## Введение
Этот документ служит **источником истины** по актуальным JSON Schema проекта.
JSON Schema лежать в папках 
Ограничения:
- Содержимое схем переносится **без интерпретаций** и **без пересериализации**.
- Вне JSON-блоков выносится только минимальная техническая информация (Schema Passport).
Формат:
- Каждая схема — отдельная глава.
- Схемы содержат контекст в "description" и "$comment".
- Блок "examples" в конце схемы содержим примеры json реализации.

JSON Schema лежат относительно коревого проекта `auth-parser-remake`):
- `auth-parser-remake/manifest/v1`
- `auth-parser-remake/configs/v1`

## 1. Auth Config (v1-min) — urn:auth-parser:config:v1:auth

### Schema Passport
- `$id`: `urn:auth-parser:config:v1:auth`
- `title`: `Auth Config (v1-min)`
- `required`: `meta`, `profiles`
- `$defs`: `api_token_spec`, `auth_request`, `csrf_spec`, `profile`, `success_check`

### Полный JSON Schema
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "urn:auth-parser:config:v1:auth",
  "title": "Auth Config (v1-min)",
  "description": "meta + profiles[]. В v1 поддерживаются ветки form и api_token. Выбор профилей через meta.active_profiles.",
  "type": "object",
  "additionalProperties": false,
  "required": ["meta", "profiles"],
  "properties": {
    "meta": {
      "$ref": "./meta.schema.json",
      "description": "Общие метаданные профиля."
    },
    "profiles": {
      "type": "array",
      "description": "Если meta.active_profiles пуст — используется первый элемент.",
      "default": [],
      "items": { "$ref": "#/$defs/profile" }
    }
  },

  "$defs": {
    "profile": {
      "type": "object",
      "title": "AuthProfile",
      "additionalProperties": false,
      "required": ["profile_id", "auth_type"],
      "properties": {
        "profile_id": { "type": "string", "description": "Уникальный ID профиля." },
        "auth_type": {
          "type": "string",
          "description": "Поддерживаемые ветки v1.",
          "enum": ["form", "api_token"]
        },

        "request": {
          "$ref": "#/$defs/auth_request",
          "description": "HTTP-запрос логина. Требуется только для form.",
          "default": null
        },

        "form_fields": {
          "type": ["object", "null"],
          "description": "Ключи формы для form. Значения допускают ${env:...}/${secret:...}.",
          "default": null,
          "additionalProperties": { "type": "string" }
        },

        "csrf": {
          "$ref": "#/$defs/csrf_spec",
          "description": "CSRF для form.",
          "default": { "enabled": false }
        },

        "success_checks": {
          "type": "array",
          "description": "Критерии успеха (AND). Минимальный набор v1.",
          "default": [],
          "items": { "$ref": "#/$defs/success_check" }
        },

        "api_token": {
          "$ref": "#/$defs/api_token_spec",
          "description": "Настройка токена для ветки api_token.",
          "default": { "enabled": false }
        }
      }
    },

    "auth_request": {
      "type": "object",
      "title": "AuthRequest",
      "additionalProperties": false,
      "required": ["method", "endpoint"],
      "properties": {
        "method": {
          "type": "string",
          "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"],
          "description": "HTTP-метод."
        },
        "endpoint": {
          "type": "string",
          "description": "Абсолютный URL точки входа.",
          "minLength": 1
        },
        "headers": {
          "type": "object",
          "description": "Доп. заголовки.",
          "default": {},
          "additionalProperties": { "type": "string" }
        },
        "timeout_s": {
          "type": ["number", "null"],
          "description": "Локальный таймаут логина. null — брать из network.",
          "default": null
        },
        "redirect_policy": {
          "type": "string",
          "enum": ["follow", "error", "ignore"],
          "description": "Поведение при 3xx.",
          "default": "follow"
        }
      }
    },

    "csrf_spec": {
      "type": "object",
      "title": "CsrfSpec",
      "additionalProperties": false,
      "required": ["enabled"],
      "properties": {
        "enabled": { "type": "boolean", "description": "Включить шаг CSRF." },
        "source_url": { "type": ["string", "null"], "default": null, "description": "Откуда брать токен." },
        "token_selector": { "type": ["string", "null"], "default": null, "description": "CSS-селектор токена." },
        "token_field_name": { "type": ["string", "null"], "default": null, "description": "Имя поля формы для токена." }
      }
    },

    "success_check": {
      "type": "object",
      "title": "SuccessCheck",
      "additionalProperties": false,
      "required": ["check_type", "params"],
      "properties": {
        "check_type": {
          "type": "string",
          "enum": ["status_code", "header_present", "body_not_contains"],
          "description": "Минимальный набор v1."
        },
        "params": {
          "type": "object",
          "description": "Форма зависит от check_type; валидируется в ConfigLoader.",
          "default": {},
          "additionalProperties": true
        }
      }
    },

    "api_token_spec": {
      "type": "object",
      "title": "ApiTokenSpec",
      "additionalProperties": false,
      "required": ["enabled"],
      "properties": {
        "enabled": { "type": "boolean", "description": "Включить ветку api_token." },
        "token_header": {
          "type": ["string", "null"],
          "default": null,
          "description": "Имя заголовка (например, Authorization)."
        },
        "token_value": {
          "type": ["string", "null"],
          "default": null,
          "description": "Значение токена (${env:API_TOKEN}/${secret:...})."
        }
      }
    }
  },

  "examples": [
    {
      "meta": {
        "pipeline_step": "auth",
        "config_id": "auth:demo",
        "version": "1.0.0",
        "description": "Демо профиль авторизации",
        "tags": ["demo", "auth"],
        "active_profiles": []
      },
      "profiles": [
        {
          "profile_id": "auth:demo:form",
          "auth_type": "form",
          "request": {
            "method": "POST",
            "endpoint": "https://example.com/auth/login",
            "headers": { "Content-Type": "application/x-www-form-urlencoded" }
          },
          "form_fields": {
            "USER_LOGIN": "${env:AUTH_EMAIL}",
            "USER_PASSWORD": "${env:AUTH_PASSWORD}"
          },
          "csrf": { "enabled": true, "source_url": "https://example.com/login", "token_selector": "input[name='sessid']", "token_field_name": "sessid" },
          "success_checks": [
            { "check_type": "status_code", "params": { "allowed": [200, 302] } },
            { "check_type": "header_present", "params": { "key": "Set-Cookie" } },
            { "check_type": "body_not_contains", "params": { "patterns": ["ошибка"] } }
          ]
        },
        {
          "profile_id": "auth:demo:token",
          "auth_type": "api_token",
          "api_token": { "enabled": true, "token_header": "Authorization", "token_value": "${env:API_TOKEN}" }
        }
      ]
    }
  ]
}
```

## 2. Export Config (v1) — urn:auth-parser:config:v1:export

### Schema Passport
- `$id`: `urn:auth-parser:config:v1:export`
- `title`: `Export Config (v1)`
- `required`: `meta`, `profiles`
- `$defs`: `export_profile`

### Полный JSON Schema
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "urn:auth-parser:config:v1:export",
  "title": "Export Config (v1)",
  "description": "Параметры экспорта результатов (XLSX). Единый формат: meta + profiles[]. Выбор активных профилей — через meta.active_profiles. many_id не используется.",
  "type": "object",
  "additionalProperties": false,
  "required": ["meta", "profiles"],
  "properties": {
    "meta": {
      "$ref": "./meta.schema.json",
      "description": "Общие метаданные профиля (pipeline_step, config_id, version, tags, active_profiles)."
    },
    "profiles": {
      "type": "array",
      "description": "Список профилей экспорта. Если meta.active_profiles пуст — используется первый профиль.",
      "items": { "$ref": "#/$defs/export_profile" },
      "default": []
    }
  },
  "$defs": {
    "export_profile": {
      "title": "Export Profile",
      "type": "object",
      "additionalProperties": false,
      "required": [
        "profile_id",
        "filename_pattern",
        "sheet_name_max_len",
        "sheet_name_core_limit",
        "multiple_sheets",
        "sheet_styles",
        "empty_sheet_name"
      ],
      "properties": {
        "profile_id": {
          "type": "string",
          "description": "Уникальный ID профиля для трассировки.",
          "minLength": 1
        },

        "filename_pattern": {
          "type": "string",
          "description": "Шаблон имени файла. Поддерживаемые плейсхолдеры: {date}=YYYYMMDD, {time}=HHMM, {site}=site_id, {profile}=profile_id. Неподдерживаемые токены игнорируются/не подставляются."
        },

        "sheet_name_max_len": {
          "type": "integer",
          "description": "Максимальная длина имени листа (Excel hard limit = 31).",
          "minimum": 1,
          "maximum": 31
        },

        "sheet_name_core_limit": {
          "type": "integer",
          "description": "Длина «ядра» имени без служебных суффиксов (для коллизий).",
          "minimum": 1,
          "maximum": 31
        },

        "multiple_sheets": {
          "type": "boolean",
          "description": "Разбивать результат на несколько листов (например, по site_id)."
        },

        "sheet_styles": {
          "type": "object",
          "description": "Оформление таблиц в XLSX.",
          "additionalProperties": false,
          "required": ["header", "cell", "freeze_header", "autowidth"],
          "properties": {
            "header": {
              "type": "object",
              "additionalProperties": false,
              "required": [],
              "properties": {
                "bold": { "type": "boolean", "description": "Жирный шрифт." },
                "align": {
                  "type": "string",
                  "description": "Горизонтальное выравнивание.",
                  "enum": ["left", "center", "right", "justify"]
                },
                "valign": {
                  "type": "string",
                  "description": "Вертикальное выравнивание.",
                  "enum": ["top", "vcenter", "bottom"]
                },
                "bg_color": {
                  "type": "string",
                  "description": "Цвет фона (например, #F2F2F2)."
                },
                "font_color": {
                  "type": "string",
                  "description": "Цвет текста (например, #000000)."
                },
                "border": {
                  "type": "integer",
                  "description": "Толщина границ (0..3).",
                  "minimum": 0,
                  "maximum": 3
                },
                "text_wrap": {
                  "type": "boolean",
                  "description": "Перенос по словам."
                }
              }
            },
            "cell": {
              "type": "object",
              "additionalProperties": false,
              "required": [],
              "properties": {
                "align": {
                  "type": "string",
                  "enum": ["left", "center", "right", "justify"],
                  "description": "Горизонтальное выравнивание."
                },
                "valign": {
                  "type": "string",
                  "enum": ["top", "vcenter", "bottom"],
                  "description": "Вертикальное выравнивание."
                },
                "border": {
                  "type": "integer",
                  "minimum": 0,
                  "maximum": 3,
                  "description": "Толщина границ (0..3)."
                },
                "text_wrap": {
                  "type": "boolean",
                  "description": "Перенос по словам."
                }
              }
            },
            "freeze_header": {
              "type": "boolean",
              "description": "Заморозить первую строку."
            },
            "autowidth": {
              "type": "boolean",
              "description": "Автоподбор ширины колонок."
            }
          }
        },

        "empty_sheet_name": {
          "type": "string",
          "description": "Имя листа при отсутствии данных.",
          "minLength": 1
        }
      },

      "$comment": "Перекрёстная проверка sheet_name_core_limit ≤ sheet_name_max_len выполняется в ConfigLoader (см. требования). Также Loader нормализует имена листов под ограничения Excel и разрешает коллизии суффиксами."
    }
  },

  "examples": [
    {
      "meta": {
        "pipeline_step": "export",
        "config_id": "export:xlsx-default",
        "version": "1.0.0",
        "description": "Экспорт XLSX по умолчанию",
        "tags": ["xlsx", "default"],
        "active_profiles": []
      },
      "profiles": [
        {
          "profile_id": "export:xlsx-default",
          "filename_pattern": "results_{site}_{date}_{time}.xlsx",
          "sheet_name_max_len": 31,
          "sheet_name_core_limit": 28,
          "multiple_sheets": true,
          "sheet_styles": {
            "header": {
              "bold": true,
              "align": "center",
              "valign": "vcenter",
              "bg_color": "#F2F2F2",
              "font_color": "#000000",
              "border": 1,
              "text_wrap": true
            },
            "cell": {
              "align": "left",
              "valign": "top",
              "border": 1,
              "text_wrap": false
            },
            "freeze_header": true,
            "autowidth": true
          },
          "empty_sheet_name": "Empty"
        }
      ]
    }
  ],

  "$comment": "Fail-fast проверки массива profiles (пустой массив, отсутствующие profile_id из active_profiles) — ответственность ConfigLoader согласно правилам проекта. $ref указывает на локальный ./meta.schema.json; убедитесь, что URN всех схем уникален и ссылки разрешимы."
}
```

## 3. Logging Config (v1) — urn:auth-parser:config:v1:logging

### Schema Passport
- `$id`: `urn:auth-parser:config:v1:logging`
- `title`: `Logging Config (v1)`
- `required`: `meta`, `profiles`
- `$defs`: `events_policy`, `log_level`, `logbus_spec`, `logging_profile`, `ui_spec`

### Полный JSON Schema
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "urn:auth-parser:config:v1:logging",
  "title": "Logging Config (v1)",
  "description": "Схема профиля логирования: LogBus, UI-параметры и политика событий. Универсальная форма: meta + profiles[]. Выбор активных профилей выполняется через meta.active_profiles.",
  "type": "object",
  "additionalProperties": false,
  "required": ["meta", "profiles"],
  "properties": {
    "meta": {
      "$ref": "./meta.schema.json",
      "description": "Общие метаданные профиля (pipeline_step, config_id, version и пр.)."
    },
    "profiles": {
      "type": "array",
      "description": "Набор вариантов профиля логирования.",
      "items": { "$ref": "#/$defs/logging_profile" },
      "default": []
    }
  },

  "$defs": {
    "log_level": {
      "type": "string",
      "enum": ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"],
      "description": "Допустимый уровень логирования."
    },

    "logging_profile": {
      "type": "object",
      "additionalProperties": false,
      "required": ["profile_id", "logbus", "ui", "events"],
      "properties": {
        "profile_id": {
          "type": "string",
          "description": "Уникальный ID профиля для трассировки.",
          "minLength": 1
        },
        "logbus": {
          "$ref": "#/$defs/logbus_spec",
          "description": "Параметры очереди событий и drain-поведения."
        },
        "ui": {
          "$ref": "#/$defs/ui_spec",
          "description": "Параметры отображения логов в UI."
        },
        "events": {
          "$ref": "#/$defs/events_policy",
          "description": "Политика уровней и агрегации событий."
        }
      }
    },

    "logbus_spec": {
      "type": "object",
      "additionalProperties": false,
      "required": ["queue_max_size", "drain_batch_limit", "drop_oldest"],
      "properties": {
        "queue_max_size": {
          "type": "integer",
          "minimum": 1,
          "description": "Максимальная ёмкость очереди событий."
        },
        "drain_batch_limit": {
          "type": "integer",
          "minimum": 1,
          "description": "Максимум событий, извлекаемых за один цикл обработки."
        },
        "drop_oldest": {
          "type": "boolean",
          "description": "Стратегия при переполнении: true — удалять старые, false — отклонять новые."
        }
      }
    },

    "ui_spec": {
      "type": "object",
      "additionalProperties": false,
      "required": ["log_buffer_max_lines", "batch_pull", "render_limit", "poll_interval_ms", "level"],
      "properties": {
        "log_buffer_max_lines": {
          "type": "integer",
          "minimum": 1,
          "description": "Размер кольцевого буфера отображения."
        },
        "batch_pull": {
          "type": "integer",
          "minimum": 1,
          "description": "Сколько событий UI запрашивает за один тик."
        },
        "render_limit": {
          "type": "integer",
          "minimum": 1,
          "description": "Максимум строк, выводимых за раз."
        },
        "poll_interval_ms": {
          "type": "integer",
          "minimum": 50,
          "description": "Интервал опроса LogBus UI-слоем, мс (рекомендуется ≥ 50)."
        },
        "level": {
          "$ref": "#/$defs/log_level",
          "description": "Минимальный уровень отображения в UI."
        }
      }
    },

    "events_policy": {
      "type": "object",
      "additionalProperties": false,
      "required": ["levels", "aggregate_batch_summary", "include_context"],
      "properties": {
        "levels": {
          "type": "array",
          "description": "Белый список допустимых уровней входящих событий.",
          "items": { "$ref": "#/$defs/log_level" },
          "default": ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
        },
        "aggregate_batch_summary": {
          "type": "boolean",
          "description": "Включает агрегацию однотипных событий в сводки."
        },
        "include_context": {
          "type": "boolean",
          "description": "Добавлять в событие контекст (task_id, url и т.д.)."
        }
      },
      "$comment": "Семантическая проверка согласованности: ui.level ∈ events.levels и обработка недопустимых уровней выполняется в ConfigLoader (fail-fast)."
    }
  },

  "examples": [
    {
      "meta": {
        "pipeline_step": "logging",
        "config_id": "logging:default",
        "version": "1.0.0",
        "description": "Демонстрационный профиль логирования",
        "tags": ["default"],
        "active_profiles": []
      },
      "profiles": [
        {
          "profile_id": "logging:default",
          "logbus": { "queue_max_size": 2000, "drain_batch_limit": 200, "drop_oldest": true },
          "ui": { "log_buffer_max_lines": 2000, "batch_pull": 200, "render_limit": 2000, "poll_interval_ms": 500, "level": "INFO" },
          "events": { "levels": ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"], "aggregate_batch_summary": true, "include_context": true }
        }
      ]
    }
  ]
}
```

## 4. Network Config (v1) — urn:auth-parser:config:v1:network

### Schema Passport
- `$id`: `urn:auth-parser:config:v1:network`
- `title`: `Network Config (v1)`
- `required`: `meta`, `profiles`
- `$defs`: `network_profile`, `network_session`, `rate_limit`, `retry_policy`

### Полный JSON Schema
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "urn:auth-parser:config:v1:network",
  "title": "Network Config (v1)",
  "description": "Сетевой профиль: HTTP-сессия, политика ретраев и ограничение скорости. Единственный формат для всех файлов v1: meta + profiles[].",
  "type": "object",
  "additionalProperties": false,
  "required": ["meta", "profiles"],
  "properties": {
    "meta": {
      "$ref": "./meta.schema.json",
      "description": "Общие метаданные профиля (pipeline_step, config_id, version, tags, active_profiles)."
    },
    "profiles": {
      "type": "array",
      "description": "Список профилей. Выбор активных — через meta.active_profiles. Пусто → используется первый профиль.",
      "default": [],
      "items": { "$ref": "#/$defs/network_profile" }
    }
  },

  "$defs": {
    "network_profile": {
      "title": "NetworkProfile",
      "type": "object",
      "additionalProperties": false,
      "required": ["profile_id", "concurrency", "session", "retry_policy", "rate_limit"],
      "properties": {
        "profile_id": {
          "type": "string",
          "description": "Идентификатор профиля, уникальный внутри файла."
        },
        "concurrency": {
          "type": "integer",
          "description": "Максимум конкурентных запросов в сетевом слое.",
          "minimum": 1,
          "default": 24
        },
        "session": {
          "$ref": "#/$defs/network_session",
          "description": "Параметры HTTP-сессии клиента."
        },
        "retry_policy": {
          "$ref": "#/$defs/retry_policy",
          "description": "Глобальные правила повторов запросов."
        },
        "rate_limit": {
          "$ref": "#/$defs/rate_limit",
          "description": "Ограничение скорости (RPS/бурст/джиттер)."
        }
      }
    },

    "network_session": {
      "title": "NetworkSession",
      "type": "object",
      "additionalProperties": false,
      "required": [
        "connect_timeout_s",
        "read_timeout_s",
        "write_timeout_s",
        "max_connections",
        "max_keepalive_connections",
        "http2",
        "default_headers",
        "verify_tls"
      ],
      "properties": {
        "base_url": {
          "type": ["string", "null"],
          "description": "Базовый URL; null — использовать абсолютные URL.",
          "default": null
        },
        "connect_timeout_s": {
          "type": "number",
          "minimum": 0,
          "default": 5.0
        },
        "read_timeout_s": {
          "type": "number",
          "minimum": 0,
          "default": 10.0
        },
        "write_timeout_s": {
          "type": "number",
          "minimum": 0,
          "default": 10.0
        },
        "request_timeout_s": {
          "type": ["number", "null"],
          "description": "End-to-end таймаут одной попытки запроса; null — использовать поведение клиента по умолчанию.",
          "minimum": 0,
          "default": null
        },
        "max_connections": {
          "type": "integer",
          "minimum": 1,
          "default": 64
        },
        "max_keepalive_connections": {
          "type": "integer",
          "minimum": 0,
          "default": 20
        },
        "http2": {
          "type": "boolean",
          "default": true
        },
        "default_headers": {
          "type": "object",
          "description": "Базовые заголовки клиента (str→str).",
          "additionalProperties": { "type": "string" },
          "default": {
            "User-Agent": "Mozilla/5.0 (compatible; ParserBot/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "ru-RU,ru;q=0.9",
            "Cache-Control": "no-cache"
          }
        },
        "verify_tls": {
          "type": "boolean",
          "default": true
        },
        "ca_path": {
          "type": ["string", "null"],
          "description": "Кастомный CA bundle; null — системный.",
          "default": null
        }
      },
      "$comment": "Кросс-проверки вроде согласования request_timeout_s с retry_policy.timeout_budget_s выполняет ConfigLoader (fail-fast), не JSON Schema."
    },

    "retry_policy": {
      "title": "RetryPolicy",
      "type": "object",
      "additionalProperties": false,
      "required": [
        "max_attempts",
        "initial_delay_s",
        "backoff_multiplier",
        "max_delay_s",
        "timeout_budget_s",
        "retry_on_status",
        "retry_on_exceptions_category"
      ],
      "properties": {
        "max_attempts": {
          "type": "integer",
          "minimum": 1,
          "default": 3
        },
        "initial_delay_s": {
          "type": "number",
          "minimum": 0,
          "default": 0.3
        },
        "backoff_multiplier": {
          "type": "number",
          "minimum": 1.0,
          "default": 2.0
        },
        "max_delay_s": {
          "type": "number",
          "minimum": 0,
          "default": 10.0
        },
        "timeout_budget_s": {
          "type": "number",
          "minimum": 0,
          "default": 30.0
        },
        "retry_non_idempotent": {
          "type": ["boolean", "null"],
          "description": "Разрешать ли ретраи для неидемпотентных методов (POST/PUT/PATCH). По умолчанию запрещено.",
          "default": false
        },
        "retry_on_status": {
          "type": "array",
          "description": "Коды/маски статусов, при которых выполняется повтор (например, 500, 502, 429, \"5xx\").",
          "items": { "type": ["integer", "string"] },
          "default": ["5xx", 429]
        },
        "retry_on_exceptions_category": {
          "type": "array",
          "description": "Категории исключений для ретраев.",
          "items": {
            "type": "string",
            "enum": ["timeout", "network", "connection"]
          },
          "default": ["timeout", "network", "connection"]
        }
      },
      "$comment": "Проверка, что timeout_budget_s покрывает сумму попыток с учётом задержек backoff — обязанность ConfigLoader (fail-fast)."
    },

    "rate_limit": {
      "title": "RateLimit",
      "type": "object",
      "additionalProperties": false,
      "required": ["enabled", "requests_per_second", "burst_size", "jitter_s"],
      "properties": {
        "enabled": {
          "type": "boolean",
          "default": true
        },
        "requests_per_second": {
          "type": "integer",
          "minimum": 1,
          "default": 10
        },
        "burst_size": {
          "type": "integer",
          "minimum": 1,
          "default": 20
        },
        "jitter_s": {
          "type": "number",
          "minimum": 0,
          "default": 0.5
        }
      },
      "$comment": "Проверка burst_size ≥ requests_per_second переносится в ConfigLoader (fail-fast)."
    }
  },

  "examples": [
    {
      "meta": {
        "pipeline_step": "network",
        "config_id": "network:default",
        "version": "1.0.0",
        "description": "Сетевой профиль по умолчанию",
        "tags": ["prod"],
        "active_profiles": []
      },
      "profiles": [
        {
          "profile_id": "network:default",
          "concurrency": 24,
          "session": {
            "base_url": null,
            "connect_timeout_s": 5.0,
            "read_timeout_s": 10.0,
            "write_timeout_s": 10.0,
            "request_timeout_s": null,
            "max_connections": 64,
            "max_keepalive_connections": 20,
            "http2": true,
            "default_headers": {
              "User-Agent": "Mozilla/5.0 (compatible; ParserBot/1.0)",
              "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
              "Accept-Encoding": "gzip, deflate, br",
              "Accept-Language": "ru-RU,ru;q=0.9",
              "Cache-Control": "no-cache"
            },
            "verify_tls": true,
            "ca_path": null
          },
          "retry_policy": {
            "max_attempts": 3,
            "initial_delay_s": 0.3,
            "backoff_multiplier": 2.0,
            "max_delay_s": 10.0,
            "timeout_budget_s": 30.0,
            "retry_non_idempotent": false,
            "retry_on_status": ["5xx", 429],
            "retry_on_exceptions_category": ["timeout", "network", "connection"]
          },
          "rate_limit": {
            "enabled": true,
            "requests_per_second": 10,
            "burst_size": 20,
            "jitter_s": 0.5
          }
        }
      ]
    }
  ]
}
```

## 5. Pipeline Config (v1) — urn:auth-parser:config:v1:pipeline

### Schema Passport
- `$id`: `urn:auth-parser:config:v1:pipeline`
- `title`: `Pipeline Config (v1)`
- `required`: `meta`, `profiles`
- `$defs`: отсутствуют

### Полный JSON Schema
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "urn:auth-parser:config:v1:pipeline",
  "title": "Pipeline Config (v1)",
  "description": "Параметры оркестрации пайплайна. Единая форма: meta + profiles[].",
  "type": "object",
  "additionalProperties": false,
  "required": ["meta", "profiles"],
  "properties": {
    "meta": {
      "$ref": "./meta.schema.json",
      "description": "Общие метаданные профиля (pipeline_step, config_id, version, active_profiles)."
    },
    "profiles": {
      "type": "array",
      "description": "Набор профилей. Если meta.active_profiles пуст — используется первый профиль.",
      "default": [],
      "items": {
        "type": "object",
        "title": "PipelineProfile",
        "description": "Профиль настроек пайплайна.",
        "additionalProperties": false,
        "required": ["profile_id", "batch_size"],
        "properties": {
          "profile_id": {
            "type": "string",
            "description": "Уникальный ID профиля для трассировки.",
            "minLength": 1
          },
          "batch_size": {
            "type": "integer",
            "description": "Размер логической партии URL на итерацию пайплайна (≥1).",
            "minimum": 1
          }
        }
      }
    }
  },
  "examples": [
    {
      "meta": {
        "pipeline_step": "pipeline",
        "config_id": "pipeline:default",
        "version": "1.0.0",
        "description": "Параметры батчей/конкурентности",
        "tags": ["default"],
        "active_profiles": []
      },
      "profiles": [
        {
          "profile_id": "pipeline:default",
          "batch_size": 10
        }
      ]
    }
  ]
}
```

## 6. Site Config (v1) — urn:auth-parser:config:v1:site

### Schema Passport
- `$id`: `urn:auth-parser:config:v1:site`
- `title`: `Site Config (v1)`
- `required`: `meta`, `profiles`
- `$defs`: `field_spec`, `item_container`, `normalize_step`, `site_profile`, `value_selector`

### Полный JSON Schema
```json
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "urn:auth-parser:config:v1:site",
    "title": "Site Config (v1)",
    "description": "Декларативные правила извлечения и нормализации данных со страниц сайта. Унифицированная форма: { meta, profiles[] }.",
    "type": "object",
    "additionalProperties": false,
    "required": [
        "meta",
        "profiles"
    ],
    "properties": {
        "meta": {
            "$ref": "./meta.schema.json",
            "description": "Общие метаданные профиля (pipeline_step, config_id, version и пр.)."
        },
        "profiles": {
            "type": "array",
            "description": "Набор профилей сайта. Активный выбирается через meta.active_profiles; если пусто — используется первый.",
            "items": {
                "$ref": "#/$defs/site_profile"
            },
            "default": []
        }
    },
    "$defs": {
        "site_profile": {
            "title": "SiteProfile",
            "type": "object",
            "additionalProperties": false,
            "required": [
                "profile_id",
                "page_container_selector",
                "item_containers"
            ],
            "properties": {
                "profile_id": {
                    "type": "string",
                    "description": "Уникальный ID профиля для трассировки.",
                    "minLength": 1
                },
                "page_container_selector": {
                    "type": "string",
                    "description": "CSS-селектор корневого контейнера страницы; ограничивает область поиска.",
                    "minLength": 1
                },
                "item_containers": {
                    "type": "array",
                    "description": "Список деклараций контейнеров элементов (карточки, строки таблиц и т.д.).",
                    "items": {
                        "$ref": "#/$defs/item_container"
                    },
                    "default": []
                }
            }
        },
        "item_container": {
            "title": "ItemContainer",
            "type": "object",
            "additionalProperties": false,
            "required": [
                "container_id",
                "item_selectors",
                "fields"
            ],
            "properties": {
                "container_id": {
                    "type": "string",
                    "description": "Уникальный ID контейнера в рамках профиля.",
                    "minLength": 1
                },
                "item_selectors": {
                    "type": "array",
                    "description": "Один или несколько CSS-селекторов, определяющих карточку/строку. Порядок важен.",
                    "items": {
                        "type": "string"
                    },
                    "default": []
                },
                "fields": {
                    "type": "array",
                    "description": "Список полей, извлекаемых из элемента.",
                    "items": {
                        "$ref": "#/$defs/field_spec"
                    },
                    "default": []
                }
            }
        },
        "field_spec": {
            "title": "FieldSpec",
            "type": "object",
            "additionalProperties": false,
            "required": [
                "name",
                "value_selectors"
            ],
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Имя поля (уникально в рамках контейнера).",
                    "minLength": 1
                },
                "is_unique": {
                    "type": "boolean",
                    "description": "Участвует ли поле в построении ключа уникальности.",
                    "default": false
                },
                "missing_as_error": {
                    "type": "boolean",
                    "description": "Если true и поле не извлечено — фиксируется ошибка.",
                    "default": true
                },
                "value_selectors": {
                    "type": "array",
                    "description": "Стратегии извлечения значения; выполняются по порядку до первого успеха.",
                    "items": {
                        "$ref": "#/$defs/value_selector"
                    },
                    "default": []
                },
                "normalize_pipeline": {
                    "type": "array",
                    "description": "Последовательность инструментов нормализации.",
                    "items": {
                        "$ref": "#/$defs/normalize_step"
                    },
                    "default": []
                }
            }
        },
        "value_selector": {
            "title": "ValueSelector",
            "type": "object",
            "additionalProperties": false,
            "required": [
                "selector",
                "extract"
            ],
            "properties": {
                "selector": {
                    "type": "string",
                    "description": "CSS-селектор внутри элемента (item_selector) для поиска значения.",
                    "minLength": 1
                },
                "extract": {
                    "type": "string",
                    "description": "Способ извлечения значения.",
                    "enum": [
                        "text",
                        "attr"
                    ]
                },
                "attr": {
                    "type": "string",
                    "description": "Имя атрибута (нужно, когда extract = \"attr\"). Оставлено опциональным — проверяется в ConfigLoader.",
                    "minLength": 1
                }
            }
        },
        "normalize_step": {
            "title": "NormalizeStep",
            "type": "object",
            "additionalProperties": false,
            "required": [
                "tool"
            ],
            "properties": {
                "tool": {
                    "type": "string",
                    "description": "Имя инструмента нормализации (должно существовать в TOOL_REGISTRY).",
                    "minLength": 1
                },
                "params": {
                    "type": "object",
                    "description": "Параметры шага; произвольные ключи для конкретного инструмента.",
                    "additionalProperties": true,
                    "default": {}
                }
            }
        }
    },
    "examples": [
        {
            "meta": {
                "pipeline_step": "site",
                "config_id": "site:cnc_demo",
                "version": "1.1.0",
                "description": "Демо конфиг для сайта",
                "tags": [
                    "demo"
                ],
                "active_profiles": []
            },
            "profiles": [
                {
                    "profile_id": "site:cnc_demo",
                    "page_container_selector": "div.inner_wrapper",
                    "item_containers": [
                        {
                            "container_id": "cards",
                            "item_selectors": [
                                "div.catalog_item",
                                "li.product-card"
                            ],
                            "fields": [
                                {
                                    "name": "Товар",
                                    "value_selectors": [
                                        {
                                            "selector": ".item-title a",
                                            "extract": "text"
                                        },
                                        {
                                            "selector": ".card-name",
                                            "extract": "text"
                                        }
                                    ],
                                    "normalize_pipeline": [
                                        {
                                            "tool": "clean_text"
                                        }
                                    ]
                                },
                                {
                                    "name": "Артикул",
                                    "is_unique": true,
                                    "value_selectors": [
                                        {
                                            "selector": ".article",
                                            "extract": "text"
                                        },
                                        {
                                            "selector": "[data-sku]",
                                            "extract": "attr",
                                            "attr": "data-sku"
                                        }
                                    ],
                                    "normalize_pipeline": []
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ],
    "$comment": "Условные зависимости (например, обязательность 'attr' при extract='attr'), уникальность ID и минимальные размеры массивов — проверяются в ConfigLoader (fail-fast), чтобы сохранить дружелюбность к генерации dataclass DTO.⚠️ Логическая зависимость: поле 'attr' должно быть обязательным, если extract='attr'. Эта проверка выполняется в ConfigLoader. ⚠️ Уникальность: profile_id в профиле, container_id в контейнерах и field.name в полях должны быть уникальными. Это проверяет ConfigLoader. ⚠️ Минимальные размеры: item_containers и fields должны содержать хотя бы один элемент. В схеме разрешены пустые массивы (default: []), но ConfigLoader выполняет fail-fast проверку."
}
```

## 7. URL Config (v1) — urn:auth-parser:config:v1:url

### Schema Passport
- `$id`: `urn:auth-parser:config:v1:url`
- `title`: `URL Config (v1)`
- `required`: `meta`, `profiles`
- `$defs`: `url_condition`, `url_profile`, `url_rule`

### Полный JSON Schema
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "urn:auth-parser:config:v1:url",
  "title": "URL Config (v1)",
  "description": "Правила детерминированной трансформации URL до сетевого запроса. Единая форма: meta + profiles[]. Выбор активных профилей — через meta.active_profiles.",
  "type": "object",
  "additionalProperties": false,
  "required": ["meta", "profiles"],
  "properties": {
    "meta": {
      "$ref": "./meta.schema.json",
      "description": "Общие метаданные профиля (pipeline_step, config_id, version и пр.)."
    },
    "profiles": {
      "type": "array",
      "description": "Список профилей URL-трансформаций. Если meta.active_profiles пуст — используется первый профиль.",
      "items": { "$ref": "#/$defs/url_profile" },
      "default": []
    }
  },

  "$defs": {
    "url_profile": {
      "title": "UrlProfile",
      "type": "object",
      "additionalProperties": false,
      "required": ["profile_id", "rules"],
      "properties": {
        "profile_id": {
          "type": "string",
          "description": "Уникальный идентификатор профиля в рамках файла."
        },
        "rules": {
          "type": "array",
          "description": "Упорядоченный список правил. Порядок имеет значение.",
          "items": { "$ref": "#/$defs/url_rule" },
          "default": []
        }
      }
    },

    "url_rule": {
      "title": "UrlRule",
      "type": "object",
      "additionalProperties": false,
      "required": ["condition", "action"],
      "properties": {
        "condition": {
          "$ref": "#/$defs/url_condition",
          "description": "Все указанные условия должны быть истинны (логика AND)."
        },
        "action": {
          "type": "string",
          "description": "Операция трансформации URL.",
          "enum": ["add_params", "remove_params", "normalize_slash", "replace_host", "strip_fragment"]
        },
        "params": {
          "description": "Параметры операции (generic). Конкретная форма интерпретируется модулем-потребителем (UrlTransformer).",
          "type": ["object", "array", "null"],
          "default": {}
        },
        "priority": {
          "type": "integer",
          "description": "Чем меньше, тем раньше применяется правило. При равенстве — порядок в массиве.",
          "default": 100
        }
      }
    },

    "url_condition": {
      "title": "UrlCondition",
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "host": {
          "type": "string",
          "description": "Совпадение netloc (без схемы/порта), например: \"example.com\"."
        },
        "path_regex": {
          "type": "string",
          "description": "Регулярное выражение для URL path (Python-совместимое). Валидность и компиляция — на стороне потребителя."
        },
        "has_param": {
          "type": "string",
          "description": "Истина, если указанный ключ параметра присутствует в query."
        },
        "scheme": {
          "type": "string",
          "description": "Схема URL.",
          "enum": ["http", "https"]
        }
      }
    }
  },

  "examples": [
    {
      "meta": {
        "pipeline_step": "url",
        "config_id": "url:demo",
        "version": "1.0.0",
        "description": "Демонстрационный профиль URL-трансформации",
        "tags": ["demo"],
        "active_profiles": []
      },
      "profiles": [
        {
          "profile_id": "profile:prod",
          "rules": [
            {
              "condition": { "host": "cnc1.ru", "path_regex": "^/catalog/.+" },
              "action": "add_params",
              "params": [
                { "key": "SHOWALL_1", "value": "1", "overwrite": true },
                { "key": "SHOWALL_3", "value": "1", "overwrite": true }
              ],
              "priority": 10
            },
            {
              "condition": { "host": "cnc1.ru", "has_param": "page" },
              "action": "remove_params",
              "params": [
                { "key": "page" },
                { "key": "utm_source" }
              ],
              "priority": 20
            },
            {
              "condition": { "host": "cnc1.ru" },
              "action": "normalize_slash",
              "params": { "mode": "ensure" },
              "priority": 30
            }
          ]
        },
        {
          "profile_id": "profile:test",
          "rules": [
            {
              "condition": { "host": "test.cnc1.ru" },
              "action": "replace_host",
              "params": { "new_host": "staging.cnc1.ru" },
              "priority": 5
            },
            {
              "condition": { "path_regex": "^/search/" },
              "action": "strip_fragment",
              "params": {},
              "priority": 15
            }
          ]
        }
      ]
    }
  ],

  "$comment": "Требования проекта: 1) profiles всегда массив; 2) выбор активных профилей — только через meta.active_profiles; 3) пустые/несуществующие profile_id и другие fail-fast проверки — ответственность ConfigLoader; 4) Поле params — generic (object|array|null); семантика и валидация по action полностью на стороне модуля-потребителя (UrlTransformer)."
}
```

## 8. Config Meta (v1) — urn:auth-parser:defs:v1:meta

### Schema Passport
- `$id`: `urn:auth-parser:defs:v1:meta`
- `title`: `Config Meta (v1)`
- `required`: `pipeline_step`, `config_id`, `version`
- `$defs`: отсутствуют

### Полный JSON Schema
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "urn:auth-parser:defs:v1:meta",
  "title": "Config Meta (v1)",
  "description": "Общие метаданные профилей конфигурации. Единый формат для всех типов профилей. Выбор активных профилей осуществляется через meta.active_profiles.",
  "type": "object",
  "additionalProperties": false,
  "required": ["pipeline_step", "config_id", "version"],
  "properties": {
    "pipeline_step": {
      "type": "string",
      "description": "Тип профиля/этап конвейера.",
      "enum": ["pipeline", "site", "url", "network", "auth", "logging", "export"]
    },
    "config_id": {
      "type": "string",
      "description": "Стабильный идентификатор конфига (namespace:name). Должен совпадать с тем, что указан в манифесте.",
      "minLength": 1
    },
    "version": {
      "type": "string",
      "description": "Версия профиля в формате SemVer.",
      "pattern": "^\\d+\\.\\d+\\.\\d+$"
    },
    "description": {
      "type": "string",
      "description": "Человекочитаемое описание профиля.",
      "default": ""
    },
    "tags": {
      "type": "array",
      "description": "Произвольные метки (не влияют на валидацию/исполнение).",
      "items": { "type": "string" },
      "default": []
    },
    "active_profiles": {
      "type": "array",
      "description": "Список profile_id, которые должны быть активированы. Если пусто — используется первый профиль из массива profiles.",
      "items": { "type": "string" },
      "default": []
    }
  },
  "examples": [
    {
      "pipeline_step": "network",
      "config_id": "network:default",
      "version": "1.0.0",
      "description": "Сетевой профиль по умолчанию",
      "tags": ["prod"],
      "active_profiles": []
    }
  ]
}
```

## 9. ConfigManifest v1 — urn:auth-parser:manifest:v1

### Schema Passport
- `$id`: `urn:auth-parser:manifest:v1`
- `title`: `ConfigManifest v1`
- `required`: `meta`, `configs`
- `$defs`: `config_entry`, `config_type`, `path`, `semver`, `version_constraint`

### Полный JSON Schema
```json
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "urn:auth-parser:manifest:v1",
    "title": "ConfigManifest v1",
    "description": "JSON Schema для манифеста конфигураций. Источник правды для списка профилей и их местоположения. Используется в ConfigLoader до нормализации/DTO.",
    "type": "object",
    "additionalProperties": false,
    "required": [
        "meta",
        "configs"
    ],
    "properties": {
        "meta": {
            "type": "object",
            "description": "Метаданные манифеста",
            "additionalProperties": false,
            "required": [
                "manifest_id",
                "version",
                "created_at"
            ],
            "properties": {
                "manifest_id": {
                    "type": "string",
                    "pattern": "^manifest:[A-Za-z0-9._:-]+$",
                    "description": "Стабильный ID манифеста (например, manifest:cnc1-default)"
                },
                "version": {
                    "$ref": "#/$defs/semver",
                    "description": "Версия самого манифеста (SemVer)"
                },
                "created_at": {
                    "type": "string",
                    "format": "date-time",
                    "description": "Момент создания в ISO 8601 (UTC)"
                },
                "description": {
                    "type": "string",
                    "default": "",
                    "description": "Человекочитаемое описание"
                },
                "tags": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "default": []
                }
            }
        },
        "configs": {
            "type": "array",
            "minItems": 1,
            "items": {
                "$ref": "#/$defs/config_entry"
            },
            "$comment": "Проверка наличия всех обязательных типов выполняется в бизнес-логике ConfigLoader; схема гарантирует форму и значения."
        }
    },
    "$defs": {
        "semver": {
            "type": "string",
            "pattern": "^\\d+\\.\\d+\\.\\d+$",
            "description": "Строгий SemVer: MAJOR.MINOR.PATCH"
        },
        "version_constraint": {
            "type": "string",
            "pattern": "^(?:\\d+\\.\\d+\\.\\d+|\\d+\\.x|\\d+\\.\\d+\\.x)$",
            "description": "Ожидаемая версия профиля: точная (1.2.3) или с подстановками по правилам: '1.x' либо '1.2.x'"
        },
        "config_type": {
            "type": "string",
            "enum": [
                "pipeline",
                "site",
                "url",
                "network",
                "auth",
                "logging",
                "export"
            ],
            "description": "Тип подключаемого профиля"
        },
        "path": {
            "type": "string",
            "minLength": 1,
            "pattern": "\\.json$",
            "description": "Путь к JSON-файлу профиля относительно рабочего каталога или абсолютный"
        },
        "config_entry": {
            "type": "object",
            "additionalProperties": false,
            "required": [
                "type",
                "config_id",
                "path",
                "expected_version"
            ],
            "$comment": "Детальное соответствие (type=pipeline ⇒ config_id^=pipeline:) переносим в ConfigLoader как бизнес-проверку.",
            "properties": {
                "type": {
                    "$ref": "#/$defs/config_type"
                },
                "config_id": {
                    "type": "string",
                    "minLength": 3,
                    "pattern": "^[a-z]+:[A-Za-z0-9._:-]+$",
                    "description": "Стабильный ID профиля в формате <type>:<name> (например, network:default)"
                },
                "path": {
                    "$ref": "#/$defs/path"
                },
                "expected_version": {
                    "$ref": "#/$defs/version_constraint"
                }
            }
        }
    },
    "examples": [
        {
            "meta": {
                "manifest_id": "manifest:cnc1-default",
                "version": "1.0.0",
                "created_at": "2025-08-22T12:00:00Z",
                "description": "Манифест для пайплайна"
            },
            "configs": [
                {
                    "type": "pipeline",
                    "config_id": "pipeline:default",
                    "path": "configs/pipeline_default.json",
                    "expected_version": "1.0.0"
                },
                {
                    "type": "site",
                    "config_id": "site:cnc1",
                    "path": "configs/sites/cnc1.json",
                    "expected_version": "1.0.0"
                },
                {
                    "type": "url",
                    "config_id": "url:cnc1",
                    "path": "configs/urls/cnc1.json",
                    "expected_version": "1.0.0"
                },
                {
                    "type": "network",
                    "config_id": "network:default",
                    "path": "configs/network_default.json",
                    "expected_version": "1.0.0"
                },
                {
                    "type": "auth",
                    "config_id": "auth:cnc1",
                    "path": "configs/auth/cnc1.json",
                    "expected_version": "1.0.0"
                },
                {
                    "type": "logging",
                    "config_id": "logging:default",
                    "path": "configs/logging_default.json",
                    "expected_version": "1.0.0"
                },
                {
                    "type": "export",
                    "config_id": "export:default",
                    "path": "configs/export_xlsx.json",
                    "expected_version": "1.0.0"
                }
            ]
        }
    ],
    "$comment": "Уникальность $id проверяется вне схемы (линтером). Уникальность типов в массиве configs и полнота набора типов контролируются в ConfigLoader."
}
```
