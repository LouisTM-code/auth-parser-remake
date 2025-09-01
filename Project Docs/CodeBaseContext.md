# CodeBaseContext

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
  │   ├── interface.py          # минималистичный интерфейс и управление пайплайном
  │   └── state.py        # `UIState` — централизованное состояние интерфейса Streamlit
  └── test/    # Тесты (unit, integration)
requirements.txt   # Python requirements
main.py            # Production entrypoint для Streamlit Cloud
```

---

## 2. Публичный API (YAML)

```yaml
app.app_logging.logbus:
  classes:
    LogBus:
      methods:
        info:
          signature: "(self, code: str, msg: str, context: Optional[Any] = None) -> None"
          description: ""
        warn:
          signature: "(self, code: str, msg: str, context: Optional[Any] = None) -> None"
          description: ""
        error:
          signature: "(self, code: str, msg: str, context: Optional[Any] = None) -> None"
          description: ""
        push:
          signature: "(self, event: LogEvent) -> None"
          description: "Неблокирующая публикация события"
        drain_batch:
          signature: "(self, max_items: Optional[int] = None) -> list[LogEvent]"
          description: "Забирает пачку событий без ожидания (non-blocking)"
        drain_batch_nowait:
          signature: "(self, max_items: Optional[int] = None) -> list[LogEvent]:"
          description: "Синхронная версия drain_batch, удобна для вызова из синхронного кода UI"
  internal_functions:
      _make_event: {}


app.core.utils_text:
  functions:
    clean_text:
      signature: "(text: str) -> str"
      description: "NBSP→space, trim, схлопывание пробельных последовательностей."
    normalize_price_to_float_or_na:
      signature: "(raw: str | None) -> float | Literal['NA']"
      description: "Извлекает число из ценовой строки; поддерживает ,/., сепараторы тысяч; некорректное → 'NA'."
    add_showall_params:
      signature: "(url: str) -> str"
      description: "Добавляет/заменяет в query параметры SHOWALL_1=1 и SHOWALL_3=1."
    normalize_and_dedupe_urls:
      signature: "(lines: Iterable[str]) -> list[str]"
      description: "Трим, пропуск пустых, добавление SHOWALL_*, дедупликация с сохранением порядка."


app.core.errors:
  description: "Модуль — Базовый класс PipelineError с полем code для мэппинга в логи"


app.core.models_and_specs:
  description: "Модуль — Единая «истина» по структурам данных"


app.export_io.writer:
  classes:
    XlsxWriterService:
      methods:
        write:
          signature: "(self, groups: list[dict[str, Any]]) -> str"
          description: "Экспортирует сгруппированные данные в XLSX; по листу на каждую группу."
  internal_functions:
    _normalize_rows: {}
    _infer_headers: {}
    _make_unique_sheet_name: {}
    _fit_to_limit: {}
    _to_str_for_width: {}


app.net.auth:
  classes:
    BaseAuthAdapter:
      methods:
        login:
          signature: "(self, session: SessionManager) -> AuthResult"
          description: "Абстрактный метод авторизации через SessionManager."
    FormAuthAdapter:
      methods:
        login:
          signature: "(self, session: SessionManager) -> AuthResult"
          description: "POST-форма авторизации; помечает сессию аутентифицированной при успехе."
  constants:
      _BROWSER_EXTRAS: "Final[dict[str, str]]"


app.net.session_and_fetcher:
  classes:
    SessionManager:
      methods:
        default_headers:
          signature: "(self) -> dict[str, str]"
          description: "Возвращает копию базовых заголовков клиента."
        mark_authenticated:
          signature: "(self, value: bool = True) -> None"
          description: "Устанавливает флаг успешной аутентификации."
        is_authenticated:
          signature: "(self) -> bool"
          description: "Проверяет флаг успешной аутентификации."
        get:
          signature: "(self, url: str, *, headers: Optional[dict[str, str]] = None, max_retries: int = 2, retry_backoff_base: float = 0.3, acceptable_statuses: tuple[int, ...] = (200,)) -> httpx.Response"
          description: "Асинхронный GET с ручными ретраями и проверкой статуса."
        post:
          signature: "(self, url: str, *, data: dict | None = None, headers: Optional[dict[str, str]] = None, max_retries: int = 1, retry_backoff_base: float = 0.3, acceptable_statuses: tuple[int, ...] = (200,)) -> httpx.Response"
          description: "Асинхронный POST с ручными ретраями и проверкой статуса."
        close:
          signature: "(self) -> None"
          description: "Закрывает внутренний httpx.AsyncClient."
    PageFetcher:
      methods:
        fetch_many:
          signature: "(self, urls: Iterable[str]) -> list[FetchedPage]"
          description: "Конкурентно загружает набор URL, возвращает список FetchedPage."
  constants:
      _DEFAULT_UA: "Final[str]"
  internal_functions:
    _fetch_one: {}


app.parsing.extractor:
  classes:
    ProductExtractor:
      methods:
        extract:
          signature: "(self, html: str, *, task_id: int) -> tuple[list[ProductRecord], list[ParseIssue], str]"
          description: "Парсит HTML в пределах контейнеров карточек; устраняет дубликаты по уникальным полям."
  internal_functions:
    _find_card_containers: {}
    _build_unique_key: {}
    _extract_in_container: {}
    _find_spec_index_by_name: {}


app.parsing.normalizer:
  classes:
    PriceNormalizer:
      methods:
        normalize:
          signature: "(self, products: list[ProductRecord]) -> list[ProductRecord]"
          description: "Выполняет нормализацию цен и прочих полей по заданным правилам."
  internal_functions:
    _compile_actions: {}
    _t_default_clean: {}
    _t_price_to_float: {}
    _t_mark_supplier: {}


app.pipeline.runner:
  classes:
    ParserPipeline:
      methods:
        run:
          signature: "(self, urls: Iterable[str]) -> None"
          description: "Асинхронный запуск полного цикла: login → fetch → parse → normalize → export."
  internal_functions:
    _batched: {}
    _fetch_one_with_timeout: {}
    _is_stop_and_cancel_pending: {}
    _is_stop_and_handle_before_export: {}
    _safe_export_partial: {}
    _dedupe_keep_order: {}
    _ensure_not_stopped: {}


app.ui.interface:
  description: "Модуль — точка входа Streamlit UI. Не содержит внешнего API; все функции внутренние."
  entrypoint: true
  constants:
    AUTH_EMAIL: "str"
    AUTH_PASSWORD: "str"
    BATCH_SIZE: "int"
    CONCURRENCY: "int"
    FETCH_TIMEOUT_S: "float"
    LOG_POLL_INTERVAL_MS: "int"
  internal_functions:
    _init_singletons: {}
    _get_worker_thread: {}
    _set_worker_thread: {}
    _start_pipeline_in_background: {}
    _append_logs_to_buffer: {}
    _render_logs: {}
    _read_urls_from_text: {}


app.ui.state:
  classes:
    UIStatus:
      kind: "enum"
    UIState:
      methods:
        reset:
          signature: "(self) -> None"
          description: "Сбрасывает состояние до значений по умолчанию."
        begin_task:
          signature: "(self, total: int = 0, task_name: Optional[str] = None) -> None"
          description: "Начинает новую задачу с полным сбросом состояния."
        end_task:
          signature: "(self, success: bool, xlsx_path: Optional[str] = None) -> None"
          description: "Завершает задачу, устанавливая статус FINISHED или ERROR."
        set_total:
          signature: "(self, total: int) -> None"
          description: "Устанавливает общее количество единиц работы."
        inc_done:
          signature: "(self, delta: int = 1) -> None"
          description: "Увеличивает количество выполненных единиц работы."
        set_done:
          signature: "(self, done: int) -> None"
          description: "Прямо задаёт количество выполненной работы."
        set_status:
          signature: "(self, status: UIStatus) -> None"
          description: "Устанавливает статус пайплайна."
        request_stop:
          signature: "(self) -> None"
          description: "Запрашивает остановку пайплайна пользователем."
        clear_stop:
          signature: "(self) -> None"
          description: "Сбрасывает флаг остановки."
        add_error:
          signature: "(self, code: Optional[str] = None, *, critical: bool = True) -> None"
          description: "Регистрирует ошибку; в счётчик идут только критические."
        progress_ratio:
          signature: "(self) -> float"
          description: "Возвращает долю прогресса 0.0–1.0."
        as_dict:
          signature: "(self) -> dict"
          description: "Сериализует состояние в словарь."
  functions:
    ensure_in_session:
      signature: "() -> UIState"
      description: "Гарантирует наличие UIState в st.session_state и возвращает его."
    get_state:
      signature: "() -> UIState"
      description: "Возвращает UIState из st.session_state (создаёт при отсутствии)."
    reset_state:
      signature: "() -> UIState"
      description: "Сбрасывает UIState в st.session_state."
    update_state:
      signature: "(fn) -> UIState"
      description: "Применяет функцию-мутацию к UIState и возвращает его."
```

---

## 3. Ключевые структуры данных (Python с комментариями)

```python
@dataclass(slots=True, frozen=True)
class LogEvent:
    """
    Событие лога для отображения в UI.
    Расположен в `app.app_logging.logbus`
    Attributes:
        ts: строка времени в формате HH:MM:SS (локальное время).
        level: "INFO" | "WARN" | "ERROR".
        code: короткий код события (например, STAGE_START, FETCH_DONE, ERR_HTTP_STATUS).
        msg: человекочитаемое сообщение.
        context: произвольный контекст (URL, task_id, словарь полей и т.п.).
    """
    ts: str
    level: LogLevel
    code: str
    msg: str
    context: Optional[Any] = None


class ErrorCode(StrEnum):
    """
    Расположен в `app.core.errors`
    """
    ERR_LOGIN_FAILED = "ERR_LOGIN_FAILED"
    ERR_HTTP_STATUS = "ERR_HTTP_STATUS"
    ERR_TIMEOUT = "ERR_TIMEOUT"
    ERR_NETWORK = "ERR_NETWORK"
    ERR_ENCODING = "ERR_ENCODING"
    ERR_STOP_REQUESTED = "ERR_STOP_REQUESTED"
    ERR_UNEXPECTED = "ERR_UNEXPECTED"


class ExtractType(StrEnum):
    """
    Расположен в `app.core.models_and_specs`
    Тип извлечения значения из DOM-элемента.
    - text: текстовое содержимое узла
    - attr: значение атрибута (вместе с FieldSpec.attr)
    """
    TEXT = "text"
    ATTR = "attr"


@dataclass(slots=True, frozen=True)
class SelectorVariant:
    """
    Расположен в `app.core.models_and_specs`
    Описание одного варианта поиска значения для поля.
    selector: CSS-селектор
    extract: тип извлечения (text/attr)
    attr: атрибут, если extract=attr
    """
    selector: str
    extract: ExtractType
    attr: Optional[str] = None


@dataclass(slots=True, frozen=True)
class NormalizeRules:
    """
    Расположен в `app.core.models_and_specs`
    Правила нормализации значения поля.
    Attributes:
        tools: Набор идентификаторов инструментов нормализации.
        supplier_id: Внешнее условие (идентификатор поставщика)
    """
    tools: Optional[list[str]] = None
    supplier_id: Optional[int] = None


@dataclass(slots=True, frozen=True)
class FieldSpec:
    """
    Расположен в `app.core.models_and_specs`
    Спецификация поля для извлечения.
    Может содержать несколько альтернативных вариантов (SelectorVariant),
    которые будут проверяться по порядку до первого успеха.
    Attributes:
        name: Заголовок столбца.
        selectors: Список вариантов селекторов/извлечения для поля.
        is_unique: Участвует ли поле в построении ключа уникальности товара.
        normalize: Список правил нормализации, обрабатываемых parsing.normalizer.
    """
    name: str
    selectors: list[SelectorVariant]
    is_unique: bool = False
    normalize: list[NormalizeRules] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class ContainerSpecs:
    """
    Расположен в `app.core.models_and_specs`
    Спецификация контейнеров карточек товаров.
    selectors: Список CSS-селекторов контейнеров карточек. Можно передать несколько вариантов для совместимости с разными версиями вёрстки. Приоритет — по порядку. Если список пуст — парсер выполняет fallback-логику (определяет границы карточки по ближайшим предкам якоря внутри global-контейнера).
    """
    selectors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ProductRecord:
    """
    Расположен в `app.core.models_and_specs`
    Строка результата для одного товара.
    Связанно с FieldSpec.
    Замечание:
        Служебные поля (URL, статусы и т.п.) не входят в итоговую таблицу.
        Их следует хранить отдельно в других структурах, если потребуется.
    """
    Товар: str | Literal["NA"]
    Оптовая_цена: str | float | Literal["NA"]
    Артикул: str | Literal["NA"]
    Наличие: str | Literal["NA"]
    Розничная_цена: str | float | Literal["NA"]
    def to_ordered_values(self) -> list[str | float]:
        """
        Возвращает значения в фиксированном порядке колонок, соответствующем FIELD_SPECS (см. ниже).
        """
        return [
            self.Товар,
            self.Оптовая_цена,
            self.Артикул,
            self.Наличие,
            self.Розничная_цена,
        ]


@dataclass(slots=True, frozen=True)
class PageTask:
    """
    Расположен в `app.core.models_and_specs`
    Задача на загрузку листинга.
    Attributes:
        id: Внутренний идентификатор задачи (для логов/трассировки).
        url: Оригинальный URL из ввода пользователя.
        normalized_url: URL с гарантированными параметрами SHOWALL_*.
    """
    id: int
    url: str
    normalized_url: str


@dataclass(slots=True)
class ParseIssue:
    """
    Расположен в `app.core.models_and_specs`
    Описание проблемы парсинга (не падение конвейера, а диагностическая запись).
    Attributes:
        task_id: Идентификатор PageTask.
        field_name: Имя поля (как в FieldSpec.name), по которому произошла проблема.
        code: Короткий код проблемы (например, 'ERR_PARSE_MISSING_FIELD').
        details: Текстовое описание/контекст.
    """
    task_id: int
    field_name: str
    code: str
    details: str = field(default_factory=str)


@dataclass(slots=True, frozen=True)
class AuthConfig:
    """
    Расположен в `app.net.auth`
    Конфигурация для формы логина.
    Attributes:
        email: Логин (email).
        password: Пароль.
        login_url: Абсолютный или относительный URL точки входа.
    """
    email: str
    password: str
    login_url: str = "https://cnc1.ru/auth/?login=yes"


@dataclass(slots=True, frozen=True)
class AuthResult:
    """
    Расположен в `app.net.auth`
    Результат авторизации.
    Attributes:
        ok: Признак успеха.
        message: Краткое текстовое описание итога (для логов/UI).
    """
    ok: bool
    message: str = ""


@dataclass(slots=True, frozen=True)
class SessionConfig:
    """
    Расположен в `app.net.session_and_fetcher`
    Конфигурация HTTP-сессии.
    Attributes:
        base_url: Базовый URL (можно оставить пустым).
        connect_timeout_s: Таймаут установки соединения.
        read_timeout_s: Таймаут чтения ответа.
        max_connections: Максимум одновременных соединений в пуле.
        max_keepalive_connections: Максимум keep-alive соединений.
        http2: Включение HTTP/2.
        default_headers: Базовые заголовки клиента (User-Agent и т.д.).
    """
    base_url: str = ""
    connect_timeout_s: float = 5.0
    read_timeout_s: float = 10.0
    max_connections: int = 64
    max_keepalive_connections: int = 20
    http2: bool = True
    default_headers: Mapping[str, str] | None = None


@dataclass(slots=True, frozen=True)
class FetchedPage:
    """
    Расположен в `app.net.session_and_fetcher`
    Результат загрузки одной страницы.
    Attributes:
        url: Запрашиваемый URL (уже с SHOWALL_*).
        status: HTTP-статус ответа (или None при сетевом исключении).
        text: Текст ответа (None при ошибке статуса или исключении).
        error: Исключение (если было); не выбрасывается наружу при mode='collect'.
    """
    url: str
    status: Optional[int]
    text: Optional[str]
    error: Optional[Exception] = None


@dataclass(slots=True, frozen=True)
class ExtractorConfig:
    """
    Расположен в `app.parsing.extractor`
    Настройки извлечения.
    """
    # Глобальный контейнер страницы
    page_container_selector: str = "div.inner_wrapper"
    # Отсутствие оптовой цены не считать ошибкой
    treat_wholesale_missing_as_error: bool = False


@dataclass(slots=True)
class PipelineConfig:
    """
    Расположен в `app.pipeline.runner`
    Настройки пайплайна.
    Attributes:
        batch_size: Размер партии URL, обрабатываемых одновременно.
        concurrency: Глобальный предел параллелизма PageFetcher (делегируется ему).
        fetch_timeout_s: Таймаут на ПАРАЛЛЕЛЬНУЮ загрузку *одного URL* (обёртка вокруг вызова fetcher для одной ссылки). Позволяет не зависнуть на долгих запросах.
    """
    batch_size: int = 10
    concurrency: int = 24
    fetch_timeout_s: float = 25.0


class UIStatus(StrEnum):
    """
    Расположен в `app.ui.state`
    Допустимые статусы жизненного цикла пайплайна.
    """
    IDLE = "idle"
    RUNNING = "running"
    STOPPED = "stopped"
    FINISHED = "finished"
    ERROR = "error"


@dataclass(slots=True)
class UIState:
    """
    Расположен в `app.ui.state`
    Состояние UI для Streamlit.
    Поля:
        progress_total: целевое кол-во единиц работы (страниц/шагов).
        progress_done: выполненное кол-во.
        status: текущее состояние пайплайна.
        errors_count: число критических ошибок.
        xlsx_path: путь к результирующему XLSX (или None).
        stop_requested: флаг запроса остановки пользователем.
        task_name: опциональное имя текущей задачи (для UI).
        started_at: unix-время старта текущей задачи (или 0).
        finished_at: unix-время завершения (или 0).
    """
    progress_total: int = 0
    progress_done: int = 0
    status: UIStatus = UIStatus.IDLE
    errors_count: int = 0
    xlsx_path: Optional[str] = None
    stop_requested: bool = False
    task_name: Optional[str] = None
    started_at: float = 0.0
    finished_at: float = 0.0
```
