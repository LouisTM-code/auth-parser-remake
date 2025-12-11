# Спецификация обновления Pipeline

## 1. Новая логика относительно старой

**Старая схема:**

* `UI (interface.py)` напрямую вызывает `ParserPipeline.run(urls)`.
* `ParserPipeline` внутри себя создаёт и инициализирует все модули (SessionManager, Fetcher, Auth, Extractor, Normalizer, Export, LogBus).
* Логика смешана: инициализация + исполнение.

**Новая схема:**

* `UI` вызывает **только** `PipelineLauncher.launch(manifest_path, urls, ui_state)`.
* `PipelineLauncher` отвечает за:
  1. Загрузку конфигураций через `ConfigLoader` (валидация, fail-fast проверки).
  2. Создание экземпляров всех сервисов на основе DTO.
  3. Формирование контейнера зависимостей `PipelineModulesContext`.
  4. Передачу этого пакета в `ParserPipeline`.
* `ParserPipeline` теперь занимается **только исполнением бизнес-логики** (login → transform → fetch → extract → normalize → export).
* Исключения и fail-fast проверки выполняются в `PipelineLauncher` до момента инициализации `LogBus`. После его создания все события логируются централизованно.

**Методы и подходы:**

* Dependency Injection (DI): DTO → сервисы.
* Fail-fast: проверки согласованности вынесены в `ConfigLoader`.
* Контейнер зависимостей: `PipelineModulesContext` как единый объект для передачи в `ParserPipeline`.
* Исключения как способ сигнализации об ошибках на этапе конфигурации.

**Преимущества:**

* Чистое разделение обязанностей (инициализация vs. исполнение).
* Упрощённое тестирование: можно мокать `PipelineModulesContext`.
* Гибкость: изменения конфигов не требуют переписывать `ParserPipeline`.
* Масштабируемость: новые сервисы и этапы подключаются через DTO без изменения бизнес-ядра.

---

## 2. Пошаговое описание схемы новой части pipeline

**Этапы:**

1. **UI (interface.py)**
   
   * Передаёт `manifest_path`, список URL и `ui_state` в `PipelineLauncher.launch(...)`.
   * Не работает напрямую с `ParserPipeline` и модулями.

2. **PipelineLauncher**
   
   * Создаёт `ConfigLoader(manifest_path)`.
   * Загружает и валидирует все профили конфигурации (`*ConfigV1`).
   * Формирует объекты сервисов:
     * `SessionManager`, `PageFetcher` из `NetworkConfigV1`.
     * `AuthAdapter` из `AuthConfigV1Min`.
     * `UrlTransformer` из `UrlConfigV1`.
     * `ProductExtractor`, `PriceNormalizer` из `SiteConfigV1`.
     * `XlsxWriterService` из `ExportConfigV1`.
     * `LogBus` из `LoggingConfigV1`.
   * Оборачивает всё в `PipelineModulesContext`.
   * Передаёт пакет в `ParserPipeline`.

3. **ParserPipeline**
   
   * Получает `PipelineModulesContext`.
   * Управляет последовательностью шагов:
     1. Авторизация (AuthAdapter).
     2. Трансформация URL (UrlTransformer).
     3. Загрузка страниц (PageFetcher).
     4. Извлечение данных (ProductExtractor).
     5. Нормализация (PriceNormalizer).
     6. Экспорт (XlsxWriterService).
   * Все ошибки и события фиксируются в `LogBus`.

---

## 3. Допущения и ограничения

* `ParserPipeline` обновляется:
  * Лишается обязанности создавать экземпляры сервисов.
  * Принимает только один аргумент — `PipelineModulesContext`.
* `PipelineLauncher` выполняет все fail-fast проверки через `ConfigLoader`.
* Исключения на уровне `PipelineLauncher` выбрасываются до инициализации `LogBus`. После его создания используется только централизованное логирование.
* UI не имеет прямого доступа к сервисам пайплайна, кроме статуса через `ui_state` и логов через `LogBus`.
* Схема актуальна только для v1 конфигов и DTO, дальнейшие версии потребуют адаптации `ConfigLoader`.
