# Цель обновления

Обеспечить модульность и гибкость извлечения данных из HTML, убрав зависимость от фиксированных пяти полей и глобальных констант. Экстрактор должен возвращать «сырые» значения по именам полей, а вся нормализация переносится в `PriceNormalizer`. Конфигурация полей и контейнеров выносится во внешний JSON‑профиль (с версионированием), подключаемый через DI.

## Проблемы (как есть)

1. **Жёсткая пятёрка полей**: `ProductExtractor` собирает `ProductRecord` по индексам первых пяти `FIELD_SPECS`. Добавление новых полей требует правок кода.

2. **Глобальные константы**: извлечение и контейнеры завязаны на `FIELD_SPECS` и `CONTAINER_SPECS` из Python‑модулей. Под другой сайт/версию приходится менять код.

3. **Смешение обязанностей**: экстрактор выполняет частичную нормализацию (например, цен), дублируя логику `PriceNormalizer`.

## Область применения

Изменения затрагивают:

* `app/parsing/extractor.py` — формат выхода, отказ от индексов, DI‑конфиг, optional trace.

* `app/parsing/normalizer.py` — работа с `dict[str, Any]` (или адаптер), единая точка нормализации.

* `app/pipeline/runner.py` — прокидка профиля полей/контейнеров и передача `task_id`.

* `app/core/models_and_specs.py` — сохранение обратной совместимости, добавление загрузки из JSON‑профиля (через новый loader).

* `app/export_io/writer.py` — без изменений по API (принимает `dict`), только регрессионные тесты.

## Объяснить новую концепцию работы через json конфиг

> **Концепция:** все сведения об извлекаемых полях, селекторах контейнеров и правилах нормализации описываются во внешнем **JSON‑профиле сайта**. Профиль имеет версию схемы (`schema_version`) и может различаться для разных сайтов/вариантов вёрстки.

**Мини‑схема (упрощённая):**
```json
    {
      "schema_version": "1.0",
      "containers": {
        "selectors": ["div.inner_wrapper", "section.catalog"]
      },
      "fields": [
        {
          "name": "Товар",
          "selectors": [
            {"selector": ".card .title", "extract": "text"},
            {"selector": ".h3 a", "extract": "text"}
          ],
          "is_unique": true,
          "normalize": [ {"tools": ["default_clean"]} ]
        },
        {
          "name": "Розничная_цена",
          "selectors": [
            {"selector": ".price", "extract": "text"}
          ],
          "normalize": [ {"tools": ["default_clean", "price_to_float"]} ]
        }
      ]
    }
```
**Поведение:** пайплайн загружает профиль, валидирует и передаёт `field_specs`/`container_specs` в экстрактор и нормализатор. Добавление нового поля или сайта = правка/добавление JSON без изменений кода.

---

## Пайплайн (требования и кодовые примеры) [По порядоку внедрения]

### 1) Перестроить `ProductExtractor` на имена полей и сырой вывод (A + B + C)

**Требование**

* Экстрактор возвращает `dict[str, Any]` с ключами = `FieldSpec.name`.

* Проход по **всем** `field_specs` без обращения по индексам.

* Любая нормализация в экстракторе запрещена (только извлечение строк/атрибутов).

**Пример кода**
```python
    class ProductExtractor:
        def __init__(self, field_specs: list[FieldSpec], container_specs: ContainerSpecs, *, trace: bool = False):
            self._specs = field_specs
            self._containers = container_specs
            self._trace = trace

        def extract(self, html: str, *, task_id: int) -> tuple[list[dict[str, Any]], list[ParseIssue], str]:
            soup = BeautifulSoup(html, "lxml")
            containers = self._find_card_containers(soup)
            results: list[dict[str, Any]] = []
            issues: list[ParseIssue] = []

            for card in containers:
                row: dict[str, Any] = {}
                for spec in self._specs:
                    value = self._extract_field(card, spec)
                    if value is None:
                        issues.append(ParseIssue(task_id=task_id, field_name=spec.name, code="ERR_PARSE_MISSING_FIELD"))
                        continue
                    row[spec.name] = value  # только сырые значения
                if row:
                    results.append(row)

            page_title = soup.title.get_text(strip=True) if soup.title else ""
            return results, issues, page_title
```
**Мотивация**

* Отвязка от индексов: новые поля добавляются конфигом.

* Единый контракт: экстрактор не превращает данные — только достаёт.

> Описание нового поведения: Добавление поля в профиль → колонка автоматически появляется в выходном `dict` и далее в XLSX после нормализации.

---

### 2) Централизация нормализации в `PriceNormalizer` (B)

**Требование**

* `PriceNormalizer.normalize` принимает `list[dict[str, Any]]` и возвращает такой же список.

* Инструменты вызываются согласно `NormalizeRules` из профиля (реестр функций).

**Пример кода**
```python
    class PriceNormalizer:
        def __init__(self, field_specs: list[FieldSpec]):
            self._rules_by_field: dict[str, list[NormalizeRules]] = {
                fs.name: list(fs.normalize) for fs in field_specs if fs.normalize
            }

        def normalize(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            for row in rows:
                updated = dict(row)
                for name, rules in self._rules_by_field.items():
                    if name not in row:
                        continue
                    val = row[name]
                    for rule in rules:
                        for tool in (rule.tools or []):
                            fn = TOOL_REGISTRY.get(tool)
                            if fn is None:
                                continue
                            val = fn(val, rule)
                    updated[name] = val
                out.append(updated)
            return out
```
**Мотивация** Единая точка истины по нормализации, отсутствие дублирования.

> Описание нового поведения: Любые преобразования (цена→float, очистка текста, supplier_id) происходят единообразно в одном месте.

---

### 3) DI: загрузка профиля и прокидка в экстрактор/нормализатор (D + F)

**Требование**

* `ParserPipeline` получает путь к JSON‑профилю, загружает его в структуры (`FieldSpec`, `ContainerSpecs`) и передаёт в конструкторы.

**Пример кода**
```python
    def load_profile(path: str) -> tuple[list[FieldSpec], ContainerSpecs]:
        data = json.load(open(path, "r", encoding="utf-8"))
        validate_profile(data)  # jsonschema / pydantic
        field_specs = [FieldSpec.from_dict(f) for f in data["fields"]]
        container_specs = ContainerSpecs(selectors=data["containers"]["selectors"])
        return field_specs, container_specs

    class ParserPipeline:
        def __init__(self, profile_path: str, writer: XlsxWriterService):
            fields, containers = load_profile(profile_path)
            self.extractor = ProductExtractor(fields, containers)
            self.normalizer = PriceNormalizer(fields)
            self.writer = writer
```
**Мотивация**

* Поддержка разных сайтов/версий вёрстки без изменения кода.

> Описание нового поведения: Подмена профиля = подмена набора полей/селекторов/правил, код остаётся тем же.

---

### 4) Трассируемость (опционально, точечно) (E)

**Требование**

* Экстрактор в режиме `trace=True` собирает сведения: какой `SelectorVariant` сработал, сырой текст; нормализатор — вход/выход каждого инструмента.

**Пример кода**
```python
    if self._trace:
        debug_log.append({
            "field": spec.name,
            "selector": used_selector,
            "raw": value
        })
```
**Мотивация**

* Локализация проблем (сломавшийся селектор, пустые поля, «грязные» значения).

> Описание нового поведения: В debug‑режиме виден полный путь: селектор → сырое значение → применённые инструменты → финальный результат.

---

## Изменения для каждого зависимого компонента от основного обновления

### ParserPipeline

**Требование**

* Передаёт `task_id` из `PageTask` в `extract`.

* Работает со списками `dict` от экстрактора и нормализатора без адаптеров.

**Пример кода**
```python
    rows_raw, issues, page_title = self.extractor.extract(html, task_id=task.id)
    rows_norm = self.normalizer.normalize(rows_raw)
```
**Мотивация**

* Полноценная трассировка по URL/странице; отсутствие преобразований в пайплайне.

Описание нового поведения: Пайплайн остаётся «оркестратором», не вмешиваясь в структуру данных.

### XlsxWriterService

**Требование**

* Принимать `list[dict[str, Any]]` без изменений публичного API.

**Пример кода**
```python
    path = self.writer.write([
        {"title": page_title, "rows": rows_norm}
    ])
```
**Мотивация**

* Уже поддерживает dict и авто‑заголовки; проводим только регресс‑тест.

> Описание нового поведения: Новые колонки появляются автоматически, согласно ключам словаря.

### models_and_specs

**Требование**

* Добавить загрузчик профилей и валидацию; сохранить существующие Python‑спеки для обратной совместимости (fallback).

**Пример кода**
```python
    fields, containers = load_profile(profile_path) if profile_path else (FIELD_SPECS, CONTAINER_SPECS)
```
**Мотивация**

* Плавная миграция: можно работать как со старым кодом, так и с JSON‑профилем.

> Описание нового поведения: Использование профиля становится стандартным путём, но старый режим доступен.

---

## Новые модули или утилиты (Но можно обойтись раширением существующих)

* `app/core/profile_loader.py` — загрузка/валидация JSON‑профиля, преобразование в `FieldSpec`/`ContainerSpecs`/`NormalizeRules`.

* `app/parsing/extract_trace.py` — (по желанию) вспомогательные структуры для трассировки.

---

## Нефункциональные требования

* **Обратная совместимость**: наличие fallback‑режима без JSON‑профиля.

* **Надёжность**: нормализатор единственный преобразует данные; экстрактор только извлекает.

* **Тестируемость**: детерминированные юнит‑тесты на профили; интеграционные — на реальные страницы.

* **Производительность**: отсутствие двойной нормализации; минимум аллокаций в горячем пути.

* **Наблюдаемость**: опциональный trace‑режим без шумных логов по умолчанию.
