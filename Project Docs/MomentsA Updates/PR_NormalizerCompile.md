# Цель обновления

Перевести текущий `PriceNormalizer` от фиктивной «компиляции» к **реальной функциональной композиции** шагов нормализации, управляемой конфигом (`FieldSpec.normalize`) и единым **контекстом**.
Код `Normalizer` не должен знать о конкретных инструментах и не меняться при их добавлении; вместо этого он применяет **скомпилированные пайплайны** по именам из конфига. Это устраняет проблему пустого `_compile_actions` и дублирующих структур данных, повышает модульность и трассируемость.&#x20;

---

# Пайплайн (требования и кодовые примеры)

## 1) Единый контекст нормализации

**Требование.** Все инструменты принимают `(value, ctx)`; любые параметры (сегодня `supplier_id`, завтра ещё 10) добавляются в `NormalizationContext` без изменений API.

```python
# app/parsing/normalizer_context.py  (новый небольшой модуль)
from dataclasses import dataclass
from typing import Optional

@dataclass(slots=True)
class NormalizationContext:
    supplier_id: Optional[int] = None
    locale: str = "ru"
    currency: str = "RUB"
    # расширяем по мере роста требований без изменения интерфейса инструментов
```

> Мотивация: сейчас `mark_supplier` использует `supplier_id`, передаваемый через rules; контекст снимает проблему «растущих аргументов».&#x20;

---

## 2) Чистые функции‑инструменты

**Требование.** Каждый инструмент — чистая функция `fn(value, ctx) -> value`. Никакой логики конфигурации/доступа к правилам внутри `Normalizer`.

```python
# app/parsing/tools.py  (новый модуль с инструментами)
import re
from typing import Any, Literal
from .normalizer_context import NormalizationContext

NA: Literal["NA"] = "NA"

def default_clean(value: Any, ctx: NormalizationContext) -> Any:
    if value is None or value == NA:
        return value
    s = str(value).replace("\u00A0", " ").strip()
    return re.sub(r"\s+", " ", s)

def price_to_float(value: Any, ctx: NormalizationContext) -> Any:
    if value is None or value == NA:
        return value
    s = str(value).replace(",", ".")
    s = re.sub(r"[^0-9.]", "", s)
    if not s:
        return NA
    try:
        return float(s)
    except ValueError:
        return NA

def mark_supplier(value: Any, ctx: NormalizationContext) -> Any:
    if value is None or value == NA or ctx.supplier_id is None:
        return value
    s = str(value).strip()
    prefix = f"{ctx.supplier_id}-"
    return s if s.startswith(prefix) else prefix + s
```

> Это покрывает имеющиеся правила из `FIELD_SPECS.normalize` и заменяет текущие приватные методы `_t_default_clean`, `_t_price_to_float`, `_t_mark_supplier`.&#x20;

---

## 3) Реестр инструментов

**Требование.** Реестр «имя → функция». Добавление нового инструмента = регистрация в одном месте.

```python
# app/parsing/tool_registry.py
from typing import Callable, Any, Dict
from .normalizer_context import NormalizationContext
from .tools import default_clean, price_to_float, mark_supplier

ToolFn = Callable[[Any, NormalizationContext], Any]

TOOL_REGISTRY: Dict[str, ToolFn] = {
    "default_clean": default_clean,
    "price_to_float": price_to_float,
    "mark_supplier": mark_supplier,
}
```

> В текущей реализации имена инструментов уже приезжают из `FieldSpec.normalize.tools`, но диспетчеризация идёт через `if tool is ... or tool == ...`. Реестр убирает этот анти‑паттерн и «магические строки» в коде `normalize`.&#x20;

---

## 4) Компилятор пайплайна

**Требование.** На старте собрать для каждого поля «план» — список функций‑шагов. Код компиляции **не меняется**, как бы ни рос список инструментов в конфиге.

```python
# app/parsing/pipeline_compiler.py
from typing import Any, List, Tuple
from .tool_registry import TOOL_REGISTRY, ToolFn
from app.core.models_and_specs import NormalizeRules  # существующий тип rules :contentReference[oaicite:4]{index=4}

CompiledStep = Tuple[ToolFn, NormalizeRules]  # (функция, исходное правило — для трассировки)

def compile_pipeline(rules: List[NormalizeRules]) -> List[CompiledStep]:
    """
    Превращает список NormalizeRules в плоский список шагов.
    Правила могут содержать несколько tools — разворачиваем по порядку.
    """
    steps: List[CompiledStep] = []
    for rule in (rules or []):
        for name in (rule.tools or []):
            fn = TOOL_REGISTRY.get(name)
            if fn:
                steps.append((fn, rule))
            # при неизвестном имени можно: warn/log и пропустить
    return steps
```

> В отличие от текущего `_compile_actions`, этот компилятор возвращает **реальные шаги**, а не пустой список.&#x20;

---

## 5) Трассируемый вызов шагов (опционально)

**Требование.** Возможность логировать каждый шаг без вмешательства в бизнес‑код.

```python
# app/parsing/tracing.py
from typing import Callable, Any
from time import perf_counter
from .normalizer_context import NormalizationContext

def traced(fn) -> Callable[[Any, NormalizationContext, str], Any]:
    def wrapper(value: Any, ctx: NormalizationContext, step_name: str):
        t0 = perf_counter()
        out = fn(value, ctx)
        dt = (perf_counter() - t0) * 1000
        # здесь можно слать в LogBus/метрики
        # log.info("NORM_STEP", f"{step_name} took {dt:.2f}ms; in={value!r} out={out!r}")
        return out
    return wrapper
```

> Внедряется как обёртка вокруг функций из реестра, если нужна детальная трассировка по шагам. Лог‑шина у нас уже есть (`LogBus`).&#x20;

---

## 6) `Normalizer` (новое имя вместо `PriceNormalizer`)

**Требование.** Класс знает только: «по каким полям есть rules», «какие пайплайны собраны», «как их применить к записям». Никаких `if/elif` по именам инструментов; никакого `is/==` со строками; никакого второго дублирующего словаря «псевдо‑действий».

```python
# app/parsing/normalizer.py  (обновлённый)
from __future__ import annotations
from dataclasses import replace
from typing import Any, Dict, List
from app.core.models_and_specs import FIELD_SPECS, ProductRecord, NormalizeRules  # source of truth :contentReference[oaicite:7]{index=7}
from .normalizer_context import NormalizationContext
from .pipeline_compiler import compile_pipeline, CompiledStep

class Normalizer:
    """
    Универсальный нормализатор, применяющий скомпилированные пайплайны по FIELD_SPECS.
    Расширение делается через реестр функций и конфиг; сам класс не меняется.
    """
    def __init__(self, field_specs=None) -> None:
        specs = field_specs or FIELD_SPECS
        self._pipelines: Dict[str, List[CompiledStep]] = {}
        for spec in specs:
            if spec.normalize:
                self._pipelines[spec.name] = compile_pipeline(spec.normalize)

    def normalize(self, records: List[ProductRecord], ctx: NormalizationContext) -> List[ProductRecord]:
        out: List[ProductRecord] = []
        for rec in records:
            updates: Dict[str, Any] = {}
            for field_name, steps in self._pipelines.items():
                current = getattr(rec, field_name, None)
                new_val = current
                for fn, rule in steps:
                    # Правило rule доступно для более сложной логики, если понадобится
                    new_val = fn(new_val, ctx)
                if new_val is not current:
                    updates[field_name] = new_val
            out.append(replace(rec, **updates) if updates else rec)
        return out
```

> В текущей кодовой базе `ParserPipeline` вызывает нормализатор после парсинга: `products = self._normalizer.normalize(products)`. После обновления потребуется передать `ctx` (контекст) — это минимальная и предсказуемая адаптация вызова.&#x20;

---

## 7) Встраивание в пайплайн

**Требование.** В `ParserPipeline` при создании нормализатора/вызове `normalize` добавить контекст.

```python
# app/pipeline/runner.py (фрагмент изменения вызова)
from app.parsing.normalizer_context import NormalizationContext
from app.parsing.normalizer import Normalizer

# ...
self._normalizer = normalizer or Normalizer()
# ...
ctx = NormalizationContext(supplier_id=123)  # можно собирать из конфигов/профиля поставщика

products, issues, page_title = self._extractor.extract(page.text, task_id=task_id)
products = self._normalizer.normalize(products, ctx=ctx)
```

> Сейчас `ParserPipeline` передаёт фиксированный `task_id=0` в `extract` — это отдельная проблема 7/10; здесь показано только место интеграции `ctx`.&#x20;

---

# Нефункциональные требования

* **Модульность/читаемость.**
  `Normalizer` не зависит от конкретных инструментов; добавление новых функций — только в реестр + конфиг.

* **Гибкость/конфигурируемость.**
  `FieldSpec.normalize` остаётся источником правды (как сейчас), но теперь реально «управляет» вызовом через компиляцию шагов.&#x20;

* **Трассируемость/тестируемость.**
  Каждая функция может быть обёрнута в `traced`‑декоратор; юнит‑тесты легко подменяют `TOOL_REGISTRY[name]` заглушкой.

* **Производительность.**
  Компиляция делается один раз на инициализации; рантайм — линейный прогон плоского списка шагов без ветвлений по строкам (устранён `if tool is/== …`). Текущая проблема с пустым `_compile_actions` исчезает.&#x20;

* **Отказ от дублирования.**
  `Normalizer` хранит **только один** словарь: `field_name -> [steps]`. Нет `_actions_by_field` и `_rules_by_field` как параллельных сущностей. (Правила разворачиваются в шаги при компиляции.)

---

# Мини‑пример целиком

```python
# 1) Контекст
ctx = NormalizationContext(supplier_id=123)

# 2) Инструменты уже зарегистрированы в TOOL_REGISTRY

# 3) Normalizer сам компилирует пайплайны по FIELD_SPECS на старте
norm = Normalizer()

# 4) Вызов на записях
out_records = norm.normalize(records_in, ctx=ctx)
```

**Плюс:** если завтра добавим инструмент `"normalize_phone"` и пропишем его в `FIELD_SPECS.normalize.tools`, `Normalizer` начнёт его применять **без единой правки** в своём коде — по тем же механикам.


