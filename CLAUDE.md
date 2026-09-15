# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Streamlit web app that logs into `cnc1.ru`, fetches catalog listing pages (and optionally individual product cards), parses product data out of the HTML, normalizes it, and exports it to an XLSX workbook (one sheet per input URL). All business logic lives in `app/`; `main.py` is a thin Streamlit Cloud entrypoint that reloads `app.ui.interface` on every rerun and optionally overrides a few constants from `st.secrets`/env vars (`AUTH_EMAIL`, `AUTH_PASSWORD`, `BATCH_SIZE`, `CONCURRENCY`, `FETCH_TIMEOUT_S`).

There is no test suite, linter config, or build step in this repo — verification is manual (run the app and check output).

## Running the app

```bash
pip install -r requirements.txt
streamlit run main.py
```

`requirements.txt` is UTF-16 encoded — read/write it accordingly if editing programmatically (plain `cat`/text tools will show garbled output).

Login credentials and tunables currently default to hardcoded values in `app/ui/interface.py` (`AUTH_EMAIL`, `AUTH_PASSWORD`, `BATCH_SIZE`, `CONCURRENCY`, `FETCH_TIMEOUT_S`, `REQUEST_DELAY_S`, `REQUEST_DELAY_JITTER_S`); `main.py` overrides them at runtime from Streamlit `secrets`/env when present without editing the module.

## Architecture

The pipeline is orchestrated by `app/pipeline/runner.py::ParserPipeline`, invoked from the UI on a background thread (`app/ui/interface.py`) via `asyncio.run`. Progress/status is shared with the UI thread through `app/ui/state.py::UIState` (a thread-safe session-scoped state object) and log lines flow through `app/app_logging/logbus.py::LogBus` (a queue-backed pub/sub the UI polls and renders).

There are two parsing modes (`app/core/parsing_mode.py::ParsingMode`), selected in the UI and passed into `PipelineConfig`:

- **SHALLOW** (`ParserPipeline._run_shallow`): login → fetch listing pages → `ProductExtractor.extract` → `PriceNormalizer.normalize` → export. One sheet's worth of `ProductRecord`s per listing URL.
- **EXTENDED** (`ParserPipeline._run_extended`): login → fetch listing pages → `ProductExtractor.extract_partials` (partial products + per-product card URLs) → batch-fetch each product card page → `ProductCardExtractor.extract` (characteristics/attributes) → `ProductRecordAggregator.aggregate` (merge partial + card data into a flat dict) → `MappingFieldNormalizer.normalize` → export. Progress total grows dynamically as card URLs are discovered per listing (`ui_state.add_total`).

Both modes batch listing URLs (`PipelineConfig.batch_size`) and, in extended mode, further batch card URLs per listing (`PipelineConfig.cards_batch_size`) to bound concurrent tasks; per-request concurrency is capped by `PipelineConfig.concurrency` inside `PageFetcher`. Stop requests (`ui_state.stop_requested`, set by the UI's "Остановить" button) are checked between batches and cancel in-flight `asyncio.Task`s; a partial export is always written on stop.

Key modules and their roles:

- `app/core/models_and_specs.py` — single source of truth for data shapes and site-specific scraping rules: `FIELD_SPECS` (CSS selectors + extraction/normalization rules per column, in `SHALLOW` mode these define the 5 exported columns exactly — column headers come from `FieldSpec.name`, spaces included) and `CONTAINER_SPECS` (selectors identifying a product's container element on the listing page). Change selectors here when the target site's markup changes.
- `app/core/dto_extended.py` — extended-mode DTOs (`PartialProduct`, card data types) that flow between listing extraction, card extraction, and aggregation.
- `app/core/errors.py` — `PipelineError` hierarchy with a stable `ErrorCode` per exception, used for structured logging; infrastructure failures raise, missing/malformed fields are recorded as non-fatal `ParseIssue`s instead (see `models_and_specs.py::ParseIssue`).
- `app/net/session_and_fetcher.py` — `SessionManager` (httpx client + cookies + auth state) and `PageFetcher` (concurrent fetch with retries/timeouts, `SHOWALL_*` query param injection for listing pages).
- `app/net/auth.py` — `BaseAuthAdapter`/`FormAuthAdapter`: POSTs the login form to `cnc1.ru/auth/`; success = HTTP 200 and no "ошибка" (case-insensitive) in the response body.
- `app/parsing/extractor.py` — `ProductExtractor`: listing-page parsing for both shallow (`extract`) and extended (`extract_partials`) modes, driven by `FIELD_SPECS`/`CONTAINER_SPECS`.
- `app/parsing/card_extractor.py` — `ProductCardExtractor`: parses an individual product card page into characteristics/attributes (extended mode only).
- `app/parsing/aggregator.py` — `ProductRecordAggregator`: merges a listing partial with its corresponding card data into one flat record.
- `app/parsing/normalizer.py` / `app/parsing/mapping_normalizer.py` — value normalization; `PriceNormalizer` operates on shallow-mode `ProductRecord` dataclasses, `MappingFieldNormalizer` on extended-mode dict records. Normalization tool identifiers (e.g. `price_to_float`, `mark_supplier`) referenced from `FieldSpec.normalize` are implemented here.
- `app/export_io/writer.py` — `XlsxWriterService`: writes accumulated `{page_title, data}` groups to a multi-sheet XLSX (one sheet per listing page).
- `app/ui/state.py` — `UIState`: thread-safe progress/status container shared between the worker thread and Streamlit's rerun-driven main thread.

## Conventions

- Comments, docstrings, and log messages are in Russian; keep this consistent when editing.
- Prefer adding new normalization "tools" as named entries under `FieldSpec.normalize` / `NormalizeRules.tools` and implementing them in `normalizer.py`/`mapping_normalizer.py`, rather than special-casing fields inline in the extractor.
- Parsing failures for individual fields should become `ParseIssue` entries (non-fatal, logged/counted), not exceptions — exceptions are reserved for infrastructure-level failures (`app/core/errors.py`).
