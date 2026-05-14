# AGENTS.md

## Purpose
- This repository hosts the `binance_datatool` Python package.
- Roadmap: evolve from single-source CLI → multi-source data engineering platform with dlt,
  SQLMesh, DuckDB/DuckLake, and Prefect. See `docs/brainstorms/2026-05-11-platform-evolution-requirements.md`
  and `docs/plans/2026-05-11-005-platform-evolution-plan.md` for the full plan.
- Treat checked-in source code, tests, configuration, and public documentation as the source of
  truth for current behavior.
- Keep the repository easy for both human developers and AI agents to navigate.

## Local Instructions
- Before making changes, check whether `.AGENTS.local.md` exists at the repository root.
- If `.AGENTS.local.md` exists, read it and follow it as additional local guidance.
- Public instructions in this file apply to everyone; local instructions add developer-specific context.

## Documentation Navigation
- Read `docs/architecture.md` before making structural or cross-layer changes.
- Read `docs/extending.md` before adding a new command, workflow, enum member, or sub-command group.
- Read `docs/brainstorms/2026-05-11-platform-evolution-requirements.md` before making multi-source or
  stack-level decisions (dlt, SQLMesh, tardis.dev, Databento integration).
- Read `docs/reference/testing.md` before changing tests, fixtures, or test layout conventions.
- Use `docs/reference/README.md` as the entry point for module and CLI reference details.
- Treat checked-in code and tests as higher priority than documentation if they conflict.
- When a change alters behavior, public interfaces, or contributor workflows, update the relevant
  `docs/` promptly after the code change is committed.
- Unless explicitly requested otherwise, do not mix code changes and documentation updates in the
  same commit.
- **Documentation accuracy rule**: If docs describe features not implemented (e.g., Bronze layer, Migration, Skills), either implement them or move docs to `docs/proposals/`. Never maintain false documentation about what the tool can do.
- **Overspec prevention**: Original specs may have promised 34 Pydantic models, Bronze/Silver layers, 5 Skills, etc. The codebase uses `@dataclass` (29 total), not Pydantic. Keep docs aligned with reality (currently 98% accurate).

## Working Model
- Build the project as a modern Python package named `binance_datatool`.
- Use the intended package layout: shared code under `binance_datatool.common`, archive access
  under `binance_datatool.archive`, CLI entrypoints under `binance_datatool.cli`, and business
  workflows under `binance_datatool.workflow`.
- Prefer clear, composable workflows and thin CLI entrypoints.
- Keep the root package minimal. Export only version metadata from `binance_datatool.__init__`.

## Stack Architecture

Each component handles its strength — no overlap:

| Component | Role | Module |
|-----------|------|--------|
| **dlt** | Extract + Load (pagination, retries, incremental state, schema inference, S3 listing) | `binance_datatool.dlt` — resources (5 per-data-type modules), sources (unified builders), models (Pydantic), destinations (DuckDB/DuckLake) |
| **Polars** | Bronze→Silver transforms (type casting, column renaming, timestamp normalization) | `binance_datatool.transforms` — 3 modules: klines, aggTrades, fundingRate |
| **Pandera** | DataFrame-level validation at pipeline boundaries (column types, nullability, cross-column checks) | `binance_datatool.validation.schemas` — 6 schemas: bronze klines/aggTrades/fundingRate, silver klines/aggTrades/fundingRate |
| **Pydantic** | Per-record validation within dlt resources (business rules, type coercion, authoritative schema) | `binance_datatool.dlt.models` — 8 models: KlineModel, AggTradeModel, FundingRateModel, VenueModel, SymbolMetaModel, Raw* models |
| **SQLMesh** | Versioned SQL transforms (INCREMENTAL_BY_TIME_RANGE models, audits for data quality, column-level lineage) | `models/` — bronze/silver models, audits; optional via `dlt_sqlmesh_pipeline` |
| **Prefect** | Orchestration (flow composition, parallelism, retries, concurrency guards, DLQ, cron scheduling) | `binance_datatool.workflow.prefect_flows` (thin @flow defs) + `workflow.prefect_tasks` (importable business logic) |
| **DuckLake** | Primary storage — lakehouse with Parquet files + sqlite catalog. dlt destination by default. Handles partitioning, ACID, snapshots automatically. | `dlt.destinations.ducklake()` — managed by dlt; no manual catalog setup needed |
| **DuckDB** | Legacy/fallback storage — single-file database. Used for unit tests and backward compatibility. | `dlt.destinations.duckdb()` — use via explicit `destination="duckdb"` |

### Data Flow

```
dlt Sources (5 modules)
  ↓ Extract + Load into shared bronze tables (one per data type)
        bronze.klines       (all trade types, all sources)
        bronze.agg_trades   (all trade types, all sources)
        bronze.funding_rate (all trade types, all sources)
Polars transforms (3 modules)
  ↓ Bronze → Silver with Pandera validation at boundaries
        silver.klines       (19 columns, normalized schema)
        silver.agg_trades   (18 columns, normalized schema)
        silver.funding_rate (12 columns, normalized schema)
Arrow writes → DuckDB silver tables
  ↓
Prefect orchestrates (dlt_sqlmesh_pipeline dispatcher)
  └─ data_type=klines       → rest/archive/ws → transform_to_silver
  └─ data_type=aggTrades    → rest/archive    → transform_agg_trades_to_silver
  └─ data_type=fundingRate  → rest/archive    → transform_funding_rate_to_silver
  └─ Stage 0: gap detection (Prefect + DuckDB utility)
  └─ Stage 3: SQLMesh plan (versioned transforms, optional)
```

## Toolchain
- Package and dependency management: `uv` **only** — never `pip`, `uv pip`, or global `python3`
- Build backend: `hatchling`
- Linting and formatting: `ruff`
- Testing: `pytest`
- Git hooks: `pre-commit`
- External downloader: `aria2` (legacy archive download only)

## Expected Commands (uv-native)
All commands use `uv run` — never `pip`, `uv pip`, or bare `python3`:

- Environment setup: `uv sync`
- Lint: `uv run ruff check .`
- Format: `uv run ruff format .`
- Tests: `uv run pytest`
- Targeted tests: `uv run pytest tests/path_to_test.py`
- CLI entrypoint: `uv run binance-datatool --help`
- Python scripts: `uv run python3 script.py`
- Type check: `uv run pytest tests/ --co` (collect tests) or `uvx ty check src`
- Pre-commit: `pre-commit run --all-files`

## Code Standards
- Use written English for code comments, docstrings, logs, CLI text, and developer-facing messages.
- Write clear comments only where they add real value; do not narrate obvious code.
- Use Google-style docstrings for public modules, classes, and functions.
- Use modern Python syntax compatible with Python 3.11, but **MUST NOT** use syntax introduced after 3.11
- Keep Python source line length at 100 characters or less. This limit does not apply to
  Markdown files under `docs/`, where long table rows and wide ASCII diagrams are allowed.
- Keep imports at module top level unless a local import is technically necessary.
- Prefer explicit types and clear names over implicit behavior.
- Prefer small, focused modules with straightforward responsibilities.
- Follow a let-it-crash mindset. Do not add casual fallback logic, silent recovery, or defensive
  branches unless there is a concrete and justified failure mode.
- Replace hand-rolled infrastructure with mature third-party libraries when the design already
  names a standard dependency.

## Architecture Expectations
- Keep CLI modules thin: parse arguments, construct workflows, and present output.
- Put business logic in importable workflow or domain modules, not inside CLI command functions.
- Prefer Polars LazyFrame-based data processing when working with tabular pipelines.
- Preserve a clear separation between shared utilities, archive access, parsing, completion,
  metadata tracking, and holographic kline generation.
- Design commands to be atomic and composable so an agent can inspect state and then choose the
  next step.
- **Prefect tasks are thin wrappers**: business logic lives in `workflow/prefect_tasks/` as plain
  importable functions. `prefect_flows.py` contains only `@flow` and `@task` decorators that
  delegate to these functions. This makes the business logic testable and reusable without Prefect.
- **dlt package is standalone**: `binance_datatool.dlt` can be imported independently of the rest
  of the package. It provides models, resources, source builders, and destination helpers.

## Testing Expectations
- Add or update tests for every meaningful behavior change.
- Prefer the smallest test scope that proves the change.
- Use `pytest` as the test runner and keep tests readable enough for agent-assisted maintenance.
- When adding data-processing behavior, include representative fixtures or focused regression
  coverage.
- Focus tests on functional correctness and observable behavior, not on logging implementation
  details such as how loguru renders or routes messages to stderr.

## Exchange Client SDK Migration

The `exchange/` module uses **official Binance SDK packages** (not hand-rolled `aiohttp`):

| Package | Market | REST Method | WS Method |
|---------|--------|-------------|-----------|
| `binance-sdk-spot` | Spot | `rest_api.klines()` | `connection.kline(symbol, interval)` with `KlineIntervalEnum` |
| `binance-sdk-derivatives-trading-usds-futures` | UM | `rest_api.kline_candlestick_data()` | `connection.kline_candlestick_streams()` |
| `binance-sdk-derivatives-trading-coin-futures` | CM | `rest_api.kline_candlestick_data()` | `connection.kline_candlestick_streams()` |

### Key Design Decisions
- **Async generator interface preserved**: SDK callback-based WS streams are wrapped via `asyncio.Queue` bridge. `ExchangeClient.stream_ohlcv()` still returns `AsyncIterator[KlineData]`.
- **No auth**: `ConfigurationRestAPI(api_key="", api_secret="")` for public market data endpoints only.
- **Archive client intact**: `archive/` module still uses `aiohttp` for S3 access (`data.binance.vision`).
- **Backward compat**: `BinanceRestClient = BinanceSpotRestClient`, `BinanceWsClient = BinanceSpotWsClient`.
- **KlineData.from_binance_api()**: This classmethod on `common/types.py` maps the 12-element Binance kline array to our `KlineData` dataclass.
- **Required dependencies**: SDK packages are required (not optional). CCXT remains optional (`[exchange]` extra).

### SDK Response Format
- REST klines endpoint returns array of 12 elements (same across all market types):
  `[open_time, open, high, low, close, volume, close_time, quote_volume, num_trades, taker_buy_volume, taker_buy_quote_volume, ignore]`
- WS kline stream returns dict with structure `{"k": {"t": ..., "o": ..., ...}}`

## Running Workflows with Prefect

```bash
# Serve deployments (both daily backfill + hourly metadata — no server/worker needed)
uv run python -m binance_datatool.workflow.prefect_flows serve

# In another terminal, trigger a run
uv run prefect deployment run 'Historical Data Pipeline/historical_pipeline'

# List symbols from local catalog (instant, no network)
uv run binance-datatool list-symbols spot --from-catalog --catalog /path/to/lake

# Or run flows directly via Python
uv run python3 -c "
from binance_datatool.workflow.prefect_flows import historical_pipeline, bulk_backfill

# Single symbol (last 3 days of 1h klines)
result = historical_pipeline(trade_type='spot', symbols=['BTCUSDT'],
                             data_type='klines', interval='1h', lookback_days=3)
print(result)

# Multi-symbol (parallel via .map(), DuckDB serialized via concurrency guard)
result = historical_pipeline(trade_type='spot', symbols=['BTCUSDT', 'ETHUSDT'],
                             data_type='klines', interval='1h', lookback_days=3)
print(result)

# Bulk backfill with explicit symbols
result = bulk_backfill(trade_type='spot', symbols=['BTCUSDT', 'ETHUSDT', 'SOLUSDT'],
                       data_type='klines', interval='1h', lookback_days=3)
print(result)

# Bulk backfill with auto-discovered symbols (max 10 by default, configurable)
result = bulk_backfill(trade_type='um', max_symbols=5)
print(result)
"
```

Available flows:
- `historical_pipeline` — full metadata → download → verify → gap-fill → sink → health
- `bulk_backfill` — multi-symbol, delegates to historical_pipeline
- `refresh_metadata_flow` — venue/symbol metadata refresh
- `health_flow` — health check + anomaly detection
- `sink_flow`, `gap_fill_flow`, `download_flow`, `verify_flow` — individual steps

### Task Graph

```
historical_pipeline (ThreadPoolTaskRunner, max_workers=4)
  │
  ├── refresh_metadata_flow (subflow, sequential)
  │
  ├── prepare_symbol.map() ←── parallel fan-out (download→verify→fill_gaps)
  │     ├── BTCUSDT  ── download → verify → fill_gaps
  │     ├── ETHUSDT  ── download → verify → fill_gaps
  │     └── SOLUSDT  ── download → verify → fill_gaps
  │
  ├── sink_silver() ←── sequential (concurrency guard: ducklake-writer)
  │     ├── BTCUSDT → DuckLake  (concurrency slot acquired)
  │     ├── ETHUSDT → DuckLake  (waits for slot)
  │     └── SOLUSDT → DuckLake  (waits for slot)
  │
  └── health_flow() ←── per-symbol health check (skips errored symbols)
        ├── BTCUSDT → anomaly detection (null prices, date gaps, outliers)
        ├── ETHUSDT → anomaly detection
        └── SOLUSDT → anomaly detection
```

### Prefect-Native Patterns

**Error isolation** — Use `future.result(raise_on_failure=False)` and
`future.state.is_completed()` instead of `try/except`. Pair futures with
their inputs via `zip(sym_list, futures, strict=True)` so the symbol is
known even when the task fails:

```python
for sym, future in zip(sym_list, prep_futures, strict=True):
    meta = future.result(raise_on_failure=False)
    if future.state.is_completed():
        rows = sink_silver(tt, sym, ...)
        results[sym] = {"rows_sunk": rows, "gaps_filled": meta["gaps"]}
    else:
        results[sym] = {"rows_sunk": 0, "gaps_filled": 0, "error": str(meta)}
```

**Concurrency guard for DuckDB** — Use Prefect's `concurrency` context
manager with `occupy=1` to serialize writes. This works across task runs
within the same process:

```python
from prefect.concurrency.sync import concurrency as _pcon

with _pcon("ducklake-writer", occupy=1):
    ...
```

**Metadata concurrency guard** — `refresh_metadata_flow` also uses the
`ducklake-writer` guard to prevent races with `sink_silver` when both
run as separate deployments:

```python
with concurrency("ducklake-writer", occupy=1):
    wf.save_venues(wf.refresh_venues())
    syms = asyncio.run(wf.refresh_symbols(TradeType(trade_type)))
    wf.save_symbols(syms)
```

**Async bridging** — Prefect 3.x does not support calling native async
tasks from sync flows. Use `asyncio.run()` inside sync tasks to bridge:

```python
result = asyncio.run(wf.run())
```

**Deployments** — Use `prefect.serve()` with `flow.to_deployment()` to
register multiple cron deployments in a single process:

```python
from prefect import serve
serve(
    historical_pipeline.to_deployment(name="daily-backfill", cron="0 6 * * *"),
    refresh_metadata_flow.to_deployment(name="hourly-metadata", cron="0 * * * *"),
)
```

### Task Dependency Graph

```
historical_pipeline (ThreadPoolTaskRunner, max_workers=4)
  │
  ├── refresh_metadata_flow (subflow, sequential)
  │     └── MetadataWorkflow.save_venues() + save_symbols()
  │           (ducklake-writer concurrency guard)
  │
  ├── prepare_symbol.map() ←── parallel fan-out (download→verify→fill_gaps)
  │     ├── BTCUSDT  ── download → verify → fill_gaps
  │     ├── ETHUSDT  ── download → verify → fill_gaps
  │     └── SOLUSDT  ── download → verify → fill_gaps
  │
  ├── sink_silver() ←── sequential (concurrency guard: ducklake-writer)
  │     ├── BTCUSDT → DuckLake  (concurrency slot acquired)
  │     ├── ETHUSDT → DuckLake  (waits for slot)
  │     └── SOLUSDT → DuckLake  (waits for slot)
  │
  └── health_flow() ←── per-symbol health check (skips errored symbols)
        ├── BTCUSDT → anomaly detection (null prices, date gaps, outliers)
        ├── ETHUSDT → anomaly detection
        └── SOLUSDT → anomaly detection

bulk_backfill (no task runner — delegates to historical_pipeline)
  └── historical_pipeline (subflow)

Standalone flows:
  download_flow  → download_archive.map()  (ThreadPoolTaskRunner=4)
  verify_flow    → verify_archive.map()    (ThreadPoolTaskRunner=4)
  gap_fill_flow  → fill_gaps()             (single symbol)
  sink_flow      → sink_silver()           (sequential loop)
  health_flow    → check_ducklake_anomalies()
```

### Known Issues & Edge Cases

| Issue | File:Line | Impact | Workaround |
|-------|-----------|--------|------------|
| Sink retry inserts duplicate DuckDB rows | `prefect_flows.py:191` | Fixed — sink_silver no longer has retries | (resolved) |
| `DataType` fallback to `klines` on unknown value | `prefect_flows.py:119` | Fixed — now raises ValueError | (resolved) |
| `verify_flow` data_freq hardcoded to daily | `prefect_flows.py:146` | Fixed — now uses monthly for fundingRate | (resolved) |
| `prepare_symbol` calls sub-tasks inline (not via `.submit()`) | `prefect_flows.py:220-238` | No per-sub-task parallelism within a symbol | By design — download→verify→fill is sequential per symbol. Cross-symbol parallelism via `.map()` |
| S3 listing is slow for full archive | `binance_archive.py:72` | Lists ALL files before filtering by lookback | Use `lookback_days=7` to limit processing. Full listing for BTCUSDT 1d = ~6400 files, ~30s |
| WS streaming blocks in sync context | `binance_ws.py` | Stream runs in asyncio.run() bridge | WS is realtime-only; 1m candles take up to 60s. Use REST for batch. |
| DuckLake is default destination | `pipeline.py:29` | dlt sources now default to ``destination="ducklake"`` | Pass ``destination="duckdb"`` to revert to single-file DuckDB |
| **EXCHANGE NAMING** — canonical function | `common/enums.py:exchange_for()` | Legacy sink used tardis.dev convention (`"binance"`, `"binance-futures"`); transforms used DuckLake convention (`"binance-spot"`, `"binance-perps-um"`) | Fixed — all paths now use `exchange_for(trade_type)` from `common/enums.py`. DuckLake convention is canonical. |
| Pandera `ts_date` typed `object` | `validation/schemas.py:98,119` | AggTradesSilverSchema and FundingRateSilverSchema used `object` instead of `pl.Date` | Fixed — both now use `pl.Date` matching SilverKlinesSchema |
| Missing bronze Pandera schemas | `validation/schemas.py` | No BronzeAggTradesSchema or BronzeFundingRateSchema existed | Added — both now mirror RawAggTradeModel/RawFundingRateModel with VARCHAR-only columns |
| SQLMesh bronze model hardcoded symbol | `models/bronze/klines.sql:29` | Referenced `bronze.BTCUSDT_klines` (single-symbol only) | Fixed — now references `bronze.klines` which all symbols write to via `.apply_hints(table_name="klines")` |
| DuckLake TABLE_DEFS missing restored columns | `catalog.py` TABLE_DEFS | `aggTrades` was missing `first_trade_id`, `last_trade_id`; had stale `interval` column | Fixed — aligned with actual silver schemas (18 columns for aggTrades, 12 for fundingRate) |
| Unused `IcebergCatalog` (YAGNI) | `catalog.py` | Fully implemented Iceberg catalog class, never imported anywhere | Removed — moved to ARCHIVED. Analytics views SQL also removed. |
| gap_detection VARCHAR handling | `gap_detection.py:51` | `CAST(open_time / 86400000 AS BIGINT)` fails when `open_time` is VARCHAR | Fixed — `CAST(CAST(open_time AS BIGINT) / 86400000 AS BIGINT)` handles both TEXT and numeric |
| CM klines symbol naming | `exchange/binance_rest.py` | CM futures use `BTCUSD_PERP` not `BTCUSDT` — E2E test with BTCUSDT fails | Use correct per-market symbol: spot/um use `BTCUSDT`, cm uses `BTCUSD_PERP` |
| S3 download URL missing slash | `binance_archive.py:196` | `S3_DOWNLOAD_PREFIX + key` produced `data.binance.visiondata/...` (no separator) | Fixed — now uses f-string with `/` separator |
| Archive aggTrades `_DATA_TYPE_COLUMNS` missing fields | `binance_archive.py:89-95` | `is_buyer_maker`, `first_trade_id`, `last_trade_id` not in type defs → silently dropped during dlt ingest | Fixed — added all CSV columns to `_DATA_TYPE_COLUMNS` |
| Archive fundingRate `_DATA_TYPE_COLUMNS` missing `mark_price` | `binance_archive.py:104-108` | `mark_price` not in type defs → silently dropped | Fixed — added `mark_price` and `funding_interval_hours` |
| Archive klines `open_time` μs vs ms | `transforms/klines.py` | Binance archive switched from ms (13-digit) to μs (16-digit). `ts_date` computed as `open_time // 86400000` fails for μs → year 58327 | Fixed — auto-detect: `open_time >= 1e15` is μs, divide by 86400000000 instead of 86400000 |
| Empty `mark_price` in fundingRate | `transforms/funding_rate.py:42` | CM fundingRate API returns empty strings for mark_price → `cast(Float64)` fails | Fixed — `str.replace("", "0")` before cast |

## Data Sources

The pipeline ingests from three source layers. See `docs/data-sources.md` for
the complete field-to-source mapping matrix.

| Layer | Source | Latency | CLI |
|-------|--------|---------|-----|
| **Archive** | data.binance.vision (S3) | ~1-2 days | `download`, `verify` |
| **REST API** | api.binance.com via SDK | real-time | `gap-fill`, `refresh-metadata` |
| **WS Stream** | stream.binance.com via SDK | real-time continuous | `stream` (Phase 8) |

### Data Type Coverage

| Type | Archive | REST API | WS Stream | Sink/Transform | Silver Table | E2E Validated |
|------|---------|----------|-----------|----------------|--------------|---------------|
| klines (spot) | ✓ daily zips | ✓ via `build_binance_source` | ✓ via `ws_klines_resource` | ✓ `bronze_klines_to_silver` | `silver.klines` | ✅ REST (1000 rows) + Archive (1 file, μs detected) |
| klines (um) | ✓ daily zips | ✓ via `build_binance_source` | ✓ via `ws_klines_resource` | ✓ `bronze_klines_to_silver` | `silver.klines` | ✅ REST (2000 rows) |
| klines (cm) | ✓ daily zips | ✓ via `build_binance_source` | ✓ via `ws_klines_resource` | ✓ `bronze_klines_to_silver` | `silver.klines` | ⚠ (BTCUSD_PERP naming) |
| aggTrades (spot) | ✓ daily zips | ✓ via `build_rest_source` | — | ✓ `bronze_agg_trades_to_silver` | `silver.agg_trades` | ✅ REST (500 rows) |
| aggTrades (um) | ✓ daily zips | ✓ via `build_rest_source` | — | ✓ `bronze_agg_trades_to_silver` | `silver.agg_trades` | ✅ REST (1000 rows) |
| fundingRate (um) | ✓ monthly zips | ✓ via `build_rest_source` | — | ✓ `bronze_funding_rate_to_silver` | `silver.funding_rate` | ✅ REST (100 rows) |
| fundingRate (cm) | ✓ monthly zips | ✓ via `build_rest_source` | — | ✓ `bronze_funding_rate_to_silver` | `silver.funding_rate` | ✅ REST (100 rows) |
| trades | ✓ daily zips | — | — | ✓ (legacy `sink.py`) | `silver.agg_trades` | ⏳ (no dlt source yet) |
| bookDepth/... | empty dirs | ✗ | ✗ | ✗ | — | No data in archive |

### Silver Schema Column Counts (validated)

| Silver Table | Columns | Includes |
|-------------|---------|----------|
| `silver.klines` | 19 | ts_event, ts_recv, open, high, low, close, volume, quote_volume, trade_count, taker_buy_volume, taker_buy_quote_volume, source, exchange, trade_type, symbol, interval, data_type, ingested_at, ts_date |
| `silver.agg_trades` | 18 | ts_event, ts_recv, price, size, side, trade_id, is_buyer_maker, agg_trade_id, first_trade_id, last_trade_id, rtype, source, exchange, trade_type, symbol, data_type, ingested_at, ts_date |
| `silver.funding_rate` | 12 | ts_event, ts_recv, funding_rate, mark_price, funding_timestamp, source, exchange, trade_type, symbol, data_type, ingested_at, ts_date |

### Archive Structure (data.binance.vision)

```
data/
├── spot/
│   ├── daily/     klines(3,591 syms, 13 intv)  aggTrades(3,599 syms)  trades(3,599 syms)
│   └── monthly/   klines(3,602 syms, 16 intv)  aggTrades(3,597 syms)  trades(3,597 syms)
├── futures/um/
│   ├── daily/     aggTrades bookDepth bookTicker indexPriceKlines klines
│   │              markPriceKlines metrics premiumIndexKlines trades  (833 syms each)
│   └── monthly/   aggTrades bookTicker fundingRate indexPriceKlines klines
│                  markPriceKlines premiumIndexKlines trades  (833 syms each)
├── futures/cm/
│   ├── daily/     Same as um but 267 syms, USD_PERP naming (BTCUSD_PERP)
│   └── monthly/   Same as um less bookDepth/metrics/liquidationSnapshot
└── option/
    └── daily/     BVOLIndex(2 syms)  EOHSummary(5 syms)
```

File naming: `{symbol}-{dataType}-{date}.zip` with companion `.CHECKSUM`
Date format: `YYYY-MM-DD` (daily) or `YYYY-MM` (monthly)
Klines have interval subdirectory: `{symbol}/{interval}/{symbol}-{dataType}-{interval}-{date}.zip`

## Data Pipeline Architecture

### Legacy Pipeline (existing commands: download, verify, gap-fill, sink, health)

```
Archive (S3) → Download → Verify
                    ↓
GapFillWorkflow → REST API fill
                    ↓
HealthCheckWorkflow → completeness, freshness, integrity
                    ↓
SinkWorkflow → Polars → DuckLake v1.0
```

### dlt Pipeline (Prefect-orchestrated: ``dlt_sqlmesh_pipeline``)

```
dlt resources (from binance_datatool.dlt.resources):
  klines_resource     ─┐
  agg_trades_resource ─┤  → DuckDB Bronze (one table per data type)
  funding_rate_res.   ─┤
  archive_data_res.   ─┤
  ws_klines_resource  ─┘
           ↓
Polars transforms (from binance_datatool.transforms):
  bronze_klines_to_silver()        → silver.klines
  bronze_agg_trades_to_silver()   → silver.agg_trades
  bronze_funding_rate_to_silver() → silver.funding_rate
           ↓
Pandera validation (at Polars boundary)
           ↓
DuckDB Silver tables (via storage.duckdb)
```

### Validation Layer (``binance_datatool.validation``)

| Layer | Pandera Schema | Pydantic Model | Enforces |
|-------|---------------|----------------|----------|
| Bronze klines | ``BronzeKlinesSchema`` | ``KlineModel`` | types, nullability, ge/le, high>=low |
| Bronze aggTrades | ``BronzeAggTradesSchema`` | ``RawAggTradeModel`` | VARCHAR-only columns, nullability |
| Bronze fundingRate | ``BronzeFundingRateSchema`` | ``RawFundingRateModel`` | VARCHAR-only columns, nullability |
| Silver klines | ``SilverKlinesSchema`` | — | 19 silver columns, cross-column checks |
| Silver aggTrades | ``AggTradesSilverSchema`` | — | 18 columns, price>0, size>=0, include first/last_trade_id |
| Silver fundingRate | ``FundingRateSilverSchema`` | — | 12 columns |
| dlt REST klines | — | ``KlineModel`` | pydantic field validators (dlt authoritative) |
| dlt REST aggTrades | — | ``AggTradeModel`` | price>0, quantity>=0 |
| dlt REST fundingRate | — | ``FundingRateModel`` | type-safe fields |

### Prefect Flow: ``dlt_sqlmesh_pipeline``

```
dlt_sqlmesh_pipeline(symbol, data_type, source, trade_type)
                      │
           ┌──────────┴──────────┐
           │  data_type dispatch  │
           │ klines | aggTrades  │
           │ fundingRate         │
           └──────────┬──────────┘
                      │
        ┌─────────────┼─────────────┐
        │ source=rest │ source=arch │ source=ws
        │ run_dlt_*   │ run_dlt_*   │ run_dlt_*
        └─────────────┴─────────────┘
                      ↓
          DuckLake (dlt destination — Parquet + catalog)
                      ↓
           transform_*_to_silver()
           (Polars + Pandera)
                      ↓
          DuckLake silver tables
                      ↓
           run_sqlmesh_plan()  (optional)
```

### Running Workflows with Prefect

```bash
# Serve deployments (both daily backfill + hourly metadata — no server/worker needed)
uv run python -m binance_datatool.workflow.prefect_flows serve

# In another terminal, trigger a run
uv run prefect deployment run 'Historical Data Pipeline/historical_pipeline'

# List symbols from local catalog (instant, no network)
uv run binance-datatool list-symbols spot --from-catalog --catalog /path/to/lake

# Or run flows directly via Python
uv run python3 -c "
from binance_datatool.workflow.prefect_flows import historical_pipeline, bulk_backfill

# Single symbol (last 3 days of 1h klines)
result = historical_pipeline(trade_type='spot', symbols=['BTCUSDT'],
                             data_type='klines', interval='1h', lookback_days=3)
print(result)

# Multi-symbol (parallel via .map(), DuckDB serialized via concurrency guard)
result = historical_pipeline(trade_type='spot', symbols=['BTCUSDT', 'ETHUSDT'],
                             data_type='klines', interval='1h', lookback_days=3)
print(result)

# Bulk backfill with explicit symbols
result = bulk_backfill(trade_type='spot', symbols=['BTCUSDT', 'ETHUSDT', 'SOLUSDT'],
                       data_type='klines', interval='1h', lookback_days=3)
print(result)

# Bulk backfill with auto-discovered symbols (max 10 by default, configurable)
result = bulk_backfill(trade_type='um', max_symbols=5)
print(result)
"
```

## CLI Commands Reference

### archive commands
```bash
binance-datatool list-symbols spot --freq daily --type klines --quote USDT
binance-datatool list-files --symbols BTCUSDT --trade-type spot --type klines --interval 1d
binance-datatool download --symbols BTCUSDT --trade-type spot --type klines --interval 1d --dry-run
binance-datatool verify --symbols BTCUSDT --trade-type spot --type klines --interval 1d
```

### gap-fill command (REST API backfill)
```bash
# Auto-detect gaps and fill (uses REST API via SDK)
binance-datatool gap-fill spot --auto-detect --symbol BTCUSDT --type klines --interval 1h --lookback 30
# Manual time range
binance-datatool gap-fill spot --symbol BTCUSDT --type klines --interval 1h --start-time 1700000000000 --end-time 1700088000000
```

### health command (data quality monitoring)
```bash
# Check completeness, freshness, integrity
binance-datatool health spot --type klines --interval 1h BTCUSDT --max-stale 3
```

### sink command (transform to Parquet/DuckDB)
```bash
# Transform archive data to partitioned Parquet
binance-datatool sink spot --type klines --interval 1h --target parquet --catalog /path/to/lake BTCUSDT
# Load into DuckDB as well
binance-datatool sink spot --type klines --interval 1h --target all --duckdb /path/to/db.duckdb BTCUSDT
```

### refresh-metadata (venue/symbol tables)
```bash
# From archive (symbols discovered via S3 listing)
binance-datatool refresh-metadata spot --catalog /path/to/lake
# From REST API (richer metadata: status, contract type)
binance-datatool refresh-metadata um --from-api --catalog /path/to/lake
# Register as DuckLake native tables:
binance-datatool refresh-metadata spot --catalog /path/to/lake --duckdb /path/to/db.duckdb
```

## Key Design Patterns

- **SinkWorkflow**: Polars-based transform + Parquet write + DuckDB load
- **GapFillWorkflow**: Auto-detect gaps via `_scan_existing_dates()` → `_fetch_data()` → `_save_filled()`
- **HealthCheckWorkflow**: Scans local archive for completeness (missing dates), freshness (staleness), integrity (checksums)
- **Lineage**: Every data operation records `LineageEvent` via `LineageTracker` (exportable to JSON)
- **DuckLake catalog**: Uses official DuckLake v1.0 format (`ATTACH 'ducklake:metadata.ducklake'`). Lake views scan Parquet in-place via `read_parquet()` — zero copy, no data duplication.
- **Canonical exchange naming**: All pipeline paths use ``exchange_for(trade_type)`` from ``common/enums.py`` — the single source of truth for DuckLake exchange names (``"binance-spot"``, ``"binance-perps-um"``, ``"binance-perps-cm"``). Legacy ``sink.py`` now uses this function too. Eliminates data inconsistency between pipeline paths.

## Silver Layer (Transform/Normalize)

The Silver layer normalizes Bronze archive data into unified schemas following
Databento DBN and tardis.dev conventions.

### Silver Schema (klines)
| Silver Column | Bronze Source | Type | Description |
|--------------|---------------|------|-------------|
| `ts_event` | `open_time` | INT64 μs | Event timestamp (DBN convention, epoch microseconds) |
| `ts_recv` | Auto | INT64 μs | Receive timestamp (DBN convention, epoch microseconds) |
| `open` | CSV column | FLOAT64 | Open price |
| `high` | CSV column | FLOAT64 | High price |
| `low` | CSV column | FLOAT64 | Low price |
| `close` | CSV column | FLOAT64 | Close price |
| `volume` | CSV column | FLOAT64 | Base volume |
| `quote_volume` | CSV column | FLOAT64 | Quote volume |
| `trade_count` | `count` | INT64 | Number of trades |
| `taker_buy_volume` | CSV column | FLOAT64 | Maker buy volume |
| `taker_buy_quote_volume` | CSV column | FLOAT64 | Maker buy quote volume |
| `source` | Auto | UTF8 | `"archive"`, `"api_filled"`, `"ws_stream"` |
| `exchange` | Auto | UTF8 | `"binance-spot"`, `"binance-perps-um"`, `"binance-perps-cm"` |
| `trade_type` | Auto | UTF8 | `"spot"`, `"um"`, `"cm"` |
| `symbol` | Auto | UTF8 | e.g. `"BTCUSDT"` |
| `interval` | Auto | UTF8 | e.g. `"1h"` |
| `data_type` | Auto | UTF8 | `"klines"` |
| `ingested_at` | Auto | INT64 μs | Ingestion timestamp |

### DuckLake Catalog Path (self-describing)
```
data/exchange=binance-spot/data-type=klines/symbol=BTCUSDT/interval=1h/date=2026-05-08/data.parquet
```

Partition levels (all Hive-style):
| Level | Example | Purpose |
|-------|---------|---------|
| `exchange` | `binance-spot`, `binance-perps-um`, `binance-perps-cm` | Venue + market type |
| `data-type` | `klines`, `aggTrades`, `fundingRate` | Dataset type |
| `symbol` | `BTCUSDT` | Trading pair |
| `interval` | `1h`, `1m` (klines only) | Bar size |
| `date` | `2026-05-08` | Temporal partition |

The file is always `data.parquet` — no redundancy with path components.

### Metadata Tables
```
{lake}/metadata/venues.parquet     — Venue metadata (3 venues)
{lake}/metadata/symbols.parquet    — Symbol metadata (all symbols per trade type)
```

### Silver Spec
See `docs/silver-layer-spec.md` for full schema definitions for klines, trades,
aggTrades, and funding rate across all trade types.

### Populate Metadata from Archive/API
```bash
# From archive (symbols discovered via S3 listing)
binance-datatool refresh-metadata spot --catalog /path/to/lake

# From REST API (richer metadata: status, contract type)
binance-datatool refresh-metadata um --from-api --catalog /path/to/lake
```

## E2E Data Correctness Pipeline

The canonical E2E correctness test suite lives at `tests/test_e2e_correctness.py`. It validates
the full raw→bronze→silver pipeline with field-level mappings for all data types × trade types.

### Test Matrix

| Test Class | Data Types | Trade Types | Validates |
|------------|-----------|-------------|-----------|
| `TestRestKlinesCorrectness` | klines | spot, um | bronze columns, ts_event, ts_date, exchange, source, field types |
| `TestRestAggTradesCorrectness` | aggTrades | spot, um | bronze columns, side derivation, size mapping, first/last_trade_id, rtype |
| `TestRestFundingRateCorrectness` | fundingRate | um, cm | bronze columns, mark_price, funding_timestamp, exchange |
| `TestArchiveKlinesCorrectness` | klines (archive ZIP) | spot | bronze columns, μs auto-detection, ts_event, ts_date, exchange |
| `TestCrossTableConsistency` | all | — | ts_date=DATE type, exchange=DuckLake convention across all tables |

### Running

```bash
# Full E2E suite (makes real Binance API calls + S3 requests)
uv run pytest tests/test_e2e_correctness.py --run-integration -v

# Unit tests only (no network)
uv run pytest tests/ -q
```

### Data Flow (raw → bronze → silver)

```
REST API (Binance SDK)
  ↓ klines_resource / agg_trades_resource / funding_rate_resource
  ↓ dlt pipeline (write_disposition="merge", schema_contract="evolve")
DuckDB bronze tables (all VARCHAR: open_time TEXT, price TEXT, ...)
  ↓ bronze_klines_to_silver() / bronze_agg_trades_to_silver() / bronze_funding_rate_to_silver()
  ↓ Polars type casting + field renaming + metadata enrichment
  ↓ Pandera schema validation (SilverKlinesSchema / AggTradesSilverSchema / FundingRateSilverSchema)
DuckDB silver tables (typed: ts_event BIGINT, price DOUBLE, ...)
  ↓ Assertions: column presence, exchange naming, ts_date type, field mapping correctness

Archive (S3 ZIP)
  ↓ archive_data_resource() — dlt fetch + CSV parse
  ↓ DuckDB bronze (typed: open_time BIGINT, open DOUBLE, ...)
  ↓ Same Polars transforms + μs auto-detection
  ↓ Same silver schema + assertions
```

### Known Data Correctness Issues (Resolved)
- **Archive μs timestamps**: Binance switched from ms (13-digit) to μs (16-digit) in archive CSVs. Transform auto-detects via `open_time >= 1e15` and uses correct divisor (86400000000 vs 86400000).
- **Empty mark_price**: CM fundingRate API may return empty strings for `mark_price`. Transform replaces `""` with `"0"` before Float64 cast.
- **First/last_trade_id restored**: Previously dropped in aggTrades silver; now preserved from REST API and archive CSV.

## Repository Boundaries
- `temp/` is git-ignored and may contain temporary or non-public materials.
- Do not treat `temp/` as part of the public package surface or public project documentation.
- Keep checked-in guidance focused on the package and stable workflows, not on transient working files.
