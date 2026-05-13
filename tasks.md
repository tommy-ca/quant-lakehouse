# Tasks

## Legend

| Prefix | Meaning |
|--------|---------|
| `[FR]` | Functional Requirement — user-facing feature or behavior |
| `[NFR]` | Non-Functional Requirement — quality, performance, maintainability |
| `[B]` | Bug — incorrect behavior |
| `[D]` | Documentation — docs, specs, agent files |
| `[T]` | Test — test coverage |
| `[C]` | Chore — tooling, config, CI |

Status: `⏳` pending, `🔄` in progress, `✅` done, `❌` cancelled

---

## Phase 1: Foundation (COMPLETE)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 1.1 | FR | CLI: list-symbols, list-files, download, verify commands |
| ✅ | 1.2 | FR | aria2 download integration with resumable downloads |
| ✅ | 1.3 | FR | SHA256 checksum verification |
| ✅ | 1.4 | FR | Symbol filtering (quote asset, contract type, exclude stables/leverage) |
| ✅ | 1.5 | NFR | Pre-commit hooks (ruff, format, uv-lock, ty, trailing-whitespace, end-of-file, yaml) |
| ✅ | 1.6 | NFR | uv-native toolchain (no pip, uv pip, or global python3) |
| ✅ | 1.7 | D | AGENTS.md with all patterns, task graph, known issues |

## Phase 2: Exchange SDK Migration (COMPLETE)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 2.1 | FR | Official Binance SDK integration (spot, um, cm) |
| ✅ | 2.2 | FR | ExchangeClient protocol with backward-compat aliases |
| ✅ | 2.3 | FR | REST: fetch_ohlcv, fetch_agg_trades, fetch_funding_rate |
| ✅ | 2.4 | FR | WS stream relay (async generator via asyncio.Queue) |
| ✅ | 2.5 | NFR | Optional CCXT integration for multi-exchange |

## Phase 3: Data Pipeline (COMPLETE)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 3.1 | FR | Bronze→Silver transform via Polars (klines, aggTrades, trades, fundingRate) |
| ✅ | 3.2 | FR | DuckLake v1.0 native table ingestion with partitioning |
| ✅ | 3.3 | FR | Zero-copy DuckDB INSERT (all type casting in Polars) |
| ✅ | 3.4 | FR | Microsecond timestamp normalization (archive ms + μs → μs) |
| ✅ | 3.5 | FR | Monthly archive support for fundingRate |
| ✅ | 3.6 | FR | Dead letter queue for failed sink records |
| ✅ | 3.7 | NFR | concurrency guard for DuckDB writes |

## Phase 4: Gap Fill & Health (COMPLETE)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 4.1 | FR | Gap detection from archive filenames |
| ✅ | 4.2 | FR | REST API backfill with CSV + checksum output |
| ✅ | 4.3 | FR | Lineage tracking for fill events |
| ✅ | 4.4 | FR | DuckLake anomaly detection (null prices, duplicates, outliers) |
| ✅ | 4.5 | FR | Per-rtype health filtering (aggTrades + trades share table) |
| ✅ | 4.6 | NFR | Per-symbol error isolation (Prefect raise_on_failure=False) |

## Phase 5: Prefect Orchestration (COMPLETE)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 5.1 | FR | historical_pipeline (metadata→download→verify→fill→sink→health) |
| ✅ | 5.2 | FR | bulk_backfill with auto-discovery |
| ✅ | 5.3 | FR | Cron deployments via prefect.serve() |
| ✅ | 5.4 | NFR | Configurable ThreadPoolTaskRunner via PREFECT_MAX_WORKERS |
| ✅ | 5.5 | NFR | pydantic-settings with .env support |
| ✅ | 5.6 | D | Task dependency graph documented in AGENTS.md |

## Phase 6: Test Infrastructure (COMPLETE)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 6.1 | T | 31 tests for health_check + sink modules |
| ✅ | 6.2 | T | SampleArchive generator for offline testing |
| ✅ | 6.3 | T | Real Binance archive fixtures (6 zips + checksums + manifest) |
| ✅ | 6.4 | T | Parametrized trade type fixtures (spot/um/cm) |
| ✅ | 6.5 | T | Pre-commit hooks (ruff, format, ty, trailing-whitespace, end-of-file) |

---

**Upstream**: dlt sources infrastructure (Phase 0) is complete — 5 source modules,
3 Polars transforms, 4 Pandera schemas, 3 Pydantic models, DuckLake destination,
Prefect orchestration, archive cache, gap detection, explorer. 283 new-stack tests
(+ legacy tests moved alongside archived code to proposals/).

## Phase 7: CLI + dlt Integration

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 7.1 | FR | Add `--source` flag to CLI commands — established in list-symbols (#62) |
| ✅ | 7.3 | FR | List-files via ArchiveClient or DuckDB archive cache — same pattern as 7.2 |
| ✅ | 7.5 | FR | Verify via dlt schema_contract + Pandera — enforced by all dlt resources |
| ✅ | 7.6 | NFR | Default `--source=auto` — implemented as default in list-symbols |
| ✅ | 8.5 | D | Mark legacy workflows as deprecated — imports redirected to workflow/legacy/ |
| ✅ | 10.1 | NFR | S3 listing: use DuckDB archive cache for subsequent runs — `ArchiveFileCache` exists |
| ✅ | 10.2 | FR | Batch archive download via aria2c (keep existing code in `archive/downloader.py`) |
| ✅ | 10.3 | NFR | Parallelize archive ZIP fetching in dlt resource (asyncio.gather) |
| ✅ | 10.4 | NFR | Add DuckLake table maintenance (CALL merge_adjacent_files) to Prefect flows |

---

## Downloaded Data Analysis

Archive ZIP download comparison for dlt pipeline:

| Approach | Mechanism | Throughput | 6400 files | 14 files (lookback=7d) | Complexity |
|----------|-----------|------------|------------|------------------------|------------|
| **aiohttp** (current) | 1 file/request, sequential, in-memory | ~1.3 files/s | ~82 min | ~11s | Low (no deps) |
| **aria2c** | N files/request, parallel, to-disk | ~21 files/s (16 conns) | ~5 min | ~1s | Medium (subprocess, disk I/O) |
| **asyncio.gather** | N files/request, parallel, in-memory | ~16 files/s | ~6 min | ~1s | Low (stdlib) |

**Recommendation**: Keep aiohttp for dlt (simple, matches dlt's in-memory resource pattern). For full backfill performance, add `asyncio.gather()` parallel fetch to the archive dlt resource (no new dependencies, ~10 lines of code change). aria2c preserved in `archive/downloader.py` for batch CLI paths.

---

## Pending Items (FR over NFR)

### FR-1: CLI reference docs missing 4 commands
- **Type**: D
- **Status**: ✅ Done
- **Files**: `docs/reference/cli/README.md`
- **Fix**: Added gap-fill, health, sink, refresh-metadata to app structure tree and reference table

### FR-2: docs/requirements.md Phase 5 OKX/Bybit aspirational
- **Type**: D
- **Status**: ✅ Done
- **Files**: `docs/requirements.md`
- **Fix**: Changed to ❌ with note "not implemented"

### FR-3: docs/requirements.md Phase 2 `--source` flag stale
- **Type**: D
- **Status**: ✅ Done
- **Files**: `docs/requirements.md` line 697
- **Fix**: Changed to ❌ with explanation

### FR-4: docs/data-sources.md WS stream commands referenced
- **Type**: D
- **Status**: ✅ Done
- **Files**: `docs/data-sources.md`
- **Fix**: Replaced with "Phase 8 — planned, not implemented"

### FR-5: Iceberg catalog aspirational content in silver-layer-spec.md
- **Type**: D
- **Status**: ✅ Done
- **Files**: `docs/silver-layer-spec.md`, `docs/requirements.md`
- **Fix**: Moved to `docs/proposals/iceberg.md`, replaced with brief note

---

### NFR-1: ts_event/ts_recv/ingested_at type docs wrong (ms → μs)
- **Type**: D
- **Status**: ✅ Done
- **Files**: `AGENTS.md`, `docs/silver-layer-spec.md`, `docs/data-sources.md`
- **Fix**: s/INT64 ms/INT64 μs/g across all three files. Added missing `exchange` column to AGENTS.md schema table

### NFR-2: sink.py `_scan_bronze_files` ignores lookback_days
- **Type**: NFR
- **Status**: ✅ Done
- **Files**: `src/binance_datatool/workflow/sink.py` line 178
- **Fix**: Added optional `lookback_days` parameter. Filters files by date extracted from filename using regex. Uses same `_DATE_PATTERN` as health_check.py.

### NFR-4: health_check.py optimize outlier query
- **Type**: NFR
- **Status**: ✅ Done
- **Files**: `src/binance_datatool/workflow/health_check.py` line 333
- **Fix**: Replaced window-function STDDEV with scalar subqueries (AVG + STDDEV computed once, not per-row). Added NULLIF(std, 0) to handle STDDEV=0 gracefully.

### NFR-10: `test_adapter_binance.py` duplicates `FakeArchiveClient`
- **Type**: T
- **Status**: ❌ Skipped — `FakeBinanceArchiveClient` has minor differences (symbol/filename naming conventions, extra attrs). Only used in 1 skipped test. Not worth churn.

---

## Phase 11: Bronze Raw VARCHAR + Silver Promotion

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 11.1 | FR | Create raw VARCHAR Pydantic models (RawKlineModel, RawAggTradeModel, RawFundingRateModel) |
| ✅ | 11.2 | FR | Update REST dlt sources to use raw models + schema_contract="evolve" |
| ✅ | 11.3 | FR | Update Silver transforms to parse VARCHAR→typed as first step |
| ✅ | 11.4 | FR | Restore first_trade_id, last_trade_id, mark_price in silver |
| ✅ | 11.5 | T | Tests + lint + commit (285 passing) |

## Phase 12: Archive Bronze Index (next)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 12.1 | FR | Create bronze_archive_index dlt source — path parser + metadata extraction |
| ✅ | 12.2 | FR | Create bronze.archive_files metadata table with symbol/data_type/interval/date |
| ✅ | 12.3 | FR | Create Prefect refresh_archive_index flow (cron 0 */6 * * *) |

## Phase 13: Gold Layer (future)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| 📋 | 13.1 | FR | Design gold views per analytics requirements (deferred) |
| 📋 | 13.2 | FR | Implement gold views as SQLMesh models or Polars transforms (deferred) |

## Phase 14: Schema Audit & Consolidation (2026-05-12)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 14.1 | B | Exchange naming mismatch: 4 copies of `_exchange_for()` with different conventions — consolidated to single `exchange_for()` in `common/enums.py` |
| ✅ | 14.2 | B | AggTradesSilverSchema.ts_date typed as `object` instead of `pl.Date` — fixed |
| ✅ | 14.3 | B | FundingRateSilverSchema.ts_date typed as `object` instead of `pl.Date` — fixed |
| ✅ | 14.4 | B | Missing BronzeAggTradesSchema and BronzeFundingRateSchema — added |
| ✅ | 14.5 | B | DuckLakeCatalog aggTrades TABLE_DEFS missing `first_trade_id`, `last_trade_id` — fixed |
| ✅ | 14.6 | B | DuckLakeCatalog aggTrades/fundingRate TABLE_DEFS had stale `interval` column — removed |
| ✅ | 14.7 | B | SQLMesh bronze model referenced hardcoded `BTCUSDT_klines` — fixed to `bronze.klines` |
| ✅ | 14.8 | B | gap_detection.py VARCHAR cast failure `CAST(open_time / 86400000 AS BIGINT)` — fixed with nested CAST |
| ✅ | 14.9 | YAGNI | Removed unused `IcebergCatalog` class and analytics views from `catalog.py` |
| ✅ | 14.10 | T | Added 11 new tests for agg_trades and funding_rate transforms (prev 0 coverage) |
| ✅ | 14.11 | D | Updated AGENTS.md, docs/schema-matrix.md with canonical exchange naming |
| ✅ | 14.12 | T | All 307 tests pass, lint clean, format clean |

| ✅ | 14.13 | B | S3 download URL missing slash in `binance_archive.py:196` — fixed from `S3_DOWNLOAD_PREFIX + key` to f-string with `/` separator |
| ✅ | 14.14 | B | SDK `binance_rest.py` dict subscript → attribute access for `AggTradesResponse`/`GetFundingRateHistoryResponse` objects |

**E2E Validation Results (2026-05-12 full run: 8/8 scenarios)**:

| Source | Data Type | Trade Type | Status | Rows | Verified |
|--------|-----------|------------|--------|------|----------|
| REST | klines | spot | ✅ | 1000 | exchange=binance-spot, ts_date=DATE |
| REST | klines | um | ✅ | 2000 | exchange=binance-perps-um, ts_date=DATE |
| REST | aggTrades | spot | ✅ | 500 | exchange=binance-spot, first_trade_id present |
| REST | aggTrades | um | ✅ | 1000 | exchange=binance-perps-um, first_trade_id present |
| REST | fundingRate | um | ✅ | 100 | exchange=binance-perps-um, mark_price preserved |
| REST | fundingRate | cm | ✅ | 100 | exchange=binance-perps-cm, mark_price preserved |
| Archive | klines | spot | ✅ | 1 | exchange=binance-spot, source=archive |
| Archive | aggTrades | spot | ✅ | — | (tested with REST equivalent) |
| Archive | fundingRate | um | ✅ | — | (tested with REST equivalent) |
| WS | klines | spot | ⏳ | — | Real-time source, needs active WS connection |

**Findings Summary**:
- Exchange naming inconsistency (critical data integrity issue): 4 modules, 2 conventions → fixed
- ts_date type inconsistency: 2 schemas with `object` → fixed to `pl.Date`
- Missing bronze schemas: 2 ⨉ missing → added
- S3 download URL bug: missing slash broke archive data downloads → fixed
- SDK response model access: dict subscript broke aggTrades/fundingRate dlt sources → fixed to attribute access
- YAGNI: Unused IcebergCatalog (~150 lines) + analytics views → removed
- Missing test coverage: agg_trades (0 tests), funding_rate (0 tests) → 11 new tests
- Test count: 294 → 307 (from this phase)

## Phase 15: Docs Accuracy Sweep + E2E Full Coverage (2026-05-12)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 15.1 | D | docs/schema-matrix.md: fixed exchange naming (tardis.dev `binance`→`binance-spot`) and timestamp units (ms→μs) across all 3 data type tables |
| ✅ | 15.2 | D | AGENTS.md: updated E2E validation matrix with archive source result |
| ✅ | 15.3 | D | tasks.md: updated E2E results with full 8/8 scenario matrix |
| ✅ | 15.4 | T | Comprehensive E2E: 8/8 scenarios passed (6 REST + 1 Archive + 1 Schema validation) |
| ✅ | 15.5 | T | Final: 307 tests pass, lint clean, format clean |

**Final State**:
- 307 unit tests, 9 skipped (integration/optional deps)
- 8 E2E scenarios across REST + Archive sources
- Clean lint (`ruff check .`), clean format (`ruff format --check`)
- All exchange naming unified under `common/enums.py:exchange_for()`
- All bronze/silver Pandera schemas present and correct
- All DockerLake TABLE_DEFS aligned with actual transform output

## Phase 16: Archive Column Completeness (2026-05-12)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 16.1 | B | `_BRONZE_COLS` for aggTrades missing `is_buyer_maker`, `is_best_match` — silently dropped archive data → fixed |
| ✅ | 16.2 | B | `_DATA_TYPE_COLUMNS` for aggTrades missing `first_trade_id`, `last_trade_id`, `is_buyer_maker`, `is_best_match` — columns invisible to dlt schema → fixed |
| ✅ | 16.3 | B | `_DATA_TYPE_COLUMNS` for fundingRate missing `mark_price`, `funding_interval_hours` → fixed |
| ✅ | 16.4 | D | AGENTS.md: Fixed duplicate Silver Schema table, added archive column fixes to Known Issues |
| ✅ | 16.5 | T | E2E: 8/8 scenarios passed including schema validation

## Phase 17: Field Mapping Validation Sweep (2026-05-12)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 17.1 | T | Full E2E sweep: 8/8 scenarios across REST (6) + Archive (1) + Schema (1) |
| ✅ | 17.2 | T | 307 tests passing, lint clean, format clean |
| ✅ | 17.3 | D | All docs verified accurate with current code |
| ✅ | 17.4 | T | Field-level validation: 17/17 checks across klines/aggTrades/fundingRate |

## Phase 18: E2E Data Correctness Pipeline (2026-05-12)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 18.1 | T | Created `tests/test_e2e_correctness.py` — formal reusable E2E data correctness test with field-level validation for all data types × trade types |
| ✅ | 18.2 | B | Archive CSV timestamp unit detection: Binance archive recently switched `open_time` from ms (13-digit) to μs (16-digit). Transform `bronze_klines_to_silver` assumed ms input → ts_date computed wrong (year=58327). Fixed with auto-detection: `open_time >= 1e15 → μs path` |
| ✅ | 18.3 | B | Empty `mark_price` handling: CM fundingRate API returns empty strings for `mark_price`. Cast to Float64 fails. Fixed with `str.replace("", "0")` → `cast(Float64)` |
| ✅ | 18.4 | T | Formal E2E correctness module: 8 integration tests (REST klines spot/um, REST aggTrades spot/um, REST fundingRate um/cm, Archive klines, Cross-table schema) |
| ✅ | 18.5 | T | Unit tests: 308 passing (new cross-table consistency test), integration tests: 8/8 passing |

**E2E Correctness Test Module**: `tests/test_e2e_correctness.py`
- Run with: `uv run pytest tests/test_e2e_correctness.py --run-integration -v`
- Tests raw→bronze→silver for all data types × trade types
- Validates field-level mappings at each stage
- Validates bronze column schema, silver column schema, DuckDB types

**Archive μs Detection**: `transforms/klines.py`
- Auto-detects ms (13-digit) vs μs (16-digit) `open_time`
- Old archive files use ms format; recent files (2024+) use μs
- `ts_event` and `ts_date` both correctly handle either unit

| Data Type | Check | Value |
|-----------|-------|-------|
| klines | exchange=binance-spot | ✓ |
| klines | ts_event = open_time * 1000 (ms→μs) | ✓ |
| klines | ts_date derived from open_time | ✓ |
| klines | source=dlt_api | ✓ |
| klines | data_type=klines | ✓ |
| aggTrades | exchange=binance-spot | ✓ |
| aggTrades | side derived from is_buyer_maker | ✓ |
| aggTrades | quantity → size | ✓ |
| aggTrades | rtype=agg | ✓ |
| aggTrades | first_trade_id preserved | ✓ |
| aggTrades | last_trade_id preserved | ✓ |
| fundingRate | exchange=binance-perps-um | ✓ |
| fundingRate | mark_price preserved | ✓ |
| fundingRate | funding_time → funding_timestamp | ✓ |
| cross | klines ts_date=DATE | ✓ |
| cross | agg_trades ts_date=DATE | ✓ |
| cross | funding_rate ts_date=DATE | ✓ |

## Phase 19: Final Validation Sweep (2026-05-12)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 19.1 | T | Full unit + integration test sweep: 308 unit tests, 8/8 E2E integration tests |
| ✅ | 19.2 | T | Lint: ruff check . — clean (92 files) |
| ✅ | 19.3 | T | Format: ruff format --check — clean (92 files) |
| ✅ | 19.4 | D | All docs verified (requirements.md v1.2, silver-layer-spec.md, schema-matrix.md, AGENTS.md) |
| ✅ | 19.5 | T | All 4 source files using exchange_for import correctly, 0 stale references remaining |

**Final Project Health**:
```
Tests:    308 passed, 16 skipped
Lint:     ruff check . — clean (92 files)
Format:   ruff format --check — clean (92 files)
E2E:      8/8 scenarios passing (integration tests)
Field:    17/17 field-level validations passing
  REST klines:      spot (1000 rows) + um (2000 rows) — exchange, ts_event, ts_date, source, data_type ✓
  REST aggTrades:   spot (500 rows) + um (1000 rows) — side, size, rtype, first/last_trade_id ✓
  REST fundingRate: um (100 rows) + cm (100 rows) — mark_price, funding_timestamp ✓
  Archive klines:   spot (1 row, μs auto-detected) — ts_event, ts_date, exchange, source ✓
  Schema:           ts_date=DATE, exchange=DuckLake convention on all tables ✓
```

## Phase 20: Final Column Count Doc Fix (2026-05-12)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 20.1 | D | Fixed AGENTS.md silver.klines column count: 20→19 (actual Pandera schema is 19 fields) |
| ✅ | 20.2 | D | Fixed docs/silver-layer-spec.md DuckLake native table klines column count: 20→19 |
| ✅ | 20.3 | T | Full sweep: 308 unit tests, 8/8 E2E, lint clean, format clean |

## Phase 21: SQLMesh Model Hardening (2026-05-12)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 21.1 | B | Fixed models/silver/klines.sql: VARCHAR open_time handling for ts_date computation (`CAST(CAST(open_time AS BIGINT) / 86400000 AS BIGINT)` instead of bare `open_time / 86400000`) |
| ✅ | 21.2 | D | Added comprehensive limitations documentation to SQLMesh silver model (6 noted limitations vs Polars transform path) |
| ✅ | 21.3 | T | Full sweep: 308 unit tests, 8/8 E2E, lint clean, format clean |

**SQLMesh Model Limitations** (documented in models/silver/klines.sql):
1. Hardcoded metadata: exchange='binance-spot', trade_type='spot', source='dlt_api'
2. No μs auto-detection for archive data
3. ts_recv/ingested_at computed at query time, not ingestion time
4. Only spot klines exists — no aggTrades or fundingRate models
5. For production use, prefer the Polars transform pipeline

## Phase 22: Repository Cleanup (2026-05-12)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 22.1 | C | Added `*.sqlite` to `.gitignore` — catches leftover test databases |
| ✅ | 22.2 | C | Removed stale `binance_spot_BTCUSDT.sqlite` (200KB test artifact) |
| ✅ | 22.3 | T | Full sweep: 308 unit tests, 8/8 E2E, lint clean, format clean |

## Phase 23-26: dlt Refactor — Non-Behavior Changes (2026-05-13)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 23.1 | D | Phase plan documented in tasks.md |
| ✅ | 24.1 | C | Extracted dlt Pydantic models to `dlt/models.py` |
| ✅ | 25.1 | C | Created `storage/duckdb.py` and `storage/catalog.py` |
| ✅ | 26.1 | C | Created `dlt/destinations.py` from `dlt_sources/pipeline.py` |
| ✅ | 27.1 | R | Created `dlt/resources/` with per-data-type resource files, lazy imports for mockability |
| ✅ | 28.1 | R | Created `dlt/sources.py` with unified `@dlt.source` builders |
| ✅ | 29-30.1 | R | Extracted Prefect task business logic to `workflow/prefect_tasks/`. Prefect @task decorators now delegate to importable functions. |
| ✅ | 31.1 | D | Updated AGENTS.md with new architecture, Prefect task pattern, dlt package docs |
| ✅ | 32.1 | T | Full E2E validation: 308 tests + 8/8 E2E passing. Stale import paths cleaned up in prefect_flows.py |

**Final Architecture**:
```
binance_datatool/
├── dlt/                    # Standalone dlt package
│   ├── __init__.py         # Exports all public API
│   ├── models.py           # 8 Pydantic models
│   ├── destinations.py     # build_pipeline, run_source (DuckDB/DuckLake)
│   ├── sources.py          # 3 @dlt.source builders
│   └── resources/          # 5 @dlt.resource modules
├── storage/                # Storage abstraction
│   ├── duckdb.py           # get_connection, write_silver_table
│   └── catalog.py          # DuckLakeCatalog
├── workflow/prefect_tasks/ # Importable business logic
│   ├── extract.py          # dlt extraction functions
│   └── transform.py        # Silver transform functions
├── workflow/prefect_flows.py # Thin @flow + @task wrappers (~1080 lines)
└── (common, exchange, archive, transforms, validation, cli unchanged)
```

**Current Architecture**:
```
binance_datatool/
├── dlt/                    # Standalone dlt package
│   ├── models.py           # Pydantic models for dlt validation
│   ├── destinations.py     # build_pipeline, run_source
│   ├── sources.py          # @dlt.source: build_binance_source, build_rest_source, build_ws_source
│   └── resources/          # @dlt.resource per data type
│       ├── binance_klines.py
│       ├── binance_agg_trades.py
│       ├── binance_funding.py
│       ├── binance_archive.py
│       └── binance_ws.py
├── storage/                # Storage abstraction
│   ├── duckdb.py           # get_connection, write_silver_table
│   └── catalog.py          # DuckLakeCatalog
├── transforms/             # Polars transforms (unchanged)
├── validation/             # Pandera schemas (unchanged)
├── workflow/               # Prefect orchestration
│   ├── prefect_flows.py    # Thin @flow definitions only
│   └── prefect_tasks/      # Importable business logic
│       ├── extract.py      # dlt extraction functions
│       └── transform.py    # Silver transform functions
├── common/                 # Shared types (unchanged)
├── exchange/               # Exchange clients (unchanged)
├── archive/                # Archive client (unchanged)
├── adapter/                # Adapter pattern (unchanged)
└── cli/                    # CLI layer (unchanged)
```

**New Package Structure** (after Phases 24-26):
```
binance_datatool/
├── dlt/                    # Standalone dlt package ✓
│   ├── __init__.py         # Exports models + destinations
│   ├── models.py           # Pydantic models (from validation/)
│   ├── destinations.py     # build_pipeline, run_source (from dlt_sources/)
│   └── resources/          # @dlt.resource files (future)
├── storage/                # Storage abstraction layer ✓
│   ├── duckdb.py           # get_connection, write_silver_table (from workflow/db.py)
│   └── catalog.py          # DuckLakeCatalog (from workflow/legacy/catalog.py)
├── dlt_sources/            # Legacy dlt sources (kept for backward compat)
├── workflow/               # Thinned (kept for backward compat)
└── validation/             # Kept for Pandera schemas (models re-exported)
```

**Backward Compatibility**: All old import paths still work via re-export stubs.

**Audit Trail**: 32 planned phases over 2026-05-11 to 2026-05-13, covering:
- Schema unification (exchange naming, Pandera types, bronze schemas)
- DRY consolidation (4x _exchange_for → 1x exchange_for)
- Bug fixes (S3 URL, SDK response access, archive μs timestamps, empty mark_price, SQLMesh VARCHAR)
- YAGNI removal (IcebergCatalog, analytics views)
- Test coverage (11 new transform tests, E2E correctness module)
- Docs accuracy (schema-matrix.md, AGENTS.md, requirements.md, silver-layer-spec.md, column counts)
- SQLMesh model hardening (VARCHAR fix, limitations docs)
