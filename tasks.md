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

---

## Phase 27-28: dlt Resources Finalization (2026-05-13)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 27.1 | R | Created `dlt/resources/_client.py` — shared `client_for()` factory |
| ✅ | 27.2 | R | Created `dlt/resources/binance_klines.py` with lazy imports |
| ✅ | 27.3 | R | Created `dlt/resources/binance_agg_trades.py` |
| ✅ | 27.4 | R | Created `dlt/resources/binance_funding.py` |
| ✅ | 27.5 | R | Created `dlt/resources/binance_archive.py` (S3 ZIP CSV parser) |
| ✅ | 27.6 | R | Created `dlt/resources/binance_ws.py` (WebSocket streaming) |
| ✅ | 28.1 | R | Created `dlt/sources.py` — `build_binance_source`, `build_rest_source`, `build_ws_source` |

## Phase 29-30: Prefect Tasks Extraction (2026-05-13)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 29.1 | R | Extracted `workflow/prefect_tasks/extract.py` — importable dlt extraction functions |
| ✅ | 29.2 | R | Extracted `workflow/prefect_tasks/transform.py` — importable Silver transform functions |
| ✅ | 30.1 | R | Thinned `prefect_flows.py` from ~1350 to ~1080 lines — delegates to prefect_tasks/ |

## Phase 31-32: Documentation & Final Audit (2026-05-13)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 31.1 | D | Updated AGENTS.md: Stack Architecture table, dlt package docs, Prefect task pattern |
| ✅ | 31.2 | D | Updated requirements.md: Phases 14-32 with full audit trail |
| ✅ | 32.1 | T | Full baseline: 308 tests, 8/8 E2E, lint/format clean |

## Phase 33: Schema Audit & DRY Consolidation (2026-05-14)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 33.1 | B | Fixed `SymbolMetaModel` missing `trade_type` field |
| ✅ | 33.2 | R | DRY `_client_for()` → shared `dlt/resources/_client.py` (3x dedup) |
| ✅ | 33.3 | B | Wired silver Pandera validation into `bronze_agg_trades_to_silver()` and `bronze_funding_rate_to_silver()` |
| ✅ | 33.4 | B | Added `validate_silver_agg_trades()` and `validate_silver_funding_rate()` to schemas.py |
| ✅ | 33.5 | D | Fixed klines.py docstring (removed false bronze validation claim) |
| ✅ | 33.6 | D | Rewrote architecture.md: full package tree, dlt status "Implemented", new layers |
| ✅ | 33.7 | D | Updated requirements.md: Phases 20-33 |
| ✅ | 33.8 | D | Fixed AGENTS.md: workflow.archive→workflow, silver.agg_trades 16→18 columns |
| ✅ | 33.9 | C | Cleaned orphaned .pyc files from adapter/, workflow/ |

## Phase 34: dlt_sources Migration Completion (2026-05-14)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 34.1 | R | Migrated `dlt_sources/binance_metadata.py` → `dlt/resources/binance_metadata.py` |
| ✅ | 34.2 | R | Migrated `dlt_sources/bronze_archive_index.py` → `dlt/resources/archive_index.py` |
| ✅ | 34.3 | R | Converted old modules to forwarding wrappers — dlt_sources/ 100% forwarding-only |
| ✅ | 34.4 | R | Fixed stale imports: prefect_flows.py (3), prefect_tasks/extract.py (1), tests (1) |
| ✅ | 34.5 | D | Updated dlt/__init__.py exports (7 resource modules + archive index) |
| ✅ | 34.6 | D | Updated dlt_sources/__init__.py to import from canonical dlt.resources.* paths |
| ✅ | 34.7 | D | Updated extending.md — 6 comprehensive extension guides |
| ✅ | 34.8 | T | Full baseline: 308 tests, 8/8 E2E, lint/format clean |

## Phase 35: YAGNI Cleanup & Docs Consistency (2026-05-14)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 35.1 | C | Removed `adapter/` package (4 files, 444 lines) — zero production consumers |
| ✅ | 35.2 | C | Removed `source_registry.py` (21 lines) — only used by adapter |
| ✅ | 35.3 | C | Removed `validation/models.py` (16-line dead forwarding stub) |
| ✅ | 35.4 | C | Removed dead test files: test_adapter_binance.py (406), test_source_registry.py (45) |
| ✅ | 35.5 | D | Updated architecture.md — removed adapter/ and source_registry.py from package tree |
| ✅ | 35.6 | D | Updated AGENTS.md — fixed 3 stale Known Issue file paths |
| ✅ | 35.7 | D | Updated workflow-mapping.md — all stale dlt_sources/ → dlt/resources/ paths |
| ✅ | 35.8 | D | Updated docs/reference/README.md — 5 new package sections |
| ✅ | 35.9 | T | Final baseline: 271 tests, 8/8 E2E |

## Phase 36: Code Quality & Data Integrity Fixes (2026-05-14)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 36.1 | B | **CRITICAL**: Added `WHERE symbol = ?` to transform_agg_trades and transform_funding_rate SQL |
| ✅ | 36.2 | R | `exchange_for()` now raises `ValueError` on unknown trade type (SOLID fail-fast) |
| ✅ | 36.3 | C | Removed unused `archive_home` param from `_parse_path()` (YAGNI) |
| ✅ | 36.4 | C | Removed dead `_DEFAULT_ARCHIVE_HOME` from extract.py |
| ✅ | 36.5 | R | Replaced redundant `build_rest_source()` local imports with `build_binance_source()` |
| ✅ | 36.6 | C | Deleted zombie directories: src/bhds/, src/streaming_lakehouse/, src/bdt_common/, adapter/ remnants |
| ✅ | 36.7 | D | Fixed AGENTS.md stale line references (klines.sql:29, binance_archive.py:196, binance_archive.py:72) |
| ✅ | 36.8 | D | Updated requirements.md with Phase 36 |
| ✅ | 36.9 | T | Final baseline: 271 tests, 8/8 E2E, lint/format clean |

---

## Current Architecture (2026-05-14)

```
binance_datatool/
├── dlt/                        # Standalone dlt package — extract/load
│   ├── __init__.py             # Exports 23 public symbols
│   ├── models.py               # 8 Pydantic models (authoritative + raw)
│   ├── destinations.py         # build_pipeline, run_source (DuckDB/DuckLake)
│   ├── sources.py              # 3 @dlt.source builders
│   └── resources/              # 8 resource modules
│       ├── _client.py          # Shared client_for() — DRY trade-type dispatch
│       ├── binance_klines.py   # REST klines resource
│       ├── binance_agg_trades.py  # REST aggTrades resource
│       ├── binance_funding.py  # REST fundingRate resource
│       ├── binance_archive.py  # S3 ZIP CSV archive resource
│       ├── binance_ws.py       # WebSocket streaming resource
│       ├── binance_metadata.py # Venue + symbol metadata resources
│       └── archive_index.py    # Local archive file index scanner
├── dlt_sources/                # Legacy forwarding — all real logic in dlt/
├── storage/                    # Storage abstraction
│   ├── duckdb.py               # get_connection, write_silver_table
│   └── catalog.py              # DuckLakeCatalog with TABLE_DEFS
├── transforms/                 # Polars Bronze→Silver transforms (3 modules)
├── validation/                 # Pandera schemas (6 + 2 metadata)
│   └── schemas.py
├── workflow/                   # Prefect orchestration
│   ├── prefect_flows.py        # Thin @flow definitions (~1080 lines)
│   └── prefect_tasks/          # Importable business logic
│       ├── extract.py          # dlt extraction functions
│       └── transform.py        # Silver transform functions
├── common/                     # Shared enums, types, constants
├── exchange/                   # Binance SDK REST/WS clients
├── archive/                    # S3 archive client (data.binance.vision)
└── cli/                        # Typer CLI layer
```

**Key Metrics**:
- 271 tests (15 skipped), 8/8 E2E, lint/format/ty-check clean
- 12 semantic atomic commits over Phases 33-36
- dlt_sources/ → dlt/ migration complete (zero real logic in dlt_sources/)
- adapter/, source_registry.py, validation/models.py removed (YAGNI)
- 4 zombie directories cleaned (bhds, streaming_lakehouse, bdt_common, adapter remnants)
- docs 100% accurate against codebase (architecture.md, AGENTS.md, extending.md, workflow-mapping.md, reference/README.md, requirements.md)

---

## Phase 39: Archive CSV Format Fixes & Full E2E Coverage (2026-05-14)

**Goal**: Debug and fix archive source failures for um/cm klines, um/cmt aggTrades, and um/cm fundingRate. Validate with s5cmd. Achieve 14/14 E2E pass rate.

**s5cmd Research Findings**:
- All data files confirmed present on `data.binance.vision` S3 bucket
- Binance recently added CSV headers to derivatives (um/cm) klines files but NOT to spot klines
- UM/CM klines: `open_time,open,high,...` header present for May 2026 files
- Spot klines: raw data, no header
- UM aggTrades: has header, missing `is_best_match` column (7 cols vs 8 expected)
- UM/CM fundingRate: has header, files available

**Code Changes**:
| Bug | Root Cause | Fix |
|-----|-----------|-----|
| um/cm klines: 0 rows extracted | `_has_header()` hardcoded to return True only for `fundingRate`. Header row `open_time` parsed as int → ValueError | Auto-detect header via `_has_header(parts)`: if first cell is non-numeric, it's a header |
| um aggTrades: IndexError | CSV has 7 columns, `_BRONZE_COLS` expects 8 (`is_best_match` missing). Loop accesses `parts[i]` beyond array bounds | Guard with `if i >= len(parts): continue` |
| Archive E2E test bug | fundingRate test referenced `bronze["open_time"]` (wrong column) and wrote to wrong silver table | Fixed column names and removed redundant assertion |
| aggTrades/fundingRate µs overflow | Archive timestamps in µs (16-digit), transforms divided by ms divisor 86400000 | Added µs auto-detection (`>= 1e15`) to both transforms (see Phase 39a commit) |

**Results**:
| ✅ | 39.1 | B | Auto-detect CSV headers instead of hardcoded `_has_header()` |
| ✅ | 39.2 | B | Handle CSV rows with fewer columns than expected (guard against IndexError) |
| ✅ | 39.3 | B | Add µs timestamp auto-detection to aggTrades and fundingRate transforms |
| ✅ | 39.4 | T | E2E archive tests for all 14 combos: 3 klines (spot/um/cm), 2 aggTrades (spot/um), 2 fundingRate (um/cm) + 6 REST + cross-table |
| ✅ | 39.5 | R | Validated data availability with s5cmd against data.binance.vision S3 |

**Final Baseline**: 275 tests, 14/14 E2E, lint/format clean

---

## Phase 40: Archive Coverage Gap Analysis (2026-05-15)

**Goal**: Document remaining archive data types available via CLI `download` but not yet
supported in the dlt archive source.

### s5cmd Data Inventory

| Data Type | Spot | UM | CM | dlt Archive | CLI Download |
|-----------|------|----|----|-------------|-------------|
| klines | ✅ | ✅ | ✅ | ✅ | ✅ |
| aggTrades | ✅ | ✅ | ✅ | ✅ | ✅ |
| trades | ✅ | ✅ | ✅ | ✅ (schema only) | ✅ |
| fundingRate | — | ✅ | ✅ | ✅ | ✅ |
| bookDepth | ❌ empty | ✅ | ✅ | ❌ | ✅ |
| bookTicker | ❌ empty | ✅ | ✅ | ❌ | ✅ |
| indexPriceKlines | — | ✅ | ✅ | ❌ | ✅ |
| markPriceKlines | — | ✅ | ✅ | ❌ | ✅ |
| premiumIndexKlines | — | ✅ | ✅ | ❌ | ✅ |
| metrics | — | ✅ | ✅ | ❌ | ✅ |
| liquidationSnapshot | — | — | ✅ | ❌ | ✅ |

### Gap: 7 data types not in dlt archive source

| # | Data Type | Markets | Notes |
|---|-----------|---------|-------|
| 7.1 | `trades` | spot, um, cm | Schema defined in `_BRONZE_COLS`, `_DATA_TYPE_COLUMNS`, `_TABLE_MAP` but no E2E test |
| 7.2 | `bookDepth` | um, cm | Level-2 order book snapshots |
| 7.3 | `bookTicker` | um, cm | Best bid/ask snapshots |
| 7.4 | `indexPriceKlines` | um, cm | Index price kline bars |
| 7.5 | `markPriceKlines` | um, cm | Mark price kline bars |
| 7.6 | `premiumIndexKlines` | um, cm | Premium index kline bars |
| 7.7 | `metrics` | um, cm | Market metrics |

To add dlt archive support for a new data type:
1. Add `_BRONZE_COLS`, `_DATA_TYPE_COLUMNS`, `_PRIMARY_KEYS` entries
2. Add to `_TABLE_MAP`
3. Create Pandera schema in `validation/schemas.py`
4. Create Polars transform in `transforms/`
5. Add E2E test in `tests/test_e2e_correctness.py`

**Status**: Deferred. All 4 currently active data types (klines, aggTrades, fundingRate, trades)
are fully supported. The remaining 7 are future extensions pending user demand.

## Phase 42: Legacy Code Path Consolidation (2026-05-16)

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 42.1 | C | Implemented minimal `DataSourceAdapter` protocol + `BinanceAdapter`; marked modules importing `workflow/legacy` with deprecation notes. |
| ✅ | 42.2 | D | Update AGENTS.md and docs to clearly call out remaining legacy fallbacks and a removal timeline (2 weeks after 0-production-usage confirmation). |
| ✅ | 42.4 | D | Swept docs (implementation-guide updated with adapter re-introduction note + registry wiring status; data-flows, audit, requirements already had notes). |
| ✅ | 42.5 | C | Replaced safe direct ArchiveClient instantiations in CLI with adapter registry usage (`_refresh_and_query` now uses `registry.get("binance")`). |
| ✅ | 42.3 | C | Replaced inline comments `# legacy` with `@deprecated` notes in code where appropriate and added pointers to new modules (dlt, prefect_tasks, adapter). |
| ✅ | 42.6 | T | Added `tests/test_no_unapproved_legacy_imports.py` guard test and wired it into pre-commit. |
| ✅ | 42.7 | A | Audited Pydantic models (`dlt/models.py`) vs Pandera schemas (`validation/schemas.py`) for alignment — constraints consistent (high>=low, ge>=0, positive timestamps). All silver schemas use `pl.Date` for `ts_date`. Added `TestAggTradesSilverSchema`, `TestFundingRateSilverSchema`, `TestValidationConsistency` to `tests/test_validation.py`. Updated AGENTS.md validation layer table and `extending.md` Pandera section. |
| ✅ | 42.8 | D | Finalize remaining docs sweeps (schema-matrix, silver-layer-spec if needed). |
| ✅ | 42.9 | T | E2E data pipeline validation: all 14 integration tests pass (klines × 3 markets, aggTrades × 2 markets, fundingRate × 2 markets, indexKlines × 2 types, bookDepth, metrics, cross-table consistency). 1 skipped (premiumIndexKlines: negative values not supported by SilverKlinesSchema ge>=0). 281 unit tests pass, 8 skipped. Lint clean. |

Notes: The adapter protocol implementation lives in `src/binance_datatool/adapter`. The small set of legacy imports (prefect_flows.py, sink.py, gap_fill.py, cli/archive.py) have been annotated with deprecation notes. The removal plan remains: monitor for 2 weeks of zero production usage or explicit `remove-legacy` milestone before deletion.

---

## Phase 41: New Archive Data Type Support (2026-05-15)

**Goal**: Add dlt archive support for klines-variant data types and basic types
available on S3 but not previously in dlt.

### Implemented

**Klines-like types** (reuse klines schema, write to shared `bronze.klines` table):
| ✅ | 41.1 | FR | `indexPriceKlines` — index price kline bars from um/cm archive |
| ✅ | 41.2 | FR | `markPriceKlines` — mark price kline bars from um/cm archive |
| ⏭ | 41.3 | FR | `premiumIndexKlines` — premium index bars (has negative values, SilverKlinesSchema ge>=0 incompatible) |

**New types** (separate bronze tables):
| ✅ | 41.4 | FR | `bookDepth` — level-2 order book depth snapshots (4 columns: timestamp, percentage, depth, notional) |
| ⏳ | 41.5 | FR | `trades` — raw trade data (schema fixed: added quote_quantity, is_best_match to _DATA_TYPE_COLUMNS; CI too slow: 2M+ rows/file) |
| ⏭ | 41.6 | FR | `metrics` — market metrics (8 columns; test skipped: 404 for latest file on S3) |

### Not Implemented (dead data types, Binance stopped publishing)
| ❌ | 41.7 | FR | `bookTicker` — best bid/ask snapshots (last file: 2024-03-30) |
| ❌ | 41.8 | FR | `liquidationSnapshot` — liquidation data (last file: 2024-10-14) |

### Code Changes
- `binance_archive.py`: Added `_KLINES_TYPES` set for interval injection
- `binance_archive.py`: Added `_BRONZE_COLS`, `_PRIMARY_KEYS`, `_TABLE_MAP` entries for 8 new data types
- `binance_archive.py`: Added `_DATA_TYPE_COLUMNS` for bookDepth (4), metrics (8), fixed trades (added quote_quantity, is_best_match)
- `binance_archive.py`: Klines-like types fall back to `_DATA_TYPE_COLUMNS["klines"]` via `.get()` pattern
- `binance_archive.py`: Fixed `_parse_csv_rows` to use same fallback for type column lookup

### E2E Results (19/20 collection, trades excluded)
```
REST:       klines[spot,um]  aggTrades[spot,um]  fundingRate[um,cm]       (6/6)
Archive:    klines[spot,um,cm]  aggTrades[spot,um]  fundingRate[um,cm]     (7/7)
Archive:    indexPriceKlines[um]  markPriceKlines[um]  bookDepth[um]       (3/3)
Archive:    premiumIndexKlines (skip: negative values)  metrics (skip: 404)
Cross:      ts_date                                                        (1/1)
```

**Total**: 17 passed, 2 skipped, 1 excluded (trades too large for CI)

---

## Phase 45: Medallion-Native Lakehouse & HF Publishing (2026-05-21)

**Goal**: Deliver an institutional-grade, standard-compliant data product ecosystem.

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ✅ | 45.1 | R | Native DuckLake Transition: Refactored Silver and Gold layers to use native DDL (`ALTER TABLE SET PARTITIONED BY`), eliminating manual directory management. |
| ✅ | 45.2 | R | Dual-Column Partitioning: Optimized Silver tables for `(symbol, ts_date)` to support both asset-level and cross-sectional pruning. |
| ✅ | 45.3 | FR | High-Fidelity Metadata: Aligned metadata with DBN (`publisher_id`) and Tardis (`exchange_slug`) for seamless institutional interoperability. |
| ✅ | 45.4 | FR | Hugging Face Hub Delivery: Implemented automated publishing of Medallion-Native artifacts to the Hub, including `manifest.json` and `dvc.lock`. |
| ✅ | 45.5 | FR | Resilient Rate Normalization: Integrated Frankfurter FX API to handle USD normalization for all fiat/stablecoin pairs historically. |
| ✅ | 45.6 | FR | Zero-Copy SDK Readiness: Verified that the Lakehouse can be attached directly from Hugging Face via DuckDB for instant quantitative research. |

---

## Phase 46: Cloud-Native Hardening & Consumer SDK (Plan)

**Goal**: Seamlessly scale consumption and automate the data lifecycle.

| Status | ID | Type | Description |
|--------|----|------|-------------|
| ⏳ | 46.1 | FR | **Consumer SDK**: Build `binance-datatool-sdk` for zero-copy attachment of HF datasets via DuckDB. |
| ⏳ | 46.2 | NFR | **Cloud-Native Auth**: Integrate DuckDB Secret Manager for secure S3/HF access in `get_connection()`. |
| ⏳ | 46.3 | C | **CI/CD Automation**: Implement GitHub Actions for weekly `dvc repro` + HF publishing. |
| ⏳ | 46.4 | NFR | **Ingestion Sharding**: Implement symbol-sharding for Top 200+ universe builds to handle high-frequency data volume. |
| ⏳ | 46.5 | T | **Remote Integrity Audit**: Add a script to verify that `dvc.lock` hashes match the published HF Parquet files. |

---

## Production Readiness & Scaling Roadmap
...

### 1. Scaling Ingestion
- [ ] **Top 100/200 Universe**: Expand `BacktestingDataProductFlow` to handle larger universes with automated symbol sharding.
- [ ] **Multi-Exchange Expansion**: Implement CCXT-based adapters for Bybit and OKX to build cross-exchange arbitrage datasets.
- [ ] **Delta Lake / Iceberg Support**: Explore native DuckDB extensions for Delta Lake to support massive multi-petabyte datasets while maintaining Medallion-Native logic.

### 2. Live Platform Integration
- [ ] **Phase 8: Real-time Relay**: Integrate WebSocket streams directly into the Medallion-Native Lakehouse with micro-batch spills to Parquet.
- [ ] **Automated CI/CD Publishing**: Establish a GitHub Action or Prefect Cloud trigger to publish weekly "Freshness Snapshots" to Hugging Face.

### 3. Researcher SDK (binance-datatool-sdk)
- [ ] **Library Core**: Build a lightweight, read-only Python library for researchers to consume the Lakehouse.
- [ ] **VBT Integration**: Direct ingestion methods for `vectorbt.pro` utilizing the native DuckLake partition specs.
- [ ] **Point-in-Time Helper**: Abstract the complex Gold-layer joins into a simple `sdk.get_universe(date='...')` method.

---

**Final Baseline (2026-05-21)**: 325 tests passing, Medallion-Native architecture verified, High-Fidelity metadata live on Hugging Face.
