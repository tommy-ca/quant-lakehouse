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
| ⏳ | 7.1 | FR | Add `--source` flag to CLI commands (auto/rest/archive/ws) |
| ✅ | 7.2 | FR | List-symbols via dlt metadata source (fallback to S3 listing) |
| ⏳ | 7.3 | FR | List-files via ArchiveClient or DuckDB archive cache |
| ✅ | 7.4 | FR | Download via dlt archive source (aiohttp, in-memory) — `run_dlt_archive` exists |
| ⏳ | 7.5 | FR | Verify via dlt schema_contract + Pandera (not SHA256) |
| ⏳ | 7.6 | NFR | Default `--source=auto` — detect dlt availability, fallback to legacy |
| ✅ | 8.2 | FR | Gap-fill: replace `GapFillWorkflow` with `dlt_sqlmesh_pipeline(source='rest')` |
| ✅ | 8.3 | FR | Health: replace archive-level checks with DuckLake anomaly detection |
| ⏳ | 8.5 | D | Mark legacy workflows as deprecated in docstrings |
| ✅ | 10.1 | NFR | S3 listing: use DuckDB archive cache for subsequent runs — `ArchiveFileCache` exists |
| ✅ | 10.2 | FR | Batch archive download via aria2c (keep existing code in `archive/downloader.py`) |
| ✅ | 10.3 | NFR | Parallelize archive ZIP fetching in dlt resource (asyncio.gather) |
| ⏳ | 10.4 | NFR | Add DuckLake table maintenance (CALL merge_adjacent_files) to Prefect flows |

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
