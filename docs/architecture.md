# Architecture

This document describes the internal architecture of `binance-datatool`. It is intended for
contributors, maintainers, and AI agents working on the codebase.

## Package Tree

```
src/binance_datatool/
├── __init__.py              # Version metadata only
├── common/                  # Shared utilities (enums, constants, types, filters, symbols, progress, intervals, logging, path, settings)
├── archive/                 # Archive access (S3 HTTP client, checksum, downloader, symbol directory)
├── exchange/                # Live exchange API clients (REST/WebSocket, CCXT integration)
│   ├── client.py            # ExchangeClient protocol (@runtime_checkable)
│   ├── binance_rest.py      # BinanceSpot/Um/CmRestClient
│   ├── binance_ws.py        # BinanceSpot/Um/CmWsClient
│   ├── ccxt_rest.py         # CCXTExchangeClient (optional dependency)
│   └── ccxt_pro.py          # CCXTProExchangeClient (optional dependency)
├── dlt/                     # Standalone dlt package — extract/load
│   ├── __init__.py          # Re-exports all resources, sources, models, destinations
│   ├── models.py            # 8 Pydantic models (Kline, AggTrade, FundingRate, Venue, SymbolMeta, Raw*)
│   ├── destinations.py      # DuckDB/DuckLake destination builders (build_pipeline, run_source)
│   ├── sources.py           # @dlt.source builders (build_binance_source, build_rest_source, build_ws_source)
│   └── resources/           # 8 @dlt.resource modules + shared client factory
│       ├── _client.py       # Shared client_for() — DRY trade-type dispatch
│       ├── binance_klines.py
│       ├── binance_agg_trades.py
│       ├── binance_funding.py
│       ├── binance_archive.py  # All data types: klines, aggTrades, trades, fundingRate, bookDepth, metrics, index/mark/premium klines
│       ├── binance_ws.py
│       ├── binance_metadata.py  # Venue + symbol metadata
│       └── archive_index.py     # Local archive file index scanner
├── dlt_sources/             # Legacy forwarding modules — all real logic migrated to dlt/resources/
│   ├── binance.py           # Forwarding → dlt.resources.binance_klines + dlt.sources
│   ├── binance_rest.py      # Forwarding → dlt.resources (agg_trades, funding) + dlt.sources
│   ├── binance_ws.py        # Forwarding → dlt.resources.binance_ws + dlt.sources
│   ├── binance_archive.py   # Forwarding → dlt.resources.binance_archive
│   ├── pipeline.py          # Forwarding → dlt.destinations
│   ├── binance_metadata.py  # Forwarding → dlt.resources.binance_metadata
│   └── bronze_archive_index.py  # Forwarding → dlt.resources.archive_index
├── transforms/              # Polars transforms (Bronze → Silver)
│   ├── klines.py            # bronze_klines_to_silver() + μs auto-detection
│   ├── agg_trades.py        # bronze_agg_trades_to_silver() + side derivation + Pandera validation
│   └── funding_rate.py      # bronze_funding_rate_to_silver() + empty mark_price handling
├── validation/              # Pandera schemas + validation helpers
│   └── schemas.py           # 6 Pandera schemas (bronze/silver for klines/aggTrades/fundingRate) + venue/symbol
├── storage/                 # DuckDB/DuckLake storage layer
│   ├── duckdb.py            # get_connection(), write_silver_table()
│   └── catalog.py           # DuckLakeCatalog with TABLE_DEFS for silver tables
├── workflow/                # Business logic orchestration
│   ├── __init__.py          # Re-exports all workflow classes and result types
│   ├── _shared.py           # Shared helpers (infer_symbol_info, validate_interval)
│   ├── download.py          # ArchiveDownloadWorkflow
│   ├── verify.py            # ArchiveVerifyWorkflow
│   ├── list_files.py        # ArchiveListFilesWorkflow
│   ├── list_symbols.py      # ArchiveListSymbolsWorkflow
│   ├── results.py           # Result dataclasses (ListSymbolsResult, DiffResult, VerifyResult, etc.)
│   ├── sink.py              # SinkWorkflow — Polars-based Bronze→Silver→DuckLake
│   ├── gap_detection.py     # GapDetectionWorkflow — detect date gaps in silver tables
│   ├── gap_fill.py          # GapFillWorkflow — REST API backfill for detected gaps
│   ├── health_check.py      # HealthCheckWorkflow — completeness/freshness/integrity
│   ├── metadata.py          # MetadataWorkflow — venue/symbol metadata refresh
│   ├── explorer.py          # ExplorerWorkflow — browse local archive by data type/symbol
│   ├── archive_cache.py     # Archive scanning + date-based caching
│   ├── db.py                # Forwarding → storage.duckdb (backward compat)
│   ├── prefect_flows.py     # Prefect @flow and @task definitions (thin wrappers)
│   ├── prefect_tasks/       # Importable business logic for Prefect tasks
│   │   ├── extract.py       # run_dlt_pipeline(), download_archive_data(), build_metadata()
│   │   └── transform.py     # bronze_to_silver(), bronze_agg_trades_to_silver(), bronze_funding_rate_to_silver()
│   └── legacy/              # Legacy/archived modules
│       ├── catalog.py       # Original IcebergCatalog (trimmed down, real logic in storage/)
│       └── lineage.py       # LineageTracker (data provenance tracking)
├── cli/                     # Typer CLI layer
│   ├── __init__.py          # Root callback with -v/-vv verbosity and --archive-home
│   └── archive.py           # All CLI commands (list-symbols, list-files, download, verify, gap-fill, health, sink, refresh-metadata)
```

## Layered Design

The package follows a multi-layer architecture. Each layer depends only on the layers
below it — outer layers import inner layers, never the reverse.

```
CLI  (cli/)
 └─▶ Workflow  (workflow/)  ── Prefect orchestration (prefect_flows + prefect_tasks)
       ├─▶ dlt  (dlt/)       ── Extract/Load (REST, WS, archive resources)
       ├─▶ Transforms  (transforms/)  ── Polars Bronze→Silver
       ├─▶ Validation  (validation/)  ── Pandera schemas at pipeline boundaries
       ├─▶ Storage  (storage/)        ── DuckDB/DuckLake read/write
       ├─▶ Archive Client  (archive/) ── S3 HTTP client (data.binance.vision)
       └─▶ Common  (common/)          ── Shared enums, types, constants
```

| Layer | Package | Responsibility |
|-------|---------|----------------|
| **CLI** | `binance_datatool.cli` | Typer command definitions, argument parsing, output formatting. |
| **Workflow** | `binance_datatool.workflow` | Business logic orchestration; Prefect flows and tasks; gap detection, health checks, metadata. |
| **dlt** | `binance_datatool.dlt` | Resources, source builders, Pydantic models, destination helpers. Standalone package. |
| **Transforms** | `binance_datatool.transforms` | Polars-based Bronze→Silver transforms with Pandera validation. |
| **Validation** | `binance_datatool.validation` | Pandera DataFrame schemas for pipeline boundary validation. |
| **Storage** | `binance_datatool.storage` | DuckDB/DuckLake read/write, catalog definitions. |
| **Archive Client** | `binance_datatool.archive` | S3 HTTP communication with data.binance.vision. |
| **Exchange** | `binance_datatool.exchange` | Live REST/WebSocket API clients via official Binance SDKs. |
| **Common** | `binance_datatool.common` | Shared enums, constants, types, and symbol filters used across the project. |

### Why This Architecture

- **Testability.** Each layer can be tested independently (resources with mocked clients, transforms with fixture DataFrames).
- **Composability.** dlt resources and transforms can be composed from scripts, notebooks, or Prefect without importing CLI cruft.
- **Extensibility.** Adding a new data type means adding: dlt resource + Pydantic model + Polars transform + Pandera schema.
- **dlt standalone.** `binance_datatool.dlt` imports independently — no dependency on workflow, CLI, or archive layers.

## Data Flow

### Full Pipeline (dlt + Polars + Pandera)

```
dlt Sources (5 resource modules)
  ↓ Extract + Load → DuckDB bronze tables (all VARCHAR for REST, typed for archive)
        bronze.klines       (all trade types, all sources)
        bronze.agg_trades   (all trade types, all sources)
        bronze.funding_rate (all trade types, all sources)
Polars transforms (3 modules)
  ↓ Bronze → Silver with Pandera validation at boundaries
        silver.klines       (19 columns, normalized schema)
        silver.agg_trades   (18 columns, normalized schema)
        silver.funding_rate (12 columns, normalized schema)
Pandera schemas validate at each stage
  ↓
DuckDB/DuckLake silver tables → Feature Store, analytics, gap detection
  ↓
Prefect orchestrates (historical_pipeline, bulk_backfill, refresh_metadata, health)
```

### Legacy Pipeline

```
Archive (S3) → Download → Verify
                    ↓
GapFillWorkflow → REST API fill
                    ↓
HealthCheckWorkflow → completeness, freshness, integrity
                    ↓
SinkWorkflow → Polars → DuckLake
```

Both pipelines coexist. The dlt pipeline is the future direction; legacy workflows remain operational.

## Platform Evolution: dlt + SQLMesh

The roadmap adds two major technologies to the stack (see
`docs/brainstorms/2026-05-11-platform-evolution-requirements.md`):

| Technology | Role | Replaces | Status |
|-----------|------|----------|--------|
| **dlt** | Extract/Load (schema inference, incremental state, pagination, S3 listing) | Hand-rolled pagination/retry/state in ExchangeClient wrappers | **Implemented** |
| **SQLMesh** | Transform (Bronze→Silver via INCREMENTAL_BY_TIME_RANGE, audits, lineage) | Ad-hoc Polars transforms in SinkWorkflow | **Partial** — SQL models exist, Polars transforms still primary |

The target architecture:

```
dlt Sources (Binance, tardis.dev, Databento)
  → Bronze DuckDB (auto-normalized per source)
    → SQLMesh Models (Bronze→Silver INCREMENTAL_BY_TIME_RANGE)
      → Silver DuckLake (partitioned, ACID)
        → Feature Store (ML features, point-in-time datasets)
          → Prefect Orchestration (schedules, SLAs, DLQ)
```

Existing workflows (download, verify, gap-fill, sink) remain fully operational as the
legacy parallel path.

## Bronze and Silver Schemas

### Bronze Layer

All bronze tables store raw data with minimal transformation. REST API sources use
VARCHAR to preserve raw values; archive sources use typed columns.

| Bronze Table | Columns | dlt Resource | Pydantic Model (authoritative) | Pydantic Model (raw) | Pandera Schema |
|-------------|---------|-------------|------|------|------|
| `bronze.klines` | 12 | `klines_resource` / `archive_data_resource` | `KlineModel` (typed) | `RawKlineModel` (VARCHAR) | `BronzeKlinesSchema` |
| `bronze.agg_trades` | 8 | `agg_trades_resource` / `archive_data_resource` | `AggTradeModel` (typed, 6 fields) | `RawAggTradeModel` (VARCHAR, 8 fields) | `BronzeAggTradesSchema` |
| `bronze.funding_rate` | 4 | `funding_rate_resource` / `archive_data_resource` | `FundingRateModel` (typed, 3 fields) | `RawFundingRateModel` (VARCHAR, 4 fields) | `BronzeFundingRateSchema` |

### Silver Layer

All silver tables are normalized with Databento DBN naming conventions.

| Silver Table | Columns | Transform | Pandera Schema |
|-------------|---------|-----------|---------------|
| `silver.klines` | 19 | `bronze_klines_to_silver()` | `SilverKlinesSchema` |
| `silver.agg_trades` | 18 | `bronze_agg_trades_to_silver()` | `AggTradesSilverSchema` |
| `silver.funding_rate` | 12 | `bronze_funding_rate_to_silver()` | `FundingRateSilverSchema` |

All silver transforms:
- Use `exchange_for(trade_type)` from `common/enums.py` (canonical DuckLake naming)
- Produce `ts_date` as `pl.Date` type
- Support `validate=True` (default) for Pandera enforcement at output boundary

## Exchange Client SDK Integration

The `exchange/` module uses **official Binance SDK packages** (not hand-rolled `aiohttp`):

| Package | Market | REST Method | WS Method |
|---------|--------|-------------|-----------|
| `binance-sdk-spot` | Spot | `rest_api.klines()` | `connection.kline(symbol, interval)` with `KlineIntervalEnum` |
| `binance-sdk-derivatives-trading-usds-futures` | UM | `rest_api.kline_candlestick_data()` | `connection.kline_candlestick_streams()` |
| `binance-sdk-derivatives-trading-coin-futures` | CM | `rest_api.kline_candlestick_data()` | `connection.kline_candlestick_streams()` |

**Key design decisions:**
- No auth: `ConfigurationRestAPI(api_key="", api_secret="")` for public market data only
- SDK callback-based WS streams are wrapped via `asyncio.Queue` bridge
- Archive client (`archive/` module) remains on `aiohttp` (S3 access is a different concern)
- Backward-compat aliases: `BinanceRestClient = BinanceSpotRestClient`, `BinanceWsClient = BinanceSpotWsClient`
- CCXT remains optional (`[exchange]` extra) for multi-exchange support

## Data Sources

The pipeline ingests from three source layers. See `docs/data-sources.md` for
the complete field-to-source mapping matrix.

| Layer | Source | Latency | CLI |
|-------|--------|---------|-----|
| **Archive** | data.binance.vision (S3) | ~1-2 days | `download`, `verify` |
| **REST API** | api.binance.com via SDK | real-time | `gap-fill`, `refresh-metadata` |
| **WS Stream** | stream.binance.com via SDK | real-time continuous | `stream` (Phase 8) |

### Data Type Classification

Data types follow a two-tier model aligned with market data conventions:

| Tier | Types | Rationale |
|------|-------|-----------|
| **Essential base** | klines, aggTrades, trades, fundingRate | Raw/primary market data served directly by Binance. Full Silver pipeline with Pandera validation. E2E validated for all 3 markets. |
| **Optional derived** | bookDepth, metrics, indexPriceKlines, markPriceKlines, premiumIndexKlines | Derived, aggregated, or specialized data. Bronze-only ingestion (no Silver transform). Available via dlt archive but not required for core pipeline. |

### Data Type Coverage

| Type | Tier | Archive | REST | WS | Silver Table | E2E |
|------|------|---------|------|-----|-------------|-----|
| klines (spot/um/cm) | **Base** | ✓ | ✓ | ✓ | `silver.klines` (19 cols) | ✅ |
| aggTrades (spot/um) | **Base** | ✓ | ✓ | — | `silver.agg_trades` (18 cols) | ✅ |
| fundingRate (um/cm) | **Base** | ✓ | ✓ | — | `silver.funding_rate` (12 cols) | ✅ |
| trades (spot/um/cm) | **Base** | ✓ | — | — | `bronze.trades` | ⏳ |
| bookDepth (um/cm) | Derived | ✓ | — | — | `bronze.book_depth` | ✅ |
| metrics (um/cm) | Derived | ✓ | — | — | `bronze.metrics` | ✅ |
| indexPriceKlines (um/cm) | Derived | ✓ | — | — | `silver.klines` | ✅ |
| markPriceKlines (um/cm) | Derived | ✓ | — | — | `silver.klines` | ✅ |
| premiumIndexKlines (um/cm) | Derived | ✓ | — | — | `silver.klines` | ⏭ |
| bookTicker | Dead | ✗ | ✗ | ✗ | — | ❌ |
| liquidationSnapshot | Dead | ✗ | ✗ | ✗ | — | ❌ |

### Known Issues

| Issue | Location | Impact | Status |
|-------|----------|--------|--------|
| Archive klines μs vs ms | `transforms/klines.py` | Auto-detected via `open_time >= 1e15` | Resolved |
| Empty `mark_price` in CM fundingRate | `transforms/funding_rate.py` | Replaced with "0" before Float64 cast | Resolved |
| DuckLake concurrency guard | `prefect_flows.py` | Serialized via `concurrency("ducklake-writer")` | Resolved |
| S3 listing slow for full archive | `archive/client.py` | Full listing of BTCUSDT 1d = ~6400 files, ~30s | Use `lookback_days` to limit |
| binance_metadata.py + bronze_archive_index.py | `dlt_sources/` | Real logic migrated to `dlt/resources/` | Resolved |
