# Architecture

This document describes the internal architecture of `binance-datatool`. It is intended for
contributors, maintainers, and AI agents working on the codebase.

## Package Tree

```
src/binance_datatool/
├── __init__.py              # Version metadata only
├── common/                  # Shared utilities (enums, constants, types, filters, symbols, progress, intervals, logging, path, settings, metadata_registry)
├── archive/                 # Archive access (S3 HTTP client, checksum, downloader, symbol directory)
├── exchange/                # Live exchange API clients (REST/WebSocket, CCXT integration)
│   ├── client.py            # ExchangeClient protocol (@runtime_checkable)
│   ├── binance_rest.py      # BinanceSpot/Um/CmRestClient
│   ├── binance_ws.py        # BinanceSpot/Um/CmWsClient
│   ├── ccxt_rest.py         # CCXTExchangeClient (optional dependency)
│   └── ccxt_pro.py          # CCXTProExchangeClient (optional dependency)
├── dlt/                     # Standalone dlt package — extract/load
│   ├── __init__.py          # Re-exports all resources, sources, models, destinations
│   ├── models.py            # 9 Pydantic models (Kline, AggTrade, FundingRate, Venue, SymbolMeta, Instrument, MarketStats, Raw*)
│   ├── destinations.py      # DuckDB/DuckLake destination builders (Hybrid DuckDB catalog support)
│   ├── sources.py           # @dlt.source builders (Unified multi-symbol sources)
│   └── resources/           # 8 @dlt.resource modules + shared client factory
│       ├── _client.py       # Shared client_for() — DRY trade-type dispatch
│       ├── binance_klines.py
│       ├── binance_agg_trades.py
│       ├── binance_funding.py
│       ├── binance_archive.py  # All data types: klines, aggTrades, trades, fundingRate, bookDepth, metrics, index/mark/premium klines
│       ├── binance_ws.py
│       ├── binance_market_stats.py # 24h market statistics
│       ├── binance_metadata.py  # Venue + symbol metadata
│       └── archive_index.py     # Local archive file index scanner
├── transforms/              # Polars transforms (Bronze → Silver)
│   ├── klines.py            # bronze_klines_to_silver() + μs auto-detection
│   ├── agg_trades.py        # bronze_agg_trades_to_silver() + side derivation + Pandera validation
│   └── funding_rate.py      # bronze_funding_rate_to_silver() + empty mark_price handling
├── validation/              # Pandera schemas + validation helpers
│   └── schemas.py           # 7 Pandera schemas (bronze/silver for klines/aggTrades/fundingRate) + Gold statistics
├── storage/                 # DuckDB/DuckLake storage layer
│   ├── duckdb.py            # get_connection() with dynamic view mapping, write_silver_table()
│   └── catalog.py           # DuckLakeCatalog with TABLE_DEFS for silver tables
├── workflow/                # Business logic orchestration
│   ├── __init__.py          # Re-exports all workflow classes and result types
│   ├── sink.py              # SinkWorkflow — Polars-based Bronze→Silver→DuckLake
│   ├── gap_detection.py     # GapDetectionWorkflow — detect date gaps in silver tables
│   ├── health_check.py      # HealthCheckWorkflow — completeness/freshness/integrity
│   ├── metadata.py          # MetadataWorkflow — venue/symbol metadata refresh
│   ├── prefect_flows.py     # Prefect @flow and @task definitions (BacktestingDatasetFlow)
│   └── prefect_tasks/       # Importable business logic for Prefect tasks
│       ├── metadata.py      # sync_metadata_task()
│       ├── extract.py       # extract_klines(), extract_archive(), etc.
│       ├── universe.py      # build_universe_stats_task()
│       └── transform.py     # transform_klines(), transform_agg_trades(), etc.
├── universe/                # Application Layer: Tradable universes & Gold Stats
│   ├── __init__.py
│   ├── builder.py           # UniverseBuilder — builds Top-50 list with institutional filters
│   ├── rates.py             # RateProvider — dynamic USD normalization (historical aware)
│   └── gold_pipeline.py     # Gold Layer Pipeline — materializes daily universe stats
├── cli/                     # Typer CLI layer
│   ├── __init__.py          # Root callback with -v/-vv verbosity and --archive-home
│   └── archive.py           # All CLI commands (build-dataset, universe-maintenance, etc.)
```

## Layered Design

The package follows a multi-layer architecture. Each layer depends only on the layers
below it — outer layers import inner layers, never the reverse.

```
CLI  (cli/)
 └─▶ Workflow  (workflow/)  ── Prefect orchestration (BacktestingDatasetFlow)
       ├─▶ Universe (universe/)  ── Application logic: Tradable Universes & Gold Stats
       ├─▶ dlt  (dlt/)       ── Extract/Load (REST, WS, archive resources)
       ├─▶ Transforms  (transforms/)  ── Polars Bronze→Silver
       ├─▶ Validation  (validation/)  ── Pandera schemas at pipeline boundaries
       ├─▶ Storage  (storage/)        ── DuckDB/DuckLake read/write
       ├─▶ Archive Client  (archive/) ── S3 HTTP client (data.binance.vision)
       └─▶ Common  (common/)          ── Shared enums, types, constants
```
|-------|---------|----------------|
| **CLI** | `binance_datatool.cli` | Typer command definitions, argument parsing, output formatting. |
| **Workflow** | `binance_datatool.workflow` | Business logic orchestration; Prefect flows and tasks; gap detection, health checks, metadata. |
| **Universe** | `binance_datatool.universe` | Tradable universes, gold layer statistics, dynamic USD rates. |
| **dlt** | `binance_datatool.dlt` | Resources, source builders, Pydantic models, destination helpers. Standalone package. |
| **Transforms** | `binance_datatool.transforms` | Polars-based Bronze→Silver transforms with Pandera validation. |
| **Validation** | `binance_datatool.validation` | Pandera DataFrame schemas for pipeline boundary validation. |
| **Storage** | `binance_datatool.storage` | DuckDB/DuckLake read/write, catalog definitions. |
| **Archive Client** | `binance_datatool.archive` | S3 HTTP communication with data.binance.vision. |
| **Exchange** | `binance_datatool.exchange` | Live REST/WebSocket API clients via official Binance SDKs. |
| **Common** | `binance_datatool.common` | Shared enums, constants, types, and symbol filters; metadata registry. |

### Why This Architecture

- **Testability.** Each layer can be tested independently (resources with mocked clients, transforms with fixture DataFrames).
- **Composability.** dlt resources and transforms can be composed from scripts, notebooks, or Prefect without importing CLI cruft.
- **Extensibility.** Adding a new data type means adding: dlt resource + Pydantic model + Polars transform + Pandera schema.
- **dlt standalone.** `binance_datatool.dlt` imports independently — no dependency on workflow, CLI, or archive layers.

## Data Flow

### Full Pipeline (dlt + Polars + Pandera)

```
dlt Sources (5 resource modules)
  ↓ Extract + Load → DuckLake bronze (Parquet + DuckDB Catalog)
        bronze.klines       (unified table, all market types)
        bronze.agg_trades   (unified table, all market types)
        bronze.funding_rate (unified table, all market types)
Polars transforms (3 modules)
  ↓ Bronze → Silver with Pandera validation at boundaries
        silver.klines       (19 columns, normalized schema)
        silver.agg_trades   (18 columns, normalized schema)
        silver.funding_rate (12 columns, normalized schema)
Pandera schemas validate at each stage
  ↓
Gold Layer stats (universe module)
  ↓ daily aggregates → gold.daily_universe_stats
  ↓
Backtesting Data Product (workflow module)
  ↓ Orchestrates Top 50 discovery → Bulk ingestion → Validation
  ↓ Emits: Curated backtesting dataset
```

## DuckLake Catalog Strategy

- **Hybrid Catalog**: Uses `metadata.duckdb` as the primary catalog.
- **Metadata Management**: `metadata.duckdb` stores dlt logs and the `registry.symbols` table.
- **Dynamic Views**: `get_connection()` automatically maps on-disk Parquet files to DuckDB views in the `bronze` schema, ensuring robust table resolution across all ingestion types.
- **Storage Layout**: `{lake_path}/bronze/{table}/*.parquet`.

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

## Known Issues

| Issue | Location | Impact | Status |
|-------|----------|--------|--------|
| DuckLake SQLite WAL | `dlt.DuckLakeSqlClient` | Environment-specific connection failures | **Resolved** (Migrated to DuckDB Catalog) |
| Symbol Fragmentation | `dlt.sources` | Per-symbol tables (e.g. btcusdt_klines) | **Resolved** (Unified tables) |
| Missing mark_price | `transforms/funding_rate.py` | Transformation failure on evolved schemas | **Resolved** (Default fallback) |
