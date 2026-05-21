---
name: binance-datatool
description: |
  Manage and query Binance historical market data from data.binance.vision.
  List symbols, browse archive files, download klines/trades/funding rates with
  aria2, verify SHA256 checksums. Trigger Prefect data pipelines for automated
  backfilling and DuckDB ingestion. Use when the user mentions Binance historical
  data, klines, candlestick data, trade data, funding rates, data.binance.vision,
  or crypto data pipeline orchestration. Do not use for Binance REST API
  real-time data or WebSocket streaming.
---

# binance-datatool

CLI for Binance historical market data + Prefect pipeline orchestration.

## Quick Start

```bash
# List USDT symbols, download 3 days of 1h klines, verify
binance-datatool -v list-symbols spot --quote USDT --exclude-stables > syms.txt
binance-datatool -v download spot --type klines --interval 1h < syms.txt
binance-datatool -v verify spot --type klines --interval 1h < syms.txt

# Or run the full Prefect pipeline (same thing, automated)
uv run python3 -c "
from binance_datatool.workflow.prefect_flows import historical_pipeline
r = historical_pipeline(trade_type='spot', symbols=['BTCUSDT'],
                        data_type='klines', interval='1h', lookback_days=3)
print(r)
"
```

## CLI Commands

All commands accept `-v` (INFO-level stderr) and `--archive-home PATH`.

### list-symbols
`binance-datatool -v list-symbols (spot|um|cm) [--quote USDT] [--exclude-stables] [--from-catalog --catalog PATH]`

Queries remote archive by default. With `--from-catalog`, queries local DuckDB
symbols table (no network, faster, supports `--contract-type`). Requires
`refresh-metadata` to have been run first.

### list-files
`binance-datatool -v list-files (spot|um|cm) [SYMBOLS...] [--type klines] [--interval 1h]`

### download
`binance-datatool -v download (spot|um|cm) [SYMBOLS...] [--type klines] [--interval 1h] [--dry-run]`

### verify
`binance-datatool -v verify (spot|um|cm) [SYMBOLS...] [--type klines] [--interval 1h]`

Verifies SHA256 checksums. Exit 0 = all pass, 2 = partial failure.

### gap-fill
`binance-datatool -v gap-fill (spot|um|cm) --symbol BTCUSDT --type klines --interval 1h [--auto-detect] [--lookback 30]`

Fetches missing data from Binance REST API. Auto-detects gaps from archive.

### health
`binance-datatool -v health (spot|um|cm) [--type klines] [--interval 1h] SYMBOLS...`

Checks data completeness, freshness, integrity. Detects anomalies (null prices, duplicates).

### sink
`binance-datatool -v sink (spot|um|cm) [--type klines] [--interval 1h] [--target parquet|duckdb|all] SYMBOLS...`

Transforms Bronze archive ZIPs to Silver DuckDB tables via Polars.

### refresh-metadata
`binance-datatool -v refresh-metadata (spot|um|cm) [--from-api] [--catalog PATH]`

Updates venue and symbol metadata tables from archive listing or REST API.

### universe-maintenance
`binance-datatool -v universe-maintenance [--date YYYY-MM-DD] [--lookback 1] [--catalog PATH]`

Maintains historical universe statistics in the Gold layer. Synchronizes metadata
and builds daily point-in-time statistics required for accurate,
zero-survivorship-bias backtesting.

## Backtesting Data Product

The primary entry point for generating a production-ready backtesting dataset is
the `scripts/build_backtesting_dataset.py` script. This script orchestrates
the full data product lifecycle.

### Command Usage
```bash
uv run python scripts/build_backtesting_dataset.py \
    --lake-path ./lake_prod \
    --lookback-days 30 \
    --top-n 50 \
    --trade-types spot um cm \
    --data-types klines aggTrades fundingRate
```

## Engineering Patterns
1.  **Medallion-Native Architecture**: The system natively manages the
    Medallion layers (`bronze`, `silver`, `gold`) within the DuckLake catalog.
    Always prefer native table access over manual file scanning.
2.  **Native Partitioning**: Silver and Gold layers are natively partitioned
    by `symbol` and `ts_date` respectively. Queries using these filters
    benefit from automatic partition pruning.
3.  **Unified DuckLake Access**: Always use `get_connection(lake_path=...)` to
    access the Lakehouse. This handles extension loading and schema mapping.

## Reproducibility Workflow

To build and freeze a reproducible dataset:

1.  **Build**: `dvc repro` (runs the production data product flow).
2.  **Verify**: Check health reports in the `lake` directory.
3.  **Tag**: `git tag -a "data/v1" -m "Institutional Top 50"`
4.  **Push**: `dvc push` (if remote is configured).

## Point-in-Time Universe
...
Use the `UniverseBuilder` to construct tradable universes (e.g., Top 50) with
institutional risk filters and zero survivorship bias.

**Core Capabilities:**
- **Institutional Filters**: Excludes memes (DOGE, PEPE, etc.), leveraged tokens (UP/DOWN), and junk symbols.
- **Zero-Survivorship Bias**: Reconstructs universes using historical Gold layer snapshots, including assets that were later delisted.
- **Look-ahead Protection**: Filters assets based on their historical `onboard_date`.
- **Dynamic Normalization**: Automatically converts all quote volumes to USD using historical parity rates.

**Usage (Python API):**
```python
from binance_datatool.universe.builder import UniverseBuilder

builder = UniverseBuilder(lake_path="./lake")

# 1. Get current Top 50 for live trading
universe = builder.build_top_50(trade_type='spot')

# 2. Get historical Top 50 for backtesting (Zero-Bias)
backtest_ts = 1704067200000  # 2024-01-01
universe = builder.build_top_50(
    trade_type='um',
    as_of_timestamp_ms=backtest_ts,
    min_volume_usd=5_000_000,
    min_age_days=180
)
```

## Prefect Workflows

Full pipeline orchestration at `src/binance_datatool/workflow/prefect_flows.py`.

**Available flows:**

| Flow | Purpose | Parallelism |
|------|---------|-------------|
| `historical_pipeline` | Full ETL: metadata→download→verify→fill→sink→health | `prepare_symbol.map()` via `PREFECT_MAX_WORKERS` |
| `bulk_backfill` | Auto-discover + historical_pipeline | Delegates to historical_pipeline |
| `download_flow` | Multi-symbol archive download | `download_archive.map()` |
| `verify_flow` | Multi-symbol checksum verification | `verify_archive.map()` |
| `sink_flow` | Bronze→Silver→DuckDB ingestion | Sequential (DuckDB constraint) |
| `health_flow` | DuckLake anomaly detection | Sequential |
| `refresh_metadata_flow` | Venue/symbol metadata refresh | Sequential |
| `universe_maintenance_flow` | Metadata sync + Gold layer stats | Sequential |

**Task graph:**
```
historical_pipeline
  ├── refresh_metadata_flow        (subflow, sequential)
  ├── prepare_symbol.map()         (parallel fan-out per symbol)
  │     └── download → verify → fill_gaps
  ├── sink_silver()                (sequential, ducklake-writer guard)
  └── health_flow()                (sequential, per-symbol)
```

**Run via Python API:**
```python
from binance_datatool.workflow.prefect_flows import historical_pipeline, bulk_backfill

# Single symbol
result = historical_pipeline(trade_type='spot', symbols=['BTCUSDT'],
                             data_type='klines', interval='1h', lookback_days=3)

# Multi-symbol batch
result = bulk_backfill(trade_type='spot', symbols=['BTCUSDT', 'ETHUSDT', 'SOLUSDT'],
                       data_type='klines', interval='1h', lookback_days=3)
```

## Data Types

| Type | Archive | REST API | Table | Needs interval |
|------|---------|----------|-------|----------------|
| klines | daily zips | ✓ | `klines` | Yes (1m/1h/1d) |
| aggTrades | daily zips | ✓ | `aggTrades` | No |
| trades | daily zips | ✓ | `aggTrades` (shared) | No |
| fundingRate | monthly zips | ✓ | `fundingRate` | No |
| universeStats | N/A (agg) | N/A | `gold.daily_universe_stats` | No |

Pipeline sink handlers exist for these 4 primary types. Gold layer stats are
aggregated from Silver klines via the universe maintenance flow.

## Archive Structure (data.binance.vision)

```
data/
├── spot/daily/     klines(13 intv)  aggTrades(6376f)  trades(6376f)
├── spot/monthly/   klines(16 intv)  aggTrades(210f)   trades(210f)
├── futures/um/daily/  9 data types,  833 symbols, 6376 files/sym
├── futures/um/monthly/ 8 data types, 833 symbols
├── futures/cm/daily/  10 data types, 267 symbols (USD_PERP naming)
├── futures/cm/monthly/ 8 data types, 267 symbols
└── option/daily/  BVOLIndex(2 syms)  EOHSummary(5 syms)
```

File naming: `{symbol}-{dataType}-{date}.zip` + `.CHECKSUM` companion.
Klines add interval subdirectory: `{symbol}/{interval}/{symbol}-{interval}-{date}.zip`.

## Configuration

Settings via environment variables or `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `PREFECT_MAX_WORKERS` | 8 | Thread pool workers |
| `ARCHIVE_HOME` | `~/.binance-datatool/archive` | Local archive root |
| `CATALOG_PATH` | `ARCHIVE_HOME/../lake` | DuckLake catalog root |
| `DEFAULT_LOOKBACK_DAYS` | 30 | Pipeline lookback window |

## Best Practices

- **Always pass `-v`** — output is nearly silent at default WARNING level.
- **Dry-run before download** — `download --dry-run` to preview.
- **Frequency matters** — fundingRate requires `--freq monthly`.
- **Type names are camelCase** — `fundingRate`, `aggTrades`, not `funding_rate`.
- **Exit code 2** means partial failure — check stderr for details.
- **Stdin composition** — `list-symbols | download` when no positional args given.
- **Workflows prefer Python API** — flows accept same params as CLI but add parallelism.
- **Serve deployments** — `uv run python -m binance_datatool.workflow.prefect_flows serve` for cron.

## Agent Usage

- Use this skill for tasks that are about managing the archive lifecycle: listing symbols, listing files, downloading archives, verifying checksums, running Prefect flows for backfills, and transforming archive data to Silver.
- Do not use this skill to interact with live WebSocket streams or to perform ad-hoc REST trading actions. The skill is read-only for archive data and orchestrates controlled pipeline flows.
- Prefer the Python API (`workflow.prefect_flows`) for complex multi-symbol or programmatic runs; prefer CLI for ad-hoc operator tasks.

Limitations:
- The archive listing path performs a live S3 listing if `--from-catalog` is not used; this can be slow for full-symbol scans. Prefer `--from-catalog` when available.
- Some legacy fallback codepaths remain (small set of deprecated wrappers). See `AGENTS.md` and `tasks.md` Phase 42 for removal timeline and status.
