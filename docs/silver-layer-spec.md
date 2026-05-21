# Silver Layer: Normalized Data Schemas

> **Note (2026-05-16):** `DuckLakeCatalog` class does not exist — dlt manages DuckLake
> natively via `dlt.destinations.ducklake()`. Analytics views (`daily_ohlcv`, `latest_klines`,
> `stale_symbols`) were removed in Phase 36. DuckLake catalog paths use the Hive-style
> partitioning format documented below. For the current stack, see `AGENTS.md`
> and `dlt/destinations.py`.

The **Silver layer** is the transform/normalization stage of the medallion architecture.
It converts raw archive data (Bronze) into clean, queryable, standardized schemas suitable
for analytics, gap detection, health checks, and downstream ML pipelines.

## Design Principles

- **Normalized across trade types**: Spot, UM, CM share a unified schema per data type
- **Normalized across sources**: Archive ZIP, API-filled, and WS-filled data use the same schema
- **Data source vs schema model**: These are separate concerns:
  - **Source**: Binance Archive (primary) + Binance REST/WS API (secondary, for gaps)
  - **Target schema**: Modeled after tardis.dev + Databento DBN conventions
- **Source-to-target mapping**: Archive CSV fields are renamed/normalized to target conventions:
  - `open_time` → `ts_event` (DBN)
  - `count` → `trade_count` (DBN)
  - `quantity` → `size` (DBN)
  - `agg_trade_id` → `trade_id` (tardis.dev)
  - `is_buyer_maker` → `side` (tardis.dev)
  - `funding_time` → `ts_event` (DBN)
  - Metadata columns (`exchange`, `rtype`, `ts_recv`) added where archive lacks them
- **Convention sources**:
  - **tardis.dev**: `exchange` + `symbol` as row-level identifiers, `price`/`side`/`funding_rate`/`mark_price` naming
  - **Databento DBN**: `ts_event`/`ts_recv` for event/receive timestamps, `rtype` for record type, `size` for trade amount
  - **Binance-specific fields preserved**: `quote_volume`, `taker_buy_*` (no tardis.dev/DBN equivalent)
- **Self-describing**: Metadata columns (`source`, `exchange`, `trade_type`, `data_type`,
  `symbol`, `interval`, `ingested_at`) make each row fully contextual without external catalog
- **Type-safe**: All numeric fields are FLOAT64/INT64, timestamps are INT64 epoch μs

## Iceberg Catalog Table Naming

```
{catalog}/{trade_type}/{data_type}/date={YYYY-MM-DD}/{file}.parquet
```

| Catalog Path | Description | Partitioning |
|---|---|---|
| `data/exchange=binance-spot/data-type=klines/symbol=BTCUSDT/interval=1h/date=N/data.parquet` | Spot klines (1h) | `exchange, data-type, symbol, interval, date` |
| `data/exchange=binance-perps-um/data-type=klines/symbol=BTCUSDT/interval=1h/date=N/data.parquet` | UM klines (1h) | `exchange, data-type, symbol, interval, date` |
| `data/exchange=binance-perps-cm/data-type=klines/symbol=BTCUSDT/interval=1h/date=N/data.parquet` | CM klines (1h) | `exchange, data-type, symbol, interval, date` |
| `data/exchange=binance-spot/data-type=aggTrades/symbol=BTCUSDT/date=N/data.parquet` | Spot aggTrades | `exchange, data-type, symbol, date` |
| `data/exchange=binance-perps-um/data-type=fundingRate/symbol=BTCUSDT/date=N/data.parquet` | UM funding rate | `exchange, data-type, symbol, date` |
| `{lake}/venues.parquet` | Venue metadata (DuckLake native table opt-in) | None |
| `{lake}/symbols.parquet` | Symbol metadata (DuckLake native table opt-in) | None |

## Metadata Tables

### venues (catalog-level)

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `venue` | UTF8 | Auto | `"binance_spot"`, `"binance_um"`, `"binance_cm"` |
| `trade_type` | UTF8 | Auto | `"spot"`, `"um"`, `"cm"` |
| `exchange` | UTF8 | Auto | `"binance"` |
| `source` | UTF8 | Auto | `"archive"` or `"api"` |
| `symbol_count` | INT64 | Derived | Number of symbols (populated after refresh) |
| `data_types` | UTF8 | Auto | Comma-separated available data types |
| `fetched_at` | INT64 (ms) | Auto | When metadata was fetched |

### symbols (catalog-level)

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `symbol` | UTF8 | Archive/API | Trading pair (e.g. `"BTCUSDT"`) |
| `trade_type` | UTF8 | Auto | `"spot"`, `"um"`, `"cm"` |
| `exchange` | UTF8 | Auto | `"binance"` |
| `base_asset` | UTF8 | Archive/API | Base asset (e.g. `"BTC"`) |
| `quote_asset` | UTF8 | Archive/API | Quote asset (e.g. `"USDT"`) |
| `contract_type` | UTF8 | API | `"perpetual"`, `"delivery"`, or empty |
| `is_leverage` | BOOL | Archive | Leveraged token flag |
| `is_stable_pair` | BOOL | Archive | Stablecoin pair flag |
| `source` | UTF8 | Auto | `"archive"` or `"api"` |
| `status` | UTF8 | API | `"trading"`, `"break"`, etc. |
| `fetched_at` | INT64 (ms) | Auto | When metadata was fetched |

## Silver Layer Schemas

### Klines (unified OHLCV)

Unifies spot/um/cm klines from all sources (archive, API, WS).
Follows DBN (`ts_event`, `ts_recv`) and tardis.dev (price/size volume) conventions.

| Silver Column | Bronze Source | Type | Description |
|--------------|---------------|------|-------------|
| `ts_event` | `open_time` | INT64 μs | Event timestamp (DBN) |
| `ts_recv` | Auto | INT64 μs | Receive timestamp (DBN) |
| `open` | CSV column | FLOAT64 | Open price |
| `high` | CSV column | FLOAT64 | High price |
| `low` | CSV column | FLOAT64 | Low price |
| `close` | CSV column | FLOAT64 | Close price |
| `volume` | CSV column | FLOAT64 | Base volume |
| `quote_volume` | CSV column | FLOAT64 | Quote volume |
| `trade_count` | `count` | INT64 | Number of trades |
| `taker_buy_volume` | CSV column | FLOAT64 | Taker buy base volume |
| `taker_buy_quote_volume` | CSV column | FLOAT64 | Maker buy quote volume |
| `source` | Auto | UTF8 | `"archive"`, `"api_filled"`, `"ws_stream"` |
| `exchange` | Auto | UTF8 | DuckLake catalog path: `"binance-spot"`, `"binance-perps-um"`, `"binance-perps-cm"` |
| `trade_type` | Auto | UTF8 | `"spot"`, `"um"`, `"cm"` |
| `symbol` | Auto | UTF8 | e.g. `"BTCUSDT"` |
| `interval` | Auto | UTF8 | e.g. `"1h"` |
| `data_type` | Auto | UTF8 | `"klines"` |
| `ingested_at` | Auto | INT64 μs | Ingestion timestamp |
| `ts_date` | Auto | DATE | Partition date from ts_event |

### Trades (unified raw/aggregated)

Unifies trades, aggTrades from all trade types.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `ts_event` | INT64 μs | DBN | Trade timestamp |
| `ts_recv` | INT64 μs | DBN | Receive timestamp |
| `price` | FLOAT64 | tardis.dev | Trade price |
| `size` | FLOAT64 | tardis.dev | Trade size |
| `side` | UTF8 | tardis.dev | `"buy"`, `"sell"`, or null |
| `trade_id` | INT64 | Binance | Unique trade ID (maps to agg_trade_id for aggTrades) |
| `is_buyer_maker` | INT8 | Binance | 1 if buyer is maker |
| `agg_trade_id` | INT64 | Binance | Aggregated trade ID |
| `first_trade_id` | INT64 | Binance | First constituent trade ID |
| `last_trade_id` | INT64 | Binance | Last constituent trade ID |
| `rtype` | UTF8 | Auto | Record type: `"trade"` or `"agg"` |
| `source` | UTF8 | Meta | Data source |
| `exchange` | UTF8 | Meta | DuckLake catalog path |
| `trade_type` | UTF8 | Meta | `"spot"`, `"um"`, `"cm"` |
| `symbol` | UTF8 | Meta | Trading pair |
| `data_type` | UTF8 | Meta | `"trades"` or `"aggTrades"` |
| `ingested_at` | INT64 μs | Meta | Ingestion timestamp |
| `ts_date` | DATE | Meta | Partition date from ts_event |

### Funding Rate

Perpetual futures funding rates (um/cm only).

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `ts_event` | INT64 μs | DBN | Funding time |
| `ts_recv` | INT64 μs | DBN | Receive timestamp |
| `funding_rate` | FLOAT64 | tardis.dev | Rate (0.0001 = 0.01%) |
| `mark_price` | FLOAT64 | Binance | Mark price |
| `funding_timestamp` | INT64 μs | Binance | Funding event timestamp |
| `source` | UTF8 | Meta | Data source |
| `exchange` | UTF8 | Meta | DuckLake catalog path |
| `trade_type` | UTF8 | Meta | `"um"`, `"cm"` |
| `symbol` | UTF8 | Meta | Trading pair |
| `data_type` | UTF8 | Meta | `"fundingRate"` |
| `ingested_at` | INT64 μs | Meta | Ingestion timestamp |
| `ts_date` | DATE | Meta | Partition date from ts_event |

## Bronze → Silver Mapping

```
Bronze Klines (Binance archive CSV)          Silver Klines
─────────────────────────────                 ─────────────
open_time                → ts_event
open (str → float)       → open
high (str → float)       → high
low (str → float)        → low
close (str → float)      → close
volume (str → float)     → volume
quote_volume (str → float) → quote_volume
count                    → trade_count
taker_buy_volume         → taker_buy_volume
taker_buy_quote_volume   → taker_buy_quote_volume
(literal "0")            → ignore (dropped)
-                        → source = "archive"
-                        → trade_type (from directory path)
-                        → symbol (from directory path)
-                        → interval (from directory path)
-                        → data_type = "klines"
-                        → ingested_at = now()
```

## Data Flow: Archive → Silver Pipeline

```
Archive (data.binance.vision S3) + REST API
  │
  ├── dlt sources (bronze tables in DuckDB/DuckLake)
  │     → RawKlineModel / RawAggTradeModel / RawFundingRateModel (VARCHAR, Pydantic)
  │     → bronze.klines / bronze.agg_trades / bronze.funding_rate
  │
  ├── Polars transforms (transforms/klines.py, transforms/agg_trades.py, transforms/funding_rate.py)
  │     → Bronze → Silver (type cast, μs auto-detection, normalize, add metadata)
  │     → Pandera validation at boundary (SilverKlinesSchema, AggTradesSilverSchema, ...)
  │
  └── DuckLake silver tables (dlt destination)
        → silver.klines (19 cols) | silver.agg_trades (18 cols) | silver.funding_rate (12 cols)
        → Partitioned: exchange=binance-spot/data-type=klines/symbol=BTCUSDT/interval=1h/date=N/data.parquet
```

## Commands

```bash
# Transform Bronz to Silver Parquet
binance-datatool sink spot --type klines --interval 1h --target parquet --catalog /path/to/lake BTCUSDT

# Load into DuckDB as well
binance-datatool sink spot --type klines --interval 1h --target all --duckdb /path/to/db.duckdb BTCUSDT

# Refresh metadata from archive
binance-datatool refresh-metadata spot --catalog /path/to/lake

# Refresh metadata from REST API (richer data)
binance-datatool refresh-metadata um --from-api --catalog /path/to/lake
```

## Catalog Directory Structure

```
{lake_path}/
├── metadata.ducklake         # DuckLake v1.0 catalog (SQLite)
├── metadata.ducklake.wal     # Write-ahead log
├── metadata/
│   ├── venues.parquet        # Venue metadata (3 rows)
│   └── symbols.parquet       # Symbol metadata (all trade types)
└── data/
    ├── exchange=binance-spot/
    │   └── data-type=klines/
    │       └── symbol=BTCUSDT/
    │           └── interval=1h/
    │               └── date=2026-05-08/
    │                   └── data.parquet        # Written by Polars sink
```

## Iceberg Catalog (Proposal)

Iceberg catalog design has been moved to `docs/proposals/iceberg.md`.
The DuckLake native tables are the primary storage layer; Iceberg integration
is deferred until multi-engine access or catalog-driven schema evolution
is required.

### Implementation
```python
# DuckLake is configured via dlt destination (dlt/destinations.py)
# No DuckLakeCatalog class exists — dlt manages the catalog natively.
# See dlt/destinations.py for ducklake() configuration.

# After sink, attach DuckLake for querying:
import duckdb
con = duckdb.connect("lake/catalog.duckdb")
con.execute("LOAD ducklake")
con.execute("ATTACH 'ducklake:lake/metadata.ducklake' AS dl "
            "(DATA_PATH 'lake/data', AUTOMATIC_MIGRATION true)")
con.execute("USE dl")

# Query silver klines
rows = con.execute(
    "SELECT symbol, COUNT(*) FROM klines WHERE trade_type = 'spot' GROUP BY symbol"
).fetchall()
```

## DuckLake Catalog Design

The Lakehouse uses the **native DuckLake extension** for DuckDB to provide ACID-compliant, zero-copy access to the Parquet data lake.

### Automated Schema Mapping
The `get_connection()` utility in `binance_datatool.storage.duckdb` automatically:
1.  **Attaches** the Lakehouse catalog (`metadata.duckdb`) as a native DuckLake database.
2.  **Exposes** all Lakehouse schemas (`registry`, `bronze`, `silver`) as top-level schemas in the DuckDB connection.
3.  **Maps** tables to consistent names across schemas:
    -   `registry.instruments`
    -   `bronze.klines`, `bronze.agg_trades`, `bronze.funding_rate`
    -   `silver.klines`, `silver.agg_trades`, `silver.funding_rate`

### Table Definitions

| Table | Columns | Partition Key | Description |
| :--- | :--- | :--- | :--- |
| `klines` | 19 | `symbol` | Unified OHLCV (Spot/UM/CM). |
| `agg_trades` | 18 | `symbol` | Unified trade stream (includes `side`, `size`). |
| `funding_rate` | 12 | `symbol` | Perpetual funding events (UM/CM). |

### Querying the Lakehouse
Once connected via `get_connection()`, the lake can be queried using standard SQL:

```sql
-- Direct access to the Top 50 universe metadata
SELECT symbol, quote_volume
FROM registry.market_stats
WHERE venue_id = 'binance_spot'
ORDER BY quote_volume DESC LIMIT 50;

-- Zero-copy scan of Silver klines
SELECT ts_event, open, close
FROM silver.klines
WHERE symbol = 'BTCUSDT' AND interval = '1h';
```

### Catalog Structure (DuckLake)

```
{lake_path}/
├── metadata.ducklake       # DuckLake v1.0 catalog (ACID metadata)
├── metadata.ducklake.wal   # Write-ahead log
├── metadata/
│   ├── venues.parquet      # Venue metadata (3 rows)
│   └── symbols.parquet     # Symbol metadata (all symbols)
└── data/
    └── exchange=binance-spot/
        └── data-type=klines/
            └── symbol=BTCUSDT/
                ├── interval=1h/
                │   └── date=2026-05-08/
                │       └── data.parquet       # <-- same file name everywhere
                └── interval=1m/
                    └── date=2026-05-08/
                        └── data.parquet
```

## Gap-Fill and Health Check on Silver

### Gap Detection (Silver-aware)
- Scan Silver layer for missing dates per `(trade_type, data_type, symbol, interval)`
- Query DuckLake catalog for date range coverage
- Fetch missing data from REST API → normalize → append to Silver

### Health Check (Silver-aware)
- **Completeness**: Silver has all expected dates?
- **Freshness**: Latest Silver record within max_stale window?
- **Integrity**: Silver schema conforms to contract? No null prices? Valid timestamps?
- **Consistency**: Cross-reference counts between Bronze and Silver?
