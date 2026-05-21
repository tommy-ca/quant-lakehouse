# Data Contract Specification

## 1. Overview
The `binance-datatool` employs a **Dual-Layer Validation Contract** to ensure total data integrity from raw ingestion (Bronze) to strategy-ready analytics (Silver/Gold). This system guarantees that every data point in the Lakehouse is type-safe, range-validated, and cross-column consistent.

## 2. Validation Layers

### 2.1 Layer 1: Per-Record (Pydantic)
- **Scope**: authoritative ingest boundary (`binance_datatool.dlt.models`).
- **Timing**: Applied during `dlt` extraction/loading.
- **Responsibility**: Enforces basic data types, business rules (e.g., `open_time > 0`), and structural integrity.
- **Fail Fast**: Invalid records are rejected at the edge of the system, preventing pollution of the Bronze layer.

### 2.2 Layer 2: Per-DataFrame (Pandera)
- **Scope**: transform and load boundaries (`binance_datatool.validation.schemas`).
- **Timing**: Applied before writing to Silver and Gold tables.
- **Responsibility**: Validates bulk consistency, column presence, nullability, and complex cross-column invariants (e.g., `high >= low`, `ts_date` alignment).
- **Data Quality**: Ensures that the final Parquet files meet the exact schema required by downstream consumers (vectorbt, ML models).

## 3. Standardized Schemas & Partition Specs

The Lakehouse enforces physical data organization via **Native DuckLake Partitioning**.

| Layer | Table | Partition Key | Transform | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Silver** | `klines` | `(symbol, ts_date)` | Identity | Optimized for symbol and date-level pruning. |
| **Silver** | `agg_trades` | `(symbol, ts_date)` | Identity | Optimized for symbol and date-level pruning. |
| **Gold** | `daily_universe_stats` | `ts_date` | Day | Optimized for point-in-time universe reconstruction. |

### 3.1 DDL Implementation
Partition specs are applied during table creation using native DuckLake syntax:
```sql
ALTER TABLE silver.klines SET PARTITIONED BY (symbol);
ALTER TABLE gold.daily_universe_stats SET PARTITIONED BY (ts_date);
```

## 4. Layer Details

| Layer | Schema | Key Constraints |
| :--- | :--- | :--- |
| **Bronze** | `Raw*Model` | VARCHAR-only, preserves raw API output for traceability. |
| **Silver** | `SilverKlinesSchema` | INT64 μs timestamps, FLOAT64 prices, 19 normalized columns. |
| **Silver** | `AggTradesSilverSchema` | Trade-level side derivation (`buy`/`sell`), 18 normalized columns. |
| **Gold** | `GoldUniverseStatsSchema` | Daily liquidity and metadata snapshots. |

## 4. Anomaly Detection (Health Audit)
Beyond schema validation, the `HealthCheckWorkflow` performs statistical audits:
- **Completeness**: Checks for missing date gaps in the time series.
- **Freshness**: Monitors staleness against a configurable `max_stale` threshold.
- **Integrity**: Verifies SHA256 checksums for all archive-sourced data.
- **Outliers**: Identifies price/volume spikes exceeding N standard deviations.

## 5. MLOps Integration
These contracts serve as the **Source of Truth** for feature engineering. Any change to a schema requires a version bump in the `catalog.duckdb` to ensure that training and inference environments remain aligned.
