# Data Flows: Medallion Architecture

## 1. Unified Lakehouse Core
- **Catalog**: Managed via `DuckLake` driver, backed by `./lake/catalog.duckdb`.
- **Registry**: Centralized `registry.instruments` serves as the **historical superset** of all discoverable assets.
- **Storage**: Multi-tier Parquet storage with native DuckDB extension access.

## 2. Reusable Pipeline Building Blocks (Medallion Layers)

### Bronze (Raw Ingestion)
- **Task**: `extract_klines`, `extract_agg_trades`, `extract_funding_rate`
- **Pattern**: `dlt` managed extraction from S3/REST. Preserves raw schema in `VARCHAR` format to handle upstream changes.
- **Validation**: Pydantic models enforce basic record integrity during ingestion.

### Silver (Standardized Normalization)
- **Task**: `sink_silver` (orchestrates `Polars` transforms)
- **Pattern**: Normalizes timestamps to μs (DBN convention), renames fields (Tardis convention), and enforces strict types (`FLOAT64`, `INT64`).
- **Validation**: Pandera schemas (`SilverKlinesSchema`, etc.) validate the DataFrame at the boundary of every transform.

### Gold (Historical Snapshots)
- **Task**: `build_daily_universe_stats`
- **Pattern**: Combines Silver liquidity with Registry metadata into daily point-in-time snapshots.
- **Responsibility**: Provides the **Source of Truth** for survivorship-bias-free backtesting.

## 3. E2E Validation Lifecycle (TDD/Specs Driven)

Every pipeline run undergoes a 4-stage validation lifecycle:
1.  **Schema Compliance**: Pydantic/Pandera enforce the **Data Contract**.
2.  **Health Audit**: `HealthCheckWorkflow` scans for gaps, nulls, and outliers post-ingestion.
3.  **Point-in-Time Verification**: `scripts/validate_universe_e2e.py` ensures that historical reconstructions are immune to survivorship and look-ahead bias.
4.  **Lineage Tracking**: SQLMesh tracks the "provenance" of every column from the raw discovery log to the strategy-ready Gold layer.

## 4. Operational Orchestration
- **Parallel Fan-out**: Prefect fan-out per symbol for extraction.
- **Serialized Writes**: `ducklake-writer` semaphore ensures thread-safe catalog updates.
- **Survivorship-Bias-Free Construction**: The `UniverseBuilder` consumes the **Gold Layer** to reconstruct history for quant research.
