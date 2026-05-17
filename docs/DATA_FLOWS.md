# Data Flows: Bronze-Silver-Gold

## Lakehouse Architecture
- **Catalog**: Managed via `DuckLake` driver, backed by `./lake/metadata.duckdb`.
- **Registry**: Centralized `registry.symbols` table in the catalog serves as the single source of truth for symbol discovery and validation across all market types.
- **Storage**: Raw data is stored as Parquet files in `{lake_path}/bronze/{table}/*.parquet`.
- **Query Engine**: DuckDB with the `ducklake` extension, providing dynamic view mapping for seamless access to the Lakehouse.

## Ingestion Pipeline (ELT)
1. **Metadata Sync**: `sync_exchange_metadata` synchronizes Binance venues and symbols into the Lakehouse registry.
2. **Extraction (dlt)**: Multi-symbol `dlt` resources ingest raw market data into the partitioned Bronze layer.
3. **Normalization (Polars)**: Fast, memory-efficient transformations convert raw JSON/CSV data to typed Silver DataFrames.
4. **Loading (DuckDB)**: Transformed data is loaded into Silver tables using the `write_silver_table` utility with full idempotency support.
5. **Health Audit**: `HealthCheckWorkflow` performs post-ingestion data quality checks (completeness, freshness, anomaly detection).

## Data Quality Patterns
- **Evolution-Aware**: Transformations provide defaults for missing columns to handle upstream API changes gracefully.
- **Resilient Pathing**: `get_connection()` dynamically maps Parquet files to DuckDB views, bypassing driver-level catalog resolution bugs.
- **Concurrency Guard**: `ducklake-writer` limit ensures safe, serialized writes to the catalog and storage.
