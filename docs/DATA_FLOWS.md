# Data Flows: Bronze-Silver-Gold

## Lakehouse Storage
- **Catalog**: Managed via `DuckLake` driver, backed by `./lake/metadata.duckdb`.
- **Data Path**: Parquet files are stored in `{lake_path}/bronze/{table}/*.parquet`.
- **Schema Resolution**: Tables are dynamically accessed via `read_parquet()` in the transform layer to bypass driver-specific catalog lookup bugs.

## Ingestion Pattern
1. **DLT Resource**: Standardized to store raw data into `{lake_path}/bronze/{table}/`.
2. **Transform**: Prefect tasks use `get_connection()` to resolve storage and execute `read_parquet()`, performing normalization into `silver` tables via DuckDB's native memory/storage engine.
