# Metadata Architecture Specification

## 1. Overview
The `binance-datatool` metadata system provides a high-fidelity instrument and venue registry stored in the Lakehouse. It unifies discovery (Archive/S3) with trading specifications (REST API), serving as the foundation for **survivorship-bias-free** universe construction.
## 2. Native Registry Management
Stored in the `registry` schema within the **Native DuckLake Catalog**.

Unlike the Silver/Gold layers, the Registry tables are global (non-partitioned) to facilitate rapid lookup and cross-referencing during ingestion.

### 2.1 `registry.venues`
### 2.1 `registry.venues`
Defines the trading environments and their industry-standard identifiers.

| Field | Type | Description | Standard |
| :--- | :--- | :--- | :--- |
| `venue_id` | `VARCHAR` | Internal Primary Key. | |
| `publisher_id` | `VARCHAR` | DBN publisher identifier (e.g., `BINA.SPOT`). | **DBN** |
| `dataset` | `VARCHAR` | DBN dataset identifier (e.g., `BINA.SPOT`). | **DBN** |
| `exchange_slug` | `VARCHAR` | Tardis exchange identifier (e.g., `binance`). | **Tardis** |
| `market_type` | `VARCHAR` | `spot`, `um`, `cm`. | |

### 2.2 `registry.instruments`
High-fidelity specifications for all assets.

| Field | Type | Description | Standard |
| :--- | :--- | :--- | :--- |
| `symbol` | `VARCHAR` | Unique identifier (e.g., `BTCUSDT`). | **DBN** (`raw_symbol`) |
| `instrument_class` | `VARCHAR` | `spot` or `future`. | **DBN** |
| `base_asset` | `VARCHAR` | Underlying currency. | **Tardis** (`baseCurrency`) |
| `quote_asset` | `VARCHAR` | Quote currency. | **Tardis** (`quoteCurrency`) |
| `tick_size` | `DOUBLE` | Min price increment. | **DBN** (`min_price_increment`) |
| `lot_size` | `DOUBLE` | Min quantity step. | **Tardis** (`amountStep`) |
| `min_notional` | `DOUBLE` | Min order value. | **Tardis** (`minNotional`) |
| `contract_type` | `VARCHAR` | Settlement style. | **Tardis** (`type`) |
| `onboard_date` | `BIGINT` | Unix timestamp of contract listing. | |


## 3. Reusable Building Blocks (Metadata Lifecycle)

The system is built using atomic, reusable tasks that can be composed into custom discovery pipelines.

### 3.1 Discovery (Archive/S3)
- **Task**: `extract_metadata`
- **Responsibility**: Scans the full S3 archive to identify the "historical superset" of assets. This ensures that assets delisted before the registry was initialized are captured.

### 3.2 Enrichment (REST API)
- **Task**: `sync_exchange_metadata`
- **Responsibility**: Queries the live exchange API to enrich discovered assets with high-precision trading constraints (`tick_size`, `min_notional`) and listing metadata (`onboard_date`).

### 3.3 Historical Maintenance (Gold Layer)
- **Task**: `build_daily_universe_stats`
- **Responsibility**: Captured snapshots of metadata joined with daily liquidity. This block is the "Source of Truth" for historical universe construction.

## 4. E2E Validation & Data Lineage
Metadata integrity is verified at every stage of the pipeline:
1.  **Ingest**: Pydantic `InstrumentModel` enforces schema and business rules (e.g., `high >= low`).
2.  **Transform**: Pandera schemas validate the consistency of the registry before it is used by the `UniverseBuilder`.
3.  **Consumption**: SQLMesh models provide column-level lineage from the raw discovery logs to the final `registry.v_tardis` and `registry.v_databento` views.
