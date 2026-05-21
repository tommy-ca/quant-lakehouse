# Specification: Backtesting Data Product Flow

## 1. Overview
The `BacktestingDatasetFlow` is a production-grade orchestration pipeline designed to generate a complete, high-fidelity dataset for a curated trading universe. It ensures that for every asset in a "Top 50" universe, all essential data types (klines, aggTrades, fundingRate) are present, validated, and consistent across all Binance trade types (Spot, USD-M, COIN-M).

## 2. Core Requirements
- **Consistency**: All assets in the Top 50 must have a matching temporal slice of data across all requested data types.
- **Completeness**: If an asset is in the Top 50, its associated `aggTrades` and `fundingRate` (where applicable) MUST be ingested.
- **Validation**: Every symbol-data-type pair must pass health checks (FR-10, FR-11).
- **Automation**: One-click orchestration from symbol discovery to Gold layer stats.

## 3. Flow Architecture (Prefect)

### 3.1 Components
1.  **`UniverseDiscoveryTask`**: Invokes `UniverseBuilder` to generate the Top 50 symbols per venue.
2.  **`DataIngestionTask`**: For each symbol in the universe, triggers:
    -   `extract_klines` / `transform_klines`
    -   `extract_agg_trades` / `transform_agg_trades`
    -   `extract_funding_rate` / `transform_funding_rate` (Futures only)
3.  **`GoldStatsMaintenanceTask`**: Updates `gold.daily_universe_stats` for the target period.
4.  **`ProductValidationTask`**: Final E2E audit of the dataset (row counts, gap analysis).

### 3.2 Data Products
| Product | Table/Schema | Frequency |
| :--- | :--- | :--- |
| **Tradable Universe** | `gold.daily_universe_stats` | Daily |
| **Pricing** | `silver.klines` | 1m, 1h |
| **Execution** | `silver.agg_trades` | Tick-level |
| **Costs** | `silver.funding_rate` | 8h (UM/CM) |
| **Metadata** | `registry.instruments` | Real-time |

## 4. Implementation Plan
1.  **Develop `BacktestingDatasetFlow`** in `src/binance_datatool/workflow/prefect_flows.py`.
2.  **Add `DataProductWorkflow`** as a thin wrapper for CLI access.
3.  **TDD**: Create `tests/test_data_product.py` to verify that the flow correctly fans out to the Top 50 symbols.
4.  **Validation**: Run an E2E test with a small subset (e.g., Top 5) to ensure cross-table consistency.
