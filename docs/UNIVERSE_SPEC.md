# Universe Architecture Specification

## 1. Overview
The `binance-datatool` universe system is an isolated application layer that consumes metadata from the Lakehouse registry and integrates external signals to construct high-fidelity tradable asset lists. This decoupled design ensures the metadata registry remains a pure "Source of Truth" while allowing the universe logic to evolve independently.

## 2. Institutional-Grade Universe Construction
The `UniverseBuilder` identifies assets that balance high execution capacity with market stability, following standards from S&P Crypto Indices and Bitwise.

### 2.1 Quality & Stability Filters
| Filter | Threshold | Rationale |
| :--- | :--- | :--- |
| **Integrity** | `status == 'trading'` | Exclude delisted or settling assets. |
| **Liquidity Floor** | Configurable (Default `$1M`) | Minimum capacity for institutional execution. Controlled by `universe_min_volume_usd`. |
| **V/MC Ratio** | `1% - 50%` | Screen out "zombie" tokens (low liquidity/high cap) and mania/wash-trading (>50%). |
| **Listing Age** | Configurable (Default `180 Days`) | Eliminate launch bias and volatility. Controlled by `universe_min_age_days`. |
| **Redundancy** | `Unique Base Asset` | Select only the most liquid primary pair per asset (e.g., BTCUSDT over BTCUSDC). |
| **Structural** | `Exclude Stables/Fiat` | Exclude base pairs (USDT, EUR, JPY) from the tradable universe. |
| **Risk: Naming** | `ASCII Only` | Filter out non-standard or junk tokens using regex `^[A-Za-z0-9\-_]+$`. |
| **Risk: Leveraged** | `Exclude UP/DOWN` | Filter out leveraged tokens (e.g., BTCUP, BTCDOWN) to prevent compound decay risks. |
| **Risk: Memes** | `Exclude Memes` | Filter out known meme/political coins (e.g., DOGE, SHIB, PEPE, FLOKI, BONK, TRUMP, MAGA). |

### 2.2 Ranking Model (Multi-Factor)
Instruments are ranked by a weighted scoring model using parameters from `settings.py`:
$$ S = w_{vol} \cdot \text{log}(\text{Volume}_{24h} + 1) + w_{mcap} \cdot \text{log}(\text{MarketCap}_{pseudo} + 1) $$

Where:
- $w_{vol}$ = `universe_volume_weight` (Default: 0.7)
- $w_{mcap}$ = `universe_mcap_weight` (Default: 0.3)
- $\text{MarketCap}_{pseudo} = \text{Volume}_{24h} \cdot \text{Multiplier}$ (Controlled by `universe_mcap_multiplier`, Default: 10.0)

This formula prioritizes actual execution capacity (liquidity) while providing a stability boost to established assets via a volume-correlated pseudo-market-cap.

## 3. Data Flows

### 3.1 Metadata-Driven construction
1.  **Registry Query**: `UniverseBuilder` fetches raw instrument specs (`registry.instruments`) and 24h market stats (`registry.market_stats`).
2.  **Normalization**: A dedicated `RateProvider` queries dynamic USD conversion rates. It prioritizes local Gold/Registry data, with automated fallback to the **Frankfurter public FX API** (ECB data) for fiats and stablecoins. This ensures accurate volume normalization even for non-crypto pairs across historical dates.
3.  **Screening**: Filters for status, age, liquidity ratios, naming conventions, and asset type are applied.
4.  **Deduplication**: Polars `unique(subset=['base_asset'])` ensures a unique asset universe.

### 3.2 Data Contracts & Validation
Universe construction is hardened by rigorous data contracts:
- **`MarketStatsModel`**: Pydantic model enforcing types and validation for 24h stats.
- **`GoldUniverseStatsSchema`**: Pandera schema for historical daily snapshots in the Gold layer.

## 4. Usage for Quant Strategies
Quants use the builder to dynamically define their trading scope for backfills and live trading:

```python
builder = UniverseBuilder(lake_path="./lake")
universe = builder.build_top_50(
    trade_type='um',
    min_volume_usd=5_000_000, # Strategy-specific liquidity floor
    min_age_days=365          # Strategy-specific stability requirement
)
```

### 4.1 Point-in-Time Backtesting
To avoid survivorship bias and look-ahead bias during historical backtesting, you can generate the universe as it existed at a specific point in time:

```python
import time
backtest_ms = int(time.time() * 1000) - (365 * 86_400_000) # 1 year ago

universe = builder.build_top_50(
    trade_type='spot',
    as_of_timestamp_ms=backtest_ms
)
```
*Note: Point-in-time universe construction queries `gold.daily_universe_stats` by default to use historically accurate 24h trailing volume and market cap. This strictly eliminates survivorship bias by including assets that were trading at that time but have since been delisted (metadata is captured historically in the Gold layer).* Alternatively, you can pass `use_silver_on_the_fly=True` to compute a 30-day trailing ADV dynamically from `silver.klines` (slower, but requires no Gold layer materialization). If Gold layer stats are unavailable and Silver is not requested, the builder falls back to the current snapshot in `registry.market_stats` but logs a warning that true point-in-time volume is compromised.*
## 5. Production Orchestration
In a production environment, historical statistics are automatically updated via the **Universe Maintenance Flow** defined in `src/binance_datatool/workflow/prefect_flows.py`. This flow should be scheduled daily after the primary Silver layer ingestion completes.

```python
from binance_datatool.workflow.prefect_flows import universe_maintenance_flow

# Daily maintenance (defaults to target_date = yesterday)
universe_maintenance_flow(lake_path="./lake")
```

## 6. Architectural Principles
- **Separation of Concerns**: `UniverseBuilder` is a consumer, not a producer of metadata.
- **TDD/Specs Driven**: Ranking and filtering logic is verified against industry-standard benchmarks.
- **KISS/DRY**: Leverages centralized `get_connection()` and standard Polars transformations.
...
