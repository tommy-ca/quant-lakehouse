# Architecture Strategy: True Point-in-Time Universe

## 1. Context & Identified Architectural Flaw
During a deep architecture and code review of the `UniverseBuilder` implementation, a significant architectural flaw was identified regarding the "Point-in-Time" backtesting feature (`as_of_timestamp_ms`).

Currently, the point-in-time implementation correctly filters out assets based on their `onboard_date` (preventing look-ahead bias for asset existence). However, the `volume_usd` and `market_cap` metrics are sourced directly from `registry.market_stats`. The `market_stats` table in the Lakehouse is a snapshot table that only holds the *latest* 24h metrics.

**The Flaw**: If a quant requests the top 50 universe for `2024-01-01`, the builder correctly only considers tokens that existed before that date. But it ranks them using their *current* (e.g., 2026) liquidity and market cap. This allows tokens that were small in 2024 but exploded in 2025/2026 to improperly rank highly in a 2024 backtest, introducing severe **survivorship bias**.

## 2. Proposed Architecture Solutions

To achieve true point-in-time construction, we must sever the dependency on the `market_stats` snapshot for historical queries.

### Option A: Slowly Changing Dimensions (SCD Type 2) for Market Stats
- **Mechanism**: Update the Lakehouse ingestion pipeline to track historical changes to `market_stats` rather than overwriting. Each record would have `valid_from` and `valid_to` timestamps.
- **Pros**: Fast query performance at read time. Keeps the `UniverseBuilder` logic relatively simple.
- **Cons**: `market_stats` is highly volatile (changes every second). Tracking full history is expensive and potentially massive. We only really need daily snapshots.

### Option B: On-the-Fly Calculation from Silver Klines (Recommended)
- **Mechanism**: When `as_of_timestamp_ms` is provided, `UniverseBuilder` dynamically queries the `silver.klines` tables to compute the 30-day trailing Average Daily Volume (ADV) and market cap (price * circulating supply, or proxy via price history).
- **Pros**: 100% accurate to the actual trading data. No new tables required. Follows a true ELT paradigm.
- **Cons**: Slower query execution (requires scanning massive partitioned parquet files).

### Option C: Daily Metadata Snapshots (Gold Layer)
- **Mechanism**: Introduce a new Gold layer table: `gold.daily_universe_stats`. A daily scheduled job aggregates volume and price data from Silver and stores a daily snapshot for all tradable instruments.
- **Pros**: Blends accuracy with high performance. Specifically designed for backtesting queries.

## 3. Implementation Status
1. **Option C (Gold Layer)** has been implemented as the default strategy for point-in-time universe construction, powered by the new `gold_pipeline.py`.
2. **Option B (Silver On-the-Fly)** has been optionally integrated via the `use_silver_on_the_fly=True` parameter in `UniverseBuilder.build_top_50()`. This allows users to skip materializing the Gold layer at the cost of query execution time.
3. **Fallback**: If point-in-time metrics are requested but fail, the system gracefully falls back to `registry.market_stats` for real-time live trading compatibility.
