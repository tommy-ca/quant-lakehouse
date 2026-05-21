"""End-to-End Validation script for Point-in-Time Universe Pipeline.

This script performs a full lifecycle validation:
1. Seeds a temporary DuckDB catalog with Silver klines and Registry metadata.
2. Executes the Universe Maintenance pipeline for a historical date.
3. Verifies zero-survivorship-bias by including a 'delisted' coin.
4. Validates the top-50 construction at that specific timestamp.
"""

import shutil
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from loguru import logger

from binance_datatool.storage.duckdb import get_connection
from binance_datatool.universe.builder import UniverseBuilder
from binance_datatool.universe.gold_pipeline import build_daily_universe_stats

# Setup temporary validation environment
VALIDATION_LAKE = Path("./lake_validation")
if VALIDATION_LAKE.exists():
    shutil.rmtree(VALIDATION_LAKE)
VALIDATION_LAKE.mkdir(parents=True)


def seed_data():
    """Seed the lake with historical data."""
    con = get_connection(lake_path=VALIDATION_LAKE)

    # 1. Setup Schemas
    con.execute("CREATE SCHEMA IF NOT EXISTS registry")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    # 2. Seed registry.instruments
    # ASSET_A: Still trading
    # ASSET_B: Future coin (listed after backtest)
    # DELISTED_X: Trading at backtest time, but status='delisted' now

    now_ms = int(time.time() * 1000)
    backtest_date = "2024-01-01"
    backtest_ts = int(
        datetime.strptime(backtest_date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp() * 1000
    )

    con.execute(
        "CREATE TABLE registry.instruments AS SELECT * FROM ?",
        [
            pl.DataFrame(
                {
                    "symbol": ["ASSET_A_USDT", "ASSET_B_USDT", "DELISTED_X_USDT"],
                    "venue_id": ["binance_spot"] * 3,
                    "base_asset": ["ASSET_A", "ASSET_B", "DELISTED_X"],
                    "quote_asset": ["USDT"] * 3,
                    "onboard_date": [
                        backtest_ts - 500 * 86400000,
                        backtest_ts + 100 * 86400000,
                        backtest_ts - 200 * 86400000,
                    ],
                    "status": ["trading", "trading", "delisted"],
                    "tick_size": [0.01] * 3,
                    "lot_size": [0.01] * 3,
                    "min_notional": [10.0] * 3,
                    "contract_type": ["spot"] * 3,
                }
            )
        ],
    )

    # 3. Seed registry.market_stats
    # Required for RateProvider dynamic rate fetching
    con.execute(
        "CREATE TABLE registry.market_stats AS SELECT * FROM ?",
        [
            pl.DataFrame(
                {
                    "symbol": [
                        "BTCUSDT",
                        "ETHUSDT",
                        "BNBUSDT",
                        "ASSET_A_USDT",
                        "ASSET_B_USDT",
                        "DELISTED_X_USDT",
                    ],
                    "venue_id": ["binance_spot"] * 6,
                    "quote_volume": [
                        100000000.0,
                        50000000.0,
                        10000000.0,
                        1000000.0,
                        5000000.0,
                        2000000.0,
                    ],
                    "market_cap": [
                        1000000000000.0,
                        300000000000.0,
                        80000000000.0,
                        10000000.0,
                        50000000.0,
                        20000000.0,
                    ],
                    "last_price": [60000.0, 3000.0, 600.0, 100.0, 50.0, 10.0],
                    "price_change_pct": [0.0] * 6,
                    "fetched_at": [now_ms] * 6,
                }
            )
        ],
    )

    # 4. Seed silver.klines for the backtest date
    # Volume: DELISTED_X (2M), ASSET_A (1M), ASSET_B (5M)
    con.execute(
        "CREATE TABLE silver.klines AS SELECT * FROM ?",
        [
            pl.DataFrame(
                {
                    "symbol": ["ASSET_A_USDT", "ASSET_B_USDT", "DELISTED_X_USDT"],
                    "exchange": ["binance_spot"] * 3,
                    "ts_date": [datetime.strptime(backtest_date, "%Y-%m-%d").date()] * 3,
                    "ts_event": [backtest_ts] * 3,
                    "close": [100.0, 50.0, 10.0],
                    "quote_volume": [1000000.0, 5000000.0, 2000000.0],
                }
            )
        ],
    )
    con.close()
    return backtest_date, backtest_ts


def run_validation():
    logger.info("Starting E2E Universe Pipeline Validation...")

    backtest_date, backtest_ts = seed_data()

    # Step 1: Run Universe Maintenance (Gold Pipeline)
    logger.info(f"Step 1: Building Gold Stats for {backtest_date}")
    build_daily_universe_stats(lake_path=VALIDATION_LAKE, target_date=backtest_date)

    # Step 2: Verify Gold Metadata Capture
    con = get_connection(lake_path=VALIDATION_LAKE)
    gold_check = con.execute(
        "SELECT symbol, base_asset, onboard_date FROM gold.daily_universe_stats"
    ).pl()
    logger.info("Gold Table Content:\n{}", gold_check)

    assert "DELISTED_X_USDT" in gold_check["symbol"].to_list(), "Delisted coin missing from Gold!"
    assert gold_check.filter(pl.col("symbol") == "DELISTED_X_USDT")["base_asset"][0] == "DELISTED_X"

    # Step 3: Run Point-in-Time Universe Construction
    logger.info(f"Step 2: Constructing Top-50 Universe as of {backtest_date}")
    builder = UniverseBuilder(lake_path=VALIDATION_LAKE)

    # We use a low volume threshold to catch our test coins
    universe = builder.build_top_50(
        trade_type="spot", as_of_timestamp_ms=backtest_ts, min_volume_usd=500_000, min_age_days=30
    )

    logger.info("Constructed Historical Universe: {}", universe)

    # Assertions:
    # 1. DELISTED_X MUST be in (Zero-Survivorship Bias check)
    # 2. ASSET_A MUST be in
    # 3. ASSET_B MUST NOT be in (Look-ahead Bias check: listed 100 days after backtest)

    assert "ASSET_A_USDT" in universe
    assert "DELISTED_X_USDT" in universe
    assert "ASSET_B_USDT" not in universe

    # Rank check: DELISTED_X (2M) should be higher than ASSET_A (1M)
    assert universe.index("DELISTED_X_USDT") < universe.index("ASSET_A_USDT")

    logger.success(
        "E2E Validation PASSED: Point-in-Time accuracy, zero-survivorship, and look-ahead bias protection verified."
    )
    con.close()


if __name__ == "__main__":
    try:
        run_validation()
    finally:
        if VALIDATION_LAKE.exists():
            shutil.rmtree(VALIDATION_LAKE)
