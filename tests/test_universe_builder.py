from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from binance_datatool.universe.builder import UniverseBuilder


@pytest.fixture
def mock_db_connection():
    with patch("binance_datatool.universe.builder.get_connection") as mock_get_conn:
        mock_con = MagicMock()
        mock_get_conn.return_value = mock_con
        yield mock_con


def test_universe_builder_empty_registry(mock_db_connection):
    # Mock an empty registry
    mock_db_connection.execute.return_value.pl.return_value = pl.DataFrame()

    builder = UniverseBuilder(lake_path="fake_lake")
    result = builder.build_top_50("spot")

    assert result == []


def test_universe_builder_filters_stables_and_returns_top(mock_db_connection):
    import time

    now_ms = int(time.time() * 1000)
    ms_per_day = 86_400_000

    # 200 days ago
    onboard_date_valid = now_ms - (200 * ms_per_day)

    # Base metadata and market stats
    instruments_df = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "ETHUSDT", "USDCUSDT", "DOGEUSDT", "NEWCOINUSDT"],
            "venue_id": ["binance_spot"] * 5,
            "base_asset": ["BTC", "ETH", "USDC", "DOGE", "NEWCOIN"],
            "quote_asset": ["USDT"] * 5,
            "onboard_date": [
                onboard_date_valid,
                onboard_date_valid,
                onboard_date_valid,
                onboard_date_valid,
                now_ms,
            ],
            "quote_volume": [1_000_000, 5_000_000, 10_000_000, 2_000_000, 5_000_000],
            "market_cap": [10_000_000, 50_000_000, 10_000_000, 10_000_000, 50_000_000],
            "last_price": [60000.0, 3000.0, 1.0, 0.1, 1.0],
            "status": ["trading", "trading", "trading", "trading", "trading"],
        }
    )

    # USD conversion rates
    rates_df = pl.DataFrame({"symbol": ["BTCUSDT", "ETHUSDT"], "usd_rate": [60000.0, 3000.0]})

    def mock_execute(query, params=None):
        mock_result = MagicMock()
        if "registry.instruments" in query:
            mock_result.pl.return_value = instruments_df
        elif "usd_rate" in query:
            mock_result.pl.return_value = rates_df
            # Mocking to_dicts for the fallback logic
            mock_result.to_dicts.return_value = rates_df.to_dicts()
        else:
            mock_result.pl.return_value = pl.DataFrame()
        return mock_result

    mock_db_connection.execute.side_effect = mock_execute

    builder = UniverseBuilder(lake_path="fake_lake")
    result = builder.build_top_50(
        "spot", min_volume_usd=500_000, min_age_days=180, exclude_stables=True, exclude_memes=False
    )

    assert "BTCUSDT" in result
    assert "ETHUSDT" in result
    assert "DOGEUSDT" in result
    assert "USDCUSDT" not in result
    assert "NEWCOINUSDT" not in result


def test_universe_builder_handles_missing_rates(mock_db_connection):
    import time

    now_ms = int(time.time() * 1000)
    ms_per_day = 86_400_000
    onboard_date_valid = now_ms - (200 * ms_per_day)

    instruments_df = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "ETHUSDT", "XRPBTC"],
            "venue_id": ["binance_spot"] * 3,
            "base_asset": ["BTC", "ETH", "XRP"],
            "quote_asset": ["USDT", "USDT", "BTC"],  # BTC is quote
            "onboard_date": [onboard_date_valid] * 3,
            "quote_volume": [1_000_000.0, 5_000_000.0, 20.0],  # 20 BTC vol
            "market_cap": [10_000_000.0, 50_000_000.0, 20_000_000.0],
            "last_price": [60000.0, 3000.0, 0.00001],
            "status": ["trading"] * 3,
        }
    )

    # Empty rates to see if fallback to static/1.0 handles it without crashing
    rates_df = pl.DataFrame({"symbol": [], "usd_rate": []})

    def mock_execute(query, params=None):
        mock_result = MagicMock()
        if "registry.instruments" in query:
            mock_result.pl.return_value = instruments_df
        elif "usd_rate" in query:
            mock_result.pl.return_value = rates_df
            mock_result.to_dicts.return_value = []
        else:
            mock_result.pl.return_value = pl.DataFrame()
        return mock_result

    mock_db_connection.execute.side_effect = mock_execute

    builder = UniverseBuilder(lake_path="fake_lake")
    result = builder.build_top_50("spot", min_volume_usd=500_000)

    assert "BTCUSDT" in result
    assert "ETHUSDT" in result
    assert "XRPBTC" not in result


def test_universe_builder_institutional_filters(mock_db_connection):
    import time

    now_ms = int(time.time() * 1000)
    ms_per_day = 86_400_000
    onboard_date_valid = now_ms - (200 * ms_per_day)

    instruments_df = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "TRUMPUSDT", "BTCUPUSDT", "币安USDT", "1000PEPEUSDT"],
            "venue_id": [
                "binance_spot",
                "binance_spot",
                "binance_spot",
                "binance_spot",
                "binance_um",
            ],
            "base_asset": ["BTC", "TRUMP", "BTCUP", "币安", "1000PEPE"],
            "quote_asset": ["USDT"] * 5,
            "onboard_date": [onboard_date_valid] * 5,
            "quote_volume": [1_000_000.0] * 5,
            "market_cap": [10_000_000.0] * 5,
            "last_price": [1.0] * 5,
            "status": ["trading"] * 5,
        }
    )

    rates_df = pl.DataFrame({"symbol": [], "usd_rate": []})

    def mock_execute(query, params=None):
        mock_result = MagicMock()
        if "registry.instruments" in query:
            mock_result.pl.return_value = instruments_df
        elif "usd_rate" in query:
            mock_result.pl.return_value = rates_df
            mock_result.to_dicts.return_value = []
        else:
            mock_result.pl.return_value = pl.DataFrame()
        return mock_result

    mock_db_connection.execute.side_effect = mock_execute

    builder = UniverseBuilder(lake_path="fake_lake")
    result = builder.build_top_50("spot", min_volume_usd=500, exclude_memes=True)

    assert "BTCUSDT" in result
    assert "TRUMPUSDT" not in result  # meme
    assert "BTCUPUSDT" not in result  # leveraged
    assert "币安USDT" not in result  # non-ascii
    assert "1000PEPEUSDT" not in result  # meme (1000 prefix)


def test_universe_builder_point_in_time(mock_db_connection):
    import time

    now_ms = int(time.time() * 1000)
    ms_per_day = 86_400_000

    # Let's say backtest date is 300 days ago
    backtest_ms = now_ms - (300 * ms_per_day)

    # Gold layer data for the historical date
    # Includes DELISTED_COIN which is NOT in the current registry
    gold_df = pl.DataFrame(
        {
            "symbol": ["ASSET_A_USDT", "ASSET_B_USDT", "DELISTED_COIN_USDT"],
            "venue_id": ["binance_spot"] * 3,
            "ts_date": [(datetime.fromtimestamp(backtest_ms / 1000, tz=UTC)).date()] * 3,
            "base_asset": ["ASSET_A", "ASSET_B", "DELISTED_COIN"],
            "quote_asset": ["USDT"] * 3,
            "onboard_date": [
                now_ms - (500 * ms_per_day),
                now_ms - (200 * ms_per_day),
                now_ms - (600 * ms_per_day),
            ],
            "quote_volume": [1_000_000.0] * 3,
            "market_cap": [10_000_000.0] * 3,
            "last_price": [1.0] * 3,
        }
    )

    # Current registry (DELISTED_COIN is missing)
    instruments_df = pl.DataFrame(
        {
            "symbol": ["ASSET_A_USDT", "ASSET_B_USDT"],
            "venue_id": ["binance_spot"] * 2,
            "base_asset": ["ASSET_A", "ASSET_B"],
            "quote_asset": ["USDT"] * 2,
            "onboard_date": [now_ms - (500 * ms_per_day), now_ms - (200 * ms_per_day)],
            "status": ["trading"] * 2,
        }
    )

    rates_df = pl.DataFrame({"symbol": [], "usd_rate": []})

    def mock_execute(query, params=None):
        mock_result = MagicMock()
        if "gold.daily_universe_stats" in query:
            mock_result.pl.return_value = gold_df
        elif "registry.instruments" in query:
            mock_result.pl.return_value = instruments_df
        elif "usd_rate" in query:
            mock_result.pl.return_value = rates_df
            mock_result.to_dicts.return_value = []
        else:
            mock_result.pl.return_value = pl.DataFrame()
        return mock_result

    mock_db_connection.execute.side_effect = mock_execute

    builder = UniverseBuilder(lake_path="fake_lake")
    result = builder.build_top_50(
        "spot", min_volume_usd=500_000, min_age_days=180, as_of_timestamp_ms=backtest_ms
    )

    # ASSET_A is valid
    # ASSET_B is filtered out because onboard_date > backtest_ms (listingLookahead)
    # DELISTED_COIN is valid because it was in Gold at that time, even if gone from registry now
    assert "ASSET_A_USDT" in result
    assert "ASSET_B_USDT" not in result
    assert "DELISTED_COIN_USDT" in result


def test_universe_builder_silver_on_the_fly(mock_db_connection):
    import time

    now_ms = int(time.time() * 1000)
    ms_per_day = 86_400_000

    backtest_ms = now_ms - (300 * ms_per_day)

    instruments_df = pl.DataFrame(
        {
            "symbol": ["ASSET_A_USDT"],
            "venue_id": ["binance_spot"],
            "base_asset": ["ASSET_A"],
            "quote_asset": ["USDT"],
            "onboard_date": [now_ms - (500 * ms_per_day)],
            "quote_volume": [1_000_000.0],
            "market_cap": [10_000_000.0],
            "last_price": [1.0],
            "status": ["trading"],
        }
    )

    rates_df = pl.DataFrame({"symbol": [], "usd_rate": []})

    def mock_execute(query, params=None):
        mock_result = MagicMock()
        # Verify the correct silver query is matched
        if "silver.klines" in query or "registry.instruments" in query:
            mock_result.pl.return_value = instruments_df
        elif "usd_rate" in query:
            mock_result.pl.return_value = rates_df
            mock_result.to_dicts.return_value = []
        else:
            mock_result.pl.return_value = pl.DataFrame()
        return mock_result

    mock_db_connection.execute.side_effect = mock_execute

    builder = UniverseBuilder(lake_path="fake_lake")
    result = builder.build_top_50(
        "spot",
        min_volume_usd=500_000,
        min_age_days=180,
        as_of_timestamp_ms=backtest_ms,
        use_silver_on_the_fly=True,
    )

    assert "ASSET_A_USDT" in result
