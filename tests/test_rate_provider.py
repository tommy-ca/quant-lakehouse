from unittest.mock import MagicMock

import polars as pl

from binance_datatool.universe.rates import RateProvider


def test_rate_provider_fetches_and_merges():
    mock_con = MagicMock()

    # Mock the DB returning dynamic rates for BTC and ETH
    mock_con.execute.return_value.pl.return_value = pl.DataFrame(
        {"symbol": ["BTCUSDT", "ETHUSDT"], "usd_rate": [60000.0, 3000.0]}
    )

    provider = RateProvider(fallback_rates={"USDT": 1.0, "TRY": 0.03})
    rates = provider.get_usd_rates(mock_con)

    assert rates["USDT"] == 1.0
    assert rates["TRY"] == 0.03
    assert rates["BTC"] == 60000.0
    assert rates["ETH"] == 3000.0


def test_rate_provider_empty_db():
    mock_con = MagicMock()
    mock_con.execute.return_value.pl.return_value = pl.DataFrame()

    provider = RateProvider(fallback_rates={"USDT": 1.0})
    rates = provider.get_usd_rates(mock_con)

    assert rates["USDT"] == 1.0
    assert "BTC" not in rates


def test_rate_provider_as_of_timestamp():
    mock_con = MagicMock()

    # Return a mocked dataframe for the gold layer query
    mock_con.execute.return_value.pl.return_value = pl.DataFrame(
        {"symbol": ["BTCUSDT"], "usd_rate": [50000.0]}
    )

    provider = RateProvider(fallback_rates={"USDT": 1.0})
    rates = provider.get_usd_rates(mock_con, as_of_timestamp_ms=1716000000000)

    assert rates["BTC"] == 50000.0
    # Verify it queried gold.daily_universe_stats
    calls = mock_con.execute.call_args_list
    assert "gold.daily_universe_stats" in calls[0][0][0]
    assert calls[0][0][1] == [1716000000000]


def test_rate_provider_as_of_timestamp_fallback(monkeypatch):
    import duckdb

    mock_con = MagicMock()

    def mock_execute(query, params=None):
        mock_result = MagicMock()
        if "gold.daily_universe_stats" in query:
            raise duckdb.Error("Table not found")
        elif "registry.market_stats" in query:
            mock_result.pl.return_value = pl.DataFrame(
                {"symbol": ["ETHUSDT"], "usd_rate": [3000.0]}
            )
        return mock_result

    mock_con.execute.side_effect = mock_execute

    provider = RateProvider(fallback_rates={"USDT": 1.0})
    rates = provider.get_usd_rates(mock_con, as_of_timestamp_ms=1716000000000)

    # Verify it fell back to registry.market_stats
    assert rates["ETH"] == 3000.0
    assert "BTC" not in rates
