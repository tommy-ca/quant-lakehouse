from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from binance_datatool.universe.gold_pipeline import build_daily_universe_stats


@pytest.fixture
def mock_db_connection():
    with patch("binance_datatool.universe.gold_pipeline.get_connection") as mock_get_conn:
        mock_con = MagicMock()
        mock_get_conn.return_value = mock_con
        yield mock_con


def test_build_daily_universe_stats_no_silver_table(mock_db_connection):
    # Mock information_schema returning no tables
    mock_db_connection.execute.return_value.fetchall.return_value = []

    build_daily_universe_stats(lake_path="fake_lake", target_date="2024-01-01")

    # Verify it checked for the table and returned early
    assert "information_schema" in mock_db_connection.execute.call_args_list[2][0][0]

    # Verify DELETE/INSERT were NOT called
    for call in mock_db_connection.execute.call_args_list:
        assert "DELETE FROM gold" not in call[0][0]


def test_build_daily_universe_stats_success(mock_db_connection):
    # Mock information_schema returning silver.klines
    mock_db_connection.execute.return_value.fetchall.return_value = [("klines",)]

    build_daily_universe_stats(lake_path="fake_lake", target_date="2024-01-01")

    # Verify execution flow
    calls = mock_db_connection.execute.call_args_list
    assert "CREATE SCHEMA IF NOT EXISTS gold" in calls[0][0][0]
    assert "CREATE TABLE IF NOT EXISTS gold.daily_universe_stats" in calls[1][0][0]
    assert "information_schema" in calls[2][0][0]
    assert "BEGIN TRANSACTION" in calls[3][0][0]
    assert "DELETE FROM gold.daily_universe_stats" in calls[4][0][0]
    assert "INSERT INTO gold.daily_universe_stats" in calls[5][0][0]
    assert "arg_max(k.close, k.ts_event)" in calls[5][0][0]  # Expecting the fixed SQL
    assert "COMMIT" in calls[6][0][0]


def test_build_daily_universe_stats_default_date(mock_db_connection):
    mock_db_connection.execute.return_value.fetchall.return_value = [("klines",)]

    with patch("binance_datatool.universe.gold_pipeline.datetime") as mock_datetime:
        # Mock current date to 2024-01-02, expecting target_date to be 2024-01-01
        mock_datetime.now.return_value = datetime(2024, 1, 2, tzinfo=UTC)
        mock_datetime.UTC = UTC

        build_daily_universe_stats(lake_path="fake_lake")

        calls = mock_db_connection.execute.call_args_list
        insert_call = calls[5]
        # target_date is passed as a parameter
        assert insert_call[0][1] == ["2024-01-01", "2024-01-01"]
