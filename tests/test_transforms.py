"""Tests for Bronze → Silver transforms (Polars-based)."""

from __future__ import annotations

import polars as pl
import pytest

from binance_datatool.transforms.klines import _exchange_for, bronze_klines_to_silver


class TestBronzeKlinesToSilver:
    """Tests for the Polars Bronze→Silver klines transform."""

    def test_basic_transform_adds_metadata_columns(self):
        """Verify the transform adds all expected Silver metadata columns."""
        bronze = pl.DataFrame(
            {
                "open_time": [1700000000000],
                "open": ["100.0"],
                "high": ["101.0"],
                "low": ["99.0"],
                "close": ["100.5"],
                "volume": ["1000.0"],
                "close_time": [1700003600000],
                "quote_volume": ["100500.0"],
                "count": [500],
                "taker_buy_volume": ["600.0"],
                "taker_buy_quote_volume": ["60300.0"],
                "symbol": ["BTCUSDT"],
                "interval": ["1h"],
            }
        )

        silver = bronze_klines_to_silver(bronze, symbol="BTCUSDT", interval="1h", trade_type="spot")

        expected_columns = {
            "ts_event",
            "ts_recv",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "trade_count",
            "taker_buy_volume",
            "taker_buy_quote_volume",
            "source",
            "exchange",
            "trade_type",
            "symbol",
            "interval",
            "data_type",
            "ingested_at",
            "ts_date",
        }
        assert set(silver.columns) == expected_columns
        assert silver.height == 1

    def test_field_types_are_correct(self):
        """Verify numeric fields are cast to Float64/Int64 and strings to Utf8."""
        bronze = pl.DataFrame(
            {
                "open_time": [1700000000000],
                "open": ["100.0"],
                "high": ["101.0"],
                "low": ["99.0"],
                "close": ["100.5"],
                "volume": ["1000.0"],
                "close_time": [1700003600000],
                "quote_volume": ["100500.0"],
                "count": [500],
                "taker_buy_volume": ["600.0"],
                "taker_buy_quote_volume": ["60300.0"],
                "symbol": ["BTCUSDT"],
                "interval": ["1h"],
            }
        )

        silver = bronze_klines_to_silver(bronze, symbol="BTCUSDT", interval="1h")

        assert silver.schema["open"] == pl.Float64
        assert silver.schema["high"] == pl.Float64
        assert silver.schema["low"] == pl.Float64
        assert silver.schema["close"] == pl.Float64
        assert silver.schema["volume"] == pl.Float64
        assert silver.schema["quote_volume"] == pl.Float64
        assert silver.schema["trade_count"] == pl.Int64
        assert silver.schema["taker_buy_volume"] == pl.Float64
        assert silver.schema["ts_event"] == pl.Int64
        assert silver.schema["symbol"] == pl.Utf8
        assert silver.schema["source"] == pl.Utf8
        assert silver.schema["trade_type"] == pl.Utf8

    def test_source_label_defaults_to_dlt_api(self):
        """Verify default source label is ``dlt_api``."""
        bronze = pl.DataFrame(
            {
                "open_time": [1700000000000],
                "open": ["100.0"],
                "high": ["101.0"],
                "low": ["99.0"],
                "close": ["100.5"],
                "volume": ["1000.0"],
                "close_time": [1700003600000],
                "quote_volume": ["100500.0"],
                "count": [500],
                "taker_buy_volume": ["600.0"],
                "taker_buy_quote_volume": ["60300.0"],
                "symbol": ["BTCUSDT"],
                "interval": ["1h"],
            }
        )

        silver = bronze_klines_to_silver(bronze, symbol="BTCUSDT", interval="1h")
        assert silver["source"][0] == "dlt_api"

    def test_source_label_can_be_overridden(self):
        """Verify ``source`` parameter overrides the default."""
        bronze = pl.DataFrame(
            {
                "open_time": [1700000000000],
                "open": ["100.0"],
                "high": ["101.0"],
                "low": ["99.0"],
                "close": ["100.5"],
                "volume": ["1000.0"],
                "close_time": [1700003600000],
                "quote_volume": ["100500.0"],
                "count": [500],
                "taker_buy_volume": ["600.0"],
                "taker_buy_quote_volume": ["60300.0"],
                "symbol": ["BTCUSDT"],
                "interval": ["1h"],
            }
        )

        silver = bronze_klines_to_silver(bronze, symbol="BTCUSDT", interval="1h", source="archive")
        assert silver["source"][0] == "archive"

    @pytest.mark.parametrize(
        "tt,expected",
        [
            ("spot", "binance-spot"),
            ("um", "binance-perps-um"),
            ("cm", "binance-perps-cm"),
        ],
    )
    def test_exchange_mapping(self, tt, expected):
        """Verify trade_type → exchange name mapping."""
        assert _exchange_for(tt) == expected

    def test_unknown_trade_type_defaults_to_spot(self):
        """Verify unknown trade_type falls back to binance-spot."""
        assert _exchange_for("unknown") == "binance-spot"

    def test_ts_date_is_derived_from_open_time(self):
        """Verify ts_date is correctly computed from open_time (ms → date)."""
        bronze = pl.DataFrame(
            {
                "open_time": [1700000000000],
                "open": ["100.0"],
                "high": ["101.0"],
                "low": ["99.0"],
                "close": ["100.5"],
                "volume": ["1000.0"],
                "close_time": [1700003600000],
                "quote_volume": ["100500.0"],
                "count": [500],
                "taker_buy_volume": ["600.0"],
                "taker_buy_quote_volume": ["60300.0"],
                "symbol": ["BTCUSDT"],
                "interval": ["1h"],
            }
        )

        silver = bronze_klines_to_silver(bronze, symbol="BTCUSDT", interval="1h")
        # 1700000000000 ms → 2023-11-14, ts_event in μs
        assert str(silver["ts_date"][0]) == "2023-11-14"
        assert silver["ts_event"][0] == 1700000000000000  # ms * 1000 = μs

    def test_empty_input_returns_empty_output(self):
        """Verify empty bronze DataFrame produces empty silver."""
        bronze = pl.DataFrame(
            {
                "open_time": [],
                "open": [],
                "high": [],
                "low": [],
                "close": [],
                "volume": [],
                "close_time": [],
                "quote_volume": [],
                "count": [],
                "taker_buy_volume": [],
                "taker_buy_quote_volume": [],
                "symbol": [],
                "interval": [],
            }
        )

        silver = bronze_klines_to_silver(bronze, symbol="BTCUSDT", interval="1h")
        assert silver.height == 0
        assert len(silver.columns) == 19

    def test_ingested_at_is_set(self):
        """Verify ingested_at is set to a reasonable recent timestamp."""
        import time

        bronze = pl.DataFrame(
            {
                "open_time": [1700000000000],
                "open": ["100.0"],
                "high": ["101.0"],
                "low": ["99.0"],
                "close": ["100.5"],
                "volume": ["1000.0"],
                "close_time": [1700003600000],
                "quote_volume": ["100500.0"],
                "count": [500],
                "taker_buy_volume": ["600.0"],
                "taker_buy_quote_volume": ["60300.0"],
                "symbol": ["BTCUSDT"],
                "interval": ["1h"],
            }
        )

        now_s = time.time()
        silver = bronze_klines_to_silver(bronze, symbol="BTCUSDT", interval="1h")
        ingested_us = silver["ingested_at"][0]
        # Should be within 10 seconds of now (in μs)
        assert abs(ingested_us / 1_000_000 - now_s) < 10
