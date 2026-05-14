"""Tests for Bronze → Silver transforms (Polars-based)."""

from __future__ import annotations

import polars as pl
import pytest

from binance_datatool.common.enums import exchange_for
from binance_datatool.transforms.agg_trades import bronze_agg_trades_to_silver
from binance_datatool.transforms.funding_rate import bronze_funding_rate_to_silver
from binance_datatool.transforms.klines import bronze_klines_to_silver


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
        assert exchange_for(tt) == expected

    def test_unknown_trade_type_raises(self):
        """Verify unknown trade_type raises ValueError."""
        with pytest.raises(ValueError, match="Unknown trade_type"):
            exchange_for("unknown")

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


class TestBronzeAggTradesToSilver:
    """Tests for the Polars Bronze→Silver aggTrades transform."""

    def test_basic_transform_adds_all_columns(self):
        """Verify the transform produces all expected Silver columns."""
        bronze = pl.DataFrame(
            {
                "agg_trade_id": [3952950559, 3952950560],
                "price": ["80006.00", "80007.00"],
                "quantity": ["1.16331", "6.24960"],
                "first_trade_id": ["6280951459", "6280951460"],
                "last_trade_id": ["6280951459", "6280951460"],
                "transact_time": [1778198400018655, 1778198400018786],
                "is_buyer_maker": ["False", "True"],
                "symbol": ["BTCUSDT", "BTCUSDT"],
            }
        )

        silver = bronze_agg_trades_to_silver(bronze, symbol="BTCUSDT", trade_type="spot")

        expected = {
            "ts_event",
            "ts_recv",
            "price",
            "size",
            "side",
            "trade_id",
            "is_buyer_maker",
            "agg_trade_id",
            "first_trade_id",
            "last_trade_id",
            "rtype",
            "source",
            "exchange",
            "trade_type",
            "symbol",
            "data_type",
            "ingested_at",
            "ts_date",
        }
        assert set(silver.columns) == expected
        assert silver.height == 2

    def test_side_is_buyer_maker_false_is_buy(self):
        bronze = pl.DataFrame(
            {
                "agg_trade_id": [1],
                "price": ["100.0"],
                "quantity": ["1.0"],
                "first_trade_id": ["100"],
                "last_trade_id": ["100"],
                "transact_time": [1700000000000],
                "is_buyer_maker": ["False"],
                "symbol": ["BTCUSDT"],
            }
        )
        silver = bronze_agg_trades_to_silver(bronze, symbol="BTCUSDT")
        assert silver["side"][0] == "buy"

    def test_side_is_buyer_maker_true_is_sell(self):
        bronze = pl.DataFrame(
            {
                "agg_trade_id": [1],
                "price": ["100.0"],
                "quantity": ["1.0"],
                "first_trade_id": ["100"],
                "last_trade_id": ["100"],
                "transact_time": [1700000000000],
                "is_buyer_maker": ["True"],
                "symbol": ["BTCUSDT"],
            }
        )
        silver = bronze_agg_trades_to_silver(bronze, symbol="BTCUSDT")
        assert silver["side"][0] == "sell"

    def test_rtype_is_agg(self):
        bronze = pl.DataFrame(
            {
                "agg_trade_id": [1],
                "price": ["100.0"],
                "quantity": ["1.0"],
                "first_trade_id": ["100"],
                "last_trade_id": ["100"],
                "transact_time": [1700000000000],
                "is_buyer_maker": ["False"],
                "symbol": ["BTCUSDT"],
            }
        )
        silver = bronze_agg_trades_to_silver(bronze, symbol="BTCUSDT")
        assert silver["rtype"][0] == "agg"

    def test_first_last_trade_id_preserved(self):
        bronze = pl.DataFrame(
            {
                "agg_trade_id": [1],
                "price": ["100.0"],
                "quantity": ["1.0"],
                "first_trade_id": ["6280951459"],
                "last_trade_id": ["6280951460"],
                "transact_time": [1700000000000],
                "is_buyer_maker": ["False"],
                "symbol": ["BTCUSDT"],
            }
        )
        silver = bronze_agg_trades_to_silver(bronze, symbol="BTCUSDT")
        assert silver["first_trade_id"][0] == 6280951459
        assert silver["last_trade_id"][0] == 6280951460

    def test_trade_id_matches_agg_trade_id(self):
        bronze = pl.DataFrame(
            {
                "agg_trade_id": [42],
                "price": ["100.0"],
                "quantity": ["1.0"],
                "first_trade_id": [""],
                "last_trade_id": [""],
                "transact_time": [1700000000000],
                "is_buyer_maker": ["False"],
                "symbol": ["BTCUSDT"],
            }
        )
        silver = bronze_agg_trades_to_silver(bronze, symbol="BTCUSDT")
        assert silver["trade_id"][0] == 42
        assert silver["first_trade_id"][0] == 0  # empty → 0
        assert silver["last_trade_id"][0] == 0  # empty → 0

    def test_empty_input(self):
        bronze = pl.DataFrame(
            {
                "agg_trade_id": [],
                "price": [],
                "quantity": [],
                "first_trade_id": [],
                "last_trade_id": [],
                "transact_time": [],
                "is_buyer_maker": [],
                "symbol": [],
            }
        )
        silver = bronze_agg_trades_to_silver(bronze, symbol="BTCUSDT")
        assert silver.height == 0
        assert len(silver.columns) == 18

    def test_field_types(self):
        bronze = pl.DataFrame(
            {
                "agg_trade_id": [1],
                "price": ["100.0"],
                "quantity": ["1.0"],
                "first_trade_id": ["100"],
                "last_trade_id": ["100"],
                "transact_time": [1700000000000],
                "is_buyer_maker": ["False"],
                "symbol": ["BTCUSDT"],
            }
        )
        silver = bronze_agg_trades_to_silver(bronze, symbol="BTCUSDT")
        assert silver.schema["price"] == pl.Float64
        assert silver.schema["size"] == pl.Float64
        assert silver.schema["trade_id"] == pl.Int64
        assert silver.schema["is_buyer_maker"] == pl.Int64
        assert silver.schema["ts_event"] == pl.Int64
        assert silver.schema["symbol"] == pl.Utf8


class TestBronzeFundingRateToSilver:
    """Tests for the Polars Bronze→Silver fundingRate transform."""

    def test_basic_transform_adds_all_columns(self):
        bronze = pl.DataFrame(
            {
                "symbol": ["BTCUSDT"],
                "funding_time": [1775001600000],
                "funding_rate": ["-0.00003449"],
                "mark_price": ["85000.0"],
            }
        )

        silver = bronze_funding_rate_to_silver(bronze, symbol="BTCUSDT", trade_type="um")

        expected = {
            "ts_event",
            "ts_recv",
            "funding_rate",
            "mark_price",
            "funding_timestamp",
            "source",
            "exchange",
            "trade_type",
            "symbol",
            "data_type",
            "ingested_at",
            "ts_date",
        }
        assert set(silver.columns) == expected
        assert silver.height == 1

    def test_funding_timestamp_matches_ts_event(self):
        bronze = pl.DataFrame(
            {
                "symbol": ["BTCUSDT"],
                "funding_time": [1775001600000],
                "funding_rate": ["0.0001"],
                "mark_price": ["85000.0"],
            }
        )
        silver = bronze_funding_rate_to_silver(bronze, symbol="BTCUSDT", trade_type="um")
        assert silver["ts_event"][0] == silver["funding_timestamp"][0]

    def test_types_are_correct(self):
        bronze = pl.DataFrame(
            {
                "symbol": ["BTCUSDT"],
                "funding_time": [1775001600000],
                "funding_rate": ["0.0001"],
                "mark_price": ["85000.0"],
            }
        )
        silver = bronze_funding_rate_to_silver(bronze, symbol="BTCUSDT", trade_type="um")
        assert silver.schema["funding_rate"] == pl.Float64
        assert silver.schema["mark_price"] == pl.Float64
        assert silver.schema["ts_event"] == pl.Int64
        assert silver.schema["symbol"] == pl.Utf8

    def test_empty_input(self):
        bronze = pl.DataFrame(
            {
                "symbol": [],
                "funding_time": [],
                "funding_rate": [],
                "mark_price": [],
            }
        )
        silver = bronze_funding_rate_to_silver(bronze, symbol="BTCUSDT", trade_type="um")
        assert silver.height == 0
        assert len(silver.columns) == 12

    def test_mark_price_round_trips(self):
        bronze = pl.DataFrame(
            {
                "symbol": ["BTCUSDT"],
                "funding_time": [1775001600000],
                "funding_rate": ["0.0001"],
                "mark_price": ["85000.50"],
            }
        )
        silver = bronze_funding_rate_to_silver(bronze, symbol="BTCUSDT", trade_type="um")
        assert silver["mark_price"][0] == 85000.5
