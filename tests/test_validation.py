"""Tests for Pandera schemas and Pydantic models (validation module)."""

from __future__ import annotations

import polars as pl
import pytest
from pydantic import ValidationError

from binance_datatool.dlt.models import AggTradeModel, FundingRateModel, KlineModel
from binance_datatool.validation.schemas import (
    BronzeKlinesSchema,
    SilverKlinesSchema,
    check_high_gte_low,
    validate_bronze_klines,
    validate_silver_klines,
)

# ── Pydantic Model Tests ─────────────────────────────────────────


class TestKlineModel:
    """Tests for the kline Pydantic model used in dlt resources."""

    def test_valid_kline(self):
        k = KlineModel(
            open_time=1700000000000,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000.0,
            close_time=1700003600000,
            quote_volume=100500.0,
            count=500,
            symbol="BTCUSDT",
            interval="1h",
        )
        assert k.symbol == "BTCUSDT"
        assert k.open == 100.0

    def test_high_lt_low_raises(self):
        with pytest.raises(ValidationError):
            KlineModel(
                open_time=1700000000000,
                open=100.0,
                high=98.0,
                low=101.0,
                close=99.0,
                volume=1000.0,
                close_time=1700003600000,
                symbol="BTCUSDT",
                interval="1h",
            )

    def test_negative_volume_raises(self):
        with pytest.raises(ValidationError, match="volume"):
            KlineModel(
                open_time=1700000000000,
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=-1.0,
                close_time=1700003600000,
                symbol="BTCUSDT",
                interval="1h",
            )

    def test_zero_open_time_raises(self):
        with pytest.raises(ValidationError, match="open_time"):
            KlineModel(
                open_time=0,
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1000.0,
                close_time=1700003600000,
                symbol="BTCUSDT",
                interval="1h",
            )

    def test_optional_fields_default_to_none(self):
        k = KlineModel(
            open_time=1700000000000,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000.0,
            close_time=1700003600000,
            symbol="BTCUSDT",
            interval="1h",
        )
        assert k.quote_volume is None
        assert k.taker_buy_volume is None
        assert k.count == 0


class TestAggTradeModel:
    """Tests for the aggTrade Pydantic model."""

    def test_valid_agg_trade(self):
        t = AggTradeModel(
            agg_trade_id=1,
            price=50000.0,
            quantity=0.5,
            transact_time=1700000000000,
            is_buyer_maker=False,
            symbol="BTCUSDT",
        )
        assert t.agg_trade_id == 1
        assert t.price == 50000.0

    def test_zero_price_raises(self):
        with pytest.raises(ValidationError, match="price"):
            AggTradeModel(
                agg_trade_id=1,
                price=0.0,
                quantity=0.5,
                transact_time=1700000000000,
                is_buyer_maker=False,
                symbol="BTCUSDT",
            )

    def test_negative_quantity_raises(self):
        with pytest.raises(ValidationError, match="quantity"):
            AggTradeModel(
                agg_trade_id=1,
                price=50000.0,
                quantity=-0.5,
                transact_time=1700000000000,
                is_buyer_maker=False,
                symbol="BTCUSDT",
            )


class TestFundingRateModel:
    """Tests for the fundingRate Pydantic model."""

    def test_valid_funding_rate(self):
        f = FundingRateModel(symbol="BTCUSDT", funding_time=1700000000000, funding_rate=0.0001)
        assert f.funding_rate == 0.0001
        assert f.funding_time == 1700000000000


# ── Pandera Schema Tests ─────────────────────────────────────────


class TestBronzeKlinesSchema:
    """Tests for the Bronze klines Pandera schema."""

    def test_valid_dataframe_passes(self):
        df = pl.DataFrame(
            {
                "open_time": [1700000000000],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000.0],
                "close_time": [1700003600000],
                "quote_volume": [100500.0],
                "count": [500],
                "taker_buy_volume": [600.0],
                "taker_buy_quote_volume": [60300.0],
                "symbol": ["BTCUSDT"],
                "interval": ["1h"],
            }
        )
        result = BronzeKlinesSchema.validate(df)
        assert result is not None

    def test_strict_rejects_extra_columns(self):
        df = pl.DataFrame(
            {
                "open_time": [1700000000000],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000.0],
                "close_time": [1700003600000],
                "quote_volume": [100500.0],
                "count": [500],
                "taker_buy_volume": [600.0],
                "taker_buy_quote_volume": [60300.0],
                "symbol": ["BTCUSDT"],
                "interval": ["1h"],
                "extra_col": ["x"],
            }
        )
        with pytest.raises((Exception,)):
            BronzeKlinesSchema.validate(df, lazy=False)

    def test_negative_volume_rejected(self):
        df = pl.DataFrame(
            {
                "open_time": [1700000000000],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [-1.0],
                "close_time": [1700003600000],
                "quote_volume": [100500.0],
                "count": [500],
                "taker_buy_volume": [600.0],
                "taker_buy_quote_volume": [60300.0],
                "symbol": ["BTCUSDT"],
                "interval": ["1h"],
            }
        )
        with pytest.raises((Exception,)):
            BronzeKlinesSchema.validate(df, lazy=False)

    def test_high_gte_low_check(self):
        df = pl.DataFrame(
            {
                "open_time": [1700000000000],
                "open": [100.0],
                "high": [98.0],
                "low": [101.0],
                "close": [99.0],
                "volume": [1000.0],
                "close_time": [1700003600000],
                "quote_volume": [100500.0],
                "count": [500],
                "taker_buy_volume": [600.0],
                "taker_buy_quote_volume": [60300.0],
                "symbol": ["BTCUSDT"],
                "interval": ["1h"],
            }
        )
        with pytest.raises(ValueError, match="high.*low"):
            validate_bronze_klines(df)

    def test_cross_column_check_passes(self):
        """Verify valid cross-column constraints pass."""
        df = pl.DataFrame(
            {
                "open_time": [1700000000000],
                "open": [100.0],
                "high": [105.0],
                "low": [95.0],
                "close": [102.0],
                "volume": [1000.0],
                "close_time": [1700003600000],
                "quote_volume": [100500.0],
                "count": [500],
                "taker_buy_volume": [600.0],
                "taker_buy_quote_volume": [60300.0],
                "symbol": ["BTCUSDT"],
                "interval": ["1h"],
            }
        )
        result = check_high_gte_low(df, "high", "low")
        assert result is not None


class TestSilverKlinesSchema:
    """Tests for the Silver klines Pandera schema."""

    @pytest.fixture
    def silver_df(self):
        return pl.DataFrame(
            {
                "ts_event": [1700000000000000],
                "ts_recv": [1700003600000000],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000.0],
                "quote_volume": [100500.0],
                "trade_count": [500],
                "taker_buy_volume": [600.0],
                "taker_buy_quote_volume": [60300.0],
                "source": ["dlt_api"],
                "exchange": ["binance-spot"],
                "trade_type": ["spot"],
                "symbol": ["BTCUSDT"],
                "interval": ["1h"],
                "data_type": ["klines"],
                "ingested_at": [1700003600000000],
                "ts_date": ["2023-11-14"],
            }
        )

    def test_valid_silver_passes(self, silver_df):
        result = SilverKlinesSchema.validate(silver_df)
        assert result is not None

    def test_high_gte_low(self, silver_df):
        bad = silver_df.with_columns(
            [
                pl.lit(90.0).alias("high"),
                pl.lit(95.0).alias("low"),
            ]
        )
        with pytest.raises(ValueError, match="high.*low"):
            validate_silver_klines(bad)
