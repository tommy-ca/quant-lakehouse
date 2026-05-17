"""Tests for Pandera schemas and Pydantic models (validation module).

Test coverage:
- Pydantic models: KlineModel (cross-field high>=low, non-negative volume),
  AggTradeModel (positive price, non-negative quantity), FundingRateModel
- Pandera schemas: BronzeKlinesSchema, SilverKlinesSchema,
  AggTradesSilverSchema, FundingRateSilverSchema
- Cross-column checks: high >= low for all kline-class schemas
- ts_date type: all silver schemas use pl.Date (not object)
- Alignment: Pydantic and Pandera enforce the same constraints
"""

from __future__ import annotations

from datetime import date

import pandera
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
        with pytest.raises(pandera.errors.SchemaError):
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
        with pytest.raises(pandera.errors.SchemaError):
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
                "ts_date": [date(2023, 11, 14)],
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


class TestAggTradesSilverSchema:
    """Tests for the silver aggTrades Pandera schema."""

    def test_valid_df_passes(self):
        df = pl.DataFrame(
            {
                "ts_event": [1700000000000000],
                "ts_recv": [1700000001000000],
                "price": [50000.0],
                "size": [0.5],
                "side": ["buy"],
                "trade_id": [1],
                "is_buyer_maker": [1],
                "agg_trade_id": [1],
                "first_trade_id": [0],
                "last_trade_id": [0],
                "rtype": ["agg"],
                "source": ["dlt_api"],
                "exchange": ["binance-spot"],
                "trade_type": ["spot"],
                "symbol": ["BTCUSDT"],
                "data_type": ["aggTrades"],
                "ingested_at": [1700000001000000],
                "ts_date": [date(2023, 11, 14)],
            }
        )
        from binance_datatool.validation.schemas import AggTradesSilverSchema

        result = AggTradesSilverSchema.validate(df)
        assert result is not None


class TestFundingRateSilverSchema:
    """Tests for the silver fundingRate Pandera schema."""

    def test_valid_df_passes(self):
        df = pl.DataFrame(
            {
                "ts_event": [1700000000000000],
                "ts_recv": [1700000001000000],
                "funding_rate": [0.0001],
                "mark_price": [50000.0],
                "funding_timestamp": [1700000000000000],
                "source": ["dlt_api"],
                "exchange": ["binance-perps-um"],
                "trade_type": ["um"],
                "symbol": ["BTCUSDT"],
                "data_type": ["fundingRate"],
                "ingested_at": [1700000001000000],
                "ts_date": [date(2023, 11, 14)],
            }
        )
        from binance_datatool.validation.schemas import FundingRateSilverSchema

        result = FundingRateSilverSchema.validate(df)
        assert result is not None


class TestValidationConsistency:
    """Verify Pydantic and Pandera enforce the same constraints on klines.

    Both layers should agree on: high >= low, non-negative volume, positive
    open_time. This test documents and enforces the consistency contract.
    """

    def test_kline_high_lt_low_rejected_by_both(self):
        with pytest.raises(ValidationError):
            KlineModel(
                open_time=1700000000000,
                open=100.0,
                high=98.0,
                low=101.0,
                close=100.0,
                volume=1000.0,
                close_time=1700003600000,
                symbol="BTCUSDT",
                interval="1h",
            )

        bad_df = pl.DataFrame(
            {
                "open_time": [1700000000000],
                "open": [100.0],
                "high": [98.0],
                "low": [101.0],
                "close": [100.0],
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
            validate_bronze_klines(bad_df)

    def test_kline_negative_volume_rejected_by_both(self):
        with pytest.raises(ValidationError):
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

        bad_df = pl.DataFrame(
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
        with pytest.raises(pandera.errors.SchemaError):
            BronzeKlinesSchema.validate(bad_df)
