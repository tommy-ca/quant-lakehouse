"""Pandera DataFrame schema definitions for pipeline data contracts.

Each schema enforces column types, nullability, value ranges at a
specific pipeline boundary.

Two primary schemas:
- ``BronzeKlinesSchema`` — raw klines from dlt REST/archive sources
- ``SilverKlinesSchema`` — normalized klines before DuckDB insert

Cross-column checks (e.g. ``high >= low``) are applied via helper
functions in ``validate_bronze_klines`` / ``validate_silver_klines``.
"""

import pandera.polars as pa
import polars as pl


class BronzeKlinesSchema(pa.DataFrameModel):
    """Schema for bronze klines (dlt REST/archive output).

    Validates column presence, types, nullability, and value ranges.
    """

    class Config:
        coerce = True
        strict = True

    open_time: int = pa.Field(ge=0, nullable=False)
    open: float = pa.Field(ge=0, nullable=False)
    high: float = pa.Field(ge=0, nullable=False)
    low: float = pa.Field(ge=0, nullable=False)
    close: float = pa.Field(ge=0, nullable=False)
    volume: float = pa.Field(ge=0, nullable=False)
    close_time: int = pa.Field(ge=0, nullable=False)
    quote_volume: float = pa.Field(ge=0)
    count: int = pa.Field(ge=0, nullable=False)
    taker_buy_volume: float = pa.Field(ge=0)
    taker_buy_quote_volume: float = pa.Field(ge=0)
    symbol: str = pa.Field(nullable=False)
    interval: str = pa.Field(nullable=False)


class SilverKlinesSchema(pa.DataFrameModel):
    """Schema for silver klines (Polars transform output, pre-DuckDB insert).

    Validates at the boundary between Polars transform and DuckDB sink.
    """

    class Config:
        coerce = True
        strict = True

    ts_event: int = pa.Field(ge=0, nullable=False)
    ts_recv: int = pa.Field(ge=0, nullable=False)
    open: float = pa.Field(ge=0, nullable=False)
    high: float = pa.Field(ge=0, nullable=False)
    low: float = pa.Field(ge=0, nullable=False)
    close: float = pa.Field(ge=0, nullable=False)
    volume: float = pa.Field(ge=0, nullable=False)
    quote_volume: float = pa.Field(ge=0)
    trade_count: int = pa.Field(ge=0, nullable=False)
    taker_buy_volume: float = pa.Field(ge=0)
    taker_buy_quote_volume: float = pa.Field(ge=0)
    source: str = pa.Field(nullable=False)
    exchange: str = pa.Field(nullable=False)
    trade_type: str = pa.Field(nullable=False)
    symbol: str = pa.Field(nullable=False)
    interval: str = pa.Field(nullable=False)
    data_type: str = pa.Field(nullable=False)
    ingested_at: int = pa.Field(ge=0, nullable=False)
    ts_date: pl.Date = pa.Field(nullable=False)


class AggTradesSilverSchema(pa.DataFrameModel):
    """Schema for silver aggTrades (Polars transform output, pre-DuckDB insert)."""

    class Config:
        coerce = True
        strict = True

    ts_event: int = pa.Field(ge=0, nullable=False)
    ts_recv: int = pa.Field(ge=0, nullable=False)
    price: float = pa.Field(ge=0, nullable=False)
    size: float = pa.Field(ge=0, nullable=False)
    side: str = pa.Field(nullable=False)
    trade_id: int = pa.Field(ge=0, nullable=False)
    is_buyer_maker: int = pa.Field(nullable=False)
    agg_trade_id: int = pa.Field(ge=0, nullable=False)
    first_trade_id: int = pa.Field(ge=0, nullable=False)
    last_trade_id: int = pa.Field(ge=0, nullable=False)
    rtype: str = pa.Field(nullable=False)
    source: str = pa.Field(nullable=False)
    exchange: str = pa.Field(nullable=False)
    trade_type: str = pa.Field(nullable=False)
    symbol: str = pa.Field(nullable=False)
    data_type: str = pa.Field(nullable=False)
    ingested_at: int = pa.Field(ge=0, nullable=False)
    ts_date: pl.Date = pa.Field(nullable=False)


class FundingRateSilverSchema(pa.DataFrameModel):
    """Schema for silver fundingRate (Polars transform output, pre-DuckDB insert)."""

    class Config:
        coerce = True
        strict = True

    ts_event: int = pa.Field(ge=0, nullable=False)
    ts_recv: int = pa.Field(ge=0, nullable=False)
    funding_rate: float = pa.Field(nullable=False)
    mark_price: float = pa.Field(nullable=False)
    funding_timestamp: int = pa.Field(ge=0, nullable=False)
    source: str = pa.Field(nullable=False)
    exchange: str = pa.Field(nullable=False)
    trade_type: str = pa.Field(nullable=False)
    symbol: str = pa.Field(nullable=False)
    data_type: str = pa.Field(nullable=False)
    ingested_at: int = pa.Field(ge=0, nullable=False)
    ts_date: pl.Date = pa.Field(nullable=False)


class BronzeAggTradesSchema(pa.DataFrameModel):
    """Schema for bronze aggTrades (dlt REST output, raw VARCHAR)."""

    class Config:
        coerce = True
        strict = True

    agg_trade_id: str = pa.Field(nullable=False)
    price: str = pa.Field(nullable=False)
    quantity: str = pa.Field(nullable=False)
    first_trade_id: str = pa.Field(nullable=True)
    last_trade_id: str = pa.Field(nullable=True)
    transact_time: str = pa.Field(nullable=False)
    is_buyer_maker: str = pa.Field(nullable=False)
    symbol: str = pa.Field(nullable=False)


class BronzeFundingRateSchema(pa.DataFrameModel):
    """Schema for bronze fundingRate (dlt REST output, raw VARCHAR)."""

    class Config:
        coerce = True
        strict = True

    symbol: str = pa.Field(nullable=False)
    funding_time: str = pa.Field(nullable=False)
    funding_rate: str = pa.Field(nullable=False)
    mark_price: str = pa.Field(nullable=True)


class VenuesSchema(pa.DataFrameModel):
    """Schema for venue metadata (DBN/Tardis aligned)."""

    class Config:
        coerce = True
        strict = True

    venue_id: str = pa.Field(nullable=False)
    name: str = pa.Field(nullable=False)
    publisher_id: str = pa.Field(nullable=False)
    dataset: str = pa.Field(nullable=False)
    exchange_slug: str = pa.Field(nullable=False)
    market_type: str = pa.Field(nullable=False)
    status: str = pa.Field(nullable=False)
    fetched_at: int = pa.Field(ge=0, nullable=False)


class SymbolsSchema(pa.DataFrameModel):
    """Schema for symbol metadata (DBN/Tardis aligned)."""

    class Config:
        coerce = True
        strict = True

    symbol: str = pa.Field(nullable=False)
    trade_type: str = pa.Field(nullable=False)
    data_type: str = pa.Field(nullable=False)
    base_asset: str = pa.Field(nullable=True)
    quote_asset: str = pa.Field(nullable=True)
    contract_type: str = pa.Field(nullable=True)
    instrument_class: str = pa.Field(nullable=True)
    is_leverage: bool = pa.Field(nullable=True)
    is_stable_pair: bool = pa.Field(nullable=True)
    source: str = pa.Field(nullable=False)
    fetched_at: int = pa.Field(ge=0, nullable=False)


class GoldUniverseStatsSchema(pa.DataFrameModel):
    """Schema for daily universe statistics in the Gold layer."""

    class Config:
        coerce = True
        strict = True

    symbol: str = pa.Field(nullable=False)
    venue_id: str = pa.Field(nullable=False)
    ts_date: pl.Date = pa.Field(nullable=False)
    base_asset: str = pa.Field(nullable=True)
    quote_asset: str = pa.Field(nullable=True)
    onboard_date: int = pa.Field(ge=0, nullable=True)
    quote_volume: float = pa.Field(ge=0, nullable=False)
    market_cap: float = pa.Field(ge=0, nullable=False)
    last_price: float = pa.Field(ge=0, nullable=False)


# ── Cross-column validation helpers ──────────────────────────────


def check_high_gte_low(
    df: pl.DataFrame | pl.LazyFrame,
    hi: str = "high",
    lo: str = "low",
) -> pl.DataFrame:
    """Assert ``hi`` >= ``lo`` for all rows. Raises ``ValueError`` on failure."""
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    violators = df.filter(pl.col(hi) < pl.col(lo))
    if violators.height > 0:
        raise ValueError(f"Cross-column check failed: {hi} < {lo} for {violators.height} rows")
    return df


def validate_bronze_klines(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Validate bronze klines with Pandera + cross-column checks."""
    result = BronzeKlinesSchema.validate(df, lazy=True)
    if isinstance(result, pl.LazyFrame):
        result = result.collect()
    check_high_gte_low(result, "high", "low")
    check_high_gte_low(result, "close", "low")
    check_high_gte_low(result, "high", "close")
    return result


def validate_silver_klines(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Validate silver klines with Pandera + cross-column checks."""
    result = SilverKlinesSchema.validate(df, lazy=True)
    if isinstance(result, pl.LazyFrame):
        result = result.collect()
    check_high_gte_low(result, "high", "low")
    check_high_gte_low(result, "close", "low")
    return result


def validate_bronze_agg_trades(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Validate bronze aggTrades with Pandera."""
    result = BronzeAggTradesSchema.validate(df, lazy=True)
    if isinstance(result, pl.LazyFrame):
        result = result.collect()
    return result


def validate_silver_agg_trades(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Validate silver aggTrades with Pandera."""
    result = AggTradesSilverSchema.validate(df, lazy=True)
    if isinstance(result, pl.LazyFrame):
        result = result.collect()
    return result


def validate_silver_funding_rate(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Validate silver fundingRate with Pandera."""
    result = FundingRateSilverSchema.validate(df, lazy=True)
    if isinstance(result, pl.LazyFrame):
        result = result.collect()
    return result
