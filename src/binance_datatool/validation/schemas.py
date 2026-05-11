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
