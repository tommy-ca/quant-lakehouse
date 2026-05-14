"""Bronze → Silver transform for klines using Polars."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

import polars as pl

from binance_datatool.common.enums import exchange_for
from binance_datatool.validation.schemas import validate_silver_klines


def bronze_klines_to_silver(
    df: pl.DataFrame,
    *,
    symbol: str = "",
    interval: str = "1h",
    trade_type: Literal["spot", "um", "cm"] = "spot",
    source: Literal["dlt_api", "archive", "ws_stream", "api_filled"] = "dlt_api",
    validate: bool = True,
) -> pl.DataFrame:
    """Transform bronze klines DataFrame to Silver schema.

    The input DataFrame must have the columns produced by the dlt klines
    resource (or the existing archive CSV reader):

    ``open_time, open, high, low, close, volume, close_time, quote_volume,
    count, taker_buy_volume, taker_buy_quote_volume``

    Args:
        df: Bronze klines DataFrame.
        symbol: Trading pair.
        interval: Kline interval.
        trade_type: ``"spot"``, ``"um"``, ``"cm"``.
        source: Source label.
        validate: When True (default), validates the output DataFrame
            against ``SilverKlinesSchema``.

    Returns:
        Silver-normalized DataFrame with columns matching the DuckLake silver
        schema.
    """
    exchange = exchange_for(trade_type)
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)

    # Detect timestamp unit: μs (16+ digits) vs ms (13 digits)
    open_time_col = pl.col("open_time").cast(pl.Int64)
    is_us = open_time_col >= 1_000_000_000_000_000

    result = df.with_columns(
        [
            pl.when(is_us).then(open_time_col).otherwise(open_time_col * 1000).alias("ts_event"),
            pl.lit(now_us, dtype=pl.Int64).alias("ts_recv"),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
            pl.col("quote_volume").cast(pl.Float64),
            pl.col("count").cast(pl.Int64).alias("trade_count"),
            pl.col("taker_buy_volume").cast(pl.Float64),
            pl.col("taker_buy_quote_volume").cast(pl.Float64),
            pl.lit(source, dtype=pl.Utf8).alias("source"),
            pl.lit(exchange, dtype=pl.Utf8).alias("exchange"),
            pl.lit(trade_type, dtype=pl.Utf8).alias("trade_type"),
            pl.lit(symbol, dtype=pl.Utf8).alias("symbol"),
            pl.lit(interval, dtype=pl.Utf8).alias("interval"),
            pl.lit("klines", dtype=pl.Utf8).alias("data_type"),
            pl.lit(now_us, dtype=pl.Int64).alias("ingested_at"),
            pl.when(is_us)
            .then((open_time_col // 86_400_000_000).cast(pl.Int32).cast(pl.Date))
            .otherwise((open_time_col // 86_400_000).cast(pl.Int32).cast(pl.Date))
            .alias("ts_date"),
        ]
    ).select(
        [
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
        ]
    )

    if validate:
        validate_silver_klines(result)

    return result
