"""Bronze → Silver transform for klines using Polars."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl


def bronze_klines_to_silver(
    df: pl.DataFrame,
    *,
    symbol: str = "",
    interval: str = "1h",
    trade_type: str = "spot",
    source: str = "dlt_api",
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
        source: Source label (``"dlt_api"``, ``"archive"``, ``"ws_stream"``).

    Returns:
        Silver-normalized DataFrame with columns matching the DuckLake silver
        schema.
    """
    exchange = _exchange_for(trade_type)
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)

    return df.with_columns(
        [
            (pl.col("open_time").cast(pl.Int64) * 1000).alias("ts_event"),
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
            (pl.col("open_time").cast(pl.Int64) // 86_400_000)
            .cast(pl.Int32)
            .cast(pl.Date)
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


def _exchange_for(trade_type: str) -> str:
    """Map trade_type to DuckLake exchange path component."""
    mapping = {
        "spot": "binance-spot",
        "um": "binance-perps-um",
        "cm": "binance-perps-cm",
    }
    return mapping.get(trade_type, "binance-spot")
