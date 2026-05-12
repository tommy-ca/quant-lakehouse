"""Bronze → Silver transform for fundingRate using Polars."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

import polars as pl


def _exchange_for(trade_type: str) -> str:
    mapping = {"um": "binance-perps-um", "cm": "binance-perps-cm"}
    return mapping.get(trade_type, "binance-perps-um")


def bronze_funding_rate_to_silver(
    df: pl.DataFrame,
    *,
    symbol: str = "",
    trade_type: Literal["um", "cm"] = "um",
    source: Literal["dlt_api", "archive"] = "dlt_api",
) -> pl.DataFrame:
    """Transform bronze fundingRate DataFrame to Silver schema.

    Input columns (from ``funding_rate_resource``):
    ``symbol, funding_time, funding_rate``

    Args:
        df: Bronze fundingRate DataFrame.
        symbol: Trading pair.
        trade_type: Market type (``"um"`` or ``"cm"``).
        source: Source label.

    Returns:
        Silver-normalized DataFrame matching the DuckLake fundingRate schema.
    """
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
    exchange = _exchange_for(trade_type)

    return df.with_columns(
        [
            pl.col("funding_time").cast(pl.Int64).alias("ts_event"),
            pl.lit(now_us, dtype=pl.Int64).alias("ts_recv"),
            pl.col("funding_rate").cast(pl.Float64),
            pl.col("mark_price").cast(pl.Float64),
            pl.col("funding_time").cast(pl.Int64).alias("funding_timestamp"),
            pl.lit(source, dtype=pl.Utf8).alias("source"),
            pl.lit(exchange, dtype=pl.Utf8).alias("exchange"),
            pl.lit(trade_type, dtype=pl.Utf8).alias("trade_type"),
            pl.lit(symbol, dtype=pl.Utf8).alias("symbol"),
            pl.lit("fundingRate", dtype=pl.Utf8).alias("data_type"),
            pl.lit(now_us, dtype=pl.Int64).alias("ingested_at"),
            (pl.col("funding_time").cast(pl.Int64) // 86_400_000)
            .cast(pl.Int32)
            .cast(pl.Date)
            .alias("ts_date"),
        ]
    ).select(
        [
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
        ]
    )
