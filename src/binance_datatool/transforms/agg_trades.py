"""Bronze → Silver transform for aggTrades using Polars."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

import polars as pl

from binance_datatool.common.enums import exchange_for


def bronze_agg_trades_to_silver(
    df: pl.DataFrame,
    *,
    symbol: str = "",
    trade_type: Literal["spot", "um", "cm"] = "spot",
    source: Literal["dlt_api", "archive", "ws_stream"] = "dlt_api",
) -> pl.DataFrame:
    """Transform bronze aggTrades DataFrame to Silver schema.

    Handles both VARCHAR bronze input (strings) and typed input.
    Includes ``first_trade_id`` and ``last_trade_id`` (previously dropped).

    Input columns: ``agg_trade_id, price, quantity, transact_time,
    is_buyer_maker, symbol``

    Args:
        df: Bronze aggTrades DataFrame.
        symbol: Trading pair.
        trade_type: Market type.
        source: Source label.

    Returns:
        Silver-normalized DataFrame matching the DuckLake aggTrades schema.
    """
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
    exchange = exchange_for(trade_type)

    return df.with_columns(
        [
            pl.col("transact_time").cast(pl.Int64).alias("ts_event"),
            pl.lit(now_us, dtype=pl.Int64).alias("ts_recv"),
            pl.col("price").cast(pl.Float64),
            pl.col("quantity").cast(pl.Float64).alias("size"),
            pl.when(pl.col("is_buyer_maker").cast(pl.Utf8).str.to_lowercase() == "false")
            .then(pl.lit("buy"))
            .otherwise(pl.lit("sell"))
            .alias("side"),
            pl.col("agg_trade_id").cast(pl.Int64).alias("trade_id"),
            pl.when(pl.col("is_buyer_maker").cast(pl.Utf8).str.to_lowercase() == "true")
            .then(pl.lit(1, pl.Int64))
            .otherwise(pl.lit(0, pl.Int64))
            .alias("is_buyer_maker"),
            pl.col("agg_trade_id").cast(pl.Int64),
            pl.col("first_trade_id")
            .cast(pl.Utf8)
            .str.replace(r"^\s*$", "0")
            .cast(pl.Int64)
            .alias("first_trade_id"),
            pl.col("last_trade_id")
            .cast(pl.Utf8)
            .str.replace(r"^\s*$", "0")
            .cast(pl.Int64)
            .alias("last_trade_id"),
            pl.lit("agg", dtype=pl.Utf8).alias("rtype"),
            pl.lit(source, dtype=pl.Utf8).alias("source"),
            pl.lit(exchange, dtype=pl.Utf8).alias("exchange"),
            pl.lit(trade_type, dtype=pl.Utf8).alias("trade_type"),
            pl.lit(symbol, dtype=pl.Utf8).alias("symbol"),
            pl.lit("aggTrades", dtype=pl.Utf8).alias("data_type"),
            pl.lit(now_us, dtype=pl.Int64).alias("ingested_at"),
            (pl.col("transact_time").cast(pl.Int64) // 86_400_000)
            .cast(pl.Int32)
            .cast(pl.Date)
            .alias("ts_date"),
        ]
    ).select(
        [
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
        ]
    )
