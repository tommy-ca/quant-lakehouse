"""Bronze → Silver transform for aggTrades using Polars."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

import polars as pl


def _exchange_for(trade_type: str) -> str:
    mapping = {"spot": "binance-spot", "um": "binance-perps-um", "cm": "binance-perps-cm"}
    return mapping.get(trade_type, "binance-spot")


def bronze_agg_trades_to_silver(
    df: pl.DataFrame,
    *,
    symbol: str = "",
    trade_type: Literal["spot", "um", "cm"] = "spot",
    source: Literal["dlt_api", "archive", "ws_stream"] = "dlt_api",
) -> pl.DataFrame:
    """Transform bronze aggTrades DataFrame to Silver schema.

    Input columns (from ``agg_trades_resource``):
    ``agg_trade_id, price, quantity, transact_time, is_buyer_maker, symbol``

    Args:
        df: Bronze aggTrades DataFrame.
        symbol: Trading pair.
        trade_type: Market type.
        source: Source label.

    Returns:
        Silver-normalized DataFrame matching the DuckLake aggTrades schema.
    """
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
    exchange = _exchange_for(trade_type)

    return df.with_columns(
        [
            pl.col("transact_time").cast(pl.Int64).alias("ts_event"),
            pl.lit(now_us, dtype=pl.Int64).alias("ts_recv"),
            pl.col("price").cast(pl.Float64),
            pl.col("quantity").cast(pl.Float64).alias("size"),
            pl.when(~pl.col("is_buyer_maker"))
            .then(pl.lit("buy"))
            .otherwise(pl.lit("sell"))
            .alias("side"),
            pl.col("agg_trade_id").cast(pl.Int64).alias("trade_id"),
            pl.col("is_buyer_maker").cast(pl.Int64),
            pl.col("agg_trade_id").cast(pl.Int64),
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
