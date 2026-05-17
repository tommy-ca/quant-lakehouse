"""Unified dlt source builders for Binance market data.

Composes individual dlt resources into multi-symbol sources.
Canonical location. Previously defined across ``dlt_sources.binance``,
``dlt_sources.binance_rest``, ``dlt_sources.binance_ws``, etc.
"""

from __future__ import annotations

from typing import Any

import dlt  # noqa: TC002 — our dlt package shadows the module name

from binance_datatool.common.enums import TradeType
from binance_datatool.dlt.resources.binance_agg_trades import agg_trades_resource
from binance_datatool.dlt.resources.binance_funding import funding_rate_resource
from binance_datatool.dlt.resources.binance_klines import klines_resource
from binance_datatool.dlt.resources.binance_ws import ws_klines_resource


@dlt.source
def build_binance_source(
    symbols: list[str],
    interval: str = "1h",
    trade_type: TradeType = TradeType.spot,
    data_type: str = "klines",
) -> list[dlt.Resource]:
    """Build a dlt source for multiple Binance symbols.

    All symbols write to a single table in DuckDB, distinguished
    by the ``symbol`` column.

    Args:
        symbols: Trading symbols to fetch.
        interval: Kline interval (for klines only).
        trade_type: Market type.
        data_type: ``"klines"`` (default), ``"aggTrades"``, or ``"fundingRate"``.

    Returns:
        List of dlt Resources (all writing to the same table).
    """
    if data_type == "klines":
        table = "klines"
        resource = klines_resource
        kwargs: dict[str, Any] = {"interval": interval}
    elif data_type == "aggTrades":
        table = "agg_trades"
        resource = agg_trades_resource
        kwargs = {}
    elif data_type == "fundingRate":
        table = "funding_rate"
        resource = funding_rate_resource
        kwargs = {}
    else:
        raise ValueError(f"Unsupported data_type: {data_type}")

    return [
        resource(symbol=sym, trade_type=trade_type, **kwargs)
        .with_name(table)
        .apply_hints(table_name=table)
        for sym in symbols
    ]


@dlt.source
def build_ws_source(
    symbols: list[str],
    interval: str = "1h",
    trade_type: TradeType = TradeType.spot,
    max_items: int = 100,
) -> list[dlt.Resource]:
    """Build a dlt source for Binance WebSocket streaming.

    One resource per symbol. Each streams ``max_items`` klines then stops.

    Args:
        symbols: Trading symbols.
        interval: Kline interval.
        trade_type: Market type.
        max_items: Max klines per symbol.

    Returns:
        List of dlt Resources (one per symbol).
    """
    return [
        ws_klines_resource(
            symbol=sym, interval=interval, trade_type=trade_type, max_items=max_items
        ).with_name(f"ws_{sym}_klines")
        for sym in symbols
    ]


@dlt.source
def build_rest_source(
    symbols: list[str],
    data_type: str = "aggTrades",
    trade_type: TradeType = TradeType.spot,
) -> list[dlt.Resource]:
    """Build a dlt source for Binance REST API (aggTrades or fundingRate).

    Args:
        symbols: Trading symbols.
        data_type: ``"aggTrades"`` or ``"fundingRate"``.
        trade_type: Market type.

    Returns:
        List of dlt Resources (one per symbol).
    """
    return build_binance_source(symbols, trade_type=trade_type, data_type=data_type)


__all__ = [
    "build_binance_source",
    "build_rest_source",
    "build_ws_source",
]
