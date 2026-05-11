"""dlt resources for Binance REST API — aggTrades and fundingRate.

Extends the klines resource pattern to support aggTrades and fundingRate data
types. Each resource wraps the SDK REST client with write_disposition="merge"
for idempotent loading.
"""

from __future__ import annotations

import asyncio
from typing import Any

import dlt

from binance_datatool.common.enums import TradeType
from binance_datatool.exchange.binance_rest import BinanceSpotRestClient


def _client_for(trade_type: TradeType):
    """Return the appropriate REST client for a trade type."""
    if trade_type == TradeType.spot:
        return BinanceSpotRestClient()
    if trade_type == TradeType.um:
        from binance_datatool.exchange.binance_rest import BinanceUmRestClient

        return BinanceUmRestClient()
    from binance_datatool.exchange.binance_rest import BinanceCmRestClient

    return BinanceCmRestClient()


@dlt.resource(
    name="agg_trades",
    write_disposition="merge",
    primary_key=("symbol", "agg_trade_id"),
    columns={
        "agg_trade_id": {"data_type": "bigint", "nullable": False},
        "price": {"data_type": "double", "nullable": False},
        "quantity": {"data_type": "double", "nullable": False},
        "transact_time": {"data_type": "bigint", "nullable": False},
        "is_buyer_maker": {"data_type": "bool", "nullable": False},
        "symbol": {"data_type": "text", "nullable": False},
    },
)
def agg_trades_resource(
    symbol: str,
    trade_type: TradeType = TradeType.spot,
    start_time: int | None = None,
    end_time: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch aggTrades from Binance REST API.

    Args:
        symbol: Trading pair.
        trade_type: Market type.
        start_time: Epoch ms start (overrides default 0).
        end_time: Epoch ms end.

    Returns:
        List of aggTrade dicts.
    """
    client = _client_for(trade_type)
    raw_data = asyncio.run(
        client.fetch_agg_trades(
            symbol=symbol,
            since=start_time or 0,
            until=end_time,
        )
    )
    return [
        {
            "agg_trade_id": int(t["a"]),
            "price": float(t["p"]),
            "quantity": float(t["q"]),
            "transact_time": int(t["T"]),
            "is_buyer_maker": bool(t["m"]),
            "symbol": symbol,
        }
        for t in raw_data
    ]


@dlt.resource(
    name="funding_rate",
    write_disposition="merge",
    primary_key=("symbol", "funding_time"),
    columns={
        "symbol": {"data_type": "text", "nullable": False},
        "funding_time": {"data_type": "bigint", "nullable": False},
        "funding_rate": {"data_type": "double", "nullable": False},
    },
)
def funding_rate_resource(
    symbol: str,
    trade_type: TradeType = TradeType.um,
    start_time: int | None = None,
    end_time: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch fundingRate from Binance REST API (um/cm only).

    Args:
        symbol: Trading pair.
        trade_type: Market type (``"um"`` or ``"cm"``).
        start_time: Epoch ms start.
        end_time: Epoch ms end.

    Returns:
        List of funding rate dicts.
    """
    client = _client_for(trade_type)
    raw_data = asyncio.run(
        client.fetch_funding_rate(
            symbol=symbol,
            since=start_time or 0,
            until=end_time,
        )
    )
    return [
        {
            "symbol": symbol,
            "funding_time": int(t["fundingTime"]),
            "funding_rate": float(t["fundingRate"]),
        }
        for t in raw_data
    ]


@dlt.source
def build_rest_source(
    symbols: list[str],
    data_type: str = "aggTrades",
    trade_type: TradeType = TradeType.spot,
) -> list[dlt.Resource]:
    """Build a dlt source for Binance REST API.

    Args:
        symbols: Trading symbols.
        data_type: ``"aggTrades"`` or ``"fundingRate"``.
        trade_type: Market type.

    Returns:
        List of dlt Resources (one per symbol).
    """
    if data_type == "aggTrades":
        resource = agg_trades_resource
    elif data_type == "fundingRate":
        resource = funding_rate_resource
    else:
        raise ValueError(f"Unsupported REST data_type: {data_type}")

    return [
        resource(symbol=sym, trade_type=trade_type).with_name(f"rest_{sym}_{data_type}")
        for sym in symbols
    ]
