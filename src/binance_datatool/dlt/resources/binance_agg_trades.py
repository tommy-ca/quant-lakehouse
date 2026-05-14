"""dlt resource for Binance REST API aggTrades.

Canonical location. Previously at ``dlt_sources.binance_rest``.
"""

from __future__ import annotations

import asyncio
from typing import Any

import dlt  # noqa: TC002 — our dlt package shadows the module name

from binance_datatool.common.enums import TradeType
from binance_datatool.dlt.models import RawAggTradeModel
from binance_datatool.dlt.resources._client import client_for


@dlt.resource(
    name="agg_trades",
    write_disposition="merge",
    primary_key=("symbol", "agg_trade_id"),
    columns=RawAggTradeModel,
    schema_contract={"columns": "evolve", "data_type": "evolve"},
)
def agg_trades_resource(
    symbol: str,
    trade_type: TradeType = TradeType.spot,
    start_time: int | None = None,
    end_time: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch aggTrades from Binance REST API (bronze raw).

    Returns all fields as strings — no type casting.
    Type casting happens in Silver via Polars transforms.

    Args:
        symbol: Trading pair.
        trade_type: Market type.
        start_time: Epoch ms start.
        end_time: Epoch ms end.

    Returns:
        List of aggTrade dicts (all values as strings).
    """
    client = client_for(trade_type)
    raw_data = asyncio.run(
        client.fetch_agg_trades(
            symbol=symbol,
            since=start_time or 0,
            until=end_time,
        )
    )
    return [
        {
            "agg_trade_id": str(t.a),
            "price": str(t.p),
            "quantity": str(t.q),
            "first_trade_id": str(getattr(t, "f", "")),
            "last_trade_id": str(getattr(t, "l", "")),
            "transact_time": str(t.T),
            "is_buyer_maker": str(t.m),
            "symbol": symbol,
        }
        for t in raw_data
    ]
