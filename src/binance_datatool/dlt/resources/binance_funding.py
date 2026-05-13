"""dlt resource for Binance REST API funding rate.

Canonical location. Previously at ``dlt_sources.binance_rest``.
"""

from __future__ import annotations

import asyncio
from typing import Any

import dlt  # noqa: TC002 — our dlt package shadows the module name

from binance_datatool.common.enums import TradeType
from binance_datatool.dlt.models import RawFundingRateModel


def _client_for(trade_type: TradeType):
    if trade_type == TradeType.spot:
        from binance_datatool.exchange.binance_rest import BinanceSpotRestClient

        return BinanceSpotRestClient()
    if trade_type == TradeType.um:
        from binance_datatool.exchange.binance_rest import BinanceUmRestClient

        return BinanceUmRestClient()
    from binance_datatool.exchange.binance_rest import BinanceCmRestClient

    return BinanceCmRestClient()


@dlt.resource(
    name="funding_rate",
    write_disposition="merge",
    primary_key=("symbol", "funding_time"),
    columns=RawFundingRateModel,
    schema_contract={"columns": "evolve", "data_type": "evolve"},
)
def funding_rate_resource(
    symbol: str,
    trade_type: TradeType = TradeType.um,
    start_time: int | None = None,
    end_time: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch fundingRate from Binance REST API (um/cm only) — bronze raw.

    Returns all fields as strings. ``mark_price`` is preserved.

    Args:
        symbol: Trading pair.
        trade_type: Market type (``"um"`` or ``"cm"``).
        start_time: Epoch ms start.
        end_time: Epoch ms end.

    Returns:
        List of funding rate dicts (all values as strings).
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
            "funding_time": str(t.funding_time),
            "funding_rate": str(t.funding_rate),
            "mark_price": str(getattr(t, "mark_price", "")),
        }
        for t in raw_data
    ]
