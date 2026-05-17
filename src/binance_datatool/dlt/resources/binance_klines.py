"""dlt resource for Binance REST API klines.

This is the canonical location. Previously at ``dlt_sources.binance``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import dlt  # noqa: TC002 — our dlt package shadows the module name

from binance_datatool.common.async_utils import sync_run
from binance_datatool.common.enums import TradeType
from binance_datatool.dlt.models import RawKlineModel
from binance_datatool.dlt.resources._client import client_for

if TYPE_CHECKING:
    from binance_datatool.common.types import KlineData


@dlt.resource(
    name="klines",
    write_disposition="merge",
    primary_key=("symbol", "interval", "open_time"),
    columns=RawKlineModel,
    schema_contract={"columns": "evolve", "data_type": "evolve"},
)
def klines_resource(
    symbol: str,
    interval: str = "1h",
    start_time: int | None = None,
    end_time: int | None = None,
    limit: int = 1000,
    trade_type: TradeType = TradeType.spot,
) -> list[dict[str, Any]]:
    """Fetch klines from Binance REST API.

    Args:
        symbol: Trading pair (e.g. ``"BTCUSDT"``).
        interval: Kline interval (e.g. ``"1h"``, ``"1d"``).
        start_time: Epoch ms start time.
        end_time: Epoch ms end time.
        limit: Max rows per API call (max 1000).
        trade_type: Market type (spot, um, cm).

    Returns:
        List of kline dicts with fields matching RawKlineModel schema.
    """
    client = client_for(trade_type)
    raw_data: list[KlineData] = sync_run(
        client.fetch_ohlcv(
            symbol=symbol,
            interval=interval,
            since=start_time or 0,
            until=end_time,
            limit=limit,
        )
    )

    return [
        {
            "open_time": str(k.open_time),
            "open": k.open,
            "high": k.high,
            "low": k.low,
            "close": k.close,
            "volume": k.volume,
            "close_time": str(k.close_time),
            "quote_volume": k.quote_volume if k.quote_volume else None,
            "count": str(k.num_trades),
            "taker_buy_volume": k.taker_buy_volume if k.taker_buy_volume else None,
            "taker_buy_quote_volume": k.taker_buy_quote_volume
            if k.taker_buy_quote_volume
            else None,
            "symbol": symbol,
            "interval": interval,
        }
        for k in raw_data
    ]
