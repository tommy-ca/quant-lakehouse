"""dlt resource for Binance WebSocket stream — real-time klines.

Canonical location. Previously at ``dlt_sources.binance_ws``.
"""

from __future__ import annotations

from typing import Any

import dlt  # noqa: TC002 — our dlt package shadows the module name

from binance_datatool.common.async_utils import sync_run
from binance_datatool.common.enums import TradeType


@dlt.resource(
    name="ws_klines",
    write_disposition="append",
    schema_contract={"columns": "freeze", "data_type": "freeze"},
    columns={
        "open_time": {"data_type": "bigint", "nullable": False},
        "open": {"data_type": "double", "nullable": False},
        "high": {"data_type": "double", "nullable": False},
        "low": {"data_type": "double", "nullable": False},
        "close": {"data_type": "double", "nullable": False},
        "volume": {"data_type": "double", "nullable": False},
        "close_time": {"data_type": "bigint", "nullable": False},
        "quote_volume": {"data_type": "double", "nullable": False},
        "trade_count": {"data_type": "bigint", "nullable": False},
        "taker_buy_volume": {"data_type": "double", "nullable": False},
        "taker_buy_quote_volume": {"data_type": "double", "nullable": False},
        "symbol": {"data_type": "text", "nullable": False},
        "interval": {"data_type": "text", "nullable": False},
    },
)
def ws_klines_resource(
    symbol: str,
    interval: str = "1h",
    trade_type: TradeType = TradeType.spot,
    max_items: int = 100,
) -> list[dict[str, Any]]:
    """Stream klines from Binance WebSocket.

    Collects up to ``max_items`` klines, disconnects, returns as list.

    Args:
        symbol: Trading pair.
        interval: Kline interval.
        trade_type: Market type.
        max_items: Max klines to collect.

    Returns:
        List of kline dicts.
    """
    if trade_type == TradeType.spot:
        from binance_datatool.exchange.binance_ws import BinanceSpotWsClient

        ws_client = BinanceSpotWsClient()
    elif trade_type == TradeType.um:
        from binance_datatool.exchange.binance_ws import BinanceUmWsClient

        ws_client = BinanceUmWsClient()
    else:
        from binance_datatool.exchange.binance_ws import BinanceCmWsClient

        ws_client = BinanceCmWsClient()

    async def _collect() -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        count = 0
        async for kline in ws_client.stream_ohlcv(symbol, interval):
            results.append(
                {
                    "open_time": kline.open_time,
                    "open": float(kline.open),
                    "high": float(kline.high),
                    "low": float(kline.low),
                    "close": float(kline.close),
                    "volume": float(kline.volume),
                    "close_time": kline.close_time,
                    "quote_volume": float(kline.quote_volume),
                    "trade_count": kline.num_trades,
                    "taker_buy_volume": float(kline.taker_buy_volume),
                    "taker_buy_quote_volume": float(kline.taker_buy_quote_volume),
                    "symbol": symbol,
                    "interval": interval,
                }
            )
            count += 1
            if count >= max_items:
                break
        await ws_client.close()
        return results

    return sync_run(_collect())
