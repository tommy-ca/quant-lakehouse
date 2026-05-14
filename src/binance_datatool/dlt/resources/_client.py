"""Shared client factory for dlt REST API resources.

All REST resources (klines, aggTrades, fundingRate) use the same
trade-type dispatch logic. Centralised here to avoid DRY violations.
"""

from __future__ import annotations

from binance_datatool.common.enums import TradeType


def client_for(trade_type: TradeType):
    """Return the correct REST client for a given trade type.

    Uses lazy imports so tests can mock ``exchange.binance_rest``
    independently for each resource.
    """
    if trade_type == TradeType.spot:
        from binance_datatool.exchange.binance_rest import BinanceSpotRestClient

        return BinanceSpotRestClient()
    if trade_type == TradeType.um:
        from binance_datatool.exchange.binance_rest import BinanceUmRestClient

        return BinanceUmRestClient()
    from binance_datatool.exchange.binance_rest import BinanceCmRestClient

    return BinanceCmRestClient()
