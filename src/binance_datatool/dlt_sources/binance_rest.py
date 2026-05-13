"""Backward-compatible re-exports — resources moved to ``dlt.resources``."""

from binance_datatool.dlt.resources.binance_agg_trades import agg_trades_resource  # noqa: F401
from binance_datatool.dlt.resources.binance_funding import funding_rate_resource  # noqa: F401
from binance_datatool.dlt.sources import build_rest_source  # noqa: F401

# Backward compat: client classes used by tests at this import path
from binance_datatool.exchange.binance_rest import (  # noqa: F401
    BinanceCmRestClient,
    BinanceSpotRestClient,
    BinanceUmRestClient,
)
