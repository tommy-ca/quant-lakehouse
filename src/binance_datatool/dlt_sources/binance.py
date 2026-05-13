"""Backward-compatible re-exports — resources moved to ``dlt.resources.binance_klines``
and ``dlt.sources``.
"""

from binance_datatool.dlt.resources.binance_klines import klines_resource  # noqa: F401
from binance_datatool.dlt.sources import build_binance_source  # noqa: F401

# Backward compat: BinanceSpotRestClient used by tests at this import path
from binance_datatool.exchange.binance_rest import BinanceSpotRestClient  # noqa: F401
