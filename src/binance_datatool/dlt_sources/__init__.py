"""dlt (data load tool) sources for binance-datatool.

Provides custom ``@dlt.resource`` generators that wrap existing exchange
clients. Each resource handles one data type (klines, aggTrades, fundingRate)
with incremental loading, retry, and schema inference via dlt.
"""

from binance_datatool.dlt_sources.binance import build_binance_source, klines_resource

__all__ = [
    "klines_resource",
    "build_binance_source",
]
