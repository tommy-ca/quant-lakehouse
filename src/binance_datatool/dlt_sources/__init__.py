"""dlt (data load tool) sources for binance-datatool.

Provides custom ``@dlt.resource`` generators that wrap existing exchange
clients. Each resource handles one data type with appropriate write
disposition (merge for REST, append for WS) and schema inference.

Source layers:
- ``binance`` — REST API klines (live gap-fill)
- ``binance_archive`` — S3 archive ZIP CSV (historical batch)
- ``binance_rest`` — REST API aggTrades/fundingRate (live gap-fill)
- ``binance_ws`` — WebSocket streaming klines (real-time)
- ``binance_metadata`` — Symbol/venue metadata from archive + API
"""

from binance_datatool.dlt_sources.binance import build_binance_source, klines_resource
from binance_datatool.dlt_sources.binance_archive import (
    archive_klines_resource,
    build_archive_source,
)
from binance_datatool.dlt_sources.binance_metadata import build_metadata_source, symbols_resource
from binance_datatool.dlt_sources.binance_rest import (
    agg_trades_resource,
    build_rest_source,
    funding_rate_resource,
)
from binance_datatool.dlt_sources.binance_ws import build_ws_source, ws_klines_resource

__all__ = [
    "klines_resource",
    "build_binance_source",
    "archive_klines_resource",
    "build_archive_source",
    "agg_trades_resource",
    "funding_rate_resource",
    "build_rest_source",
    "ws_klines_resource",
    "build_ws_source",
    "symbols_resource",
    "build_metadata_source",
]
