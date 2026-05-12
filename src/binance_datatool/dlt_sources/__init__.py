"""dlt sources for binance-datatool.

Each module wraps an exchange/SDK client as ``@dlt.resource`` generators:

- ``binance`` — REST API klines (gap-fill)
- ``binance_archive`` — S3 archive CSV ZIPs (historical batch) — all data types
- ``binance_rest`` — REST API aggTrades/fundingRate (gap-fill)
- ``binance_ws`` — WebSocket streaming klines (real-time)
- ``binance_metadata`` — Symbol/venue metadata from archive + API
"""

from binance_datatool.dlt_sources.binance import build_binance_source, klines_resource
from binance_datatool.dlt_sources.binance_archive import archive_data_resource
from binance_datatool.dlt_sources.binance_metadata import build_metadata_source, symbols_resource
from binance_datatool.dlt_sources.binance_rest import (
    agg_trades_resource,
    build_rest_source,
    funding_rate_resource,
)
from binance_datatool.dlt_sources.binance_ws import build_ws_source, ws_klines_resource
from binance_datatool.dlt_sources.bronze_archive_index import (
    archive_files_resource,
    build_archive_index_source,
)

__all__ = [
    "klines_resource",
    "build_binance_source",
    "archive_data_resource",
    "agg_trades_resource",
    "funding_rate_resource",
    "build_rest_source",
    "ws_klines_resource",
    "build_ws_source",
    "symbols_resource",
    "build_metadata_source",
    "archive_files_resource",
    "build_archive_index_source",
]
