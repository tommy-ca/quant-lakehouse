"""dlt sources for binance-datatool — backward-compat re-exports.

All canonical sources now live in ``binance_datatool.dlt``.
Import from there for new code.
"""

from binance_datatool.dlt.destinations import build_pipeline, run_source
from binance_datatool.dlt.resources.archive_index import (
    archive_files_resource,
    build_archive_index_source,
)
from binance_datatool.dlt.resources.binance_agg_trades import agg_trades_resource
from binance_datatool.dlt.resources.binance_archive import archive_data_resource
from binance_datatool.dlt.resources.binance_funding import funding_rate_resource
from binance_datatool.dlt.resources.binance_klines import klines_resource
from binance_datatool.dlt.resources.binance_metadata import (
    build_metadata_source,
    symbols_resource,
    venues_resource,
)
from binance_datatool.dlt.resources.binance_ws import ws_klines_resource
from binance_datatool.dlt.sources import build_binance_source, build_rest_source, build_ws_source

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
    "venues_resource",
    "build_metadata_source",
    "archive_files_resource",
    "build_archive_index_source",
    "build_pipeline",
    "run_source",
]
