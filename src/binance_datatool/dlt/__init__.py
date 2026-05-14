"""Standalone dlt package for binance-datatool.

Provides dlt resources, source builders, models, and destination
configuration that can be imported independently.

Sub-modules:
- ``models`` — Pydantic models for dlt resource schemas
- ``destinations`` — Standard destination builders (DuckDB, DuckLake)
- ``resources`` — ``@dlt.resource`` per data type
- ``sources`` — ``@dlt.source`` composing all resources
"""

from binance_datatool.dlt.destinations import build_pipeline, run_source
from binance_datatool.dlt.models import (
    AggTradeModel,
    FundingRateModel,
    KlineModel,
    RawAggTradeModel,
    RawFundingRateModel,
    RawKlineModel,
    SymbolMetaModel,
    VenueModel,
)
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
    "build_pipeline",
    "run_source",
    "KlineModel",
    "AggTradeModel",
    "FundingRateModel",
    "VenueModel",
    "SymbolMetaModel",
    "RawKlineModel",
    "RawAggTradeModel",
    "RawFundingRateModel",
    "klines_resource",
    "agg_trades_resource",
    "funding_rate_resource",
    "archive_data_resource",
    "ws_klines_resource",
    "build_binance_source",
    "build_rest_source",
    "build_ws_source",
    "venues_resource",
    "symbols_resource",
    "build_metadata_source",
    "archive_files_resource",
    "build_archive_index_source",
]
