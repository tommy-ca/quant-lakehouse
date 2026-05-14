"""Importable dlt extraction functions (thin wrappers for Prefect @task).

These functions contain the actual business logic for dlt extraction.
Prefect ``@task`` decorators in ``prefect_flows.py`` delegate to these.
They accept explicit dependency injection for testability.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from binance_datatool.archive.client import ArchiveClient
from binance_datatool.common.enums import DataFrequency, DataType, TradeType
from binance_datatool.dlt.destinations import run_source as _run_dlt
from binance_datatool.dlt.resources.binance_archive import archive_data_resource
from binance_datatool.dlt.resources.binance_metadata import build_metadata_source
from binance_datatool.dlt.sources import build_binance_source
from binance_datatool.workflow.archive_cache import ArchiveFileCache
from binance_datatool.workflow.gap_detection import detect_bronze_gaps as _detect_gaps

_DEFAULT_ARCHIVE_HOME = None  # Set by caller


def _resolve_db(catalog_path: str | None, _archive_home: Path | None = None) -> str:
    if catalog_path:
        return catalog_path
    home = _archive_home or Path.home() / ".binance-datatool" / "archive"
    return str(home.parent / "lake" / "catalog.duckdb")


def extract_klines(
    symbol: str,
    interval: str = "1h",
    trade_type: str = "spot",
    catalog_path: str | None = None,
) -> dict:
    """Run dlt pipeline to ingest Binance klines for one symbol."""
    tt = TradeType(trade_type)
    source = build_binance_source(symbols=[symbol], interval=interval, trade_type=tt)
    return _run_dlt(source, source_name=f"binance_{trade_type}", catalog_path=catalog_path)


def extract_agg_trades(
    symbol: str,
    trade_type: str = "spot",
    catalog_path: str | None = None,
) -> dict:
    """Run dlt pipeline to ingest Binance aggTrades for one symbol."""
    from binance_datatool.dlt.sources import build_rest_source

    tt = TradeType(trade_type)
    source = build_rest_source(symbols=[symbol], data_type="aggTrades", trade_type=tt)
    return _run_dlt(source, source_name=f"agg_trades_{trade_type}", catalog_path=catalog_path)


def extract_funding_rate(
    symbol: str,
    trade_type: str = "um",
    catalog_path: str | None = None,
) -> dict:
    """Run dlt pipeline to ingest Binance fundingRate for one symbol."""
    from binance_datatool.dlt.sources import build_rest_source

    tt = TradeType(trade_type)
    source = build_rest_source(symbols=[symbol], data_type="fundingRate", trade_type=tt)
    return _run_dlt(source, source_name=f"funding_rate_{trade_type}", catalog_path=catalog_path)


def extract_archive(
    symbol: str,
    interval: str | None = "1h",
    trade_type: str = "spot",
    data_type: str = "klines",
    lookback_days: int | None = 7,
    catalog_path: str | None = None,
    archive_home: Path | None = None,
) -> dict:
    """Run dlt pipeline to ingest Binance archive data for one symbol.

    Pure EL flow:
    1. Resolve S3 file keys (cache or live listing)
    2. Pass keys to dlt for download + parse
    """
    tt = TradeType(trade_type)
    freq = DataFrequency.monthly if data_type == "fundingRate" else DataFrequency.daily
    dt_enum = DataType(data_type)
    iv = interval if data_type == "klines" else None

    db_path = _resolve_db(catalog_path, archive_home)
    cache = ArchiveFileCache(db_path)
    cache.ensure_table()
    freq_str = freq.value
    if cache.is_fresh(symbol, data_type, iv, trade_type, freq_str):
        files = cache.list_cached(symbol, data_type, iv, trade_type, freq_str)
    else:
        client = ArchiveClient()
        raw_files = asyncio.run(client.list_symbol_files(tt, freq, dt_enum, symbol, interval=iv))
        files = [{"key": f.key, "last_modified": f.last_modified} for f in raw_files]
        cache.refresh(symbol, data_type, iv, trade_type, freq_str)

    if lookback_days is not None and files:
        from datetime import UTC, datetime, timedelta

        cutoff = datetime.now(UTC) - timedelta(days=int(lookback_days))
        files = [
            f
            for f in files
            if (f.get("last_modified") and f["last_modified"].replace(tzinfo=UTC) > cutoff)
        ]

    s3_keys = [f["key"] for f in files]
    if not s3_keys:
        return {"tables_loaded": [], "files_count": 0}

    resource = archive_data_resource(symbol, s3_keys, interval=iv, data_type=data_type)
    return _run_dlt(
        resource,
        source_name=f"archive_{trade_type}_{data_type}",
        catalog_path=catalog_path,
    )


def extract_metadata(
    trade_types: list[str] | None = None,
    catalog_path: str | None = None,
) -> dict:
    """Run dlt pipeline to discover symbols from the archive."""
    if trade_types is None:
        trade_types = ["spot", "um", "cm"]
    types = [TradeType(tt) for tt in trade_types]
    source = build_metadata_source(trade_types=types)
    return _run_dlt(
        source, source_name="metadata", catalog_path=catalog_path, dataset_name="metadata"
    )


def detect_gaps(
    symbol: str,
    data_type: str = "klines",
    lookback_days: int = 30,
    catalog_path: str | None = None,
    archive_home: Path | None = None,
) -> list[tuple[str, int, int]]:
    """Detect date gaps in DuckDB bronze table for a symbol."""
    db_path = _resolve_db(catalog_path, archive_home)
    table_map = {
        "klines": "bronze.klines",
        "aggTrades": "bronze.agg_trades",
        "fundingRate": "bronze.funding_rate",
    }
    table = table_map.get(data_type, "bronze.klines")
    return _detect_gaps(db_path, table, [symbol], lookback_days)
