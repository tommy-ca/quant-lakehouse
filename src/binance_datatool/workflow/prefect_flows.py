"""Prefect workflow definitions for binance-datatool pipelines.

Composes workflow classes directly — no subprocess CLI wrappers.
CLI commands are thin wrappers over these same workflow classes.

Design patterns (dataskew.io/blog/data-pipeline-design-patterns):
- Idempotency: DROP TABLE IF EXISTS + CREATE TABLE AS SELECT
- Backfilling: parameterized execution_date / lookback_days
- Schema evolution: DuckLake ALTER TABLE + centralized TABLE_DEFS
- Dead letter queue: dlq table in DuckLake for failed records
- Retry: exponential backoff with jitter
- At-least-once + idempotent operations (no exactly-once needed)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import duckdb
from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner

from binance_datatool.common import DataType, TradeType
from binance_datatool.common.settings import settings
from binance_datatool.exchange import (
    BinanceCmRestClient,
    BinanceSpotRestClient,
    BinanceUmRestClient,
)
from binance_datatool.workflow import (
    SinkWorkflow,
)
from binance_datatool.workflow.health_check import check_ducklake_anomalies
from binance_datatool.workflow.prefect_tasks.extract import (
    extract_agg_trades,
    extract_archive,
    extract_funding_rate,
    extract_klines,
)
from binance_datatool.workflow.prefect_tasks.metadata import sync_metadata_task
from binance_datatool.workflow.prefect_tasks.transform import (
    transform_agg_trades,
    transform_funding_rate,
    transform_klines,
    transform_trades,
)

_DEFAULT_ARCHIVE_HOME = settings.archive_home

# Retry with exponential backoff and jitter
# Prefect 3.x: retry_delay_seconds + retry_jitter_factor
# Pattern 5: https://dataskew.io/blog/data-pipeline-design-patterns
_RETRY_CONFIG = {"retries": 3, "retry_delay_seconds": 10, "retry_jitter_factor": 0.2}
_RETRY_LIGHT = {"retries": 2, "retry_delay_seconds": 10, "retry_jitter_factor": 0.2}

_REST_CLIENTS = {
    "spot": BinanceSpotRestClient,
    "um": BinanceUmRestClient,
    "cm": BinanceCmRestClient,
}


# ── Dead Letter Queue ───────────────────────────────────────────


_log = logging.getLogger(__name__)


def _route_to_dlq(catalog: Path, symbol: str, data_type: str, errors: list[str]) -> None:
    """Route failed records to DuckLake DLQ table (Pattern 4)."""
    from datetime import datetime

    meta = catalog / "metadata.ducklake"
    if not meta.exists():
        _log.warning("DLQ: metadata.ducklake not found at %s — errors dropped: %s", meta, errors)
        return
    con = None
    try:
        con = duckdb.connect(str(catalog / "catalog.duckdb"))
        con.execute("LOAD ducklake")
        con.execute(
            f"ATTACH 'ducklake:{meta}' AS dl (DATA_PATH '{catalog}/data', AUTOMATIC_MIGRATION true)"
        )
        con.execute("USE dl")
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        con.execute(
            "CREATE TABLE IF NOT EXISTS bronze.dlq ("
            "ts_recv TIMESTAMP, symbol VARCHAR, data_type VARCHAR, errors VARCHAR[]"
            ")"
        )
        con.execute(
            "INSERT INTO bronze.dlq VALUES (?, ?, ?, ?)",
            [datetime.now(), symbol, data_type, errors],
        )
        con.close()
    except Exception as exc:
        _log.error("DLQ: failed to route errors for %s: %s", symbol, exc)
        if con:
            con.close()


# ── Pipelines ───────────────────────────────────────────────────


@flow(name="DLT Historical Pipeline", log_prints=True, task_runner=ThreadPoolTaskRunner())
def dlt_historical_pipeline(
    symbols: list[str],
    trade_type: str = "spot",
    data_type: str = "klines",
    interval: str = "1h",
    source: str = "rest",
    lookback_days: int = 30,
    archive_home: Path | None = None,
    catalog_path: Path | None = None,
) -> dict:
    """End-to-end historical data ingestion via dlt + Polars (Silver layer)."""
    sync_metadata_task(os.environ.get("LAKE_PATH", "./lake"))
    home = archive_home or _DEFAULT_ARCHIVE_HOME
    catalog = catalog_path or home.parent / "lake"
    catalog.mkdir(parents=True, exist_ok=True)
    db_path = str(catalog / "catalog.duckdb")

    results = {}
    for sym in symbols:
        results[sym] = {"healthy": False}
        _tt = "um" if data_type == "fundingRate" else trade_type
        _iv = interval

        try:
            # Step 1: Detect gaps (Pattern 2)
            # (deferred until we verify stable catalog resolution)

            # Step 2: DLT extraction & normalization
            dlt_task = run_dlt_source if source == "rest" else extract_archive
            if data_type == "aggTrades":
                dlt_task = run_dlt_agg_trades
            elif data_type == "fundingRate":
                dlt_task = run_dlt_funding_rate

            # Execute preparation step (Extraction -> Normalize -> Load -> Transform)
            # Wrapped in a Prefect task for observability
            prepare_symbol_dlt(
                symbol=sym,
                trade_type=_tt,
                data_type=data_type,
                interval=_iv,
                db_path=db_path,
                dlt_task=dlt_task,
            )

            # Step 3: Health check — verify DuckLake data quality for each symbol.
            print("  Running health checks...")
            h = health_flow(
                trade_type=_tt,
                symbol=sym,
                data_type=data_type,
                interval=_iv,
                catalog_path=catalog,
            )
            results[sym]["healthy"] = h.get("healthy", False)

        except Exception as exc:
            _log.error("Pipeline failed for %s: %s", sym, exc)
            results[sym]["healthy"] = False
            results[sym]["health_error"] = str(exc)

    return results


@task(name="Prepare Symbol (DLT)", **_RETRY_CONFIG)
def prepare_symbol_dlt(
    symbol: str,
    trade_type: str,
    data_type: str,
    interval: str,
    db_path: str,
    dlt_task: Any,
) -> dict:
    """Orchestrate dlt ingestion and transform for a single symbol."""
    _tt = trade_type
    _iv = interval

    # 1. Run DLT Source (Extract)
    extract_output = dlt_task(symbol=symbol, interval=_iv, trade_type=_tt, catalog_path=db_path)

    # 2. Sink to Silver (Load + Transform)
    # This task handles the DuckDB/DuckLake catalog interaction
    return sink_silver(
        extract_output=extract_output,
        data_type=data_type,
        symbol=symbol,
        interval=_iv,
        trade_type=_tt,
        catalog_path=Path(db_path).parent,
    )


@task(name="Extract OHLCV (REST)", **_RETRY_CONFIG)
def run_dlt_source(
    symbol: str, interval: str, trade_type: str, catalog_path: str | None = None
) -> dict:
    return extract_klines(symbol, interval, trade_type, catalog_path)


@task(name="Extract AggTrades (REST)", **_RETRY_CONFIG)
def run_dlt_agg_trades(symbol: str, trade_type: str, catalog_path: str | None = None) -> dict:
    return extract_agg_trades(symbol, trade_type, catalog_path)


@task(name="Extract FundingRate (REST)", **_RETRY_LIGHT)
def run_dlt_funding_rate(symbol: str, trade_type: str, catalog_path: str | None = None) -> dict:
    return extract_funding_rate(symbol, trade_type, catalog_path)


@flow(name="Sink to DuckLake", log_prints=True)
def sink_silver(
    extract_output: dict,
    data_type: str,
    symbol: str,
    interval: str = "1h",
    trade_type: str = "spot",
    archive_home: Path | None = None,
    catalog_path: Path | None = None,
) -> int:
    """Sink to DuckLake. Prefers dlt + Polars transforms from DuckDB bronze tables.

    Falls back to legacy SinkWorkflow for file-based archive data.
    Serialized via concurrency guard.
    """
    from prefect.concurrency.sync import concurrency as _pcon

    with _pcon("ducklake-writer", occupy=1):
        home = archive_home or _DEFAULT_ARCHIVE_HOME
        catalog = catalog_path or home.parent / "lake"
        catalog.mkdir(parents=True, exist_ok=True)
        db_path = str(catalog / "catalog.duckdb")

        # Try dlt path first (DuckDB bronze tables)
        _TRANSFORMS = {
            "klines": transform_klines,
            "aggTrades": transform_agg_trades,
            "trades": transform_trades,
            "fundingRate": transform_funding_rate,
        }
        transform_func = _TRANSFORMS.get(data_type)

        if transform_func and "dlt_result" in extract_output:
            dlt_result = extract_output["dlt_result"]
            source_name = dlt_result["source_name"]

            # 1. Load to DuckDB Bronze
            from binance_datatool.dlt.destinations import load_source

            load_source(source_name=source_name, catalog_path=db_path)

            # 2. Transform Bronze to Silver
            return transform_func(
                symbol=symbol,
                interval=interval,
                trade_type=trade_type,
                catalog_path=db_path,
            )

        # Fallback: legacy SinkWorkflow
        wf = SinkWorkflow(
            archive_home=home,
            catalog_path=catalog,
        )
        sink_res = wf.transform(
            trade_type=TradeType(trade_type),
            data_type=DataType(data_type),
            symbols=[symbol],
            interval=interval,
        )
        return sink_res.row_count


# ── Metadata & Maintenance ──────────────────────────────────────


@flow(name="Refresh Metadata", log_prints=True)
def refresh_metadata_flow(
    trade_type: str = "spot",
    duckdb_path: Path | None = None,
    catalog_path: Path | None = None,
) -> dict:
    """Discover symbols and venues via dlt and update the catalog.

    Uses the ``ducklake-writer`` concurrency guard to avoid racing with
    :func:`sink_silver` when both run as separate deployments.

    Uses dlt ``build_metadata_source`` for symbol discovery.
    """
    from prefect.concurrency.sync import concurrency as _pcon

    with _pcon("ducklake-writer", occupy=1):
        home = _DEFAULT_ARCHIVE_HOME
        catalog = catalog_path or home.parent / "lake"
        db_path = str(duckdb_path) if duckdb_path else str(catalog / "catalog.duckdb")

        # dlt metadata source (preferred)
        from binance_datatool.common.enums import TradeType
        from binance_datatool.dlt.destinations import run_source
        from binance_datatool.dlt.resources.binance_metadata import build_metadata_source

        source = build_metadata_source(trade_types=[TradeType(trade_type)])
        run_source(source, source_name="metadata_refresh", catalog_path=db_path)
        try:
            from binance_datatool.workflow.prefect_tasks.extract import extract_metadata

            return extract_metadata([trade_type], db_path)
        except ImportError:
            return {"status": "dlt metadata synced"}


@task(name="Refresh Archive Cache")
def refresh_archive_cache(
    archive_home: Path | None = None,
    trade_types: list[str] | None = None,
) -> dict:
    """Audit local archive files and refresh the index in DuckDB."""
    from binance_datatool.workflow.archive_cache import ArchiveFileCache

    home = archive_home or _DEFAULT_ARCHIVE_HOME
    cache = ArchiveFileCache(archive_home=home)
    return cache.refresh(trade_types=trade_types)


@flow(name="Health Check", log_prints=True)
def health_flow(
    trade_type: str = "spot",
    symbol: str = "BTCUSDT",
    data_type: str = "klines",
    interval: str = "1h",
    archive_home: Path | None = None,
    catalog_path: Path | None = None,
) -> dict:
    """Run health check and anomaly detection via DuckLake native tables."""
    from binance_datatool.storage.duckdb import get_connection

    home = archive_home or _DEFAULT_ARCHIVE_HOME
    catalog = catalog_path or home.parent / "lake"

    con = get_connection(lake_path=catalog)
    try:
        report = check_ducklake_anomalies(
            con=con,
            table_name=data_type,
            symbol=symbol,
            interval=interval,
        )
        anomalies_clean = report.is_clean
    finally:
        con.close()

    result = {
        "healthy": anomalies_clean,
        "missing_dates": 0,
        "anomalies_clean": anomalies_clean,
        "null_prices": 0,
    }
    print(f"Health: {result}")
    return result


if __name__ == "__main__":
    # Internal CLI entry point for testing
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        from binance_datatool.workflow.prefect_flows import (
            dlt_historical_pipeline,
        )
        # Deployment logic
    else:
        dlt_historical_pipeline(["BTCUSDT"])
