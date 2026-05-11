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

import asyncio
import logging
from pathlib import Path
from typing import Any

from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner

from binance_datatool.archive.client import ArchiveClient
from binance_datatool.common import DataFrequency, DataType, TradeType
from binance_datatool.common.settings import settings
from binance_datatool.exchange import (
    BinanceCmRestClient,
    BinanceSpotRestClient,
    BinanceUmRestClient,
)
from binance_datatool.lineage import LineageTracker
from binance_datatool.workflow import (
    ArchiveDownloadWorkflow,
    ArchiveListSymbolsWorkflow,
    ArchiveVerifyWorkflow,
    GapFillWorkflow,
    MetadataWorkflow,
    SinkWorkflow,
)
from binance_datatool.workflow.health_check import check_ducklake_anomalies

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

    import duckdb

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
        con.execute(
            "CREATE TABLE IF NOT EXISTS dlq ("
            "symbol VARCHAR, data_type VARCHAR, error VARCHAR, "
            "ingested_at BIGINT, source VARCHAR"
            ")"
        )
        now = int(datetime.now().timestamp() * 1000)
        for err in errors:
            con.execute(
                "INSERT INTO dlq VALUES (?, ?, ?, ?, ?)",
                [symbol, data_type, err, now, "sink_silver"],
            )
        cnt = (con.execute("SELECT COUNT(*) FROM dlq").fetchone() or [0])[0]
        _log.info("DLQ: %d total failures for %s/%s", cnt, symbol, data_type)
    except Exception as e:
        _log.warning("DLQ route failed: %s", e)
    finally:
        if con is not None:
            con.close()


# ── Tasks ───────────────────────────────────────────────────────


@task(**_RETRY_CONFIG)
def download_archive(
    trade_type: str,
    symbol: str,
    data_type: str = "klines",
    interval: str | None = "1h",
    lookback_days: int | None = None,
    archive_home: Path | None = None,
) -> int:
    """Download archive data via ArchiveDownloadWorkflow."""
    home = archive_home or _DEFAULT_ARCHIVE_HOME
    dt = DataType(data_type)
    wf = ArchiveDownloadWorkflow(
        trade_type=TradeType(trade_type),
        data_freq=DataFrequency.monthly if data_type == "fundingRate" else DataFrequency.daily,
        data_type=dt,
        symbols=[symbol],
        archive_home=home,
        interval=interval,
        lookback_days=lookback_days,
    )
    result = asyncio.run(wf.run())
    return result.downloaded  # type: ignore[unresolved-attribute]


@task(**_RETRY_CONFIG)
def verify_archive(
    trade_type: str,
    symbol: str,
    data_type: str = "klines",
    interval: str = "1h",
    archive_home: Path | None = None,
) -> int:
    """Verify checksums via ArchiveVerifyWorkflow."""
    home = archive_home or _DEFAULT_ARCHIVE_HOME
    dt = DataType(data_type)
    wf = ArchiveVerifyWorkflow(
        trade_type=TradeType(trade_type),
        data_freq=DataFrequency.monthly if data_type == "fundingRate" else DataFrequency.daily,
        data_type=dt,
        symbols=[symbol],
        archive_home=home,
        interval=interval,
    )
    result = wf.run()
    return result.verified if hasattr(result, "verified") else 0  # type: ignore[return-value]


@task(**_RETRY_LIGHT)
def fill_gaps(
    trade_type: TradeType,
    symbol: str,
    data_type: str = "klines",
    interval: str = "1h",
    lookback_days: int = 30,
    archive_home: Path | None = None,
) -> list[tuple[str, int, int]]:
    """Detect and fill gaps via GapFillWorkflow."""
    home = archive_home or _DEFAULT_ARCHIVE_HOME
    Client = _REST_CLIENTS.get(trade_type.value, BinanceSpotRestClient)
    workflow = GapFillWorkflow(
        exchange_client=Client(),
        archive_home=home,
        symbols=[symbol],
        data_type=data_type,
        interval=interval,
        tracker=LineageTracker(),
        lookback_days=lookback_days,
    )
    result = asyncio.run(workflow.run(detect_gaps=True))
    return result.gaps_detected


@task
def sink_silver(
    trade_type: TradeType,
    symbol: str,
    data_type: str = "klines",
    interval: str = "1h",
    lookback_days: int = 30,
    archive_home: Path | None = None,
    catalog_path: Path | None = None,
) -> int:
    """Sink to DuckLake via SinkWorkflow. Serialized via concurrency guard."""
    from prefect.concurrency.sync import concurrency as _pcon

    with _pcon("ducklake-writer", occupy=1):
        home = archive_home or _DEFAULT_ARCHIVE_HOME
        catalog = catalog_path or home.parent / "lake"
        catalog.mkdir(parents=True, exist_ok=True)
        (catalog / "data").mkdir(parents=True, exist_ok=True)
        dt = DataType(data_type)
        workflow = SinkWorkflow(
            archive_home=home,
            catalog_path=catalog,
            duckdb_path=catalog / "catalog.duckdb",
            tracker=LineageTracker(),
        )
        stats = workflow.transform(
            trade_type=TradeType(trade_type),
            data_type=dt,
            symbols=[symbol],
            interval=interval,
        )
        if stats.errors:
            _route_to_dlq(catalog, symbol, data_type, stats.errors)
        return stats.row_count


# ── Composed Flows ───────────────────────────────────────────────


@task
def prepare_symbol(
    trade_type: str,
    symbol: str,
    data_type: str = "klines",
    interval: str = "1h",
    lookback_days: int = 30,
    archive_home: Path | None = None,
) -> dict[str, Any]:
    """Prepare data: download → verify → fill_gaps (parallel-safe).
    Does NOT write to DuckDB — avoids concurrent write conflicts.
    """
    home = archive_home or _DEFAULT_ARCHIVE_HOME
    tt = TradeType(trade_type)
    iv = interval if data_type == "klines" else None
    download_archive(trade_type, symbol, data_type, iv, lookback_days, home)
    verify_archive(trade_type, symbol, data_type, iv, home)
    gaps = fill_gaps(tt, symbol, data_type, iv, lookback_days, home)
    return {"symbol": symbol, "gaps": len(gaps)}


@flow(
    name="Historical Data Pipeline",
    description="Metadata → download → verify → gap-fill → sink (parallel symbols)",
    log_prints=True,
    task_runner=ThreadPoolTaskRunner(),
)
def historical_pipeline(
    trade_type: str = "spot",
    symbols: list[str] | None = None,
    data_type: str = "klines",
    interval: str = "1h",
    lookback_days: int = 30,
    archive_home: Path | None = None,
    catalog_path: Path | None = None,
) -> dict[str, Any]:
    """Full historical data pipeline with parallel symbol processing."""
    home = archive_home or _DEFAULT_ARCHIVE_HOME
    catalog = catalog_path or home.parent / "lake"

    # Step 0: Metadata refresh (sequential, single task)
    refresh_metadata_flow(trade_type=trade_type, catalog_path=catalog)
    print(f"  Metadata refreshed for {trade_type}")

    # Step 1: Fan-out — prepare symbols in parallel (no DuckDB writes)
    sym_list = symbols or ["BTCUSDT"]
    prep_futures = prepare_symbol.map(
        trade_type=[trade_type] * len(sym_list),
        symbol=sym_list,
        data_type=[data_type] * len(sym_list),
        interval=[interval] * len(sym_list),
        lookback_days=[lookback_days] * len(sym_list),
        archive_home=[home] * len(sym_list),
    )

    # Step 2: Sequential sink — DuckDB does not support concurrent writers.
    # Prefect-native error isolation: zip symbols with futures so we always
    # know which symbol failed, even when the task itself raises.
    tt = TradeType(trade_type)
    iv = interval if data_type == "klines" else None
    results: dict[str, Any] = {}
    for sym, future in zip(sym_list, prep_futures, strict=True):
        meta = future.result(raise_on_failure=False)
        if future.state.is_completed():
            rows = sink_silver(tt, sym, data_type, iv, lookback_days, home, catalog)
            results[sym] = {"gaps_filled": meta["gaps"], "rows_sunk": rows}
            print(f"  {sym}: {meta['gaps']} gaps, {rows} rows")
        else:
            err = str(meta) if meta else "unknown error"
            results[sym] = {"gaps_filled": 0, "rows_sunk": 0, "error": err}
            print(f"  {sym}: FAILED — {err}")

    # Step 3: Health check — verify DuckLake data quality for each symbol.
    # Sequential subflow calls (DuckDB reads are fast; no bottleneck).
    # Skips symbols that errored during prepare.
    print("  Running health checks...")
    for sym in sym_list:
        if sym not in results or results[sym].get("error"):
            continue
        try:
            h = health_flow(
                trade_type=trade_type,
                symbol=sym,
                data_type=data_type,
                interval=interval,
                archive_home=home,
                catalog_path=catalog,
            )
            results[sym]["healthy"] = h.get("healthy", False)
        except Exception as exc:
            results[sym]["healthy"] = False
            results[sym]["health_error"] = str(exc)

    return results


@flow(
    name="Bulk Historical Backfill",
    log_prints=True,
)
def bulk_backfill(
    trade_type: str = "spot",
    symbols: list[str] | None = None,
    data_type: str = "klines",
    interval: str = "1h",
    lookback_days: int = 30,
    max_symbols: int = 10,
    archive_home: Path | None = None,
    catalog_path: Path | None = None,
) -> dict[str, Any]:
    """Backfill multiple symbols using historical_pipeline (parallel by design).

    Args:
        max_symbols: Max auto-discovered symbols to backfill (default 10).
                     Ignored when ``symbols`` is provided explicitly.
    """
    if not symbols:
        client = ArchiveClient()
        wf = ArchiveListSymbolsWorkflow(
            client=client,
            trade_type=TradeType(trade_type),
            data_freq=DataFrequency.daily,
            data_type=DataType(data_type),
        )
        result = asyncio.run(wf.run())
        symbols = [s.symbol for s in result.matched[:max_symbols]]
        print(f"  Auto-discovered {len(symbols)} symbols (max_symbols={max_symbols})")

    return historical_pipeline(
        trade_type=trade_type,
        symbols=symbols,
        data_type=data_type,
        interval=interval,
        lookback_days=lookback_days,
        archive_home=archive_home,
        catalog_path=catalog_path,
    )


# ── Standalone Flows (callable from CLI) ────────────────────────


@flow(
    name="Download",
    log_prints=True,
    task_runner=ThreadPoolTaskRunner(),
)
def download_flow(
    trade_type: str,
    symbols: list[str],
    data_type: str = "klines",
    interval: str | None = None,
    archive_home: Path | None = None,
) -> int:
    """Download archive data for multiple symbols in parallel."""
    futures = download_archive.map(
        trade_type=[trade_type] * len(symbols),
        symbol=symbols,
        data_type=[data_type] * len(symbols),
        interval=[interval] * len(symbols),
        archive_home=[archive_home] * len(symbols),
    )
    return sum(f.result() for f in futures)


@flow(
    name="Verify",
    log_prints=True,
    task_runner=ThreadPoolTaskRunner(),
)
def verify_flow(
    trade_type: str,
    symbols: list[str],
    data_type: str = "klines",
    interval: str | None = None,
    archive_home: Path | None = None,
) -> int:
    """Verify checksums for multiple symbols in parallel."""
    futures = verify_archive.map(
        trade_type=[trade_type] * len(symbols),
        symbol=symbols,
        data_type=[data_type] * len(symbols),
        interval=[interval] * len(symbols),
        archive_home=[archive_home] * len(symbols),
    )
    return sum(f.result() for f in futures)


@flow(name="Gap Fill", log_prints=True)
def gap_fill_flow(
    trade_type: str,
    symbol: str,
    data_type: str = "klines",
    interval: str | None = None,
    lookback_days: int = 30,
    archive_home: Path | None = None,
) -> int:
    """Auto-detect and fill gaps. Wraps GapFillWorkflow with Prefect."""
    tt = TradeType(trade_type)
    gaps = fill_gaps(tt, symbol, data_type, interval, lookback_days, archive_home)
    return len(gaps)


@flow(name="Sink", log_prints=True)
def sink_flow(
    trade_type: str,
    symbols: list[str],
    data_type: str = "klines",
    interval: str | None = None,
    archive_home: Path | None = None,
    catalog_path: Path | None = None,
) -> int:
    """Sink to DuckLake. Wraps SinkWorkflow with Prefect."""
    tt = TradeType(trade_type)
    home = archive_home or _DEFAULT_ARCHIVE_HOME
    catalog = catalog_path or home.parent / "lake"
    total = 0
    for sym in symbols:
        total += sink_silver(tt, sym, data_type, interval, home, catalog)
    return total


@flow(name="Refresh Metadata", log_prints=True)
def refresh_metadata_flow(
    trade_type: str = "spot",
    catalog_path: Path | None = None,
    from_api: bool = False,
    duckdb_path: str | None = None,
) -> int:
    """Refresh venue/symbol metadata. Wraps MetadataWorkflow with Prefect.

    Uses the ``ducklake-writer`` concurrency guard to avoid racing with
    :func:`sink_silver` when both run as separate deployments.
    """
    from prefect.concurrency.sync import concurrency as _pcon

    with _pcon("ducklake-writer", occupy=1):
        home = _DEFAULT_ARCHIVE_HOME
        catalog = catalog_path or home.parent / "lake"
        client = ArchiveClient()
        wf = MetadataWorkflow(
            archive_client=client,
            catalog_path=catalog,
            source_label="api" if from_api else "archive",
            duckdb_path=Path(duckdb_path) if duckdb_path else catalog / "catalog.duckdb",
        )
        wf.save_venues(wf.refresh_venues())
        syms = asyncio.run(wf.refresh_symbols(TradeType(trade_type)))
        wf.save_symbols(syms)
        return len(syms)


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
    home = archive_home or _DEFAULT_ARCHIVE_HOME
    catalog = catalog_path or home.parent / "lake"
    db_file = catalog / "catalog.duckdb"
    meta = catalog / "metadata.ducklake"

    anomalies_clean = True
    null_prices = 0
    missing_dates = 0

    if meta.exists():
        import duckdb

        con = duckdb.connect(str(db_file))
        try:
            con.execute("LOAD ducklake")
            con.execute(
                f"ATTACH 'ducklake:{meta}' AS dl "
                f"(DATA_PATH '{catalog}/data', AUTOMATIC_MIGRATION true)"
            )
            con.execute("USE dl")
            rtype = {"aggTrades": "agg", "trades": "trade"}.get(data_type)
            anomalies = check_ducklake_anomalies(
                con, data_type.replace("-", "_"), symbol, rtype=rtype
            )
            anomalies_clean = anomalies.is_clean
            null_prices = anomalies.null_prices
            missing_dates = len(anomalies.date_gaps)
        finally:
            con.close()
    else:
        print(f"  DuckLake catalog not found at {meta} — run sink first")

    result = {
        "healthy": anomalies_clean,
        "missing_dates": missing_dates,
        "anomalies_clean": anomalies_clean,
        "null_prices": null_prices,
    }
    print(f"Health: {result}")
    return result


# ── dlt + SQLMesh Pipeline ──────────────────────────────────────


@task(retries=2, retry_delay_seconds=10, retry_jitter_factor=0.2)
def run_dlt_source(
    symbol: str,
    interval: str = "1h",
    trade_type: str = "spot",
    catalog_path: str | None = None,
) -> dict:
    """Run dlt pipeline to ingest Binance klines for one symbol."""
    from binance_datatool.dlt_sources.binance import build_binance_source
    from binance_datatool.dlt_sources.pipeline import run_source

    tt = TradeType(trade_type)
    source = build_binance_source(symbols=[symbol], interval=interval, trade_type=tt)
    return run_source(
        source, source_name=f"binance_{trade_type}_{symbol}", catalog_path=catalog_path
    )


@task(retries=2, retry_delay_seconds=10, retry_jitter_factor=0.2)
def transform_to_silver(
    symbol: str,
    interval: str = "1h",
    trade_type: str = "spot",
    catalog_path: str | None = None,
) -> int:
    """Read bronze klines from DuckDB, transform to Silver, write back.

    Uses Polars for the Bronze→Silver transform. The silver DataFrame is
    registered as a DuckDB view (zero-copy) then inserted via SQL.
    """
    import duckdb

    from binance_datatool.transforms.klines import bronze_klines_to_silver

    db_file = catalog_path or str(
        (_DEFAULT_ARCHIVE_HOME.parent / "lake" / "catalog.duckdb").resolve()
    )
    con = duckdb.connect(db_file)
    try:
        bronze = con.execute("SELECT * FROM bronze.klines WHERE symbol = ?", [symbol]).pl()
        if bronze.is_empty():
            return 0
        silver = bronze_klines_to_silver(
            bronze, symbol=symbol, interval=interval, trade_type=trade_type
        )
        if silver.is_empty():
            return 0
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute(
            "CREATE TABLE IF NOT EXISTS silver.klines AS SELECT * FROM silver.klines WHERE FALSE"
        )
        con.execute("DELETE FROM silver.klines WHERE symbol = ?", [symbol])
        _silver_pd = silver.to_pandas()
        con.execute("INSERT INTO silver.klines SELECT * FROM _silver_pd")
        return silver.height
    finally:
        con.close()


@task(retries=2, retry_delay_seconds=10, retry_jitter_factor=0.2)
def run_dlt_archive(
    symbol: str,
    interval: str = "1h",
    trade_type: str = "spot",
    catalog_path: str | None = None,
) -> dict:
    """Run dlt pipeline to ingest Binance archive klines for one symbol."""
    from binance_datatool.dlt_sources.binance_archive import build_archive_source
    from binance_datatool.dlt_sources.pipeline import run_source

    tt = TradeType(trade_type)
    source = build_archive_source(symbols=[symbol], interval=interval, trade_type=tt)
    return run_source(
        source, source_name=f"archive_{trade_type}_{symbol}", catalog_path=catalog_path
    )


@task(retries=2, retry_delay_seconds=10, retry_jitter_factor=0.2)
def run_dlt_agg_trades(
    symbol: str,
    trade_type: str = "spot",
    catalog_path: str | None = None,
) -> dict:
    """Run dlt pipeline to ingest Binance aggTrades for one symbol."""
    from binance_datatool.dlt_sources.binance_rest import build_rest_source
    from binance_datatool.dlt_sources.pipeline import run_source

    tt = TradeType(trade_type)
    source = build_rest_source(symbols=[symbol], data_type="aggTrades", trade_type=tt)
    return run_source(
        source, source_name=f"agg_trades_{trade_type}_{symbol}", catalog_path=catalog_path
    )


@task(retries=2, retry_delay_seconds=10, retry_jitter_factor=0.2)
def run_dlt_funding_rate(
    symbol: str,
    trade_type: str = "um",
    catalog_path: str | None = None,
) -> dict:
    """Run dlt pipeline to ingest Binance fundingRate for one symbol."""
    from binance_datatool.dlt_sources.binance_rest import build_rest_source
    from binance_datatool.dlt_sources.pipeline import run_source

    tt = TradeType(trade_type)
    source = build_rest_source(symbols=[symbol], data_type="fundingRate", trade_type=tt)
    return run_source(
        source, source_name=f"funding_rate_{trade_type}_{symbol}", catalog_path=catalog_path
    )


@task(retries=2, retry_delay_seconds=10, retry_jitter_factor=0.2)
def run_dlt_ws(
    symbol: str,
    interval: str = "1h",
    trade_type: str = "spot",
    max_items: int = 100,
    catalog_path: str | None = None,
) -> dict:
    """Run dlt pipeline to stream Binance klines via WebSocket for one symbol."""
    from binance_datatool.dlt_sources.binance_ws import build_ws_source
    from binance_datatool.dlt_sources.pipeline import run_source

    tt = TradeType(trade_type)
    source = build_ws_source(
        symbols=[symbol], interval=interval, trade_type=tt, max_items=max_items
    )
    return run_source(source, source_name=f"ws_{trade_type}_{symbol}", catalog_path=catalog_path)


@task(retries=2, retry_delay_seconds=10, retry_jitter_factor=0.2)
def transform_agg_trades_to_silver(
    symbol: str,
    trade_type: str = "spot",
    catalog_path: str | None = None,
) -> int:
    """Read bronze aggTrades from DuckDB, transform to Silver, write back."""
    import duckdb

    from binance_datatool.transforms.agg_trades import bronze_agg_trades_to_silver

    db_file = catalog_path or str(
        (_DEFAULT_ARCHIVE_HOME.parent / "lake" / "catalog.duckdb").resolve()
    )
    con = duckdb.connect(db_file)
    try:
        bronze = con.execute(f"SELECT * FROM bronze.rest_{symbol.lower()}_agg_trades").pl()
        silver = bronze_agg_trades_to_silver(bronze, symbol=symbol, trade_type=trade_type)
        if silver.is_empty():
            return 0
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute(
            "CREATE TABLE IF NOT EXISTS silver.agg_trades AS "
            "SELECT * FROM silver.agg_trades WHERE FALSE"
        )
        con.execute("DELETE FROM silver.agg_trades WHERE symbol = ?", [symbol])
        _silver_pd = silver.to_pandas()
        con.execute("INSERT INTO silver.agg_trades SELECT * FROM _silver_pd")
        return silver.height
    finally:
        con.close()


@task(retries=2, retry_delay_seconds=10, retry_jitter_factor=0.2)
def transform_funding_rate_to_silver(
    symbol: str,
    trade_type: str = "um",
    catalog_path: str | None = None,
) -> int:
    """Read bronze fundingRate from DuckDB, transform to Silver, write back."""
    import duckdb

    from binance_datatool.transforms.funding_rate import bronze_funding_rate_to_silver

    db_file = catalog_path or str(
        (_DEFAULT_ARCHIVE_HOME.parent / "lake" / "catalog.duckdb").resolve()
    )
    con = duckdb.connect(db_file)
    try:
        bronze = con.execute(f"SELECT * FROM bronze.rest_{symbol.lower()}_funding_rate").pl()
        silver = bronze_funding_rate_to_silver(bronze, symbol=symbol, trade_type=trade_type)
        if silver.is_empty():
            return 0
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute(
            "CREATE TABLE IF NOT EXISTS silver.funding_rate AS "
            "SELECT * FROM silver.funding_rate WHERE FALSE"
        )
        con.execute("DELETE FROM silver.funding_rate WHERE symbol = ?", [symbol])
        _silver_pd = silver.to_pandas()
        con.execute("INSERT INTO silver.funding_rate SELECT * FROM _silver_pd")
        return silver.height
    finally:
        con.close()


@task
def run_sqlmesh_plan(
    environment: str = "prod",
    start: str | None = None,
    end: str | None = None,
) -> dict:
    """Run SQLMesh plan to apply pending model changes.

    Requires ``sqlmesh`` to be installed. Falls back gracefully when absent.
    """
    try:
        from sqlmesh import Context
    except ImportError:
        return {"environment": environment, "applied": False, "error": "sqlmesh not installed"}

    ctx = Context(paths=["config.yaml"])
    plan = ctx.plan(environment, start=start, end=end, include_unmodified=False)
    plan.apply()
    return {"environment": environment, "applied": True}


@flow(
    name="DLT Pipeline",
    description="dlt → Polars transform → SQLMesh → DuckLake",
    log_prints=True,
)
def dlt_sqlmesh_pipeline(
    symbol: str = "BTCUSDT",
    interval: str = "1h",
    trade_type: str = "spot",
    data_type: str = "klines",
    source: str = "rest",
    catalog_path: str | None = None,
) -> dict:
    """dlt → Polars transform → SQLMesh → DuckLake.

    Supports multiple data types and source backends:
    - data_type: ``"klines"``, ``"aggTrades"``, ``"fundingRate"``
    - source: ``"rest"`` (API), ``"archive"`` (ZIP), ``"ws"`` (streaming)

    Args:
        symbol: Trading pair.
        interval: Kline interval (for klines only).
        trade_type: Market type (``"spot"``, ``"um"``, ``"cm"``).
        data_type: Data type.
        source: Source backend.
        catalog_path: Full path to ``catalog.duckdb``.

    Returns:
        Dict with stage results.
    """
    db_path = catalog_path or str(
        (_DEFAULT_ARCHIVE_HOME.parent / "lake" / "catalog.duckdb").resolve()
    )

    _STAGES: dict[str, tuple] = {
        "klines": (run_dlt_source, transform_to_silver),
        "aggTrades": (run_dlt_agg_trades, transform_agg_trades_to_silver),
        "fundingRate": (run_dlt_funding_rate, transform_funding_rate_to_silver),
    }
    dlt_task, transform_task = _STAGES.get(data_type, (run_dlt_source, transform_to_silver))
    _iv = interval if data_type == "klines" else None
    _tt = "um" if data_type == "fundingRate" else trade_type

    print(f"  Stage 1: dlt ({source}) — ingesting {symbol} {data_type}")
    if source == "archive":
        dlt_result = run_dlt_archive(symbol, _iv, _tt, db_path)
    elif source == "ws":
        dlt_result = run_dlt_ws(symbol, _iv, _tt, 100, db_path)
    else:
        dlt_result = dlt_task(symbol=symbol, trade_type=_tt, catalog_path=db_path)
    print(f"    dlt tables: {dlt_result['tables_loaded']}")

    print(f"  Stage 2: Polars + Pandera — transforming {data_type} → silver")
    kwargs = {"symbol": symbol, "trade_type": _tt, "catalog_path": db_path}
    if data_type == "klines":
        kwargs["interval"] = _iv
    rows = transform_task(**kwargs)
    print(f"    silver rows: {rows}")

    print("  Stage 3: SQLMesh — applying model")
    sm_result = run_sqlmesh_plan()
    print(f"    SQLMesh applied: {sm_result['applied']}")

    return {
        "symbol": symbol,
        "data_type": data_type,
        "source": source,
        "dlt_tables": dlt_result["tables_loaded"],
        "silver_rows": rows,
        "sqlmesh_applied": sm_result["applied"],
    }


# ── Deployment Entry Points ─────────────────────────────────────

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        from prefect import serve as _serve

        _serve(
            historical_pipeline.to_deployment(name="daily-backfill", cron="0 6 * * *"),
            refresh_metadata_flow.to_deployment(name="hourly-metadata", cron="0 * * * *"),
            dlt_sqlmesh_pipeline.to_deployment(name="dlt-sqlmesh-e2e", cron="0 */12 * * *"),
        )
    else:
        dlt_sqlmesh_pipeline()
