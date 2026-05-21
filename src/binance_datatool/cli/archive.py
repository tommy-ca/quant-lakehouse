"""Archive CLI commands."""

from __future__ import annotations

import asyncio
import sys
from datetime import UTC
from typing import TYPE_CHECKING, Annotated

import typer
from loguru import logger

from binance_datatool.cli import app
from binance_datatool.common import (
    ArchiveHomeNotConfiguredError,
    ContractType,
    DataFrequency,
    DataType,
    TradeType,
    build_symbol_filter,
    resolve_archive_home,
)
from binance_datatool.workflow import (
    ArchiveDownloadWorkflow,
    ArchiveListFilesWorkflow,
    ArchiveListSymbolsWorkflow,
    ArchiveVerifyWorkflow,
    DiffResult,
    DownloadResult,
    HealthCheckWorkflow,
    SymbolListingError,
    VerifyDiffResult,
    VerifyResult,
)
from binance_datatool.workflow.prefect_flows import (
    backtesting_data_product_flow,
    gap_fill_flow,
    health_flow,
    refresh_metadata_flow,
    sink_flow,
    universe_maintenance_flow,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from binance_datatool.exchange.client import ExchangeClient


from pathlib import Path


def _exchange_client_for_trade_type(trade_type: TradeType) -> ExchangeClient:
    """Create a REST exchange client for the given trade type."""
    match trade_type:
        case TradeType.spot:
            from binance_datatool.exchange import BinanceSpotRestClient

            return BinanceSpotRestClient()
        case TradeType.um:
            from binance_datatool.exchange import BinanceUmRestClient

            return BinanceUmRestClient()
        case TradeType.cm:
            from binance_datatool.exchange import BinanceCmRestClient

            return BinanceCmRestClient()


@app.command("list-symbols")
def list_symbols_command(
    trade_type: Annotated[TradeType, typer.Argument(help="Market segment.")],
    data_freq: Annotated[
        DataFrequency,
        typer.Option("--freq", help="Partition frequency."),
    ] = DataFrequency.daily,
    data_type: Annotated[
        DataType,
        typer.Option("--type", help="Dataset type."),
    ] = DataType.klines,
    quotes: Annotated[
        list[str] | None,
        typer.Option("--quote", help="Filter by quote asset. Repeat to allow multiple values."),
    ] = None,
    exclude_leverage: Annotated[
        bool,
        typer.Option("--exclude-leverage", help="Exclude leveraged spot tokens."),
    ] = False,
    exclude_stables: Annotated[
        bool,
        typer.Option("--exclude-stables", help="Exclude stablecoin pairs."),
    ] = False,
    contract_type: Annotated[
        ContractType | None,
        typer.Option("--contract-type", help="Filter futures symbols by contract type."),
    ] = None,
    from_catalog: Annotated[
        bool,
        typer.Option(
            "--from-catalog", help="Query local DuckDB metadata table instead of remote archive."
        ),
    ] = False,
    catalog_path: Annotated[
        str | None,
        typer.Option(
            "--catalog", help="DuckLake catalog path (default: ~/.binance-datatool/lake)."
        ),
    ] = None,
    source: Annotated[
        str,
        typer.Option("--source", help="Source: auto, archive (S3), or catalog (DuckDB)."),
    ] = "auto",
) -> None:
    """List symbol directories under a Binance archive prefix.

    Prints one symbol per line to stdout. Defaults to DuckDB catalog
    (dlt metadata). Use ``--source archive`` for live S3 listing.
    """
    if source == "catalog" or (source == "auto" and catalog_path):
        # DuckDB metadata path (dlt)
        _cat = catalog_path or str((resolve_archive_home().parent / "lake").resolve())
        from binance_datatool.workflow.prefect_flows import run_dlt_metadata

        run_dlt_metadata(trade_types=[trade_type.value], catalog_path=_cat)

        from binance_datatool.storage.duckdb import get_connection

        con = get_connection(lake_path=_cat)
        rows = con.execute(
            "SELECT symbol FROM metadata.symbols WHERE trade_type = ? ORDER BY symbol",
            [trade_type.value],
        ).fetchall()
        for r in rows:
            # Apply CLI filters in-memory
            sym = r[0]
            if quotes and not any(sym.endswith(q) for q in [q.upper() for q in quotes]):
                continue
            if exclude_stables and any(
                stable in sym for stable in ("USDC", "USDP", "DAI", "TUSD", "BUSD", "FDUSD")
            ):
                continue
            typer.echo(sym)
        return

    # Live S3 listing path: prefer the legacy ArchiveListSymbolsWorkflow by
    # default to preserve existing behaviour and tests. An explicit
    # `--source adapter` option will use the adapter registry to resolve a
    # source implementation (this is an opt-in path for new integrations).
    symbol_filter = build_symbol_filter(
        trade_type=trade_type,
        quote_assets=frozenset(quote.upper() for quote in quotes) if quotes else None,
        exclude_leverage=exclude_leverage,
        exclude_stable_pairs=exclude_stables,
        contract_type=contract_type,
    )

    if source == "adapter":
        from binance_datatool.adapter.registry import registry

        adapter = registry.get("binance")
        if adapter is None:
            # Fallback to the legacy workflow path if registry resolution fails.
            workflow = ArchiveListSymbolsWorkflow(
                trade_type=trade_type,
                data_freq=data_freq,
                data_type=data_type,
                symbol_filter=symbol_filter,
            )
            for info in asyncio.run(workflow.run()).matched:
                typer.echo(info.symbol)
            return

        syms = asyncio.run(adapter.list_symbols(trade_type, data_freq, data_type))
        # Apply CLI filters in-memory (same as catalog path)
        for s in syms:
            if quotes and not any(s.endswith(q) for q in [q.upper() for q in quotes]):
                continue
            if exclude_stables and any(
                stable in s for stable in ("USDC", "USDP", "DAI", "TUSD", "BUSD", "FDUSD")
            ):
                continue
            typer.echo(s)
        return

    # Default legacy flow (ArchiveListSymbolsWorkflow) — preserves existing
    # behaviour and test expectations.
    workflow = ArchiveListSymbolsWorkflow(
        trade_type=trade_type,
        data_freq=data_freq,
        data_type=data_type,
        symbol_filter=symbol_filter,
    )
    for info in asyncio.run(workflow.run()).matched:
        typer.echo(info.symbol)


def _refresh_and_query(trade_type: TradeType, catalog_path: str, ttl: int) -> None:
    """Run metadata refresh inline (no Prefect server)."""
    import asyncio

    from binance_datatool.adapter.registry import registry
    from binance_datatool.workflow.metadata import MetadataWorkflow

    adapter = registry.get("binance")
    client = adapter.client if adapter else None
    if client is None:
        typer.echo("ArchiveClient unavailable — cannot refresh metadata.", err=True)
        raise typer.Exit(1)

    wf = MetadataWorkflow(
        archive_client=client,
        catalog_path=Path(catalog_path),
        source_label="archive",
        duckdb_path=Path(catalog_path) / "catalog.duckdb",
    )
    wf.save_venues(wf.refresh_venues())
    syms = asyncio.run(wf.refresh_symbols(trade_type))
    wf.save_symbols(syms)
    typer.echo(f"  Refreshed {len(syms)} {trade_type.value} symbols", err=True)


def _list_symbols_from_catalog(
    trade_type: TradeType,
    quotes: list[str] | None,
    exclude_leverage: bool,
    exclude_stables: bool,
    contract_type: ContractType | None,
    catalog_path: str | None,
    cache_ttl: int | None = None,
) -> None:
    """List symbols from local DuckDB catalog (no network call).

    Auto-refreshes metadata if cache is stale (older than ``cache_ttl``).
    """
    import time

    import duckdb

    from binance_datatool.common.settings import settings

    ttl = (cache_ttl if cache_ttl is not None else settings.cache_ttl_seconds) * 1000

    if not catalog_path:
        catalog_path = str(settings.catalog_path or settings.archive_home.parent / "lake")

    db_file = Path(catalog_path) / "catalog.duckdb"
    meta = Path(catalog_path) / "metadata.ducklake"

    # Ensure catalog exists and is fresh
    if not meta.exists():
        typer.echo("Catalog not found — auto-refreshing metadata...", err=True)
        _refresh_and_query(trade_type, catalog_path, ttl)
        if not meta.exists():
            typer.echo("Failed to create catalog.", err=True)
            raise typer.Exit(1)

    def _connect() -> duckdb.DuckDBPyConnection:
        c = duckdb.connect(str(db_file))
        c.execute("LOAD ducklake")
        c.execute(
            f"ATTACH 'ducklake:{meta}' AS dl (DATA_PATH '{catalog_path}/data', AUTOMATIC_MIGRATION true)"
        )
        c.execute("USE dl")
        return c

    con = _connect()
    try:
        row = con.execute(
            "SELECT MAX(fetched_at) FROM venues WHERE trade_type = ?", [trade_type.value]
        ).fetchone()
        last_fetch = row[0] if row and row[0] else 0
        now_ms = int(time.time() * 1000)
        if now_ms - last_fetch > ttl:
            typer.echo(
                f"Cache stale (age={(now_ms - last_fetch) // 1000}s, ttl={ttl // 1000}s) — auto-refreshing...",
                err=True,
            )
            con.close()
            _refresh_and_query(trade_type, catalog_path, ttl)
            con = _connect()
        conditions = ["trade_type = ?"]
        params: list[str | bool] = [trade_type.value]
        if quotes:
            qs = [q.upper() for q in quotes]
            placeholders = ",".join("?" for _ in qs)
            conditions.append(f"quote_asset IN ({placeholders})")
            params.extend(qs)
        if exclude_leverage:
            conditions.append("(is_leverage IS NULL OR is_leverage = false)")
        if exclude_stables:
            conditions.append("(is_stable_pair IS NULL OR is_stable_pair = false)")
        if contract_type:
            conditions.append("contract_type = ?")
            params.append(contract_type.value)

        rows = con.execute(
            f"SELECT symbol FROM symbols WHERE {' AND '.join(conditions)} ORDER BY symbol",
            params,
        ).fetchall()
        for row in rows:
            typer.echo(row[0])
    finally:
        con.close()


def _resolve_symbols(symbols: list[str] | None) -> list[str]:
    """Resolve symbol inputs from CLI arguments or piped stdin."""
    if symbols:
        return [symbol.strip().upper() for symbol in symbols if symbol.strip()]

    if not sys.stdin.isatty():
        return [line.strip().upper() for line in sys.stdin.read().splitlines() if line.strip()]

    return []


def _validate_interval(data_type: DataType, interval: str | None) -> None:
    """Validate whether an interval matches the selected data type."""
    if data_type.has_interval_layer and interval is None:
        raise typer.BadParameter(
            "Option '--interval' is required for kline-class data types.",
            param_hint="--interval",
        )
    if not data_type.has_interval_layer and interval is not None:
        raise typer.BadParameter(
            "Option '--interval' is only valid for kline-class data types.",
            param_hint="--interval",
        )


def _matches_output_filter(
    key: str,
    *,
    only_zip: bool,
    only_checksum: bool,
) -> bool:
    """Return whether a listed file should be printed."""
    if only_zip:
        return not key.endswith(".CHECKSUM")
    if only_checksum:
        return key.endswith(".CHECKSUM")
    return True


def _format_relative_path(
    key: str,
    trade_type: TradeType,
    data_freq: DataFrequency,
    data_type: DataType,
) -> str:
    """Format a key relative to the shared archive prefix."""
    prefix = f"data/{trade_type.s3_path}/{data_freq.value}/{data_type.value}/"
    return key.removeprefix(prefix)


def _format_local_relative_path(
    path: Path,
    *,
    archive_home: Path,
    trade_type: TradeType,
    data_freq: DataFrequency,
    data_type: DataType,
) -> str:
    """Format a local archive path relative to the shared archive prefix."""
    prefix = archive_home / "data" / trade_type.s3_path / data_freq.value / data_type.value
    return str(path.relative_to(prefix))


def _warn_if_empty_remote(*, total_remote: int, has_failures: bool) -> None:
    """Print a heuristic warning for empty remote results."""
    if total_remote != 0 or has_failures:
        return

    typer.echo(
        (
            "Warning: no archive files found; check --freq, --type, and trade_type "
            "(for example, futures fundingRate requires --freq monthly)."
        ),
        err=True,
    )


def _warn_if_empty_local_scan(*, total_zips: int) -> None:
    """Print a warning when verify did not find any local zip files."""
    if total_zips != 0:
        return

    typer.echo(
        (
            "Warning: no local zip files found; check --archive-home, --freq, --type, "
            "--interval, and symbols."
        ),
        err=True,
    )


def _resolve_archive_home(ctx: typer.Context) -> Path:
    """Resolve the archive home directory for commands that use local files."""
    override = ctx.obj.get("archive_home_override")

    try:
        return resolve_archive_home(override)
    except ArchiveHomeNotConfiguredError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc


def _print_listing_errors(listing_errors: Sequence[SymbolListingError]) -> None:
    """Print per-symbol listing errors to stderr."""
    for entry in listing_errors:
        logger.error("{}: {}", entry.symbol, entry.error)


def _finalize_download_result(result: DiffResult | DownloadResult) -> None:
    """Emit shared warnings and exit status for download-style commands."""
    _print_listing_errors(result.listing_errors)
    _warn_if_empty_remote(
        total_remote=result.total_remote,
        has_failures=result.listing_failed_symbols > 0,
    )

    if result.listing_failed_symbols > 0:
        raise typer.Exit(code=2)


@app.command("list-files")
def list_files_command(
    trade_type: Annotated[TradeType, typer.Argument(help="Market segment.")],
    symbols: Annotated[list[str] | None, typer.Argument(help="Symbols to list files for.")] = None,
    data_freq: Annotated[
        DataFrequency,
        typer.Option("--freq", help="Partition frequency."),
    ] = DataFrequency.daily,
    data_type: Annotated[
        DataType,
        typer.Option("--type", help="Dataset type."),
    ] = DataType.klines,
    interval: Annotated[
        str | None,
        typer.Option("--interval", help="Interval for kline-class data types."),
    ] = None,
    long_format: Annotated[
        bool,
        typer.Option("-l", "--long", help="Print three-column TSV output."),
    ] = False,
    only_zip: Annotated[
        bool,
        typer.Option("--only-zip", help="Print only .zip files."),
    ] = False,
    only_checksum: Annotated[
        bool,
        typer.Option("--only-checksum", help="Print only .zip.CHECKSUM files."),
    ] = False,
    progress_bar: Annotated[
        bool,
        typer.Option(
            "--progress-bar",
            help=(
                "Show interactive tqdm progress bar. By default no interactive progress is shown."
            ),
        ),
    ] = False,
) -> None:
    """List archive files under one or more symbol directories."""
    if only_zip and only_checksum:
        raise typer.BadParameter(
            "Options '--only-zip' and '--only-checksum' are mutually exclusive.",
            param_hint="--only-zip",
        )

    _validate_interval(data_type, interval)

    resolved_symbols = _resolve_symbols(symbols)
    if not resolved_symbols:
        raise typer.BadParameter("No symbols given.", param_hint="SYMBOLS")

    workflow = ArchiveListFilesWorkflow(
        trade_type=trade_type,
        data_freq=data_freq,
        data_type=data_type,
        symbols=resolved_symbols,
        interval=interval,
        progress_bar=progress_bar,
    )
    result = asyncio.run(workflow.run())

    for entry in result.per_symbol:
        if entry.error is not None:
            logger.error("{}: {}", entry.symbol, entry.error)
            continue

        for archive_file in entry.files:
            if not _matches_output_filter(
                archive_file.key,
                only_zip=only_zip,
                only_checksum=only_checksum,
            ):
                continue

            relative_path = _format_relative_path(
                archive_file.key,
                trade_type=trade_type,
                data_freq=data_freq,
                data_type=data_type,
            )
            if long_format:
                timestamp = archive_file.last_modified.astimezone(UTC).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                typer.echo(f"{archive_file.size}\t{timestamp}\t{relative_path}")
            else:
                typer.echo(relative_path)

    _warn_if_empty_remote(
        total_remote=result.total_remote_files,
        has_failures=result.has_failures,
    )

    if result.has_failures:
        raise typer.Exit(code=2)


@app.command("download")
def download_command(
    ctx: typer.Context,
    trade_type: Annotated[TradeType, typer.Argument(help="Market segment.")],
    symbols: Annotated[list[str] | None, typer.Argument(help="Symbols to download.")] = None,
    data_freq: Annotated[
        DataFrequency,
        typer.Option("--freq", help="Partition frequency."),
    ] = DataFrequency.daily,
    data_type: Annotated[
        DataType,
        typer.Option("--type", help="Dataset type."),
    ] = DataType.klines,
    interval: Annotated[
        str | None,
        typer.Option("--interval", help="Interval for kline-class data types."),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "-n", "--dry-run", help="Show what would be downloaded without writing files."
        ),
    ] = False,
    aria2_proxy: Annotated[
        bool,
        typer.Option(
            "--aria2-proxy",
            help="Allow aria2c to inherit system proxy environment variables.",
        ),
    ] = False,
    progress_bar: Annotated[
        bool,
        typer.Option(
            "--progress-bar",
            help=(
                "Show interactive tqdm progress bar. By default no interactive progress is shown."
            ),
        ),
    ] = False,
) -> None:
    """Download archive files into the local archive directory."""
    _validate_interval(data_type, interval)

    resolved_symbols = _resolve_symbols(symbols)
    if not resolved_symbols:
        raise typer.BadParameter("No symbols given.", param_hint="SYMBOLS")

    archive_home = _resolve_archive_home(ctx)
    workflow = ArchiveDownloadWorkflow(
        trade_type=trade_type,
        data_freq=data_freq,
        data_type=data_type,
        symbols=resolved_symbols,
        archive_home=archive_home,
        interval=interval,
        dry_run=dry_run,
        inherit_aria2_proxy=aria2_proxy,
        progress_bar=progress_bar,
    )
    result = asyncio.run(workflow.run())

    if isinstance(result, DiffResult):
        for entry in result.to_download:
            typer.echo(
                f"{entry.reason}\t{entry.remote.size}\t"
                f"{_format_relative_path(entry.remote.key, trade_type, data_freq, data_type)}"
            )
        _finalize_download_result(result)
        return

    logger.info(
        "download finished: {} downloaded, {} failed, {} skipped",
        result.downloaded,
        result.failed,
        result.skipped,
    )
    _finalize_download_result(result)
    if result.failed > 0:
        raise typer.Exit(code=2)


@app.command("verify")
def verify_command(
    ctx: typer.Context,
    trade_type: Annotated[TradeType, typer.Argument(help="Market segment.")],
    symbols: Annotated[list[str] | None, typer.Argument(help="Symbols to verify.")] = None,
    data_freq: Annotated[
        DataFrequency,
        typer.Option("--freq", help="Partition frequency."),
    ] = DataFrequency.daily,
    data_type: Annotated[
        DataType,
        typer.Option("--type", help="Dataset type."),
    ] = DataType.klines,
    interval: Annotated[
        str | None,
        typer.Option("--interval", help="Interval for kline-class data types."),
    ] = None,
    keep_failed: Annotated[
        bool,
        typer.Option("--keep-failed", help="Keep failed zip and checksum files."),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option("-n", "--dry-run", help="Show what would be verified without writing files."),
    ] = False,
    progress_bar: Annotated[
        bool,
        typer.Option(
            "--progress-bar",
            help=(
                "Show interactive tqdm progress bar. By default no interactive progress is shown."
            ),
        ),
    ] = False,
) -> None:
    """Verify local archive zip files against sibling checksum files."""
    _validate_interval(data_type, interval)

    resolved_symbols = _resolve_symbols(symbols)
    if not resolved_symbols:
        raise typer.BadParameter("No symbols given.", param_hint="SYMBOLS")

    archive_home = _resolve_archive_home(ctx)
    workflow = ArchiveVerifyWorkflow(
        trade_type=trade_type,
        data_freq=data_freq,
        data_type=data_type,
        symbols=resolved_symbols,
        archive_home=archive_home,
        interval=interval,
        keep_failed=keep_failed,
        dry_run=dry_run,
        progress_bar=progress_bar,
    )
    result = workflow.run()

    if isinstance(result, VerifyDiffResult):
        for zip_path in result.to_verify:
            typer.echo(
                _format_local_relative_path(
                    zip_path,
                    archive_home=archive_home,
                    trade_type=trade_type,
                    data_freq=data_freq,
                    data_type=data_type,
                )
            )
        typer.echo(
            (
                f"{len(result.to_verify)} to verify, {result.skipped} up to date, "
                f"{len(result.orphan_zips)} orphan zip, "
                f"{len(result.orphan_checksums)} orphan checksum"
            ),
            err=True,
        )
        _warn_if_empty_local_scan(total_zips=result.total_zips)
        return

    assert isinstance(result, VerifyResult)
    for path, detail in result.failed_details.items():
        logger.error("{}: {}", path.name, detail)

    typer.echo(
        f"Done: {result.verified} verified, {result.failed} failed, {result.skipped} skipped",
        err=True,
    )
    if result.orphan_zips > 0 or result.orphan_checksums > 0:
        typer.echo(
            (
                f"Cleaned {result.orphan_zips} orphan zip markers, "
                f"deleted {result.orphan_checksums} orphan checksums"
            ),
            err=True,
        )
    if keep_failed and result.failed > 0:
        typer.echo("Failed files were kept because --keep-failed is enabled.", err=True)

    _warn_if_empty_local_scan(total_zips=result.total_zips)


@app.command("gap-fill")
def gap_fill_command(
    trade_type: Annotated[TradeType, typer.Argument(help="Market segment.")],
    data_type: Annotated[
        DataType,
        typer.Option("--type", help="Dataset type to fill."),
    ] = DataType.klines,
    symbol: Annotated[
        str, typer.Option("--symbol", help="Trading symbol (e.g. BTCUSDT).")
    ] = "BTCUSDT",
    interval: Annotated[
        str | None,
        typer.Option("--interval", help="Kline interval (required for klines)."),
    ] = None,
    start_time: Annotated[
        int | None,
        typer.Option("--start-time", help="Start time in ms."),
    ] = None,
    end_time: Annotated[
        int | None,
        typer.Option("--end-time", help="End time in ms."),
    ] = None,
    auto_detect: Annotated[
        bool,
        typer.Option("--auto-detect", help="Auto-detect gaps from local archive."),
    ] = False,
    lookback: Annotated[
        int,
        typer.Option("--lookback", help="Days to look back for gap detection."),
    ] = 30,
    archive_home_path: Annotated[
        str | None,
        typer.Option("--archive-home", help="Override archive home."),
    ] = None,
) -> None:
    """Fill archive data gaps via REST API.

    Uses the Binance REST API (via official SDK) to fill gaps in archive data
    for klines, aggregated trades, or funding rate data.
    """
    archive_home = resolve_archive_home(archive_home_path)

    if data_type.has_interval_layer and interval is None:
        typer.echo("Error: --interval is required for kline-class data types", err=True)
        raise typer.Exit(code=2)
    if not data_type.has_interval_layer and interval is not None:
        typer.echo("Error: --interval is not applicable for non-kline data types", err=True)
        raise typer.Exit(code=2)

    n_gaps = gap_fill_flow(
        trade_type=trade_type.value,
        symbol=symbol,
        data_type=data_type.value,
        interval=interval,
        lookback_days=lookback,
        archive_home=archive_home,
    )

    typer.echo(f"Gaps detected: {n_gaps}", err=True)
    if n_gaps == 0:
        return


@app.command("health")
def health_command(
    trade_type: Annotated[TradeType, typer.Argument(help="Market segment.")],
    symbols: Annotated[list[str] | None, typer.Argument(help="Symbols to check.")] = None,
    data_type: Annotated[
        DataType,
        typer.Option("--type", help="Dataset type to check."),
    ] = DataType.klines,
    interval: Annotated[
        str | None,
        typer.Option("--interval", help="Interval for kline-class data types."),
    ] = None,
    max_stale: Annotated[
        int,
        typer.Option("--max-stale", help="Max days since latest data before warning."),
    ] = 3,
    archive_home_path: Annotated[
        str | None,
        typer.Option("--archive-home", help="Override archive home."),
    ] = None,
    ducklake: Annotated[
        bool,
        typer.Option("--ducklake", help="Check DuckLake silver tables instead of archive files."),
    ] = True,
) -> None:
    """Check health of market data.

    By default checks DuckLake silver tables (anomaly detection).
    Pass ``--no-ducklake`` to check archive file integrity instead.
    """
    archive_home = resolve_archive_home(archive_home_path)

    resolved_symbols = symbols or []
    if not resolved_symbols:
        typer.echo("Error: At least one SYMBOL argument required.", err=True)
        raise typer.Exit(code=2)

    if ducklake:
        # DuckLake anomaly detection (new stack)
        catalog = archive_home.parent / "lake"
        for sym in resolved_symbols:
            try:
                h = health_flow(
                    trade_type=trade_type.value,
                    symbol=sym,
                    data_type=data_type.value,
                    interval=interval,
                    archive_home=archive_home,
                    catalog_path=catalog,
                )
                status = "HEALTHY" if h.get("healthy", False) else "ISSUES"
                typer.echo(
                    f"{sym}: {status} "
                    f"(null_prices: {h.get('null_prices', 'N/A')}, "
                    f"missing_dates: {h.get('missing_dates', 'N/A')})"
                )
            except Exception as exc:
                typer.echo(f"{sym}: ERROR — {exc}", err=True)
    else:
        # Archive file integrity check (legacy)
        from binance_datatool.common import DataFrequency

        workflow = HealthCheckWorkflow(
            trade_type=trade_type,
            data_freq=DataFrequency.daily,
            data_type=data_type,
            symbols=resolved_symbols,
            archive_home=archive_home,
            interval=interval,
            max_stale_days=max_stale,
        )
        report = workflow.run()
        for health in report.per_symbol:
            status = "HEALTHY" if health.is_healthy else "ISSUES"
            typer.echo(
                f"{health.symbol}: {status} "
                f"(dates: {health.date_count}, "
                f"missing: {len(health.missing_dates)}, "
                f"corrupted: {len(health.corrupted_files)}, "
                f"latest: {health.latest_date or 'N/A'})"
            )
        if report.errors:
            for err in report.errors:
                typer.echo(f"Error: {err}", err=True)


@app.command("sink")
def sink_command(
    trade_type: Annotated[TradeType, typer.Argument(help="Market segment.")],
    symbols: Annotated[list[str] | None, typer.Argument(help="Symbols to transform.")] = None,
    data_type: Annotated[
        DataType,
        typer.Option("--type", help="Dataset type."),
    ] = DataType.klines,
    interval: Annotated[
        str | None,
        typer.Option("--interval", help="Kline interval."),
    ] = None,
    target: Annotated[
        str,
        typer.Option("--target", help="Sink target: parquet, duckdb, or all."),
    ] = "parquet",
    duckdb_path: Annotated[
        str | None,
        typer.Option("--duckdb", help="DuckDB file path (default: :memory:)."),
    ] = None,
    catalog_path: Annotated[
        str | None,
        typer.Option("--catalog", help="Parquet catalog directory."),
    ] = None,
    archive_home_path: Annotated[
        str | None,
        typer.Option("--archive-home", help="Override archive home."),
    ] = None,
) -> None:
    """Transform archive data to Parquet and load into DuckDB.

    Reads raw archive data (ZIP CSVs and filled CSVs), normalizes schemas,
    writes partitioned Parquet files, and optionally loads into DuckDB.
    """
    archive_home = resolve_archive_home(archive_home_path)

    resolved_symbols = symbols or []
    if not resolved_symbols:
        typer.echo("Error: At least one SYMBOL argument required.", err=True)
        raise typer.Exit(code=2)

    total_rows = sink_flow(
        trade_type=trade_type.value,
        symbols=resolved_symbols,
        data_type=data_type.value,
        interval=interval,
        archive_home=archive_home,
        catalog_path=Path(catalog_path) if catalog_path else None,
    )

    typer.echo(f"Transformed {len(resolved_symbols)} symbols, {total_rows} rows", err=True)


@app.command("refresh-metadata")
def refresh_metadata_command(
    trade_type: Annotated[TradeType, typer.Argument(help="Market segment.")],
    data_freq: Annotated[
        DataFrequency,
        typer.Option("--freq", help="Partition frequency."),
    ] = DataFrequency.daily,
    data_type: Annotated[
        DataType,
        typer.Option("--type", help="Dataset type."),
    ] = DataType.klines,
    from_api: Annotated[
        bool,
        typer.Option("--from-api", help="Fetch symbols from REST API instead of archive."),
    ] = False,
    duckdb_path: Annotated[
        str | None,
        typer.Option("--duckdb", help="Also register in DuckLake catalog (path to .duckdb)."),
    ] = None,
    catalog_path: Annotated[
        str | None,
        typer.Option("--catalog", help="Output catalog directory."),
    ] = None,
    archive_home_path: Annotated[
        str | None,
        typer.Option("--archive-home", help="Override archive home."),
    ] = None,
) -> None:
    """Refresh venue and symbol metadata tables.

    Lists symbols from the Binance archive (or REST API), saves as Parquet,
    and optionally registers in a DuckLake catalog as native tables.
    """

    archive_home = resolve_archive_home(archive_home_path)
    catalog = Path(catalog_path) if catalog_path else archive_home.parent / "lake"

    n_syms = refresh_metadata_flow(
        trade_type=trade_type.value,
        catalog_path=catalog,
        from_api=from_api,
        duckdb_path=duckdb_path or str(catalog / "catalog.duckdb"),
    )
    typer.echo(f"Saved {n_syms} symbols for {trade_type.value}", err=True)


@app.command("universe-maintenance")
def universe_maintenance_command(
    target_date: Annotated[
        str | None,
        typer.Option("--date", help="Target date for gold stats (YYYY-MM-DD)."),
    ] = None,
    lookback: Annotated[
        int,
        typer.Option("--lookback", help="Days to look back for catch-up/backfill."),
    ] = 1,
    catalog_path: Annotated[
        str | None,
        typer.Option("--catalog", help="DuckLake catalog directory."),
    ] = None,
    archive_home_path: Annotated[
        str | None,
        typer.Option("--archive-home", help="Override archive home."),
    ] = None,
) -> None:
    """Maintain historical universe statistics in the Gold layer.

    Synchronizes exchange metadata and builds daily point-in-time statistics
    required for accurate, zero-survivorship-bias backtesting.
    """
    archive_home = resolve_archive_home(archive_home_path)
    catalog = catalog_path or str(archive_home.parent / "lake")

    universe_maintenance_flow(
        lake_path=catalog,
        target_date=target_date,
        lookback_days=lookback,
    )
    typer.echo("Universe maintenance complete.", err=True)


@app.command("build-dataset")
def build_dataset_command(
    trade_types: Annotated[
        list[str] | None,
        typer.Option("--trade-type", help="Trade types (spot, um, cm)."),
    ] = None,
    data_types: Annotated[
        list[str] | None,
        typer.Option("--data-type", help="Data types (klines, aggTrades)."),
    ] = None,
    lookback: Annotated[
        int,
        typer.Option("--lookback", help="Days of history to ingest."),
    ] = 30,
    catalog_path: Annotated[
        str | None,
        typer.Option("--catalog", help="DuckLake catalog directory."),
    ] = None,
    archive_home_path: Annotated[
        str | None,
        typer.Option("--archive-home", help="Override archive home."),
    ] = None,
) -> None:
    """Build a production-ready backtesting dataset for the Top 50 universe.

    Orchestrates the full lifecycle:
    1. Instrument discovery & Gold layer statistics maintenance.
    2. Top 50 Tradable Universe construction for each trade type.
    3. Bulk ingestion and transformation of essential data types.
    4. Health and consistency validation.
    """

    archive_home = resolve_archive_home(archive_home_path)
    catalog = catalog_path or str(archive_home.parent / "lake")

    report = backtesting_data_product_flow(
        trade_types=trade_types,
        data_types=data_types,
        lookback_days=lookback,
        lake_path=catalog,
    )

    typer.echo("--- Backtesting Data Product Report ---", err=True)
    for tt, tt_data in report.items():
        typer.echo(f"Venue: {tt.upper()}")
        typer.echo(f"  Symbols in Universe: {tt_data['symbols_count']}")
        for dt, results in tt_data["results"].items():
            healthy_count = sum(1 for r in results.values() if r.get("healthy"))
            typer.echo(f"  Data Type: {dt} -> {healthy_count}/{len(results)} healthy")
    typer.echo("Dataset build complete.", err=True)
