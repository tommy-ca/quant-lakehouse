"""dlt resource for Binance archive (data.binance.vision S3).

Fetches market data directly from the live S3 archive — no pre-downloaded
local files needed. Supports all data types matching the original CLI:
klines, aggTrades, trades, fundingRate.

Per-symbol flow:
1. List available S3 files via ``ArchiveClient.list_symbol_files()``
2. Download ZIP content via aiohttp
3. Parse CSV inside each ZIP (accounting for data-type-specific format)
4. Yield rows with merge-on-primary-key for idempotent loading
"""

from __future__ import annotations

import asyncio
import csv
import io
import zipfile
from typing import Any

import dlt

from binance_datatool.archive.client import ArchiveClient
from binance_datatool.common.constants import S3_DOWNLOAD_PREFIX
from binance_datatool.common.enums import DataFrequency, DataType, TradeType

# ── Data-type-specific CSV column counts and parsers ─────────────

_BRONZE_COLS: dict[str, list[str]] = {
    "klines": [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "count",
        "taker_buy_volume",
        "taker_buy_quote_volume",
    ],
    "aggTrades": [
        "agg_trade_id",
        "price",
        "quantity",
        "first_trade_id",
        "last_trade_id",
        "transact_time",
    ],
    "trades": [
        "trade_id",
        "price",
        "quantity",
        "quote_quantity",
        "transact_time",
        "is_buyer_maker",
        "is_best_match",
    ],
    "fundingRate": [
        "funding_time",
        "funding_rate",
        "mark_price",
        "funding_interval_hours",
    ],
}

_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "klines": ("symbol", "interval", "open_time"),
    "aggTrades": ("symbol", "agg_trade_id"),
    "trades": ("symbol", "trade_id"),
    "fundingRate": ("symbol", "funding_time"),
}

_DATA_TYPE_COLUMNS: dict[str, dict[str, dict[str, Any]]] = {
    "klines": {
        "open_time": {"data_type": "bigint", "nullable": False},
        "open": {"data_type": "double", "nullable": False},
        "high": {"data_type": "double", "nullable": False},
        "low": {"data_type": "double", "nullable": False},
        "close": {"data_type": "double", "nullable": False},
        "volume": {"data_type": "double", "nullable": False},
        "close_time": {"data_type": "bigint", "nullable": False},
        "quote_volume": {"data_type": "double", "nullable": False},
        "count": {"data_type": "bigint", "nullable": False},
        "taker_buy_volume": {"data_type": "double", "nullable": False},
        "taker_buy_quote_volume": {"data_type": "double", "nullable": False},
        "symbol": {"data_type": "text", "nullable": False},
        "interval": {"data_type": "text", "nullable": False},
    },
    "aggTrades": {
        "agg_trade_id": {"data_type": "bigint", "nullable": False},
        "price": {"data_type": "double", "nullable": False},
        "quantity": {"data_type": "double", "nullable": False},
        "transact_time": {"data_type": "bigint", "nullable": False},
        "symbol": {"data_type": "text", "nullable": False},
    },
    "trades": {
        "trade_id": {"data_type": "bigint", "nullable": False},
        "price": {"data_type": "double", "nullable": False},
        "quantity": {"data_type": "double", "nullable": False},
        "transact_time": {"data_type": "bigint", "nullable": False},
        "is_buyer_maker": {"data_type": "bool", "nullable": False},
        "symbol": {"data_type": "text", "nullable": False},
    },
    "fundingRate": {
        "funding_time": {"data_type": "bigint", "nullable": False},
        "funding_rate": {"data_type": "double", "nullable": False},
        "symbol": {"data_type": "text", "nullable": False},
    },
}


def _data_freq_for(data_type: str, interval: str | None) -> DataFrequency:
    """Pick monthly for fundingRate or weekly/monthly klines; daily otherwise."""
    if data_type == "fundingRate":
        return DataFrequency.monthly
    if interval in ("1w", "1M"):
        return DataFrequency.monthly
    return DataFrequency.daily


def _has_header(data_type: str) -> bool:
    """Funding rate CSVs have a header row that must be skipped."""
    return data_type in ("fundingRate",)


def _parse_csv_rows(
    text: str, data_type: str, symbol: str, interval: str | None
) -> list[dict[str, Any]]:
    """Parse CSV text into a list of dicts based on data type schema."""
    reader = csv.reader(io.StringIO(text))
    cols = _BRONZE_COLS[data_type]
    rows: list[dict[str, Any]] = []

    for line_no, parts in enumerate(reader):
        if _has_header(data_type) and line_no == 0:
            continue
        if len(parts) < len(cols):
            continue

        row: dict[str, Any] = {"symbol": symbol}
        if interval is not None and data_type == "klines":
            row["interval"] = interval

        for i, col in enumerate(cols):
            raw = parts[i].strip()
            if not raw:
                continue
            col_type = _DATA_TYPE_COLUMNS[data_type].get(col, {}).get("data_type", "text")
            if col_type == "bigint":
                row[col] = int(raw)
            elif col_type == "double":
                row[col] = float(raw)
            elif col_type == "bool":
                row[col] = raw.lower() == "true"
            else:
                row[col] = raw

        rows.append(row)
    return rows


# ── dlt resources ────────────────────────────────────────────────


def _build_resource_kwargs(
    data_type: str,
) -> dict[str, Any]:
    """Build @dlt.resource kwargs for a given data type."""
    return {
        "name": f"archive_{data_type}",
        "write_disposition": "merge",
        "primary_key": _PRIMARY_KEYS[data_type],
        "columns": _DATA_TYPE_COLUMNS[data_type],
    }


def archive_data_resource(
    symbol: str,
    interval: str | None = None,
    trade_type: TradeType = TradeType.spot,
    data_type: str = "klines",
    lookback_days: int | None = None,
    catalog_path: str | None = None,
) -> dlt.Resource:
    """Build a dlt resource for one symbol from the Binance S3 archive.

    Uses a DuckDB-backed file cache (``ArchiveFileCache``) when
    ``catalog_path`` is provided — subsequent runs skip the S3 listing.

    Args:
        symbol: Trading pair.
        interval: Kline interval (required for ``"klines"``).
        trade_type: Market type.
        data_type: One of ``"klines"``, ``"aggTrades"``, ``"trades"``, ``"fundingRate"``.
        lookback_days: Only process files newer than N days.
        catalog_path: Path to ``catalog.duckdb`` for cache. When set, uses
            cached file listings instead of S3 on subsequent runs.

    Returns:
        Configured ``dlt.Resource``.
    """

    def _gen() -> list[list[dict[str, Any]]]:
        import aiohttp

        freq = _data_freq_for(data_type, interval)
        freq_str = freq.value
        iv = interval if data_type == "klines" else None

        # ── Resolve file list (cache or S3) ───────────────────────
        if catalog_path:
            from binance_datatool.workflow.archive_cache import ArchiveFileCache

            cache = ArchiveFileCache(catalog_path)
            cache.ensure_table()
            if cache.is_fresh(symbol, data_type, iv, trade_type.value, freq_str):
                files = cache.list_cached(symbol, data_type, iv, trade_type.value, freq_str)
            else:
                cache.refresh(symbol, data_type, iv, trade_type.value, freq_str)
                files = cache.list_cached(symbol, data_type, iv, trade_type.value, freq_str)
        else:
            client = ArchiveClient()
            dt_enum = DataType(data_type)
            files_raw = asyncio.run(
                client.list_symbol_files(
                    trade_type=trade_type,
                    data_freq=freq,
                    data_type=dt_enum,
                    symbol=symbol,
                    interval=iv,
                )
            )
            files = [
                {"key": f.key, "size": f.size, "last_modified": f.last_modified} for f in files_raw
            ]

        if not files:
            return []

        if lookback_days is not None:
            from datetime import UTC, datetime, timedelta

            _days = int(lookback_days)
            cutoff = datetime.now(UTC) - timedelta(days=_days)
            files = [
                f
                for f in files
                if (f.get("last_modified") and f["last_modified"].replace(tzinfo=UTC) > cutoff)
            ]

        async def _fetch(url: str) -> str:
            async with (
                aiohttp.ClientSession() as session,
                session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp,
            ):
                raw = await resp.read()
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
                return zf.read(csv_name).decode()

        results: list[list[dict[str, Any]]] = []
        for f in files:
            url = S3_DOWNLOAD_PREFIX + f["key"]
            try:
                text = asyncio.run(_fetch(url))
                rows = _parse_csv_rows(text, data_type, symbol, interval)
                if rows:
                    results.append(rows)
            except Exception:
                continue
        return results

    return dlt.resource(
        _gen,
        **_build_resource_kwargs(data_type),
    )


# ── Source builder ───────────────────────────────────────────────


@dlt.source
def build_archive_source(
    symbols: list[str],
    interval: str | None = None,
    trade_type: TradeType = TradeType.spot,
    data_type: str = "klines",
    lookback_days: int | None = None,
) -> list[dlt.Resource]:
    """Build a dlt source for Binance S3 archive data.

    Args:
        symbols: Trading symbols.
        interval: Kline interval (required for ``"klines"``).
        trade_type: Market type.
        data_type: One of ``"klines"``, ``"aggTrades"``, ``"trades"``, ``"fundingRate"``.
        lookback_days: Only process recent files.

    Returns:
        A list of dlt Resources (one per symbol).
    """
    _name = f"archive_{data_type}"
    return [
        archive_data_resource(
            symbol=sym,
            interval=interval,
            trade_type=trade_type,
            data_type=data_type,
            lookback_days=lookback_days,
        ).with_name(f"{_name}_{sym}")
        for sym in symbols
    ]


# ── List-files resource (discovery, no download) ─────────────────


def archive_list_files_resource(
    symbol: str,
    interval: str | None = None,
    trade_type: TradeType = TradeType.spot,
    data_type: str = "klines",
) -> dlt.Resource:
    """List available archive files for a symbol without downloading.

    Yields file metadata (key, size, last_modified) for discovery and
    planning. The dlt equivalent of ``ArchiveListFilesWorkflow``.
    """

    def _gen() -> list[dict[str, Any]]:
        client = ArchiveClient()
        freq = _data_freq_for(data_type, interval)
        dt_enum = DataType(data_type)
        iv = interval if data_type == "klines" else None

        files = asyncio.run(
            client.list_symbol_files(
                trade_type=trade_type,
                data_freq=freq,
                data_type=dt_enum,
                symbol=symbol,
                interval=iv,
            )
        )
        return [
            {
                "symbol": symbol,
                "data_type": data_type,
                "key": f.key,
                "size": f.size,
                "last_modified": f.last_modified.isoformat() if f.last_modified else None,
            }
            for f in files
        ]

    return dlt.resource(
        _gen,
        name=f"archive_files_{symbol}",
        write_disposition="replace",
    )
