"""dlt resource for Binance archive (data.binance.vision S3).

Canonical location. Previously at ``dlt_sources.binance_archive``.
"""

from __future__ import annotations

import asyncio
import csv
import io
import zipfile
from typing import Any

import dlt  # noqa: TC002 — our dlt package shadows the module name

from binance_datatool.common.async_utils import sync_run
from binance_datatool.common.constants import S3_DOWNLOAD_PREFIX

_KLINES_TYPES = {"klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"}

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
    "indexPriceKlines": [
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
    "markPriceKlines": [
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
    "premiumIndexKlines": [
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
        "is_buyer_maker",
        "is_best_match",
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
    "bookDepth": [
        "timestamp",
        "percentage",
        "depth",
        "notional",
    ],
    "metrics": [
        "create_time",
        "symbol",
        "sum_open_interest",
        "sum_open_interest_value",
        "count_toptrader_long_short_ratio",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    ],
}

_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "klines": ("symbol", "interval", "open_time"),
    "indexPriceKlines": ("symbol", "interval", "open_time"),
    "markPriceKlines": ("symbol", "interval", "open_time"),
    "premiumIndexKlines": ("symbol", "interval", "open_time"),
    "aggTrades": ("symbol", "agg_trade_id"),
    "trades": ("symbol", "trade_id"),
    "fundingRate": ("symbol", "funding_time"),
    "bookDepth": ("symbol", "timestamp"),
    "metrics": ("symbol", "create_time"),
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
        "first_trade_id": {"data_type": "bigint", "nullable": True},
        "last_trade_id": {"data_type": "bigint", "nullable": True},
        "transact_time": {"data_type": "bigint", "nullable": False},
        "is_buyer_maker": {"data_type": "text", "nullable": False},
        "is_best_match": {"data_type": "text", "nullable": True},
        "symbol": {"data_type": "text", "nullable": False},
    },
    "trades": {
        "trade_id": {"data_type": "bigint", "nullable": False},
        "price": {"data_type": "double", "nullable": False},
        "quantity": {"data_type": "double", "nullable": False},
        "quote_quantity": {"data_type": "double", "nullable": True},
        "transact_time": {"data_type": "bigint", "nullable": False},
        "is_buyer_maker": {"data_type": "bool", "nullable": False},
        "is_best_match": {"data_type": "bool", "nullable": True},
        "symbol": {"data_type": "text", "nullable": False},
    },
    "fundingRate": {
        "funding_time": {"data_type": "bigint", "nullable": False},
        "funding_rate": {"data_type": "double", "nullable": False},
        "mark_price": {"data_type": "double", "nullable": True},
        "funding_interval_hours": {"data_type": "bigint", "nullable": True},
        "symbol": {"data_type": "text", "nullable": False},
    },
    "bookDepth": {
        "timestamp": {"data_type": "text", "nullable": False},
        "percentage": {"data_type": "double", "nullable": False},
        "depth": {"data_type": "double", "nullable": False},
        "notional": {"data_type": "double", "nullable": False},
        "symbol": {"data_type": "text", "nullable": False},
    },
    "metrics": {
        "create_time": {"data_type": "text", "nullable": False},
        "sum_open_interest": {"data_type": "double", "nullable": False},
        "sum_open_interest_value": {"data_type": "double", "nullable": False},
        "count_toptrader_long_short_ratio": {"data_type": "double", "nullable": True},
        "sum_toptrader_long_short_ratio": {"data_type": "double", "nullable": True},
        "count_long_short_ratio": {"data_type": "double", "nullable": True},
        "sum_taker_long_short_vol_ratio": {"data_type": "double", "nullable": True},
        "symbol": {"data_type": "text", "nullable": False},
    },
}

_TABLE_MAP: dict[str, str] = {
    "klines": "klines",
    "indexPriceKlines": "klines",
    "markPriceKlines": "klines",
    "premiumIndexKlines": "klines",
    "aggTrades": "agg_trades",
    "trades": "trades",
    "fundingRate": "funding_rate",
    "bookDepth": "book_depth",
    "metrics": "metrics",
}


def _has_header(first_row_parts: list[str]) -> bool:
    """Detect whether the first CSV row is a header by checking cell type."""
    if not first_row_parts:
        return False
    try:
        int(first_row_parts[0].strip())
        return False
    except ValueError:
        return True


def _parse_csv_rows(
    text: str, data_type: str, symbol: str, interval: str | None
) -> list[dict[str, Any]]:
    reader = csv.reader(io.StringIO(text))
    cols = _BRONZE_COLS[data_type]
    rows: list[dict[str, Any]] = []
    skip_header: bool | None = None
    for parts in reader:
        if skip_header is None:
            skip_header = _has_header(parts)
            if skip_header:
                continue
        if len(parts) < len(cols) - 1:
            continue
        row: dict[str, Any] = {"symbol": symbol}
        if interval is not None and data_type in _KLINES_TYPES:
            row["interval"] = interval
        for i, col in enumerate(cols):
            if i >= len(parts):
                continue
            raw = parts[i].strip()
            if not raw:
                continue
            col_type = (
                (_DATA_TYPE_COLUMNS.get(data_type) or _DATA_TYPE_COLUMNS["klines"])
                .get(col, {})
                .get("data_type", "text")
            )
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


def archive_data_resource(
    symbol: str,
    s3_keys: list[str],
    interval: str | None = None,
    data_type: str = "klines",
    use_s5cmd: bool = False,
    s5cmd_concurrency: int = 10,
) -> dlt.Resource:
    """Build a dlt resource for one symbol from pre-resolved S3 file keys.

    Pure EL: no S3 listing, no caching. Accepts file keys (resolved
    by Prefect via ``ArchiveFileCache`` or ``ArchiveClient``).

    Args:
        symbol: Trading pair.
        s3_keys: S3 file keys to download and parse.
        interval: Kline interval (required for ``"klines"``).
        data_type: One of ``"klines"``, ``"aggTrades"``, ``"trades"``,
            ``"fundingRate"``.
        use_s5cmd: Use s5cmd for parallel batch downloads instead of
            aiohttp. Requires s5cmd installed. Faster for many files.
        s5cmd_concurrency: Number of parallel s5cmd connections.

    Returns:
        Configured ``dlt.Resource``.
    """
    _table = _TABLE_MAP.get(data_type, data_type.replace("-", "_"))

    if use_s5cmd:
        from binance_datatool.archive.s5cmd_download import download_and_parse

        def _gen() -> list[list[dict[str, Any]]]:
            return download_and_parse(
                s3_keys,
                symbol=symbol,
                data_type=data_type,
                interval=interval,
                concurrency=s5cmd_concurrency,
            )

    else:

        def _gen() -> list[list[dict[str, Any]]]:
            if not s3_keys:
                return []

            async def _fetch(key: str) -> str:
                import aiohttp

                url = f"{S3_DOWNLOAD_PREFIX}/{key}"
                async with (
                    aiohttp.ClientSession() as session,
                    session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp,
                ):
                    raw = await resp.read()
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
                    return zf.read(csv_name).decode()

            async def _fetch_all() -> list[tuple[int, list[dict[str, Any]] | None]]:
                async def _one(idx: int, key: str) -> tuple[int, list[dict[str, Any]] | None]:
                    try:
                        text = await _fetch(key)
                        rows = _parse_csv_rows(text, data_type, symbol, interval)
                        return idx, rows if rows else None
                    except Exception:
                        return idx, None

                tasks = [_one(i, k) for i, k in enumerate(s3_keys)]
                return await asyncio.gather(*tasks)

            gathered = sync_run(_fetch_all())
            return [r for _, r in sorted(gathered) if r is not None]

    return dlt.resource(
        _gen,
        name=f"archive_{data_type}",
        table_name=_table,
        write_disposition="merge",
        primary_key=_PRIMARY_KEYS[data_type],
        columns=_DATA_TYPE_COLUMNS.get(data_type) or _DATA_TYPE_COLUMNS["klines"],
        schema_contract={"columns": "freeze", "data_type": "freeze"},
    )
