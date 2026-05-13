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

from binance_datatool.common.constants import S3_DOWNLOAD_PREFIX

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
        "transact_time": {"data_type": "bigint", "nullable": False},
        "is_buyer_maker": {"data_type": "bool", "nullable": False},
        "symbol": {"data_type": "text", "nullable": False},
    },
    "fundingRate": {
        "funding_time": {"data_type": "bigint", "nullable": False},
        "funding_rate": {"data_type": "double", "nullable": False},
        "mark_price": {"data_type": "double", "nullable": True},
        "funding_interval_hours": {"data_type": "bigint", "nullable": True},
        "symbol": {"data_type": "text", "nullable": False},
    },
}

_TABLE_MAP: dict[str, str] = {
    "klines": "klines",
    "aggTrades": "agg_trades",
    "trades": "trades",
    "fundingRate": "funding_rate",
}


def _has_header(data_type: str) -> bool:
    return data_type in ("fundingRate",)


def _parse_csv_rows(
    text: str, data_type: str, symbol: str, interval: str | None
) -> list[dict[str, Any]]:
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


def archive_data_resource(
    symbol: str,
    s3_keys: list[str],
    interval: str | None = None,
    data_type: str = "klines",
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

    Returns:
        Configured ``dlt.Resource``.
    """
    _table = _TABLE_MAP.get(data_type, data_type.replace("-", "_"))

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

        gathered = asyncio.run(_fetch_all())
        return [r for _, r in sorted(gathered) if r is not None]

    return dlt.resource(
        _gen,
        name=f"archive_{data_type}",
        table_name=_table,
        write_disposition="merge",
        primary_key=_PRIMARY_KEYS[data_type],
        columns=_DATA_TYPE_COLUMNS[data_type],
        schema_contract={"columns": "freeze", "data_type": "freeze"},
    )
