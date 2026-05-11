"""dlt resource for Binance archive (data.binance.vision S3).

Fetches klines directly from the live ``data.binance.vision`` S3 archive using
the existing ``ArchiveClient`` — no pre-downloaded local files needed.

For each symbol, the resource:
1. Lists available S3 files via ``ArchiveClient.list_symbol_files()``
2. Downloads ZIP content via aiohttp
3. Parses the CSV inside each ZIP
4. Yields rows with merge-on-primary-key for idempotent loading
"""

from __future__ import annotations

import asyncio
import io
import zipfile
from typing import TYPE_CHECKING

import dlt

from binance_datatool.archive.client import ArchiveClient
from binance_datatool.common.constants import S3_DOWNLOAD_PREFIX
from binance_datatool.common.enums import DataFrequency, DataType, TradeType

if TYPE_CHECKING:
    from collections.abc import Iterator


@dlt.resource(
    name="archive_klines",
    write_disposition="merge",
    primary_key=("symbol", "interval", "open_time"),
    columns={
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
)
def archive_klines_resource(
    symbol: str,
    interval: str = "1h",
    trade_type: TradeType = TradeType.spot,
    lookback_days: int | None = None,
) -> Iterator[list[dict]]:
    """Fetch klines from the Binance S3 archive (data.binance.vision).

    Lists available S3 files for the symbol/interval, downloads ZIPs,
    parses CSV contents, and yields rows. No local download required.

    Args:
        symbol: Trading pair.
        interval: Kline interval.
        trade_type: Market type.
        lookback_days: If set, only process files newer than N days.

    Yields:
        Lists of kline dicts per ZIP file.
    """
    import aiohttp

    client = ArchiveClient()
    freq = DataFrequency.monthly if interval in ("1w", "1M") else DataFrequency.daily

    files = asyncio.run(
        client.list_symbol_files(
            trade_type=trade_type,
            data_freq=freq,
            data_type=DataType.klines,
            symbol=symbol,
            interval=interval,
        )
    )
    if not files:
        return

    if lookback_days is not None:
        from datetime import UTC, datetime, timedelta

        _days = int(lookback_days)
        cutoff = datetime.now(UTC) - timedelta(days=_days)
        files = [f for f in files if f.last_modified.replace(tzinfo=UTC) > cutoff]

    async def _fetch_and_parse(url: str) -> list[dict]:
        async with (
            aiohttp.ClientSession() as session,
            session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp,
        ):
            raw = await resp.read()
        rows: list[dict] = []
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            csv_name = [n for n in zf.namelist() if n.endswith(".csv")][0]
            for line in zf.read(csv_name).decode().strip().split("\n"):
                parts = line.split(",")
                if len(parts) < 11:
                    continue
                rows.append(
                    {
                        "open_time": int(parts[0]),
                        "open": float(parts[1]) if parts[1] else 0.0,
                        "high": float(parts[2]) if parts[2] else 0.0,
                        "low": float(parts[3]) if parts[3] else 0.0,
                        "close": float(parts[4]) if parts[4] else 0.0,
                        "volume": float(parts[5]) if parts[5] else 0.0,
                        "close_time": int(parts[6]) if parts[6] else 0,
                        "quote_volume": float(parts[7]) if parts[7] else 0.0,
                        "count": int(parts[8]) if parts[8] else 0,
                        "taker_buy_volume": float(parts[9]) if parts[9] else 0.0,
                        "taker_buy_quote_volume": float(parts[10]) if parts[10] else 0.0,
                        "symbol": symbol,
                        "interval": interval,
                    }
                )
        return rows

    for f in files:
        url = S3_DOWNLOAD_PREFIX + f.key
        rows = asyncio.run(_fetch_and_parse(url))
        if rows:
            yield rows


@dlt.source
def build_archive_source(
    symbols: list[str],
    interval: str = "1h",
    trade_type: TradeType = TradeType.spot,
    lookback_days: int | None = None,
) -> list[dlt.Resource]:
    """Build a dlt source for Binance S3 archive data.

    One resource per symbol, each fetching directly from
    ``data.binance.vision``.

    Args:
        symbols: Trading symbols.
        interval: Kline interval.
        trade_type: Market type.
        lookback_days: Only process recent files.

    Returns:
        List of dlt Resources (one per symbol).
    """
    return [
        archive_klines_resource(
            symbol=sym, interval=interval, trade_type=trade_type, lookback_days=lookback_days
        ).with_name(f"archive_{sym}_klines")
        for sym in symbols
    ]
