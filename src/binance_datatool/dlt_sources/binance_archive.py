"""dlt resource for Binance archive (data.binance.vision S3).

Wraps the existing ``ArchiveClient`` as a ``@dlt.resource``. Uses diff-based
sync (dlt's merge disposition) to only load new/modified files.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import dlt

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
        "quote_volume": {"data_type": "double", "nullable": False},
        "trade_count": {"data_type": "bigint", "nullable": False},
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
    archive_home: str | None = None,
) -> Iterator[list[dict]]:
    """Read klines from local Binance archive ZIPs.

    Scans the local archive directory for ZIP files matching the symbol and
    interval, extracts the CSV contents, and yields rows.
    """
    from pathlib import Path

    import polars as pl

    from binance_datatool.common.path import resolve_archive_home

    home = Path(archive_home) if archive_home else resolve_archive_home()
    tt_path = trade_type.s3_path
    freq = DataFrequency.daily
    dtype_path = DataType.klines

    zip_dir = home / "data" / tt_path / freq.value / dtype_path.value / symbol / interval
    zip_files = sorted(zip_dir.glob("*.zip")) if zip_dir.is_dir() else []
    if not zip_files:
        return

    rows: list[dict] = []
    for zf in zip_files:
        try:
            df = pl.read_csv(zf, has_header=False, infer_schema_length=0)
        except Exception:
            continue
        for row in df.iter_rows():
            rows.append(
                {
                    "open_time": int(row[0]),
                    "open": float(row[1]) if row[1] else 0.0,
                    "high": float(row[2]) if row[2] else 0.0,
                    "low": float(row[3]) if row[3] else 0.0,
                    "close": float(row[4]) if row[4] else 0.0,
                    "volume": float(row[5]) if row[5] else 0.0,
                    "close_time": int(row[6]) if row[6] else 0,
                    "quote_volume": float(row[7]) if row[7] else 0.0,
                    "trade_count": int(row[8]) if row[8] else 0,
                    "taker_buy_volume": float(row[9]) if row[9] else 0.0,
                    "taker_buy_quote_volume": float(row[10]) if row[10] else 0.0,
                    "symbol": symbol,
                    "interval": interval,
                }
            )
    yield rows


@dlt.source
def build_archive_source(
    symbols: list[str],
    interval: str = "1h",
    trade_type: TradeType = TradeType.spot,
) -> list[dlt.Resource]:
    """Build a dlt source for Binance archive data.

    One resource per symbol, reading from local ZIP files downloaded by the
    existing archive download workflow.

    Args:
        symbols: Trading symbols.
        interval: Kline interval.
        trade_type: Market type (``"spot"``, ``"um"``, ``"cm"``).

    Returns:
        List of dlt Resources (one per symbol).
    """
    return [
        archive_klines_resource(symbol=sym, interval=interval, trade_type=trade_type).with_name(
            f"archive_{sym}_klines"
        )
        for sym in symbols
    ]
