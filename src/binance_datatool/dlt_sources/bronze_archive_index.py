"""dlt resource for bronze archive file index.

Scans the local archive mirror (``{archive_home}/data/...``) and builds
a ``bronze.archive_files`` metadata table tracking what files are available.

File path pattern::
    data/{trade_type}/{freq}/{data_type}/{symbol}/{interval}/{file}.zip
    data/{trade_type}/{freq}/{data_type}/{symbol}/{file}.zip

Extracts: trade_type, freq, data_type, symbol, interval (for klines), date.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dlt

# Regex to extract date from filename: YYYY-MM-DD (daily) or YYYY-MM (monthly)
_DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2}|\d{4}-\d{2})")

# Map trade_type path segment → TradeType value
_S3_PATH_MAP = {
    "spot": "spot",
    "futures/um": "um",
    "futures/cm": "cm",
}


def _parse_path(rel_path: str, archive_home: Path) -> dict[str, Any] | None:
    """Parse a relative archive path into metadata fields.

    Args:
        rel_path: Relative path like ``data/spot/daily/klines/BTCUSDT/1h/file.zip``.
        archive_home: Archive root (unused here, kept for interface compat).

    Returns:
        Dict with file metadata or None if path can't be parsed.
    """
    parts = rel_path.replace("\\", "/").split("/")
    # Expected: data/{tt}/{freq}/{dt}/{symbol}/[{interval}/]{file}
    if len(parts) < 6 or parts[0] != "data":
        return None

    # Reconstruct trade_type path (may be multi-segment: "futures/um")
    idx = 1
    trade_type_parts = [parts[idx]]
    idx += 1
    if trade_type_parts[0] == "futures":
        trade_type_parts.append(parts[idx])
        idx += 1
    tt_path = "/".join(trade_type_parts)

    freq = parts[idx]
    data_type = parts[idx + 1]
    symbol = parts[idx + 2]
    filename = parts[-1]

    # Detect interval layer (klines have interval subdirectory)
    has_interval = data_type in (
        "klines",
        "indexPriceKlines",
        "markPriceKlines",
        "premiumIndexKlines",
    )
    interval = parts[idx + 3] if has_interval and len(parts) > idx + 4 else None

    # Extract date from filename
    date_match = _DATE_PATTERN.search(filename)
    date_str = date_match.group(1) if date_match else None

    return {
        "trade_type": _S3_PATH_MAP.get(tt_path, tt_path),
        "freq": freq,
        "data_type": data_type,
        "symbol": symbol,
        "interval": interval,
        "date": date_str,
        "file_path": rel_path,
        "file_name": filename,
        "scanned_at": datetime.now(UTC).isoformat(),
    }


@dlt.resource(
    name="archive_files",
    write_disposition="replace",
    columns={
        "trade_type": {"data_type": "text", "nullable": False},
        "freq": {"data_type": "text", "nullable": False},
        "data_type": {"data_type": "text", "nullable": False},
        "symbol": {"data_type": "text", "nullable": False},
        "interval": {"data_type": "text", "nullable": True},
        "date": {"data_type": "text", "nullable": True},
        "file_path": {"data_type": "text", "nullable": False},
        "file_name": {"data_type": "text", "nullable": False},
        "scanned_at": {"data_type": "text", "nullable": False},
    },
    schema_contract={"columns": "freeze", "data_type": "freeze"},
)
def archive_files_resource(
    archive_home: str | None = None,
) -> list[dict[str, Any]]:
    """Scan the local archive mirror and yield file metadata.

    Args:
        archive_home: Path to archive root. Defaults to
            ``~/.binance-datatool/archive``.

    Returns:
        List of file metadata dicts.
    """
    from binance_datatool.common.path import resolve_archive_home

    home = Path(archive_home) if archive_home else resolve_archive_home()
    data_dir = home / "data"
    if not data_dir.is_dir():
        return []

    results: list[dict[str, Any]] = []
    for fpath in sorted(data_dir.rglob("*.zip")):
        rel = str(fpath.relative_to(home))
        meta = _parse_path(rel, home)
        if meta:
            results.append(meta)
    return results


@dlt.source
def build_archive_index_source(
    archive_home: str | None = None,
) -> list[dlt.Resource]:
    """Build a dlt source for the bronze archive file index.

    Args:
        archive_home: Path to archive root.

    Returns:
        List with one ``archive_files`` resource.
    """
    return [archive_files_resource(archive_home=archive_home)]
