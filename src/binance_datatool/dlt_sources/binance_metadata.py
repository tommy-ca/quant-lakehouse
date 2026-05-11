"""dlt resources for Binance metadata — venues and symbols.

Discovers venues (trade types), available data types per venue, and
symbols from the live S3 archive. Uses the ``ArchiveExplorer`` for
directory-level discovery and ``ArchiveListSymbolsWorkflow`` for
per-symbol metadata.

Two resources:
- ``venues_resource`` — scans S3 for trade types + data types per freq
- ``symbols_resource`` — lists symbols per (trade_type, data_type, interval)
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import dlt

from binance_datatool.archive.client import ArchiveClient
from binance_datatool.common.enums import DataFrequency, DataType, TradeType
from binance_datatool.workflow.list_symbols import ArchiveListSymbolsWorkflow

ALL_TRADE_TYPES = [TradeType.spot, TradeType.um, TradeType.cm]
ALL_DATA_TYPES = [
    "klines",
    "aggTrades",
    "trades",
    "fundingRate",
    "bookDepth",
    "bookTicker",
    "indexPriceKlines",
    "markPriceKlines",
    "premiumIndexKlines",
    "metrics",
]
ALL_FREQUENCIES = ["daily", "monthly"]
INTERVAL_TYPES = {"klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"}


# ── Venues resource ──────────────────────────────────────────────


@dlt.resource(
    name="venues",
    write_disposition="replace",
    columns={
        "trade_type": {"data_type": "text", "nullable": False},
        "data_types": {"data_type": "text", "nullable": True},
        "frequencies": {"data_type": "text", "nullable": True},
        "fetched_at": {"data_type": "bigint", "nullable": False},
    },
)
def venues_resource() -> list[dict[str, Any]]:
    """Discover venues (trade types) and their capabilities from S3.

    Scans the archive directory structure for each trade type to find
    available data types and frequencies. Returns one row per venue.
    """
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    results: list[dict[str, Any]] = []

    for tt in ALL_TRADE_TYPES:
        # Scan available data types per frequency
        all_types: set[str] = set()
        freqs: list[str] = []
        for freq in ALL_FREQUENCIES:
            try:
                dtypes = _scan_data_types_for_freq(tt, freq)
                if dtypes:
                    all_types.update(dtypes)
                    freqs.append(freq)
            except Exception:
                continue
        results.append(
            {
                "trade_type": tt.value,
                "data_types": ",".join(sorted(all_types)) if all_types else None,
                "frequencies": ",".join(freqs) if freqs else None,
                "fetched_at": now_ms,
            }
        )
    return results


def _scan_data_types_for_freq(trade_type: TradeType, freq: str) -> list[str]:
    """List data type directories under ``data/{trade_type}/{freq}/``."""
    client = ArchiveClient()
    freq_enum = DataFrequency(freq)
    try:
        prefixes = asyncio.run(
            client.list_dir(
                asyncio.run(client._create_session()),
                f"data/{trade_type.s3_path}/{freq_enum.value}/",
            )
        )
        return sorted(p.rstrip("/").split("/")[-1] for p in prefixes)
    except Exception:
        return []


# ── Symbols resource ─────────────────────────────────────────────


def _list_archive_symbols(trade_type: TradeType, data_type: DataType) -> list[str]:
    """List symbols for a trade type using ArchiveListSymbolsWorkflow."""
    try:
        client = ArchiveClient()
        wf = ArchiveListSymbolsWorkflow(
            client=client,
            trade_type=trade_type,
            data_freq=DataFrequency.daily,
            data_type=data_type,
        )
        result = asyncio.run(wf.run())
        return [s.symbol for s in result.matched]
    except Exception:
        return []


@dlt.resource(
    name="symbols",
    write_disposition="replace",
    columns={
        "symbol": {"data_type": "text", "nullable": False},
        "trade_type": {"data_type": "text", "nullable": False},
        "data_type": {"data_type": "text", "nullable": False},
        "base_asset": {"data_type": "text", "nullable": True},
        "quote_asset": {"data_type": "text", "nullable": True},
        "contract_type": {"data_type": "text", "nullable": True},
        "is_leverage": {"data_type": "bool", "nullable": True},
        "is_stable_pair": {"data_type": "bool", "nullable": True},
        "source": {"data_type": "text", "nullable": False},
        "fetched_at": {"data_type": "bigint", "nullable": False},
    },
)
def symbols_resource(
    trade_type: TradeType = TradeType.spot,
    data_type: str = "klines",
) -> list[dict[str, Any]]:
    """Discover symbols from Binance archive for a trade type.

    Uses ``ArchiveListSymbolsWorkflow`` for rich metadata per symbol.
    """
    client = ArchiveClient()
    dt_enum = DataType(data_type)
    wf = ArchiveListSymbolsWorkflow(
        client=client,
        trade_type=trade_type,
        data_freq=DataFrequency.daily,
        data_type=dt_enum,
    )
    result = asyncio.run(wf.run())
    now_ms = int(datetime.now(UTC).timestamp() * 1000)

    symbols: list[dict[str, Any]] = []
    for entry in result.matched:
        symbols.append(
            {
                "symbol": entry.symbol,
                "trade_type": trade_type.value,
                "data_type": data_type,
                "base_asset": entry.base_asset,
                "quote_asset": entry.quote_asset,
                "contract_type": getattr(entry, "contract_type", None),
                "is_leverage": getattr(entry, "is_leverage", False),
                "is_stable_pair": getattr(entry, "is_stable_pair", False),
                "source": "archive",
                "fetched_at": now_ms,
            }
        )
    for entry in result.unmatched:
        symbols.append(
            {
                "symbol": entry,
                "trade_type": trade_type.value,
                "data_type": data_type,
                "base_asset": None,
                "quote_asset": None,
                "contract_type": None,
                "is_leverage": None,
                "is_stable_pair": None,
                "source": "archive",
                "fetched_at": now_ms,
            }
        )
    return symbols


# ── Source builder ───────────────────────────────────────────────


@dlt.source
def build_metadata_source(
    trade_types: list[TradeType] | None = None,
) -> list[dlt.Resource]:
    """Build a dlt source for Binance metadata — venues + symbols.

    Returns all resources:
    - ``venues`` — one row per trade type with capabilities
    - ``symbols_{tt}`` — symbols per trade type (klines by default)

    Args:
        trade_types: Trade types to scan (defaults to all: spot, um, cm).

    Returns:
        List of dlt Resources.
    """
    if trade_types is None:
        trade_types = ALL_TRADE_TYPES

    resources: list[dlt.Resource] = [venues_resource()]
    for tt in trade_types:
        resources.append(symbols_resource(trade_type=tt).with_name(f"symbols_{tt.value}"))
    return resources
