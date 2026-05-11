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
from binance_datatool.validation.models import SymbolMetaModel, VenueModel

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
    columns=VenueModel,
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

        async def _scan() -> list[str]:
            async with client._create_session() as session:
                prefixes = await client.list_dir(
                    session, f"data/{trade_type.s3_path}/{freq_enum.value}/"
                )
                return sorted(p.rstrip("/").split("/")[-1] for p in prefixes)

        return asyncio.run(_scan())
    except Exception:
        return []


# ── Symbols resource ─────────────────────────────────────────────


def _fetch_symbols_for(
    trade_types: list[TradeType], data_type: str = "klines"
) -> list[dict[str, Any]]:
    """Fetch symbols for multiple trade types, returned as a flat list."""
    from binance_datatool.workflow.list_symbols import ArchiveListSymbolsWorkflow

    client = ArchiveClient()
    dt_enum = DataType(data_type)
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    all_symbols: list[dict[str, Any]] = []

    for tt in trade_types:
        wf = ArchiveListSymbolsWorkflow(
            client=client,
            trade_type=tt,
            data_freq=DataFrequency.daily,
            data_type=dt_enum,
        )
        result = asyncio.run(wf.run())
        for entry in result.matched:
            all_symbols.append(
                {
                    "symbol": entry.symbol,
                    "trade_type": tt.value,
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
            all_symbols.append(
                {
                    "symbol": entry,
                    "trade_type": tt.value,
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
    return all_symbols


@dlt.resource(
    name="symbols",
    write_disposition="replace",
    columns=SymbolMetaModel,
)
def symbols_resource(
    trade_types: list[TradeType] | None = None,
    data_type: str = "klines",
) -> list[dict[str, Any]]:
    """Discover symbols from Binance archive for all trade types.

    Returns a single list with ``trade_type`` column for filtering.
    """
    if trade_types is None:
        trade_types = ALL_TRADE_TYPES
    return _fetch_symbols_for(trade_types, data_type)


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

    return [venues_resource(), symbols_resource(trade_types=trade_types)]
