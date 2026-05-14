"""dlt resources for Binance metadata — venues and symbols.

Pure extract+load: no S3 directory scanning, no discovery logic.
Prefect resolves what to fetch (via ArchiveExplorer) and calls
these resources with concrete parameters.

Two resources:
- ``venues_resource`` — accepts pre-scanned venue data
- ``symbols_resource`` — lists symbols per trade type
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import dlt

from binance_datatool.archive.client import ArchiveClient
from binance_datatool.common.enums import DataFrequency, DataType, TradeType
from binance_datatool.dlt.models import SymbolMetaModel, VenueModel
from binance_datatool.workflow.list_symbols import ArchiveListSymbolsWorkflow

ALL_TRADE_TYPES = [TradeType.spot, TradeType.um, TradeType.cm]


@dlt.resource(
    name="venues",
    write_disposition="replace",
    columns=VenueModel,
    schema_contract={"columns": "freeze", "data_type": "freeze"},
)
def venues_resource(
    venues: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Load venue metadata.

    When ``venues`` is provided, loads them directly (Prefect has
    already run ArchiveExplorer to discover them).  When ``None``,
    returns hardcoded default venues.

    Args:
        venues: Pre-discovered venue data. Each entry needs
            ``trade_type``, ``data_types``, ``frequencies``.

    Returns:
        List of venue dicts.
    """
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    if venues is not None:
        for v in venues:
            v.setdefault("fetched_at", now_ms)
        return venues
    # Fallback defaults when explorer hasn't run
    return [
        {"trade_type": "spot", "data_types": None, "frequencies": None, "fetched_at": now_ms},
        {"trade_type": "um", "data_types": None, "frequencies": None, "fetched_at": now_ms},
        {"trade_type": "cm", "data_types": None, "frequencies": None, "fetched_at": now_ms},
    ]


@dlt.resource(
    name="symbols",
    write_disposition="replace",
    columns=SymbolMetaModel,
    schema_contract={"columns": "freeze", "data_type": "freeze"},
)
def symbols_resource(
    trade_types: list[TradeType] | None = None,
    data_type: str = "klines",
) -> list[dict[str, Any]]:
    """Discover symbols from Binance archive for all trade types.

    Pure EL: lists S3 symbol directories per trade type. No directory
    scanning (ArchiveExplorer handles that separately).

    Args:
        trade_types: Trade types to scan.
        data_type: Data type for symbol listing.

    Returns:
        List of symbol dicts with ``trade_type`` column.
    """
    if trade_types is None:
        trade_types = ALL_TRADE_TYPES

    import asyncio

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


@dlt.source
def build_metadata_source(
    trade_types: list[TradeType] | None = None,
    venues: list[dict[str, Any]] | None = None,
) -> list[dlt.Resource]:
    """Build a dlt source for Binance metadata — venues + symbols.

    Args:
        trade_types: Trade types to scan for symbols.
        venues: Pre-discovered venue data (from ArchiveExplorer).

    Returns:
        List of dlt Resources.
    """
    if trade_types is None:
        trade_types = ALL_TRADE_TYPES
    return [venues_resource(venues=venues), symbols_resource(trade_types=trade_types)]
