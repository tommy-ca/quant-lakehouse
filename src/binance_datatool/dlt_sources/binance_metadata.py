"""dlt resource for Binance metadata — symbols and venues.

Wraps the existing ``ArchiveListSymbolsWorkflow`` and SDK ``exchange_info``
as ``@dlt.resource`` generators with incremental loading.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import dlt

from binance_datatool.archive.client import ArchiveClient
from binance_datatool.common.enums import DataFrequency, DataType, TradeType
from binance_datatool.workflow.list_symbols import ArchiveListSymbolsWorkflow


@dlt.resource(
    name="symbols",
    write_disposition="replace",
    columns={
        "symbol": {"data_type": "text", "nullable": False},
        "trade_type": {"data_type": "text", "nullable": False},
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
) -> list[dict[str, Any]]:
    """Discover symbols from Binance archive for a trade type.

    Uses the existing ``ArchiveListSymbolsWorkflow`` to list symbols from
    ``data.binance.vision`` S3 listing. Returns symbol metadata including
    base/quote asset, contract type, leverage/stable flags.
    """
    import asyncio

    client = ArchiveClient()
    wf = ArchiveListSymbolsWorkflow(
        client=client,
        trade_type=trade_type,
        data_freq=DataFrequency.daily,
        data_type=DataType.klines,
    )
    result = asyncio.run(wf.run())
    now_ms = int(datetime.now(UTC).timestamp() * 1000)

    symbols: list[dict[str, Any]] = []
    for entry in result.matched:
        symbols.append(
            {
                "symbol": entry.symbol,
                "trade_type": trade_type.value,
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


@dlt.source
def build_metadata_source(
    trade_types: list[TradeType] | None = None,
) -> list[dlt.Resource]:
    """Build a dlt source for Binance metadata.

    One resource per trade type, each discovering symbols from the archive.

    Args:
        trade_types: Trade types to scan (defaults to all: spot, um, cm).

    Returns:
        List of dlt Resources (one per trade type).
    """
    if trade_types is None:
        trade_types = [TradeType.spot, TradeType.um, TradeType.cm]

    return [symbols_resource(trade_type=tt).with_name(f"symbols_{tt.value}") for tt in trade_types]
