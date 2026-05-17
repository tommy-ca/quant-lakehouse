from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from binance_datatool.archive.client import ArchiveFile
    from binance_datatool.common import DataFrequency, DataType
    from binance_datatool.common.enums import TradeType


class DataSourceAdapter(Protocol):
    """Minimal protocol for a data source adapter.

    Keep the surface intentionally small to avoid premature abstraction.
    Workflows should accept this protocol rather than concrete clients.
    """

    async def list_symbols(
        self, trade_type: TradeType, data_freq: DataFrequency, data_type: DataType
    ) -> list[str]: ...

    async def list_symbol_files(
        self,
        trade_type: TradeType,
        data_freq: DataFrequency,
        data_type: DataType,
        symbol: str,
        interval: str | None = None,
    ) -> list[ArchiveFile]: ...

    def get_name(self) -> str: ...
