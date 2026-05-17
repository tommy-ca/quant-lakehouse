from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from binance_datatool.adapter.protocol import DataSourceAdapter
from binance_datatool.archive.client import ArchiveClient

if TYPE_CHECKING:
    from binance_datatool.archive.client import ArchiveFile
    from binance_datatool.common import DataFrequency, DataType
    from binance_datatool.common.enums import TradeType


@dataclass
class BinanceAdapter(DataSourceAdapter):
    """Thin adapter wrapping ArchiveClient to satisfy DataSourceAdapter.

    This keeps a clear boundary for multi-source extension while reusing the
    battle-tested ArchiveClient implementation (avoid rewrites; DRY).
    """

    client: ArchiveClient | None = None

    @property
    def _client(self) -> ArchiveClient:
        """Lazily create and return the underlying ArchiveClient."""
        if self.client is None:
            self.client = ArchiveClient()
        return self.client

    async def list_symbols(
        self, trade_type: TradeType, data_freq: DataFrequency, data_type: DataType
    ) -> list[str]:
        return await self._client.list_symbols(trade_type, data_freq, data_type)

    async def list_symbol_files(
        self,
        trade_type: TradeType,
        data_freq: DataFrequency,
        data_type: DataType,
        symbol: str,
        interval: str | None = None,
    ) -> list[ArchiveFile]:
        return await self._client.list_symbol_files(
            trade_type, data_freq, data_type, symbol, interval
        )

    def get_name(self) -> str:
        return "binance-archive"
