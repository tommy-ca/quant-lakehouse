"""Binance WebSocket clients implementing the ExchangeClient protocol."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from binance_common.configuration import ConfigurationWebSocketStreams
from binance_common.constants import (
    DERIVATIVES_TRADING_COIN_FUTURES_WS_STREAMS_PROD_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL,
    SPOT_WS_STREAMS_PROD_URL,
)
from binance_sdk_derivatives_trading_coin_futures.derivatives_trading_coin_futures import (
    DerivativesTradingCoinFutures,
)
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DerivativesTradingUsdsFutures,
)
from binance_sdk_spot.spot import Spot
from binance_sdk_spot.websocket_streams.models.enums import KlineIntervalEnum

from binance_datatool.common.types import KlineData

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

__all__ = ["BinanceSpotWsClient", "BinanceUmWsClient", "BinanceCmWsClient"]

_WS_BASE_URLS = {
    "spot": SPOT_WS_STREAMS_PROD_URL,
    "um": DERIVATIVES_TRADING_USDS_FUTURES_WS_STREAMS_PROD_URL,
    "cm": DERIVATIVES_TRADING_COIN_FUTURES_WS_STREAMS_PROD_URL,
}

_SDK_WS_CLASSES = {
    "spot": Spot,
    "um": DerivativesTradingUsdsFutures,
    "cm": DerivativesTradingCoinFutures,
}

_WS_INTERVAL_METHOD = {
    "spot": "kline",
    "um": "kline_candlestick_streams",
    "cm": "kline_candlestick_streams",
}

_INTERVAL_ENUM_MAP = {
    "1s": KlineIntervalEnum.INTERVAL_1s,
    "1m": KlineIntervalEnum.INTERVAL_1m,
    "3m": KlineIntervalEnum.INTERVAL_3m,
    "5m": KlineIntervalEnum.INTERVAL_5m,
    "15m": KlineIntervalEnum.INTERVAL_15m,
    "30m": KlineIntervalEnum.INTERVAL_30m,
    "1h": KlineIntervalEnum.INTERVAL_1h,
    "2h": KlineIntervalEnum.INTERVAL_2h,
    "4h": KlineIntervalEnum.INTERVAL_4h,
    "6h": KlineIntervalEnum.INTERVAL_6h,
    "8h": KlineIntervalEnum.INTERVAL_8h,
    "12h": KlineIntervalEnum.INTERVAL_12h,
    "1d": KlineIntervalEnum.INTERVAL_1d,
    "3d": KlineIntervalEnum.INTERVAL_3d,
    "1w": KlineIntervalEnum.INTERVAL_1w,
    "1M": KlineIntervalEnum.INTERVAL_1M,
}


def _parse_kline_message(data: dict) -> KlineData:
    k = data["k"]
    return KlineData(
        open_time=int(k["t"]),
        open=str(k["o"]),
        high=str(k["h"]),
        low=str(k["l"]),
        close=str(k["c"]),
        volume=str(k["v"]),
        close_time=int(k["T"]),
        quote_volume=str(k["q"]),
        num_trades=int(k["n"]),
        taker_buy_volume=str(k["V"]),
        taker_buy_quote_volume=str(k["Q"]),
    )


class _BinanceWsClientBase:
    """Base class for Binance WebSocket clients using official SDK.

    Not intended for direct use — use market-specific subclasses.
    """

    def __init__(self, ws_url: str, market_type: str) -> None:
        self._ws_url = ws_url
        self._market_type = market_type
        self._connection = None

    @property
    def trade_type(self) -> str:
        return self._market_type

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close_connection(close_session=True)
            self._connection = None

    async def fetch_ohlcv(self, symbol: str, interval: str, **kwargs) -> list[KlineData]:
        raise NotImplementedError("Use REST client for historical klines")

    async def fetch_agg_trades(self, symbol: str, **kwargs) -> list:
        raise NotImplementedError("Use REST client for aggregated trades")

    async def fetch_funding_rate(self, symbol: str, **kwargs) -> list:
        raise NotImplementedError("Use REST client for funding rate")

    async def _connect(self) -> None:
        ws_config = ConfigurationWebSocketStreams(stream_url=self._ws_url)
        sdk_class = _SDK_WS_CLASSES[self._market_type]
        client = sdk_class(config_ws_streams=ws_config)
        self._connection = await client.websocket_streams.create_connection()


class BinanceSpotWsClient(_BinanceWsClientBase):
    """Binance Spot market WebSocket client.

    Implements the ExchangeClient protocol for spot trading pairs.
    """

    def __init__(self, testnet: bool = False) -> None:
        ws_url = "wss://testnet.binance.vision/ws" if testnet else _WS_BASE_URLS["spot"]
        super().__init__(ws_url=ws_url, market_type="spot")

    @property
    def exchange_id(self) -> str:
        return "binance_spot"

    async def stream_ohlcv(
        self,
        symbol: str,
        interval: str,
    ) -> AsyncIterator[KlineData]:
        queue: asyncio.Queue = asyncio.Queue()
        await self._connect()

        method_name = _WS_INTERVAL_METHOD["spot"]
        method = getattr(self._connection, method_name)
        interval_enum = _INTERVAL_ENUM_MAP[interval]

        stream = await method(symbol=symbol.lower(), interval=interval_enum)
        stream.on("message", lambda msg: queue.put_nowait(msg))

        try:
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=60)
                except TimeoutError:
                    continue
                if "k" in data:
                    yield _parse_kline_message(data)
        finally:
            await self.close()


class BinanceUmWsClient(_BinanceWsClientBase):
    """Binance USDⓈM futures (UM) WebSocket client.

    Implements the ExchangeClient protocol for USD-M perpetual
    and delivery futures.
    """

    def __init__(self) -> None:
        super().__init__(ws_url=_WS_BASE_URLS["um"], market_type="um")

    @property
    def exchange_id(self) -> str:
        return "binance_um"

    async def stream_ohlcv(
        self,
        symbol: str,
        interval: str,
    ) -> AsyncIterator[KlineData]:
        queue: asyncio.Queue = asyncio.Queue()
        await self._connect()

        method_name = _WS_INTERVAL_METHOD["um"]
        method = getattr(self._connection, method_name)

        stream = await method(symbol=symbol.lower(), interval=interval)
        stream.on("message", lambda msg: queue.put_nowait(msg))

        try:
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=60)
                except TimeoutError:
                    continue
                if "k" in data:
                    yield _parse_kline_message(data)
        finally:
            await self.close()


class BinanceCmWsClient(_BinanceWsClientBase):
    """Binance COINⓈM futures (CM) WebSocket client.

    Implements the ExchangeClient protocol for COIN-M perpetual
    and delivery futures.
    """

    def __init__(self) -> None:
        super().__init__(ws_url=_WS_BASE_URLS["cm"], market_type="cm")

    @property
    def exchange_id(self) -> str:
        return "binance_cm"

    async def stream_ohlcv(
        self,
        symbol: str,
        interval: str,
    ) -> AsyncIterator[KlineData]:
        queue: asyncio.Queue = asyncio.Queue()
        await self._connect()

        method_name = _WS_INTERVAL_METHOD["cm"]
        method = getattr(self._connection, method_name)

        stream = await method(symbol=symbol.lower(), interval=interval)
        stream.on("message", lambda msg: queue.put_nowait(msg))

        try:
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=60)
                except TimeoutError:
                    continue
                if "k" in data:
                    yield _parse_kline_message(data)
        finally:
            await self.close()


BinanceWsClient = BinanceSpotWsClient
