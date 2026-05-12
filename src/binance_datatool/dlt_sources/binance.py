"""dlt resources for Binance market data.

Wraps the existing ``BinanceSpotRestClient`` as ``@dlt.resource`` generators
with incremental loading support. This is the reference implementation for
adding new dlt-based sources.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import dlt

from binance_datatool.common.enums import TradeType
from binance_datatool.exchange.binance_rest import BinanceSpotRestClient

if TYPE_CHECKING:
    from binance_datatool.common.types import KlineData
    from binance_datatool.exchange.binance_rest import _BinanceRestClientBase


from binance_datatool.validation.models import RawKlineModel


@dlt.resource(
    name="klines",
    write_disposition="merge",
    primary_key=("symbol", "interval", "open_time"),
    columns=RawKlineModel,
    schema_contract={"columns": "evolve", "data_type": "evolve"},
)
def klines_resource(
    symbol: str,
    interval: str = "1h",
    start_time: int | None = None,
    end_time: int | None = None,
    limit: int = 1000,
    trade_type: TradeType = TradeType.spot,
) -> list[dict[str, Any]]:
    """Fetch klines from Binance REST API.

    Args:
        symbol: Trading pair (e.g. ``"BTCUSDT"``).
        interval: Kline interval (e.g. ``"1h"``, ``"1d"``).
        start_time: Epoch ms start time.
        end_time: Epoch ms end time.
        limit: Max rows per API call (max 1000).
        trade_type: Market type (spot, um, cm).

    Yields:
        dicts with kline fields matching the schema declared in ``@dlt.resource``.
    """
    if trade_type == TradeType.spot:
        client: _BinanceRestClientBase = BinanceSpotRestClient()  # type: ignore[no-redef]
    elif trade_type == TradeType.um:
        from binance_datatool.exchange.binance_rest import BinanceUmRestClient

        client = BinanceUmRestClient()
    elif trade_type == TradeType.cm:
        from binance_datatool.exchange.binance_rest import BinanceCmRestClient

        client = BinanceCmRestClient()
    else:
        raise ValueError(f"Unsupported trade_type: {trade_type}")

    raw_data: list[KlineData] = asyncio.run(
        client.fetch_ohlcv(
            symbol=symbol,
            interval=interval,
            since=start_time or 0,
            until=end_time,
            limit=limit,
        )
    )

    return [
        {
            "open_time": str(k.open_time),
            "open": k.open,
            "high": k.high,
            "low": k.low,
            "close": k.close,
            "volume": k.volume,
            "close_time": str(k.close_time),
            "quote_volume": k.quote_volume if k.quote_volume else None,
            "count": str(k.num_trades),
            "taker_buy_volume": k.taker_buy_volume if k.taker_buy_volume else None,
            "taker_buy_quote_volume": k.taker_buy_quote_volume
            if k.taker_buy_quote_volume
            else None,
            "symbol": symbol,
            "interval": interval,
        }
        for k in raw_data
    ]


@dlt.source
def build_binance_source(
    symbols: list[str],
    interval: str = "1h",
    trade_type: TradeType = TradeType.spot,
    data_type: str = "klines",
) -> list[dlt.Resource]:
    """Build a dlt source for multiple Binance symbols.

    All symbols write to a single ``klines`` table in DuckDB,
    distinguished by the ``symbol`` column. No per-symbol tables.

    Args:
        symbols: Trading symbols to fetch.
        interval: Kline interval.
        trade_type: Market type.
        data_type: Data type ("klines", "aggTrades", "fundingRate").

    Returns:
        A list of dlt Resources (all writing to the same table).
    """
    if data_type != "klines":
        raise NotImplementedError(f"dlt source for {data_type} not yet implemented")

    return [
        klines_resource(
            symbol=sym,
            interval=interval,
            trade_type=trade_type,
        )
        .with_name(f"{sym}_klines")
        .apply_hints(table_name="klines")
        for sym in symbols
    ]
