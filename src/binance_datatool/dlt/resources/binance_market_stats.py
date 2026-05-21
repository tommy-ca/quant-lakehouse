from __future__ import annotations

from datetime import UTC, datetime

import dlt

from binance_datatool.common.enums import TradeType
from binance_datatool.common.settings import settings
from binance_datatool.dlt.models import MarketStatsModel
from binance_datatool.exchange import (
    BinanceCmRestClient,
    BinanceSpotRestClient,
    BinanceUmRestClient,
)


@dlt.resource(
    name="market_stats",
    write_disposition="replace",
    primary_key=["symbol", "venue_id"],
    columns=MarketStatsModel,
)
def market_stats_resource(trade_types=None):
    """Fetch 24h market statistics for liquidity analysis."""
    if trade_types is None:
        trade_types = [TradeType.spot, TradeType.um, TradeType.cm]

    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    clients = {
        TradeType.spot: BinanceSpotRestClient(),
        TradeType.um: BinanceUmRestClient(),
        TradeType.cm: BinanceCmRestClient(),
    }

    for tt in trade_types:
        client = clients[tt]
        venue_id = f"binance_{tt.value}"

        # Fetch tickers
        if tt == TradeType.spot:
            resp = client._client.rest_api.ticker24hr()
        else:
            resp = client._client.rest_api.ticker24hr_price_change_statistics()

        data = resp.data()
        # Handle list-wrappers and variations across SDKs
        tickers = getattr(data, "actual_instance", data)
        if not isinstance(tickers, list):
            tickers = getattr(tickers, "symbols", getattr(tickers, "tickers", []))

        for t in tickers:
            # Multi-SDK attribute mapping
            sym = getattr(t, "symbol", None)
            if not sym:
                continue

            q_vol = getattr(t, "quote_volume", getattr(t, "quoteVolume", 0.0))
            p_change = getattr(t, "price_change_percent", getattr(t, "priceChangePercent", 0.0))
            last_p = getattr(t, "last_price", getattr(t, "lastPrice", 0.0))

            # MOCK Market Cap for universe building:
            # In production, this would be joined from a CM/CG data source.
            # Here we derive a 'pseudo_market_cap' based on volume and price
            # to enable the multi-factor ranking logic.
            # Formula: pseudo_mc = Volume * (Configurable Scaling Factor)
            # This ensures top volume coins have high pseudo-mc for demonstration.
            pseudo_mc = float(q_vol) * settings.universe_mcap_multiplier

            yield {
                "symbol": sym,
                "venue_id": venue_id,
                "quote_volume": float(q_vol),
                "price_change_pct": float(p_change),
                "last_price": float(last_p),
                "market_cap": pseudo_mc,
                "fetched_at": now_ms,
            }
