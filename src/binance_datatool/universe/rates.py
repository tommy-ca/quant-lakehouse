from __future__ import annotations

import json
import urllib.request
from contextlib import suppress
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

import duckdb
from loguru import logger

from binance_datatool.common.constants import FIATS, STABLECOINS

DEFAULT_FALLBACK_RATES = {
    "USDT": 1.0,
    "USDC": 1.0,
    "BUSD": 1.0,
    "DAI": 1.0,
    "FDUSD": 1.0,
    "TUSD": 1.0,
    "USD": 1.0,
    "TRY": 0.028,
    "IDR": 0.000063,
    "JPY": 0.0064,
    "BRL": 0.17,
    "EUR": 1.08,
    "GBP": 1.26,
}

# Ensure all staples/fiats have at least a basic default
for s in STABLECOINS:
    if s not in DEFAULT_FALLBACK_RATES:
        DEFAULT_FALLBACK_RATES[s] = 1.0
for f in FIATS:
    if f not in DEFAULT_FALLBACK_RATES:
        DEFAULT_FALLBACK_RATES[f] = 0.0


@lru_cache(maxsize=32)
def _fetch_external_rates_helper(date_str: str | None = None) -> dict[str, float]:
    """Fetch latest or historical rates from Frankfurter (ECB data)."""
    path = date_str if date_str else "latest"
    url = f"https://api.frankfurter.dev/v2/{path}?base=USD"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "binance-datatool/1.0"})
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())
            rates = data.get("rates", {})
            # Frankfurter returns 1 USD = X EUR.
            # We need USD value of 1 EUR, which is 1/X.
            return {asset: 1.0 / rate for asset, rate in rates.items()}
    except Exception as e:
        logger.debug(f"External FX fetch failed ({url}): {e}")
        return {}


class RateProvider:
    """Provides USD conversion rates for various base/quote assets."""

    def __init__(self, fallback_rates: dict[str, float] | None = None):
        self.fallback_rates = (
            fallback_rates if fallback_rates is not None else DEFAULT_FALLBACK_RATES.copy()
        )

    def get_usd_rates(
        self, db_connection: duckdb.DuckDBPyConnection | Any, as_of_timestamp_ms: int | None = None
    ) -> dict[str, float]:
        """Fetch dynamic rates from catalog, external API, or fallbacks."""
        rates = self.fallback_rates.copy()
        date_str = None
        if as_of_timestamp_ms:
            date_str = datetime.fromtimestamp(as_of_timestamp_ms / 1000, tz=UTC).strftime(
                "%Y-%m-%d"
            )

        # 1. Try local catalog (Gold or Registry)
        try:
            rates_df = None
            if as_of_timestamp_ms:
                with suppress(duckdb.Error):
                    rates_df = db_connection.execute(
                        """
                        SELECT symbol, last_price as usd_rate
                        FROM gold.daily_universe_stats
                        WHERE venue_id = 'binance_spot'
                          AND symbol IN ('BTCUSDT', 'ETHUSDT', 'BNBUSDT')
                          AND ts_date = CAST(EPOCH_ms(?) AS DATE)
                        """,
                        [as_of_timestamp_ms],
                    ).pl()

            if rates_df is None or rates_df.is_empty():
                with suppress(duckdb.Error):
                    rates_df = db_connection.execute(
                        """
                        SELECT symbol, last_price as usd_rate
                        FROM registry.market_stats
                        WHERE venue_id = 'binance_spot'
                          AND symbol IN ('BTCUSDT', 'ETHUSDT', 'BNBUSDT')
                        """
                    ).pl()

            if rates_df is not None and not rates_df.is_empty():
                for row in rates_df.to_dicts():
                    asset = row["symbol"].replace("USDT", "")
                    rates[asset] = row["usd_rate"]
        except Exception as e:
            logger.debug(f"Local catalog rate fetch failed: {e}")

        # 2. Fetch external FX rates (Frankfurter) for fiats
        # This ensures accurate normalization for EUR, JPY, TRY etc.
        external = _fetch_external_rates_helper(date_str)
        if external:
            rates.update(external)
            logger.debug(
                f"Updated {len(external)} rates from external FX source ({date_str or 'latest'})"
            )

        return rates
