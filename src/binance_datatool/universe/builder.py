"""Universe builder for constructing tradable asset lists."""

from __future__ import annotations

import time
from pathlib import Path

import polars as pl
from loguru import logger

from binance_datatool.common.constants import (
    FIATS,
    LEVERAGE_EXCLUDES,
    LEVERAGE_SUFFIXES,
    MEME_COINS,
    STABLECOINS,
)
from binance_datatool.common.settings import settings
from binance_datatool.storage.duckdb import get_connection
from binance_datatool.universe.rates import RateProvider


class UniverseBuilder:
    """Consumes metadata to build curated trading universes."""

    def __init__(self, lake_path: str | Path, rate_provider: RateProvider | None = None):
        self.lake_path = Path(lake_path).resolve()
        self.catalog_path = self.lake_path / "catalog.duckdb"
        self.rate_provider = rate_provider or RateProvider()

    def build_top_50(
        self,
        trade_type: str,
        min_volume_usd: float | None = None,
        min_age_days: int | None = None,
        exclude_stables: bool = True,
        exclude_memes: bool = True,
        as_of_timestamp_ms: int | None = None,
        use_silver_on_the_fly: bool = False,
        top_n: int = 50,
    ) -> list[str]:
        """Build a curated, unique top-N universe for a venue."""
        min_volume_usd = (
            min_volume_usd if min_volume_usd is not None else settings.universe_min_volume_usd
        )
        min_age_days = min_age_days if min_age_days is not None else settings.universe_min_age_days

        con = get_connection(lake_path=self.lake_path)
        venue_id = f"binance_{trade_type}"

        try:
            # 1. Fetch base metadata and market stats
            # Includes temporal lifecycle data for age filtering
            df = None
            if as_of_timestamp_ms is not None:
                if use_silver_on_the_fly:
                    try:
                        # Option B: On-the-Fly Calculation from Silver Klines
                        # Still joins with registry for metadata (survivorship bias risk if not in registry)
                        df = con.execute(
                            """
                            SELECT
                                i.symbol,
                                i.venue_id,
                                i.base_asset,
                                i.quote_asset,
                                i.onboard_date,
                                s.quote_volume,
                                s.market_cap,
                                s.last_price,
                                i.status
                            FROM registry.instruments i
                            JOIN (
                                SELECT
                                    symbol,
                                    exchange as venue_id,
                                    SUM(quote_volume) as quote_volume,
                                    SUM(quote_volume) * ? as market_cap,
                                    arg_max(close, ts_event) as last_price
                                FROM silver.klines
                                WHERE ts_date = CAST(EPOCH_ms(?) AS DATE)
                                GROUP BY symbol, exchange
                            ) s ON i.symbol = s.symbol AND i.venue_id = s.venue_id
                            WHERE i.venue_id = ?
                            """,
                            [settings.universe_mcap_multiplier, as_of_timestamp_ms, venue_id],
                        ).pl()
                    except Exception as e:
                        logger.warning(
                            f"silver.klines on-the-fly calculation failed: {e}. Falling back to current snapshot market_stats."
                        )
                        df = None
                else:
                    try:
                        # Point-in-time: Use Gold layer daily snapshots
                        # Uses captured metadata from Gold layer to support delisted assets
                        df = con.execute(
                            """
                            SELECT
                                symbol,
                                venue_id,
                                base_asset,
                                quote_asset,
                                onboard_date,
                                quote_volume,
                                market_cap,
                                last_price,
                                'trading' as status
                            FROM gold.daily_universe_stats
                            WHERE venue_id = ?
                              AND ts_date = CAST(EPOCH_ms(?) AS DATE)
                            """,
                            [venue_id, as_of_timestamp_ms],
                        ).pl()
                    except Exception:
                        logger.warning(
                            "gold.daily_universe_stats unavailable or missing for this date. Falling back to current snapshot market_stats. True point-in-time volume is compromised."
                        )
                        df = None

            if df is None or df.is_empty():
                df = con.execute(
                    """
                    SELECT
                        i.symbol,
                        i.venue_id,
                        i.base_asset,
                        i.quote_asset,
                        i.onboard_date,
                        s.quote_volume,
                        s.market_cap,
                        s.last_price,
                        i.status
                    FROM registry.instruments i
                    JOIN registry.market_stats s ON i.symbol = s.symbol AND i.venue_id = s.venue_id
                    WHERE i.venue_id = ?
                      AND i.status = 'trading'
                    """,
                    [venue_id],
                ).pl()

            if df.is_empty():
                return []

            # 2. Dynamic USD Normalization
            rates = self.rate_provider.get_usd_rates(con, as_of_timestamp_ms)

            df = df.with_columns(
                pl.col("quote_asset")
                .map_elements(
                    lambda x: rates.get(x, 1.0 if "USD" in x else 0.0), return_dtype=pl.Float64
                )
                .alias("quote_usd_rate")
            ).with_columns((pl.col("quote_volume") * pl.col("quote_usd_rate")).alias("volume_usd"))

            # 3. Apply Quality & Stability Filters
            now_ms = (
                as_of_timestamp_ms if as_of_timestamp_ms is not None else int(time.time() * 1000)
            )
            ms_per_day = 86_400_000

            # Point-in-time filter: Exclude assets that were not listed yet at as_of_timestamp_ms
            df = df.filter((pl.col("onboard_date").is_null()) | (pl.col("onboard_date") <= now_ms))

            # Institutional filter 1: ASCII only symbols
            df = df.filter(pl.col("symbol").str.contains(r"^[A-Za-z0-9\-_]+$"))

            # Institutional filter 2: No leveraged tokens
            df = df.filter(
                ~(
                    pl.col("base_asset").str.ends_with(LEVERAGE_SUFFIXES[0])
                    | pl.col("base_asset").str.ends_with(LEVERAGE_SUFFIXES[1])
                    | pl.col("base_asset").str.ends_with(LEVERAGE_SUFFIXES[2])
                    | pl.col("base_asset").str.ends_with(LEVERAGE_SUFFIXES[3])
                )
                | pl.col("base_asset").is_in(LEVERAGE_EXCLUDES)
            )

            df = df.filter(
                (pl.col("volume_usd") >= min_volume_usd)
                &
                # Listing Age Filter (eliminate launch bias)
                (
                    (pl.col("onboard_date").is_null())
                    | ((now_ms - pl.col("onboard_date")) >= (min_age_days * ms_per_day))
                )
                &
                # Liquidity Quality Filter (V/MC ratio)
                # Ensure MC is not zero to avoid div by zero
                (
                    (pl.col("market_cap") > 0)
                    & ((pl.col("volume_usd") / pl.col("market_cap")).is_between(0.01, 0.5))
                )
            )

            if exclude_stables:
                stables = STABLECOINS | FIATS
                df = df.filter(~pl.col("base_asset").is_in(stables))

            if exclude_memes:
                # Strip 1000 prefix for futures comparison
                df = df.with_columns(
                    pl.col("base_asset").str.replace(r"^1000", "").alias("normalized_base")
                )
                df = df.filter(~pl.col("normalized_base").is_in(MEME_COINS))

            # 4. Multi-Factor Scoring
            vol_w = settings.universe_volume_weight
            mcap_w = settings.universe_mcap_weight
            df = df.with_columns(
                (
                    vol_w * (pl.col("volume_usd") + 1).log()
                    + mcap_w * (pl.col("market_cap") + 1).log()
                ).alias("score")
            )

            # 5. Uniqueness: Pick primary pair for each base asset
            df = df.sort("score", descending=True).unique(subset=["base_asset"], keep="first")

            return df.sort("score", descending=True).head(top_n)["symbol"].to_list()

        except Exception as e:
            logger.error(f"Failed to build universe for {venue_id}: {e}")
            return []
        finally:
            con.close()
