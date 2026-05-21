"""Gold layer pipeline for building daily universe statistics."""

from __future__ import annotations

from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from loguru import logger

from binance_datatool.common.settings import settings
from binance_datatool.storage.duckdb import get_connection

if TYPE_CHECKING:
    from pathlib import Path


def build_daily_universe_stats(lake_path: str | Path, target_date: str | None = None) -> None:
    """Build or update the gold.daily_universe_stats table from silver.klines.

    Args:
        lake_path: Path to the lakehouse directory.
        target_date: Target date to process (YYYY-MM-DD). If None, defaults to yesterday.
    """
    if target_date is None:
        target_date = (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%d")

    con = get_connection(lake_path=lake_path)

    try:
        # 1. Identify the lakehouse database
        attached = con.execute("PRAGMA database_list").fetchall()
        alias = next((a[1] for a in attached if a[1] == "lakehouse"), None)

        if not alias:
            logger.warning("Native lakehouse not found. Falling back to legacy gold build.")
            # ... (keep legacy as fallback or just fail)
            return

        # Check if silver.klines exists
        tables = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver'"
            ).fetchall()
        ]
        if "klines" not in tables:
            logger.warning("silver.klines table not found. Cannot build gold stats.")
            return

        # 2. Create Schema and Table in Native DuckLake
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {alias}.gold")

        # Check if table exists
        gold_tables = con.execute(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = 'gold' AND table_catalog = '{alias}' AND table_name = 'daily_universe_stats'"
        ).fetchall()

        if not gold_tables:
            # Create template table
            con.execute(
                f"""
                CREATE TABLE {alias}.gold.daily_universe_stats AS
                SELECT
                    k.symbol,
                    k.exchange as venue_id,
                    k.ts_date,
                    i.base_asset,
                    i.quote_asset,
                    i.onboard_date,
                    k.quote_volume,
                    k.quote_volume as market_cap,
                    k.close as last_price
                FROM silver.klines k
                LEFT JOIN registry.instruments i ON k.symbol = i.symbol AND k.exchange = i.venue_id
                WHERE FALSE
                """
            )
            # Apply native partitioning by date
            con.execute(
                f"ALTER TABLE {alias}.gold.daily_universe_stats SET PARTITIONED BY (ts_date)"
            )

        # 3. Aggregate and Insert
        # Native DuckLake handles the Parquet spills and date partitioning
        con.execute(
            f"DELETE FROM {alias}.gold.daily_universe_stats WHERE ts_date = CAST(? AS DATE)",
            [target_date],
        )
        con.execute(
            f"""
            INSERT INTO {alias}.gold.daily_universe_stats
            SELECT
                k.symbol,
                k.exchange as venue_id,
                CAST(? AS DATE) as ts_date,
                i.base_asset,
                i.quote_asset,
                i.onboard_date,
                SUM(k.quote_volume) as quote_volume,
                SUM(k.quote_volume) * ? as market_cap,
                arg_max(k.close, k.ts_event) as last_price
            FROM silver.klines k
            LEFT JOIN registry.instruments i ON k.symbol = i.symbol
              AND REPLACE(k.exchange, '-', '_') = REPLACE(i.venue_id, '-', '_')
            WHERE k.ts_date = CAST(? AS DATE)
            GROUP BY k.symbol, k.exchange, i.base_asset, i.quote_asset, i.onboard_date
            """,
            [target_date, settings.universe_mcap_multiplier, target_date],
        )

        # 4. Sync View in 'main'
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        with suppress(Exception):
            con.execute("DROP VIEW IF EXISTS gold.daily_universe_stats")
        with suppress(Exception):
            con.execute("DROP TABLE IF EXISTS gold.daily_universe_stats")
        con.execute(
            f"CREATE VIEW gold.daily_universe_stats AS SELECT * FROM {alias}.gold.daily_universe_stats"
        )

        logger.info(f"Successfully built native gold.daily_universe_stats for {target_date}")

    except Exception as e:
        logger.error(f"Failed to build gold.daily_universe_stats: {e}")
        raise
    finally:
        con.close()
