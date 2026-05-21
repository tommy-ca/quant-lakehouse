"""Metadata registry for managing venues and symbols in the Lakehouse."""

from __future__ import annotations

from pathlib import Path

import dlt

from binance_datatool.dlt.destinations import load_source, run_source
from binance_datatool.dlt.resources.binance_market_stats import market_stats_resource
from binance_datatool.dlt.resources.binance_metadata import build_metadata_source
from binance_datatool.storage.duckdb import get_connection


def sync_exchange_metadata(lake_path: str, scan_lifecycle: bool = False):
    """Sync high-fidelity Binance metadata to the Lakehouse registry."""
    lp = Path(lake_path).resolve()
    db_path = str(lp / "catalog.duckdb")

    # 1. Fetch Instruments and Venues
    source = build_metadata_source(scan_lifecycle=scan_lifecycle)
    run_source(
        source,
        source_name="exchange_metadata",
        catalog_path=db_path,
        dataset_name="registry",
        destination="ducklake",
        lake_path=str(lp),
    )
    load_source(
        source_name="exchange_metadata",
        catalog_path=db_path,
        dataset_name="registry",
        destination="ducklake",
        lake_path=str(lp),
    )

    # 2. Fetch Market Stats (24h volume)
    @dlt.source(name="market_stats")
    def stats_source():
        return market_stats_resource()

    run_source(
        stats_source(),
        source_name="market_stats",
        catalog_path=db_path,
        dataset_name="registry",
        destination="ducklake",
        lake_path=str(lp),
    )
    load_source(
        source_name="market_stats",
        catalog_path=db_path,
        dataset_name="registry",
        destination="ducklake",
        lake_path=str(lp),
    )

    # 3. Create interoperability views
    con = get_connection(lake_path=lp)
    # Databento-compatible view
    con.execute(
        """
        CREATE OR REPLACE VIEW registry.v_databento AS
        SELECT
            i.symbol AS raw_symbol,
            v.publisher_id,
            v.dataset,
            i.tick_size AS min_price_increment,
            i.lot_size AS lot_size,
            i.base_asset AS base_asset,
            i.quote_asset AS quote_asset,
            i.instrument_class
        FROM registry.instruments i
        JOIN registry.venues v ON i.venue_id = v.venue_id
        """
    )
    # Tardis-compatible view
    con.execute(
        """
        CREATE OR REPLACE VIEW registry.v_tardis AS
        SELECT
            i.symbol AS symbol,
            v.exchange_slug AS exchange,
            i.base_asset AS baseCurrency,
            i.quote_asset AS quoteCurrency,
            i.tick_size AS tickSize,
            i.lot_size AS amountStep,
            i.min_notional AS minNotional,
            i.contract_type AS type
        FROM registry.instruments i
        JOIN registry.venues v ON i.venue_id = v.venue_id
        """
    )

    con.close()


def get_venues(lake_path: str) -> list[dict]:
    """Get all registered trading venues."""
    lp = Path(lake_path).resolve()
    con = get_connection(lake_path=lp)
    try:
        res = con.execute("SELECT * FROM registry.venues").pl()
        return res.to_dicts()
    except Exception:
        return []
    finally:
        con.close()


def get_symbols(trade_type: str, lake_path: str, status: str | None = "trading") -> list[str]:
    """Query symbols from the Lakehouse registry for a specific trade type."""
    lp = Path(lake_path).resolve()
    # Check if catalog exists first
    if not (lp / "catalog.duckdb").exists():
        return ["BTCUSDT", "ETHUSDT"]

    con = get_connection(lake_path=lp)
    venue_id = f"binance_{trade_type}"
    try:
        # The 'registry' dataset in dlt becomes a schema in DuckDB
        query = "SELECT symbol FROM registry.instruments WHERE venue_id = ?"
        params = [venue_id]
        if status:
            query += " AND status = ?"
            params.append(status.lower())

        res = con.execute(query, params).fetchall()
        return [r[0] for r in res]
    except Exception:
        # Fallback for bootstrap
        return ["BTCUSDT", "ETHUSDT"]
    finally:
        con.close()


def get_instrument_details(symbol: str, trade_type: str, lake_path: str) -> dict | None:
    """Get high-fidelity details for an instrument."""
    lp = Path(lake_path).resolve()
    con = get_connection(lake_path=lp)
    venue_id = f"binance_{trade_type}"
    try:
        res = con.execute(
            "SELECT * FROM registry.instruments WHERE symbol = ? AND venue_id = ?",
            [symbol, venue_id],
        ).pl()
        if res.is_empty():
            return None
        return res.to_dicts()[0]
    except Exception:
        return None
    finally:
        con.close()
