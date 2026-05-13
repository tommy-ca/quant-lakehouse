"""Importable transform functions (thin wrappers for Prefect @task).

These functions contain the actual Bronze→Silver transform logic.
Prefect ``@task`` decorators in ``prefect_flows.py`` delegate to these.
"""

from __future__ import annotations

from binance_datatool.storage.duckdb import get_connection, write_silver_table
from binance_datatool.transforms.agg_trades import bronze_agg_trades_to_silver
from binance_datatool.transforms.funding_rate import bronze_funding_rate_to_silver
from binance_datatool.transforms.klines import bronze_klines_to_silver


def transform_klines(
    symbol: str,
    interval: str = "1h",
    trade_type: str = "spot",
    catalog_path: str | None = None,
) -> int:
    """Read bronze klines from DuckDB, transform to Silver, write back."""
    con = get_connection(catalog_path=catalog_path)
    try:
        bronze = con.execute(
            "SELECT open_time, open, high, low, close, volume, close_time, "
            "quote_volume, count, taker_buy_volume, taker_buy_quote_volume, "
            "symbol, interval FROM bronze.klines WHERE symbol = ?",
            [symbol],
        ).pl()
        if bronze.is_empty():
            return 0
        silver = bronze_klines_to_silver(
            bronze, symbol=symbol, interval=interval, trade_type=trade_type
        )
        if silver.is_empty():
            return 0
        return write_silver_table(con, "klines", silver.to_arrow(), symbol)
    finally:
        con.close()


def transform_agg_trades(
    symbol: str,
    trade_type: str = "spot",
    catalog_path: str | None = None,
) -> int:
    """Read bronze aggTrades from DuckDB, transform to Silver, write back."""
    con = get_connection(catalog_path=catalog_path)
    try:
        bronze = con.execute(
            "SELECT agg_trade_id, price, quantity, transact_time, "
            "is_buyer_maker, first_trade_id, last_trade_id, symbol "
            "FROM bronze.agg_trades"
        ).pl()
        silver = bronze_agg_trades_to_silver(bronze, symbol=symbol, trade_type=trade_type)
        if silver.is_empty():
            return 0
        return write_silver_table(con, "agg_trades", silver.to_arrow(), symbol)
    finally:
        con.close()


def transform_funding_rate(
    symbol: str,
    trade_type: str = "um",
    catalog_path: str | None = None,
) -> int:
    """Read bronze fundingRate from DuckDB, transform to Silver, write back."""
    con = get_connection(catalog_path=catalog_path)
    try:
        bronze = con.execute(
            "SELECT symbol, funding_time, funding_rate FROM bronze.funding_rate"
        ).pl()
        silver = bronze_funding_rate_to_silver(bronze, symbol=symbol, trade_type=trade_type)
        if silver.is_empty():
            return 0
        return write_silver_table(con, "funding_rate", silver.to_arrow(), symbol)
    finally:
        con.close()
