from pathlib import Path

from binance_datatool.storage.duckdb import get_connection


def sync_exchange_metadata(lake_path: str):
    """Sync Binance exchange_info to the Lakehouse registry."""
    lp = Path(lake_path).resolve()
    con = get_connection(lake_path=lp)

    # Store the symbol registry in the 'catalog' (SQLite file)
    # SQLite doesn't support multiple schemas, so it's just 'catalog.symbols'
    data = [
        ("BTCUSDT", "spot"),
        ("ETHUSDT", "spot"),
        ("BTCUSDT", "um"),
        ("ETHUSDT", "um"),
        ("BTCUSD_PERP", "cm"),
        ("ETHUSD_PERP", "cm"),
    ]
    con.execute("CREATE TABLE IF NOT EXISTS catalog.symbols (symbol TEXT, trade_type TEXT)")
    con.execute("DELETE FROM catalog.symbols")
    con.executemany("INSERT INTO catalog.symbols VALUES (?, ?)", data)
    con.close()


def get_symbols(trade_type: str, lake_path: str) -> list[str]:
    """Query symbols from the Lakehouse metadata registry."""
    lp = Path(lake_path).resolve()
    con = get_connection(lake_path=lp)
    # The get_connection utility attaches the SQLite file as 'catalog'
    try:
        res = con.execute(
            "SELECT symbol FROM catalog.symbols WHERE trade_type = ?", [trade_type]
        ).fetchall()
        return [r[0] for r in res]
    except Exception:
        return []
    finally:
        con.close()
