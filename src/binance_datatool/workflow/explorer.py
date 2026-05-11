"""Archive explorer — discover venues, data types, frequencies, symbols.

Builds and queries DuckDB metadata tables for archive exploration.

Usage::

    explorer = ArchiveExplorer(\"path/to/catalog.duckdb\")

    # Refresh from live S3
    import asyncio
    asyncio.run(explorer.refresh_all())

    # Query from cached metadata
    explorer.list_venues()         # [\"spot\", \"um\", \"cm\"]
    explorer.list_data_types(\"spot\")   # [\"klines\", \"aggTrades\", ...]
    explorer.list_symbols(\"spot\", \"klines\", \"1h\")
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import duckdb

from binance_datatool.archive.client import ArchiveClient
from binance_datatool.common.enums import DataFrequency, DataType, TradeType

ALL_TRADE_TYPES = ["spot", "um", "cm"]
ALL_FREQUENCIES = ["daily", "monthly"]
ALL_DATA_TYPES = [
    "klines",
    "aggTrades",
    "trades",
    "fundingRate",
    "bookDepth",
    "bookTicker",
    "indexPriceKlines",
    "markPriceKlines",
    "premiumIndexKlines",
    "metrics",
]
INTERVAL_TYPES = {"klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"}


class ArchiveExplorer:
    """Explore the Binance S3 archive structure via cached DuckDB tables."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    def ensure_tables(self) -> None:
        con = duckdb.connect(self._db_path)
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS metadata")
            con.execute("""
                CREATE TABLE IF NOT EXISTS metadata.venues (
                    trade_type VARCHAR PRIMARY KEY,
                    data_types VARCHAR,
                    symbol_count BIGINT DEFAULT 0,
                    refreshed_at TIMESTAMP
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS metadata.data_types (
                    trade_type VARCHAR, data_type VARCHAR,
                    frequency VARCHAR, symbol_count BIGINT DEFAULT 0,
                    refreshed_at TIMESTAMP,
                    PRIMARY KEY (trade_type, data_type, frequency)
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS metadata.intervals (
                    trade_type VARCHAR, data_type VARCHAR,
                    interval VARCHAR, symbol_count BIGINT DEFAULT 0,
                    refreshed_at TIMESTAMP,
                    PRIMARY KEY (trade_type, data_type, interval)
                )
            """)
        finally:
            con.close()

    # ── S3 Scanning ──────────────────────────────────────────────

    async def refresh_all(self) -> dict[str, Any]:
        await self.refresh_venues()
        dt_count = await self._refresh_data_types()
        iv_count = await self._refresh_intervals()
        return {"venues": len(ALL_TRADE_TYPES), "data_types": dt_count, "intervals": iv_count}

    async def refresh_venues(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        con = duckdb.connect(self._db_path)
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS metadata")
            now = datetime.now(UTC)
            for tt in ALL_TRADE_TYPES:
                dtypes = await self._scan_data_types_for(tt)
                con.execute(
                    "INSERT OR REPLACE INTO metadata.venues VALUES (?, ?, ?, ?)",
                    [tt, ",".join(dtypes), 0, now],
                )
                results.append({"trade_type": tt, "data_types": dtypes})
        finally:
            con.close()
        return results

    async def _scan_data_types_for(self, trade_type: str) -> list[str]:
        client = ArchiveClient()
        tt = TradeType(trade_type)
        available: list[str] = []
        for dt_name in ALL_DATA_TYPES:
            try:
                dt = DataType(dt_name)
                prefixes = await client.list_dir(
                    await client._create_session(),
                    f"data/{tt.s3_path}/daily/{dt.value}/",
                )
                if prefixes:
                    available.append(dt_name)
            except Exception:
                continue
        return available

    async def _refresh_data_types(self) -> int:
        con = duckdb.connect(self._db_path)
        try:
            now = datetime.now(UTC)
            count = 0
            for tt in ALL_TRADE_TYPES:
                for freq in ALL_FREQUENCIES:
                    dtypes = await self._scan_data_types_for_freq(tt, freq)
                    for dt in dtypes:
                        con.execute(
                            "INSERT OR REPLACE INTO metadata.data_types VALUES (?, ?, ?, 0, ?)",
                            [tt, dt, freq, now],
                        )
                        count += 1
            return count
        finally:
            con.close()

    async def _scan_data_types_for_freq(self, trade_type: str, freq: str) -> list[str]:
        client = ArchiveClient()
        tt = TradeType(trade_type)
        freq_enum = DataFrequency(freq)
        prefixes = await client.list_dir(
            await client._create_session(),
            f"data/{tt.s3_path}/{freq_enum.value}/",
        )
        return [p.rstrip("/").split("/")[-1] for p in prefixes]

    async def _refresh_intervals(self) -> int:
        con = duckdb.connect(self._db_path)
        try:
            now = datetime.now(UTC)
            count = 0
            rows = con.execute(
                "SELECT DISTINCT trade_type, data_type FROM metadata.data_types"
            ).fetchall()
            for tt, dt in rows:
                if dt not in INTERVAL_TYPES:
                    continue
                intervals = await self._scan_intervals(tt, dt)
                for iv in intervals:
                    con.execute(
                        "INSERT OR REPLACE INTO metadata.intervals VALUES (?, ?, ?, 0, ?)",
                        [tt, dt, iv, now],
                    )
                    count += 1
            return count
        finally:
            con.close()

    async def _scan_intervals(self, trade_type: str, data_type: str) -> list[str]:
        client = ArchiveClient()
        tt = TradeType(trade_type)
        dt = DataType(data_type)
        for freq in ALL_FREQUENCIES:
            freq_enum = DataFrequency(freq)
            prefixes = await client.list_dir(
                await client._create_session(),
                f"data/{tt.s3_path}/{freq_enum.value}/{dt.value}/BTCUSDT/",
            )
            if prefixes:
                return [p.rstrip("/").split("/")[-1] for p in prefixes]
        return []

    # ── Queries ──────────────────────────────────────────────────

    def list_venues(self) -> list[str]:
        con = duckdb.connect(self._db_path)
        try:
            return [
                r[0]
                for r in con.execute(
                    "SELECT trade_type FROM metadata.venues ORDER BY trade_type"
                ).fetchall()
            ]
        finally:
            con.close()

    def list_data_types(self, trade_type: str) -> list[str]:
        con = duckdb.connect(self._db_path)
        try:
            return [
                r[0]
                for r in con.execute(
                    "SELECT DISTINCT data_type FROM metadata.data_types "
                    "WHERE trade_type = ? ORDER BY data_type",
                    [trade_type],
                ).fetchall()
            ]
        finally:
            con.close()

    def list_frequencies(self, trade_type: str, data_type: str) -> list[str]:
        con = duckdb.connect(self._db_path)
        try:
            return [
                r[0]
                for r in con.execute(
                    "SELECT frequency FROM metadata.data_types "
                    "WHERE trade_type = ? AND data_type = ? ORDER BY frequency",
                    [trade_type, data_type],
                ).fetchall()
            ]
        finally:
            con.close()

    def list_intervals(self, trade_type: str, data_type: str) -> list[str]:
        con = duckdb.connect(self._db_path)
        try:
            return [
                r[0]
                for r in con.execute(
                    "SELECT DISTINCT interval FROM metadata.intervals "
                    "WHERE trade_type = ? AND data_type = ? ORDER BY interval",
                    [trade_type, data_type],
                ).fetchall()
            ]
        finally:
            con.close()

    def list_symbols(
        self,
        trade_type: str,
        data_type: str,
        interval: str | None = None,
        freq: str | None = None,
    ) -> list[str]:
        """List symbols from the archive S3 (live listing)."""
        client = ArchiveClient()
        tt = TradeType(trade_type)
        dt = DataType(data_type)
        df = DataFrequency(freq) if freq else DataFrequency.daily
        return asyncio.run(client.list_symbols(tt, df, dt))

    def list_symbols_from_cache(
        self,
        trade_type: str,
    ) -> list[dict[str, Any]]:
        """Return cached symbols from the symbols metadata table."""
        con = duckdb.connect(self._db_path)
        try:
            rows = con.execute(
                "SELECT symbol, base_asset, quote_asset, status, contract_type, "
                "is_leverage, is_stable_pair FROM metadata.symbols "
                "WHERE trade_type = ? ORDER BY symbol",
                [trade_type],
            ).fetchall()
            return [
                {
                    "symbol": r[0],
                    "base_asset": r[1],
                    "quote_asset": r[2],
                    "status": r[3],
                    "contract_type": r[4],
                    "is_leverage": r[5],
                    "is_stable_pair": r[6],
                }
                for r in rows
            ]
        finally:
            con.close()
