"""Archive file metadata cache — DuckDB-backed S3 listing cache.

The Binance S3 archive paginates listings (~1000 files/page).
For BTCUSDT 1d, a full listing takes ~30s and 7 HTTP requests.

This module caches the listing in a DuckDB ``metadata.archive_files``
table so subsequent pipeline runs avoid the S3 cost.  The cache is
refreshed explicitly (via Prefect task) or implicitly when stale.

Usage::

    from binance_datatool.workflow.archive_cache import ArchiveFileCache

    cache = ArchiveFileCache("path/to/catalog.duckdb")
    files = cache.list_files("spot", "klines", "BTCUSDT", "1d")
    # Returns cached data if fresh, otherwise lists S3 and caches.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

import duckdb


class ArchiveFileCache:
    """DuckDB-backed cache for Binance S3 archive file listings.

    Stores file metadata in the ``metadata.archive_files`` table with
    columns: ``symbol, data_type, interval, trade_type, freq, key, size,
    last_modified, fetched_at``.

    Cache entries are keyed on ``(symbol, data_type, interval, trade_type, freq)``.
    """

    _TTL_HOURS = 24

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)

    def ensure_table(self) -> None:
        """Create the metadata schema and archive_files table if missing."""
        con = duckdb.connect(self._db_path)
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS metadata")
            con.execute("""
                CREATE TABLE IF NOT EXISTS metadata.archive_files (
                    symbol      VARCHAR NOT NULL,
                    data_type   VARCHAR NOT NULL,
                    interval    VARCHAR,
                    trade_type  VARCHAR NOT NULL,
                    freq        VARCHAR NOT NULL,
                    key         VARCHAR NOT NULL,
                    size        BIGINT,
                    last_modified TIMESTAMP,
                    fetched_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
        finally:
            con.close()

    def is_fresh(
        self, symbol: str, data_type: str, interval: str | None, trade_type: str, freq: str
    ) -> bool:
        """Check if cache has fresh data for the given key."""
        con = duckdb.connect(self._db_path)
        try:
            row = con.execute(
                "SELECT COUNT(*), MAX(fetched_at) FROM metadata.archive_files "
                "WHERE symbol = ? AND data_type = ? "
                "AND (interval = ? OR (interval IS NULL AND ? IS NULL)) "
                "AND trade_type = ? AND freq = ?",
                [symbol, data_type, interval, interval, trade_type, freq],
            ).fetchone()
            count, max_fetched = row
            if count == 0 or max_fetched is None:
                return False
            age = datetime.now(UTC) - max_fetched.replace(tzinfo=UTC)
            return age < timedelta(hours=self._TTL_HOURS)
        finally:
            con.close()

    def list_cached(
        self, symbol: str, data_type: str, interval: str | None, trade_type: str, freq: str
    ) -> list[dict[str, Any]]:
        """Return cached file entries for the given key.

        Returns dicts with ``key, size, last_modified`` matching the
        shape produced by ``ArchiveClient.list_symbol_files()``.
        """
        con = duckdb.connect(self._db_path)
        try:
            rows = con.execute(
                "SELECT key, size, last_modified FROM metadata.archive_files "
                "WHERE symbol = ? AND data_type = ? "
                "AND (interval = ? OR (interval IS NULL AND ? IS NULL)) "
                "AND trade_type = ? AND freq = ? "
                "ORDER BY key",
                [symbol, data_type, interval, interval, trade_type, freq],
            ).fetchall()
            result: list[dict[str, Any]] = []
            for r in rows:
                entry: dict[str, Any] = {"key": r[0], "size": r[1]}
                lm = r[2]
                if isinstance(lm, str):
                    from datetime import datetime as dt

                    lm = dt.fromisoformat(lm)
                entry["last_modified"] = lm
                result.append(entry)
            return result
        finally:
            con.close()

    def refresh(
        self,
        symbol: str,
        data_type: str,
        interval: str | None,
        trade_type: str,
        freq: str,
    ) -> int:
        """List S3 archive and update the cache.

        Args:
            symbol: Trading pair.
            data_type: Dataset type (klines, aggTrades, etc.).
            interval: Kline interval or None.
            trade_type: Market segment (spot, um, cm).
            freq: Data frequency (daily, monthly).

        Returns:
            Number of files cached.
        """
        from binance_datatool.archive.client import ArchiveClient
        from binance_datatool.common.enums import DataFrequency, DataType, TradeType

        dt_enum = DataType(data_type)
        tt_enum = TradeType(trade_type)
        df_enum = DataFrequency(freq)

        files = asyncio.run(
            ArchiveClient().list_symbol_files(
                trade_type=tt_enum,
                data_freq=df_enum,
                data_type=dt_enum,
                symbol=symbol,
                interval=interval,
            )
        )
        now = datetime.now(UTC)

        con = duckdb.connect(self._db_path)
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS metadata")
            # Remove stale entries for this key
            con.execute(
                "DELETE FROM metadata.archive_files "
                "WHERE symbol = ? AND data_type = ? "
                "AND (interval = ? OR (interval IS NULL AND ? IS NULL)) "
                "AND trade_type = ? AND freq = ?",
                [symbol, data_type, interval, interval, trade_type, freq],
            )
            # Insert fresh entries
            for f in files:
                con.execute(
                    "INSERT INTO metadata.archive_files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        symbol,
                        data_type,
                        interval,
                        trade_type,
                        freq,
                        f.key,
                        f.size,
                        f.last_modified.isoformat() if f.last_modified else None,
                        now,
                    ],
                )
            return len(files)
        finally:
            con.close()
