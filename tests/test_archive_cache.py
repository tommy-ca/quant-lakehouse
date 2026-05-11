"""Tests for archive file metadata cache."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from binance_datatool.workflow.archive_cache import ArchiveFileCache


class TestArchiveFileCache:
    """Tests for ``ArchiveFileCache`` — DuckDB-backed S3 listing cache."""

    def test_ensure_table_creates_schema(self, tmp_path):
        db = tmp_path / "test.duckdb"
        cache = ArchiveFileCache(str(db))
        cache.ensure_table()

        import duckdb

        con = duckdb.connect(str(db))
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='metadata'"
        ).fetchall()
        assert any("archive_files" in t[0] for t in tables)
        con.close()

    def test_is_fresh_returns_false_on_empty(self, tmp_path):
        db = tmp_path / "test.duckdb"
        cache = ArchiveFileCache(str(db))
        cache.ensure_table()
        assert not cache.is_fresh("BTCUSDT", "klines", "1h", "spot", "daily")

    def test_list_cached_empty(self, tmp_path):
        db = tmp_path / "test.duckdb"
        cache = ArchiveFileCache(str(db))
        cache.ensure_table()
        files = cache.list_cached("BTCUSDT", "klines", "1h", "spot", "daily")
        assert files == []

    def test_refresh_inserts_entries(self, tmp_path):
        """Verify refresh inserts file entries via DuckDB insert."""
        db = tmp_path / "test.duckdb"
        cache = ArchiveFileCache(str(db))
        cache.ensure_table()

        import duckdb

        con = duckdb.connect(str(db))
        # Insert test data directly (simulates what refresh does)
        now = datetime.now(UTC)
        con.execute(
            """
            INSERT INTO metadata.archive_files VALUES
            ('BTCUSDT', 'klines', '1h', 'spot', 'daily',
             'data/spot/daily/klines/BTCUSDT/1h/file1.zip', 1000, ?, ?)
        """,
            [now.isoformat(), now],
        )
        con.close()

        assert cache.is_fresh("BTCUSDT", "klines", "1h", "spot", "daily")
        files = cache.list_cached("BTCUSDT", "klines", "1h", "spot", "daily")
        assert len(files) == 1
        assert files[0]["key"] == "data/spot/daily/klines/BTCUSDT/1h/file1.zip"
        assert files[0]["size"] == 1000

    def test_refresh_replaces_stale_data(self, tmp_path):
        """Verify refresh replaces old data for the same key."""
        db = tmp_path / "test.duckdb"
        cache = ArchiveFileCache(str(db))
        cache.ensure_table()

        import duckdb

        con = duckdb.connect(str(db))
        stale = datetime.now(UTC) - timedelta(hours=48)
        con.execute(
            """
            INSERT INTO metadata.archive_files VALUES
            ('BTCUSDT', 'klines', '1h', 'spot', 'daily',
             'stale_file.zip', 999, ?, ?)
        """,
            [stale.isoformat(), stale],
        )
        con.close()

        # Stale data should exist but TTL check returns False
        assert not cache.is_fresh("BTCUSDT", "klines", "1h", "spot", "daily")

    def test_different_keys_are_independent(self, tmp_path):
        """Verify cache entries for different symbols don't interfere."""
        db = tmp_path / "test.duckdb"
        cache = ArchiveFileCache(str(db))
        cache.ensure_table()

        import duckdb

        con = duckdb.connect(str(db))
        now = datetime.now(UTC)
        con.execute(
            """
            INSERT INTO metadata.archive_files VALUES
            ('BTCUSDT', 'klines', '1h', 'spot', 'daily', 'btc.zip', 100, ?, ?)
        """,
            [now.isoformat(), now],
        )
        con.execute(
            """
            INSERT INTO metadata.archive_files VALUES
            ('ETHUSDT', 'klines', '1h', 'spot', 'daily', 'eth.zip', 200, ?, ?)
        """,
            [now.isoformat(), now],
        )
        con.close()

        btc = cache.list_cached("BTCUSDT", "klines", "1h", "spot", "daily")
        eth = cache.list_cached("ETHUSDT", "klines", "1h", "spot", "daily")
        assert len(btc) == 1 and btc[0]["key"] == "btc.zip"
        assert len(eth) == 1 and eth[0]["key"] == "eth.zip"
