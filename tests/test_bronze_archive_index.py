"""Tests for bronze archive file index."""

from __future__ import annotations

from pathlib import Path

from binance_datatool.dlt_sources.bronze_archive_index import (
    _parse_path,
    archive_files_resource,
    build_archive_index_source,
)


class TestParsePath:
    """Tests for ``_parse_path`` — archive file path parser."""

    def test_spot_klines(self):
        meta = _parse_path(
            "data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-klines-1h-2024-01-01.zip", Path("/a")
        )
        assert meta is not None
        assert meta["trade_type"] == "spot"
        assert meta["data_type"] == "klines"
        assert meta["symbol"] == "BTCUSDT"
        assert meta["interval"] == "1h"
        assert meta["date"] == "2024-01-01"

    def test_um_funding_rate(self):
        meta = _parse_path(
            "data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip",
            Path("/a"),
        )
        assert meta is not None
        assert meta["trade_type"] == "um"
        assert meta["freq"] == "monthly"
        assert meta["data_type"] == "fundingRate"
        assert meta["symbol"] == "BTCUSDT"
        assert meta["date"] == "2024-01"

    def test_cm_agg_trades(self):
        meta = _parse_path(
            "data/futures/cm/daily/aggTrades/BTCUSD_PERP/BTCUSD_PERP-aggTrades-2024-01-01.zip",
            Path("/a"),
        )
        assert meta is not None
        assert meta["trade_type"] == "cm"
        assert meta["symbol"] == "BTCUSD_PERP"
        assert meta["date"] == "2024-01-01"

    def test_spot_agg_trades_no_interval(self):
        meta = _parse_path(
            "data/spot/daily/aggTrades/ETHUSDT/ETHUSDT-aggTrades-2024-06-15.zip", Path("/a")
        )
        assert meta is not None
        assert meta["symbol"] == "ETHUSDT"
        assert meta["interval"] is None
        assert meta["data_type"] == "aggTrades"

    def test_malformed_path_returns_none(self):
        assert _parse_path("not_data/something.txt", Path("/a")) is None
        assert _parse_path("data/spot/daily/klines/BTCUSDT.xyz", Path("/a")) is None

    def test_file_name_extracted(self):
        meta = _parse_path("data/spot/daily/trades/BTCUSDT/trades-2024-01-01.zip", Path("/a"))
        assert meta is not None
        assert meta["file_name"] == "trades-2024-01-01.zip"


class TestArchiveFilesResource:
    """Tests for ``archive_files_resource``."""

    def test_resource_name_and_disposition(self):
        assert archive_files_resource.name == "archive_files"
        assert archive_files_resource.write_disposition == "replace"

    def test_empty_archive_returns_resource(self, tmp_path):
        """Verify resource handles nonexistent archive gracefully."""
        res = archive_files_resource(archive_home=str(tmp_path / "nonexistent"))
        assert res.name == "archive_files"
        assert res.write_disposition == "replace"

    def test_build_source_returns_source(self):
        source = build_archive_index_source()
        assert source.name == "build_archive_index_source"
        assert "archive_files" in source.selected_resources
