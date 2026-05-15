"""Tests for s5cmd-backed archive download client."""

from __future__ import annotations

from typing import TYPE_CHECKING

from binance_datatool.archive.s5cmd_download import _check_s5cmd, download_files, read_zips

if TYPE_CHECKING:
    from pathlib import Path


class TestCheckS5cmd:
    def test_s5cmd_available(self):
        """s5cmd should be installed in the environment."""
        assert _check_s5cmd(), "s5cmd not found on PATH"


class TestDownloadFiles:
    def test_empty_keys_returns_empty(self):
        result = download_files([])
        assert result == []

    def test_download_single_file(self, tmp_path: Path):
        """Download a single archive file from Binance S3 using s5cmd."""
        key = "data/spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2026-05-12.zip"
        result = download_files([key], target_dir=tmp_path)
        assert len(result) == 1
        assert result[0].exists()
        assert result[0].suffix == ".zip"
        assert result[0].stat().st_size > 0

    def test_download_multiple_files(self, tmp_path: Path):
        """Download multiple archive files in parallel."""
        keys = [
            "data/spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2026-05-11.zip",
            "data/spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2026-05-12.zip",
        ]
        result = download_files(keys, target_dir=tmp_path, concurrency=5)
        assert len(result) == 2
        for path in result:
            assert path.exists()
            assert path.stat().st_size > 0


class TestReadZips:
    def test_read_valid_zips(self, tmp_path: Path):
        """Download with s5cmd and read CSV content from ZIPs."""
        key = "data/spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2026-05-12.zip"
        paths = download_files([key], target_dir=tmp_path)
        results = list(read_zips(paths))
        assert len(results) == 1
        csv_name, text = results[0]
        assert csv_name.endswith(".csv")
        assert len(text) > 0
        assert "," in text

    def test_empty_zips_returns_empty(self):
        results = list(read_zips([]))
        assert results == []
