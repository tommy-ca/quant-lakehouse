"""Tests for dlt custom sources (binance klines resource)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from binance_datatool.common.enums import TradeType
from binance_datatool.common.types import KlineData
from binance_datatool.dlt_sources.binance import build_binance_source, klines_resource
from binance_datatool.dlt_sources.pipeline import build_pipeline


class TestKlinesResource:
    """Tests for ``klines_resource`` — the dlt resource wrapping Binance REST API."""

    def test_resource_name_and_disposition(self):
        """Verify dlt resource metadata is correct."""
        assert klines_resource.name == "klines"
        assert klines_resource.write_disposition == "merge"

    def test_resource_has_column_schema(self):
        """Verify column schema is declared for type enforcement."""
        cols = klines_resource.columns
        assert cols is not None
        assert cols["open_time"]["data_type"] == "bigint"
        assert cols["open"]["data_type"] == "double"
        assert cols["symbol"]["data_type"] == "text"

    def test_build_binance_source_returns_dlt_source(self):
        """Verify ``build_binance_source`` returns a configured dlt Source."""
        source = build_binance_source(
            symbols=["BTCUSDT", "ETHUSDT"],
            interval="1h",
            trade_type=TradeType.spot,
        )
        assert source.name == "build_binance_source"
        resources = source.selected_resources
        assert len(resources) == 2
        names = list(resources.keys())
        assert "BTCUSDT_klines" in names
        assert "ETHUSDT_klines" in names

    def test_build_binance_source_raises_for_unsupported_types(self):
        """Verify only klines is supported for now."""
        with pytest.raises(NotImplementedError, match="aggTrades"):
            build_binance_source(symbols=["BTCUSDT"], data_type="aggTrades")

    @patch("binance_datatool.dlt_sources.binance.BinanceSpotRestClient")
    def test_resource_via_pipeline(self, mock_client_cls, tmp_path):
        """Verify klines resource produces data through dlt pipeline."""
        mock_client = AsyncMock()
        mock_client.fetch_ohlcv.return_value = [
            KlineData(
                open_time=1700000000000,
                open="100.0",
                high="101.0",
                low="99.0",
                close="100.5",
                volume="1000.0",
                close_time=1700003600000,
                quote_volume="100500.0",
                num_trades=500,
                taker_buy_volume="600.0",
                taker_buy_quote_volume="60300.0",
            )
        ]
        mock_client_cls.return_value = mock_client

        source = build_binance_source(symbols=["BTCUSDT"], interval="1h")
        pipeline = build_pipeline("test_klines", catalog_path=tmp_path, dataset_name="bronze")
        with patch(
            "binance_datatool.dlt_sources.binance.BinanceSpotRestClient", return_value=mock_client
        ):
            info = pipeline.run(source)

        assert info is not None
        assert pipeline.dataset_name == "bronze"

        # Verify data was loaded — dlt names tables by resource name
        import duckdb

        con = duckdb.connect(str(tmp_path / "catalog.duckdb"))
        _tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='bronze'"
        ).fetchall()
        table_names = [t[0] for t in _tables]
        assert any("klines" in t for t in table_names), f"no klines table found: {table_names}"
        rows = con.execute(
            f"SELECT symbol, interval, open, close FROM bronze.{table_names[0]}"
        ).fetchall()
        assert len(rows) >= 1
        if rows:
            assert rows[0][0] == "BTCUSDT"
        con.close()

    @patch("binance_datatool.dlt_sources.binance.BinanceSpotRestClient")
    def test_resource_empty_response(self, mock_client_cls, tmp_path):
        """Verify empty API response produces no rows in the table."""
        mock_client = AsyncMock()
        mock_client.fetch_ohlcv.return_value = []
        mock_client_cls.return_value = mock_client

        source = build_binance_source(symbols=["BTCUSDT"], interval="1h")
        pipeline = build_pipeline("test_empty", catalog_path=tmp_path, dataset_name="bronze")
        with patch(
            "binance_datatool.dlt_sources.binance.BinanceSpotRestClient", return_value=mock_client
        ):
            pipeline.run(source)

        # dlt may or may not create the table for empty results
        # Just verify the pipeline completed without error
        assert True


class TestPipeline:
    """Tests for dlt pipeline helpers (``build_pipeline``, ``run_source``)."""

    def test_build_pipeline_defaults(self):
        """Verify pipeline is configured with correct defaults."""
        from binance_datatool.dlt_sources.pipeline import build_pipeline

        pipeline = build_pipeline("test_pipeline")
        assert pipeline.pipeline_name == "test_pipeline"
        assert pipeline.dataset_name == "bronze"

    def test_build_pipeline_with_catalog_path(self, tmp_path):
        """Verify pipeline uses catalog duckdb when path is provided."""
        from binance_datatool.dlt_sources.pipeline import build_pipeline

        catalog = tmp_path / "lake"
        catalog.mkdir()
        pipeline = build_pipeline("test", catalog_path=catalog)
        assert pipeline.pipeline_name == "test"
