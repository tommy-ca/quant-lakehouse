"""Tests for dlt custom sources (Binance klines, archive, REST, WS)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from binance_datatool.common.enums import TradeType
from binance_datatool.common.types import KlineData
from binance_datatool.dlt_sources.binance import build_binance_source, klines_resource
from binance_datatool.dlt_sources.binance_archive import (
    archive_klines_resource,
    build_archive_source,
)
from binance_datatool.dlt_sources.binance_rest import (
    agg_trades_resource,
    build_rest_source,
    funding_rate_resource,
)
from binance_datatool.dlt_sources.binance_ws import build_ws_source, ws_klines_resource
from binance_datatool.dlt_sources.pipeline import build_pipeline

# ── REST Klines ──────────────────────────────────────────────────


class TestKlinesResource:
    """Tests for ``klines_resource`` — the dlt resource wrapping Binance REST API."""

    def test_resource_name_and_disposition(self):
        assert klines_resource.name == "klines"
        assert klines_resource.write_disposition == "merge"

    def test_resource_has_column_schema(self):
        cols = klines_resource.columns
        assert cols is not None
        assert cols["open_time"]["data_type"] == "bigint"
        assert cols["open"]["data_type"] == "double"

    def test_build_binance_source_returns_dlt_source(self):
        source = build_binance_source(
            symbols=["BTCUSDT", "ETHUSDT"], interval="1h", trade_type=TradeType.spot
        )
        assert source.name == "build_binance_source"
        resources = source.selected_resources
        assert len(resources) == 2
        assert "BTCUSDT_klines" in resources
        assert "ETHUSDT_klines" in resources

    def test_build_binance_source_raises_for_unsupported_types(self):
        with pytest.raises(NotImplementedError, match="aggTrades"):
            build_binance_source(symbols=["BTCUSDT"], data_type="aggTrades")

    @patch("binance_datatool.dlt_sources.binance.BinanceSpotRestClient")
    def test_resource_via_pipeline(self, mock_client_cls, tmp_path):
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
        db = str(tmp_path / "catalog.duckdb")
        pipeline = build_pipeline("test_klines", catalog_path=db, dataset_name="bronze")
        with patch(
            "binance_datatool.dlt_sources.binance.BinanceSpotRestClient", return_value=mock_client
        ):
            pipeline.run(source)

        import duckdb

        con = duckdb.connect(db)
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='bronze'"
        ).fetchall()
        names = [t[0] for t in tables]
        assert any("klines" in n for n in names), f"no klines table: {names}"
        rows = con.execute("SELECT symbol, open, close FROM bronze.BTCUSDT_klines").fetchall()
        assert len(rows) == 1
        assert rows[0] == ("BTCUSDT", 100.0, 100.5)
        con.close()

    @patch("binance_datatool.dlt_sources.binance.BinanceSpotRestClient")
    def test_resource_empty_response(self, mock_client_cls, tmp_path):
        mock_client = AsyncMock()
        mock_client.fetch_ohlcv.return_value = []
        mock_client_cls.return_value = mock_client

        source = build_binance_source(symbols=["BTCUSDT"], interval="1h")
        db = str(tmp_path / "catalog.duckdb")
        pipeline = build_pipeline("test_empty", catalog_path=db, dataset_name="bronze")
        with patch(
            "binance_datatool.dlt_sources.binance.BinanceSpotRestClient", return_value=mock_client
        ):
            pipeline.run(source)
        assert True


# ── Archive ───────────────────────────────────────────────────────


class TestArchiveResource:
    """Tests for ``archive_klines_resource`` — Binance archive ZIP reader."""

    def test_resource_has_merge_disposition(self):
        assert archive_klines_resource.write_disposition == "merge"

    def test_build_archive_source_returns_resources(self):
        source = build_archive_source(symbols=["BTCUSDT"], interval="1h", trade_type=TradeType.spot)
        assert "archive_BTCUSDT_klines" in source.selected_resources
        assert len(source.selected_resources) == 1


# ── REST AggTrades / FundingRate ─────────────────────────────────


class TestRestResources:
    """Tests for ``agg_trades_resource`` and ``funding_rate_resource``."""

    def test_agg_trades_disposition(self):
        assert agg_trades_resource.write_disposition == "merge"

    def test_funding_rate_disposition(self):
        assert funding_rate_resource.write_disposition == "merge"

    def test_build_rest_source_agg_trades(self):
        source = build_rest_source(
            symbols=["BTCUSDT"], data_type="aggTrades", trade_type=TradeType.spot
        )
        assert "rest_BTCUSDT_aggTrades" in source.selected_resources

    def test_build_rest_source_funding_rate(self):
        source = build_rest_source(
            symbols=["BTCUSDT"], data_type="fundingRate", trade_type=TradeType.um
        )
        assert "rest_BTCUSDT_fundingRate" in source.selected_resources

    @patch("binance_datatool.dlt_sources.binance_rest.BinanceSpotRestClient")
    def test_agg_trades_via_pipeline(self, mock_client_cls, tmp_path):
        mock_client = AsyncMock()
        mock_client.fetch_agg_trades.return_value = [
            {"a": 1, "p": "50000.0", "q": "0.5", "T": 1700000000000, "m": False}
        ]
        mock_client_cls.return_value = mock_client

        source = build_rest_source(symbols=["BTCUSDT"], data_type="aggTrades")
        db = str(tmp_path / "catalog.duckdb")
        pipeline = build_pipeline("test_agg", catalog_path=db, dataset_name="bronze")
        with patch(
            "binance_datatool.dlt_sources.binance_rest.BinanceSpotRestClient",
            return_value=mock_client,
        ):
            pipeline.run(source)

        import duckdb

        con = duckdb.connect(db)
        rows = con.execute(
            "SELECT symbol, price, quantity FROM bronze.rest_btcusdt_agg_trades"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0] == ("BTCUSDT", 50000.0, 0.5)
        con.close()


# ── WebSocket ─────────────────────────────────────────────────────


class TestWsResource:
    """Tests for ``ws_klines_resource`` — Binance WebSocket streaming."""

    def test_resource_has_append_disposition(self):
        assert ws_klines_resource.write_disposition == "append"

    def test_build_ws_source_returns_resources(self):
        source = build_ws_source(
            symbols=["BTCUSDT"], interval="1h", trade_type=TradeType.spot, max_items=5
        )
        assert "ws_BTCUSDT_klines" in source.selected_resources
        assert len(source.selected_resources) == 1


# ── Pipeline ──────────────────────────────────────────────────────


class TestPipeline:
    """Tests for dlt pipeline helpers."""

    def test_build_pipeline_defaults(self):
        pipeline = build_pipeline("test_pipeline")
        assert pipeline.pipeline_name == "test_pipeline"
        assert pipeline.dataset_name == "bronze"

    def test_build_pipeline_with_catalog_path(self, tmp_path):
        db = str(tmp_path / "lake" / "catalog.duckdb")
        pipeline = build_pipeline("test", catalog_path=db)
        assert pipeline.pipeline_name == "test"

    def test_run_source_returns_tables(self, tmp_path):
        """Verify run_source returns table names from a successful pipeline run."""

        source = build_binance_source(symbols=["BTCUSDT"], interval="1h")
        assert len(source.selected_resources) == 1
