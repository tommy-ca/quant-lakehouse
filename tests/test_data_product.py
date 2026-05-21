from unittest.mock import patch

import pytest

from binance_datatool.workflow.prefect_flows import backtesting_data_product_flow


@pytest.fixture
def mock_universe_builder():
    with patch("binance_datatool.universe.builder.UniverseBuilder") as mock:
        builder_instance = mock.return_value
        builder_instance.build_top_50.return_value = ["BTCUSDT", "ETHUSDT"]
        yield builder_instance


@pytest.fixture
def mock_dlt_pipeline():
    with patch("binance_datatool.workflow.prefect_flows.dlt_historical_pipeline") as mock:
        mock.return_value = {"BTCUSDT": {"healthy": True}, "ETHUSDT": {"healthy": True}}
        yield mock


@pytest.fixture
def mock_maintenance():
    with patch("binance_datatool.workflow.prefect_flows.universe_maintenance_flow") as mock:
        yield mock


def test_backtesting_data_product_flow_orchestration(
    mock_universe_builder, mock_dlt_pipeline, mock_maintenance, tmp_path
):
    lake_path = tmp_path / "lake"
    lake_path.mkdir()

    report = backtesting_data_product_flow(
        trade_types=["spot"], data_types=["klines"], lookback_days=10, lake_path=str(lake_path)
    )

    # 1. Verify Maintenance was called
    mock_maintenance.assert_called_once()

    # 2. Verify Universe Builder was called
    mock_universe_builder.build_top_50.assert_called_with(trade_type="spot")

    # 3. Verify DLT pipeline was called for the discovered symbols
    assert mock_dlt_pipeline.call_count == 1
    args, kwargs = mock_dlt_pipeline.call_args
    assert kwargs["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert kwargs["trade_type"] == "spot"
    assert kwargs["data_type"] == "klines"
    assert kwargs["lookback_days"] == 10

    # 4. Verify Report Structure
    assert "spot" in report
    assert report["spot"]["symbols_count"] == 2
    assert "klines" in report["spot"]["results"]
    assert report["spot"]["results"]["klines"]["BTCUSDT"]["healthy"] is True


def test_backtesting_data_product_flow_futures_mandatory_funding(
    mock_universe_builder, mock_dlt_pipeline, mock_maintenance, tmp_path
):
    lake_path = tmp_path / "lake"
    lake_path.mkdir()

    # Requesting only klines for UM futures
    report = backtesting_data_product_flow(
        trade_types=["um"], data_types=["klines"], lake_path=str(lake_path)
    )

    # DLT pipeline should be called TWICE: once for klines, once for mandatory fundingRate
    assert mock_dlt_pipeline.call_count == 2

    calls = mock_dlt_pipeline.call_args_list
    data_types_called = [c[1]["data_type"] for c in calls]
    assert "klines" in data_types_called
    assert "fundingRate" in data_types_called

    assert "fundingRate" in report["um"]["results"]
