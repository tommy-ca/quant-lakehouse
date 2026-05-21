from unittest.mock import patch

import pytest
from prefect.testing.utilities import prefect_test_harness

from binance_datatool.workflow.prefect_flows import universe_maintenance_flow


@pytest.fixture(autouse=True)
def prefect_harness():
    with prefect_test_harness():
        yield


def test_universe_maintenance_flow_execution():
    """Verify the Universe Maintenance Flow orchestrates correctly."""

    with (
        patch("binance_datatool.workflow.prefect_flows.sync_metadata_task") as mock_sync_meta,
        patch(
            "binance_datatool.workflow.prefect_flows.build_universe_stats_task"
        ) as mock_build_stats,
        patch("prefect.concurrency.sync.concurrency") as mock_concurrency,
    ):
        # Setup mock concurrency guard
        mock_concurrency.return_value.__enter__.return_value = None

        # Execute the flow with explicit date
        universe_maintenance_flow(lake_path="fake_lake", target_date="2024-01-01")

        # Verify step 1: Metadata sync was called
        mock_sync_meta.assert_called_once_with("fake_lake")

        # Verify step 2: Gold stats build was called
        mock_build_stats.assert_called_once_with(lake_path="fake_lake", target_date="2024-01-01")

        # Verify concurrency guard was requested
        mock_concurrency.assert_called_once_with("ducklake-writer", occupy=1)


def test_universe_maintenance_flow_lookback():
    """Verify the Universe Maintenance Flow backfills correctly."""

    with (
        patch("binance_datatool.workflow.prefect_flows.sync_metadata_task") as mock_sync_meta,
        patch(
            "binance_datatool.workflow.prefect_flows.build_universe_stats_task"
        ) as mock_build_stats,
        patch("prefect.concurrency.sync.concurrency") as mock_concurrency,
    ):
        mock_concurrency.return_value.__enter__.return_value = None

        # Execute with lookback_days=3
        universe_maintenance_flow(lake_path="fake_lake", lookback_days=3)

        # Metadata sync called once
        mock_sync_meta.assert_called_once_with("fake_lake")

        # Build stats called 3 times
        assert mock_build_stats.call_count == 3
