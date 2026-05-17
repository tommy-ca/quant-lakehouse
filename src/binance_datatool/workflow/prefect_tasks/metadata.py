from prefect import task

from binance_datatool.common.metadata_registry import sync_exchange_metadata


@task
def sync_metadata_task(lake_path: str):
    sync_exchange_metadata(lake_path)
