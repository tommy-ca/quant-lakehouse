from pathlib import Path

from prefect import task

from binance_datatool.universe.gold_pipeline import build_daily_universe_stats


@task(name="Build Daily Universe Stats")
def build_universe_stats_task(lake_path: str | Path, target_date: str | None = None):
    """Prefect task to build daily historical universe statistics in the Gold layer."""
    build_daily_universe_stats(lake_path=lake_path, target_date=target_date)
