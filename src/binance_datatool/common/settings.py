"""Application settings via pydantic-settings (.env support).

Configures Prefect task runners, default paths, and pipeline parameters
via environment variables or ``.env`` file at project root.

Usage:
    from binance_datatool.common.settings import settings

    workers = settings.prefect_max_workers
    home = settings.archive_home
"""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Prefect task runner
    prefect_max_workers: int = 8

    # Default paths
    archive_home: Path = Path.home() / ".binance-datatool" / "archive"
    catalog_path: Path | None = None  # defaults to archive_home.parent / "lake"

    # Pipeline defaults
    default_lookback_days: int = 30
    default_interval: str = "1h"

    # REST API
    rest_timeout_seconds: int = 30
    rest_retries: int = 3

    # S3 archive
    s3_endpoint: str = "https://s3-ap-northeast-1.amazonaws.com"
    s3_bucket: str = "data.binance.vision"

    # Metadata cache TTL in seconds (default 1 hour)
    cache_ttl_seconds: int = 3600

    # Universe Construction
    universe_volume_weight: float = 0.7
    universe_mcap_weight: float = 0.3
    universe_min_age_days: int = 180
    universe_min_volume_usd: float = 1_000_000
    universe_mcap_multiplier: float = 10.0


settings = Settings()
