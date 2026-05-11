"""dlt pipeline helpers for binance-datatool.

Provides ``build_pipeline`` and ``run_source`` to standardize how dlt
pipelines connect to the project's DuckDB/DuckLake catalog.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import dlt

if TYPE_CHECKING:
    from pathlib import Path


def build_pipeline(
    source_name: str,
    catalog_path: Path | None = None,
    dataset_name: str = "bronze",
) -> dlt.Pipeline:
    """Build a dlt pipeline configured for the project's DuckDB catalog.

    Args:
        source_name: Unique pipeline name (e.g. ``"binance_spot"``).
        catalog_path: Path to the lake directory. When provided, dlt writes
            directly to ``catalog.duckdb`` so SQLMesh can share the same DB.
        dataset_name: dlt dataset schema name (default ``"bronze"``).

    Returns:
        Configured ``dlt.Pipeline``.
    """
    destination: str | dlt.Destination
    if catalog_path:
        db_path = str(catalog_path / "catalog.duckdb")
        destination = dlt.destinations.duckdb(db_path)
    else:
        destination = "duckdb"

    return dlt.pipeline(
        pipeline_name=source_name,
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
    )


def run_source(
    source: dlt.SupportsPipeline,
    *,
    source_name: str = "binance_spot",
    catalog_path: Path | None = None,
    dataset_name: str = "bronze",
) -> dict:
    """Run a dlt source and return load info.

    Args:
        source: A dlt source (from ``build_binance_source`` or similar).
        source_name: Pipeline name.
        catalog_path: Passed to ``build_pipeline``.
        dataset_name: Passed to ``build_pipeline``.

    Returns:
        Dict with ``load_info``, ``dataset_name``, and ``tables_loaded``.
    """
    pipeline = build_pipeline(source_name, catalog_path, dataset_name)
    load_info = pipeline.run(source)
    schema = pipeline.default_schema
    return {
        "load_info": str(load_info),
        "dataset_name": dataset_name,
        "tables_loaded": [t["name"] for t in schema.data_tables()] if schema else [],
    }
