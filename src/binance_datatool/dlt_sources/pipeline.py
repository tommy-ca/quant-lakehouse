"""dlt pipeline helpers for binance-datatool.

Provides ``build_pipeline`` and ``run_source`` to standardize how dlt
pipelines connect to the project's DuckDB/DuckLake catalog.
"""

from __future__ import annotations

import dlt


def build_pipeline(
    source_name: str,
    catalog_path: str | None = None,
    dataset_name: str = "bronze",
) -> dlt.Pipeline:
    """Build a dlt pipeline configured for the project's DuckDB catalog.

    Args:
        source_name: Unique pipeline name (e.g. ``"binance_spot"``).
        catalog_path: Full path to ``catalog.duckdb``. When provided, dlt writes
            directly to it so SQLMesh can share the same DB. Defaults to
            in-memory DuckDB when ``None``.
        dataset_name: dlt dataset schema name (default ``"bronze"``).

    Returns:
        Configured ``dlt.Pipeline``.
    """
    destination: str | dlt.Destination = (
        dlt.destinations.duckdb(catalog_path) if catalog_path else "duckdb"
    )

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
    catalog_path: str | None = None,
    dataset_name: str = "bronze",
) -> dict:
    """Run a dlt source and return load info.

    Args:
        source: A dlt source (from ``build_binance_source`` or similar).
        source_name: Pipeline name.
        catalog_path: Full path to ``catalog.duckdb``.
        dataset_name: dlt dataset schema name.

    Returns:
        Dict with ``load_info``, ``dataset_name``, and ``tables_loaded``.
    """
    pipeline = build_pipeline(source_name, catalog_path, dataset_name)
    load_info = pipeline.run(source)
    schema = pipeline.default_schema
    tables_loaded: list[str] = []
    if schema:
        tables_loaded = [t["name"] for t in schema.data_tables()]
    return {
        "load_info": str(load_info),
        "dataset_name": dataset_name,
        "tables_loaded": tables_loaded,
    }
