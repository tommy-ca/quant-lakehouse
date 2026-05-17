"""Standard dlt destination builders for binance-datatool.

Provides ``build_pipeline`` and ``run_source`` to standardize how dlt
pipelines connect to DuckDB or DuckLake destinations.

DuckLake (default): lakehouse storage with catalog + filesystem.
  Automatic partitioning, ACID transactions, metadata management.
  Files stored in ``{lake_path}/data/``, catalog in ``{lake_path}/_catalog/``.

DuckDB (legacy): single-file database.
  Used when ``destination="duckdb"`` is explicitly requested.

This is the canonical location. Previously at ``dlt_sources.pipeline``.
"""

from __future__ import annotations

from pathlib import Path

import dlt


def build_pipeline(
    source_name: str,
    catalog_path: str | None = None,
    dataset_name: str = "bronze",
    destination: str = "ducklake",
    lake_path: str | None = None,
) -> dlt.Pipeline:
    """Build a dlt pipeline configured for DuckDB or DuckLake.

    Args:
        source_name: Unique pipeline name (e.g. ``"binance_spot"``).
        catalog_path: Path to ``catalog.duckdb`` (DuckDB destination only).
        dataset_name: dlt dataset schema name (default ``"bronze"``).
        destination: ``"ducklake"`` (default) or ``"duckdb"``.
        lake_path: Path to the lake directory (DuckLake storage + catalog).
            Defaults to parent of ``catalog_path`` or ``./lake``.

    Returns:
        Configured ``dlt.Pipeline``.
    """
    if destination == "duckdb":
        if catalog_path:
            Path(catalog_path).parent.mkdir(parents=True, exist_ok=True)
        dest: str | dlt.Destination = (
            dlt.destinations.duckdb(catalog_path) if catalog_path else "duckdb"
        )
    else:
        # DuckLake: lakehouse with catalog + storage
        _lp = lake_path or (str(Path(catalog_path).parent) if catalog_path else "./lake")
        lp_abs = str(Path(_lp).resolve())

        # DuckLake catalog is a sqlite file named metadata.ducklake by project convention
        catalog_abs = str(Path(lp_abs) / "metadata.ducklake")
        Path(lp_abs).mkdir(parents=True, exist_ok=True)

        dest = dlt.destinations.ducklake(
            credentials=dlt.destinations.impl.ducklake.configuration.DuckLakeCredentials(
                ducklake_name=source_name.replace("-", "_"),
                catalog=f"sqlite:///{catalog_abs}",
                storage=f"file://{lp_abs}",
            ),
        )

    return dlt.pipeline(
        pipeline_name=source_name,
        destination=dest,
        dataset_name=dataset_name,
        progress="log",
    )


def run_source(
    source: dlt.SupportsPipeline,
    *,
    source_name: str = "binance_spot",
    catalog_path: str | None = None,
    dataset_name: str = "bronze",
    destination: str = "ducklake",
    lake_path: str | None = None,
) -> dict:
    """Run a dlt source and return load info.

    Args:
        source: A dlt source or resource.
        source_name: Pipeline name.
        catalog_path: Path to ``catalog.duckdb`` (DuckDB destination only).
        dataset_name: dlt dataset schema name.
        destination: ``"ducklake"`` (default) or ``"duckdb"``.
        lake_path: DuckLake storage path (ignored for ``"duckdb"``).

    Returns:
        Dict with ``load_info``, ``dataset_name``, and ``tables_loaded``.
    """
    pipeline = build_pipeline(source_name, catalog_path, dataset_name, destination, lake_path)
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
