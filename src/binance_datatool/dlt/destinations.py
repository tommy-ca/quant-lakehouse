"""Standard dlt destination builders for binance-datatool.

Provides ``build_pipeline`` and ``run_source`` to standardize how dlt
pipelines connect to DuckDB or DuckLake destinations.
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
    """Build a dlt pipeline configured for DuckDB or DuckLake."""

    lp_abs = str(Path(lake_path or "./lake").resolve())
    Path(lp_abs).mkdir(parents=True, exist_ok=True)

    if destination == "duckdb":
        # Standard DuckDB destination
        db_path = catalog_path or str(Path(lp_abs) / "catalog.duckdb")
        dest = dlt.destinations.duckdb(db_path)
    else:
        # DuckLake: lakehouse with catalog + storage
        catalog_abs = str(Path(lp_abs) / "metadata.duckdb")
        dest = dlt.destinations.ducklake(
            credentials=dlt.destinations.impl.ducklake.configuration.DuckLakeCredentials(
                ducklake_name=source_name.replace("-", "_"),
                catalog=f"duckdb:///{catalog_abs}",
                storage=f"file://{lp_abs}",
            ),
            dataset_name=dataset_name,
            local_dir=lp_abs,
            override_data_path=True,
        )

    # Use a lake-specific pipelines directory to avoid state collision
    pipelines_dir = str(Path(lp_abs) / ".dlt" / "pipelines")
    Path(pipelines_dir).mkdir(parents=True, exist_ok=True)

    return dlt.pipeline(
        pipeline_name=source_name,
        pipelines_dir=pipelines_dir,
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
    pipeline = build_pipeline(source_name, catalog_path, dataset_name, destination, lake_path)
    extract_info = pipeline.extract(source)
    schema = pipeline.default_schema
    tables_loaded: list[str] = []
    if schema:
        tables_loaded = [t["name"] for t in schema.data_tables()]
    return {
        "extract_info": str(extract_info),
        "source_name": source_name,
        "dataset_name": dataset_name,
        "tables_loaded": tables_loaded,
    }


def load_source(
    source_name: str = "binance_spot",
    catalog_path: str | None = None,
    dataset_name: str = "bronze",
    destination: str = "ducklake",
    lake_path: str | None = None,
) -> dict:
    pipeline = build_pipeline(source_name, catalog_path, dataset_name, destination, lake_path)
    pipeline.normalize()
    load_info = pipeline.load()
    return {"load_info": str(load_info)}
