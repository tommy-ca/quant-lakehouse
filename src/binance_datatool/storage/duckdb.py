"""Shared DuckDB/DuckLake connection utilities.

Provides a single ``get_connection()`` function that connects to the
pipeline's DuckLake catalog or DuckDB database.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger(__name__)


def get_connection(
    lake_path: str | Path | None = None,
    catalog_path: str | Path | None = None,
) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection to the pipeline's storage."""

    if lake_path:
        lp = Path(lake_path).resolve()
        catalog_file = lp / "metadata.duckdb"
        if lp.is_dir() and catalog_file.exists():
            con = duckdb.connect(str(catalog_file))
            con.execute("LOAD ducklake")
            # Map the tables to schemas using direct Parquet reads
            # as a reliable fallback/abstraction for the DuckLake driver.
            con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            tables = con.execute(
                "SELECT table_name FROM ducklake_table WHERE table_name NOT LIKE '_dlt%'"
            ).fetchall()
            for (table,) in set(tables):
                path = lp / "bronze" / "klines" / "*.parquet"
                con.execute(
                    f"CREATE OR REPLACE VIEW bronze.{table} AS SELECT * FROM read_parquet('{path}')"
                )
            return con

    # Fallback: direct DuckDB file
    db_file = catalog_path or (Path(lake_path) / "catalog.duckdb" if lake_path else ":memory:")
    return duckdb.connect(str(db_file))


def write_silver_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    arrow_table: Any,
    symbol: str,
    schema: str = "silver",
) -> int:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} AS SELECT * FROM arrow_table WHERE FALSE"
    )
    con.execute(f"DELETE FROM {schema}.{table_name} WHERE symbol = ?", [symbol])
    con.execute(f"INSERT INTO {schema}.{table_name} SELECT * FROM arrow_table")
    return arrow_table.num_rows


def query_silver(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    symbol: str | None = None,
    schema: str = "silver",
) -> list:
    if symbol:
        return con.execute(
            f"SELECT * FROM {schema}.{table_name} WHERE symbol = ?", [symbol]
        ).fetchall()
    return con.execute(f"SELECT * FROM {schema}.{table_name}").fetchall()
