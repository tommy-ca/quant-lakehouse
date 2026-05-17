"""Shared DuckDB/DuckLake connection utilities.

Provides a single ``get_connection()`` function that connects to the
pipeline's DuckLake catalog or DuckDB database.
"""

from __future__ import annotations

import logging
from contextlib import suppress
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
        # dlt DuckLake creates a SQLite file for its metadata catalog,
        # regardless of the file extension or connection string used.
        # We try both common extensions used in this project.
        meta_sqlite = lp / "metadata.ducklake"
        if not meta_sqlite.exists():
            meta_sqlite = lp / "metadata.duckdb"

        if lp.is_dir() and meta_sqlite.exists():
            # Use an in-memory DuckDB session as the query engine
            con = duckdb.connect(":memory:")

            # Load extensions
            with suppress(Exception):
                con.execute("LOAD ducklake")

            # Attach the SQLite catalog as 'catalog'
            # This allows us to query metadata and create schemas/views in DuckDB
            con.execute(f"ATTACH '{meta_sqlite}' AS catalog (TYPE SQLITE)")

            # Map the tables in the catalog to the 'bronze' schema in DuckDB
            con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            with suppress(Exception):
                # Query the table registry from the attached SQLite catalog
                tables = con.execute(
                    "SELECT table_name FROM catalog.ducklake_table WHERE table_name NOT LIKE '_dlt%'"
                ).fetchall()
                for (table,) in set(tables):
                    # Data layout: ./lake/bronze/{table}/*.parquet
                    path = lp / "bronze" / table / "*.parquet"
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
