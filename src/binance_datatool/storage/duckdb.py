"""Shared DuckDB/DuckLake connection utilities.

Provides a single ``get_connection()`` function that connects to the
pipeline's DuckLake catalog or DuckDB database — no manual ``LOAD ducklake``
or ``ATTACH`` needed.

This is the canonical location for storage operations. Previously lived at
``workflow.db`` — old import path still works via backward-compat re-export.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb


def get_connection(
    lake_path: str | Path | None = None,
    catalog_path: str | Path | None = None,
) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection to the pipeline's storage.

    Args:
        lake_path: DuckLake lake directory (contains ``metadata.ducklake``).
            When set, connects to the DuckLake catalog.
        catalog_path: Direct path to a ``.duckdb`` file (DuckDB destination).
            Used when ``lake_path`` is not set.

    Returns:
        A DuckDB connection with DuckLake attached (if lake_path) or
        directly connected to the database file.
    """
    if lake_path:
        lp = Path(lake_path)
        if lp.is_dir() and (lp / "metadata.ducklake").exists():
            con = duckdb.connect(str(lp / "catalog.duckdb"))
            con.execute("LOAD ducklake")
            con.execute(
                f"ATTACH 'ducklake:{lp / 'metadata.ducklake'}' AS dl "
                f"(DATA_PATH '{lp}/data', AUTOMATIC_MIGRATION true)"
            )
            con.execute("USE dl")
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
    """Write a Polars Arrow table to a silver DuckDB table.

    Uses Arrow zero-copy (no pandas). Creates schema + table if missing.

    Args:
        con: DuckDB connection.
        table_name: Target table name (e.g. ``"klines"``).
        arrow_table: PyArrow Table from ``pl.DataFrame.to_arrow()``.
        symbol: Trading symbol (for DELETE before INSERT).
        schema: Target schema name (default ``"silver"``).

    Returns:
        Number of rows inserted.
    """
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} AS
        SELECT * FROM arrow_table WHERE FALSE
    """)
    con.execute(f"DELETE FROM {schema}.{table_name} WHERE symbol = ?", [symbol])
    con.execute(f"INSERT INTO {schema}.{table_name} SELECT * FROM arrow_table")
    return arrow_table.num_rows


def query_silver(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    symbol: str | None = None,
    schema: str = "silver",
):
    """Query a silver table through the DuckLake catalog.

    Args:
        con: DuckDB connection.
        table_name: Table name.
        symbol: Optional symbol filter.
        schema: Schema name.

    Returns:
        Query result rows.
    """
    if symbol:
        return con.execute(
            f"SELECT * FROM {schema}.{table_name} WHERE symbol = ?", [symbol]
        ).fetchall()
    return con.execute(f"SELECT * FROM {schema}.{table_name}").fetchall()
