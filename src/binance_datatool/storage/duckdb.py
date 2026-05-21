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
    """Get a DuckDB connection with native DuckLake support."""
    lp = Path(lake_path or "./lake").resolve()
    db_file = catalog_path or lp / "catalog.duckdb"
    lp.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_file))

    meta_sqlite = lp / "metadata.ducklake"
    if not meta_sqlite.exists():
        meta_sqlite = lp / "metadata.duckdb"

    if meta_sqlite.exists():
        try:
            con.execute("SET extension_directory = '/tmp/duckdb_extensions';")
            try:
                con.execute("LOAD ducklake;")
            except Exception:
                con.execute("INSTALL ducklake; LOAD ducklake;")

            # Use a consistent alias for the primary lakehouse
            alias = "lakehouse"

            attached = con.execute("PRAGMA database_list").fetchall()
            if not any(a[1] == alias for a in attached):
                con.execute(
                    f"ATTACH 'ducklake:sqlite:{meta_sqlite}' AS {alias} "
                    f"(DATA_PATH '{lp}/', OVERRIDE_DATA_PATH TRUE)"
                )

            # Expose all schemas from 'lakehouse' to the main connection
            schemas_res = con.execute(
                f"SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '{alias}'"
            ).fetchall()
            schemas = [
                s[0]
                for s in schemas_res
                if s[0] not in ["information_schema", "pg_catalog", "main"]
            ]

            for schema in schemas:
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                tables = con.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}' AND table_catalog = '{alias}'"
                ).fetchall()
                for (table,) in tables:
                    # Point the main schema views to the native DuckLake tables
                    with suppress(Exception):
                        con.execute(f"DROP VIEW IF EXISTS {schema}.{table}")
                    with suppress(Exception):
                        con.execute(f"DROP TABLE IF EXISTS {schema}.{table}")

                    con.execute(
                        f"CREATE VIEW {schema}.{table} AS SELECT * FROM {alias}.{schema}.{table}"
                    )

                    # Backward compatibility mappings
                    if schema in ["metadata", "registry"] and schema != "registry":
                        con.execute("CREATE SCHEMA IF NOT EXISTS registry")
                        con.execute(
                            f"CREATE OR REPLACE VIEW registry.{table} AS SELECT * FROM {alias}.{schema}.{table}"
                        )
                    if schema in ["bronze", "main"] and schema != "bronze":
                        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
                        con.execute(
                            f"CREATE OR REPLACE VIEW bronze.{table} AS SELECT * FROM {alias}.{schema}.{table}"
                        )

            logger.info(f"Connected to Native Lakehouse at {lp}")
        except Exception as e:
            logger.warning(f"Native DuckLake attachment failed: {e}")

    # Ensure default schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS registry")
    return con


def write_silver_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    arrow_table: Any,
    symbol: str,
    schema: str = "silver",
    lake_path: str | Path | None = None,
) -> int:
    """Write data using native DuckLake management.

    This ensures the table is tracked in the catalog and partitioned by symbol.
    """
    # 1. Identify the lakehouse database
    attached = con.execute("PRAGMA database_list").fetchall()
    alias = next((a[1] for a in attached if a[1] == "lakehouse"), None)

    if not alias:
        # Fallback to manual Hive partitioning if native lakehouse is not attached
        return _write_silver_manual(con, table_name, arrow_table, symbol, schema, lake_path)

    # 2. Ensure Schema and Table exist in the native DuckLake
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {alias}.{schema}")

    # Check if table exists
    tables = con.execute(
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_catalog = '{alias}' AND table_name = '{table_name}'"
    ).fetchall()

    if not tables:
        # Create empty table with the correct schema
        con.execute(
            f"CREATE TABLE {alias}.{schema}.{table_name} AS SELECT * FROM arrow_table WHERE FALSE"
        )

        # Apply optimal partition spec (Native DuckLake DDL)
        # We use (symbol, ts_date) for Silver tables to support both asset-level
        # and cross-sectional (date-level) pruning.
        partition_cols = ["symbol"]
        if "ts_date" in arrow_table.column_names:
            partition_cols.append("ts_date")

        con.execute(
            f"ALTER TABLE {alias}.{schema}.{table_name} SET PARTITIONED BY ({', '.join(partition_cols)})"
        )

    # 3. Insert Data
    # Native DuckLake handles the Parquet spills and partition organization
    con.execute(f"DELETE FROM {alias}.{schema}.{table_name} WHERE symbol = ?", [symbol])
    con.execute(f"INSERT INTO {alias}.{schema}.{table_name} SELECT * FROM arrow_table")

    # 4. Sync View in 'main' for easy access
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    with suppress(Exception):
        con.execute(f"DROP VIEW IF EXISTS {schema}.{table_name}")
    with suppress(Exception):
        con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    con.execute(f"CREATE VIEW {schema}.{table_name} AS SELECT * FROM {alias}.{schema}.{table_name}")

    return arrow_table.num_rows


def _write_silver_manual(con, table_name, arrow_table, symbol, schema, lake_path):
    """Legacy fallback for non-native storage."""
    lp = Path(lake_path or "./lake").resolve()
    out_dir = lp / schema / table_name / f"symbol={symbol}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{symbol}.parquet"
    con.execute(f"COPY arrow_table TO '{out_path}' (FORMAT PARQUET)")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    with suppress(Exception):
        con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    glob_path = lp / schema / table_name / "**" / "*.parquet"
    con.execute(
        f"CREATE OR REPLACE VIEW {schema}.{table_name} AS "
        f"SELECT * FROM read_parquet('{glob_path}', hive_partitioning=True, union_by_name=True)"
    )
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
