"""Iceberg lakehouse and DuckDB catalog management.

Designs the catalog structure for the medallion architecture:
- Iceberg: Namespace-based, partitioned, versioned snapshots
- DuckDB: Local SQL views, materialized aggregations

Catalog follows Databento DBN and tardis.dev schema conventions.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger


def duckdb_table_name(trade_type: str, data_type: str) -> str:
    """Generate a DuckDB-safe table name."""
    return f"{trade_type}_{data_type}".replace("-", "_").replace(".", "_")


def iceberg_table_name(data_type: str, interval: str | None = None) -> str:
    """Generate an Iceberg table name."""
    base = data_type.replace("_", "").replace("-", "")
    if interval:
        return f"{base}_{interval}"
    return base


def catalog_path(
    base: Path,
    trade_type: str,
    data_type: str,
    date_str: str | None = None,
) -> Path:
    """Build the file-system catalog path for a data partition."""
    p = base / trade_type / data_type
    if date_str:
        p = p / f"date={date_str}"
    return p


# ── Iceberg Catalog ─────────────────────────────────────────────────────


def _iceberg_partition_spec(table_name: str) -> list[dict[str, Any]]:
    """Return Iceberg partition spec for a table.

    All time-series tables partition by day on ts_event.
    Metadata tables are unpartitioned.
    """
    if table_name in ("venues", "symbols", "symbol"):
        return []
    return [{"column": "ts_event", "transform": "day"}]


def _iceberg_table_properties() -> dict[str, str]:
    """Default Iceberg table properties."""
    return {
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "zstd",
        "write.parquet.compression-level": "9",
        "commit.retry.num-retries": "3",
        "history.expire.max-snapshot-age-ms": "2592000000",  # 30 days
    }


class IcebergCatalog:
    """Iceberg catalog manager using PyIceberg.

    Manages namespaces, tables, partitions, and snapshots for the
    medallion lakehouse architecture.

    Iceberg tables are stored under `{warehouse}/iceberg/` with
    a standard Hadoop catalog layout.
    """

    NAMESPACE = "binance"

    TABLE_SPECS = {
        "klines": {"partition": ["days(ts_event)"], "sort": ["symbol", "ts_event"]},
        "klines_1h": {"partition": ["days(ts_event)"], "sort": ["symbol", "ts_event"]},
        "klines_1d": {"partition": ["months(ts_event)"], "sort": ["symbol", "ts_event"]},
        "trades": {"partition": ["days(ts_event)"], "sort": ["symbol", "ts_event"]},
        "aggTrades": {"partition": ["days(ts_event)"], "sort": ["symbol", "ts_event"]},
        "fundingRate": {"partition": ["days(ts_event)"], "sort": ["symbol", "ts_event"]},
        "venues": {"partition": [], "sort": []},
        "symbols": {"partition": [], "sort": ["trade_type", "symbol"]},
    }

    def __init__(self, warehouse_path: Path) -> None:
        self._warehouse = Path(warehouse_path)
        self._initialized = False

    def _init_catalog(self) -> None:
        """Initialize PyIceberg catalog if available."""
        if self._initialized:
            return
        self._initialized = True
        try:
            import pyiceberg.catalog  # noqa: F401
            import pyiceberg.schema  # noqa: F401
            import pyiceberg.types  # noqa: F401

            logger.info("PyIceberg available; catalog ready")
        except ImportError:
            logger.info("PyIceberg not installed; using file-system catalog")

    def create_namespace(self) -> None:
        """Create the binance namespace if not exists."""
        ns_path = self._warehouse / "iceberg" / self.NAMESPACE
        ns_path.mkdir(parents=True, exist_ok=True)
        logger.info("Iceberg namespace: {}", ns_path)

    def table_path(self, table_name: str) -> Path:
        """Return the file-system path for an Iceberg table."""
        return self._warehouse / "iceberg" / self.NAMESPACE / table_name

    def register_parquet(
        self,
        df: pl.DataFrame,
        table_name: str,
        trade_type: str | None = None,
        interval: str | None = None,
    ) -> Path:
        """Register a DataFrame as an Iceberg table (file-system catalog).

        Path convention matches DuckLake for interop.
        Metadata tables (venues, symbols) are stored flat.
        """
        self._init_catalog()
        table_root = self.table_path(table_name)

        if table_name in ("venues", "symbols"):
            table_root.mkdir(parents=True, exist_ok=True)
            out = table_root / f"{table_name}.parquet"
            df.write_parquet(out)
            return out

        df = df.with_columns(pl.col("symbol").alias("_sym"))
        df = df.with_columns(
            pl.from_epoch(pl.col("ts_event") // 1000).dt.strftime("%Y-%m-%d").alias("_dt")
        )

        written = 0
        for (sym, dt), group in df.group_by(["_sym", "_dt"], maintain_order=True):
            out_dir = table_root / f"symbol={sym}" / f"date={dt}"
            if interval:
                out_dir = table_root / f"symbol={sym}" / f"interval={interval}" / f"date={dt}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out = out_dir / "data.parquet"
            group.drop(["_sym", "_dt"]).write_parquet(out)
            written += 1

        logger.info("Iceberg: {} files under {}", written, table_root)
        return table_root


# ── DuckDB Catalog ──────────────────────────────────────────────────────


_DUCKLAKE_ANALYTICS_VIEWS = """
-- Analytics views on top of unified DuckLake tables.

CREATE OR REPLACE VIEW daily_ohlcv AS
SELECT CAST(epoch_ms(ts_event // 1000) AS DATE) AS trade_date,

       CAST(epoch_ms(MAX(ts_event // 1000)) AS DATE) AS latest_date,

       DATEDIFF('day', CAST(epoch_ms(MAX(ts_event // 1000)) AS DATE), CURRENT_DATE) AS days_stale
FROM klines GROUP BY symbol, trade_type
HAVING days_stale > 3;
"""


class DuckLakeCatalog:
    """DuckLake v1.0 catalog — native DuckDB lakehouse tables + ACID.

    Uses the official DuckLake format (ATTACH 'ducklake:metadata.ducklake') from
    the DuckDB team. Provides:
    - ACID transactions over multi-table operations
    - Native table management with partitioning
    - Snapshots, time-travel, schema evolution
    - ducklake_add_data_files for registering externally-written Parquet

    Data is written as Parquet via Polars (efficient batch transform),
    then registered as native DuckLake tables for ACID-compliant querying.

    Docs: https://ducklake.select/docs/stable
    """

    # Column order must match the Silver Parquet output from sink.py
    # for INSERT INTO ... SELECT * FROM read_parquet() to work correctly.
    # Unified tables (merged across trade types — use trade_type column for filtering)
    # Column order matches Silver Parquet output from sink.py
    TABLE_DEFS: dict[str, str] = {
        "klines": "ts_event BIGINT, ts_recv BIGINT, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, quote_volume DOUBLE, trade_count BIGINT, taker_buy_volume DOUBLE, taker_buy_quote_volume DOUBLE, source VARCHAR, exchange VARCHAR, trade_type VARCHAR, symbol VARCHAR, interval VARCHAR, data_type VARCHAR, ingested_at BIGINT, ts_date DATE",
        "aggTrades": "ts_event BIGINT, ts_recv BIGINT, price DOUBLE, size DOUBLE, side VARCHAR, trade_id BIGINT, is_buyer_maker BIGINT, agg_trade_id BIGINT, rtype VARCHAR, source VARCHAR, exchange VARCHAR, trade_type VARCHAR, symbol VARCHAR, interval VARCHAR, data_type VARCHAR, ingested_at BIGINT, ts_date DATE",
        "fundingRate": "ts_event BIGINT, ts_recv BIGINT, funding_rate DOUBLE, mark_price DOUBLE, funding_timestamp BIGINT, source VARCHAR, exchange VARCHAR, trade_type VARCHAR, symbol VARCHAR, interval VARCHAR, data_type VARCHAR, ingested_at BIGINT, ts_date DATE",
        "venues": "venue VARCHAR, trade_type VARCHAR, exchange VARCHAR, source VARCHAR, symbol_count BIGINT, data_types VARCHAR, fetched_at BIGINT",
        "symbols": "symbol VARCHAR, trade_type VARCHAR, exchange VARCHAR, base_asset VARCHAR, quote_asset VARCHAR, contract_type VARCHAR, is_leverage BOOLEAN, is_stable_pair BOOLEAN, source VARCHAR, status VARCHAR, fetched_at BIGINT",
    }

    TABLE_PARTITIONS: dict[str, str] = {
        "klines": "trade_type, symbol, interval, ts_date",
        "aggTrades": "trade_type, symbol, ts_date",
        "fundingRate": "trade_type, symbol, ts_date",
        "symbols": "trade_type",
    }

    def __init__(
        self,
        lake_path: Path | str,
        db_path: Path | str | None = None,
        catalog_name: str = "binance_lake",
    ) -> None:
        self._lake_path = Path(lake_path).resolve()
        self._data_path = self._lake_path / "data"
        self._db_path = str(db_path) if db_path else ":memory:"
        self._catalog_name = catalog_name

    def connect(self):
        """Create a DuckDB connection with DuckLake v1.0 attached."""
        import duckdb

        con = duckdb.connect(self._db_path)
        con.execute("SET enable_progress_bar=false")
        con.execute("LOAD ducklake")
        con.execute("LOAD parquet")
        con.execute("LOAD iceberg")

        metadata_path = self._lake_path / "metadata.ducklake"
        self._lake_path.mkdir(parents=True, exist_ok=True)
        self._data_path.mkdir(parents=True, exist_ok=True)
        try:
            con.execute(
                f"ATTACH 'ducklake:{metadata_path}' AS {self._catalog_name} "
                f"(DATA_PATH '{self._data_path}', AUTOMATIC_MIGRATION true)"
            )
            con.execute(f"USE {self._catalog_name}")
        except Exception:
            con.execute(
                f"ATTACH 'ducklake:{metadata_path}' AS {self._catalog_name} (DATA_PATH '{self._data_path}')"
            )
            con.execute(f"USE {self._catalog_name}")
        return con

    def ensure_table(self, con: Any, table_name: str) -> None:
        """Create a DuckLake native table with partitioning if not exists."""
        if table_name not in self.TABLE_DEFS:
            logger.warning("Unknown table: {}", table_name)
            return
        exists = con.execute(
            f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchone()[0]
        if exists:
            return
        cols = self.TABLE_DEFS[table_name]
        con.execute(f"CREATE TABLE {table_name} ({cols})")
        parts = self.TABLE_PARTITIONS.get(table_name)
        if parts:
            con.execute(f"ALTER TABLE {table_name} SET PARTITIONED BY ({parts})")
        logger.info("DuckLake: created native table {} with partitions ({})", table_name, parts)

    def ingest_dataframe(
        self,
        con: Any,
        table_name: str,
        df: pl.DataFrame,
    ) -> int:
        """Ingest a Polars DataFrame into a DuckLake native table.

        All type casting and ts_date computation happens in the Polars
        transform stage (sink.py) — this is a zero-copy INSERT.
        Reorders DataFrame columns to match TABLE_DEFS for positional INSERT.
        Any TABLE_DEF columns missing from the DataFrame are filled with NULL.
        """
        self.ensure_table(con, table_name)
        ingested = 0
        try:
            cols = self.TABLE_DEFS.get(table_name, "")
            table_cols = [c.split()[0] for c in cols.split(",")]
            # Build select list: existing columns + NULL for missing ones
            select_parts = []
            for c in table_cols:
                if c in df.columns:
                    select_parts.append(c)
                else:
                    select_parts.append(f"NULL AS {c}")
            # Dedup: delete existing rows for symbol/date before re-inserting
            if "symbol" in df.columns and "ts_date" in df.columns:
                for sym, dt in df.select(["symbol", "ts_date"]).unique().rows():
                    con.execute(
                        f"DELETE FROM {table_name} WHERE symbol = ? AND ts_date = ?",
                        [sym, str(dt)],
                    )
            df = df.with_columns(pl.lit(None).alias("_null_placeholder"))
            con.execute(f"INSERT INTO {table_name} SELECT {', '.join(select_parts)} FROM df")
            ingested = len(df)
        except Exception as e:
            logger.error("DuckLake: failed to ingest DataFrame: {}", e)
            raise
        logger.info("DuckLake: ingested {} rows into native table {}", ingested, table_name)
        return ingested

    def register_metadata(self, con: Any, lake_path: Path) -> None:
        """Register metadata Parquet files as DuckLake native tables.

        Ingest venues.parquet and symbols.parquet (written by MetadataWorkflow)
        into DuckLake-managed tables with schema enforcement.
        """
        for table_name in ("venues", "symbols"):
            meta_path = lake_path / f"{table_name}.parquet"
            if not meta_path.exists():
                logger.info("DuckLake: {} not found, skipping", meta_path)
                continue
            self.ensure_table(con, table_name)
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if count > 0:
                logger.info("DuckLake: {} already has {} rows", table_name, count)
                continue
            try:
                con.execute(f"INSERT INTO {table_name} SELECT * FROM read_parquet('{meta_path}')")
                count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                logger.info("DuckLake: ingested {} rows into {}", count, table_name)
            except Exception as e:
                logger.warning("DuckLake: failed to ingest {}: {}", table_name, e)

    def find_parquet_files(
        self,
        trade_type: str,
        data_type: str,
        interval: str | None = None,
    ) -> list[Path]:
        """Find all data.parquet files matching a trade/data type pattern."""
        exchange_map = {"spot": "binance-spot", "um": "binance-perps-um", "cm": "binance-perps-cm"}
        exchange = exchange_map.get(trade_type, trade_type)
        base = self._data_path / f"exchange={exchange}" / f"data-type={data_type}"
        if not base.exists():
            return []
        pattern = "**/data.parquet"
        if interval:
            pattern = f"**/interval={interval}/**/data.parquet"
        return sorted(base.glob(pattern))

    def create_analytics_views(self, con: Any) -> None:
        """Create analytics views on top of native DuckLake tables."""
        for stmt in _DUCKLAKE_ANALYTICS_VIEWS.split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    con.execute(stmt)
                except Exception as e:
                    logger.warning("DuckLake analytics error: {}", e)
        logger.info("DuckLake: analytics views created")

    def run_query(self, query: str) -> pl.DataFrame:
        """Run a SQL query against the DuckLake catalog."""
        con = self.connect()
        try:
            return pl.from_arrow(con.execute(query).fetch_arrow_table())  # type: ignore[return-value]
        finally:
            con.close()
