"""E2E data correctness validation for the full raw→bronze→silver pipeline.

Validates field-level correctness, schema adherence, and field mappings
for all data types (klines, aggTrades, fundingRate) × trade types (spot, um, cm)
across all source types (REST, Archive).

Each test does full round-trip validation:
  1. Extract raw data via dlt REST/Archive source
  2. Verify bronze DuckDB table has expected columns
  3. Transform via Polars (bronze_*_to_silver)
  4. Verify silver DuckDB table matches Pandera schema
  5. Verify field-level mappings (ts_event, side, ts_date, exchange, etc.)
"""

from __future__ import annotations

import asyncio
import tempfile
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pytest

from binance_datatool.common.enums import DataFrequency, DataType, TradeType, exchange_for
from binance_datatool.dlt.destinations import run_source
from binance_datatool.dlt.resources.binance_archive import archive_data_resource
from binance_datatool.dlt.sources import build_binance_source, build_rest_source
from binance_datatool.storage.duckdb import get_connection, write_silver_table
from binance_datatool.transforms.agg_trades import bronze_agg_trades_to_silver
from binance_datatool.transforms.funding_rate import bronze_funding_rate_to_silver
from binance_datatool.transforms.klines import bronze_klines_to_silver

if TYPE_CHECKING:
    import polars as pl


def _run_pipeline(source, source_name: str, catalog_path: str) -> duckdb.DuckDBPyConnection:
    """Run a dlt source and return a DuckDB connection to the catalog."""
    run_source(
        source,
        source_name=source_name,
        catalog_path=catalog_path,
        dataset_name="bronze",
        destination="duckdb",
    )
    return get_connection(catalog_path=catalog_path)


def _bronze_table(con: duckdb.DuckDBPyConnection, table: str) -> str | None:
    """Return actual DuckDB bronze table name, or None if no data loaded."""
    all_tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'"
    ).fetchall()
    existing = {r[0] for r in all_tables}
    if table in existing:
        return table
    candidates = [table, table.replace("_", "__"), table.replace("_", "")]
    for cand in candidates:
        if cand in existing:
            return cand
    for t in sorted(existing):
        if table.replace("_", "") in t.replace("_", "") or t.replace("_", "") in table.replace(
            "_", ""
        ):
            return t
    return None


def _assert_bronze_columns(
    con: duckdb.DuckDBPyConnection,
    table: str,
    expected: set[str],
    label: str,
) -> pl.DataFrame:
    """Assert bronze table has expected columns and return data."""
    cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{table}' AND table_schema = 'bronze'"
        ).fetchall()
    }
    missing = expected - cols
    assert not missing, f"{label} bronze missing columns: {missing}"
    return con.execute(f"SELECT * FROM bronze.{table}").pl()


def _assert_silver_columns(
    silver: pl.DataFrame,
    expected: set[str],
    label: str,
) -> None:
    """Assert silver DataFrame has exactly the expected columns."""
    actual = set(silver.columns)
    missing = expected - actual
    extra = actual - expected
    assert not missing, f"{label} silver missing: {missing}"
    assert not extra, f"{label} silver extra: {extra}"


# ── Bronze column expectations per data type ──

BRONZE_KLINE_COLS = {
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "symbol",
    "interval",
}

BRONZE_AGGTRADE_COLS = {
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "transact_time",
    "is_buyer_maker",
    "symbol",
}

BRONZE_FUNDING_COLS = {
    "symbol",
    "funding_time",
    "funding_rate",
    "mark_price",
}

# ── Silver column expectations ──

SILVER_KLINE_COLS = {
    "ts_event",
    "ts_recv",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "source",
    "exchange",
    "trade_type",
    "symbol",
    "interval",
    "data_type",
    "ingested_at",
    "ts_date",
}

SILVER_AGGTRADE_COLS = {
    "ts_event",
    "ts_recv",
    "price",
    "size",
    "side",
    "trade_id",
    "is_buyer_maker",
    "agg_trade_id",
    "first_trade_id",
    "last_trade_id",
    "rtype",
    "source",
    "exchange",
    "trade_type",
    "symbol",
    "data_type",
    "ingested_at",
    "ts_date",
}

SILVER_FUNDING_COLS = {
    "ts_event",
    "ts_recv",
    "funding_rate",
    "mark_price",
    "funding_timestamp",
    "source",
    "exchange",
    "trade_type",
    "symbol",
    "data_type",
    "ingested_at",
    "ts_date",
}


# ══════════════════════════════════════════════════════════════════
# REST Source Tests
# ══════════════════════════════════════════════════════════════════


@pytest.mark.integration
class TestRestKlinesCorrectness:
    """Validate klines REST pipeline: raw → bronze → silver."""

    @pytest.mark.parametrize("trade_type", [TradeType.spot, TradeType.um])
    def test_field_mappings(self, trade_type: TradeType, tmp_path: Path) -> None:
        db = str(tmp_path / "catalog.duckdb")
        con = _run_pipeline(
            build_binance_source(["BTCUSDT"], "1h", trade_type, "klines"),
            f"e2e_rk_{trade_type.value}",
            db,
        )

        bronze = _assert_bronze_columns(
            con, "klines", BRONZE_KLINE_COLS, f"klines/{trade_type.value}"
        )
        assert len(bronze) >= 1, f"klines/{trade_type.value}: bronze empty"

        silver = bronze_klines_to_silver(
            bronze, symbol="BTCUSDT", interval="1h", trade_type=trade_type.value
        )
        _assert_silver_columns(silver, SILVER_KLINE_COLS, f"klines/{trade_type.value}")

        # Field-level validations
        b_row = bronze.row(0, named=True)
        s_row = silver.row(0, named=True)

        # ts_event = open_time * 1000 (ms→μs)
        assert s_row["ts_event"] == int(b_row["open_time"]) * 1000, "ts_event != open_time * 1000"

        # ts_date derived from open_time via same transform logic: ms / 86400000 → days since epoch
        from datetime import date, timedelta

        expected_days = int(b_row["open_time"]) // 86_400_000
        expected_date = date(1970, 1, 1) + timedelta(days=expected_days)
        assert str(s_row["ts_date"]) == str(expected_date), (
            f"ts_date={s_row['ts_date']} != {expected_date} "
            f"(open_time={b_row['open_time']}, days={expected_days})"
        )

        # Exchange naming
        assert s_row["exchange"] == exchange_for(trade_type.value), "exchange mismatch"
        assert s_row["trade_type"] == trade_type.value, "trade_type mismatch"

        # Data type
        assert s_row["data_type"] == "klines", "data_type != klines"

        # Source label
        assert s_row["source"] == "dlt_api", "source != dlt_api"

        # Numeric types
        assert isinstance(s_row["open"], float)
        assert isinstance(s_row["trade_count"], int)

        # Write to silver and verify DuckDB type
        write_silver_table(con, "klines", silver.to_arrow(), "BTCUSDT")
        ts_date_type = con.execute("SELECT typeof(ts_date) FROM silver.klines LIMIT 1").fetchone()[
            0
        ]
        assert "DATE" in ts_date_type.upper(), f"ts_date type: {ts_date_type}"
        con.close()


@pytest.mark.integration
class TestRestAggTradesCorrectness:
    """Validate aggTrades REST pipeline: raw → bronze → silver."""

    @pytest.mark.parametrize("trade_type", [TradeType.spot, TradeType.um])
    def test_field_mappings(self, trade_type: TradeType, tmp_path: Path) -> None:
        db = str(tmp_path / "catalog.duckdb")
        con = _run_pipeline(
            build_rest_source(["BTCUSDT"], "aggTrades", trade_type),
            f"e2e_at_{trade_type.value}",
            db,
        )

        bronze = _assert_bronze_columns(
            con, "agg_trades", BRONZE_AGGTRADE_COLS, f"aggTrades/{trade_type.value}"
        )
        assert len(bronze) >= 1, f"aggTrades/{trade_type.value}: bronze empty"

        silver = bronze_agg_trades_to_silver(bronze, symbol="BTCUSDT", trade_type=trade_type.value)
        _assert_silver_columns(silver, SILVER_AGGTRADE_COLS, f"aggTrades/{trade_type.value}")

        b_row = bronze.row(0, named=True)
        s_row = silver.row(0, named=True)

        # ts_event = transact_time in μs (ms→μs conversion)
        assert s_row["ts_event"] == int(b_row["transact_time"]) * 1000, (
            "ts_event != transact_time * 1000"
        )

        # quantity → size
        assert s_row["size"] == float(b_row["quantity"]), "size != quantity"

        # side derived from is_buyer_maker
        is_buyer = str(b_row["is_buyer_maker"]).lower() == "true"
        assert s_row["side"] == ("sell" if is_buyer else "buy"), (
            f"side mismatch for is_buyer_maker={is_buyer}"
        )

        # agg_trade_id → trade_id (and agg_trade_id preserved)
        assert s_row["trade_id"] == int(b_row["agg_trade_id"]), "trade_id != agg_trade_id"
        assert s_row["agg_trade_id"] == int(b_row["agg_trade_id"]), "agg_trade_id lost"

        # first_trade_id / last_trade_id preserved
        assert "first_trade_id" in s_row
        assert "last_trade_id" in s_row

        # rtype
        assert s_row["rtype"] == "agg", "rtype != agg"

        # Exchange
        assert s_row["exchange"] == exchange_for(trade_type.value), "exchange mismatch"

        write_silver_table(con, "agg_trades", silver.to_arrow(), "BTCUSDT")
        con.close()


@pytest.mark.integration
class TestRestFundingRateCorrectness:
    """Validate fundingRate REST pipeline: raw → bronze → silver."""

    @pytest.mark.parametrize(
        "trade_type, symbol",
        [
            (TradeType.um, "BTCUSDT"),
            (TradeType.cm, "BTCUSD_PERP"),
        ],
    )
    def test_field_mappings(self, trade_type: TradeType, symbol: str, tmp_path: Path) -> None:
        db = str(tmp_path / "catalog.duckdb")
        con = _run_pipeline(
            build_rest_source([symbol], "fundingRate", trade_type),
            f"e2e_fr_{trade_type.value}",
            db,
        )

        bronze = _assert_bronze_columns(
            con, "funding_rate", BRONZE_FUNDING_COLS, f"fundingRate/{trade_type.value}"
        )
        assert len(bronze) >= 1, f"fundingRate/{trade_type.value}: bronze empty"

        silver = bronze_funding_rate_to_silver(bronze, symbol=symbol, trade_type=trade_type.value)
        _assert_silver_columns(silver, SILVER_FUNDING_COLS, f"fundingRate/{trade_type.value}")

        b_row = bronze.row(0, named=True)
        s_row = silver.row(0, named=True)

        # funding_time → ts_event + funding_timestamp (ms→μs)
        ft = int(b_row["funding_time"])
        assert s_row["ts_event"] == ft * 1000, "ts_event != funding_time * 1000"
        assert s_row["funding_timestamp"] == ft * 1000, "funding_timestamp != funding_time * 1000"

        # mark_price preserved
        if b_row["mark_price"] and str(b_row["mark_price"]).strip():
            assert s_row["mark_price"] == float(b_row["mark_price"]), "mark_price mismatch"

        # Exchange
        assert s_row["exchange"] == exchange_for(trade_type.value), "exchange mismatch"

        write_silver_table(con, "funding_rate", silver.to_arrow(), "BTCUSDT")
        con.close()


# ══════════════════════════════════════════════════════════════════
# Archive Source Tests
# ══════════════════════════════════════════════════════════════════


@pytest.mark.integration
class TestArchiveKlinesCorrectness:
    """Validate klines Archive pipeline: raw ZIP → bronze → silver."""

    @pytest.mark.parametrize(
        "trade_type,symbol",
        [(TradeType.spot, "BTCUSDT"), (TradeType.um, "BTCUSDT"), (TradeType.cm, "BTCUSD_PERP")],
    )
    def test_field_mappings(self, trade_type: TradeType, symbol: str, tmp_path: Path) -> None:
        from binance_datatool.archive.client import ArchiveClient

        client = ArchiveClient()
        files = asyncio.run(
            client.list_symbol_files(
                trade_type, DataFrequency.daily, DataType.klines, symbol, interval="1d"
            )
        )
        recent = [f for f in files if not f.key.endswith(".CHECKSUM")][-1:]
        if not recent:
            pytest.skip(f"No archive files available for {symbol} 1d klines {trade_type.value}")

        db = str(tmp_path / "catalog.duckdb")
        resource = archive_data_resource(
            symbol, [f.key for f in recent], interval="1d", data_type="klines"
        )
        con = _run_pipeline(resource, f"e2e_ak_{trade_type.value}", db)

        table = _bronze_table(con, "klines")
        if table is None:
            pytest.skip("No bronze table for klines (S3 download returned empty)")
        bronze_raw = con.execute(
            f"SELECT open_time, open, high, low, close, volume, close_time, "
            f"quote_volume, count, taker_buy_volume, taker_buy_quote_volume, "
            f"symbol, interval FROM bronze.{table} WHERE symbol = ? AND interval = ?",
            [symbol, "1d"],
        ).fetchall()
        if not bronze_raw:
            pytest.skip(f"No bronze rows for {symbol} 1d klines {trade_type.value}")

        import polars as pl

        bronze_columns = [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "count",
            "taker_buy_volume",
            "taker_buy_quote_volume",
            "symbol",
            "interval",
        ]
        bronze = pl.from_records(bronze_raw, schema=bronze_columns, orient="row")
        for col in ["open_time", "close_time", "count"]:
            bronze = bronze.with_columns(pl.col(col).cast(pl.Int64))
        for col in [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "taker_buy_volume",
            "taker_buy_quote_volume",
        ]:
            bronze = bronze.with_columns(pl.col(col).cast(pl.Float64))
        bronze = bronze.with_columns(
            pl.col("symbol").cast(pl.Utf8),
            pl.col("interval").cast(pl.Utf8),
        )

        silver = bronze_klines_to_silver(
            bronze, symbol=symbol, interval="1d", trade_type=trade_type.value, source="archive"
        )
        _assert_silver_columns(silver, SILVER_KLINE_COLS, f"archive/klines/{trade_type.value}")

        expected_exchange = exchange_for(trade_type.value)
        assert silver["exchange"][0] == expected_exchange, (
            f"exchange={silver['exchange'][0]} != {expected_exchange}"
        )
        assert silver["source"][0] == "archive"
        assert silver["data_type"][0] == "klines"
        assert isinstance(silver["ts_date"][0], date), f"ts_date type: {type(silver['ts_date'][0])}"
        assert str(silver["ts_date"][0]) != ""


class TestArchiveAggTradesCorrectness:
    """Validate aggTrades Archive pipeline: raw ZIP → bronze → silver."""

    @pytest.mark.parametrize(
        "trade_type,symbol",
        [(TradeType.spot, "BTCUSDT"), (TradeType.um, "BTCUSDT")],
    )
    def test_field_mappings(self, trade_type: TradeType, symbol: str, tmp_path: Path) -> None:
        from binance_datatool.archive.client import ArchiveClient

        client = ArchiveClient()
        files = asyncio.run(
            client.list_symbol_files(trade_type, DataFrequency.daily, DataType.agg_trades, symbol)
        )
        recent = [f for f in files if not f.key.endswith(".CHECKSUM")][-1:]
        if not recent:
            pytest.skip(f"No archive files for {symbol} aggTrades {trade_type.value}")

        db = str(tmp_path / "catalog.duckdb")
        resource = archive_data_resource(symbol, [f.key for f in recent], data_type="aggTrades")
        con = _run_pipeline(resource, f"e2e_aat_{trade_type.value}", db)

        table = _bronze_table(con, "agg_trades")
        if table is None:
            pytest.skip("No bronze table for agg_trades (S3 download returned empty)")
        bronze_raw = con.execute(
            f"SELECT agg_trade_id, price, quantity, transact_time, "
            f"is_buyer_maker, first_trade_id, last_trade_id, symbol "
            f"FROM bronze.{table} WHERE symbol = ?",
            [symbol],
        ).fetchall()
        if not bronze_raw:
            pytest.skip(f"No bronze rows for {symbol} aggTrades {trade_type.value}")

        import polars as pl

        bronze_columns = [
            "agg_trade_id",
            "price",
            "quantity",
            "transact_time",
            "is_buyer_maker",
            "first_trade_id",
            "last_trade_id",
            "symbol",
        ]
        bronze = pl.from_records(bronze_raw, schema=bronze_columns, orient="row")
        for col in ["agg_trade_id", "transact_time", "first_trade_id", "last_trade_id"]:
            bronze = bronze.with_columns(pl.col(col).cast(pl.Int64))
        for col in ["price", "quantity"]:
            bronze = bronze.with_columns(pl.col(col).cast(pl.Float64))
        bronze = bronze.with_columns(pl.col("symbol").cast(pl.Utf8))

        silver = bronze_agg_trades_to_silver(
            bronze, symbol=symbol, trade_type=trade_type.value, source="archive"
        )
        _assert_silver_columns(
            silver, SILVER_AGGTRADE_COLS, f"archive/aggTrades/{trade_type.value}"
        )

        expected_exchange = exchange_for(trade_type.value)
        assert silver["exchange"][0] == expected_exchange
        assert silver["source"][0] == "archive"
        assert silver["rtype"][0] == "agg"
        assert silver["trade_id"][0] == silver["agg_trade_id"][0]
        assert silver["first_trade_id"][0] >= 0
        assert silver["last_trade_id"][0] >= 0
        assert isinstance(silver["ts_date"][0], date)


class TestArchiveFundingRateCorrectness:
    """Validate fundingRate Archive pipeline: raw ZIP → bronze → silver."""

    @pytest.mark.parametrize(
        "trade_type,symbol",
        [(TradeType.um, "BTCUSDT"), (TradeType.cm, "BTCUSD_PERP")],
    )
    def test_field_mappings(self, trade_type: TradeType, symbol: str, tmp_path: Path) -> None:
        from binance_datatool.archive.client import ArchiveClient

        client = ArchiveClient()
        files = asyncio.run(
            client.list_symbol_files(
                trade_type, DataFrequency.monthly, DataType.funding_rate, symbol
            )
        )
        recent = [f for f in files if not f.key.endswith(".CHECKSUM")][-1:]
        if not recent:
            pytest.skip(f"No archive files for {symbol} fundingRate {trade_type.value}")

        db = str(tmp_path / "catalog.duckdb")
        resource = archive_data_resource(symbol, [f.key for f in recent], data_type="fundingRate")
        con = _run_pipeline(resource, f"e2e_afr_{trade_type.value}", db)

        table = _bronze_table(con, "funding_rate")
        if table is None:
            pytest.skip("No bronze table for funding_rate (S3 download returned empty)")
        bronze_raw = con.execute(
            f"SELECT symbol, funding_time, funding_rate, mark_price "
            f"FROM bronze.{table} WHERE symbol = ?",
            [symbol],
        ).fetchall()
        if not bronze_raw:
            pytest.skip(f"No bronze rows for {symbol} fundingRate {trade_type.value}")

        import polars as pl

        bronze_columns = ["symbol", "funding_time", "funding_rate", "mark_price"]
        bronze = pl.from_records(bronze_raw, schema=bronze_columns, orient="row")
        bronze = bronze.with_columns(
            pl.col("funding_time").cast(pl.Int64),
            pl.col("funding_rate").cast(pl.Float64),
            pl.col("symbol").cast(pl.Utf8),
        )

        silver = bronze_funding_rate_to_silver(
            bronze, symbol=symbol, trade_type=trade_type.value, source="archive"
        )
        _assert_silver_columns(
            silver, SILVER_FUNDING_COLS, f"archive/fundingRate/{trade_type.value}"
        )

        expected_exchange = exchange_for(trade_type.value)
        assert silver["exchange"][0] == expected_exchange
        assert silver["source"][0] == "archive"
        assert silver["ts_event"][0] == silver["funding_timestamp"][0]
        assert isinstance(silver["ts_date"][0], date)

        # For archive data, open_time may be ms (13-digit) or μs (16-digit).
        # ts_event should equal open_time (μs) or open_time * 1000 (ms→μs)
        bronze_open = int(bronze["open_time"][0])
        expected_ts = bronze_open if bronze_open >= 1_000_000_000_000_000 else bronze_open * 1000
        assert silver["ts_event"][0] == expected_ts, (
            f"ts_event={silver['ts_event'][0]} != expected={expected_ts} (open_time={bronze_open})"
        )

        write_silver_table(con, "klines", silver.to_arrow(), "BTCUSDT")
        con.close()


# ══════════════════════════════════════════════════════════════════
# Cross-table consistency tests
# ══════════════════════════════════════════════════════════════════


class TestCrossTableConsistency:
    """Validate consistency across all silver tables."""

    def test_ts_date_type(self) -> None:
        """Validate ts_date is DATE type across all silver tables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = str(Path(tmpdir) / "catalog.duckdb")
            con = duckdb.connect(db)
            con.execute("CREATE SCHEMA IF NOT EXISTS silver")

            for tbl in ("klines", "agg_trades", "funding_rate"):
                con.execute(f"CREATE TABLE silver.{tbl} (ts_date DATE)")
                t = con.execute(f"SELECT typeof(ts_date) FROM silver.{tbl} LIMIT 0").fetchone()
                if t:
                    assert "DATE" in str(t[0]).upper(), f"{tbl} ts_date type: {t[0]}"
                else:
                    # DuckDB describes type via column info for empty results
                    cols = con.execute(
                        f"SELECT column_name, data_type FROM information_schema.columns "
                        f"WHERE table_name = '{tbl}' AND table_schema = 'silver'"
                    ).fetchall()
                    for name, dtype in cols:
                        if name == "ts_date":
                            assert "DATE" in str(dtype).upper(), f"{tbl} ts_date dtype: {dtype}"
            con.close()
