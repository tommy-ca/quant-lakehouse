"""Gap detection utility for dlt pipelines.

Detects missing date ranges in DuckDB bronze tables — the dlt equivalent
of ``GapFillWorkflow.detect_gaps()`` (which scans the filesystem).

Operations:
- ``detect_bronze_gaps()`` — scan DuckDB bronze table for missing dates
- ``detect_bronze_gaps_ducklake()`` — scan DuckLake silver table for gaps

Returns list of ``(symbol, start_ms, end_ms)`` tuples for each gap.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb


def detect_bronze_gaps(
    db_path: str,
    table_name: str,
    symbols: list[str],
    lookback_days: int = 30,
) -> list[tuple[str, int, int]]:
    """Detect date gaps in a DuckDB bronze table.

    Queries the bronze table for existing ``open_time`` timestamps per
    symbol, then checks for gaps in the lookback window.

    Args:
        db_path: Path to ``catalog.duckdb``.
        table_name: Bronze table name (e.g. ``"bronze.btcusdt_klines"``).
        symbols: Symbols to check.
        lookback_days: How far back to check for gaps.

    Returns:
        List of ``(symbol, start_ms, end_ms)`` for each gap found.
    """
    con = duckdb.connect(db_path)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        now = datetime.now(UTC)
        window_start = int((now - timedelta(days=lookback_days)).timestamp() * 1000)
        window_end = int(now.timestamp() * 1000)

        gaps: list[tuple[str, int, int]] = []
        for symbol in symbols:
            try:
                existing = con.execute(
                    f"SELECT DISTINCT CAST(open_time / 86400000 AS BIGINT) AS day "
                    f"FROM {table_name} WHERE symbol = ? "
                    f"AND open_time BETWEEN ? AND ? "
                    f"ORDER BY day",
                    [symbol, window_start, window_end],
                ).fetchall()
            except Exception:
                existing = []

            existing_days = {row[0] for row in existing}
            expected_start = window_start // 86400000
            expected_end = window_end // 86400000

            for day in range(expected_start, expected_end + 1):
                if day not in existing_days:
                    gap_start = day * 86400000
                    gap_end = gap_start + 86400000
                    if gap_start < window_start:
                        gap_start = window_start
                    if gap_end > window_end:
                        gap_end = window_end
                    if gap_end > gap_start:
                        gaps.append((symbol, gap_start, gap_end))
        return gaps
    finally:
        con.close()


def detect_bronze_gaps_ducklake(
    db_path: str,
    catalog_path: str,
    symbol: str,
    table_name: str = "klines",
    lookback_days: int = 30,
) -> list[tuple[int, int]]:
    """Detect gaps in DuckLake Silver tables (post-sink).

    Queries the Duck-attached catalog for missing dates.
    """
    con = duckdb.connect(db_path)
    try:
        con.execute("LOAD ducklake")
        con.execute(
            f"ATTACH 'ducklake:{catalog_path}/metadata.ducklake' AS dl "
            f"(DATA_PATH '{catalog_path}/data', AUTOMATIC_MIGRATION true)"
        )
        con.execute("USE dl")

        now = datetime.now(UTC)
        window_start = int((now - timedelta(days=lookback_days)).timestamp() * 1_000_000)
        window_end = int(now.timestamp() * 1_000_000)

        try:
            existing = con.execute(
                f"SELECT DISTINCT CAST(ts_event / 86400000000000 AS BIGINT) AS day "
                f"FROM {table_name} WHERE symbol = ? "
                f"AND ts_event BETWEEN ? AND ? ORDER BY day",
                [symbol, window_start, window_end],
            ).fetchall()
        except Exception:
            existing = []

        existing_days = {row[0] for row in existing}
        expected_start = window_start // 86400000000000
        expected_end = window_end // 86400000000000

        gaps: list[tuple[int, int]] = []
        for day in range(expected_start, expected_end + 1):
            if day not in existing_days:
                gap_start_us = day * 86400000000000
                gap_end_us = gap_start_us + 86400000000000
                if gap_start_us < window_start:
                    gap_start_us = window_start
                if gap_end_us > window_end:
                    gap_end_us = window_end
                if gap_end_us > gap_start_us:
                    gaps.append((gap_start_us // 1000, gap_end_us // 1000))
        return gaps
    finally:
        con.close()
