"""Workflow for health-checking local archive data."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from binance_datatool.common import DataFrequency, DataType, TradeType


_DATE_PATTERN = re.compile(r".*?-(\d{4}-\d{2}-\d{2})(?:\.zip|\.csv|\.filled\.csv)")


@dataclass
class SymbolHealth:
    """Health status for a single symbol/data_type/interval."""

    symbol: str
    total_files: int = 0
    total_bytes: int = 0
    date_count: int = 0
    missing_dates: list[str] = field(default_factory=list)
    corrupted_files: list[str] = field(default_factory=list)
    latest_date: str | None = None
    days_since_latest: int = 0
    has_checksum: int = 0
    missing_checksum: int = 0

    @property
    def is_healthy(self) -> bool:
        """Healthy = no missing or corrupted files in expected range."""
        return len(self.missing_dates) == 0 and len(self.corrupted_files) == 0

    @property
    def completeness_pct(self) -> float:
        total_expected = self.date_count + len(self.missing_dates)
        if total_expected == 0:
            return 100.0
        return round(self.date_count / total_expected * 100, 1)


@dataclass
class HealthReport:
    """Aggregate health report for a set of symbols."""

    per_symbol: list[SymbolHealth] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def total_symbols(self) -> int:
        return len(self.per_symbol)

    @property
    def healthy_symbols(self) -> int:
        return sum(1 for s in self.per_symbol if s.is_healthy)

    @property
    def total_missing_dates(self) -> int:
        return sum(len(s.missing_dates) for s in self.per_symbol)

    @property
    def total_corrupted(self) -> int:
        return sum(len(s.corrupted_files) for s in self.per_symbol)


class HealthCheckWorkflow:
    """Check health of local archive data: completeness, freshness, integrity."""

    def __init__(
        self,
        trade_type: TradeType,
        data_freq: DataFrequency,
        data_type: DataType,
        symbols: Sequence[str],
        archive_home: Path,
        interval: str | None = None,
        max_stale_days: int = 3,
        verify_integrity: bool = False,
    ) -> None:
        self._trade_type = trade_type
        self._data_freq = data_freq
        self._data_type = data_type
        self._symbols = symbols
        self._archive_home = Path(archive_home)
        self._interval = interval
        self._max_stale_days = max_stale_days
        self._verify_integrity = verify_integrity

    def run(self) -> HealthReport:
        """Run the health check."""
        report = HealthReport()

        for symbol in self._symbols:
            try:
                health = self._check_symbol(symbol)
                report.per_symbol.append(health)
            except Exception as e:
                logger.error("Health check failed for {}: {}", symbol, e)
                report.errors.append(f"{symbol}: {e}")

        return report

    def _symbol_path(self, symbol: str) -> Path:
        base = (
            self._archive_home
            / "data"
            / self._trade_type.s3_path
            / self._data_freq.value
            / self._data_type.value
            / symbol
        )
        if self._interval:
            base = base / self._interval
        return base

    def _check_symbol(self, symbol: str) -> SymbolHealth:
        symbol_dir = self._symbol_path(symbol)
        health = SymbolHealth(symbol=symbol)

        if not symbol_dir.exists():
            logger.warning("No data directory for {}", symbol)
            return health

        dates_found: set[str] = set()
        total_bytes = 0
        csv_count = 0

        for entry in sorted(symbol_dir.iterdir()):
            if entry.is_dir() and entry.name == "_filled":
                for f in entry.iterdir():
                    if f.suffix == ".csv":
                        csv_count += 1
                        total_bytes += f.stat().st_size
                        m = _DATE_PATTERN.match(f.name)
                        if m:
                            dates_found.add(m.group(1))
                            if self._verify_integrity:
                                cs_path = f.with_name(f.name + ".CHECKSUM")
                                if cs_path.exists():
                                    health.has_checksum += 1
                                    if not _check_file_hash(f, cs_path):
                                        health.corrupted_files.append(f.name)
                                else:
                                    health.missing_checksum += 1
                continue

            if entry.suffix == ".zip":
                health.total_files += 1
                total_bytes += entry.stat().st_size
                cs_path = entry.with_name(entry.name + ".CHECKSUM")
                if cs_path.exists():
                    health.has_checksum += 1
                    if self._verify_integrity and not _check_file_hash(entry, cs_path):
                        health.corrupted_files.append(entry.name)
                else:
                    health.missing_checksum += 1

                m = _DATE_PATTERN.match(entry.name)
                if m:
                    dates_found.add(m.group(1))

            elif entry.suffix == ".csv":
                csv_count += 1
                total_bytes += entry.stat().st_size
                m = _DATE_PATTERN.match(entry.name)
                if m:
                    dates_found.add(m.group(1))

        health.total_bytes = total_bytes
        health.date_count = len(dates_found)

        if dates_found:
            sorted_dates = sorted(dates_found)
            health.latest_date = sorted_dates[-1]
            latest = datetime.strptime(sorted_dates[-1], "%Y-%m-%d").replace(tzinfo=UTC)
            health.days_since_latest = (datetime.now(UTC) - latest).days

            # Detect missing dates within range
            all_dates = _date_range(sorted_dates[0], sorted_dates[-1])
            health.missing_dates = [d for d in all_dates if d not in dates_found]

            # Check staleness
            if health.days_since_latest > self._max_stale_days:
                health.missing_dates.append(f"stale:+{health.days_since_latest}d")

        return health


def _date_range(start_str: str, end_str: str) -> list[str]:
    """Return all date strings between start and end (inclusive)."""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    result = []
    current = start
    while current <= end:
        result.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return result


def _check_file_hash(file_path: Path, checksum_path: Path) -> bool:
    """Verify a file against its SHA256 checksum."""
    try:
        expected = checksum_path.read_text().strip().split()[0]
        h = hashlib.sha256()
        with file_path.open("rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest() == expected
    except Exception:
        return False


@dataclass
class AnomalyReport:
    """Anomalies detected in DuckLake data."""

    symbol: str
    null_prices: int = 0
    zero_volumes: int = 0
    duplicate_timestamps: int = 0
    date_gaps: list[str] = field(default_factory=list)
    outlier_rows: int = 0

    @property
    def is_clean(self) -> bool:
        return (
            self.null_prices == 0
            and self.zero_volumes == 0
            and self.duplicate_timestamps == 0
            and len(self.date_gaps) == 0
            and self.outlier_rows == 0
        )


def _sanitize_identifier(name: str) -> str:
    """Sanitize a DuckDB identifier (table/column name) for safe f-string usage."""
    return "".join(c for c in name if c.isalnum() or c == "_")


def check_ducklake_anomalies(
    con: Any,
    table_name: str,
    symbol: str,
    interval: str | None = None,
    outlier_std: float = 3.0,
    rtype: str | None = None,
) -> AnomalyReport:
    """Query DuckLake native table for data quality anomalies.

    Checks for:
    - NULL or zero prices (open/high/low/close)
    - Zero volumes
    - Duplicate ts_event values
    - Missing dates within range
    - Price outliers (values beyond mean +/- N standard deviations)

    When ``rtype`` is provided (e.g. ``"agg"`` or ``"trade"``), filters
    all queries to that rtype. This allows per-type health stats when
    aggTrades and trades share the same DuckDB table.
    """
    report = AnomalyReport(symbol=symbol)
    tn = _sanitize_identifier(table_name)

    def _where() -> str:
        where = "WHERE symbol = ?"
        if rtype:
            where += " AND rtype = ?"
        if interval:
            where += " AND interval = ?"
        return where

    def _params() -> list:
        params = [symbol]
        if rtype:
            params.append(rtype)
        if interval:
            params.append(interval)
        return params

    if not _table_exists(con, tn):
        return report

    # Null/zero price check (only for columns that exist in this table)
    schema_cols = [
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [tn],
        ).fetchall()
    ]
    for col in ("open", "high", "low", "close"):
        if col not in schema_cols:
            continue
        nulls = con.execute(
            f"SELECT COUNT(*) FROM {tn} {_where()} AND ({col} IS NULL OR {col} = 0)",
            _params(),
        ).fetchone()[0]
        report.null_prices += nulls

    # Zero volume check
    if "volume" in schema_cols:
        zeros = con.execute(
            f"SELECT COUNT(*) FROM {tn} {_where()} AND (volume IS NULL OR volume = 0)",
            _params(),
        ).fetchone()[0]
        report.zero_volumes = zeros

    # Duplicate timestamps
    dups = con.execute(
        f"SELECT COUNT(*) FROM (SELECT ts_event FROM {tn} {_where()} GROUP BY ts_event HAVING COUNT(*) > 1)",
        _params(),
    ).fetchone()[0]
    report.duplicate_timestamps = dups

    # Date gaps
    dates = con.execute(
        f"SELECT DISTINCT ts_date FROM {tn} {_where()} AND ts_date IS NOT NULL ORDER BY ts_date",
        _params(),
    ).fetchall()
    if len(dates) > 1:
        date_strs = [str(d[0]) for d in dates]
        try:
            expected = _date_range(date_strs[0], date_strs[-1])
            report.date_gaps = [d for d in expected if d not in date_strs]
        except ValueError:
            logger.warning(
                "Date range computation failed for {}/{} — bad timestamps in data", tn, symbol
            )

    # Price outliers (Z-score > threshold)
    try:
        outliers = con.execute(
            f"SELECT COUNT(*) FROM {tn}, (SELECT AVG(close) AS avg_c, STDDEV(close) AS std_c FROM {tn} {_where()}) stats {_where()} AND ABS((close - avg_c) / NULLIF(std_c, 0)) > ?",
            _params() + _params() + [outlier_std],
        ).fetchone()[0]
        report.outlier_rows = outliers
    except Exception:
        logger.warning(
            "Outlier query failed for {}/{} — STDDEV may be 0 or schema mismatch",
            tn,
            symbol,
        )

    return report


def _table_exists(con: Any, table_name: str) -> bool:
    """Check if a DuckDB/DuckLake table exists."""
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except Exception:
        return False
