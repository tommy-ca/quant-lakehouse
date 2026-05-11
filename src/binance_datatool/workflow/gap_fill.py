"""Workflow for filling archive data gaps via REST API."""

from __future__ import annotations

import csv
import hashlib
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from binance_datatool.workflow.legacy.lineage import LineageEvent, LineageEventType

if TYPE_CHECKING:
    from collections.abc import Sequence

    from binance_datatool.common.enums import TradeType
    from binance_datatool.common.types import KlineData
    from binance_datatool.exchange.client import ExchangeClient
    from binance_datatool.workflow.legacy.lineage import LineageTracker


def _archive_path(
    archive_home: Path,
    trade_type: TradeType,
    data_type: str,
    symbol: str,
    interval: str | None = None,
) -> Path:
    """Build the local archive path for a given symbol."""
    path = archive_home / "data" / trade_type.s3_path / "daily" / data_type / symbol
    if interval:
        path = path / interval
    return path


def _csv_header(data_type: str) -> list[str]:
    """Return CSV header for the given data type.

    Matches Binance archive CSV format.
    """
    if data_type == "klines":
        return [
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
            "ignore",
        ]
    if data_type == "aggTrades":
        return [
            "agg_trade_id",
            "price",
            "quantity",
            "first_trade_id",
            "last_trade_id",
            "transact_time",
            "is_buyer_maker",
        ]
    if data_type == "fundingRate":
        return [
            "symbol",
            "funding_time",
            "funding_rate",
            "mark_price",
        ]
    return []


def _kline_to_row(kline: KlineData) -> list[str]:
    """Convert a KlineData to a CSV row."""
    return [
        str(kline.open_time),
        kline.open,
        kline.high,
        kline.low,
        kline.close,
        kline.volume,
        str(kline.close_time),
        kline.quote_volume,
        str(kline.num_trades),
        kline.taker_buy_volume,
        kline.taker_buy_quote_volume,
        "0",
    ]


def _write_csv(
    path: Path,
    header: list[str],
    rows: list[list[str]],
) -> None:
    """Write rows to a CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def _checksum(path: Path) -> str:
    """Compute SHA256 of a file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


_DATE_PATTERN = re.compile(r".*?-(\d{4}-\d{2}-\d{2})(?:\.zip|\.csv|\.filled\.csv)")


class GapFillResult:
    """Result of a gap-fill operation."""

    def __init__(self) -> None:
        self.filled: list[Path] = []
        self.failed: list[tuple[str, str]] = []
        self.gaps_detected: list[tuple[str, int, int]] = []
        self.lineage_events: list[LineageEvent] = []

    @property
    def files_filled(self) -> int:
        return len(self.filled)

    @property
    def files_failed(self) -> int:
        return len(self.failed)


def _scan_existing_dates(symbol_dir: Path) -> set[str]:
    """Scan a symbol directory for existing archive files and extract dates."""
    dates: set[str] = set()
    if not symbol_dir.exists():
        return dates
    for entry in symbol_dir.iterdir():
        if entry.is_dir() and entry.name == "_filled":
            continue
        if entry.is_dir():
            dates.update(_scan_existing_dates(entry))
            continue
        m = _DATE_PATTERN.match(entry.name)
        if m:
            dates.add(m.group(1))
    return dates


def _detect_date_gaps(
    existing_dates: set[str],
    trade_type: TradeType | None = None,
    lookback_days: int = 30,
) -> list[tuple[int, int]]:
    """Detect date gaps in the last N days.

    Returns list of (start_ms, end_ms) for each gap.
    """
    if not existing_dates:
        now = datetime.now(UTC)
        end = int(now.timestamp() * 1000)
        start = int((now - timedelta(days=lookback_days)).timestamp() * 1000)
        return [(start, end)]

    existing_parsed = sorted(
        datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=UTC) for d in existing_dates
    )
    now = datetime.now(UTC)
    gaps: list[tuple[int, int]] = []

    # Gap before earliest date
    earliest = existing_parsed[0]
    if (now - earliest).days < lookback_days:
        expected_earliest = now - timedelta(days=lookback_days)
        if earliest > expected_earliest:
            gaps.append(
                (
                    int(expected_earliest.timestamp() * 1000),
                    int(earliest.timestamp() * 1000),
                )
            )

    # Gaps between dates
    for i in range(len(existing_parsed) - 1):
        current = existing_parsed[i]
        next_date = existing_parsed[i + 1]
        gap_days = (next_date - current).days
        if gap_days > 1:
            gaps.append(
                (
                    int((current + timedelta(days=1)).timestamp() * 1000),
                    int(next_date.timestamp() * 1000),
                )
            )

    # Gap after latest date
    latest = existing_parsed[-1]
    days_since_latest = (now - latest).days
    if days_since_latest >= 1:
        gaps.append(
            (
                int((latest + timedelta(days=1)).timestamp() * 1000),
                int(now.timestamp() * 1000),
            )
        )

    return gaps


class GapFillWorkflow:
    """Workflow for filling archive data gaps via REST API."""

    def __init__(
        self,
        exchange_client: ExchangeClient,
        archive_home: Path,
        symbols: Sequence[str],
        data_type: str,
        interval: str | None = None,
        tracker: LineageTracker | None = None,
        lookback_days: int = 30,
    ) -> None:
        self._client = exchange_client
        self._archive_home = Path(archive_home)
        self._symbols = symbols
        self._data_type = data_type
        self._interval = interval
        self._tracker = tracker
        self._lookback_days = lookback_days

    def detect_gaps(
        self,
        trade_type: TradeType,
    ) -> list[tuple[str, int, int]]:
        """Detect date gaps for each symbol from local archive files.

        Returns list of (symbol, start_ms, end_ms).
        """
        gaps: list[tuple[str, int, int]] = []
        for symbol in self._symbols:
            symbol_dir = _archive_path(
                self._archive_home, trade_type, self._data_type, symbol, self._interval
            )
            existing = _scan_existing_dates(symbol_dir)
            symbol_gaps = _detect_date_gaps(existing, trade_type, self._lookback_days)
            for start_ms, end_ms in symbol_gaps:
                gaps.append((symbol, start_ms, end_ms))
                logger.info("Gap detected for {}: {} -> {}", symbol, start_ms, end_ms)
        return gaps

    def detect_gaps_from_ducklake(
        self,
        duckdb_path: str | None = None,
    ) -> list[tuple[str, int, int]]:
        """Detect date gaps from DuckLake native tables (post-sink).

        Queries DuckLake klines table for existing ts_event ranges and
        finds gaps vs expected lookback window.
        """
        import duckdb

        gaps: list[tuple[str, int, int]] = []
        try:
            con = duckdb.connect(duckdb_path or ":memory:")
            con.execute("LOAD ducklake")
            lake_path = self._archive_home.parent / "lake"
            meta = lake_path / "metadata.ducklake"
            con.execute(
                f"ATTACH 'ducklake:{meta}' AS dl (DATA_PATH '{lake_path}/data', AUTOMATIC_MIGRATION true)"
            )
            con.execute("USE dl")

            table = self._data_type.replace("-", "_")
            for symbol in self._symbols:
                dates = con.execute(
                    f"SELECT DISTINCT ts_date FROM {table} WHERE symbol = ? ORDER BY ts_date",
                    [symbol],
                ).fetchall()
                existing = {str(d[0]) for d in dates}
                symbol_gaps = _detect_date_gaps(existing, None, self._lookback_days)
                for start_ms, end_ms in symbol_gaps:
                    gaps.append((symbol, start_ms, end_ms))
                    logger.info("DuckLake gap for {}: {} -> {}", symbol, start_ms, end_ms)
            con.close()
        except Exception as e:
            logger.warning("DuckLake gap detection unavailable: {}", e)
        return gaps

    async def run(
        self,
        start_time: int | None = None,
        end_time: int | None = None,
        detect_gaps: bool = False,
    ) -> GapFillResult:
        """Run the gap-fill workflow.

        Args:
            start_time: Explicit start time (ms). Ignored if detect_gaps=True.
            end_time: Explicit end time (ms). Ignored if detect_gaps=True.
            detect_gaps: Auto-detect gaps before filling.
        """
        result = GapFillResult()
        trade_type = self._client.trade_type

        if detect_gaps:
            gaps = self.detect_gaps(trade_type)
            result.gaps_detected = gaps
            if not gaps:
                logger.info("No gaps detected for any symbol")
                return result
            logger.info("Detected {} gaps to fill", len(gaps))
        elif start_time is not None or end_time is not None:
            gaps = [(s, start_time or 0, end_time or 0) for s in self._symbols]
        else:
            logger.info("No gaps specified (use detect_gaps or start/end time)")
            return result

        for symbol, s_time, e_time in gaps:
            logger.info("Filling gap for {} {} [{}, {}]", symbol, self._data_type, s_time, e_time)
            try:
                data = await self._fetch_data(symbol, s_time, e_time)
                if not data:
                    logger.info("No data returned for {}", symbol)
                    continue
                paths = self._save_filled(trade_type, symbol, data)
                result.filled.extend(paths)
                row_count = len(data)
                logger.info("Filled {} rows for {} -> {}", row_count, symbol, paths)

                if self._tracker:
                    event = LineageEvent(
                        source=f"binance_{trade_type.value}",
                        symbol=symbol,
                        event_type=LineageEventType.FILLED,
                        timestamp=datetime.now(UTC),
                        message=f"Gap filled: {self._data_type} from REST API",
                        metadata={
                            "data_type": self._data_type,
                            "interval": self._interval,
                            "start_ms": s_time,
                            "end_ms": e_time,
                            "row_count": row_count,
                            "files": [str(p) for p in paths],
                        },
                    )
                    self._tracker.record(event)
                    result.lineage_events.append(event)
            except Exception as e:
                logger.error("Failed to fill {}: {}", symbol, e)
                result.failed.append((symbol, str(e)))

        return result

    async def _fetch_data(
        self,
        symbol: str,
        start_time: int | None,
        end_time: int | None,
    ) -> list:
        """Fetch data from REST API based on data type."""
        client = self._client

        if self._data_type == "klines":
            return await client.fetch_ohlcv(
                symbol=symbol,
                interval=self._interval or "1m",
                since=start_time,
                until=end_time,
            )
        if self._data_type == "aggTrades":
            return await client.fetch_agg_trades(
                symbol=symbol,
                since=start_time,
                until=end_time,
            )
        if self._data_type == "fundingRate":
            return await client.fetch_funding_rate(
                symbol=symbol,
                since=start_time,
                until=end_time,
            )
        msg = f"Unsupported data type: {self._data_type}"
        raise ValueError(msg)

    def _save_filled(
        self,
        trade_type: TradeType,
        symbol: str,
        data: list,
    ) -> list[Path]:
        """Save fetched data as CSV in the archive hierarchy."""
        base = _archive_path(
            self._archive_home,
            trade_type,
            self._data_type,
            symbol,
            self._interval,
        )
        filled_dir = base / "_filled"
        filled_dir.mkdir(parents=True, exist_ok=True)

        header = _csv_header(self._data_type)
        paths = []

        if self._data_type == "klines":
            rows = [_kline_to_row(k) for k in data]
            csv_path = filled_dir / f"{symbol}-{self._interval}-filled.csv"
            _write_csv(csv_path, header, rows)
            paths.append(csv_path)
        elif self._data_type == "aggTrades":
            rows = []
            for t in data:
                # Normalize Pydantic model or dict to flat dict
                record = t
                if hasattr(t, "model_dump"):
                    record = t.model_dump()
                elif not isinstance(t, dict):
                    record = dict(t) if hasattr(t, "__dict__") else {"val": str(t)}
                if isinstance(record, dict):
                    rows.append(
                        [
                            str(record.get("a", "")),
                            str(record.get("p", "")),
                            str(record.get("q", "")),
                            str(record.get("f", "")),
                            str(record.get("l", "")),
                            str(record.get("T", "")),
                            str(record.get("m", "")),  # is_buyer_maker in API
                        ]
                    )
            csv_path = filled_dir / f"{symbol}-aggTrades-filled.csv"
            _write_csv(csv_path, header, rows)
            paths.append(csv_path)
        elif self._data_type == "fundingRate":
            rows = []
            for fr in data:
                record = fr
                if hasattr(fr, "model_dump"):
                    record = fr.model_dump()
                elif not isinstance(fr, dict):
                    record = dict(fr) if hasattr(fr, "__dict__") else {"val": str(fr)}
                if isinstance(record, dict):
                    rows.append(
                        [
                            str(record.get("symbol", "")),
                            str(record.get("fundingTime", "")),
                            str(record.get("fundingRate", "")),
                            str(record.get("markPrice", "")),
                        ]
                    )
            csv_path = filled_dir / f"{symbol}-fundingRate-filled.csv"
            _write_csv(csv_path, header, rows)
            paths.append(csv_path)

        # Create checksum sidecar
        for p in paths:
            cs = _checksum(p)
            cs_path = p.with_suffix(p.suffix + ".CHECKSUM")
            cs_path.write_text(f"{cs}  {p.name}\n")

        return paths
