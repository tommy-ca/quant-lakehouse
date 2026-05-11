"""Lineage tracking for data provenance and transformation history.

LineageTracker records and queries data source operations to enable:
- Provenance tracking (where did this data come from?)
- Transformation audit trails (what happened to it?)
- Data quality tracking (was it verified?)
- Recovery and replay (what versions exist?)

Example:
    ```python
    tracker = LineageTracker()

    # Record a download event
    tracker.record(LineageEvent(
        source="binance",
        symbol="BTCUSDT",
        timestamp=datetime(2024, 1, 1, 0, 0, 0),
        event_type=LineageEventType.DOWNLOADED,
        metadata={"file": "BTCUSDT-1d-2024-01-01.zip"}
    ))

    # Query lineage for a symbol
    events = tracker.query(symbol="BTCUSDT")

    # Export all events
    json_str = tracker.export(format="json")
    ```
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    import polars as pl


class LineageEventType(StrEnum):
    """Types of lineage events."""

    DISCOVERED = "discovered"  # Symbol/file discovered
    FETCHED = "fetched"  # File fetched from source
    DOWNLOADED = "downloaded"  # File downloaded to local/S3
    VERIFIED = "verified"  # File verified (hash checked)
    VERIFICATION_FAILED = "verification_failed"  # Hash mismatch
    PARSED = "parsed"  # File parsed into records
    VALIDATED = "validated"  # Data validated against contract
    VALIDATION_FAILED = "validation_failed"  # Validation failed
    TRANSFORMED = "transformed"  # Data transformed
    LOADED = "loaded"  # Data loaded to storage
    REJECTED = "rejected"  # Data rejected
    FILLED = "filled"  # Gap filled via REST API
    HEALTH_CHECKED = "health_checked"  # Health check ran
    SUNK = "sunk"  # Data sinked to DuckLake native table
    STREAMED = "streamed"  # Data received via WebSocket stream
    ANOMALY_DETECTED = "anomaly_detected"  # Anomaly found in health check
    EXPORTED = "exported"  # Data exported for downstream use
    BACKTESTED = "backtested"  # Data used in backtesting


@dataclass(frozen=True)
class LineageEvent:
    """A single lineage event.

    Attributes:
        source: Data source (e.g., "binance", "coinbase").
        symbol: Trading pair symbol (e.g., "BTCUSDT", "BTC").
        date: Date associated with the data (if applicable).
        event_type: Type of event (discovered, downloaded, verified, etc.).
        timestamp: When the event occurred.
        message: Human-readable description.
        metadata: Additional event-specific data (file path, hash, row count).
    """

    source: str
    symbol: str
    event_type: LineageEventType
    timestamp: datetime
    date: str | None = None
    message: str = ""
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary (JSON-serializable)."""
        d = asdict(self)
        d["event_type"] = self.event_type.value
        d["timestamp"] = self.timestamp.isoformat()
        return d


class LineageTracker:
    """Accumulate and query lineage events.

    Thread-safe in-memory store of lineage events with query and export.
    """

    def __init__(self) -> None:
        """Initialize empty tracker."""
        self._events: list[LineageEvent] = []

    def record(self, event: LineageEvent) -> None:
        """Record a lineage event.

        Args:
            event: LineageEvent to record.
        """
        self._events.append(event)

    def record_many(self, events: list[LineageEvent]) -> None:
        """Record multiple lineage events.

        Args:
            events: List of LineageEvents to record.
        """
        self._events.extend(events)

    def query(
        self,
        source: str | None = None,
        symbol: str | None = None,
        date: str | None = None,
        event_type: LineageEventType | None = None,
        date_range: tuple[datetime, datetime] | None = None,
    ) -> list[LineageEvent]:
        """Query lineage events with optional filters.

        Args:
            source: Filter by source (e.g., "binance").
            symbol: Filter by symbol (e.g., "BTCUSDT").
            date: Filter by date (e.g., "2024-01-01").
            event_type: Filter by event type.
            date_range: Filter by (start, end) datetime tuple.

        Returns:
            List of LineageEvents matching all filters.
        """
        result = []

        for event in self._events:
            # Apply filters
            if source and event.source != source:
                continue
            if symbol and event.symbol != symbol:
                continue
            if date and event.date != date:
                continue
            if event_type and event.event_type != event_type:
                continue
            if date_range:
                start, end = date_range
                if not (start <= event.timestamp <= end):
                    continue

            result.append(event)

        return result

    def get_latest(
        self,
        source: str | None = None,
        symbol: str | None = None,
    ) -> LineageEvent | None:
        """Get the most recent lineage event matching filters.

        Args:
            source: Filter by source.
            symbol: Filter by symbol.

        Returns:
            Most recent LineageEvent, or None if no matches.
        """
        matches = self.query(source=source, symbol=symbol)
        if not matches:
            return None
        return max(matches, key=lambda e: e.timestamp)

    def count(
        self,
        source: str | None = None,
        symbol: str | None = None,
        event_type: LineageEventType | None = None,
    ) -> int:
        """Count lineage events matching filters.

        Args:
            source: Filter by source.
            symbol: Filter by symbol.
            event_type: Filter by event type.

        Returns:
            Count of matching events.
        """
        return len(self.query(source=source, symbol=symbol, event_type=event_type))

    def clear(self) -> None:
        """Clear all lineage events."""
        self._events.clear()

    def all_events(self) -> list[LineageEvent]:
        """Return all recorded events."""
        return self._events.copy()

    def export(self, format: str = "json") -> str:
        """Export all lineage events in specified format.

        Args:
            format: Export format ("json", "csv", or "jsonl").

        Returns:
            Serialized lineage data as string.

        Raises:
            ValueError: If format is not supported.
        """
        if format == "json":
            return self._export_json()
        elif format == "jsonl":
            return self._export_jsonl()
        elif format == "csv":
            return self._export_csv()
        else:
            raise ValueError(f"Unsupported export format: {format}. Supported: json, jsonl, csv")

    def _export_json(self) -> str:
        """Export events as JSON array."""
        events_dicts = [e.to_dict() for e in self._events]
        return json.dumps(events_dicts, indent=2)

    def _export_jsonl(self) -> str:
        """Export events as JSON Lines (one per line)."""
        lines = [json.dumps(e.to_dict()) for e in self._events]
        return "\n".join(lines)

    def _export_csv(self) -> str:
        """Export events as CSV."""
        if not self._events:
            return ""

        # Get all unique keys from metadata across events
        import csv
        from io import StringIO

        # Build headers
        headers = [
            "source",
            "symbol",
            "date",
            "event_type",
            "timestamp",
            "message",
        ]

        # Collect all unique metadata keys
        metadata_keys = set()
        for event in self._events:
            metadata_keys.update(event.metadata.keys())

        headers.extend(sorted(metadata_keys))

        # Write CSV
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()

        for event in self._events:
            row = {
                "source": event.source,
                "symbol": event.symbol,
                "date": event.date or "",
                "event_type": event.event_type.value,
                "timestamp": event.timestamp.isoformat(),
                "message": event.message,
            }
            # Add metadata columns
            for key in metadata_keys:
                row[key] = event.metadata.get(key, "")

            writer.writerow(row)

        return output.getvalue()

    def to_dataframe(self) -> pl.DataFrame:
        """Export lineage events as a Polars DataFrame.

        Returns:
            DataFrame with columns: source, symbol, date, event_type,
            timestamp, message, plus metadata columns.
        """
        import polars as pl

        rows = []
        for event in self._events:
            row = {
                "source": event.source,
                "symbol": event.symbol,
                "date": event.date or "",
                "event_type": event.event_type.value,
                "timestamp": event.timestamp.isoformat(),
                "message": event.message,
                "metadata": json.dumps(event.metadata),
            }
            rows.append(row)
        return pl.DataFrame(rows) if rows else pl.DataFrame()

    def save_ducklake(self, duckdb_path: str, catalog_path: str | None = None) -> int:
        """Persist lineage events to a DuckLake native table.

        Creates/updates a ``lineage`` table in DuckLake so downstream
        tools (backtesting, replay, live trading) can query data provenance.

        Args:
            duckdb_path: Path to DuckDB database file.
            catalog_path: Path to DuckLake catalog (default: auto-derived).

        Returns:
            Number of events persisted.
        """
        if not self._events:
            return 0
        import duckdb

        lake = Path(catalog_path) if catalog_path else Path(duckdb_path).parent / "lake"
        meta = lake / "metadata.ducklake"

        con = duckdb.connect(duckdb_path)
        try:
            con.execute("LOAD ducklake")
            con.execute(
                f"ATTACH 'ducklake:{meta}' AS dl (DATA_PATH '{lake}/data', AUTOMATIC_MIGRATION true)"
            )
            con.execute("USE dl")

            # Create lineage table if not exists
            con.execute(
                "CREATE TABLE IF NOT EXISTS lineage ("
                "source VARCHAR, symbol VARCHAR, date VARCHAR, "
                "event_type VARCHAR, timestamp VARCHAR, message VARCHAR, "
                "metadata VARCHAR, ingested_at BIGINT"
                ")"
            )

            now_ms = int(datetime.now().timestamp() * 1000)
            for event in self._events:
                con.execute(
                    "INSERT INTO lineage VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        event.source,
                        event.symbol,
                        event.date or "",
                        event.event_type.value,
                        event.timestamp.isoformat(),
                        event.message,
                        json.dumps(event.metadata),
                        now_ms,
                    ],
                )

            count = con.execute("SELECT COUNT(*) FROM lineage").fetchone()[0]
            logger.info("Lineage: saved {} events to DuckLake table", count)
        finally:
            con.close()
        return len(self._events)

    def stats(self) -> dict:
        """Return summary statistics about recorded events.

        Returns:
            Dictionary with event counts by type, source, etc.
        """
        stats_dict = {
            "total_events": len(self._events),
            "by_event_type": {},
            "by_source": {},
            "by_symbol": set(),
        }

        for event in self._events:
            # Count by event type
            event_type_str = event.event_type.value
            stats_dict["by_event_type"][event_type_str] = (
                stats_dict["by_event_type"].get(event_type_str, 0) + 1
            )

            # Count by source
            stats_dict["by_source"][event.source] = stats_dict["by_source"].get(event.source, 0) + 1

            # Track unique symbols
            stats_dict["by_symbol"].add(event.symbol)

        # Convert set to list for JSON serialization
        stats_dict["by_symbol"] = sorted(stats_dict["by_symbol"])
        stats_dict["unique_symbols"] = len(stats_dict["by_symbol"])

        return stats_dict
