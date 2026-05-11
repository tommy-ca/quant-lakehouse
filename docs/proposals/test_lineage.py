"""Tests for lineage tracking."""

from datetime import datetime, timedelta

import pytest

from binance_datatool.workflow.legacy.lineage import LineageEvent, LineageEventType, LineageTracker


class TestLineageEvent:
    """Test LineageEvent data class."""

    def test_create_basic_event(self):
        """Test creating a basic lineage event."""
        event = LineageEvent(
            source="binance",
            symbol="BTCUSDT",
            event_type=LineageEventType.DOWNLOADED,
            timestamp=datetime(2024, 1, 1, 0, 0, 0),
        )
        assert event.source == "binance"
        assert event.symbol == "BTCUSDT"
        assert event.event_type == LineageEventType.DOWNLOADED
        assert event.date is None
        assert event.message == ""
        assert event.metadata == {}

    def test_create_event_with_all_fields(self):
        """Test creating an event with all optional fields."""
        metadata = {"file": "BTCUSDT-1d-2024-01-01.zip", "size_bytes": 1024}
        event = LineageEvent(
            source="binance",
            symbol="BTCUSDT",
            date="2024-01-01",
            event_type=LineageEventType.VERIFIED,
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            message="Hash verification passed",
            metadata=metadata,
        )
        assert event.source == "binance"
        assert event.symbol == "BTCUSDT"
        assert event.date == "2024-01-01"
        assert event.message == "Hash verification passed"
        assert event.metadata == metadata

    def test_event_is_frozen(self):
        """Test that LineageEvent is immutable."""
        event = LineageEvent(
            source="binance",
            symbol="BTCUSDT",
            event_type=LineageEventType.DOWNLOADED,
            timestamp=datetime(2024, 1, 1),
        )
        with pytest.raises(AttributeError):
            event.source = "coinbase"

    def test_event_to_dict(self):
        """Test converting event to dict."""
        event = LineageEvent(
            source="binance",
            symbol="BTCUSDT",
            date="2024-01-01",
            event_type=LineageEventType.DOWNLOADED,
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            message="Test message",
            metadata={"key": "value"},
        )
        d = event.to_dict()
        assert d["source"] == "binance"
        assert d["symbol"] == "BTCUSDT"
        assert d["date"] == "2024-01-01"
        assert d["event_type"] == "downloaded"
        assert d["timestamp"] == "2024-01-01T12:00:00"
        assert d["message"] == "Test message"
        assert d["metadata"] == {"key": "value"}


class TestLineageTracker:
    """Test LineageTracker class."""

    @pytest.fixture
    def tracker(self):
        """Create a fresh LineageTracker instance."""
        return LineageTracker()

    def test_record_single_event(self, tracker):
        """Test recording a single event."""
        event = LineageEvent(
            source="binance",
            symbol="BTCUSDT",
            event_type=LineageEventType.DOWNLOADED,
            timestamp=datetime(2024, 1, 1),
        )
        tracker.record(event)
        assert len(tracker.all_events()) == 1
        assert tracker.all_events()[0] == event

    def test_record_many_events(self, tracker):
        """Test recording multiple events."""
        events = [
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DISCOVERED,
                timestamp=datetime(2024, 1, 1),
            ),
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 2),
            ),
        ]
        tracker.record_many(events)
        assert len(tracker.all_events()) == 2
        assert tracker.all_events() == events

    def test_query_by_source(self, tracker):
        """Test querying by source."""
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1),
            )
        )
        tracker.record(
            LineageEvent(
                source="coinbase",
                symbol="BTC-USD",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1),
            )
        )

        binance_events = tracker.query(source="binance")
        assert len(binance_events) == 1
        assert binance_events[0].source == "binance"

        coinbase_events = tracker.query(source="coinbase")
        assert len(coinbase_events) == 1
        assert coinbase_events[0].source == "coinbase"

    def test_query_by_symbol(self, tracker):
        """Test querying by symbol."""
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1),
            )
        )
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="ETHUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1),
            )
        )

        btc_events = tracker.query(symbol="BTCUSDT")
        assert len(btc_events) == 1
        assert btc_events[0].symbol == "BTCUSDT"

    def test_query_by_date(self, tracker):
        """Test querying by date."""
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                date="2024-01-01",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1),
            )
        )
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                date="2024-01-02",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 2),
            )
        )

        jan1_events = tracker.query(date="2024-01-01")
        assert len(jan1_events) == 1
        assert jan1_events[0].date == "2024-01-01"

    def test_query_by_event_type(self, tracker):
        """Test querying by event type."""
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1),
            )
        )
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.VERIFIED,
                timestamp=datetime(2024, 1, 1),
            )
        )

        downloaded = tracker.query(event_type=LineageEventType.DOWNLOADED)
        assert len(downloaded) == 1
        assert downloaded[0].event_type == LineageEventType.DOWNLOADED

    def test_query_by_date_range(self, tracker):
        """Test querying by datetime range."""
        start = datetime(2024, 1, 1)
        middle = datetime(2024, 1, 15)
        end = datetime(2024, 2, 1)

        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=start,
            )
        )
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=middle,
            )
        )
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=end,
            )
        )

        # Query events between start and end (exclusive on end)
        date_range = (start + timedelta(days=1), end - timedelta(days=1))
        events = tracker.query(date_range=date_range)
        assert len(events) == 1
        assert events[0].timestamp == middle

    def test_query_multiple_filters(self, tracker):
        """Test querying with multiple filters."""
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1),
            )
        )
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="ETHUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1),
            )
        )
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.VERIFIED,
                timestamp=datetime(2024, 1, 1),
            )
        )

        # Query for BTCUSDT downloaded events
        events = tracker.query(
            source="binance",
            symbol="BTCUSDT",
            event_type=LineageEventType.DOWNLOADED,
        )
        assert len(events) == 1
        assert events[0].symbol == "BTCUSDT"
        assert events[0].event_type == LineageEventType.DOWNLOADED

    def test_query_empty_result(self, tracker):
        """Test querying with no matches."""
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1),
            )
        )

        events = tracker.query(source="coinbase")
        assert len(events) == 0

    def test_get_latest_event(self, tracker):
        """Test retrieving the most recent event."""
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DISCOVERED,
                timestamp=datetime(2024, 1, 1, 0, 0, 0),
            )
        )
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1, 1, 0, 0),
            )
        )
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.VERIFIED,
                timestamp=datetime(2024, 1, 1, 2, 0, 0),
            )
        )

        latest = tracker.get_latest(source="binance", symbol="BTCUSDT")
        assert latest is not None
        assert latest.event_type == LineageEventType.VERIFIED

    def test_get_latest_no_match(self, tracker):
        """Test get_latest with no matching events."""
        latest = tracker.get_latest(source="nonexistent")
        assert latest is None

    def test_count_events(self, tracker):
        """Test counting events."""
        for i in range(3):
            tracker.record(
                LineageEvent(
                    source="binance",
                    symbol="BTCUSDT",
                    event_type=LineageEventType.DOWNLOADED,
                    timestamp=datetime(2024, 1, 1) + timedelta(hours=i),
                )
            )

        assert tracker.count(source="binance") == 3
        assert tracker.count(source="binance", symbol="BTCUSDT") == 3
        assert tracker.count(source="coinbase") == 0
        assert tracker.count(event_type=LineageEventType.VERIFIED) == 0

    def test_clear_events(self, tracker):
        """Test clearing all events."""
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1),
            )
        )
        assert len(tracker.all_events()) == 1

        tracker.clear()
        assert len(tracker.all_events()) == 0

    def test_export_json(self, tracker):
        """Test exporting events as JSON."""
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1, 12, 0, 0),
                message="Download complete",
                metadata={"file": "test.zip"},
            )
        )

        json_str = tracker.export(format="json")
        assert isinstance(json_str, str)
        assert "binance" in json_str
        assert "BTCUSDT" in json_str
        assert "downloaded" in json_str

        # Verify it's valid JSON
        import json

        data = json.loads(json_str)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["source"] == "binance"

    def test_export_jsonl(self, tracker):
        """Test exporting events as JSONL."""
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1),
            )
        )
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="ETHUSDT",
                event_type=LineageEventType.VERIFIED,
                timestamp=datetime(2024, 1, 2),
            )
        )

        jsonl_str = tracker.export(format="jsonl")
        lines = jsonl_str.strip().split("\n")
        assert len(lines) == 2

        # Verify each line is valid JSON
        import json

        for line in lines:
            data = json.loads(line)
            assert "source" in data

    def test_export_csv(self, tracker):
        """Test exporting events as CSV."""
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1, 12, 0, 0),
                metadata={"file": "test.zip", "size": "1024"},
            )
        )
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="ETHUSDT",
                event_type=LineageEventType.VERIFIED,
                timestamp=datetime(2024, 1, 2),
                metadata={"hash": "abc123"},
            )
        )

        csv_str = tracker.export(format="csv")
        lines = csv_str.strip().split("\n")
        # Header + 2 events
        assert len(lines) == 3
        # Verify header exists
        assert "source" in lines[0]
        assert "symbol" in lines[0]

    def test_export_csv_empty(self, tracker):
        """Test exporting empty tracker as CSV."""
        csv_str = tracker.export(format="csv")
        assert csv_str == ""

    def test_export_unsupported_format(self, tracker):
        """Test exporting with unsupported format."""
        with pytest.raises(ValueError, match="Unsupported export format"):
            tracker.export(format="xml")

    def test_stats(self, tracker):
        """Test generating statistics."""
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 1),
            )
        )
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="BTCUSDT",
                event_type=LineageEventType.VERIFIED,
                timestamp=datetime(2024, 1, 1),
            )
        )
        tracker.record(
            LineageEvent(
                source="binance",
                symbol="ETHUSDT",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 2),
            )
        )
        tracker.record(
            LineageEvent(
                source="coinbase",
                symbol="BTC-USD",
                event_type=LineageEventType.DOWNLOADED,
                timestamp=datetime(2024, 1, 2),
            )
        )

        stats = tracker.stats()
        assert stats["total_events"] == 4
        assert stats["by_event_type"]["downloaded"] == 3
        assert stats["by_event_type"]["verified"] == 1
        assert stats["by_source"]["binance"] == 3
        assert stats["by_source"]["coinbase"] == 1
        assert len(stats["by_symbol"]) == 3
        assert stats["unique_symbols"] == 3
        assert "BTCUSDT" in stats["by_symbol"]

    def test_stats_empty(self, tracker):
        """Test stats on empty tracker."""
        stats = tracker.stats()
        assert stats["total_events"] == 0
        assert stats["by_event_type"] == {}
        assert stats["by_source"] == {}
        assert stats["by_symbol"] == []
        assert stats["unique_symbols"] == 0
