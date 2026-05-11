# Skills & Subagents Specification (Proposal)

> **Status: PROPOSAL** — The skills framework and subagent protocols described here are aspirational.
> The `skills/` module does not yet exist in the codebase. See `docs/requirements.md` (Section 8) and
> `docs/proposals/` for the current implementation status. Implement per AGENTS.md documentation accuracy rule.

This document defines the formal skills and subagent protocols for `binance-datatool`. Skills are small, composable units of functionality that can be invoked by CLI users, scripts, or AI agents.

---

## 1. Skill: `discover-symbols`

**Purpose**: Query a data source for all available trading symbols, with optional filtering.

### Input Schema

```json
{
  "source": "string (binance|coinbase|kraken)",
  "market_type": "string (spot|um|cm|options)",
  "data_type": "string (klines|trades|aggTrades|bookDepth|bookTicker|fundingRate)",
  "partition_freq": "string (daily|monthly|hourly)",
  "quote_asset": "string|null (e.g., 'USDT')",
  "exclude_leverage": "boolean (skip 2x, 3x, 5x... symbols)",
  "exclude_stables": "boolean (skip stablecoin pairs like USDTUSD)",
  "timeout_seconds": "int (default: 30)"
}
```

### Output Schema

```json
{
  "success": "boolean",
  "symbols": ["string"],
  "filtered_out": ["string"],
  "errors": ["string"],
  "counts": {
    "total_found": "int",
    "matched": "int",
    "filtered": "int"
  },
  "duration_seconds": "float"
}
```

### Error Modes

| Error | Behavior |
|-------|----------|
| `source_not_configured` | Return `success: false`, empty symbols, error message |
| `source_not_found` | Return `success: false`, error message (404 → no symbols) |
| `network_timeout` | Retry 3x with exponential backoff; if all fail, return error |
| `rate_limited` | Backoff and retry; if limit not lifted, return error |
| `invalid_input` | Return validation error immediately (fail fast) |

### Success Criteria

- Returns all available symbols for the given market type and data type
- Applies filters correctly (quote asset, leverage, stables)
- Results are sorted alphabetically
- Empty list is OK if market not found (not an error)

### Test Cases

```python
# Happy path: list Binance spot symbols with USDT filter
test_discover_symbols_binance_spot_usdt():
    result = await discover_symbols(
        source="binance",
        market_type="spot",
        data_type="klines",
        partition_freq="daily",
        quote_asset="USDT"
    )
    assert result["success"] is True
    assert len(result["symbols"]) > 100
    assert "BTCUSDT" in result["symbols"]
    assert all("USDT" in s for s in result["symbols"])

# Edge case: exclude leverage
test_discover_symbols_exclude_leverage():
    result = await discover_symbols(
        source="binance",
        market_type="um",
        exclude_leverage=True
    )
    assert result["success"] is True
    assert not any("5L" in s or "3L" in s for s in result["symbols"])

# Edge case: unmatched filter
test_discover_symbols_no_matches():
    result = await discover_symbols(
        source="binance",
        market_type="spot",
        quote_asset="NONEXISTENT"
    )
    assert result["success"] is True
    assert result["symbols"] == []
    assert result["matched"] == 0

# Error case: network timeout
test_discover_symbols_network_timeout():
    result = await discover_symbols(
        source="binance",
        market_type="spot",
        timeout_seconds=0.001
    )
    assert result["success"] is False
    assert "timeout" in result["errors"][0].lower()
```

---

## 2. Skill: `list-files`

**Purpose**: List available data files for one or more symbols.

### Input Schema

```json
{
  "symbols": ["string"],
  "source": "string (binance|coinbase|kraken)",
  "market_type": "string",
  "data_type": "string",
  "partition_freq": "string",
  "interval": "string|null (1m, 5m, 1h, 1d, 1Mo, ...)",
  "timeout_seconds": "int (default: 30)"
}
```

### Output Schema

```json
{
  "success": "boolean",
  "per_symbol": [
    {
      "symbol": "string",
      "file_count": "int",
      "total_bytes": "int",
      "date_range": ["string", "string"],
      "files": [
        {
          "key": "string",
          "url": "string",
          "size": "int",
          "last_modified": "ISO8601",
          "checksum": "string|null"
        }
      ],
      "error": "string|null"
    }
  ],
  "summary": {
    "total_files": "int",
    "total_bytes": "int",
    "errors_count": "int"
  },
  "duration_seconds": "float"
}
```

### Error Modes

| Error | Behavior |
|-------|----------|
| `symbol_not_found` | Include symbol in result with `error: "Symbol not found"`, continue |
| `network_timeout` | Per-symbol: retry 3x, then include error in result |
| `invalid_interval` | Return validation error immediately |
| `partial_failure` | Return all symbols; some with files, some with errors |

### Success Criteria

- Returns files for all symbols (or error per symbol)
- Files are sorted by last_modified (ascending)
- Date range is correctly computed
- Partial failures don't abort the entire request

### Test Cases

```python
# Happy path: list files for multiple symbols
test_list_files_happy_path():
    result = await list_files(
        symbols=["BTCUSDT", "ETHUSDT"],
        source="binance",
        market_type="spot",
        data_type="klines",
        partition_freq="daily",
        interval="1d"
    )
    assert result["success"] is True
    assert len(result["per_symbol"]) == 2
    assert result["per_symbol"][0]["file_count"] > 100
    assert result["per_symbol"][0]["date_range"][0] < result["per_symbol"][0]["date_range"][1]

# Edge case: one symbol fails
test_list_files_partial_failure():
    result = await list_files(
        symbols=["BTCUSDT", "NONEXISTENTPAIR"],
        source="binance",
        market_type="spot"
    )
    assert result["success"] is True  # Partial success
    assert result["per_symbol"][0]["file_count"] > 0
    assert result["per_symbol"][1]["error"] is not None

# Edge case: no files for symbol
test_list_files_empty_symbol():
    result = await list_files(
        symbols=["BTCUSDT"],
        data_type="klines",
        interval="1Mo"  # Very rare
    )
    assert result["success"] is True
    assert result["per_symbol"][0]["file_count"] == 0
    assert result["per_symbol"][0]["files"] == []
```

---

## 3. Skill: `download-partition`

**Purpose**: Download data for symbols on a specific partition (date/month).

### Input Schema

```json
{
  "symbols": ["string"],
  "source": "string",
  "market_type": "string",
  "data_type": "string",
  "partition_freq": "string",
  "interval": "string|null",
  "partition_key": "string (e.g., '2026-01-01' for daily, '2026-01' for monthly)",
  "archive_home": "string (output directory)",
  "dry_run": "boolean (default: false)",
  "cleanup_failures": "boolean (delete failed downloads; default: false)",
  "timeout_seconds": "int (default: 600)"
}
```

### Output Schema

```json
{
  "success": "boolean",
  "dry_run": "boolean",
  "summary": {
    "to_download": "int",
    "downloaded": "int",
    "failed": "int",
    "skipped": "int",
    "total_bytes": "int",
    "throughput_mbps": "float"
  },
  "per_symbol": [
    {
      "symbol": "string",
      "status": "string (success|partial|failed)",
      "downloaded": "int",
      "failed": "int",
      "errors": ["string"]
    }
  ],
  "duration_seconds": "float"
}
```

### Error Modes

| Error | Behavior |
|-------|----------|
| `archive_home_not_writable` | Fail fast; return error |
| `all_symbols_failed` | Return `success: false`, error details |
| `some_symbols_failed` | Return `success: true` (partial), include per-symbol errors |
| `network_timeout` | Retry file downloads 5x; if exhausted, mark as failed |

### Success Criteria

- All requested files downloaded successfully (or `dry_run: true` shows what would download)
- Checksum files written alongside .zip files
- Interrupted downloads can be resumed (diff + download skips existing)
- On `dry_run: false` and all success → exit code 0

### Test Cases

```python
# Happy path: dry-run shows what would download
test_download_partition_dry_run():
    result = await download_partition(
        symbols=["BTCUSDT"],
        source="binance",
        market_type="spot",
        data_type="klines",
        partition_freq="daily",
        partition_key="2026-01-01",
        archive_home="/tmp/archive",
        dry_run=True
    )
    assert result["dry_run"] is True
    assert result["success"] is True
    assert result["summary"]["to_download"] > 0
    assert result["summary"]["downloaded"] == 0  # Dry run doesn't download

# Edge case: resumable download (some files already exist)
test_download_partition_resumable():
    # First download: 100 files
    result1 = await download_partition(symbols=["BTCUSDT"], ...)
    assert result1["summary"]["downloaded"] == 100

    # Second call: skips already downloaded, only new files
    result2 = await download_partition(symbols=["BTCUSDT"], ...)
    assert result2["summary"]["downloaded"] == 0  # All skipped
    assert result2["summary"]["skipped"] == 100

# Error case: partition doesn't exist
test_download_partition_no_data():
    result = await download_partition(
        symbols=["BTCUSDT"],
        partition_key="1999-01-01"  # Before Binance existed
    )
    assert result["success"] is True
    assert result["summary"]["to_download"] == 0
```

---

## 4. Skill: `verify-partition`

**Purpose**: Verify integrity of downloaded files via checksum.

### Input Schema

```json
{
  "symbols": ["string"],
  "source": "string",
  "market_type": "string",
  "data_type": "string",
  "partition_freq": "string",
  "archive_home": "string",
  "dry_run": "boolean (default: false)",
  "cleanup_failed": "boolean (delete failed files; default: false)",
  "timeout_seconds": "int (default: 300)"
}
```

### Output Schema

```json
{
  "success": "boolean",
  "dry_run": "boolean",
  "summary": {
    "to_verify": "int",
    "verified": "int",
    "failed": "int",
    "skipped": "int",
    "orphans_cleaned": "int",
    "throughput_mbps": "float"
  },
  "per_symbol": [
    {
      "symbol": "string",
      "verified": "int",
      "failed": "int",
      "errors": ["string"]
    }
  ],
  "duration_seconds": "float"
}
```

### Error Modes

| Error | Behavior |
|-------|----------|
| `checksum_mismatch` | Mark file as failed; optionally delete (if `cleanup_failed: true`) |
| `orphaned_file` | File exists but no .checksum → clean (if `cleanup_failed: true`) or warn |
| `archive_home_not_readable` | Fail fast |

### Success Criteria

- All downloaded files verified successfully (hash matches .checksum)
- Failed files can be deleted (with `cleanup_failed: true`)
- Already-verified files are skipped (via .verified marker)
- Exit code 0 if all verified, 2 if any failures

### Test Cases

```python
# Happy path: verify downloaded files
test_verify_partition_all_valid():
    result = await verify_partition(
        symbols=["BTCUSDT"],
        source="binance",
        archive_home="/tmp/archive"
    )
    assert result["success"] is True
    assert result["summary"]["verified"] > 0
    assert result["summary"]["failed"] == 0

# Edge case: skip already verified
test_verify_partition_skip_verified():
    # First verify
    result1 = await verify_partition(symbols=["BTCUSDT"], ...)
    verified1 = result1["summary"]["verified"]

    # Second verify: should skip (via .verified marker)
    result2 = await verify_partition(symbols=["BTCUSDT"], ...)
    assert result2["summary"]["skipped"] == verified1
    assert result2["summary"]["to_verify"] == 0

# Error case: corrupted file
test_verify_partition_corrupted_file():
    # Corrupt a file by modifying bytes
    corrupt_file("/tmp/archive/.../BTCUSDT_2026-01-01_1d.zip")

    result = await verify_partition(symbols=["BTCUSDT"], ...)
    assert result["success"] is False
    assert result["summary"]["failed"] == 1
    assert "checksum mismatch" in result["per_symbol"][0]["errors"][0].lower()
```

---

## 5. Skill: `validate-contract`

**Purpose**: Validate data files against a schema contract.

### Input Schema

```json
{
  "symbol": "string",
  "source": "string",
  "market_type": "string",
  "data_type": "string",
  "partition_freq": "string",
  "partition_key": "string",
  "archive_home": "string",
  "extract_format": "string (csv|json|parquet; default: csv)"
}
```

### Output Schema

```json
{
  "success": "boolean",
  "symbol": "string",
  "row_count": "int",
  "validation": {
    "passed": "boolean",
    "error_count": "int",
    "errors": [
      {
        "row_index": "int|null",
        "column": "string|null",
        "reason": "string",
        "value": "any|null"
      }
    ],
    "duration_seconds": "float"
  },
  "lineage": {
    "source": "string",
    "symbol": "string",
    "partition_key": "string",
    "status": "string (VALID|INVALID)",
    "row_count": "int",
    "checksum": "string"
  }
}
```

### Error Modes

| Error | Behavior |
|-------|----------|
| `file_not_found` | Return error message |
| `extraction_failed` | Return error message (zip corrupt, etc.) |
| `contract_not_found` | Return error message (no contract for source+market+data_type) |
| `validation_failed` | Return validation errors; mark as INVALID in lineage |

### Success Criteria

- Data extracted and parsed successfully
- All validators passed
- Lineage record created with status VALID or INVALID

### Test Cases

```python
# Happy path: validate valid data
test_validate_contract_valid():
    result = await validate_contract(
        symbol="BTCUSDT",
        source="binance",
        market_type="spot",
        data_type="klines",
        partition_key="2026-01-01",
        archive_home="/tmp/archive"
    )
    assert result["success"] is True
    assert result["validation"]["passed"] is True
    assert result["lineage"]["status"] == "VALID"

# Error case: invalid data
test_validate_contract_invalid():
    result = await validate_contract(
        symbol="BTCUSDT",
        partition_key="2026-01-01",
        ...
    )
    assert result["success"] is True  # Validation ran
    assert result["validation"]["passed"] is False
    assert result["validation"]["error_count"] > 0
    assert result["lineage"]["status"] == "INVALID"
```

---

## 6. Subagent Protocol

Subagents are stateful actors coordinating multi-step workflows. They differ from skills in that they:

- Maintain state across steps (e.g., list → diff → download → verify)
- Return structured progress/status updates
- Can be orchestrated into DAGs by higher-level agents
- Support cancellation and pausing

### Subagent: `DownloadPartitionSubagent`

```python
class DownloadPartitionSubagent:
    """Coordinates symbol download pipeline for a partition."""

    async def run(self,
        symbols: list[str],
        source: str,
        market_type: str,
        data_type: str,
        partition_freq: str,
        partition_key: str,
        archive_home: str,
        dry_run: bool = False
    ) -> DownloadPartitionResult:
        """
        Phase 1: List files from source
        Phase 2: Scan local archive
        Phase 3: Compute diff
        Phase 4a (dry_run): return diff
        Phase 4b (dry_run=false): download files
        Phase 5: Record lineage
        → return aggregated result
        """
        ...

    def on_progress(self, callback: Callable[[ProgressEvent], None]) -> None:
        """Register callback for progress updates."""
        ...

    async def cancel(self) -> None:
        """Cancel in-flight downloads."""
        ...
```

---

## 7. Skill Tests

All skills must have unit tests with fakes and integration tests with real I/O.

### Test Structure

```
skills/
├── discover-symbols/
│   ├── SKILL.md                           # Skill spec (this doc)
│   ├── discover_symbols.py                # Implementation
│   └── test_discover_symbols.py
│       ├── class TestDiscoverSymbolsUnit: # Fakes, no I/O
│       ├── class TestDiscoverSymbolsIntegration: # @pytest.mark.integration
│       └── class TestDiscoverSymbolsErrors: # Error cases
```

### Test Template

```python
"""Tests for discover-symbols skill."""

import pytest
from unittest.mock import AsyncMock

from binance_datatool.skills.discover_symbols import discover_symbols


class TestDiscoverSymbolsUnit:
    """Unit tests with fakes (no I/O)."""

    @pytest.fixture
    def fake_adapter(self):
        """Fake adapter for testing."""
        adapter = AsyncMock()
        adapter.list_symbols = AsyncMock(
            return_value=["BTCUSDT", "ETHUSDT", "BNBUSDT", ...]
        )
        return adapter

    async def test_discover_symbols_happy_path(self, fake_adapter):
        """All symbols returned when no filters applied."""
        result = await discover_symbols(
            source="binance",
            market_type="spot",
            adapter=fake_adapter  # Injected fake
        )
        assert result["success"] is True
        assert len(result["symbols"]) > 0


@pytest.mark.integration
class TestDiscoverSymbolsIntegration:
    """Integration tests with real I/O."""

    async def test_discover_symbols_real_binance_api(self):
        """Call real Binance API (slow, may be skipped in CI)."""
        result = await discover_symbols(
            source="binance",
            market_type="spot"
            # No fake adapter; uses real HTTP
        )
        assert result["success"] is True
        assert "BTCUSDT" in result["symbols"]
```

---

## 8. Summary Table

| Skill | Input | Output | Error Handling | Priority |
|-------|-------|--------|----------------|----------|
| **discover-symbols** | source, market, data_type, filters | symbols[], counts | Resilient per source | HIGH |
| **list-files** | symbols[], source, market, data_type | files per symbol | Partial success | HIGH |
| **download-partition** | symbols[], partition_key, archive_home | download summary | Retry + resume | HIGH |
| **verify-partition** | symbols[], archive_home | verify summary | Skip verified | HIGH |
| **validate-contract** | symbol, partition_key, contract | validation result | Lineage + errors | MEDIUM |

---

## 9. Skill Development Workflow (TDD)

1. **Write Spec** (this doc)
   - Define inputs, outputs, error modes
   - Write test cases

2. **Write Tests** (failing)
   - Unit tests with fakes
   - Integration tests (marked @pytest.mark.integration)
   - Error case tests

3. **Implement Skill** (minimal)
   - Make tests pass
   - Ensure error handling matches spec

4. **Refine & Document**
   - Add docstrings
   - Update this spec with any changes
   - Add examples to SKILL.md

5. **Code Review**
   - Verify against audit checklist (docs/specs-driven-development.md)
   - Ensure SOLID principles followed
   - Verify tests cover happy path + errors

---

**Document Version**: 1.0
**Last Updated**: 2026-05-07
**Status**: Skills specification in progress; implementation to follow
