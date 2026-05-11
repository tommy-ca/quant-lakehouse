# Implementation Guide

This document guides developers through implementing the specifications and building out binance-datatool into a scalable, multi-source data pipeline framework.

---

## Phase Overview

### ✅ Phase 1: Foundation & Specification (COMPLETE)
- [x] Architecture overview and layering patterns
- [x] Formal requirements document (requirements.md)
- [x] Detailed data flows and diagrams (data-flows.md)
- [x] DataContract implementation with validation
- [x] DataContract unit tests (24 tests, all passing)
- [x] Skills & subagents specification (skills-subagents.md)

### ✅ Phase 2: Adapter Pattern & Multi-Source (COMPLETE — Binance only)
- [x] Implement LineageTracker for data provenance
- [x] Implement BinanceAdapter wrapping ArchiveClient
- [ ] CoinbaseAdapter (skeleton) — not implemented, deferred
- [x] Write adapter integration tests (25 tests in test_adapter_binance.py)
- [ ] Integrate SourceRegistry with CLI (--source flag not yet wired)

### ✅ Phase 3: Data Contracts & Validation (COMPLETE)
- [x] Implement DataContract class
- [x] Add schema validation to VerifyTask
- [x] Create data contract fixtures for tests
- [x] DataContract unit tests (24 tests in test_datacontract.py)

### ✅ Phase 4: Lineage & Observability (COMPLETE)
- [x] Implement LineageTracker
- [x] Implement MetricsCollector
- [x] Emit operation logs and metrics
- [ ] Add Prometheus endpoints (optional, not prioritized)

### ✅ Phase 5: Exchange Clients (COMPLETE)
- [x] Implement market-type-specific Binance clients (Spot/UM/CM)
- [x] Add ExchangeClient protocol with @runtime_checkable
- [x] Implement REST and WebSocket clients for each market type
- [x] Add optional CCXT integration (ccxt_rest.py, ccxt_pro.py)
- [x] Add exchange client tests (20 tests in test_exchange.py)

### ⏳ Phase 6: Skills Implementation (NEXT)
- [ ] Implement discover-symbols skill
- [ ] Implement list-files skill
- [ ] Implement download-partition skill
- [ ] Implement verify-partition skill
- [ ] Implement validate-contract skill

### ⏳ Phase 7: Multi-Source Expansion (FUTURE)
- [ ] Complete CoinbaseAdapter implementation
- [ ] Implement KrakenAdapter
- [ ] Add integration tests for each source
- [ ] Document configuration per source

### ⏳ Phase 8: Documentation & DevEx (FUTURE)
- [ ] Add Mermaid diagrams to docs
- [ ] Update AGENTS.md with development guidance
- [ ] Create contributor onboarding guide

---

## Phase 2: Adapter Pattern & Multi-Source

### Step 1: Implement LineageTracker

**File**: `src/binance_datatool/lineage.py`

**Purpose**: Record data provenance for audit, debugging, and compliance.

**Key Classes**:
- `LineageEvent`: Single provenance record (source, symbol, partition, status, etc.)
- `LineageTracker`: Accumulate and query events
- `LineageStore`: Persist events (file, DB)

**API Design**:
```python
class LineageTracker:
    """Track data provenance through pipeline."""

    def record_discovery(self, source: str, symbol_count: int, filters: dict) -> None:
        """Record symbol discovery."""
        ...

    def record_download(self, source: str, symbol: str, partition: str,
                       file_count: int, status: str, errors: list[str]) -> None:
        """Record download completion."""
        ...

    def record_verification(self, symbol: str, partition: str,
                           verified: int, failed: int) -> None:
        """Record verification results."""
        ...

    def record_validation(self, source: str, symbol: str, partition: str,
                         contract_id: str, status: str, errors: list[str]) -> None:
        """Record data contract validation."""
        ...

    def to_dict(self) -> dict:
        """Serialize all events."""
        ...
```

**TDD Approach**:
1. Write tests for happy path (record + serialize)
2. Test querying (filter by source, symbol, date range)
3. Test error modes (missing fields, invalid status)
4. Implement minimal class
5. Run tests; all should pass

**Testing**:
```python
def test_record_download():
    tracker = LineageTracker()
    tracker.record_download(
        source="binance",
        symbol="BTCUSDT",
        partition="2026-01-01",
        file_count=100,
        status="SUCCESS",
        errors=[]
    )
    events = tracker.to_dict()
    assert len(events) == 1
    assert events[0]["status"] == "SUCCESS"

def test_query_by_symbol():
    tracker = LineageTracker()
    tracker.record_download(..., symbol="BTCUSDT", ...)
    tracker.record_download(..., symbol="ETHUSDT", ...)

    btc_events = tracker.query(symbol="BTCUSDT")
    assert len(btc_events) == 1
```

---

### Step 2: Implement BinanceAdapter

**File**: `src/binance_datatool/adapter/binance.py`

**Purpose**: Wrap existing `ArchiveClient` to implement `DataSourceAdapter` protocol.

**Key Points**:
- Adapter is a thin wrapper, not a refactor of ArchiveClient
- Implement required protocol methods: `list_symbols`, `list_files`, `fetch_file`, `parse_symbol`, `get_metadata`
- Use dependency injection: `BinanceAdapter(client=ArchiveClient())`
- All existing behavior preserved (tests still pass)

**API Design**:
```python
from binance_datatool.adapter import DataSourceAdapter

class BinanceAdapter(DataSourceAdapter):
    """Adapter for Binance public archive."""

    source: str = "binance"

    def __init__(self, client: ArchiveClient):
        self.client = client

    async def list_symbols(
        self,
        market_type: str,
        partition: str,
        data_type: str
    ) -> list[str]:
        """Delegate to ArchiveClient."""
        return await self.client.list_symbols(...)

    async def list_files(
        self,
        symbol: str,
        market_type: str,
        partition: str,
        data_type: str,
        interval: str | None = None
    ) -> list[FileMetadata]:
        """Delegate to ArchiveClient."""
        return await self.client.list_files(...)

    async def fetch_file(
        self,
        symbol: str,
        file_key: str,
        output_path: str
    ) -> FileResult:
        """Download a file."""
        # Use existing downloader; return FileResult(success, bytes, checksum)
        ...

    def parse_symbol(self, symbol_str: str) -> SymbolMetadata:
        """Parse 'BTCUSDT' → SymbolMetadata(base='BTC', quote='USDT', ...)"""
        base, quote = infer_spot_info(symbol_str)
        return SymbolMetadata(
            symbol=symbol_str,
            base=base,
            quote=quote,
            source=DataSource.BINANCE,
            market_type=MarketType.SPOT  # Simplified; real code infers
        )

    async def get_metadata(self) -> dict:
        """Return info about this source."""
        return {
            "rate_limit": "1200 requests/minute",
            "available_markets": ["spot", "um", "cm"],
            "available_data_types": ["klines", "trades", "aggTrades", ...],
        }
```

**TDD Approach**:
1. Write tests that verify adapter protocol
2. Test delegation to ArchiveClient (with fakes)
3. Test symbol parsing (edge cases)
4. Implement wrapper methods
5. Run tests; all should pass

**Testing**:
```python
@pytest.fixture
def fake_archive_client():
    client = AsyncMock()
    client.list_symbols = AsyncMock(return_value=["BTCUSDT", "ETHUSDT"])
    return client

async def test_binance_adapter_list_symbols(fake_archive_client):
    adapter = BinanceAdapter(client=fake_archive_client)
    symbols = await adapter.list_symbols("spot", "daily", "klines")
    assert symbols == ["BTCUSDT", "ETHUSDT"]
    fake_archive_client.list_symbols.assert_called_once()

def test_binance_adapter_parse_symbol():
    adapter = BinanceAdapter(client=None)  # Don't need client for parsing
    metadata = adapter.parse_symbol("BTCUSDT")
    assert metadata.base == "BTC"
    assert metadata.quote == "USDT"
    assert metadata.source == DataSource.BINANCE
```

---

### Step 3: Create CoinbaseAdapter (Skeleton)

**File**: `src/binance_datatool/adapter/coinbase.py`

**Purpose**: Show multi-source extensibility; demonstrate how to add a new exchange.

**Key Points**:
- Use real HTTP client (httpx or aiohttp) to call Coinbase REST API
- Implement same protocol as BinanceAdapter
- Can stub with NotImplementedError for now
- Will be fleshed out in Phase 5

**Skeleton**:
```python
class CoinbaseAdapter(DataSourceAdapter):
    """Adapter for Coinbase API."""

    source: str = "coinbase"

    def __init__(self, api_key: str | None = None, base_url: str = "https://api.exchange.coinbase.com"):
        self.api_key = api_key
        self.base_url = base_url

    async def list_symbols(self, market_type: str, partition: str, data_type: str) -> list[str]:
        """Fetch from GET /products; filter by product_id pattern."""
        raise NotImplementedError("Coinbase adapter in progress")

    async def list_files(self, ...) -> list[FileMetadata]:
        raise NotImplementedError("Coinbase adapter in progress")

    async def fetch_file(self, ...) -> FileResult:
        raise NotImplementedError("Coinbase adapter in progress")

    def parse_symbol(self, symbol_str: str) -> SymbolMetadata:
        """Parse 'BTC-USD' → SymbolMetadata(base='BTC', quote='USD', ...)"""
        raise NotImplementedError("Coinbase adapter in progress")

    async def get_metadata(self) -> dict:
        return {
            "rate_limit": "15 requests/second",
            "available_markets": ["spot"],
            "available_data_types": ["trades"],  # Coinbase has different data model
        }
```

---

### Step 4: Write Adapter Integration Tests

**File**: `tests/test_binance_adapter.py` (and similar for Coinbase)

**Purpose**: Verify adapter correctly implements protocol; test with real and fake clients.

**Structure**:
```python
# Unit tests: with fakes
class TestBinanceAdapterUnit:
    async def test_list_symbols_delegates_to_client(self, fake_client):
        ...

    def test_parse_symbol_handles_spot_pairs(self):
        ...

    def test_parse_symbol_handles_futures_contracts(self):
        ...

# Integration tests: with real S3 (slow, optional)
@pytest.mark.integration
class TestBinanceAdapterIntegration:
    async def test_real_binance_s3_list_symbols(self):
        adapter = BinanceAdapter(client=ArchiveClient())
        symbols = await adapter.list_symbols("spot", "daily", "klines")
        assert len(symbols) > 100
        assert "BTCUSDT" in symbols
```

---

### Step 5: Integrate SourceRegistry with CLI

**File**: `src/binance_datatool/cli/archive.py` (modify)

**Changes**:
- Add `--source` flag to commands (default: "binance" for backward compatibility)
- Resolve adapter from registry: `adapter = registry.get(source)`
- Pass adapter to workflows instead of hardcoded ArchiveClient

**Example**:
```python
@app.command()
async def list_symbols(
    trade_type: str = typer.Option(...),
    source: str = typer.Option("binance", help="Data source (binance, coinbase, kraken)"),
    # ... existing options ...
):
    """List symbols from a data source."""
    registry = SourceRegistry()
    adapter = registry.get(source)
    if not adapter:
        typer.echo(f"Unknown source: {source}", err=True)
        raise typer.Exit(1)

    # Construct workflow with adapter
    workflow = ArchiveListSymbolsWorkflow(adapter=adapter, filter=...)
    result = await workflow.run()
    # ... existing output logic ...
```

**Backward Compatibility**:
- Default source is "binance" (existing behavior preserved)
- Existing commands work without `--source` flag
- New code path only taken if `--source` is specified

---

## Phase 3: Skills Implementation

### Overview

Skills are thin, composable functions that wrap workflows and handle I/O formatting. They're the public API for agents and scripts.

### Skill: discover-symbols (Implementation Example)

**File**: `src/binance_datatool/skills/discover_symbols.py`

```python
"""Skill: discover-symbols

Lists all trading symbols from a data source with optional filters.
"""

import asyncio
from binance_datatool.adapter import SourceRegistry
from binance_datatool.common.filter import build_symbol_filter

async def discover_symbols(
    source: str,
    market_type: str,
    data_type: str,
    partition_freq: str = "daily",
    quote_asset: str | None = None,
    exclude_leverage: bool = False,
    exclude_stables: bool = False,
    timeout_seconds: int = 30,
) -> dict:
    """Discover trading symbols with filters.

    Args:
        source: Data source (binance, coinbase, kraken).
        market_type: Market segment (spot, um, cm, options).
        data_type: Dataset type (klines, trades, etc.).
        partition_freq: Temporal partitioning (daily, monthly, hourly).
        quote_asset: Optional filter by quote asset (USDT, USD, etc.).
        exclude_leverage: Skip leverage symbols (2x, 3x, 5x, etc.).
        exclude_stables: Skip stablecoin pairs (USDTUSD, etc.).
        timeout_seconds: Network timeout.

    Returns:
        {
            "success": bool,
            "symbols": list[str],
            "filtered_out": list[str],
            "errors": list[str],
            "counts": {"total_found": int, "matched": int, "filtered": int},
            "duration_seconds": float
        }
    """
    import time
    start_time = time.time()

    try:
        # Get adapter
        registry = SourceRegistry()
        adapter = registry.get(source)
        if not adapter:
            return {
                "success": False,
                "symbols": [],
                "filtered_out": [],
                "errors": [f"Unknown source: {source}"],
                "counts": {"total_found": 0, "matched": 0, "filtered": 0},
                "duration_seconds": time.time() - start_time,
            }

        # List symbols
        all_symbols = await asyncio.wait_for(
            adapter.list_symbols(market_type, partition_freq, data_type),
            timeout=timeout_seconds
        )

        # Parse and filter
        filter_obj = build_symbol_filter(
            market_type=market_type,
            quote_assets=frozenset([quote_asset]) if quote_asset else None,
            exclude_leverage=exclude_leverage,
            exclude_stables=exclude_stables
        )

        matched = []
        filtered_out = []

        for symbol in all_symbols:
            try:
                info = adapter.parse_symbol(symbol)
                if filter_obj.matches(info):
                    matched.append(symbol)
                else:
                    filtered_out.append(symbol)
            except Exception as e:
                # Parsing error; skip this symbol
                pass

        return {
            "success": True,
            "symbols": sorted(matched),
            "filtered_out": sorted(filtered_out),
            "errors": [],
            "counts": {
                "total_found": len(all_symbols),
                "matched": len(matched),
                "filtered": len(filtered_out),
            },
            "duration_seconds": time.time() - start_time,
        }

    except asyncio.TimeoutError:
        return {
            "success": False,
            "symbols": [],
            "filtered_out": [],
            "errors": ["Request timeout"],
            "counts": {"total_found": 0, "matched": 0, "filtered": 0},
            "duration_seconds": time.time() - start_time,
        }

    except Exception as e:
        return {
            "success": False,
            "symbols": [],
            "filtered_out": [],
            "errors": [str(e)],
            "counts": {"total_found": 0, "matched": 0, "filtered": 0},
            "duration_seconds": time.time() - start_time,
        }
```

**Unit Tests** (`tests/test_discover_symbols.py`):
```python
@pytest.fixture
def fake_adapter():
    adapter = AsyncMock()
    adapter.list_symbols = AsyncMock(return_value=["BTCUSDT", "ETHUSDT", "USDTUSD"])
    adapter.parse_symbol = Mock(side_effect=lambda s: SymbolMetadata(
        symbol=s,
        base=s[:-4],
        quote=s[-4:],
        source=DataSource.BINANCE,
        ...
    ))
    return adapter

@pytest.fixture
def fake_registry(fake_adapter):
    registry = Mock()
    registry.get = Mock(return_value=fake_adapter)
    return registry

async def test_discover_symbols_happy_path(fake_adapter, fake_registry, monkeypatch):
    # Inject fake registry
    monkeypatch.setattr("binance_datatool.skills.discover_symbols.SourceRegistry", lambda: fake_registry)

    result = await discover_symbols(
        source="binance",
        market_type="spot",
        data_type="klines",
        quote_asset="USDT"
    )

    assert result["success"] is True
    assert "BTCUSDT" in result["symbols"]
    assert "USDTUSD" not in result["symbols"]  # Filtered
```

---

## Implementation Checklist (Phase 2 & 3)

### Phase 2: Adapter Pattern

- [ ] **LineageTracker** (lineage.py + test_lineage.py)
  - [ ] Record methods for discovery, download, verification, validation
  - [ ] Query and serialization
  - [ ] Unit tests (happy path, errors, serialization)

- [ ] **BinanceAdapter** (adapter/binance.py + test_binance_adapter.py)
  - [ ] Implement protocol methods
  - [ ] Wrap ArchiveClient
  - [ ] Parse symbol metadata
  - [ ] Unit tests (with fakes)
  - [ ] Integration tests (with real S3, if available)

- [ ] **CoinbaseAdapter** (adapter/coinbase.py + test_coinbase_adapter.py)
  - [ ] Create skeleton with NotImplementedError
  - [ ] Document API structure (for future implementers)

- [ ] **CLI Integration** (cli/archive.py)
  - [ ] Add `--source` flag to commands
  - [ ] Integrate SourceRegistry
  - [ ] Test backward compatibility (no `--source` = "binance")

### Phase 3: Skills

For each skill, repeat this cycle:

1. [ ] Write failing unit tests (with fakes)
2. [ ] Implement minimal skill function
3. [ ] Run unit tests; should pass
4. [ ] Add integration test (if I/O involved)
5. [ ] Write docstring with spec
6. [ ] Code review (audit checklist)

Skills to implement:
- [ ] `discover-symbols` (discover_symbols.py + test_discover_symbols.py)
- [ ] `list-files` (list_files.py + test_list_files.py)
- [ ] `download-partition` (download_partition.py + test_download_partition.py)
- [ ] `verify-partition` (verify_partition.py + test_verify_partition.py)
- [ ] `validate-contract` (validate_contract.py + test_validate_contract.py)

---

## Code Review Gates (Before Merging)

Every PR must pass these checks:

1. **Specification**: Linked in PR description; spec template followed
2. **Tests**: TDD (tests written first); coverage of happy path + error cases
3. **SOLID**: Single responsibility; open/closed; Liskov; interface segregation; inversion of control
4. **KISS**: Solution is as simple as possible; no premature abstraction
5. **DRY**: No duplicated code or logic
6. **YAGNI**: No speculative features or "future-proofing"
7. **Linting**: `uv run ruff check .` passes
8. **Type Hints**: All public functions typed
9. **Docstrings**: Google-style docstrings for modules, classes, public functions
10. **No Secrets**: No hard-coded API keys, credentials, passwords

Use the audit checklist from `docs/specs-driven-development.md` for detailed guidance.

---

## Testing Commands

```bash
# Run all tests
uv run pytest

# Run specific test class
uv run pytest tests/test_datacontract.py::TestDataContractValidation -v

# Run with coverage
uv run pytest --cov=binance_datatool --cov-report=html

# Run only integration tests (slow)
uv run pytest -m integration

# Run specific test file
uv run pytest tests/test_discover_symbols.py -v

# Lint
uv run ruff check .

# Format
uv run ruff format .

# Type check (optional, with mypy)
# uv run mypy src/
```

---

## Documentation & Communication

As you implement, update:

1. **docs/requirements.md**: If requirements change
2. **docs/data-flows.md**: If data flow changes
3. **docs/skills-subagents.md**: As skills are implemented
4. **Docstrings**: Every public function/class
5. **README.md** (if user-facing): Examples, new flags, new commands

---

## Summary

This implementation guide provides a clear path to:
1. Solidify the adapter pattern (Phase 2)
2. Build composable skills (Phase 3)
3. Enable multi-source and agent-driven workflows (Phase 4+)

All work follows TDD, SOLID principles, and clear specifications. Success criteria are defined upfront. Code review gates ensure quality.

**Next Step**: See `docs/requirements.md` Phase 5 for multi-CEX expansion via CCXT.

---

**Document Version**: 1.1
**Last Updated**: 2026-05-11
**Status**: Phases 1–7 complete (core workflows, exchange SDK, Prefect orchestration, sink). Phase 8 (WS streaming) planned but not implemented.
