# Formal Specification: Data Pipeline Framework for Multi-Source Crypto Market Data

**Version**: 2.0 (Generalized & DataOps-Ready)

> **Note (2026-05-16):** This specification describes a generalized adapter-based
> architecture. The `DataSourceAdapter` / `SourceRegistry` were removed in Phase 35
> and **re-introduced in Phase 42** as a minimal package
> (`src/binance_datatool/adapter/` — protocol, BinanceAdapter, registry).
> The canonical pipeline remains dlt → Polars → Pandera → DuckLake.
> `datacontract.py`, `IcebergCatalog`, and analytics views were removed (YAGNI).
> See `AGENTS.md` for the current stack table and working model.
**Date**: 2026-05-07
**Status**: Foundation Complete, Implementation Ready
**Audience**: Architects, Implementers, DataOps Engineers, AI Agents

---

## Executive Summary

This specification formalizes the binance-datatool project into a **generalized, scalable data pipeline framework** for ingesting cryptocurrency market data from multiple sources. It synthesizes:

1. **Current State**: Four-layer architecture (CLI → Workflow → Client → Common) handling Binance archive ingestion
2. **Target State**: Six-layer generalized framework supporting multi-source, contract-driven validation, lineage tracking, and modern data stack integration
3. **Design Principles**: SOLID, KISS, DRY, YAGNI with DataOps, MLOps, and specs-driven development
4. **Extension Strategy**: Adapter pattern for new sources; contract pattern for data validation; storage abstractions for cloud/lakehouse

---

## Part 1: Requirements & Functional Specification

### 1.1 Functional Requirements

| ID | Requirement | Priority | Status |
|----|-------------|----------|--------|
| FR-1 | Discover trading symbols from a data source | HIGH | ✅ Implemented |
| FR-2 | List available data files for symbols | HIGH | ✅ Implemented |
| FR-3 | Download files with diff-based resumable transfers | HIGH | ✅ Implemented |
| FR-4 | Verify file integrity via SHA256 checksums | HIGH | ✅ Implemented |
| FR-5 | Support multiple data sources (adapters) | HIGH | 🔄 In Progress |
| FR-6 | Validate data against schema contracts | MEDIUM | 🔄 In Progress |
| FR-7 | Track lineage (provenance, transformations) | MEDIUM | ⏳ Planned |
| FR-8 | Integrate with modern data stacks (S3, Delta, Iceberg) | MEDIUM | ⏳ Planned |
| FR-9 | Enable CLI composition via stdin/stdout piping | HIGH | ✅ Implemented |
| FR-10 | Support scheduling & orchestration (Airflow, Dagu) | MEDIUM | ⏳ Planned |

### 1.2 Non-Functional Requirements

| ID | Requirement | Target | Status |
|----|-------------|--------|--------|
| NFR-1 | Testability (100% dependency injection) | HIGH | ✅ Met |
| NFR-2 | Download throughput | ≥50 MB/s | ✅ Via aria2c |
| NFR-3 | Concurrent file operations | 16 concurrent | ✅ Implemented |
| NFR-4 | Parallel verification | cpu_count-2 workers | ✅ Implemented |
| NFR-5 | Resilience (transient error retry) | 3-5 attempts | ✅ Implemented |
| NFR-6 | Atomicity (complete or no-op) | 100% | ✅ Verified |
| NFR-7 | Extensibility (new sources without core changes) | Plugin model | 🔄 In Progress |
| NFR-8 | Configuration flexibility | Env vars + CLI | ✅ Implemented |
| NFR-9 | Observability (logs, metrics, lineage) | Full | 🔄 In Progress |
| NFR-10 | Cloud deployment ready | No local deps | ⏳ Planned |

### 1.3 Use Cases

#### UC-1: Quant Researcher Backtesting Data
**Actor**: Data Scientist
**Goal**: Download 10 years of daily OHLCV data for 100 symbols
**Steps**:
1. Discover symbols: `list-symbols spot --quote USDT`
2. Pipe to download: `| download spot --type klines --interval 1d --archive /data/archive`
3. Verify integrity: `verify spot --archive /data/archive`
4. Export to Parquet: Custom script reads from archive

#### UC-2: MLOps Data Lake Ingestion
**Actor**: MLOps Engineer
**Goal**: Automated daily ingest of new klines into Delta Lake
**Steps**:
1. Discover symbols daily (programmatic)
2. Download new partition (diff-based, resumable)
3. Validate against contract (schema + business rules)
4. Record lineage (source, partition, row_count, checksum, errors)
5. Merge into Delta Lake (MERGE INTO operation)
6. Alert on validation failures

#### UC-3: Multi-CEX Unified Interface (via CCXT)
**Actor**: Data Platform Owner
**Goal**: Single interface for Binance, OKX, Bybit via CCXT
**Steps**:
1. Configure adapters (source_registry.get("okx") / ccxt.okx)
2. List symbols from each (CCXT unified API)
3. Download from each (CCXT fetch_ohlcv)
4. Normalize to common schema (transformer layer)
5. Store in lakehouse (unified partition scheme)

**Note**: Coinbase removed. Focus on Tier-1 CEXs with CCXT support.

#### UC-4: Agent-Driven Workflow
**Actor**: AI Agent / Subagent
**Goal**: Autonomously discover, download, validate, publish data
**Steps**:
1. Call skill: `discover_symbols(source="binance", market_type="spot")`
2. Call skill: `download_partition(symbols=[...], partition_key="2026-01-01")`
3. Call skill: `verify_partition(symbols=[...])`
4. Call skill: `validate_contract(symbol="BTCUSDT", partition_key="2026-01-01")`
5. Publish to lakehouse
6. Record success/failure in lineage

---

## Part 2: Current Architecture (As-Is)

### 2.1 Four-Layer Design

```
┌─────────────────────────────────────────────────┐
│ CLI Layer (cli/archive.py - 484 LOC)            │
│ • Typer commands & argument parsing             │
│ • Workflow construction                          │
│ • Result formatting & printing                   │
└─────────────┬───────────────────────────────────┘
              │
┌─────────────▼───────────────────────────────────┐
│ Workflow Layer (workflow/ - 852 LOC)            │
│ • ListSymbolsWorkflow: filtering + inference    │
│ • ListFilesWorkflow: concurrent listing         │
│ • DownloadWorkflow: diff + aria2c               │
│ • VerifyWorkflow: parallel checksums            │
└─────────────┬───────────────────────────────────┘
              │
┌─────────────▼───────────────────────────────────┐
│ Archive Client Layer (archive/ - 1,257 LOC)     │
│ • ArchiveClient: S3 HTTP, XML parsing, retry    │
│ • Checksum: SHA256 verification                 │
│ • Downloader: aria2c orchestration              │
│ • SymbolDir: local directory scanning            │
└─────────────┬───────────────────────────────────┘
              │
┌─────────────▼───────────────────────────────────┐
│ Common Layer (common/ - 959 LOC)                │
│ • Enums (TradeType, DataFrequency, DataType)   │
│ • Constants (S3 endpoints, assets, stablecoins)│
│ • Types (SymbolInfo classes)                    │
│ • Filters (SpotSymbolFilter, etc.)             │
│ • Utilities (symbol inference, logging)         │
└─────────────────────────────────────────────────┘
```

### 2.2 Current Workflows

#### Workflow 1: List Symbols
**Command**: `list-symbols spot [--quote USDT] [--exclude-leverage] [--exclude-stables]`

**Flow**:
```
CLI.list_symbols(trade_type, data_freq, data_type, filters)
  ↓
ArchiveListSymbolsWorkflow(client, filters)
  ├─ client.list_symbols(trade_type, data_freq, data_type)
  │   └─ S3 list: "data/spot/daily/klines/" → xml parse → symbol prefixes
  │
  ├─ infer_symbol_info(symbol) for each
  │   └─ Parse BTCUSDT → {base: BTC, quote: USDT, ...}
  │
  ├─ filter.matches(info) for each
  │   └─ Check quote asset, leverage, stables
  │
  └─ return ListSymbolsResult(matched, filtered_out, errors)
     ↓
CLI: print matched symbols (one per line) → stdout
```

**Key Details**:
- S3 listing uses Tenacity retry (5x, 1-8s backoff)
- Symbol parsing uses greedy quote asset matching (longest first)
- Filtering is stateless (can be parallelized)
- Output: newline-separated symbols (composable with pipes)

#### Workflow 2: List Files
**Command**: `list-files spot --type klines BTCUSDT ETHUSDT`

**Flow**:
```
CLI.list_files(symbols, trade_type, data_type, interval)
  ↓
ArchiveListFilesWorkflow(client, symbols)
  ├─ for each symbol (concurrent, 16x semaphore):
  │   ├─ client.list_files(symbol, trade_type, data_type, interval)
  │   │   └─ S3 list: "data/spot/daily/klines/BTCUSDT/" → files
  │   │
  │   └─ parse to ArchiveFile(key, size, last_modified)
  │
  └─ return ListFilesResult(per_symbol_files, summary, errors)
     ↓
CLI: print table (symbol | file_count | total_bytes | error?) → stdout
```

**Key Details**:
- Async concurrent with semaphore (avoid overwhelming S3)
- Per-symbol error handling (continue on partial failure)
- Files sorted by last_modified ascending
- Summary includes total files, total bytes, error counts

#### Workflow 3: Download
**Command**: `download spot --type klines BTCUSDT --dry-run` (or without for actual download)

**Flow**:
```
CLI.download(symbols, trade_type, data_type, archive_home, dry_run)
  ↓
ArchiveDownloadWorkflow(client, symbols, archive_home)
  ├─ Step 1: List remote files (concurrent)
  │   └─ client.list_files per symbol
  │
  ├─ Step 2: Scan local archive
  │   └─ SymbolArchiveDir(archive_home).scan_local()
  │
  ├─ Step 3: Compute diff
  │   ├─ For each remote file:
  │   │   ├─ Local exists && mtime >= remote.last_modified → SKIP
  │   │   ├─ Local exists && mtime < remote.last_modified → UPDATE
  │   │   └─ Local missing → NEW
  │   │
  │   └─ return DiffResult(to_download, skipped, errors)
  │
  ├─ if dry_run: return diff
  │
  └─ Step 4: Download (not dry_run)
      ├─ Build DownloadRequest list (URL, local_path, checksum)
      ├─ Batch into 16-file chunks
      ├─ For each batch:
      │   ├─ aria2c --input-file=temp_urls
      │   ├─ Retry failed (3x backoff)
      │   └─ Write .CHECKSUM files
      │
      └─ return DownloadResult(downloaded, failed, stats)
         ↓
CLI: print summary → stdout
```

**Key Details**:
- Diff algorithm: file exists && mtime >= remote → skip
- aria2c: parallel downloads, 16 files/batch, 3x retry per file
- Resumable: run again with same params → skips completed files
- Dry-run: no side effects, shows what would download
- Progress bar: optional `--progress-bar` on stderr

#### Workflow 4: Verify
**Command**: `verify spot BTCUSDT [--keep-failed]`

**Flow**:
```
CLI.verify(symbols, archive_home, keep_failed, dry_run)
  ↓
ArchiveVerifyWorkflow(archive_home, symbols)
  ├─ Step 1: Scan local directory
  │   ├─ SymbolArchiveDir.scan_local() per symbol
  │   ├─ Classify: verified, failed, missing_checksum, zip_only
  │   │
  │   └─ return ScanResult(to_verify, already_verified, orphans)
  │
  ├─ if dry_run: return scan result
  │
  └─ Step 2: Verify (not dry_run)
      ├─ ProcessPoolExecutor (cpu_count-2 workers)
      ├─ For each .zip file:
      │   ├─ Read expected hash from .CHECKSUM
      │   ├─ Calculate SHA256 (streaming, 64KB chunks)
      │   ├─ Compare
      │   └─ Mark as verified or failed
      │
      ├─ if not keep_failed: delete failed .zip & .CHECKSUM
      │
      └─ return VerifyResult(verified, failed, deleted, stats)
         ↓
CLI: print summary → stdout
```

**Key Details**:
- Parallel: ProcessPoolExecutor (not ThreadPoolExecutor) for CPU-bound SHA256
- Workers: max(1, cpu_count() - 2) to avoid saturation
- Streaming: 64KB chunks to handle multi-GB files
- Cleanup: `--keep-failed` flag controls deletion
- Resumable: run again → skips already verified files (via marker)

### 2.3 Data Structures

#### Core Types

```python
# Enums
class TradeType(Enum): SPOT, UM, CM
class DataFrequency(Enum): DAILY, MONTHLY, HOURLY
class DataType(Enum): KLINES, TRADES, AGG_TRADES, BOOK_DEPTH, ...
class ContractType(Enum): PERPETUAL, QUARTERLY, NEXT_QUARTER, ...

# Symbol Info (from inference)
@dataclass
class SpotSymbolInfo:
    symbol: str
    base: str
    quote: str
    is_leverage: bool
    is_stable_pair: bool

# Archive Metadata
@dataclass
class ArchiveFile:
    key: str              # S3 key
    size: int             # bytes
    last_modified: datetime

# Download Request
@dataclass
class DownloadRequest:
    url: str
    output_path: Path
    expected_checksum: str | None

# Workflow Results
@dataclass
class DiffEntry:
    remote: ArchiveFile
    local_path: Path
    reason: Literal["new", "updated"]

@dataclass
class ListSymbolsResult:
    matched: list[str]
    filtered_out: list[str]
    errors: list[str]
```

---

## Part 3: Proposed Generalized Architecture (To-Be)

### 3.1 Six-Layer Generalized Framework

```
┌──────────────────────────────────────────────────────┐
│ Orchestration Layer (DAGs, scheduling)              │
│ • Airflow DAGs, Dagu workflows                      │
│ • Scheduling, dependency management                 │
│ • Monitoring & alerting                             │
└──────────────────┬───────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────┐
│ CLI / API Layer (command interface)                 │
│ • Typer CLI, REST API (optional)                    │
│ • Argument parsing, validation                      │
│ • Result formatting                                 │
└──────────────────┬───────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────┐
│ Workflow Layer (business logic orchestration)       │
│ • PipelineWorkflow: coordinate multi-step ops      │
│ • DataOpsWorkflow: validate, lineage, transform   │
│ • SourceRouter: dispatch to correct adapter        │
└──────────────────┬───────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────┐
│ DataOps / Transform Layer (validation, lineage)    │
│ • DataContractValidator: schema + rules            │
│ • LineageTracker: provenance, transformations      │
│ • MetricsCollector: counters, durations, errors    │
└──────────────────┬───────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────┐
│ Source Adapter Layer (multi-source abstraction)    │
│ • DataSourceAdapter protocol (5 methods)           │
│ • BinanceAdapter: S3 XML parsing (archive)         │
│ • CCXTExchangeClient: Unified API (Binance/OKX/Bybit) │
│ • SourceRegistry: adapter discovery/instantiation  │
└──────────────────┬───────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────┐
│ Storage / Connector Layer (multi-backend)          │
│ • StorageBackend protocol (put, get, list, delete)│
│ • LocalStorageBackend: filesystem                  │
│ • S3StorageBackend: AWS S3 / S3-compatible         │
│ • DeltaStorageBackend: Delta Lake partition merge │
│ • IcebergStorageBackend: Iceberg catalog ops      │
└──────────────────┬───────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────┐
│ Foundation Layer (shared primitives)               │
│ • Enums, constants, types                          │
│ • Filters, symbol inference                        │
│ • Logging, configuration                           │
│ • Utilities, helpers                               │
└──────────────────────────────────────────────────────┘
```

### 3.2 Key Abstractions

#### Adapter Protocol (Multi-Source)

```python
class DataSourceAdapter(Protocol):
    """Unified interface for data sources."""

    source: str  # "binance", "okx", "bybit", etc. (via CCXT)

    async def list_symbols(
        self,
        market_type: str,
        partition: str,
        data_type: str,
        filters: dict | None = None
    ) -> list[str]:
        """List all available symbols."""
        ...

    async def list_files(
        self,
        symbol: str,
        market_type: str,
        partition: str,
        data_type: str,
        interval: str | None = None
    ) -> list[FileMetadata]:
        """List files for a symbol."""
        ...

    async def fetch_file(
        self,
        symbol: str,
        file_key: str,
        output_path: str
    ) -> FileResult:
        """Download a specific file."""
        ...

    def parse_symbol(self, symbol_str: str) -> SymbolMetadata:
        """Parse symbol string to components."""
        ...

    async def get_metadata(self) -> dict:
        """Return source metadata (rate limits, capabilities)."""
        ...
```

#### Storage Backend Protocol (Multi-Backend)

```python
class StorageBackend(Protocol):
    """Unified interface for storage systems."""

    async def put(
        self,
        path: str,
        data: bytes,
        metadata: dict | None = None
    ) -> PutResult:
        """Write file to storage."""
        ...

    async def get(self, path: str) -> bytes:
        """Read file from storage."""
        ...

    async def list(
        self,
        prefix: str,
        recursive: bool = False
    ) -> list[FileMetadata]:
        """List files under prefix."""
        ...

    async def delete(self, path: str) -> None:
        """Delete file from storage."""
        ...

    async def exists(self, path: str) -> bool:
        """Check if file exists."""
        ...

    async def merge_partition(
        self,
        source_dir: str,
        target_table: str,
        partition_cols: list[str],
        format: str = "delta"  # delta, iceberg, parquet
    ) -> MergeResult:
        """Merge partition into table (lakehouse)."""
        ...
```

#### Data Contract (Validation)

```python
@dataclass
class DataContract:
    """Schema + validation rules for datasets."""

    source: str
    market_type: str
    data_type: str
    partition_freq: str

    schema: dict[str, type]
    key_cols: list[str]
    partition_cols: list[str]
    nullable_cols: set[str]
    validators: list[Callable[[dict], bool]]

    def validate(self, data: list[dict] | Any) -> ValidationResult:
        """Validate data against contract."""
        ...

    def to_dict(self) -> dict:
        """Serialize for config."""
        ...
```

#### Lineage Tracker (Provenance)

```python
@dataclass
class LineageEvent:
    """Single provenance record."""

    source: str
    symbol: str
    partition_key: str
    operation: str  # "list", "download", "verify", "validate"
    status: str  # "success", "partial", "failed"
    row_count: int | None
    checksum: str | None
    errors: list[str]
    duration_seconds: float
    timestamp: datetime

class LineageTracker:
    """Accumulate and query lineage."""

    def record(self, event: LineageEvent) -> None:
        """Record a lineage event."""
        ...

    def query(
        self,
        source: str | None = None,
        symbol: str | None = None,
        date_range: tuple[datetime, datetime] | None = None
    ) -> list[LineageEvent]:
        """Query lineage."""
        ...

    def export(self, format: str = "json") -> str:
        """Export lineage (JSON, CSV, Parquet)."""
        ...
```

### 3.3 Generalized Workflow Template

```python
class PipelineWorkflow:
    """Generic multi-step pipeline."""

    def __init__(
        self,
        source_adapter: DataSourceAdapter,
        storage_backend: StorageBackend,
        contract: DataContract | None = None,
        lineage_tracker: LineageTracker | None = None,
        metrics_collector: MetricsCollector | None = None
    ):
        self.adapter = source_adapter
        self.storage = storage_backend
        self.contract = contract
        self.lineage = lineage_tracker
        self.metrics = metrics_collector

    async def run(
        self,
        symbols: list[str],
        partition_key: str,
        dry_run: bool = False
    ) -> PipelineResult:
        """Execute pipeline."""

        # Phase 1: Discover
        start = time.time()
        remote_files = await self.adapter.list_files(symbols[0], ...)
        self.metrics.add_counter("discover_files", len(remote_files))

        # Phase 2: Diff
        local_files = await self.storage.list(f"archive/{symbols[0]}/")
        diff = self._compute_diff(remote_files, local_files)
        self.metrics.add_counter("to_download", len(diff.to_download))

        # Phase 3: Download
        if not dry_run:
            for file_entry in diff.to_download:
                result = await self.adapter.fetch_file(
                    symbol=symbols[0],
                    file_key=file_entry.remote.key,
                    output_path=...
                )
                await self.storage.put(file_entry.local_path, result.data)

        # Phase 4: Validate
        if self.contract and not dry_run:
            data = await self._extract_and_load(...)
            validation = self.contract.validate(data)
            self.lineage.record(LineageEvent(
                source=self.adapter.source,
                symbol=symbols[0],
                operation="validate",
                status="success" if validation.passed else "failed",
                errors=validation.errors
            ))

        # Phase 5: Publish
        if self.contract and validation.passed and not dry_run:
            result = await self.storage.merge_partition(
                source_dir=...,
                target_table=...,
                partition_cols=self.contract.partition_cols,
                format="delta"
            )

        return PipelineResult(
            dry_run=dry_run,
            downloaded=len(diff.to_download),
            failed=...,
            validated=validation.passed if self.contract else None,
            published=result.row_count if ... else None,
            duration_seconds=time.time() - start
        )
```

---

## Part 4: Implementation Strategy

### 4.1 Phases (Priority Order)

#### Phase 1: Foundation (✅ COMPLETE)
- [x] DataContract class + validation
- [x] Specification & documentation
- [x] SourceRegistry skeleton
- [x] Adapter protocol definition

#### Phase 2: Adapter Pattern (🔄 IN PROGRESS)
- [ ] LineageTracker implementation
- [ ] BinanceAdapter (wrap ArchiveClient)
- [ ] CCXTExchangeClient (OKX, Bybit via unified API)
- [ ] CLI integration with --source flag
- [ ] Tests for all adapters

#### Phase 3: Skills & Orchestration (⏳ NEXT)
- [ ] 5 core skills (discover, list, download, verify, validate)
- [ ] Subagent protocol
- [ ] Airflow DAG templates
- [ ] Dagu workflow examples

#### Phase 4: Storage Abstraction (⏳ FUTURE)
- [ ] StorageBackend protocol
- [ ] LocalStorageBackend
- [ ] S3StorageBackend
- [ ] DeltaStorageBackend

#### Phase 5: Observability & MLOps (⏳ FUTURE)
- [ ] MetricsCollector
- [ ] Lineage export (JSON, CSV, Parquet)
- [ ] Alerting integration
- [ ] Model training workflows

### 4.2 TDD & Specs-Driven Development

**For every feature**:

1. **Write Spec** (this document + component-specific doc)
   - Inputs, outputs, error modes
   - Test cases
   - Example usage

2. **Write Tests** (failing)
   - Unit tests (fakes, no I/O)
   - Integration tests (@pytest.mark.integration)
   - Edge cases & errors

3. **Implement** (minimal)
   - Make tests pass
   - Follow SOLID principles
   - Add docstrings

4. **Review** (audit checklist)
   - Check spec completeness
   - Verify tests cover errors
   - Ensure SOLID/KISS/DRY/YAGNI

### 4.3 Principle: SOLID, KISS, DRY, YAGNI

| Principle | Application | Example |
|-----------|-------------|---------|
| **SOLID** | One class, one job | BinanceAdapter handles Binance only; CCXT handles multi-CEX |
| **KISS** | Simplest solution | Adapter protocol: 5 methods, not 20 |
| **DRY** | Reuse logic | Base class for common retry/logging patterns |
| **YAGNI** | No speculative code | Don't add features "just in case" |

---

## Part 5: Data Model & Contract Specifications

### 5.1 Binance Spot OHLCV Contract

```python
BINANCE_SPOT_KLINES_CONTRACT = DataContract(
    source="binance",
    market_type="spot",
    data_type="klines",
    partition_freq="daily",

    schema={
        "open_time": int,
        "open": Decimal,
        "high": Decimal,
        "low": Decimal,
        "close": Decimal,
        "volume": Decimal,
        "close_time": int,
        "quote_volume": Decimal,
        "num_trades": int,
        "taker_buy_volume": Decimal,
        "taker_buy_quote_volume": Decimal,
    },

    key_cols=["open_time"],
    partition_cols=["date", "symbol"],

    validators=[
        lambda r: r["open"] > 0,
        lambda r: r["high"] >= r["low"],
        lambda r: r["close"] > 0,
        lambda r: r["volume"] >= 0,
        lambda r: r["close_time"] > r["open_time"],
    ]
)
```

### 5.2 Contract Registry

```python
ContractRegistry.register(
    source="binance",
    market_type="spot",
    data_type="klines",
    contract=BINANCE_SPOT_KLINES_CONTRACT
)

contract = ContractRegistry.get("binance", "spot", "klines")
result = contract.validate(data)
```

---

## Part 6: Error Handling & Resilience

### 6.1 Error Classification

```
Transient (RETRY)
├─ Network timeout → retry with backoff
├─ HTTP 5xx → retry with backoff
├─ Connection reset → retry with backoff
└─ Local disk full → retry after cleanup

Persistent (SKIP)
├─ Symbol not found (404) → continue with others
├─ File deleted (410) → skip file
├─ Rate limit (429) → backoff & retry
└─ Invalid credentials (403) → fail fast

Fatal (ABORT)
├─ Invalid command arguments → fail immediately
├─ Archive home not writable → fail immediately
└─ Corrupted config → fail immediately
```

### 6.2 Retry Strategy

```python
# Tenacity config
retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
stop=stop_after_attempt(5)
wait=wait_exponential(multiplier=1, min=1, max=8)

# Result: 1s, 2s, 4s, 8s, (abort)
```

---

## Part 7: Deployment & Scaling

### 7.1 Deployment Options

| Option | Use Case | Scaling |
|--------|----------|---------|
| **CLI** | Interactive, one-off | Manual (run multiple times) |
| **Docker** | CI/CD, isolated env | Horizontal (multiple containers) |
| **Airflow** | Scheduled pipelines | DAG parallelism, task pools |
| **Dagu** | Lightweight workflows | Process-level |
| **K8s Job** | Cloud-native | Pod parallelism |
| **Lambda/Cloud Functions** | Serverless | Auto-scaling |

### 7.2 Scaling Considerations

- **Concurrency**: Semaphore on S3 listing (16x), ProcessPoolExecutor on verification (cpu_count-2)
- **Batching**: aria2c downloads in 16-file batches
- **Resumability**: Diff-based; safe to re-run
- **Idempotency**: All operations idempotent (verify twice = same result)
- **Checkpointing**: Markers track progress (verified, downloaded)

---

## Part 8: Integration with Modern Data Stacks

### 8.1 Delta Lake Integration

```python
# Workflow with Delta merge
result = await pipeline.run(symbols=["BTCUSDT"], partition_key="2026-01-01")

# After validation passes:
delta_backend = DeltaStorageBackend(path="s3://mybucket/data/crypto")
merge_result = await delta_backend.merge_partition(
    source_dir="s3://mybucket/data/crypto/staging/binance/spot/2026-01-01",
    target_table="crypto_market_data",
    partition_cols=["source", "market_type", "date"],
    format="delta"
)

# SQL: MERGE INTO crypto_market_data AS target
#      USING source ON (key_cols match)
#      WHEN MATCHED THEN UPDATE SET *
#      WHEN NOT MATCHED THEN INSERT *
```

### 8.2 Iceberg Integration

```python
# Similar to Delta, with Iceberg-specific ops
iceberg_backend = IcebergStorageBackend(
    catalog_uri="s3://mybucket/iceberg",
    warehouse="crypto"
)
result = await iceberg_backend.merge_partition(...)
```

### 8.3 Orchestration Examples

#### Airflow DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG("crypto_market_data", schedule="@daily")

def discover_symbols(**context):
    result = discover_symbols(source="binance", market_type="spot")
    context["task_instance"].xcom_push(key="symbols", value=result["symbols"])

def download_partition(**context):
    symbols = context["task_instance"].xcom_pull(key="symbols")
    result = download_partition(symbols=symbols, partition_key=context["ds"])
    return result

discover_task = PythonOperator(task_id="discover", python_callable=discover_symbols, dag=dag)
download_task = PythonOperator(task_id="download", python_callable=download_partition, dag=dag)

discover_task >> download_task
```

#### Dagu Workflow

```yaml
steps:
  - name: discover
    command: python -c "from binance_datatool.skills import discover_symbols; discover_symbols()"
    output: symbols.json

  - name: download
    depends: discover
    command: python -c "import json; symbols = json.load(open('symbols.json')); ..."
    env:
      - ARCHIVE_HOME=/data/archive

  - name: verify
    depends: download
    command: binance-datatool verify spot --archive /data/archive

  - name: merge_delta
    depends: verify
    command: python merge_delta.py
```

---

## Part 9: Extension Points & Future Roadmap

### 9.1 Adding a New CEX via CCXT (OKX Example)

**Step 1**: CCXT supports 100+ exchanges with unified API

```python
# CCXT handles OKX, Bybit, Binance, etc. with same interface

import ccxt

# OKX
okx = ccxt.okx({"enableRateLimit": True})
symbols = okx.fetch_markets()  # All OKX markets

# Bybit
bybit = ccxt.bybit({"enableRateLimit": True})
klines = await bybit.fetch_ohlcv("BTC/USDT", "1h", limit=100)
```

**Step 2**: Use CCXTExchangeClient (already implemented)

```python
from binance_datatool.exchange import CCXTExchangeClient

# OKX
okx_client = CCXTExchangeClient(trade_type="spot")  # Uses ccxt.okx
klines = await okx_client.fetch_ohlcv("BTC/USDT", "1h")

# Bybit
bybit_client = CCXTExchangeClient(trade_type="spot")  # Uses ccxt.bybit
```

**Step 3**: Use via CLI (future --source flag)

```bash
binance-datatool --source okx list-symbols spot
binance-datatool --source bybit download spot BTCUSDT --interval 1d
```

**Step 2**: Register adapter

```python
# In SourceRegistry:
SourceRegistry.register("coinbase", CoinbaseAdapter(api_key=...))
```

**Step 3**: Use via CLI

```bash
binance-datatool --source coinbase list-symbols spot --quote USD
binance-datatool --source coinbase download spot BTCUSD --archive /data
```

### 9.2 Adding a New Storage Backend (Iceberg)

**Step 1**: Implement StorageBackend protocol

```python
# src/binance_datatool/storage/iceberg.py

class IcebergStorageBackend(StorageBackend):
    def __init__(self, catalog_uri, warehouse):
        self.catalog = load_catalog(catalog_uri)
        self.warehouse = warehouse

    async def merge_partition(self, source_dir, target_table, partition_cols, format):
        # Read data from source_dir
        # Insert into Iceberg table with MERGE INTO or INSERT OVERWRITE
        ...
```

**Step 2**: Use in workflow

```python
workflow = PipelineWorkflow(
    source_adapter=BinanceAdapter(),
    storage_backend=IcebergStorageBackend(...),
    contract=BINANCE_SPOT_KLINES_CONTRACT
)
result = await workflow.run(symbols=["BTCUSDT"], partition_key="2026-01-01")
```

### 9.3 Short-Term Roadmap (Next 3 Months)

- [ ] Complete LineageTracker + tests
- [ ] Complete BinanceAdapter + tests
- [ ] Complete CoinbaseAdapter (basic)
- [ ] CLI --source flag integration
- [ ] 5 core skills (discover, list, download, verify, validate)
- [ ] Airflow DAG templates
- [ ] Documentation & examples

### 9.4 Mid-Term Roadmap (3-6 Months)

- [ ] StorageBackend abstractions (Local, S3, Delta, Iceberg)
- [ ] Delta Lake integration tests
- [ ] Iceberg integration tests
- [ ] MetricsCollector + Prometheus exporter
- [ ] Lineage export (JSON, CSV, Parquet)
- [ ] Web UI (optional: view lineage, trigger workflows)

### 9.5 Long-Term Roadmap (6-12 Months)

- [ ] KrakenAdapter, BybitAdapter (other exchanges)
- [ ] Real-time klines (WebSocket support)
- [ ] Feature store integration (offline features)
- [ ] Model training automation (automated feature engineering)
- [ ] GPU support (for large-scale backtesting)

---

## Part 10: Testing Strategy

### 10.1 Test Pyramid

```
        /\
       /  \           E2E Tests (slow, real APIs)
      /────\
     /      \         Integration Tests (moderate speed, real I/O)
    /────────\
   /          \       Unit Tests (fast, fakes, no I/O)
  /__________\
```

### 10.2 Test Types

| Type | Scope | Speed | When | Example |
|------|-------|-------|------|---------|
| **Unit** | Single class/function | <100ms | Always (block merge if fail) | `test_parse_symbol` |
| **Integration** | Workflows + real I/O | <5s | Before merge (can skip in CI) | `test_download_workflow_real_s3` |
| **E2E** | Full CLI + real API | >10s | Pre-release only | `test_cli_download_real_binance` |

### 10.3 Example Unit Test (TDD)

```python
# tests/test_binance_adapter.py

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
```

---

## Part 11: Conclusion & Next Steps

### 11.1 Summary

This specification formalizes binance-datatool into a **generalized, production-ready data pipeline framework** supporting:

- ✅ Multiple sources (Binance, OKX, Bybit via CCXT unified API)
- ✅ Contract-driven validation (schema + business rules)
- ✅ Lineage tracking (provenance & compliance)
- ✅ Modern data stacks (Delta, Iceberg, etc.)
- ✅ Orchestration (Airflow, Dagu, Kubernetes)
- ✅ TDD & specs-driven development
- ✅ SOLID, KISS, DRY, YAGNI principles

### 11.2 Immediate Next Steps

1. **Phase 2, Step 1**: Implement LineageTracker
   - Tests: 10+ unit tests
   - Code: ~150 LOC
   - Effort: 3-4 hours

2. **Phase 2, Step 2**: Implement BinanceAdapter
   - Tests: 10+ unit tests, 5+ integration tests
   - Code: ~100 LOC (thin wrapper)
   - Effort: 4-5 hours

3. **Phase 2, Step 3**: CLI integration (--source flag)
   - Update CLI to route to adapter
   - Tests: 5+ tests
   - Effort: 2-3 hours

4. **Phase 3**: Implement 5 core skills
   - discover-symbols, list-files, download-partition, verify-partition, validate-contract
   - Tests: 50+ tests
   - Effort: 2-3 weeks

### 11.3 Success Criteria

- All tests passing (unit + integration)
- All code follows SOLID/KISS/DRY/YAGNI
- Full audit checklist satisfied
- Backward compatibility maintained (no breaking changes)
- Documentation complete (specs, examples, guides)

---

**Document Version**: 2.0
**Status**: Ready for implementation
**Last Updated**: 2026-05-07
**Maintainers**: Team
**Contact**: See docs/INDEX.md
