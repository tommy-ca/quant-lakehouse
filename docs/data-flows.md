# Data & Code Flows

> **Note (2026-05-14):** The adapter-based flow diagrams in this document
> reference the `adapter/` package and `SourceRegistry` which have been
> removed (Phase 35). For current data flows, see `architecture.md` and `AGENTS.md`.

This document provides detailed, step-by-step data and code flows for all major CLI commands and workflows. It includes ASCII sequence diagrams, state transitions, and integration points with the adapter layer.

---

## 1. Command: `list-symbols`

### 1.1 Sequence Diagram

```
User / CLI
  │
  ├─→ [CLI] parse args (trade_type, data_freq, data_type, filters)
  │
  ├─→ [CLI] construct ArchiveListSymbolsWorkflow(client, filter)
  │
  ├─→ [Workflow] call client.list_symbols(trade_type, data_freq, data_type)
  │         │
  │         ├─→ [Client] build S3 prefix: "data/spot/daily/klines/"
  │         ├─→ [Client] create async HTTP session (with retries)
  │         ├─→ [Client] list_dir(prefix) → paginated S3 listing (XML)
  │         ├─→ [Client] parse XML → extract symbol prefixes
  │         └─→ [Client] return sorted list[str]
  │
  ├─→ [Workflow] for each symbol:
  │         ├─→ infer_symbol_info(symbol) → SpotSymbolInfo | UmSymbolInfo | CmSymbolInfo
  │         ├─→ apply filter.matches(info) → True | False
  │         └─→ classify into [matched, filtered_out, unmatched]
  │
  ├─→ [Workflow] return ListSymbolsResult(matched=[...], filtered_out=[...], unmatched=[...])
  │
  └─→ [CLI] print matched symbols (one per line) to stdout
           exit code 0
```

### 1.2 State Transitions

```
START
  ↓
PARSING ARGS
  ├─ Filter object created
  └─ Validation: interval in [1m, 5m, 15m, ...]
  ↓
FETCHING SYMBOLS
  ├─ S3 List Request (paginated)
  ├─ If error: retry with exponential backoff
  ├─ If success: collect all pages
  └─ Parse XML, extract prefixes
  ↓
INFERRING METADATA
  ├─ For each symbol: parse base/quote/leverage
  ├─ Classify as: stable_pair, leverage, regular
  └─ If error: add to unmatched bucket
  ↓
FILTERING
  ├─ For each: filter.matches(info)?
  ├─ Yes → matched bucket
  └─ No → filtered_out bucket
  ↓
FORMATTING OUTPUT
  ├─ Print matched symbols
  ├─ Print count summary to stderr
  └─ If unmatched > 0: warn to stderr
  ↓
END
```

### 1.3 Inputs & Outputs

**Inputs:**
- `--trade-type`: spot | um | cm
- `--data-freq`: daily | monthly
- `--data-type`: klines (interval varies)
- `--interval`: 1m, 5m, 15m, 1h, 1d, ... (context-dependent)
- `--quote-asset`: optional, filter by USDT/BUSD/USDC/...
- `--exclude-leverage`: boolean, skip leverage symbols
- `--exclude-stables`: boolean, skip stablecoin pairs

**Outputs:**
```
matched_symbol_1
matched_symbol_2
...
```

**Stderr (logging):**
```
Found 100 symbols; filtered 20; unmatched 5
Showing 75 matched
```

---

## 2. Command: `list-files`

### 2.1 Sequence Diagram

```
User / CLI
  │
  ├─→ [CLI] parse args (trade_type, data_freq, data_type, symbols, interval)
  │
  ├─→ [CLI] validate symbols (non-empty, uppercase)
  │
  ├─→ [CLI] construct ArchiveListFilesWorkflow(client, symbols, interval)
  │
  ├─→ [Workflow] for each symbol (concurrently with semaphore):
  │         │
  │         ├─→ [Client] build S3 prefix: "data/spot/daily/klines/BTCUSDT/"
  │         ├─→ [Client] list_dir(prefix) → paginated files (XML)
  │         ├─→ [Client] filter by interval: *_1m.zip vs *_1d.zip
  │         ├─→ [Client] parse XML → extract FileMetadata (key, size, last_modified)
  │         ├─→ [Client] return sorted list[FileMetadata] (by last_modified ASC)
  │         └─→ [Workflow] collect per-symbol result or error
  │
  ├─→ [Workflow] aggregate results:
  │         ├─ Per-symbol file lists (sorted)
  │         ├─ Per-symbol errors (network, timeout)
  │         └─ Overall counts (total_files, total_bytes, errors)
  │
  ├─→ [Workflow] return ListFilesResult(per_symbol=[...], summary={...})
  │
  └─→ [CLI] print table:
           symbol | file_count | total_bytes | error?
           ─────────────────────────────────────────────
           BTCUSDT | 365 | 1.2GB |
           ETHUSDT | 240 | 800MB |
```

### 2.2 Data Structure (Per-Symbol Result)

```python
@dataclass
class PerSymbolFilesResult:
    symbol: str
    files: list[FileMetadata]  # Sorted by last_modified
    error: str | None
    counts: dict = {
        "file_count": int,
        "total_bytes": int,
        "date_range": (str, str)  # earliest, latest
    }
```

### 2.3 State Transitions

```
START
  ↓
VALIDATING INPUTS
  ├─ Symbols: non-empty, uppercase, max 16 chars each
  ├─ Interval: in [1m, 5m, ..., 1Mo]
  └─ If error: fail fast with ValidationError
  ↓
CONCURRENT S3 LISTING
  ├─ For each symbol, in parallel (max 10 concurrent):
  │   ├─ Build S3 prefix
  │   ├─ Paginate S3 list (may be 100+ pages for 10y daily data)
  │   ├─ Collect all files
  │   └─ Filter by interval pattern
  │
  ├─ On network error (timeout, 5xx):
  │   ├─ Retry with exponential backoff (up to 3 attempts)
  │   ├─ If all fail: record error, move to next symbol
  │   └─ Continue with other symbols (resilient mode)
  │
  └─ On validation error (bad input): fail fast
  ↓
AGGREGATING RESULTS
  ├─ Sort per-symbol files by last_modified
  ├─ Count files and bytes per symbol
  ├─ Compute date range (earliest, latest)
  └─ Merge counts into summary
  ↓
FORMATTING OUTPUT
  ├─ Print table (symbol | file_count | total_bytes | error?)
  ├─ Print summary: "Found N files across M symbols; X errors"
  └─ Exit code 0 if all symbols listed, 1 if any errors
  ↓
END
```

---

## 3. Command: `download`

### 3.1 Sequence Diagram (High-Level)

```
User / CLI
  │
  ├─→ [CLI] parse args (symbols, trade_type, data_type, interval, --dry-run)
  │
  ├─→ [CLI] resolve archive_home
  │
  ├─→ [CLI] construct ArchiveDownloadWorkflow(client, symbols, archive_home, dry_run)
  │
  ├─→ [Workflow] STEP 1: List remote files (concurrently)
  │         └─→ [Client.list_symbol_files_batch] fetch all files for all symbols
  │
  ├─→ [Workflow] STEP 2: Scan local archive_home
  │         └─→ [SymbolArchiveDir] scan each symbol directory for existing .zip files
  │
  ├─→ [Workflow] STEP 3: Compute diff (local vs remote)
  │         ├─ For each remote file:
  │         │   ├─ If local not exists: NEW
  │         │   ├─ If local older: UPDATED
  │         │   └─ If local same/newer: SKIP
  │         └─ Classify into [to_download, skipped, errors]
  │
  ├─→ IF dry_run == True:
  │   └─→ Return DiffResult; print to stdout; exit 0
  │
  ├─→ IF dry_run == False:
  │   ├─→ [Workflow] STEP 4: Invalidate verification markers
  │   │         └─→ Delete .{symbol}/file.zip.TIMESTAMP.verified for updated files
  │   │
  │   ├─→ [Workflow] STEP 5: Delete outdated local copies
  │   │         └─→ rm .{symbol}/file.zip for updated files
  │   │
  │   ├─→ [Workflow] STEP 6: Prepare download batches
  │   │         ├─ Group files by symbol
  │   │         ├─ For each: create output path
  │   │         └─ Build list[DownloadRequest]
  │   │
  │   ├─→ [Downloader] Download files (aria2c, concurrent)
  │   │         ├─ Batch 1: aria2c (max 16 connections per file)
  │   │         ├─ Per-file retry: exponential backoff (up to 5 attempts)
  │   │         ├─ On failure: record error, move to next
  │   │         └─ On success: write .checksum file (SHA256)
  │   │
  │   ├─→ [Downloader] Emit progress events (bytes downloaded, eta, throughput)
  │   │
  │   ├─→ [Workflow] Collect results
  │   │         └─→ Return DownloadResult(downloaded=N, failed=M, errors=[...])
  │   │
  │   └─→ [CLI] print summary; exit 0 or 2 (if failures)
  │
  └─→ END
```

### 3.2 Diff Algorithm

```
FOR each remote_file:
  local_path = archive_home / symbol_dir / remote_file.name

  IF local_path NOT EXISTS:
    → Classify as NEW

  ELSE:
    local_mtime = os.path.getmtime(local_path)
    remote_mtime = remote_file.last_modified

    IF remote_mtime > local_mtime:
      → Classify as UPDATED
    ELSE:
      → Classify as SKIPPED

AGGREGATE:
  to_download = NEW + UPDATED
  skipped = SKIPPED
```

### 3.3 State Transitions (Detailed)

```
START
  ↓
PARSE ARGS & VALIDATE
  ├─ Symbols: non-empty
  ├─ Trade type: spot | um | cm
  ├─ Data type: klines | trades | aggTrades | ...
  ├─ Interval: valid for data_type
  └─ archive_home: writable directory
  ↓
RESOLVE ARCHIVE HOME
  ├─ Check --archive-home flag
  ├─ Check env var BINANCE_DATATOOL_ARCHIVE_HOME
  ├─ Check ~/.binance-datatool/archive
  ├─ If none found: raise ArchiveHomeNotConfiguredError
  └─ Verify directory is writable (create if needed)
  ↓
LIST REMOTE FILES (concurrent)
  ├─ For each symbol (10 at a time):
  │   ├─ Call client.list_files(symbol, trade_type, data_type, interval)
  │   ├─ On success: collect files
  │   └─ On error: record per-symbol error, continue
  │
  ├─ If all symbols failed: exit 1 (critical)
  └─ Otherwise: continue with partial results
  ↓
SCAN LOCAL DIRECTORY
  ├─ For each symbol:
  │   ├─ Check if symbol dir exists in archive_home
  │   ├─ If not: create it
  │   ├─ Scan for .zip files
  │   └─ Collect mtimes
  │
  └─ Build local_files dict {symbol: {filename: mtime}}
  ↓
COMPUTE DIFF
  ├─ For each remote file:
  │   ├─ Look up in local_files
  │   ├─ Compare mtime
  │   └─ Classify: NEW | UPDATED | SKIP
  │
  └─ Aggregate: to_download = NEW + UPDATED
  ↓
IF dry_run:
  ├─ Print table:
  │   symbol | status | file | size | action
  │   ─────────────────────────────────────────
  │   BTCUSDT | new | BTCUSDT_2026-01-01_1d.zip | 1.2M | download
  │   BTCUSDT | skip | BTCUSDT_2026-01-02_1d.zip | 1.3M | skip
  │
  └─ Exit 0
  ↓
INVALIDATE VERIFICATION MARKERS
  ├─ For each UPDATED file:
  │   ├─ Find .zip.TIMESTAMP.verified marker
  │   └─ Delete if exists
  │
  └─ Continue
  ↓
DELETE OUTDATED LOCAL COPIES
  ├─ For each UPDATED file:
  │   ├─ Delete local .zip
  │   └─ (checksum file will be overwritten)
  │
  └─ Continue
  ↓
PREPARE DOWNLOAD BATCHES
  ├─ For each file in to_download:
  │   ├─ Resolve output path: archive_home/data/{trade_type}/.../{symbol}/{filename}.zip
  │   ├─ Create symbol dir if needed
  │   └─ Build DownloadRequest(url, output_path, expected_checksum)
  │
  └─ Batch into groups (e.g., 100 files per batch)
  ↓
DOWNLOAD FILES (concurrent, aria2c)
  ├─ For each batch:
  │   ├─ Invoke aria2c with:
  │   │   - 16 max connections per file (--max-connection-per-server)
  │   │   - 5 retry attempts (--max-tries)
  │   │   - Exponential backoff (--retry-wait)
  │   │
  │   ├─ Monitor progress (bytes/sec, eta)
  │   │
  │   └─ On completion (success or failure):
  │       ├─ If success: write .checksum file with expected SHA256
  │       └─ If fail (all retries exhausted):
  │           ├─ Record error
  │           ├─ Leave .zip and .checksum missing
  │           └─ Continue with next batch
  │
  └─ Aggregate results
  ↓
EMIT PROGRESS & RESULTS
  ├─ Print summary:
  │   - Downloaded: N files
  │   - Failed: M files
  │   - Skipped: K files
  │   - Total throughput: X MB/s
  │   - Duration: T seconds
  │
  ├─ If M > 0: print error details
  └─ Exit 0 (success) or 2 (has failures)
  ↓
END
```

---

## 4. Command: `verify`

### 4.1 Sequence Diagram

```
User / CLI
  │
  ├─→ [CLI] parse args (trade_type, data_type, symbols, --dry-run, --cleanup)
  │
  ├─→ [CLI] resolve archive_home
  │
  ├─→ [CLI] construct ArchiveVerifyWorkflow(archive_home, symbols, cleanup, dry_run)
  │
  ├─→ [Workflow] STEP 1: Scan local directory (threaded)
  │         ├─ For each symbol:
  │         │   ├─ Find all .zip files
  │         │   ├─ Check for .zip.CHECKSUM file (expected hash)
  │         │   ├─ Check for .zip.TIMESTAMP.verified marker
  │         │   └─ Classify: to_verify | already_verified | orphaned
  │         │
  │         └─ Return VerifyDiffResult
  │
  ├─→ IF dry_run:
  │   └─→ Print diff; exit 0
  │
  ├─→ IF dry_run == False:
  │   ├─→ [Workflow] STEP 2: Clean orphaned files
  │   │         ├─ For each orphan .zip (missing .checksum):
  │   │         │   ├─ If cleanup: rm .zip
  │   │         │   └─ Else: log warning
  │   │         └─ Emit summary: X orphans cleaned
  │   │
  │   ├─→ [Workflow] STEP 3: Verify files (process pool)
  │   │         ├─ For each file in to_verify:
  │   │         │   ├─ Calculate SHA256 hash (streaming, buffered)
  │   │         │   ├─ Read expected hash from .zip.CHECKSUM
  │   │         │   ├─ Compare
  │   │         │   ├─ If match: write .zip.TIMESTAMP.verified marker
  │   │         │   └─ If mismatch:
  │   │         │       ├─ Record failure
  │   │         │       ├─ If cleanup: delete .zip and .checksum
  │   │         │       └─ Else: leave as-is
  │   │         │
  │   │         └─ Emit progress (N/M hashes computed, eta)
  │   │
  │   ├─→ [Workflow] Collect results
  │   │         └─→ Return VerifyResult(verified=N, failed=M, orphans_cleaned=P, ...)
  │   │
  │   └─→ [CLI] print summary; exit 0 or 2 (if failures)
  │
  └─→ END
```

### 4.2 State Transitions

```
START
  ↓
PARSE ARGS & VALIDATE
  ├─ Trade type, data type, symbols
  ├─ archive_home: readable
  └─ --cleanup: boolean flag
  ↓
RESOLVE ARCHIVE HOME
  └─ Same as download command
  ↓
SCAN LOCAL DIRECTORY (ThreadPoolExecutor)
  ├─ For each symbol directory:
  │   ├─ Find all .zip files
  │   ├─ For each .zip:
  │   │   ├─ Check if .zip.CHECKSUM exists
  │   │   ├─ Check if .zip.TIMESTAMP.verified exists
  │   │   │
  │   │   ├─ If .checksum AND .verified → already_verified (SKIP)
  │   │   ├─ If .checksum AND NO .verified → to_verify (HASH)
  │   │   └─ If NO .checksum → orphaned (CLEANUP or WARN)
  │   │
  │   └─ Collect results per symbol
  │
  └─ Return VerifyDiffResult(to_verify=[...], already_verified=[...], orphaned=[...])
  ↓
IF dry_run:
  ├─ Print table:
  │   symbol | status | file | size
  │   ─────────────────────────────
  │   BTCUSDT | to_verify | BTCUSDT_2026-01-01_1d.zip | 1.2M
  │   BTCUSDT | verified | BTCUSDT_2026-01-02_1d.zip | 1.3M
  │   BTCUSDT | orphaned | BTCUSDT_2026-01-03_1d.zip | 1.1M
  │
  └─ Exit 0
  ↓
CLEANUP ORPHANED FILES
  ├─ For each orphaned .zip:
  │   ├─ If --cleanup: rm .zip (log message)
  │   └─ Else: log warning, leave as-is
  │
  └─ Emit: "Cleaned X orphaned files"
  ↓
VERIFY FILES (ProcessPoolExecutor, ~8 workers)
  ├─ For each file in to_verify:
  │   ├─ Calculate SHA256:
  │   │   ├─ Open .zip file
  │   │   ├─ Read in 64KB chunks
  │   │   ├─ Feed to hashlib.sha256()
  │   │   └─ Close file
  │   │
  │   ├─ Read expected hash from .zip.CHECKSUM file (1-line format)
  │   │
  │   ├─ Compare (case-insensitive hex)
  │   │   ├─ If MATCH:
  │   │   │   ├─ Write marker: .zip.{TIMESTAMP}.verified
  │   │   │   └─ Record success
  │   │   │
  │   │   └─ If MISMATCH:
  │   │       ├─ Log error with hashes
  │   │       ├─ If --cleanup: rm .zip and .checksum
  │   │       └─ Record failure
  │   │
  │   └─ Emit progress (Processed N/M, eta)
  │
  └─ Aggregate results
  ↓
EMIT RESULTS
  ├─ Print summary:
  │   - Verified: N files ✓
  │   - Failed: M files ✗
  │   - Skipped (already verified): K files
  │   - Orphans cleaned: P files
  │   - Duration: T seconds
  │   - Throughput: X MB/s
  │
  ├─ If M > 0:
  │   ├─ Print details of failed files
  │   └─ Print expected vs actual hashes (first 8 chars)
  │
  └─ Exit 0 (all success) or 2 (has failures)
  ↓
END
```

---

## 5. Multi-Source Adapter Flow (Proposed)

### 5.1 Unified Adapter Protocol

```
                      ┌─────────────────────┐
                      │   User / Workflow   │
                      └──────────┬──────────┘
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
            SourceRegistry              (discovers adapters)
                    │
                    ├─→ adapter = registry.get("binance")
                    ├─→ adapter = registry.get("coinbase")
                    └─→ adapter = registry.get("kraken")
                    │
                    ▼
            ┌──────────────────────────────────┐
            │ DataSourceAdapter (Protocol)     │
            │                                  │
            │ • list_symbols()                 │
            │ • list_files()                   │
            │ • fetch_file()                   │
            │ • parse_symbol()                 │
            │ • get_metadata()                 │
            └──────────────────────────────────┘
                    ▲          ▲          ▲
                    │          │          │
        ┌───────────┘          │          └──────────┐
        │                      │                     │
        ▼                      ▼                      ▼
   BinanceAdapter        CoinbaseAdapter        KrakenAdapter

   • Wraps S3/HTTP   • REST API            • REST API
   • XML parsing     • JSON parsing        • JSON parsing
   • Prefix-based    • Product IDs         • Pair symbols
```

### 5.2 Sequence: List Symbols (Multi-Source)

```
User CLI
  │
  ├─→ [CLI] --sources binance,coinbase,kraken
  │
  ├─→ [Workflow] registry.get_all(["binance", "coinbase", "kraken"])
  │         └─→ [registry] return [BinanceAdapter(), CoinbaseAdapter(), KrakenAdapter()]
  │
  ├─→ [Workflow] for each adapter (concurrently):
  │         │
  │         ├─→ [BinanceAdapter] list_symbols(spot, daily, klines)
  │         │         └─→ S3 list → ["BTCUSDT", "ETHUSDT", ...]
  │         │
  │         ├─→ [CoinbaseAdapter] list_symbols(spot, daily, klines)
  │         │         └─→ REST API /products → ["BTC-USD", "ETH-USD", ...]
  │         │
  │         └─→ [KrakenAdapter] list_symbols(spot, daily, klines)
  │                   └─→ REST API /AssetPairs → ["XXBTZUSD", "XETHZUSD", ...]
  │
  ├─→ [Workflow] normalize results:
  │         ├─ Map each to canonical SymbolInfo
  │         ├─ Merge across sources
  │         └─ Apply global filters (quote asset, etc.)
  │
  ├─→ [Workflow] return ListSymbolsResult(
  │         per_source={
  │           "binance": ["BTCUSDT", ...],
  │           "coinbase": ["BTC-USD", ...],
  │           "kraken": ["XXBTZUSD", ...]
  │         }
  │       )
  │
  └─→ [CLI] print union or per-source breakdown; exit 0
```

### 5.3 Adapter Interface (Proposed Protocol)

```python
@runtime_checkable
class DataSourceAdapter(Protocol):
    """Adapter for a data source (exchange, provider)."""

    source: str  # "binance", "coinbase", "kraken", etc.

    async def list_symbols(
        self,
        market_type: str,  # "spot" | "um" | "cm" | etc.
        partition: str,    # "daily" | "monthly" | etc.
        data_type: str     # "klines" | "trades" | etc.
    ) -> list[str]:
        """List all available symbols.

        Returns:
            Sorted list of symbol strings (e.g., ["BTCUSDT", "ETHUSDT"]).

        Raises:
            NetworkError: Connection issue.
            TimeoutError: Request timeout.
        """
        ...

    async def list_files(
        self,
        symbol: str,
        market_type: str,
        partition: str,
        data_type: str,
        interval: str | None = None
    ) -> list[FileMetadata]:
        """List available data files for a symbol.

        Returns:
            Sorted list of files (by last_modified ASC).
            Empty list if symbol not found or no data.
        """
        ...

    async def fetch_file(
        self,
        symbol: str,
        file_key: str,
        output_path: str
    ) -> FileResult:
        """Download a specific file.

        Returns:
            FileResult(success, bytes_written, checksum).
        """
        ...

    def parse_symbol(self, symbol_str: str) -> SymbolMetadata:
        """Parse a symbol string into components.

        Example: "BTCUSDT" → SymbolMetadata(
            base="BTC", quote="USDT", source=DataSource.BINANCE, ...
        )
        """
        ...

    async def get_metadata(self) -> dict:
        """Get metadata about this source (rate limits, capabilities, etc.)."""
        ...
```

---

## 6. Data Contract & Validation Flow (Proposed)

### 6.1 Contract Definition

```python
# In adapter or external config:
binance_spot_klines_contract = DataContract(
    source=DataSource.BINANCE,
    market_type=MarketType.SPOT,
    data_type=DataType.KLINES,
    partition_freq=PartitionFreq.DAILY,

    # Schema: column_name → type
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

    # Required: data must be partitioned by (date, symbol)
    partition_cols=["date", "symbol"],

    # Primary key must be unique across partition
    key_cols=["open_time"],

    # Validation rules
    validators=[
        lambda row: row["open"] > 0,
        lambda row: row["close"] > 0,
        lambda row: row["close_time"] > row["open_time"],
    ]
)
```

### 6.2 Validation Flow

```
Workflow (download complete)
  │
  ├─→ [Workflow] extract_files(downloaded_zips, archive_home)
  │         └─→ Unzip all downloaded files
  │
  ├─→ [Workflow] load_partition_data(symbol, date, format="csv")
  │         └─→ Read CSV/JSON/Parquet into in-memory dataframe
  │
  ├─→ [Validator] validate_against_contract(dataframe, contract)
  │         │
  │         ├─→ Check schema (columns present, types correct)
  │         ├─→ Check nullability (no NULLs in key_cols)
  │         ├─→ Run custom validators (price > 0, etc.)
  │         └─→ Return ValidationResult(passed, errors=[...])
  │
  ├─→ IF validation PASSED:
  │   ├─→ [Lineage] record_lineage(
  │   │         source="binance",
  │   │         symbol="BTCUSDT",
  │   │         date="2026-01-01",
  │   │         status="VALID",
  │   │         row_count=N,
  │   │         checksum=hash(dataframe)
  │   │       )
  │   │
  │   └─→ [Storage] write_to_lake(dataframe, partition_cols, format)
  │
  ├─→ ELSE (validation FAILED):
  │   ├─→ [Lineage] record_lineage(..., status="INVALID", errors=[...])
  │   └─→ [CLI] print errors; exit 1 (no further processing)
  │
  └─→ END
```

---

## 7. Integration: Workflow → Adapter → Storage

### 7.1 Full Request/Response Path

```
CLI Entry Point
│
├─→ [CLI] parse user args
│
├─→ [Workflow Factory] construct workflow with dependencies:
│         ├─ source_adapter = registry.get(source_name)
│         ├─ storage_backend = StorageRegistry.get(storage_type)
│         └─ contract = ContractRegistry.get(source, market_type, data_type)
│
├─→ [Workflow] execute():
│   │
│   ├─→ Phase 1: List
│   │         ├─ adapter.list_symbols() → list[str]
│   │         └─ adapter.list_files() → list[FileMetadata]
│   │
│   ├─→ Phase 2: Diff
│   │         ├─ storage_backend.list_local_files(symbol) → list[str]
│   │         └─ compute diff (remote vs local)
│   │
│   ├─→ Phase 3: Download
│   │         ├─ adapter.fetch_file() → FileResult
│   │         └─ storage_backend.write_file(path, data)
│   │
│   ├─→ Phase 4: Validate
│   │         ├─ extract_and_read(file)
│   │         └─ contract.validate(dataframe) → ValidationResult
│   │
│   ├─→ Phase 5: Lineage
│   │         └─ lineage_tracker.record(source, symbol, status, metadata)
│   │
│   └─→ Phase 6: Publish
│           └─ storage_backend.publish_partition(dataframe, partition_cols)
│
├─→ [Result] aggregated summary with metrics
│         ├─ downloaded: N
│         ├─ validated: M
│         ├─ published: K
│         ├─ failed: F
│         └─ duration_seconds: T
│
└─→ [CLI] format and print result; exit code
```

---

## 8. Error Handling & Resilience

### 8.1 Error Classification

```
Transient Errors (RETRY)
├─ Network timeout
├─ Temporary 5xx response (502, 503, 504)
├─ Transient connection reset
└─ Local disk full → wait & retry

Persistent Errors (SKIP)
├─ Symbol not found (404)
├─ File already deleted (410)
├─ Invalid credentials (403)
└─ Source rate limited → backoff & report

Fatal Errors (ABORT)
├─ Invalid command arguments
├─ Archive home not writable
├─ Corrupted file (checksum mismatch)
└─ Contract validation failed (data integrity issue)
```

### 8.2 Retry Strategy

```
Transient Error Detected
  │
  ├─→ Wait time = base_delay * (2 ^ attempt_count)
  │   (e.g., 1s, 2s, 4s, 8s)
  │
  ├─→ attempt_count < max_attempts (5)?
  │   ├─ YES: retry (go back to Phase X)
  │   └─ NO: give up, record error
  │
  ├─→ If RETRY exhausted:
  │   ├─ Per-symbol: move to next symbol (partial success)
  │   ├─ Global: exit code 1 (some failures)
  │   └─ Emit error summary to stderr
  │
  └─→ Continue workflow (resilient mode)
```

---

## 9. Summary

| Command | Input | Processing | Output |
|---------|-------|-----------|--------|
| **list-symbols** | trade_type, data_type, filters | S3 list + filter | Symbol list |
| **list-files** | trade_type, data_type, symbols | S3 list per symbol (concurrent) | File table |
| **download** | symbols, trade_type, data_type | List + diff + aria2c | Download summary |
| **verify** | symbols, archive_home | Scan + hash (parallel) | Verify summary |

All commands follow the same principles:
- **Resilient**: partial failures don't abort (unless critical)
- **Atomic**: files are written completely or not at all
- **Observable**: progress and errors logged to stderr; results to stdout
- **Composable**: results can be piped to next command

---

**Document Version**: 1.0
**Last Updated**: 2026-05-07
**Status**: Active (foundation documented; adapter pattern in progress)
