---
name: binance-datatool
description: Download and verify historical crypto market data from Binance archive. Use when the user asks to download klines, trades, aggTrades, funding rates, or order book data, list symbols, verify data integrity, or manage the local archive.
---

# Binance Datatool

CLI toolkit for downloading and verifying historical market data from the Binance public archive (data.binance.vision).

## Quick Reference

```bash
# List available symbols
uv run binance-datatool list-symbols --trade-type spot --data-freq daily --data-type klines

# List files for specific symbols
uv run binance-datatool list-files --symbols BTCUSDT,ETHUSDT --trade-type spot --data-freq daily --data-type klines --interval 1d

# Download (dry run first)
uv run binance-datatool download --symbols BTCUSDT --trade-type spot --data-type klines --interval 1d --dry-run
uv run binance-datatool download --symbols BTCUSDT --trade-type spot --data-type klines --interval 1d

# Verify checksums
uv run binance-datatool verify --symbols BTCUSDT --trade-type spot --data-type klines --interval 1d
```

## Command Reference

### list-symbols

Lists symbol directories from the Binance archive.

```bash
binance-datatool list-symbols \
  --trade-type {spot,um,cm} \
  --data-freq {daily,monthly} \
  --data-type {klines,aggTrades,trades,bookDepth,bookTicker,fundingRate} \
  [--interval 1m|5m|15m|1h|4h|1d|...] \
  [--quote-asset USDT] \
  [--exclude-leverage] \
  [--exclude-stables]
```

**Filtering options:**
- `--quote-asset`: Filter by quote asset (USDT, BUSD, USDC, etc.)
- `--exclude-leverage`: Skip leveraged token symbols (e.g., BTCUP, BTCDOWN)
- `--exclude-stables`: Skip stablecoin pairs (e.g., USDCUSDT)

**Output:** One symbol per line to stdout. Summary to stderr.

### list-files

Lists data files for specified symbols.

```bash
binance-datatool list-files \
  --symbols BTCUSDT,ETHUSDT \
  --trade-type spot \
  --data-freq daily \
  --data-type klines \
  --interval 1d
```

**Output:** Table with symbol, file count, total bytes, and any errors.

### download

Downloads new or updated files from the archive.

```bash
# Always dry-run first
binance-datatool download \
  --symbols BTCUSDT \
  --trade-type spot \
  --data-type klines \
  --interval 1d \
  --dry-run

# Then execute
binance-datatool download \
  --symbols BTCUSDT \
  --trade-type spot \
  --data-type klines \
  --interval 1d
```

**Features:**
- Diff-based: only downloads new or updated files
- Resumable: uses aria2c with per-file retry
- Verification markers: tracks which files have been checksum-verified

### verify

Verifies downloaded files against SHA256 checksums.

```bash
binance-datatool verify \
  --symbols BTCUSDT \
  --trade-type spot \
  --data-type klines \
  --interval 1d
  [--dry-run] \
  [--cleanup]
```

**Options:**
- `--dry-run`: Show what would be verified without doing it
- `--cleanup`: Delete orphaned files (missing checksum) and failed verifications

## Data Types

### Klines (OHLCV candles)

Requires `--interval` parameter. Valid intervals: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, `1M`

### Other Data Types

- `aggTrades` — Aggregated trades (no interval)
- `trades` — Raw trades (no interval)
- `bookDepth` — Order book snapshots (no interval)
- `bookTicker` — Best bid/ask (no interval)
- `fundingRate` — Perpetual funding rates (no interval, um/cm only)

Adapter note:
- A minimal adapter package exists at `src/binance_datatool/adapter/`. It exposes
  a tiny `DataSourceAdapter` protocol and a `BinanceAdapter` wrapper around the
  `ArchiveClient`. The canonical ingestion path remains dlt → Polars → Pandera →
  DuckLake; adapters are only necessary when adding new external sources.

## Trade Types

| Type | Description | Market |
|------|-------------|--------|
| `spot` | Spot trading | BTCUSDT, ETHUSDT, etc. |
| `um` | USD-M futures | BTCUSDT (perpetual/delivery) |
| `cm` | COIN-M futures | BTCUSD (perpetual/delivery) |

## Archive Home

The local directory where downloaded files are stored. Resolved in order:

1. `--archive-home` CLI flag
2. `BINANCE_DATATOOL_ARCHIVE_HOME` environment variable
3. `~/.binance-datatool/archive` (default)

Directory structure:
```
archive_home/
└── data/
    ├── spot/
    │   ├── daily/
    │   │   └── klines/
    │   │       ├── BTCUSDT/
    │   │       │   ├── 1m/
    │   │       │   │   ├── BTCUSDT-1m-2024-01-01.zip
    │   │       │   │   ├── BTCUSDT-1m-2024-01-01.zip.CHECKSUM
    │   │       │   │   └── BTCUSDT-1m-2024-01-01.zip.1704153600.verified
    │   │       │   └── 1d/
    │   └── monthly/
    └── um/
        └── daily/
            └── fundingRate/
                └── BTCUSDT/
```

## Error Handling

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `ArchiveHomeNotConfiguredError` | No archive home found | Set `BINANCE_DATATOOL_ARCHIVE_HOME` or use `--archive-home` |
| `ValueError: interval is required` | Kline data without interval | Add `--interval 1d` or appropriate interval |
| `ValueError: interval is not applicable` | Non-kline data with interval | Remove `--interval` flag |
| Network timeout | S3 API unavailable | Retry; tool has built-in exponential backoff (5 attempts) |
| aria2c not found | aria2 not installed | Install: `sudo apt install aria2` or `brew install aria2` |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Critical error (all symbols failed) |
| 2 | Partial failure (some symbols failed) |

## Dataclasses

All workflow results use Python dataclasses. Key types:

```python
from binance_datatool.common.types import (
    ListSymbolsResult,   # Symbol listing with matched/unmatched/filtered_out
    ListFilesResult,     # Per-symbol file listings
    DiffResult,          # Download diff (to_download, skipped)
    DownloadResult,      # Download summary (downloaded, failed)
    VerifyResult,        # Verify summary (verified, failed, orphans)
    ArchiveFile,         # Remote file metadata (key, size, last_modified)
    KlineData,           # OHLCV data with 10 fields
)
```

## Testing

```bash
# All tests (229 passing, 8 skipped)
uv run pytest

# Specific test file
uv run pytest tests/test_archive_client.py -v

# With coverage
uv run pytest --cov=binance_datatool --cov-report=html

# Integration tests (require --run-integration flag)
uv run pytest --run-integration
```

## Project Structure

```
src/binance_datatool/
├── common/          # Shared types, enums, intervals, filters, symbols
├── archive/         # S3 client, checksum, downloader, symbol directory
├── adapter/         # Multi-source adapters (BinanceAdapter, CoinbaseAdapter)
├── exchange/        # Live exchange API clients (REST/WebSocket, CCXT)
├── workflow/        # Business logic orchestration
├── cli/             # Typer CLI commands
├── datacontract.py  # DataContract and ContractRegistry
├── lineage.py       # LineageTracker (data provenance)
└── source_registry.py  # SourceRegistry (source selection)
```

**Total**: 251 tests passing, 9 skipped, ~6,000 LOC.

## Exchange Clients (Live API)

For real-time or recent data via Binance API:

```bash
# Use market-type-specific clients
from binance_datatool.exchange import (
    BinanceSpotRestClient,    # Spot market REST
    BinanceUmRestClient,      # USD-M futures REST
    BinanceCmRestClient,      # COIN-M futures REST
    BinanceSpotWsClient,      # Spot WebSocket streaming
    BinanceUmWsClient,        # USD-M futures WebSocket
    BinanceCmWsClient,        # COIN-M futures WebSocket
)
```

**Optional CCXT support** (install with `uv add binance-datatool[exchange]`):
```python
from binance_datatool.exchange import CCXTExchangeClient, CCXTProExchangeClient
```

## For DataOps/MLOps Workflows

See [data-flows.md](data-flows.md) for:
- Data contract validation (DataContract, ContractRegistry)
- Lineage tracking (LineageTracker, export to JSON/CSV)
- Quality checks and verification workflows

## For Multi-Source Adapters

See [implementation-guide.md](implementation-guide.md) for:
- DataSourceAdapter protocol
- BinanceAdapter (wraps ArchiveClient)
- SourceRegistry for source selection
- Adding new sources (CoinbaseAdapter skeleton available)
