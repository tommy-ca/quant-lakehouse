# Architecture

This document describes the internal architecture of `binance-datatool`. It is intended for
contributors, maintainers, and AI agents working on the codebase.

## Package Tree

```
src/binance_datatool/
├── __init__.py              # Version metadata only
├── common/                  # Shared utilities (enums, constants, types, filters, symbols, progress, intervals)
├── archive/                 # Archive access (S3 HTTP client, checksum, downloader, symbol directory)
├── adapter/                 # Multi-source adapter pattern (DataSourceAdapter protocol, Binance/Coinbase adapters)
│   ├── __init__.py          # DataSourceAdapter protocol, FileMetadata
│   ├── binance.py           # BinanceAdapter (wraps ArchiveClient)
│   ├── coinbase.py          # CoinbaseAdapter (skeleton)
│   ├── registry.py          # Adapter registration
│   └── bridge.py           # Backward compatibility bridges
├── exchange/                # Live exchange API clients (REST/WebSocket, CCXT integration)
│   ├── client.py            # ExchangeClient protocol (@runtime_checkable)
│   ├── binance_rest.py      # BinanceSpot/Um/CmRestClient
│   ├── binance_ws.py       # BinanceSpot/Um/CmWsClient
│   ├── ccxt_rest.py        # CCXTExchangeClient (optional dependency)
│   └── ccxt_pro.py         # CCXTProExchangeClient (optional dependency)
├── workflow/                # Business logic orchestration
│   ├── __init__.py          # Re-exports all workflow classes and result types
│   ├── _shared.py           # Shared helpers (infer_symbol_info, validate_interval)
│   ├── download.py          # ArchiveDownloadWorkflow
│   ├── list_files.py        # ArchiveListFilesWorkflow
│   ├── list_symbols.py      # ArchiveListSymbolsWorkflow
│   ├── results.py           # Result dataclasses (ListSymbolsResult, DiffResult, VerifyResult, etc.)
│   └── verify.py            # ArchiveVerifyWorkflow
├── cli/                     # Typer CLI layer
│   ├── __init__.py          # Root callback with -v/-vv verbosity and --archive-home
│   └── archive.py           # Root list-symbols, list-files, download, and verify commands
├── datacontract.py          # DataContract and ContractRegistry (schema validation)
├── lineage.py               # LineageTracker (data provenance tracking)
└── source_registry.py       # SourceRegistry (source selection by name)
```

## Layered Design

The package follows a strict four-layer architecture. Each layer depends only on the layers
below it — outer layers import inner layers, never the reverse.

```
CLI  (cli/)
 └─▶ Workflow  (workflow/)
       └─▶ Archive Client  (archive/)
             └─▶ Common  (common/)
```

| Layer | Package | Responsibility |
|-------|---------|----------------|
| **Common** | `binance_datatool.common` | Shared enums, constants, types, and symbol filters used across the project. |
| **Archive Client** | `binance_datatool.archive` | S3 HTTP communication with data.binance.vision. |
| **Workflow** | `binance_datatool.workflow` | Business logic orchestration; decouples CLI from the client. |
| **CLI** | `binance_datatool.cli` | Typer command definitions, argument parsing, output formatting. |

For detailed API docs see the [module reference](reference/).

### Why Four Layers?

- **Testability.** Workflows and clients can be tested independently of CLI parsing.
- **Composability.** Workflows can be reused from scripts or notebooks without importing Typer.
- **Extensibility.** Adding a new command means adding a thin CLI function that delegates to a
  workflow, without touching the archive client.

## Data Flow

All CLI logging flows through `stderr`; stdout is reserved exclusively for command
results so that sub-commands remain safe to pipe. See the
[CLI overview](reference/cli/) for verbosity flag details.

Every CLI command follows the same four-layer call path: the CLI function parses
arguments, constructs a workflow, and presents the result. The workflow orchestrates
business logic and delegates S3 communication to the archive client. Per-command data
flow diagrams are documented alongside each command in the
[CLI reference](reference/cli/archive.md).

## Extension: Multi-Source & DataOps (Overview)

This project started as a focused toolkit for the Binance public archive. The
design intentionally separates concerns across four layers (CLI, Workflow,
Archive Client, Common). That separation makes it straightforward to generalize
the project into a multi-source data pipeline with explicit DataOps and MLOps
concerns while preserving the existing commands and workflows.

The generalized architecture adds two explicit responsibilities:

- A Source Adapter layer that encapsulates source-specific listing and
  download behaviour (S3 XML listing, REST APIs, third-party SDKs).
- A Storage / DataOps layer that unifies local archive storage and
  downstream partitioning, contract validation, lineage, and metrics.

High-level six-layer diagram (source → sink):

```
CLI / API Layer
    ⇩
Orchestration / Pipeline Layer (workflows / DAGs)
    ⇩
DataOps / Transform Layer (validation, transforms, contracts)
    ⇩
Source Adapter Layer (binance, coinbase, bybit, etc.)
    ⇩
Storage Connector Layer (S3 / local / delta / parquet)
    ⇩
Foundation Layer (shared enums, types, filters, progress)
```

This document and the companion specs describe how to evolve current
workflows into the generalized pattern while keeping existing behaviour and the
CLI interface stable for consumers.

### Why this generalization

- Preserve the current, well-tested behaviours (list-symbols, list-files,
  download, verify) while enabling additional sources.
- Enable contract-driven validation and lineage for DataOps and MLOps.
- Allow the repository to expose small, testable adapters so agents and
  subagents can be written and tested independently.

See docs/specs-driven-development.md for the full requirements and
spec-driven development flows.

### Platform Evolution: dlt + SQLMesh

The roadmap adds two major technologies to the stack (see
`docs/brainstorms/2026-05-11-platform-evolution-requirements.md`):

| Technology | Role | Replaces | Status |
|-----------|------|----------|--------|
| **dlt** | Extract/Load (schema inference, incremental state, pagination) | Hand-rolled pagination/retry/state in ExchangeClient wrappers | Planned |
| **SQLMesh** | Transform (Bronze→Silver, audits, lineage, virtual environments) | Ad-hoc Polars transforms in SinkWorkflow | Planned |

The target architecture:

```
dlt Sources (Binance, tardis.dev, Databento)
  → Bronze DuckDB (auto-normalized per source)
    → SQLMesh Models (Bronze→Silver INCREMENTAL_BY_TIME_RANGE)
      → Silver DuckLake (partitioned, ACID)
        → Feature Store (ML features, point-in-time datasets)
          → Prefect Orchestration (schedules, SLAs, DLQ)
```

Existing workflows (download, verify, gap-fill, sink) remain fully operational as the
legacy parallel path. New source integrations default to dlt + SQLMesh.

### Why this generalization

The `exchange/` module uses **official Binance SDK packages** (`binance-sdk-spot`,
`binance-sdk-derivatives-trading-usds-futures`, `binance-sdk-derivatives-trading-coin-futures`).
These are required dependencies listed in `pyproject.toml`.

**SDK usage by market:**

| Package | Market | REST Method | WS Method |
|---------|--------|-------------|-----------|
| `binance-sdk-spot` | Spot | `rest_api.klines()` | `connection.kline(symbol, interval)` |
| `binance-sdk-derivatives-trading-usds-futures` | UM | `rest_api.kline_candlestick_data()` | `connection.kline_candlestick_streams()` |
| `binance-sdk-derivatives-trading-coin-futures` | CM | `rest_api.kline_candlestick_data()` | `connection.kline_candlestick_streams()` |

**Design decisions:**
- No auth: `ConfigurationRestAPI(api_key="", api_secret="")` for public market data only
- SDK callback-based WS streams are wrapped via `asyncio.Queue` bridge
- Archive client (`archive/` module) remains on `aiohttp` (S3 access is a different concern)
- Backward-compat aliases: `BinanceRestClient = BinanceSpotRestClient`, `BinanceWsClient = BinanceSpotWsClient`
- CCXT remains optional (`[exchange]` extra) for multi-exchange support
