# Module Reference

Reference documentation for the importable modules and CLI packages in
`binance_datatool`.

## Common

Most user-facing imports should prefer the package-level
`binance_datatool.common` re-export surface when it exposes the name you need:

```python
from binance_datatool.common import DataFrequency, DataType, TradeType, resolve_archive_home
```

That package currently re-exports the shared enums, symbol-info dataclasses,
symbol filters, symbol inference helpers, CLI logging helper, archive-home
resolver, and the main S3 / leverage / stablecoin constants. Names that are documented but not
re-exported from `binance_datatool.common` should be imported from their
defining module, for example:

```python
from binance_datatool.common.constants import QUOTE_BASE_EXCLUDES
from binance_datatool.common.path import ARCHIVE_HOME_ENV_VAR
```

| Module | Description |
|--------|-------------|
| [common.constants](common/constants.md) | S3 settings, quote assets, stablecoins, leverage rules. |
| [common.enums](common/enums.md) | TradeType, DataFrequency, DataType, ContractType. |
| [common.filter](common/filter.md) | Typed symbol filters and `build_symbol_filter()`. |
| [common.types](common/types.md) | SymbolInfoBase and per-market symbol info dataclasses. |
| [common.logging](common/logging.md) | `configure_cli_logging` helper for CLI entry points. |
| [common.path](common/path.md) | Archive-home directory resolution. |
| [common.symbols](common/symbols.md) | Symbol inference functions and quote parsing rules. |
| [common.progress](common/progress.md) | Progress-reporting framework (`ProgressEvent`, `ProgressReporter`, `make_reporter`). |

## Archive

| Module | Description |
|--------|-------------|
| [archive](archive/) | Package index and re-export surface for archive access helpers. |
| [archive.client](archive/client.md) | S3 listing client, `ArchiveFile`, and `list_symbols()`. |
| [archive.downloader](archive/downloader.md) | Aria2-backed batch download helpers and result types. |
| [archive.checksum](archive/checksum.md) | SHA256 verification helpers and `VerifyFileResult`. |
| [archive.symbol_dir](archive/symbol_dir.md) | Local symbol archive directory helpers and marker management. |
| [archive.s3-protocol](archive/s3-protocol.md) | S3 XML listing protocol, pagination, retry, and proxy. |

## dlt

Standalone extract/load package. Importable independently: `from binance_datatool.dlt import ...`

| Module | Description |
|--------|-------------|
| `dlt.models` | 8 Pydantic models (KlineModel, AggTradeModel, FundingRateModel, VenueModel, SymbolMetaModel, Raw*) |
| `dlt.destinations` | DuckDB/DuckLake destination builders (`build_pipeline`, `run_source`) |
| `dlt.sources` | `@dlt.source` builders (`build_binance_source`, `build_rest_source`, `build_ws_source`) |
| `dlt.resources` | 7 `@dlt.resource` modules (klines, aggTrades, fundingRate, archive, WS, metadata, archive_index) |

## Transforms

Polars-based Bronze→Silver transforms with Pandera validation.

| Module | Description |
|--------|-------------|
| `transforms.klines` | `bronze_klines_to_silver()` — 19-column silver klines with μs auto-detection |
| `transforms.agg_trades` | `bronze_agg_trades_to_silver()` — 18-column silver aggTrades with side derivation |
| `transforms.funding_rate` | `bronze_funding_rate_to_silver()` — 12-column silver fundingRate with empty mark_price handling |

## Validation

Pandera DataFrame schemas at pipeline boundaries.

| Module | Description |
|--------|-------------|
| `validation.schemas` | 6 Pandera schemas (bronze/silver for klines, aggTrades, fundingRate) + venue/symbol metadata schemas |

## Storage

DuckDB/DuckLake storage abstraction.

| Module | Description |
|--------|-------------|
| `storage.duckdb` | `get_connection()`, `write_silver_table()` |
| `storage.catalog` | DuckLakeCatalog with `TABLE_DEFS` for all silver tables |

## Workflow

| Module | Description |
|--------|-------------|
| [workflow](workflow/README.md) | Business logic orchestration for archive workflows. |
| `workflow.prefect_flows` | Prefect @flow and @task definitions (thin wrappers) |
| `workflow.prefect_tasks` | Importable business logic (`extract.py`, `transform.py`) |

## CLI

| Module | Description |
|--------|-------------|
| [cli](cli/) | Typer CLI overview, verbosity, and sub-command index. |
| [cli commands](cli/archive.md) | CLI commands (`list-symbols`, `list-files`, `download`, `verify`, `gap-fill`, `health`, `sink`, `refresh-metadata`). |

---

See also: [Architecture](../architecture.md) | [Extending the Project](../extending.md)
