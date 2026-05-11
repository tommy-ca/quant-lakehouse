# Architecture Comparison: Original vs New Stack

## Side-by-Side

| Concern | Original Stack | New Stack |
|---------|---------------|-----------|
| **Extract** | `ArchiveClient` (aiohttp S3) + aria2c | `dlt` sources (REST API, S3, WS streaming) |
| **Schema** | Dataclasses + `DataContract` | `Pydantic` models + `Pandera` DataFrames |
| **Transform** | Ad-hoc Polars in `SinkWorkflow` (621 lines) | Polars transforms per data type (avg 80 lines) |
| **Storage** | DuckDB file + manual `DuckLakeCatalog` | `dlt.destinations.ducklake()` — auto-managed |
| **Orchestration** | Prefect flows wrapping legacy classes | Prefect flows wrapping dlt + Polars tasks |
| **Validation** | `DataContract.validate()` (never called) | Pandera at pipeline boundaries + Pydantic in dlt |
| **State** | LineageTracker (in-memory, manual save) | dlt pipeline state (auto-synced to destination) |
| **Symbols** | `MetadataWorkflow` (hardcoded venues) | dlt `venues_resource` + `symbols_resource` (live S3) |
| **CLI** | 8 commands, all tight to archive | 8 commands unchanged + new dlt pipeline (parallel path) |

## SOLID Analysis

| Principle | Original | New Stack |
|-----------|----------|-----------|
| **S**ingle Responsibility | `SinkWorkflow` (621 lines) does scan, read, cast, rename, validate, write | Each transform = one file, one data type, one responsibility |
| **O**pen/Closed | Adding a new data type requires changes in 5+ workflow files | Add a new dlt source + transform + Pandera schema — no existing code changes |
| **L**iskov Substitution | `ExchangeClient` protocol partially implemented (WS returns `str`, REST returns `TradeType`) | All dlt sources return consistent `dict` shapes |
| **I**nterface Segregation | `DataSourceAdapter` protocol has 4 methods, some unused | `@dlt.resource` has minimal interface: yield dicts |
| **D**ependency Inversion | Workflows import concrete `ArchiveClient` directly | dlt sources take params, Prefect resolves dependencies |

## KISS Analysis

| Aspect | Original | New Stack |
|--------|----------|-----------|
| **Extract** | aria2 subprocess + S3 XML pagination | `dlt` handles pagination, retries, state |
| **Sink** | 621-line workflow with ZIP traversal, CSV parsing, column casting, schema lookups | `bronze_klines_to_silver()` — 80 lines of pure Polars |
| **Validation** | `DataContract` (443 lines) with `isinstance()` checks, never integrated | Pandera `SchemaModel.validate(df)` — one line |
| **Metadata** | `MetadataWorkflow` (234 lines) with hardcoded venues + SDK adapter | dlt resource — 50 lines, S3 discovery |

## DRY Analysis

| Duplication | Original | New Stack |
|-------------|----------|-----------|
| **Column schemas** | `types.py` dataclasses + `sink.py` dicts + `catalog.py` DDL — 3 sources of truth | `Pydantic` models = single source of truth; Pandera enforces at runtime |
| **Timestamp handling** | `_normalize_to_microseconds()` heuristic duplicated across sink, gap-fill | Per-transform `* 1000` for ms→μs, documented in each transform |
| **Connection management** | `duckdb.connect()` + `LOAD ducklake` + `ATTACH` in 5+ places | `workflow/db.py` — `get_connection()` in one place |
| **Silver table writes** | `CREATE TABLE IF NOT EXISTS ... AS SELECT * FROM ... WHERE FALSE` in 3 tasks | `write_silver_table()` in `workflow/db.py` |

## YAGNI Analysis

| Speculative Feature | Original | New Stack |
|--------------------|----------|-----------|
| **Iceberg catalog** | `IcebergCatalog` class with 9 `TABLE_SPECS`, never used | Not built — DuckLake handles partitioning |
| **Skills framework** | `docs/skills-subagents.md` — 680 lines, no code | Not built (moved to `docs/proposals/`) |
| **MetricsCollector** | Referenced in roadmap, removed from scope | Not built |
| **LineageTracker** | 17 event types, 401 lines, only 2 ever used | Not rebuilt — dlt state tracks pipeline runs |

## DataOps/MLOps Assessment

| Practice | Original | New Stack |
|----------|----------|-----------|
| **Data contracts** | `DataContract` class — defined but never called | `schema_contract="freeze"` on all dlt resources |
| **Quality gates** | None at pipeline boundaries | Pandera validates bronze/silver at every transform |
| **Schema evolution** | Manual column management | dlt `freeze` + Pydantic authoritative models |
| **Lineage** | `LineageTracker` (in-memory, manual export) | dlt pipeline state (auto-synced to DuckLake) |
| **Reproducibility** | aria2 downloads + manual CSV parsing | dlt with `write_disposition="merge"` + primary keys |
| **Observability** | `loguru` + custom metrics | Prefect UI + dlt traces + `print()` in flows |

## Key Metrics

| Metric | Original | New Stack | Delta |
|--------|----------|-----------|-------|
| Total tests | 280 | 331 | **+51** |
| Test coverage | Archive workflows + exchange clients | Same + dlt sources + transforms + Pandera + Pydantic + cache | Expanded |
| dlt resources | 0 | 7 (klines, archive, aggTrades, fundingRate, WS, venues, symbols) | **+7** |
| Pandera schemas | 0 | 4 (bronze klines, silver klines, silver aggTrades, silver fundingRate) | **+4** |
| Pydantic models | 0 | 3 (KlineModel, AggTradeModel, FundingRateModel) | **+3** |
| Polars transforms | 1 (in sink.py) | 3 (klines, aggTrades, fundingRate) — dedicated modules | **+2** |
| Prefect flows | 9 legacy | 9 legacy + 2 new (dlt_sqlmesh, dlt_historical) | **+2** |
| pandas dependency | Required (fetchdf, to_sql) | Zero | **Eliminated** |
| Manual SQL boilerplate | ~60 lines across 5 locations | 2 reusable functions in `workflow/db.py` | **-58 lines** |
