# Original vs dlt Pipeline Workflow Mapping

## Principles

The dlt pipeline is **not a replacement** for the original archive management workflows.
They serve different concerns:

- **Original workflows**: Archive logistics — S3 listing, multi-threaded download via aria2,
  SHA256 verification, filesystem marker management. These manage the local copy of Binance's
  public S3 archive (``data.binance.vision``).
- **dlt pipelines**: Data ingestion — extract data from REST APIs, archive ZIPs, and
  WebSocket streams; transform to Silver schemas; load to DuckDB. These manage data flow
  from any source into the analytics store.

Both paths coexist. Original workflows remain unchanged for users who need the archive
toolchain. dlt pipelines provide a complementary ingestion path.

## Workflow Mapping

### Direct Equivalents (dlt implements the same data flow)

| Original Workflow | dlt Equivalent | Status | Notes |
|---|---|---|---|
| ``GapFillWorkflow`` (REST klines) | ``dlt_sources/binance.py`` ``klines_resource`` | ✅ Complete | REST API fetch with merge disposition. Original also does gap detection + CSV output |
| ``SinkWorkflow`` (Bronze→Silver) | ``transforms/klines.py`` + Prefect ``transform_to_silver`` | ✅ Complete | Polars transform + Pandera validation + Arrow→DuckDB. Supports klines, aggTrades, fundingRate |
| ``MetadataWorkflow`` (symbol discovery) | ``dlt_sources/binance_metadata.py`` | ✅ Complete | Wraps ArchiveListSymbolsWorkflow as dlt resource with replace disposition |
| ``HealthCheckWorkflow`` (DuckLake anomalies) | ``workflow/health_check.py`` ``check_ducklake_anomalies`` | ⏹ Shared | Same function used by both ``historical_pipeline`` and ``dlt_sqlmesh_pipeline`` |

### Conceptually Different (same layer, different approach)

| Original Workflow | dlt Equivalent | Why Different |
|---|---|---|
| ``ArchiveDownloadWorkflow`` | ``dlt_sources/binance_archive.py`` | Original: S3 listing + aria2c download. dlt: reads local ZIP files only. dlt assumes files are already on disk |
| ``GapFillWorkflow`` (detect gaps) | (no equivalent) | Gap detection logic (``_scan_existing_dates``, ``_detect_date_gaps``) has no dlt resource. dlt's merge-on-primary-key provides idempotent loading but no gap detection |
| ``ArchiveVerifyWorkflow`` | (no equivalent) | SHA256 checksum verification is specific to the ZIP+CHECKSUM archive format. dlt relies on transport integrity (TLS for REST, filesystem for ZIPs) |

### No dlt Equivalent Needed

| Original Workflow | Reason |
|---|---|
| ``ArchiveListSymbolsWorkflow`` | Symbol listing is a discovery/lookup operation, not a data ingestion concern. dlt sources take symbols as parameters |
| ``ArchiveListFilesWorkflow`` | File listing is a prerequisite for archive download, not a data concern |
| ``refresh-metadata`` (venue) | 3 hardcoded venues — no dynamic source needed |

## When to Use Which

**Use original workflows when:**
- You need to download new data from ``data.binance.vision``
- You need SHA256 checksum verification
- You want the full gap-detect→gap-fill→sink pipeline for archive data
- You're using the CLI directly (``binance-datatool download/verify/gap-fill/sink``)

**Use dlt pipelines when:**
- You want to ingest REST API data (klines, aggTrades, fundingRate) directly
- You have local archive ZIPs and want to load them into DuckDB with validation
- You want Pandera-validated Silver schemas
- You're orchestrating with Prefect and want the ``dlt_sqlmesh_pipeline`` dispatcher
- You want to add new data sources (tardis.dev, Databento)

**Use both when:**
- You want to download first (original), then validate, transform, and load (dlt)
- The dlt archive resource can read the ZIPs that the original download workflow placed on disk

## Current State Summary

| Layer | Files | Tests |
|---|---|---|
| dlt sources (5 modules) | ``dlt_sources/`` (8 .py files) | 12 tests |
| Polars transforms (3) | ``transforms/`` (4 .py files) | 10 tests |
| Pandera schemas (4) | ``validation/`` (3 .py files) | 16 tests |
| Pydantic models (3) | ``validation/models.py`` | 9 tests |
| Prefect flows | ``workflow/prefect_flows.py`` (835 lines) | (tested via task-level coverage) |
| SQLMesh models | ``models/`` (4 files) | (optional, requires sqlmesh install) |
| **Total** | | **325 tests, 9 skipped** |
