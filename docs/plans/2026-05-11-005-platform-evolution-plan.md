# Plan 005: Platform Evolution — dlt + SQLMesh + Multi-Source

**Status**: Draft
**Date**: 2026-05-11
**Requirements**: `docs/brainstorms/2026-05-11-platform-evolution-requirements.md`

## Summary

Transform `binance-datatool` from a single-source CLI toolkit into a scalable crypto data
engineering platform by introducing dlt (extract/load), SQLMesh (transform), and multi-source
support (tardis.dev, Databento) behind the existing interfaces.

## Implementation Order

### Phase 1: dlt Integration (estimated: 2-3 weeks)

#### 1.1 Add dlt dependency

**File**: `pyproject.toml`
```toml
dependencies = [
  ...,
  "dlt>=1.26.0",
]
```

#### 1.2 Create dlt sources module

**New**: `src/binance_datatool/dlt_sources/__init__.py`
**New**: `src/binance_datatool/dlt_sources/binance.py`

Purpose: Wrap existing `ExchangeClient` implementations as `@dlt.resource` generators.

```python
import dlt
from binance_datatool.exchange import BinanceSpotRestClient, ExchangeClient

@dlt.resource(
    name="binance_klines",
    write_disposition="merge",
    primary_key=("symbol", "interval", "open_time"),
    incremental=dlt.sources.incremental("open_time", initial_value=0),
)
def klines_resource(
    client: ExchangeClient,
    symbol: str,
    interval: str = "1h",
) -> list[dict]:
    """Fetch klines from Binance REST API with incremental loading."""
    last_value = dlt.current.resource_state().get("last_open_time", 0)
    data = asyncio.run(client.fetch_ohlcv(symbol, interval, since=last_value))
    yield [k.to_dict() for k in data]
```

**Test approach**:
- Unit test: `@dlt.source` with `FakeBinanceRestClient` returns expected schema
- Integration test: `dlt pipeline run` with in-memory DuckDB destination

#### 1.3 Create dlt pipeline factory

**New**: `src/binance_datatool/dlt_sources/pipeline.py`

```python
from pathlib import Path
import dlt

def build_pipeline(
    source_name: str,
    destination: str = "duckdb",
    dataset_name: str = "bronze",
    catalog_path: Path | None = None,
) -> dlt.Pipeline:
    """Build a dlt pipeline configured for the project's DuckLake layout."""
    dest = destination
    if destination == "duckdb" and catalog_path:
        db_path = str(catalog_path / "catalog.duckdb")
        dest = dlt.destinations.duckdb(db_path)
    return dlt.pipeline(
        pipeline_name=source_name,
        destination=dest,
        dataset_name=dataset_name,
        progress="log",
    )

def run_source(
    source: dlt.SupportsPipeline,
    *,
    source_name: str = "binance",
    catalog_path: Path | None = None,
    dataset_name: str = "bronze",
) -> dict:
    """Run a dlt source and return load info and metrics."""
    pipeline = build_pipeline(source_name, catalog_path=catalog_path, dataset_name=dataset_name)
    load_info = pipeline.run(source)
    return {
        "load_info": str(load_info),
        "dataset_name": dataset_name,
        "tables_loaded": list(pipeline.default_schema.data_tables()),
    }
```

#### 1.4 Create Prefect task for dlt pipeline

**Modify**: `src/binance_datatool/workflow/prefect_flows.py`

Add a `@task` that runs dlt sources:

```python
@task(retries=2, retry_delay_seconds=30)
def run_dlt_source(
    source_name: str,
    symbol: str,
    data_type: str = "klines",
    interval: str = "1h",
    catalog_path: Path | None = None,
) -> dict:
    """Run dlt pipeline for a single symbol."""
    from binance_datatool.dlt_sources.binance import klines_resource
    from binance_datatool.dlt_sources.pipeline import run_source

    client = _REST_CLIENTS.get(source_name, BinanceSpotRestClient)()
    source = klines_resource(client, symbol=symbol, interval=interval)
    return run_source(source, source_name=source_name, catalog_path=catalog_path)
```

#### 1.5 Add dlt pipeline tests

**New**: `tests/test_dlt_sources.py`

Tests:
- `test_binance_klines_resource_yields_dicts` — verify resource yields dicts with correct keys
- `test_binance_klines_resource_incremental_state` — verify cursor tracking
- `test_pipeline_factory_creates_duckdb` — verify pipeline connects to project DuckDB
- `test_run_source_returns_load_info` — integration with in-memory DuckDB

### Phase 2: SQLMesh Transform Migration (estimated: 2-3 weeks)

#### 2.1 Add SQLMesh dependency

**File**: `pyproject.toml`
```toml
dependencies = [
  ...,
  "sqlmesh>=0.100.0",
]
```

#### 2.2 Initialize SQLMesh project

**New**: `sqlmesh_config.yaml`
**New**: `models/` directory with subdirectories

```
models/
├── bronze/
│   ├── klines.sql          # reads from dlt DuckDB output
│   └── agg_trades.sql
├── silver/
│   ├── klines.sql          # Bronze→Silver transform
│   ├── agg_trades.sql
│   └── funding_rate.sql
├── features/
│   ├── vwap_1h.sql         # Feature engineering
│   └── volatility.sql
└── audits/
    ├── not_null_prices.sql
    └── no_duplicate_timestamps.sql
```

#### 2.3 Create Silver model SQL

**New**: `models/silver/klines.sql`

```sql
MODEL (
  name silver.klines,
  kind INCREMENTAL_BY_TIME_RANGE (time_column ts_event),
  cron '@daily',
  audits (
    not_null(ts_event),
    not_null(open),
    assert_positive(volume),
    no_duplicate_timestamps
  )
);

SELECT
  ts_event,
  ts_recv,
  CAST(open AS DOUBLE) AS open,
  CAST(high AS DOUBLE) AS high,
  CAST(low AS DOUBLE) AS low,
  CAST(close AS DOUBLE) AS close,
  CAST(volume AS DOUBLE) AS volume,
  CAST(quote_volume AS DOUBLE) AS quote_volume,
  CAST(trade_count AS BIGINT) AS trade_count,
  CAST(taker_buy_volume AS DOUBLE) AS taker_buy_volume,
  CAST(taker_buy_quote_volume AS DOUBLE) AS taker_buy_quote_volume,
  source,
  exchange,
  trade_type,
  symbol,
  interval,
  data_type,
  ingested_at
FROM bronze.klines
WHERE ts_event BETWEEN @start_ds AND @end_ds;
```

#### 2.4 Create Prefect flow for SQLMesh apply

**Modify**: `src/binance_datatool/workflow/prefect_flows.py`

Add flows:

```python
@flow(name="SQLMesh Plan", log_prints=True)
def sqlmesh_plan_flow(
    environment: str = "prod",
    start: str | None = None,
    end: str | None = None,
) -> dict:
    """Run SQLMesh plan with optional time range."""
    import sqlmesh
    context = sqlmesh.Context(path="models/")
    plan = context.plan(environment, start=start, end=end)
    plan.apply()
    return {"environment": environment, "applied": True}
```

#### 2.5 Add SQLMesh model tests

**Tests**:
- Unit test each model with known input/output
- Audit test: null prices are caught
- Audit test: duplicate timestamps are caught
- Integration test: model runs against in-memory DuckDB

### Phase 3: tardis.dev Integration (estimated: 1-2 weeks)

#### 3.1 Add tardis.dev dependencies

**File**: `pyproject.toml` — add `tardis-dev` optional dependency

#### 3.2 Create TardisDevClient

**New**: `src/binance_datatool/exchange/tardis.py`

Implements `ExchangeClient` protocol for tardis.dev:

```python
class TardisDevClient:
    """Implements ExchangeClient protocol for tardis.dev API."""

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key or os.environ.get("TARDIS_DEV_API_KEY", "")

    @property
    def exchange_id(self) -> str:
        return "tardis.dev"

    async def fetch_ohlcv(self, symbol, interval, since, until, limit):
        # tardis.dev does not provide pre-computed OHLCV
        # Compute from trades if needed, or raise NotImplementedError
        raise NotImplementedError("Use fetch_agg_trades and compute OHLCV locally")

    async def fetch_agg_trades(self, symbol, since, until, limit):
        # GET /api/v1/{exchange}/trades/{date}
        # Returns: [timestamp, price, amount, side, id, ...]
        ...
```

#### 3.3 Create dlt resources for tardis.dev

**New**: `src/binance_datatool/dlt_sources/tardis.py`

```python
@dlt.resource(
    name="tardis_trades",
    write_disposition="append",
    incremental=dlt.sources.incremental("timestamp"),
)
def tardis_trades_resource(
    exchange: str,
    symbol: str,
    date: str,
) -> Iterator[dict]:
    """Fetch trades from tardis.dev for a given exchange/symbol/date."""
    ...
```

#### 3.4 Create SQLMesh models for tardis.dev

**New**: `models/silver/tardis_trades.sql`

```sql
MODEL (
  name silver.tardis_trades,
  kind INCREMENTAL_BY_TIME_RANGE (time_column ts_event),
);
SELECT
  timestamp * 1000 AS ts_event,         -- tardis uses μs, convert to ms for consistency
  ...existing Silver columns...
FROM bronze.tardis_trades
WHERE timestamp BETWEEN @start_ds AND @end_ds;
```

### Phase 4: Databento Integration (estimated: 2-3 weeks)

#### 4.1 Add Databento SDK dependency

**File**: `pyproject.toml`

#### 4.2 Create DatabentoClient

**New**: `src/binance_datatool/exchange/databento.py`

```python
class DatabentoClient:
    """Implements ExchangeClient protocol for Databento."""

    def __init__(self, api_key: str | None = None) -> None:
        import databento as db
        self._client = db.Historical(api_key or os.environ["DATABENTO_API_KEY"])

    async def fetch_ohlcv(self, symbol, interval, since, until, limit):
        # Databento's OHLCV is native Silver format — map directly
        response = self._client.timeseries.get_range(
            dataset="GLBX.MDP3",
            symbols=[symbol],
            schema="ohlcv-1h",
            start=since,
            end=until,
            limit=limit,
        )
        return [self._to_silver_kline(row) for row in response]
```

#### 4.3 Create dlt resource for Databento

**New**: `src/binance_datatool/dlt_sources/databento.py`

Databento data bypasses Bronze layer (native Silver). Resource writes directly to
`silver.klines` with `source='databento'`.

```python
@dlt.resource(
    name="databento_klines",
    write_disposition="merge",
    primary_key=("symbol", "ts_event"),
)
def databento_klines_resource(
    dataset: str,
    symbols: list[str],
    start: datetime,
    end: datetime,
) -> Iterator[dict]:
    """Fetch OHLCV from Databento. Data is already Silver-normalized."""
    client = DatabentoClient()
    for row in client.fetch_ohlcv(symbols, start=start, end=end):
        yield {
            "ts_event": row.ts_event,
            "ts_recv": row.ts_recv,
            "open": row.open,
            ...
            "source": "databento",
            "exchange": f"databento_{dataset.lower()}",
        }
```

### Phase 5: DataOps/MLOps Layer (estimated: 2-3 weeks)

#### 5.1 Integrate DataContracts as SQLMesh audits

**Modify**: `src/binance_datatool/datacontract.py`

Add method `to_sqlmesh_audit()` to `DataContract`:

```python
class DataContract:
    def to_sqlmesh_audit(self) -> tuple[str, str]:
        """Convert this contract to a SQLMesh audit SQL block."""
        audit_sql = []
        for col, col_type in self.schema.items():
            if col in self.key_cols:
                audit_sql.append(f"SELECT * FROM {self.name} WHERE {col} IS NULL")
        return (f"not_null_{self.name}", "\nUNION ALL\n".join(audit_sql))
```

#### 5.2 Create Feature Store models

**New**: `models/features/vwap_1h.sql`

```sql
MODEL (
  name features.vwap_1h,
  kind INCREMENTAL_BY_TIME_RANGE (time_column ts_event),
  cron '@hourly',
);

SELECT
  ts_event,
  symbol,
  trade_type,
  SUM(volume * close) / SUM(volume) AS vwap,
  COUNT(*) AS bar_count,
  STDDEV(close) AS volatility
FROM silver.klines
WHERE ts_event BETWEEN @start_ds AND @end_ds
GROUP BY symbol, trade_type, ts_event;
```

#### 5.3 Add Prefect SLA monitors

**New**: `src/binance_datatool/workflow/dataops.py`

```python
@flow(name="Freshness Monitor", log_prints=True, schedule="0 */6 * * *")
def freshness_monitor_flow(
    max_stale_hours: int = 24,
    catalog_path: Path | None = None,
) -> dict:
    """Check data freshness across all sources. Alert if stale."""
    import duckdb

    con = duckdb.connect(str(catalog_path / "catalog.duckdb"))
    result = con.execute("""
        SELECT symbol, trade_type, source,
               MAX(ts_event) AS latest_ts,
               DATEDIFF('hour', MAX(ts_event), CURRENT_TIMESTAMP) AS staleness_hours
        FROM silver.klines
        GROUP BY symbol, trade_type, source
        HAVING staleness_hours > ?
    """, [max_stale_hours]).fetchall()

    stale = [{"symbol": r[0], "source": r[2], "staleness_hours": r[4]} for r in result]
    if stale:
        logger.warning(f"Stale symbols: {stale}")
        # Send alert (Slack, email)
    return {"stale_count": len(stale), "stale_symbols": stale}
```

### Phase 6: Skills Framework (estimated: 1-2 weeks)

#### 6.1 Create skills directory

**New**: `skills/` with `SKILL.md` (existing skill doc), `manifest.json`, and implementation.

#### 6.2 Implement discover_symbols skill

**New**: `src/binance_datatool/skills/discover_symbols.py`

```python
class DiscoverSymbolsSkill:
    """Skill: discover trading symbols from any configured source."""

    def __init__(self, source_registry: SourceRegistry | None = None):
        self.registry = source_registry or SourceRegistry

    async def run(self, source: str, market_type: str, **filters) -> dict:
        adapter = self.registry.get(source)()
        symbols = await adapter.list_symbols(market_type, ...)
        return {"success": True, "symbols": symbols, "total": len(symbols)}
```

#### 6.3 Add CLI entry points for skills

**Modify**: `src/binance_datatool/cli/archive.py`

Add `--as-json` flag to existing commands for agent-friendly output.
Register skills CLI:
```python
@app.command("skill")
def skill_command(
    skill_name: str = typer.Argument(...),
    source: str = typer.Option("binance"),
    ...
):
    """Invoke a skill by name (agent-friendly JSON output)."""
    skill = SKILL_REGISTRY[skill_name](source_registry=SourceRegistry)
    result = asyncio.run(skill.run(source=source, ...))
    typer.echo(json.dumps(result, indent=2))
```

## Dependency Graph

```
Phase 1 (dlt) ──┐
                  ├──► Phase 2 (SQLMesh) ──┐
Phase 3 (tardis) ──┘                        ├──► Phase 5 (DataOps)
                                            │
Phase 4 (Databento) ────────────────────────┘
                                              └──► Phase 6 (Skills)
```

Phases 3 and 4 are independent of each other and can be parallelized. Phase 5 depends on
Phases 2-4 being complete. Phase 6 depends on Phase 5.

## Test Strategy

| Layer | Test Approach | Example |
|-------|--------------|---------|
| dlt resources | Assert yielded dict shape; test incremental state | `test_klines_resource_yields_dicts` |
| SQLMesh models | Assert model output matches expected Silver schema | `test_silver_klines_transform` |
| SQLMesh audits | Inject bad data, verify audit catches it | `test_not_null_price_audit_fails` |
| Exchange clients | Fake implementations returning known data | `FakeTardisDevClient` |
| Prefect flows | `@flow` tests with in-memory DuckDB | `test_dlt_source_flow` |
| CLI | Typer CliRunner with monkeypatched workflows | Existing pattern |
| Integration | Real API calls gated by `@pytest.mark.integration` | Existing pattern |

## Rollback Plan

Each phase is independently reversible:

1. **dlt**: Remove dlt dependencies, delete `dlt_sources/` module. Existing workflows unchanged.
2. **SQLMesh**: Remove SQLMesh dependency, delete `models/` directory. SinkWorkflow still works.
3. **tardis.dev/Databento**: Remove optional dependencies. ExchangeClient protocol still exists.
4. **DataOps**: SLA monitors are Prefect flows — undeploy them.
5. **Skills**: Remove `skills/` directory and CLI entry point.

## Risks and Mitigations

| Risk | Phase | Mitigation |
|------|-------|------------|
| dlt rest_api_source doesn't support Binance array responses | 1 | Use custom `@dlt.resource` wrapper instead of generic source |
| SQLMesh model syntax learning curve | 2 | Keep SinkWorkflow as parallel path; migrate one model at a time |
| tardis.dev schema drift | 3 | SQLMesh audits catch column type mismatches; virtual envs safe-iterate |
| Databento SDK API changes | 4 | Pin SDK version; write adapter layer to isolate SDK changes |
| Dependency conflicts (dlt+sqlmesh+prefect) | 1-2 | Pin all major versions; test matrix in CI |
| Performance regression from dlt normalization | 1 | Use `dlt.config` to disable normalization for already-normalized data |
