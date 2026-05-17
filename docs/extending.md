# Extending the Project

This guide explains how to add new functionality to `binance-datatool` following the patterns
established in the current codebase.

## Adding a New Enum Member

To support a new dataset type (for example, `"premiumIndex"`):

1. Add the member to the appropriate enum in `common/enums.py`:

   ```python
   class DataType(StrEnum):
       ...
       premium_index = "premiumIndex"
   ```

2. Review whether the new member needs additional enum-property updates. The new member
   automatically becomes available in:
   - CLI arguments (Typer picks up `StrEnum` members).
   - S3 path construction (the value is used directly).

3. If the new data type uses an interval directory layer, update
   `DataType.has_interval_layer` and add coverage for the interval validation
   paths exercised by the CLI, workflow, and archive client.

If the new member requires a non-trivial path mapping (like `TradeType.s3_path`), add a property
method to the enum class.

## Adding a New CLI Command

Follow the three-layer pattern: **CLI → Workflow → Client**.

### Step 1: Add a Client Method

If the command needs new data access, add an `async` method to `ArchiveClient` in
`archive/client.py`. Keep the method focused on S3 communication and return plain data
structures.

```python
async def list_dates(self, trade_type: TradeType, ...) -> list[str]:
    """List available date directories for a symbol."""
    prefix = _build_prefix(trade_type, data_freq, data_type) + f"{symbol}/"
    ...
```

See `list_symbols` and `list_symbol_files` in `archive/client.py` for real examples.

### Step 2: Create a Workflow Class

Create a workflow in `workflow/` (or a new module if the scope warrants it).
Accept an optional `client` parameter for testability and return a typed result dataclass.

```python
class ArchiveListDatesWorkflow:
    def __init__(self, trade_type: TradeType, ..., client: ArchiveClient | None = None) -> None:
        ...

    async def run(self) -> list[str]:
        ...
```

See `ArchiveListSymbolsWorkflow`, `ArchiveListFilesWorkflow`, `ArchiveDownloadWorkflow`,
and `ArchiveVerifyWorkflow` in `workflow/` for real examples.

### Step 3: Add a CLI Command

Add a Typer root command in `cli/archive.py`. The command parses arguments,
constructs a workflow, and prints the result.

```python
@app.command("list-dates")
def list_dates_command(
    trade_type: Annotated[TradeType, typer.Argument(...)],
    ...
) -> None:
    """List available dates for a symbol."""
    workflow = ArchiveListDatesWorkflow(trade_type, ...)
    for date in asyncio.run(workflow.run()):
        typer.echo(date)
```

See `list_symbols_command`, `list_files_command`, `download_command`, and
`verify_command` in `cli/archive.py` for real examples.

### Step 4: Add Tests

Add tests at each layer. See [Test Organization](reference/testing.md) for directory layout,
conventions, and shared fixtures.

## Adding a New Data Type (Full Pipeline)

To add a new data type (e.g. `"trades"`, `"bookDepth"`) end-to-end
through the dlt + Polars + Pandera pipeline:

### Step 1: Add a Pydantic Model

Add authoritative and raw models to `dlt/models.py`:

```python
class TradeModel(BaseModel):
    dlt_config: ClassVar[DltConfig] = {"is_authoritative_model": True}
    trade_id: int
    price: float
    quantity: float
    transact_time: int
    symbol: str

class RawTradeModel(BaseModel):
    dlt_config: ClassVar[DltConfig] = {"is_authoritative_model": True}
    trade_id: str
    price: str
    quantity: str
    transact_time: str
    symbol: str
```

### Step 2: Create a dlt Resource

Create a new module in `dlt/resources/` (e.g. `binance_trades.py`):

```python
@dlt.resource(name="trades", write_disposition="merge",
              primary_key=("symbol", "trade_id"),
              columns=RawTradeModel)
def trades_resource(symbol: str, trade_type: TradeType, ...) -> list[dict]:
    client = client_for(trade_type)
    raw = asyncio.run(client.fetch_trades(symbol, ...))
    return [{"trade_id": str(t.id), "price": str(t.p), ...} for t in raw]
```

Register the resource in `dlt/__init__.py` and `dlt_sources/__init__.py`.
Create a forwarding module in `dlt_sources/` for backward compat.

### Step 3: Add Bronze and Silver Pandera Schemas

Add schemas to `validation/schemas.py`:

```python
class BronzeTradesSchema(pa.DataFrameModel):
    class Config: coerce = True; strict = True
    trade_id: str = pa.Field(nullable=False)
    ...

class SilverTradesSchema(pa.DataFrameModel):
    class Config: coerce = True; strict = True
    ts_event: int = pa.Field(ge=0, nullable=False)
    ...
    ts_date: pl.Date = pa.Field(nullable=False)
```

Add validation helpers: `validate_silver_trades()`.

### Step 4: Write a Polars Transform

Create a new module in `transforms/`:

```python
def bronze_trades_to_silver(df: pl.DataFrame, *, symbol: str,
                            trade_type: str, source: str, validate: bool = True):
    result = df.with_columns([...]).select([...])
    if validate:
        validate_silver_trades(result)
    return result
```

### Step 5: Wire into Prefect Tasks

Add transformation dispatch to `workflow/prefect_tasks/transform.py`:

```python
if data_type == "trades":
    return bronze_trades_to_silver(df, symbol=symbol, ...)
```

### Step 6: Add Tests

Add at least:
- Unit test for the dlt resource (mock exchange client)
- Unit test for the Polars transform (fixture DataFrame)
- Schema validation test (Pandera)
- E2E integration test in `tests/test_e2e_correctness.py`

## Adding a New dlt Resource

1. Create a module in `dlt/resources/` using `client_for()` from `_client.py`
   for REST API resources.
2. Import and register in `dlt/__init__.py`.
3. Create a forwarding module in `dlt_sources/` if backward compatibility is needed.
4. Add tests mocking the exchange client.

See `dlt/resources/binance_klines.py` and `dlt/resources/binance_agg_trades.py`
for real examples.

## Adding a New Prefect Flow or Task

1. Add business logic to `workflow/prefect_tasks/extract.py` or
   `workflow/prefect_tasks/transform.py` as plain importable functions.
2. Create thin `@flow` or `@task` decorators in `workflow/prefect_flows.py`
   that delegate to the functions in `prefect_tasks/`.
3. Register new deployments in the `serve()` block at the bottom of
   `prefect_flows.py`.

See `workflow/prefect_tasks/extract.py` → `run_dlt_pipeline()` for a real example.

## Adding a New Pandera Schema

The validation layer has two enforcement points (see AGENTS.md for the full table):

1. **dlt Pydantic models** (`dlt/models.py`) — per-record validation at ingest.
   Used as `columns=RawKlineModel` in `@dlt.resource` decorators.
2. **Pandera schemas** (`validation/schemas.py`) — per-DataFrame validation at
   the Polars transform boundary. Wire via `validate_silver_*()` helpers.

To add a new Pandera schema:

1. Add the schema class to `validation/schemas.py` using `pa.DataFrameModel`.
2. Add a `validate_*()` helper function that wraps `Schema.validate(df, lazy=True)`.
3. Wire the helper into the transform function for that data type.
4. Ensure `ts_date` uses `pl.Date = pa.Field(nullable=False)` (not `object`).
5. For cross-column checks (e.g. `high >= low`), add a helper using
   `check_high_gte_low()` and call it in the validate function.

See `BronzeKlinesSchema`, `SilverKlinesSchema`, `validate_silver_klines()`,
and `check_high_gte_low()` for real examples.

## Adding a New SQLMesh Model

1. Create a SQL file in `models/bronze/` or `models/silver/` at the project root.
2. Use `INCREMENTAL_BY_TIME_RANGE` for time-partitioned models.
3. Run via `uv run sqlmesh plan` (optional — Polars transforms remain the primary path).

See `models/bronze/klines.sql` and `models/silver/klines.sql` for real examples.

## Adding a New CLI Command

Follow the Prefect-based pattern for data pipeline commands
or the legacy three-layer pattern for archive commands.

### For Data Pipeline Commands (gap-fill, health, sink, refresh-metadata)

Add a Typer command in `cli/archive.py` that delegates to a workflow class
in `workflow/`. Example:

```python
@app.command("gap-fill")
def gap_fill_command(trade_type, symbol, ...):
    """Backfill missing data via REST API."""
    wf = GapFillWorkflow(trade_type=TradeType(trade_type), ...)
    result = wf.run()
    typer.echo(json.dumps(result))
```

### For Archive Commands (list-symbols, list-files, download, verify)

Follow the legacy pattern: **CLI → Workflow → Client**.
See `cli/archive.py` → `workflow/download.py` → `archive/client.py` for examples.

## Adding a New Sub-command Group

To add a command group alongside the current root data commands
(for example, `binance-datatool holo ...`):

1. Define a new Typer app in `cli/__init__.py`:

   ```python
   holo_app = typer.Typer(name="holo", help="Holographic kline generation.")
   app.add_typer(holo_app)
   ```

2. Create the command module `cli/holo.py` and register it with a side-effect import in
   `cli/__init__.py`:

   ```python
   # Register command modules (side-effect import).
   from binance_datatool.cli import archive as _archive  # noqa: F401,E402
   from binance_datatool.cli import holo as _holo  # noqa: F401,E402
   ```

3. Follow the same CLI → Workflow → Client layering for the new commands.

## Test Organization

For the test directory layout, conventions, and shared fixtures, see
[Test Organization](reference/testing.md).

### Test Categories

| Test Type | Directory | Example |
|-----------|-----------|---------|
| dlt resource tests | `tests/test_dlt_sources.py` | Mock exchange client, assert resource output |
| Transform tests | `tests/test_transforms.py` | Fixture DataFrames, assert silver column mapping |
| Validation tests | `tests/test_validation.py` | Assert Pandera schema enforcement |
| Archive index tests | `tests/test_bronze_archive_index.py` | Assert path parsing and file discovery |
| E2E correctness | `tests/test_e2e_correctness.py` | Full raw→bronze→silver pipeline (requires `--run-integration`) |
| Workflow tests | `tests/test_*.py` | Mock archive clients, assert workflow outcomes |
| CLI tests | `tests/test_cli_*.py` | Typer test runner, assert stdout |
