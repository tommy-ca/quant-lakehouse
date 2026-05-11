Spec-driven Development & TDD Guide
=================================

Purpose
-------

This document codifies how to design, implement, and test new features for the
project using specification-driven development and TDD. It contains templates
for specs, test guidance, agent/skill guidance, and an audit checklist.

1. Spec Template
-----------------

Every new public behaviour (workflow, CLI command, adapter, storage
integration) must start with a short spec file. The spec is an executable
contract: it lists inputs, outputs, side effects, success criteria, error
modes, and minimal acceptance tests. Put a one-paragraph spec in
`docs/specs-driven-development.md` (or link the PR to an inline spec).

Spec example (minimal):

- Title: `ArchiveListDatesWorkflow` (example)
- Purpose: List available date partitions for a symbol from a source.
- Inputs: `source: DataSource`, `market_type: MarketType`, `data_type: DataType`, `symbol: str`
- Outputs: `list[str]` sorted ascending; empty list if none
- Side-effects: None
- Success: Returns ALL partition directories for the symbol, in ascending date order
- Errors: Raise `ValueError` for invalid inputs; propagate HTTP errors from adapter
- Tests: Unit test with `FakeAdapter` that returns sample XML; integration test hitting real S3 listing

2. TDD Workflow
---------------

1. Write the spec entry.
2. Write a failing unit test using provided fakes (e.g. FakeArchiveClient / FakeAdapter).
3. Implement the minimum code to satisfy the test.
4. Run unit tests and lint; iterate until green.
5. Add integrations if the feature touches I/O (guarded by `@pytest.mark.integration`).
6. Add documentation and update the spec with any edge cases discovered during testing.

3. Tests & Fixtures (concrete rules)
-----------------------------------

- Use `FakeArchiveClient` for archive-based workflows.
- Use dependency injection for adapters and storage backends to enable easy
  fakes and deterministic tests.
- Keep tests deterministic: avoid wall-clock dependent assertions.
- Use `tmp_path` fixtures for filesystem side-effect assertions.

4. Subagent & Skill Development
------------------------------

> The `skills/` module does not exist yet. See `docs/skills-subagents.md` for the aspirational design.

When building skills or subagents:
- Keep each skill focused: one skill → one operation (list, download, verify)
- Each skill must declare:
  - Input schema (JSON schema or procedural signature)
  - Output schema (structured JSON)
  - Failure modes and remediation steps
- Add unit tests for skills that mock the CLI and assert expected outputs.

5. Audit Checklist (pre-merge)
-----------------------------

Before merging a change that modifies behaviour, perform the following audit:

- Tests:
  - Unit tests added and passing
  - Integration tests updated or added where appropriate
- Docs:
  - Spec added/updated in this document or linked PR
  - CLI help updated when command semantics change
- Design:
  - Does the change follow SOLID? (Single responsibility, depends on abstractions)
  - KISS: is the change as small/simple as possible?
  - DRY: are there duplicated behaviours that should be shared?
  - YAGNI: avoid adding unused features or hooks
- Security / Secrets:
  - No hard-coded credentials
  - Credentials read from environment / secured config

6. Example: Adding a New Source Adapter (TDD Steps)
--------------------------------------------------

1. Spec: Add an entry describing the adapter's responsibilities (list_symbols,
   list_files, fetch_file). Include error handling behaviour.
2. Tests: Write unit tests using a `FakeHTTPClient` that simulate the source's
   listing responses.
3. Implementation: Add a `DataSourceAdapter` implementation that maps the
   source's semantics to `FileMetadata` and `SymbolMetadata`.
4. Integration: Add `@pytest.mark.integration` tests that hit the real API if
   allowed. Otherwise, capture a recorded fixture and run the test against it.

7. Skills & Subagents (Aspirational)
-----------------------------------

See `docs/skills-subagents.md` for the full skills/subagent design. None of the
following subagents are implemented yet.

For agent-driven flows, the design intent is:
- Subagents should be small stateful actors that own a single responsibility:
  - `discover_symbols` subagent: returns symbol lists
  - `download_partition` subagent: takes a symbol & partition and returns success
  - `verify_partition` subagent: verifies downloaded files
- Each subagent must provide a human-readable operation spec and unit tests
  using faked backends so agents can be validated locally.

8. Example Spec-driven Unit Test Template
----------------------------------------

```py
def test_list_files_preserves_order(fake_adapter):
    # Arrange
    fake_adapter.list_symbols_return = ["AAA", "BBB"]
    workflow = ArchiveListFilesWorkflow(client=fake_adapter, symbols=["AAA", "BBB"])

    # Act
    result = asyncio.run(workflow.run())

    # Assert
    assert [entry.symbol for entry in result.per_symbol] == ["AAA", "BBB"]
```

## Specs

### `check_ducklake_anomalies` (health_check.py)
- **Purpose**: Query DuckLake native tables for data quality anomalies (null/zero prices, duplicate timestamps, date gaps, price outliers).
- **Inputs**: DuckDB connection, table_name (sanitized), symbol (parameterized), outlier_std threshold.
- **Outputs**: `AnomalyReport` dataclass with counts for each anomaly type + `is_clean` property.
- **Side-effects**: None (read-only queries).
- **Success**: Returns accurate counts; `is_clean` is `True` only when ALL anomaly counts are zero.
- **Errors**: Outlier query failures are caught and logged as warnings (not propagated); other queries raise on DuckDB errors.
- **Tests**: Unit test with in-memory DuckDB table; edge cases: empty table, clean data, all-null prices, duplicate timestamps, missing dates, STDDEV=0.

### `_sanitize_identifier` (health_check.py)
- **Purpose**: Strip non-alphanumeric characters from DuckDB identifiers for safe f-string interpolation.
- **Inputs**: `name: str`
- **Outputs**: Sanitized `str` containing only `[a-zA-Z0-9_]`.
- **Tests**: Verify special chars removed, alphanumeric preserved, empty input.

### `_parse_symbol_from_path` (sink.py)
- **Purpose**: Extract symbol from Binance archive filename using `startswith()` pattern.
- **Inputs**: `path: Path`, `known_symbols: Sequence[str]`
- **Outputs**: Matching symbol string or `None`.
- **Side-effects**: None.
- **Success**: Returns correct symbol when filename starts with `{sym}-`; returns `None` for no match.
- **Verification**: Substring matching bug fixed: "BTC" should NOT match "BTCUSDT-1h-2024.zip".
- **Tests**: Exact match, prefix-only match (bug regression), no match, empty symbols list.

### `AnomalyReport` (health_check.py)
- **Purpose**: Data class aggregating all anomaly counts from DuckLake checks.
- **Properties**: `null_prices`, `zero_volumes`, `duplicate_timestamps`, `date_gaps`, `outlier_rows`, `is_clean`.
- **Tests**: `is_clean` returns `True` when all counts zero, `False` when any non-zero.

9. Concluding Notes
-------------------

Keep specs short, executable, and co-located with the code change when possible.
Prioritize clarity over cleverness. This ensures agents, humans, and automated
checks can reason about behaviour precisely.
