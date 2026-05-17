Repository Audit — binance-datatool
=================================

This audit summarises findings after reviewing the repository and the
spec-driven design changes added to the docs. It highlights risks, gaps,
and recommended next steps prioritized for immediate action. The goal is to
help the team move from a stable single-source tool to a generalized,
testable multi-source pipeline framework while following SOLID, KISS, DRY,
and YAGNI.

Summary of Findings
-------------------

1) Code Quality & Design (Low Risk)

- The repository already follows a clear layered design (CLI → Workflow →
  Archive → Common). Code is small, well-factored, and test-friendly.
- Logging and progress reporting have careful stderr/stdout separation.

2) Test Coverage & TDD (Medium Risk)

- Good unit test coverage for core behaviours (symbol parsing, archive
  listing, checksum verification). `conftest.py` offers useful fakes.
- Integration tests are gated behind `--run-integration` which is correct.
- There is no currently checked-in adapter abstraction; adding the
  `adapter` protocol and `SourceRegistry` scaffolding (new files) helps with
  spec-driven extension.

3) Extensibility & Multi-Source (Medium-High Risk)

- The code is tightly focused on data.binance.vision S3 semantics. There is
  no adapter registry or explicit `DataContract` implementation yet in
  the runtime code (only in docs). We added a small adapter protocol and
  registry to help incremental migration without breaking existing code.
- Archive-specific assumptions are spread across `archive/client.py` and
  `workflow/` (prefix building, kline interval rules). These should be
  consolidated into a SourceAdapter during migration to avoid duplication.

- Update (2026-05-16): a minimal `DataSourceAdapter` protocol and a `SourceRegistry`
  singleton have been added at `src/binance_datatool/adapter/`. The `BinanceAdapter`
  wraps `ArchiveClient` and preserves the existing behaviour while providing a
  small extension point for other sources. This change is intentionally narrow
  to minimize risk; next steps include consolidating prefix and interval logic
  into the adapter surface.

4) Operational Concerns (High Risk)

- The aria2 download approach is pragmatic but requires aria2 on the host.
  When moving to cloud runs or worker clusters, switch to a storage-native
  downloader (S3 range GET or boto3 multi-threaded copy) or containerized
  aria2 with a managed process supervisor.
- No metrics or lineage collector exists yet. The spec recommends adding
  LineageTracker and MetricsCollector; these are required for production
  observability and DataOps compliance.

5) Security (Low Risk)

- No obvious secrets checked into the repo. Archive access is public. For
  sources that require auth ensure credentials are read from environment
  or secret managers.

Recommended Actions (Prioritised)
---------------------------------

1. Core Abstractions (High Priority)

- Add a small `DataSourceAdapter` protocol (done) and a `SourceRegistry`
  (done). Migrate `ArchiveClient` usage behind a BinanceAdapter that
  implements the protocol. This is required to support other sources.

2. Data Contracts & Validation (High Priority)

- Implement a lean `DataContract` class in code (not just docs) with schema
  and validation hooks. Use contract validation in the pipeline `VerifyTask`.

3. Lineage & Metrics (High Priority)

- Implement `LineageTracker` and `MetricsCollector`. Start by emitting
  basic metrics to logs, then integrate Prometheus/Grafana for production.

4. Replace host-bound downloader or provide cloud-friendly option
(Medium Priority)

- Provide an optional S3 / storage-native downloader to run in container
  environments without aria2 installed. Keep aria2 as an advanced mode.

5. CI & Integration Tests (Medium Priority)

- Add a CI job that can run integration tests against controlled fixtures
  or an emulated S3 to ensure compatibility without hitting the public API
  in every change.

6. Skills / Subagents (Low-Medium Priority)

- Formalize skill specs in `skills/` and add tests that mock CLI output.
- Provide small subagent templates: `discover_symbols`, `download_partition`,
  `verify_partition`. Each should have a spec and unit tests using fakes.

Technical Debt & Risks
----------------------

- The current archive prefix logic appears in several places. When you
  implement adapters, centralize prefix construction or remove it from the
  core workflow.
- The existing marker protocol in `symbol_dir` is clever and robust, but the
  timestamping scheme is ad-hoc — document it and include tests for
  cross-platform time precision.

Audit Checklist for PRs
----------------------

Before merging these kinds of changes, ensure:

- Spec is updated in `docs/specs-driven-development.md`.
- Unit tests added and green locally.
- Integration tests added or updated (and gated).
- CI includes linting and unit tests.
- No hard-coded secrets or system changes (e.g. no altering of global git
  config in scripts).

Next Concrete Implementation Steps
---------------------------------

1. Create `binance_datatool.adapters.binance` with a `BinanceAdapter` that
   wraps `ArchiveClient` and implements `DataSourceAdapter`.
2. Implement a minimal `DataContract` object in code and use it in
   `ArchiveVerifyWorkflow` to decouple verification from symbol_dir logic.
3. Add `LineageTracker` and `MetricsCollector` interfaces and emit simple
   logs for each pipeline run as a first step.

Appendix — Quick File References
-------------------------------

- CLI layer: `src/binance_datatool/cli/archive.py`
- Workflow layer: `src/binance_datatool/workflow/*.py`
- Archive client: `src/binance_datatool/archive/client.py`
- Downloader: `src/binance_datatool/archive/downloader.py`
- Symbol dir & markers: `src/binance_datatool/archive/symbol_dir.py`
- Tests & fixtures: `tests/conftest.py`
