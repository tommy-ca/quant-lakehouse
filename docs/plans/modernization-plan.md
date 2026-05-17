# Modernization Plan: Prefect → Hatchet/LangGraph

## 1. Executive Summary
Modernize `binance-datatool` orchestration by replacing Prefect flows with Hatchet (durable execution) and LangGraph (agentic state). Maintain the existing dlt/DuckLake data pipeline, Polars transforms, and Pandera validation.

## 2. Core Concepts Mapping
| Existing (Prefect) | Modern Target (Hatchet + LangGraph) |
|---|---|
| `@flow` (Orchestration) | **Hatchet Workflow** (Triggered, durable, retryable) |
| `@task` (Execution) | **Hatchet Task** (Scalable, isolated units) |
| `ThreadPoolTaskRunner` | **Hatchet Workers** (distributed) |
| Flow branching logic | **LangGraph State Graph** (agent-native transitions) |
| Scheduled `@deployment` | **Hatchet Schedules** |

## 3. Migration Roadmap

### Phase 1: Infrastructure Setup (Hatchet/LangGraph)
- Install Hatchet and LangGraph dependencies.
- Define shared state model (LangGraph `State`).
- Create Hatchet worker service to manage parallel tasks.

### Phase 2: Orchestration Refactor (Component by Component)
- **1. Metadata Refresh:** Migrate `refresh_metadata_flow` to a Hatchet workflow.
- **2. Pipeline Components:** Convert `extract_archive`, `transform_to_silver`, etc., to Hatchet tasks.
- **3. Graph Logic:** Implement the historical pipeline logic using LangGraph for state transitions (e.g., `Discover` -> `Prepare` -> `Transform` -> `Validate`).

### Phase 4: Verification & E2E
- Port existing `tests/test_e2e_correctness.py` to the new stack.
- Validate that the new orchestration matches the 14/14 E2E pass rate.
- Run parallel benchmarks vs. Prefect.

### Phase 5: Cleanup
- Remove Prefect dependencies.
- Archive `prefect_flows.py` and `prefect_tasks/`.

## 4. Key Design Considerations
- **Concurrency Guard**: Retain `ducklake-writer` serialization logic within Hatchet workers.
- **TDD/Spec-Driven**: Every migration component requires an equivalent spec and test case.
- **Resilience**: Utilize Hatchet's durable execution for automatic retries of failed pipeline stages.
- **State management**: Leverage LangGraph for monitoring progress of multi-step, multi-symbol backfills.

## 5. Next Steps
1. Create `docs/plans/modernization-plan.md` (this doc).
2. Configure Hatchet in `config.yaml`.
3. Spike: Create a simple LangGraph agent that triggers a single Hatchet `klines` download task.
