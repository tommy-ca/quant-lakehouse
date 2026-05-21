# Formal Requirements & Specification Document

> **Note (2026-05-21):** The system has achieved full **Medallion-Native Lakehouse** maturity.
> All layers (Registry, Bronze, Silver, Gold) are natively managed by the DuckLake extension.
> Brittle Hive directory management and manual view mapping have been eliminated in
> favor of ACID-compliant catalog metadata and native partitioning (`symbol`, `ts_date`).
> The platform is now fully integrated with DVC and Hugging Face Hub for
> immutable, zero-copy dataset distribution.

## 1. Project Overview

**Project**: `binance-datatool` — A multi-source cryptocurrency market data engineering platform
**Current Scope**: Binance (Spot, UM, CM) with industry-standard DBN/Tardis alignment.
**Target Scope**: Multi-exchange support with full MLOps/DataOps lifecycle automation.
**Principles**: SOLID, KISS, DRY, YAGNI, TDD, Spec-Driven Development.

---

## 2. Functional Requirements
...
| FR-10: Institutional Universe | Dynamically build top-N tradable universes with rigorous liquidity, age, institutional risk screening, and point-in-time backtesting support. | HIGH | ✅ Implemented |
| **FR-11: Point-in-Time Metrics** | Eliminate survivorship and look-ahead bias by materializing historical daily statistics in the Gold layer and providing deterministic SQL access for backtesting. | HIGH | ✅ Implemented |
| **FR-12: Configurable Universe Parameters** | Support environment-level configuration for scoring weights (volume vs mcap), market cap multipliers, and strict liquidity/age floors. | MEDIUM | ✅ Implemented |
| **FR-13: Robust Rate Normalization** | Comprehensive `RateProvider` with dynamic Lakehouse lookup and multi-fiat/stable fallback (Frankfurter API) for accurate USD-denominated filtering. | MEDIUM | ✅ Implemented |
| **FR-14: Backtesting Data Product** | Unified orchestration flow to generate a consistent, validated dataset for a curated universe (symbols + klines + trades + funding). | HIGH | ✅ Implemented |
| **FR-15: Data Reproducibility (DVC)** | Integrate DVC to track and version the Lakehouse directory as an immutable artifact. | HIGH | ✅ Implemented |
| **FR-16: Dual-Layer Validation** | Enforce per-record (Pydantic) and per-DataFrame (Pandera) validation to ensure total data integrity. | HIGH | ✅ Implemented |
| **FR-17: High-Fidelity Metadata** | Standardized `venues` and `instruments` registries aligned with Databento (DBN) and Tardis.dev schemas for industry-standard discovery. | HIGH | ✅ Implemented |
| **FR-18: Medallion-Native Lakehouse** | Native DuckLake management of all data tiers with automatic partition pruning and zero-copy portability. | HIGH | ✅ Implemented |

---

## 10. Next Steps & Roadmap

### Phase 45: Medallion-Native Lakehouse & HF Publishing (2026-05-21)

**Goal**: Deliver an institutional-grade, standard-compliant data product ecosystem.

**Achievements**:
- ✅ **Native DuckLake Transition**: Refactored Silver and Gold layers to use native DDL
  (`ALTER TABLE SET PARTITIONED BY`), eliminating manual directory management.
- ✅ **Dual-Column Partitioning**: Optimized Silver tables for `(symbol, ts_date)` to
  support both asset-level and cross-sectional pruning.
- ✅ **High-Fidelity Standard**: Aligned metadata with DBN (`publisher_id`) and
  Tardis (`exchange_slug`) for seamless institutional interoperability.
- ✅ **Hugging Face Hub Delivery**: Implemented automated publishing of Medallion-Native
  artifacts to the Hub, including `manifest.json` and `dvc.lock`.
- ✅ **Resilient Rate Normalization**: Integrated Frankfurter FX API to handle USD
  normalization for all fiat/stablecoin pairs historically.
- ✅ **Zero-Copy SDK Readiness**: Verified that the Lakehouse can be attached directly
  from Hugging Face via DuckDB for instant quantitative research.


**Current baseline**: 325 unit tests passing, 24 E2E integration scenarios passing.
100% schema compliance for all published Medallion tiers.

---

**Document Version**: 3.0
**Last Updated**: 2026-05-21
**Maintainer**: Team
**Status**: Production-ready. Gold-standard platform for institutional crypto research.
