# Reproducibility & Dataset Versioning Specification

## 1. Overview
To ensure scientifically valid quant research, `binance-datatool` integrates **DVC (Data Version Control)** to build and publish immutable, reproducible datasets. This allows researchers to track exactly which version of the raw data and transformation code produced a specific backtesting result.

## 2. Reproducibility Stack

| Component | Role | Description |
| :--- | :--- | :--- |
| **DVC** | Data Orchestration | Tracks large Parquet files and DuckDB catalogs without bloating Git. |
| **SQLMesh** | Versioned Transforms | Ensures that schema changes are versioned and can be "time-traveled" via environments. |
| **Prefect** | Execution Control | Orchestrates the multi-threaded ingest and build tasks. |
| **Git** | Code Versioning | Pins the exact transformation logic (Polars/Python) to the data version. |

## 3. Dataset Publishing Workflow

The `scripts/build_backtesting_dataset.py` script is the primary entry point for a DVC-backed pipeline.

### Step 1: Ingest & Build
Run the production builder to populate the `./lake` directory.
```bash
uv run python scripts/build_backtesting_dataset.py --lake-path ./lake --lookback-days 30
```

### Step 2: Quality Check
The builder automatically runs the `HealthCheckWorkflow`. If data quality is <100%, the build is marked as "tainted" and cannot be published.

### Step 3: Freeze with DVC
Commit the state of the Lakehouse to DVC.
```bash
dvc add lake/
git add lake.dvc .gitignore
git commit -m "feat: publish Top 50 dataset v1.0.0"
```
### Step 4: Publish to Hugging Face
Upload the versioned Lakehouse, manifests (`dvc.lock`, `manifest.json`), and optimized `README.md`.

**Option A: Automated (Script)**
```bash
export HF_TOKEN="your_token"
uv run python scripts/publish_to_hf.py --repo-id "org/binance-top50-spot"
```

**Optimization: Disable Viewer & Parquet Conversion**
To ensure the Lakehouse structure is preserved, the `README.md` front matter MUST include:
```yaml
---
viewer: false # Disables UI viewer and automatic parquet bot
configs:
  - config_name: silver
    data_files: "lake/silver/**/*.parquet"
---
```

## 4. Manifests & Metadata Immutability
...
Every published dataset contains a `manifest.json` and `dvc.lock` providing:
- **Build Fingerprint**: Exact timestamps and parameters used for the build.
- **Data Lineage**: Provenance of raw S3 keys and API fetches (via dlt state).
- **Validation Report**: Summary of health checks and Pandera validation status.
- **Reproducibility**: The `dvc.lock` ensures that anyone with the repository can reproduce the exact environment.

## 5. Pipeline Definition (`dvc.yaml`)

```yaml
stages:
  build-dataset:
    cmd: uv run python scripts/build_backtesting_dataset.py --lake-path ./lake --lookback-days ${lookback}
    deps:
      - src/binance_datatool/
      - scripts/build_backtesting_dataset.py
    params:
      - lookback
      - top_n
    outs:
      - lake/
```

## 5. Metadata Immutability
Every published dataset includes a `lineage.json` file (exported from dlt state) that documents:
- The exact S3 file keys used.
- The timestamps of REST API fetches.
- The checksums of all source archives.
- The validation report from Pandera.
