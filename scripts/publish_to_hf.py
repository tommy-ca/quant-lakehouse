import os
from pathlib import Path

from huggingface_hub import HfApi, create_repo
from loguru import logger


def publish_to_huggingface(repo_id: str, lake_path: str = "./lake"):
    """
    Publish the Lakehouse dataset to Hugging Face Hub using native HfApi.

    Excludes redundant staging and metadata directories:
    - .dlt/ (Transient dlt state)
    - *_staging/ (Intermediate dlt normalization folders)
    """
    token = os.getenv("HF_TOKEN")
    if not token:
        logger.warning(
            "HF_TOKEN not found in environment. In-shell authentication might be required."
        )

    lp = Path(lake_path).resolve()
    if not lp.exists():
        raise FileNotFoundError(f"Lakehouse path {lp} does not exist.")

    api = HfApi()

    # 1. Ensure the repository exists
    try:
        create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True, token=token)
        logger.info(f"Verified Hugging Face dataset repository: {repo_id}")
    except Exception as e:
        logger.error(f"Failed to create/verify repository: {e}")
        return

    # 2. Upload the Lakehouse (Surgical Medallion Upload)
    logger.info(f"Uploading {lp} (cleaned medallion layers) to Hugging Face...")
    try:
        # We only upload the four formal Medallion/Registry layers
        formal_dirs = ["registry", "bronze", "silver", "gold"]
        for folder in formal_dirs:
            folder_path = lp / folder
            if folder_path.exists():
                logger.info(f"Uploading formal layer: {folder}...")
                api.upload_folder(
                    folder_path=str(folder_path),
                    path_in_repo=f"lake/{folder}",
                    repo_id=repo_id,
                    repo_type="dataset",
                    token=token,
                    allow_patterns=["**/*.parquet"],
                )

        # Upload minimal root metadata files
        root_files = ["catalog.duckdb", "metadata.duckdb", "manifest.json"]
        for rf in root_files:
            rf_path = lp / rf
            if rf_path.exists():
                logger.info(f"Uploading root artifact: {rf}...")
                api.upload_file(
                    path_or_fileobj=str(rf_path),
                    path_in_repo=f"lake/{rf}",
                    repo_id=repo_id,
                    repo_type="dataset",
                    token=token,
                )

        # Upload DVC/Workflow manifests
        manifests = ["dvc.lock", "dvc.yaml", "pyproject.toml"]
        for m in manifests:
            m_path = Path(m)
            if m_path.exists():
                api.upload_file(
                    path_or_fileobj=str(m_path),
                    path_in_repo=m,
                    repo_id=repo_id,
                    repo_type="dataset",
                    token=token,
                )

        # Create a simple README.md on HF if not exists
        manifest_path = lp / "manifest.json"
        if manifest_path.exists():
            import json

            with open(manifest_path) as f:
                manifest = json.load(f)

            readme_content = f"""---
license: bsd-3-clause
task_categories:
- robotics
- other
language:
- en
tags:
- finance
- crypto
- backtesting
---
# Binance Top 50 Backtesting Dataset

Built at: {manifest["build_at"]}

## Parameters
- Lookback: {manifest["parameters"]["lookback_days"]} days
- Top N: {manifest["parameters"]["top_n"]}
- Trade Types: {", ".join(manifest["parameters"]["trade_types"])}
- Data Types: {", ".join(manifest["parameters"]["data_types"])}

## Build Status
"""
            for venue, vdata in manifest["venues"].items():
                readme_content += f"- **{venue.upper()}**: {vdata['symbols_count']} symbols\n"
                for dt, dt_data in vdata["data_types"].items():
                    readme_content += (
                        f"  - {dt}: {dt_data['healthy_count']}/{dt_data['total_count']} healthy\n"
                    )

            with open("HF_README.md", "w") as f:
                f.write(readme_content)

            api.upload_file(
                path_or_fileobj="HF_README.md",
                path_in_repo="README.md",
                repo_id=repo_id,
                repo_type="dataset",
                token=token,
            )
            os.remove("HF_README.md")
            logger.info("Published Dataset Card (README.md)")

        logger.success(
            f"Dataset successfully published to https://huggingface.co/datasets/{repo_id}"
        )
    except Exception as e:
        logger.error(f"Upload failed: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repo-id", type=str, required=True, help="HF Repo ID (e.g. 'org/dataset')"
    )
    parser.add_argument("--lake-path", type=str, default="./lake")
    args = parser.parse_args()

    publish_to_huggingface(args.repo_id, args.lake_path)
