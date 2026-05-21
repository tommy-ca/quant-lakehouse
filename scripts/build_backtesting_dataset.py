"""Production-ready script to build a backtesting dataset for the Top 50 universe.

Orchestrates:
1. Metadata Synchronization (registry.instruments)
2. Daily Universe Stats Maintenance (gold.daily_universe_stats)
3. Top 50 Universe Discovery (Institutional Filters)
4. Bulk Ingestion (Extract -> Normalize -> Load -> Transform)
5. Health Validation
"""

import argparse
import logging
from datetime import UTC, datetime
from pathlib import Path

from binance_datatool.workflow.prefect_flows import backtesting_data_product_flow


def main():
    parser = argparse.ArgumentParser(description="Build Backtesting Data Product")
    parser.add_argument("--lake-path", type=str, default="./lake", help="Path to DuckLake house")
    parser.add_argument("--lookback-days", type=int, default=30, help="Days of history to ingest")
    parser.add_argument("--top-n", type=int, default=50, help="Universe size (default 50)")
    parser.add_argument(
        "--trade-types", nargs="+", default=["spot", "um", "cm"], help="Trade types to include"
    )
    parser.add_argument(
        "--data-types", nargs="+", default=["klines", "aggTrades"], help="Data types to include"
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("DataProductBuilder")

    lp = Path(args.lake_path).resolve()
    logger.info(f"Building data product at {lp} with {args.lookback_days} days lookback")

    # Execute the Prefect flow (which can also be run locally)
    report = backtesting_data_product_flow(
        trade_types=args.trade_types,
        data_types=args.data_types,
        lookback_days=args.lookback_days,
        lake_path=str(lp),
        top_n=args.top_n,
    )

    # Print summary report
    print("\n" + "=" * 50)
    print("BACKTESTING DATA PRODUCT SUMMARY")
    print("=" * 50)
    manifest = {
        "build_at": datetime.now(UTC).isoformat(),
        "parameters": {
            "lookback_days": args.lookback_days,
            "top_n": args.top_n,
            "trade_types": args.trade_types,
            "data_types": args.data_types,
        },
        "venues": {},
    }

    for tt, tt_res in report.items():
        count = tt_res.get("symbols_count", 0)
        print(f"Venue: {tt.upper()} ({count} symbols)")
        manifest["venues"][tt] = {"symbols_count": count, "data_types": {}}
        for dt, dt_res in tt_res.get("results", {}).items():
            healthy_count = sum(1 for r in dt_res.values() if r.get("healthy", False))
            print(f"  - {dt}: {healthy_count}/{count} healthy")
            manifest["venues"][tt]["data_types"][dt] = {
                "healthy_count": healthy_count,
                "total_count": count,
            }
    print("=" * 50)

    # Save manifest
    import json

    manifest_path = lp / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    logger.info(f"Build manifest saved to {manifest_path}")


if __name__ == "__main__":
    main()
