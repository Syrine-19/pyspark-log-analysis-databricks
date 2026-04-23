"""
main.py
-------
Entrypoint for the Big Data Log Analysis pipeline.
Can be submitted as a Databricks job or run locally.

Usage (local):
    python src/main.py --input data/logs.csv --output data/results/

Usage (Databricks job):
    Set LOG_INPUT_PATH and LOG_OUTPUT_PATH as job parameters.
"""

import argparse
import logging
import time
from pathlib import Path

from ingestion        import get_spark, load_logs
from processing       import run_processing
from anomaly_detection import run_anomaly_detection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")


def save_results(results: dict, output_dir: str) -> None:
    """Write each result DataFrame to Parquet in output_dir."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    for name, df in results.items():
        path = f"{output_dir}/{name}.parquet"
        df.write.mode("overwrite").parquet(path)
        print(f"[main] Saved '{name}' → {path}")


def main(input_path: str, output_dir: str) -> None:
    t0    = time.time()
    spark = get_spark("BigDataLogAnalysis")

    # ── Stage 1: Ingestion ──────────────────────────────────────────────────
    print("\n" + "─" * 60)
    print(" STAGE 1 — INGESTION")
    print("─" * 60)
    df = load_logs(spark, input_path)

    # ── Stage 2: Processing ─────────────────────────────────────────────────
    print("\n" + "─" * 60)
    print(" STAGE 2 — DISTRIBUTED PROCESSING")
    print("─" * 60)
    processing_results = run_processing(df)

    # ── Stage 3: Anomaly Detection ──────────────────────────────────────────
    print("\n" + "─" * 60)
    print(" STAGE 3 — ANOMALY DETECTION")
    print("─" * 60)
    # Re-enrich base df for anomaly detection (enrichment adds is_error, is_slow)
    from processing import enrich, repartition_by_service
    df_enriched = repartition_by_service(enrich(df))
    anomaly_results = run_anomaly_detection(df_enriched)

    # ── Stage 4: Persist ────────────────────────────────────────────────────
    print("\n" + "─" * 60)
    print(" STAGE 4 — WRITING RESULTS")
    print("─" * 60)
    all_results = {**processing_results, **anomaly_results}
    save_results(all_results, output_dir)

    elapsed = time.time() - t0
    print(f"\n[main] Pipeline complete in {elapsed:.1f}s")
    logger.info("Total wall-clock time: %.1fs", elapsed)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Big Data Log Analysis Pipeline")
    parser.add_argument("--input",  default="data/logs.csv",    help="Input log path")
    parser.add_argument("--output", default="data/results",     help="Output directory")
    args = parser.parse_args()
    main(args.input, args.output)
