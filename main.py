"""
Retail Batch ETL Pipeline

Batch processing system — data is processed in scheduled runs,
not in continuous real-time streaming.

Flow: CSV -> Extract -> Transform -> Incremental filter -> Load -> Report
"""

import logging
import os
import pandas as pd
from datetime import datetime

from extract import extract_csv
from transform import clean_transform_with_report, create_aggregations
from load import load_to_sqlite, export_summary_csv, export_data_quality_report
from generate_report import generate_html_report
import config


def setup_logging() -> None:
    os.makedirs(config.LOG_DIR, exist_ok=True)
    logging.basicConfig(
        filename=config.LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def read_last_processed_date(state_file: str):
    """Read incremental checkpoint date."""
    if not os.path.exists(state_file):
        return None
    date_str = open(state_file, "r", encoding="utf-8").read().strip()
    if not date_str:
        return None
    return pd.to_datetime(date_str, errors="coerce")


def write_last_processed_date(state_file: str, latest_date) -> None:
    """Save checkpoint after successful load."""
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    open(state_file, "w", encoding="utf-8").write(str(latest_date.date()))


def run_pipeline() -> None:
    start_time = datetime.now()
    logging.info("Pipeline started.")

    raw_df = extract_csv(config.INPUT_FILE)

    clean_df, quality_report = clean_transform_with_report(raw_df)
    if clean_df.empty:
        raise ValueError("No valid records after cleaning.")

    # incremental filter — only process records newer than last run
    last_date = read_last_processed_date(config.STATE_FILE)
    if last_date is not None:
        batch_df = clean_df[clean_df["Date"] > last_date].copy()
    else:
        batch_df = clean_df.copy()

    if batch_df.empty:
        run_ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        quality_report["run_timestamp"] = run_ts
        export_data_quality_report(quality_report, config.OUTPUT_QUALITY)
        elapsed = (datetime.now() - start_time).total_seconds()
        logging.info("No new records to process. Finished in %.2fs", elapsed)
        print("No new records found.")
        return

    summary_df = create_aggregations(batch_df)
    run_ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    summary_df["run_timestamp"] = run_ts

    load_to_sqlite(batch_df, summary_df, config.DB_FILE)

    # rebuild summary from actual sales_clean to ensure accuracy after dedup
    import sqlite3
    with sqlite3.connect(config.DB_FILE) as conn:
        actual_df = pd.read_sql("SELECT * FROM sales_clean", conn)
    actual_df["Date"] = pd.to_datetime(actual_df["Date"])
    summary_df = create_aggregations(actual_df)
    summary_df["run_timestamp"] = run_ts

    # update summary tables with accurate data
    with sqlite3.connect(config.DB_FILE) as conn:
        summary_df.to_sql("sales_summary", conn, if_exists="replace", index=False)
        summary_df.to_sql("sales_summary_history", conn, if_exists="append", index=False)

    export_summary_csv(summary_df, config.OUTPUT_SUMMARY)

    quality_report["run_timestamp"] = run_ts
    export_data_quality_report(quality_report, config.OUTPUT_QUALITY)

    latest_date = batch_df["Date"].max()
    write_last_processed_date(config.STATE_FILE, latest_date)

    elapsed = (datetime.now() - start_time).total_seconds()
    logging.info(
        "Pipeline success. records=%d, latest_date=%s, dropped=%d, time=%.2fs",
        len(batch_df), latest_date.date(), quality_report["total_rows_dropped"], elapsed,
    )
    print(f"Pipeline run successful. Processed {len(batch_df)} records in {elapsed:.2f}s")

    try:
        generate_html_report()
        logging.info("Report generated: %s", config.OUTPUT_HTML)
    except Exception as exc:
        logging.warning("Report generation failed: %s", exc)


if __name__ == "__main__":
    setup_logging()
    try:
        run_pipeline()
    except FileNotFoundError as exc:
        logging.error("Missing file: %s", exc)
        print(f"ERROR: {exc}")
    except ValueError as exc:
        logging.error("Data error: %s", exc)
        print(f"ERROR: {exc}")
    except RuntimeError as exc:
        logging.error("Database error: %s", exc)
        print(f"ERROR: {exc}")
    except Exception as exc:
        logging.exception("Unexpected failure.")
        print(f"ERROR: {exc}")
