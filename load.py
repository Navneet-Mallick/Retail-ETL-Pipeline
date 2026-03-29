import os
import sqlite3
import pandas as pd


def load_to_sqlite(df_clean: pd.DataFrame, df_summary: pd.DataFrame, db_path: str) -> None:
    """
    Load clean and summary data into SQLite.
    Creates/updates:
      - sales_clean (append, deduped by Transaction ID)
      - sales_summary (replace)
      - sales_summary_history (append)
    """
    try:
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            # Create sales_clean with unique constraint on Transaction ID
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sales_clean (
                    "Transaction ID" INTEGER PRIMARY KEY,
                    "Date" TEXT,
                    "Customer ID" TEXT,
                    "Gender" TEXT,
                    "Age" REAL,
                    "Product Category" TEXT,
                    "Quantity" INTEGER,
                    "Price per Unit" REAL,
                    "Total Amount" REAL,
                    "Age Group" TEXT,
                    "Month" INTEGER,
                    "Year" INTEGER
                )
            """)
            # Insert or ignore duplicates by Transaction ID
            df_clean.to_sql("sales_clean_staging", conn, if_exists="replace", index=False)
            conn.execute("""
                INSERT OR IGNORE INTO sales_clean
                SELECT * FROM sales_clean_staging
            """)
            conn.execute("DROP TABLE IF EXISTS sales_clean_staging")

            df_summary.to_sql("sales_summary", conn, if_exists="replace", index=False)
            df_summary.to_sql("sales_summary_history", conn, if_exists="append", index=False)
    except sqlite3.Error as exc:
        raise RuntimeError(f"Database error: {exc}") from exc


def export_summary_csv(df_summary: pd.DataFrame, output_path: str) -> None:
    """Export summary dataframe to CSV."""
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    df_summary.to_csv(output_path, index=False)


def export_data_quality_report(report: dict, output_path: str) -> None:
    """
    Append per-run quality stats to CSV for audit/history.
    """
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    report_df = pd.DataFrame([report])
    write_header = not os.path.exists(output_path)
    report_df.to_csv(output_path, mode="a", index=False, header=write_header)
