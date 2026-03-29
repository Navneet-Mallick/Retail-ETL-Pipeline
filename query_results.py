import sqlite3
import pandas as pd
import config


def run_demo_queries(db_path: str = None) -> None:
    """Print quick insights from the SQLite database for viva/demo."""
    if db_path is None:
        db_path = config.DB_FILE
    
    with sqlite3.connect(db_path) as conn:
        print("\n=== Top 5 Product Categories by Quantity ===")
        q1 = """
        SELECT
            "Product Category" AS category,
            SUM(Quantity) AS total_quantity
        FROM sales_clean
        GROUP BY "Product Category"
        ORDER BY total_quantity DESC
        LIMIT 5;
        """
        top_categories = pd.read_sql_query(q1, conn)
        print(top_categories.to_string(index=False))

        print("\n=== Revenue by Gender ===")
        q2 = """
        SELECT
            Gender,
            ROUND(SUM("Total Amount"), 2) AS total_revenue
        FROM sales_clean
        GROUP BY Gender
        ORDER BY total_revenue DESC;
        """
        gender_revenue = pd.read_sql_query(q2, conn)
        print(gender_revenue.to_string(index=False))

        print("\n=== Latest 5 Days Revenue ===")
        q3 = """
        SELECT
            summary_date,
            ROUND(value, 2) AS daily_revenue
        FROM sales_summary
        WHERE metric = 'daily_total_revenue'
        ORDER BY summary_date DESC
        LIMIT 5;
        """
        latest_daily = pd.read_sql_query(q3, conn)
        print(latest_daily.to_string(index=False))


if __name__ == "__main__":
    try:
        run_demo_queries()
    except sqlite3.Error as exc:
        print(f"Database error: {exc}")
    except Exception as exc:
        print(f"Unexpected error: {exc}")
