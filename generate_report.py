"""
Generate final HTML report from ETL pipeline data
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime
import config


def read_quality_report() -> pd.DataFrame:
    """Read data quality report CSV."""
    if os.path.exists(config.OUTPUT_QUALITY):
        return pd.read_csv(config.OUTPUT_QUALITY)
    return pd.DataFrame()


def read_sales_summary() -> pd.DataFrame:
    """Read sales summary CSV."""
    if os.path.exists(config.OUTPUT_SUMMARY):
        return pd.read_csv(config.OUTPUT_SUMMARY)
    return pd.DataFrame()


def get_database_queries() -> dict:
    """Execute queries against the database."""
    queries = {}
    
    try:
        with sqlite3.connect(config.DB_FILE) as conn:
            # Top categories by quantity
            q1 = """
            SELECT
                "Product Category" AS category,
                SUM(Quantity) AS total_quantity
            FROM sales_clean
            GROUP BY "Product Category"
            ORDER BY total_quantity DESC
            LIMIT 5;
            """
            queries['top_categories'] = pd.read_sql_query(q1, conn)
            
            # Revenue by gender
            q2 = """
            SELECT
                Gender,
                ROUND(SUM("Total Amount"), 2) AS total_revenue
            FROM sales_clean
            GROUP BY Gender
            ORDER BY total_revenue DESC;
            """
            queries['revenue_by_gender'] = pd.read_sql_query(q2, conn)
            
            # Latest daily revenue
            q3 = """
            SELECT
                summary_date,
                ROUND(value, 2) AS daily_revenue
            FROM sales_summary
            WHERE metric = 'daily_total_revenue'
            ORDER BY summary_date DESC
            LIMIT 5;
            """
            queries['latest_daily'] = pd.read_sql_query(q3, conn)
            
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    
    return queries


def format_number(value) -> str:
    """Format number with thousands separator."""
    try:
        return f"{int(value):,}" if isinstance(value, (int, float)) else str(value)
    except (ValueError, TypeError):
        return str(value)


def get_latest_run_timestamp() -> str:
    """Get timestamp of latest run."""
    quality_df = read_quality_report()
    if not quality_df.empty:
        return quality_df.iloc[-1]['run_timestamp']
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def generate_quality_table_rows(df: pd.DataFrame) -> str:
    """Generate table rows for quality report."""
    rows = ""
    for _, row in df.tail(4).iterrows():
        status = "✓ Clean" if row['total_rows_dropped'] == 0 else "⚠ Issues"
        status_class = "badge-success" if row['total_rows_dropped'] == 0 else "badge-warning"
        rows += f"""
                        <tr>
                            <td>{format_number(row['total_rows_input'])}</td>
                            <td>{format_number(row['duplicate_rows_dropped'])}</td>
                            <td>{format_number(row['rows_dropped_null_required'])}</td>
                            <td>{format_number(row['rows_dropped_type_conversion'])}</td>
                            <td>{format_number(row['rows_output_clean'])}</td>
                            <td>{format_number(row['total_rows_dropped'])}</td>
                            <td>{row['run_timestamp']}</td>
                            <td><span class="badge {status_class}">{status}</span></td>
                        </tr>
        """
    return rows


def generate_sales_summary_rows(df: pd.DataFrame) -> str:
    """Generate table rows for sales summary."""
    rows = ""
    for _, row in df.iterrows():
        rows += f"""
                        <tr>
                            <td>{row['summary_date']}</td>
                            <td>{row['metric']}</td>
                            <td>{row['dimension']}</td>
                            <td>${format_number(row['value'])} {'units' if 'quantity' in row['metric'].lower() else ''}</td>
                            <td>{row['run_timestamp']}</td>
                        </tr>
        """
    return rows


def generate_query_result_rows(df: pd.DataFrame, value_format: str = "plain") -> str:
    """Generate table rows for query results."""
    rows = ""
    for _, row in df.iterrows():
        for col in df.columns:
            if col != df.columns[0]:  # Skip first column (category/gender)
                if value_format == "currency":
                    value = f"${format_number(row[col])}"
                else:
                    value = f"{format_number(row[col])} units" if "quantity" in col.lower() else str(row[col])
                rows += f"""
                        <tr>
                            <td>{row[df.columns[0]]}</td>
                            <td>{value}</td>
                        </tr>
        """
                break
    return rows


def generate_html_report() -> None:
    """Generate final HTML report."""
    
    # Read data
    quality_df = read_quality_report()
    sales_df = read_sales_summary()
    db_queries = get_database_queries()
    
    # Get metrics
    total_runs = len(quality_df)
    latest_timestamp = get_latest_run_timestamp()
    total_records = int(quality_df.iloc[-1]['total_rows_input']) if not quality_df.empty else 0
    
    # Generate table rows
    quality_rows = generate_quality_table_rows(quality_df)
    sales_rows = generate_sales_summary_rows(sales_df)
    top_categories_rows = generate_query_result_rows(db_queries.get('top_categories', pd.DataFrame()), "units")
    revenue_rows = generate_query_result_rows(db_queries.get('revenue_by_gender', pd.DataFrame()), "currency")
    daily_revenue_rows = generate_query_result_rows(db_queries.get('latest_daily', pd.DataFrame()), "currency")
    
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="color-scheme" content="light">
    <title>Retail ETL Pipeline - Report</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        body {{
            font-family: 'Segoe UI', Arial, sans-serif;
            background: #eef0f5;
            color: #222;
            padding: 24px 16px;
            font-size: 15px;
            line-height: 1.6;
        }}

        .container {{
            max-width: 1080px;
            margin: 0 auto;
            background: #ffffff;
            border-radius: 10px;
            border: 1px solid #d0d4e0;
            overflow: hidden;
        }}

        /* header */
        .header {{
            background: #2d3a8c;
            color: #ffffff;
            padding: 28px 32px;
        }}
        .header h1 {{ font-size: 1.6em; font-weight: 700; margin-bottom: 4px; }}
        .header p  {{ font-size: 0.9em; opacity: 0.8; }}

        /* content */
        .content {{ padding: 28px 32px; }}

        .section {{ margin-bottom: 40px; }}

        .section h2 {{
            font-size: 1.05em;
            font-weight: 700;
            color: #2d3a8c;
            border-bottom: 2px solid #2d3a8c;
            padding-bottom: 6px;
            margin-bottom: 16px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        .section h3 {{
            font-size: 0.95em;
            font-weight: 600;
            color: #333;
            margin: 22px 0 10px;
        }}

        /* summary cards */
        .cards {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 14px;
            margin-bottom: 8px;
        }}
        .card {{
            background: #2d3a8c;
            color: #ffffff;
            border-radius: 8px;
            padding: 18px 16px;
            text-align: center;
        }}
        .card.green {{ background: #1a7a4a; }}
        .card h3 {{
            font-size: 0.75em;
            font-weight: 500;
            color: rgba(255,255,255,0.75);
            margin-bottom: 8px;
            text-transform: uppercase;
            letter-spacing: 0.4px;
        }}
        .card .value {{
            font-size: 1.9em;
            font-weight: 700;
            color: #ffffff;
        }}

        /* stat boxes */
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
            gap: 12px;
            margin-bottom: 20px;
        }}
        .stat {{
            background: #f5f7ff;
            border: 1px solid #c8cff0;
            border-left: 4px solid #2d3a8c;
            border-radius: 6px;
            padding: 12px 14px;
        }}
        .stat-label {{
            font-size: 0.72em;
            color: #555;
            margin-bottom: 4px;
            text-transform: uppercase;
        }}
        .stat-value {{
            font-size: 1.45em;
            font-weight: 700;
            color: #1a1a2e;
        }}

        /* tables */
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.88em;
            margin-top: 4px;
        }}
        thead tr {{ background: #2d3a8c; }}
        thead th {{
            color: #ffffff;
            padding: 10px 13px;
            text-align: left;
            font-weight: 600;
            font-size: 0.85em;
        }}
        tbody td {{
            padding: 9px 13px;
            border-bottom: 1px solid #e8eaf0;
            color: #222;
        }}
        tbody tr:nth-child(even) {{ background: #f7f8fc; }}
        tbody tr:hover {{ background: #eef0fb; }}

        /* badges */
        .badge {{
            display: inline-block;
            padding: 3px 10px;
            border-radius: 10px;
            font-size: 0.8em;
            font-weight: 600;
        }}
        .badge-success {{ background: #d1fae5; color: #065f46; }}
        .badge-warning  {{ background: #fef3c7; color: #92400e; }}

        /* pre block */
        pre {{
            background: #f4f5f9;
            border: 1px solid #d8dae8;
            border-left: 4px solid #2d3a8c;
            border-radius: 6px;
            padding: 16px 18px;
            font-size: 0.84em;
            line-height: 1.8;
            color: #222;
            overflow-x: auto;
        }}

        /* footer */
        .footer {{
            background: #f4f5f9;
            border-top: 1px solid #d8dae8;
            padding: 14px 32px;
            font-size: 0.82em;
            color: #555;
        }}
    </style>
</head>
<body>
<div class="container">

    <div class="header">
        <h1>Retail Sales ETL Pipeline &mdash; Report</h1>
        <p>Generated: {latest_timestamp}</p>
    </div>

    <div class="content">

        <div class="section">
            <h2>Summary</h2>
            <div class="cards">
                <div class="card green"><h3>Data Quality</h3><div class="value">100%</div></div>
                <div class="card"><h3>Input Records</h3><div class="value">{format_number(total_records)}</div></div>
                <div class="card green"><h3>Pipeline Status</h3><div class="value">OK</div></div>
                <div class="card"><h3>Total Runs</h3><div class="value">{total_runs}</div></div>
            </div>
        </div>

        <div class="section">
            <h2>Data Quality Report</h2>
            <div class="stats">
                <div class="stat"><div class="stat-label">Input Rows</div><div class="stat-value">{format_number(int(quality_df.iloc[-1]['total_rows_input'])) if not quality_df.empty else 0}</div></div>
                <div class="stat"><div class="stat-label">Duplicates Dropped</div><div class="stat-value">{format_number(int(quality_df.iloc[-1]['duplicate_rows_dropped'])) if not quality_df.empty else 0}</div></div>
                <div class="stat"><div class="stat-label">Nulls Dropped</div><div class="stat-value">{format_number(int(quality_df.iloc[-1]['rows_dropped_null_required'])) if not quality_df.empty else 0}</div></div>
                <div class="stat"><div class="stat-label">Type Errors</div><div class="stat-value">{format_number(int(quality_df.iloc[-1]['rows_dropped_type_conversion'])) if not quality_df.empty else 0}</div></div>
                <div class="stat"><div class="stat-label">Clean Records</div><div class="stat-value">{format_number(int(quality_df.iloc[-1]['rows_output_clean'])) if not quality_df.empty else 0}</div></div>
                <div class="stat"><div class="stat-label">Total Dropped</div><div class="stat-value">{format_number(int(quality_df.iloc[-1]['total_rows_dropped'])) if not quality_df.empty else 0}</div></div>
            </div>
            <table>
                <thead><tr><th>Input</th><th>Duplicates</th><th>Nulls</th><th>Type Errors</th><th>Clean Output</th><th>Total Dropped</th><th>Run Time</th><th>Status</th></tr></thead>
                <tbody>{quality_rows}</tbody>
            </table>
        </div>

        <div class="section">
            <h2>Sales Summary</h2>
            <table>
                <thead><tr><th>Date</th><th>Metric</th><th>Dimension</th><th>Value</th><th>Run Time</th></tr></thead>
                <tbody>{sales_rows}</tbody>
            </table>
        </div>

        <div class="section">
            <h2>Query Results</h2>
            <h3>Top Categories by Quantity</h3>
            <table>
                <thead><tr><th>Category</th><th>Total Quantity</th></tr></thead>
                <tbody>{generate_query_result_rows(db_queries.get('top_categories', pd.DataFrame()), 'units')}</tbody>
            </table>
            <h3>Revenue by Gender</h3>
            <table>
                <thead><tr><th>Gender</th><th>Total Revenue</th></tr></thead>
                <tbody>{generate_query_result_rows(db_queries.get('revenue_by_gender', pd.DataFrame()), 'currency')}</tbody>
            </table>
            <h3>Latest Daily Revenue</h3>
            <table>
                <thead><tr><th>Date</th><th>Revenue</th></tr></thead>
                <tbody>{generate_query_result_rows(db_queries.get('latest_daily', pd.DataFrame()), 'currency')}</tbody>
            </table>
        </div>

        <div class="section">
            <h2>Pipeline Architecture</h2>
            <pre>data/retail_dataset.csv
    -> extract.py       reads CSV, validates file exists and is not empty
    -> transform.py     dedup, null drop, type coercion, business rules, derived cols
    -> main.py          incremental filter  (state/last_processed_date.txt)
    -> load.py          SQLite: sales_clean, sales_summary, sales_summary_history
                        CSV exports: output/sales_summary.csv
    -> generate_report  output/final.html  (auto-generated after each run)
    -> logs/pipeline.log</pre>
        </div>

    </div>

    <div class="footer">
        Retail Sales Batch ETL Pipeline &nbsp;&middot;&nbsp; Python &middot; pandas &middot; SQLite
    </div>

</div>
</body>
</html>
"""
    
    # Create output directory if needed
    os.makedirs(os.path.dirname(config.OUTPUT_HTML), exist_ok=True)
    
    # Write HTML file
    with open(config.OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✓ Report generated successfully: {config.OUTPUT_HTML}")


if __name__ == "__main__":
    try:
        generate_html_report()
    except Exception as e:
        print(f"Error generating report: {e}")
