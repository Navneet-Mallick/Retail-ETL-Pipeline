# Configuration for Retail ETL Pipeline

import os

# Data paths
INPUT_FILE = "data/retail_dataset.csv"
DB_FILE = "retail.db"

# Output paths
OUTPUT_DIR = "output"
OUTPUT_SUMMARY = os.path.join(OUTPUT_DIR, "sales_summary.csv")
OUTPUT_QUALITY = os.path.join(OUTPUT_DIR, "data_quality_report.csv")
OUTPUT_HTML = os.path.join(OUTPUT_DIR, "final.html")

# Logging
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")

# State management
STATE_DIR = "state"
STATE_FILE = os.path.join(STATE_DIR, "last_processed_date.txt")

# Data validation rules
MIN_AGE = 0
MAX_AGE = 120
MIN_QUANTITY = 0
MAX_QUANTITY = 1000
MIN_PRICE = 0
MAX_PRICE = 100000
