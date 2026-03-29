import pandas as pd
import config


def age_group(age: float) -> str:
    """Bucket age into readable groups."""
    if pd.isna(age):
        return "Unknown"
    if age < 18:
        return "<18"
    if 18 <= age <= 25:
        return "18-25"
    if 26 <= age <= 35:
        return "26-35"
    if 36 <= age <= 45:
        return "36-45"
    if 46 <= age <= 60:
        return "46-60"
    return "60+"


def clean_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform sales data:
    - remove duplicates and nulls
    - fix data types
    - validate and recalculate Total Amount
    - create Age Group
    """
    required_cols = [
        "Transaction ID",
        "Date",
        "Customer ID",
        "Gender",
        "Age",
        "Product Category",
        "Quantity",
        "Price per Unit",
        "Total Amount",
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Remove duplicate rows.
    df = df.drop_duplicates()

    # Drop rows with missing values in required columns.
    df = df.dropna(subset=required_cols)

    # Type corrections.
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["Price per Unit"] = pd.to_numeric(df["Price per Unit"], errors="coerce")
    df["Total Amount"] = pd.to_numeric(df["Total Amount"], errors="coerce")

    # Remove rows that failed conversion.
    df = df.dropna(subset=["Date", "Age", "Quantity", "Price per Unit"])

    # Basic quality checks using config rules.
    df = df[
        (df["Age"] >= config.MIN_AGE) & (df["Age"] <= config.MAX_AGE) &
        (df["Quantity"] >= config.MIN_QUANTITY) & (df["Quantity"] <= config.MAX_QUANTITY) &
        (df["Price per Unit"] >= config.MIN_PRICE) & (df["Price per Unit"] <= config.MAX_PRICE)
    ]

    # Recalculate total amount for validation/consistency.
    df["Total Amount"] = df["Quantity"] * df["Price per Unit"]

    # Create derived column.
    df["Age Group"] = df["Age"].apply(age_group)

    # Standardize text values.
    df["Gender"] = df["Gender"].astype(str).str.strip().str.title()
    df["Product Category"] = df["Product Category"].astype(str).str.strip().str.title()

    return df


def clean_transform_with_report(df: pd.DataFrame):
    """
    Clean + transform data and return a quality report.

    Returns:
        (clean_df, report_dict)
    """
    required_cols = [
        "Transaction ID",
        "Date",
        "Customer ID",
        "Gender",
        "Age",
        "Product Category",
        "Quantity",
        "Price per Unit",
        "Total Amount",
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    report = {
        "total_rows_input": int(len(df)),
        "duplicate_rows_dropped": 0,
        "rows_dropped_null_required": 0,
        "rows_dropped_type_conversion": 0,
        "rows_dropped_business_rules": 0,
        "rows_output_clean": 0,
        "total_rows_dropped": 0,
    }

    # Type conversion FIRST — so dedup works on normalized values.
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["Price per Unit"] = pd.to_numeric(df["Price per Unit"], errors="coerce")
    df["Total Amount"] = pd.to_numeric(df["Total Amount"], errors="coerce")

    # Duplicate check AFTER type normalization.
    duplicate_count = int(df.duplicated().sum())
    report["duplicate_rows_dropped"] = duplicate_count
    df = df.drop_duplicates()

    # Null check in required fields.
    before_null_drop = len(df)
    df = df.dropna(subset=required_cols)
    report["rows_dropped_null_required"] = int(before_null_drop - len(df))

    before_type_drop = len(df)
    df = df.dropna(subset=["Date", "Age", "Quantity", "Price per Unit"])
    report["rows_dropped_type_conversion"] = int(before_type_drop - len(df))

    # Business rule checks using config.
    before_business_drop = len(df)
    df = df[
        (df["Age"] >= config.MIN_AGE) & (df["Age"] <= config.MAX_AGE) &
        (df["Quantity"] >= config.MIN_QUANTITY) & (df["Quantity"] <= config.MAX_QUANTITY) &
        (df["Price per Unit"] >= config.MIN_PRICE) & (df["Price per Unit"] <= config.MAX_PRICE)
    ]
    report["rows_dropped_business_rules"] = int(before_business_drop - len(df))

    # Recalculate total and derive columns.
    df["Total Amount"] = df["Quantity"] * df["Price per Unit"]
    df["Age Group"] = df["Age"].apply(age_group)
    df["Gender"] = df["Gender"].astype(str).str.strip().str.title()
    df["Product Category"] = df["Product Category"].astype(str).str.strip().str.title()
    df["Month"] = df["Date"].dt.month
    df["Year"] = df["Date"].dt.year

    report["rows_output_clean"] = int(len(df))
    report["total_rows_dropped"] = int(report["total_rows_input"] - report["rows_output_clean"])
    return df, report


def create_aggregations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a long-format summary table for BI:
    - daily total revenue
    - top-selling product categories (quantity)
    - revenue by gender
    """
    if df.empty:
        return pd.DataFrame(columns=["summary_date", "metric", "dimension", "value"])

    # Daily total revenue.
    daily = (
        df.groupby(df["Date"].dt.date)["Total Amount"]
        .sum()
        .reset_index()
        .rename(columns={"Date": "summary_date", "Total Amount": "value"})
    )
    daily["metric"] = "daily_total_revenue"
    daily["dimension"] = "all"

    # Category-wise total quantity sold.
    categories = (
        df.groupby("Product Category")["Quantity"]
        .sum()
        .reset_index()
        .rename(columns={"Product Category": "dimension", "Quantity": "value"})
    )
    categories["summary_date"] = pd.Timestamp.today().date()
    categories["metric"] = "top_selling_category_quantity"

    # Revenue by gender.
    gender = (
        df.groupby("Gender")["Total Amount"]
        .sum()
        .reset_index()
        .rename(columns={"Gender": "dimension", "Total Amount": "value"})
    )
    gender["summary_date"] = pd.Timestamp.today().date()
    gender["metric"] = "revenue_by_gender"

    # Revenue by product category.
    category_revenue = (
        df.groupby("Product Category")["Total Amount"]
        .sum()
        .reset_index()
        .rename(columns={"Product Category": "dimension", "Total Amount": "value"})
    )
    category_revenue["summary_date"] = pd.Timestamp.today().date()
    category_revenue["metric"] = "revenue_by_category"

    summary = pd.concat(
        [
            daily[["summary_date", "metric", "dimension", "value"]],
            categories[["summary_date", "metric", "dimension", "value"]],
            gender[["summary_date", "metric", "dimension", "value"]],
            category_revenue[["summary_date", "metric", "dimension", "value"]],
        ],
        ignore_index=True,
    )

    return summary
