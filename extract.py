import os
import pandas as pd


def extract_csv(file_path: str) -> pd.DataFrame:
    """
    Extract data from a CSV file.

    Raises:
        FileNotFoundError: If input file does not exist.
        ValueError: If CSV exists but is empty.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    df = pd.read_csv(file_path)

    if df.empty:
        raise ValueError("Dataset is empty. Nothing to process.")

    return df
