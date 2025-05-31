import pandas as pd
from pathlib import Path
import logging
import os

logger = logging.getLogger("LocalQuantAgent")

def save_to_csv(df: pd.DataFrame, file_path_str: str, ticker: str):
    """
    Saves a DataFrame to a CSV file.
    """
    if df.empty:
        logger.warning(f"DataFrame for {ticker} is empty. Skipping save to {file_path_str}.")
        return

    file_path = Path(file_path_str)
    try:
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(file_path, index=False)
        logger.info(f"Successfully saved data for {ticker} to {file_path}")
    except Exception as e:
        logger.error(f"Error saving data for {ticker} to {file_path}: {e}")