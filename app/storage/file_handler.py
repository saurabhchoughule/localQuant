import pandas as pd
from pathlib import Path
import logging
import os

logger = logging.getLogger("LocalQuantAgent")

def save_data(df: pd.DataFrame, file_path_str: str, identifier: str, file_format: str = "csv"):
    """
    Saves a DataFrame to a specified file format (csv or parquet).
    'identifier' is used for logging (e.g., ticker or series_id).
    """
    if df.empty:
        logger.warning(f"DataFrame for {identifier} is empty. Skipping save to {file_path_str} (format: {file_format}).")
        return

    file_path = Path(file_path_str)
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True) # Ensure directory exists

        if file_format == "csv":
            df.to_csv(file_path, index=False)
            logger.info(f"Successfully saved data for {identifier} to CSV: {file_path}")
        elif file_format == "parquet":
            df.to_parquet(file_path, index=False, engine='pyarrow') # or 'fastparquet'
            logger.info(f"Successfully saved data for {identifier} to Parquet: {file_path}")
        else:
            logger.error(f"Unsupported file format '{file_format}' for {identifier}. Cannot save.")
            return

    except Exception as e:
        logger.error(f"Error saving data for {identifier} to {file_path} (format: {file_format}): {e}")

# Keep save_to_csv for backward compatibility or specific use if needed, but prefer save_data
def save_to_csv(df: pd.DataFrame, file_path_str: str, ticker: str):
    save_data(df, file_path_str, ticker, file_format="csv")