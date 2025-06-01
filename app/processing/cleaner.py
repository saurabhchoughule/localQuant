import pandas as pd
import logging

logger = logging.getLogger("LocalQuantAgent")

def clean_stock_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df.empty:
        logger.warning(f"DataFrame for {ticker} is empty. Skipping cleaning.")
        return df

    logger.debug(f"Cleaning stock data for {ticker}. Initial rows: {len(df)}")

    critical_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    original_rows = len(df)
    df_cleaned = df.dropna(subset=critical_cols)

    if len(df_cleaned) < original_rows:
        logger.info(f"Dropped {original_rows - len(df_cleaned)} rows with NaNs in critical columns for {ticker}.")

    if isinstance(df_cleaned.index, pd.DatetimeIndex):
        df_cleaned = df_cleaned.reset_index()
        if 'Datetime' in df_cleaned.columns and 'Date' not in df_cleaned.columns:
            df_cleaned.rename(columns={'Datetime': 'Date'}, inplace=True)
        elif 'index' in df_cleaned.columns and 'Date' not in df_cleaned.columns and pd.api.types.is_datetime64_any_dtype(df_cleaned['index']):
            # Sometimes yfinance index comes as 'index' after reset_index
            df_cleaned.rename(columns={'index': 'Date'}, inplace=True)


    if 'Date' in df_cleaned.columns and pd.api.types.is_datetime64_any_dtype(df_cleaned['Date']):
         # Ensure Date is timezone-naive for consistency if it has timezone
        if df_cleaned['Date'].dt.tz is not None:
            df_cleaned['Date'] = df_cleaned['Date'].dt.tz_localize(None)
        # df_cleaned['Date'] = df_cleaned['Date'].dt.strftime('%Y-%m-%d') # Optional: convert to string

    # Add ticker column if not already present (e.g., if fetcher didn't add it)
    if 'Ticker' not in df_cleaned.columns:
        df_cleaned['Ticker'] = ticker

    logger.debug(f"Finished cleaning stock data for {ticker}. Final rows: {len(df_cleaned)}")
    return df_cleaned

def clean_macro_data(df: pd.DataFrame, series_id: str) -> pd.DataFrame:
    """
    Basic cleaning for macro data (e.g., from FRED).
    """
    if df.empty:
        logger.warning(f"DataFrame for {series_id} is empty. Skipping macro cleaning.")
        return df

    logger.debug(f"Cleaning macro data for {series_id}. Initial rows: {len(df)}")

    # Ensure 'Date' and 'Value' columns exist
    if not all(col in df.columns for col in ['Date', 'Value']):
        logger.error(f"Macro data for {series_id} missing 'Date' or 'Value' column.")
        return pd.DataFrame()

    # Drop rows where Value is NaN
    original_rows = len(df)
    df_cleaned = df.dropna(subset=['Value'])
    if len(df_cleaned) < original_rows:
        logger.info(f"Dropped {original_rows - len(df_cleaned)} rows with NaN 'Value' for {series_id}.")

    # Ensure Date is datetime (FRED usually provides this)
    if not pd.api.types.is_datetime64_any_dtype(df_cleaned['Date']):
        try:
            df_cleaned['Date'] = pd.to_datetime(df_cleaned['Date'])
        except Exception as e:
            logger.error(f"Could not convert 'Date' column to datetime for {series_id}: {e}")
            return pd.DataFrame() # Or handle differently

    # Ensure Date is timezone-naive
    if df_cleaned['Date'].dt.tz is not None:
        df_cleaned['Date'] = df_cleaned['Date'].dt.tz_localize(None)

    # df_cleaned['Date'] = df_cleaned['Date'].dt.strftime('%Y-%m-%d') # Optional

    if 'SeriesID' not in df_cleaned.columns:
        df_cleaned['SeriesID'] = series_id

    logger.debug(f"Finished cleaning macro data for {series_id}. Final rows: {len(df_cleaned)}")
    return df_cleaned