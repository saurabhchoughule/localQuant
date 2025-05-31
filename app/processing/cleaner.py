import pandas as pd
import logging

logger = logging.getLogger("LocalQuantAgent")

def clean_stock_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Performs basic cleaning on the stock DataFrame.
    - Drops rows with NaNs in critical columns (Open, High, Low, Close, Volume)
    - Ensures correct data types (though yfinance is usually good)
    - Resets index if it's a DatetimeIndex to make Date a column
    """
    if df.empty:
        logger.warning(f"DataFrame for {ticker} is empty. Skipping cleaning.")
        return df

    logger.info(f"Cleaning data for {ticker}. Initial rows: {len(df)}")

    # Drop rows where essential OHLCV data is missing
    # yfinance usually provides clean data, but this is good practice
    critical_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    df_cleaned = df.dropna(subset=critical_cols)

    if len(df_cleaned) < len(df):
        logger.info(f"Dropped {len(df) - len(df_cleaned)} rows with NaNs for {ticker}.")

    # Ensure Date is a column (yfinance returns DatetimeIndex)
    if isinstance(df_cleaned.index, pd.DatetimeIndex):
        df_cleaned = df_cleaned.reset_index()
        # yfinance often names it 'Date' or 'Datetime'. Standardize to 'Date'.
        if 'Datetime' in df_cleaned.columns and 'Date' not in df_cleaned.columns:
            df_cleaned.rename(columns={'Datetime': 'Date'}, inplace=True)
        elif 'Date' not in df_cleaned.columns and df_cleaned.index.name is not None and ('date' in df_cleaned.index.name.lower()):
            df_cleaned.rename(columns={df_cleaned.index.name: 'Date'}, inplace=True)


    # Optional: Convert Date to string for CSV if preferred, or keep as datetime
    # df_cleaned['Date'] = df_cleaned['Date'].dt.strftime('%Y-%m-%d')

    # Add ticker column
    df_cleaned['Ticker'] = ticker

    logger.info(f"Finished cleaning for {ticker}. Final rows: {len(df_cleaned)}")
    return df_cleaned