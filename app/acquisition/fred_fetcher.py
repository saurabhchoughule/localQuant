import pandas as pd
from fredapi import Fred
import logging

logger = logging.getLogger("LocalQuantAgent")

def fetch_fred_series(series_id: str, api_key: str, start_date=None, end_date=None) -> pd.DataFrame:
    """
    Fetches a specific series from FRED.
    Returns a DataFrame with 'Date' and 'Value' columns.
    """
    logger.info(f"Fetching FRED series: {series_id}")
    if not api_key:
        logger.error("FRED API key not provided. Cannot fetch FRED data.")
        return pd.DataFrame()

    try:
        fred = Fred(api_key=api_key)
        series_data = fred.get_series(series_id, observation_start=start_date, observation_end=end_date)

        if series_data is None or series_data.empty:
            logger.warning(f"No data returned for FRED series: {series_id}")
            return pd.DataFrame()

        # Convert Series to DataFrame and rename columns
        df = series_data.reset_index()
        df.columns = ['Date', 'Value'] # FRED series index is usually date, value is the series itself
        df['SeriesID'] = series_id # Add series ID for context
        logger.info(f"Successfully fetched {len(df)} data points for FRED series: {series_id}")
        return df
    except Exception as e:
        logger.error(f"Error fetching FRED series {series_id}: {e}")
        return pd.DataFrame()