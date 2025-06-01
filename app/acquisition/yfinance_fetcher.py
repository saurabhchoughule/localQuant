import yfinance as yf
import pandas as pd
import logging
import time # For retries

logger = logging.getLogger("LocalQuantAgent")

def fetch_stock_data(ticker: str, period: str = "1y", interval: str = "1d", retries: int = 3, delay: int = 5) -> pd.DataFrame:
    """
    Fetches historical stock data for a given ticker using yfinance.
    Includes a simple retry mechanism.
    """
    logger.info(f"Fetching data for {ticker} | Period: {period}, Interval: {interval}")
    for attempt in range(retries):
        try:
            stock = yf.Ticker(ticker)
            # data = stock.history(period=period, interval=interval, auto_adjust=True, prepost=False) # auto_adjust simplifies some things
            data = stock.history(period=period, interval=interval)
            if data.empty:
                # yfinance sometimes returns empty if no data for the period, not necessarily an error
                logger.warning(f"No data returned for {ticker} (Period: {period}, Interval: {interval}).")
                return pd.DataFrame() # Return empty if no data, don't retry this specifically unless an error
            logger.info(f"Successfully fetched {len(data)} rows for {ticker}.")
            return data
        except Exception as e: # Catch more generic exceptions from yfinance
            logger.error(f"Error fetching data for {ticker} (Attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"Failed to fetch data for {ticker} after {retries} attempts.")
                return pd.DataFrame()
    return pd.DataFrame() # Should be unreachable if retries > 0 but good for safety