import yfinance as yf
import pandas as pd
import logging

logger = logging.getLogger("LocalQuantAgent")

def fetch_stock_data(ticker: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
    """
    Fetches historical stock data for a given ticker using yfinance.
    """
    logger.info(f"Fetching data for {ticker} | Period: {period}, Interval: {interval}")
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(period=period, interval=interval)
        if data.empty:
            logger.warning(f"No data returned for {ticker} with period {period} and interval {interval}.")
            return pd.DataFrame()
        logger.info(f"Successfully fetched {len(data)} rows for {ticker}.")
        return data
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame() # Return empty DataFrame on error