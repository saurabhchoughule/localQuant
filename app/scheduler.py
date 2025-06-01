import schedule
import time
import logging
from pathlib import Path
import pandas as pd

from .config_manager import ConfigManager
from .acquisition.yfinance_fetcher import fetch_stock_data
from .acquisition.fred_fetcher import fetch_fred_series # New
from .processing.cleaner import clean_stock_data, clean_macro_data # Updated
from .storage.file_handler import save_data # Updated

logger = logging.getLogger("LocalQuantAgent")

class DataCuratorJob:
    def __init__(self, config: ConfigManager):
        self.config = config
        self.yfinance_settings = self.config.get_setting("yfinance", {})
        self.storage_settings = self.config.get_setting("storage", {})

        self.yfin_default_period = self.yfinance_settings.get("default_period", "1y")
        self.yfin_default_interval = self.yfinance_settings.get("default_interval", "1d")
        self.yfin_intl_period = self.yfinance_settings.get("international_equity_period", "1y")

        self.default_file_format = self.storage_settings.get("default_format", "csv")

        self.fred_api_key = self.config.get_setting("fred_api_key") # Will check .env then settings.yaml


    def _process_and_save_equity(self, ticker: str, data: pd.DataFrame, output_dir: Path, data_type: str):
        if data.empty:
            logger.warning(f"No data fetched for {ticker} ({data_type}). Skipping processing and saving.")
            return

        cleaned_data = clean_stock_data(data, ticker)

        if cleaned_data.empty:
            logger.warning(f"Data for {ticker} ({data_type}) became empty after cleaning. Skipping saving.")
            return

        safe_ticker_filename = ticker.replace(":", "_").replace("^", "_") # Sanitize more
        file_name = f"{safe_ticker_filename}.{self.default_file_format}"
        file_path = output_dir / file_name
        save_data(cleaned_data, str(file_path), ticker, file_format=self.default_file_format)


    def run_daily_indian_equity_job(self):
        logger.info("Starting daily Indian equity data collection job...")
        tickers = self.config.get_tickers("indian_equity")
        if not tickers:
            logger.warning("No Indian equity tickers configured. Skipping job.")
            return

        base_data_path = self.config.get_data_path()
        output_dir = base_data_path / "indian" / "equity" / "daily"

        for ticker in tickers:
            logger.info(f"Processing Indian Equity: {ticker}...")
            raw_data = fetch_stock_data(
                ticker,
                period=self.yfin_default_period,
                interval=self.yfin_default_interval
            )
            self._process_and_save_equity(ticker, raw_data, output_dir, "Indian Equity")
        logger.info("Daily Indian equity data collection job finished.")


    def run_daily_international_equity_job(self):
        logger.info("Starting daily International equity data collection job...")
        tickers = self.config.get_tickers("international_equity")
        if not tickers:
            logger.warning("No International equity tickers configured. Skipping job.")
            return

        base_data_path = self.config.get_data_path()
        output_dir = base_data_path / "international" / "equity" / "daily"

        for ticker in tickers:
            logger.info(f"Processing International Equity: {ticker}...")
            raw_data = fetch_stock_data(
                ticker,
                period=self.yfin_intl_period, # Use specific period for intl if defined
                interval=self.yfin_default_interval
            )
            self._process_and_save_equity(ticker, raw_data, output_dir, "International Equity")
        logger.info("Daily International equity data collection job finished.")


    def run_daily_indian_macro_job(self):
        logger.info("Starting daily Indian macro data collection job (via FRED)...")
        series_ids = self.config.get_tickers("indian_macro_fred")

        if not self.fred_api_key:
            logger.error("FRED API key not found. Skipping Indian macro data job.")
            return
        if not series_ids:
            logger.warning("No Indian macro FRED series IDs configured. Skipping job.")
            return

        base_data_path = self.config.get_data_path()
        output_dir = base_data_path / "indian" / "macro" # Path for macro data

        for series_id in series_ids:
            logger.info(f"Processing Indian Macro (FRED): {series_id}...")
            # For FRED, you might not need period/interval like yfinance,
            # it fetches all available historical data by default.
            # You can pass start_date if needed.
            raw_data = fetch_fred_series(series_id, self.fred_api_key)

            if raw_data.empty:
                logger.warning(f"No data fetched for FRED series {series_id}. Skipping.")
                continue

            cleaned_data = clean_macro_data(raw_data, series_id)

            if cleaned_data.empty:
                logger.warning(f"Data for FRED series {series_id} became empty after cleaning. Skipping.")
                continue

            file_name = f"{series_id.replace(':', '_')}.{self.default_file_format}"
            file_path = output_dir / file_name
            save_data(cleaned_data, str(file_path), series_id, file_format=self.default_file_format)
        logger.info("Daily Indian macro data collection job (via FRED) finished.")


def run_scheduler(config: ConfigManager):
    curator_job = DataCuratorJob(config)

    # Schedule jobs
    # Indian Equity (adjust time as needed, e.g., after Indian market close)
    schedule.every().day.at("15:45").do(curator_job.run_daily_indian_equity_job).tag("indian_equity")
    # International Equity (adjust time, e.g., after US market close)
    schedule.every().day.at("02:00").do(curator_job.run_daily_international_equity_job).tag("intl_equity") # e.g., 2 AM local time
    # Indian Macro (FRED data updates at various times, daily might be fine)
    schedule.every().day.at("04:00").do(curator_job.run_daily_indian_macro_job).tag("indian_macro")

    # For testing - run immediately and then schedule
    # curator_job.run_daily_indian_equity_job()
    # curator_job.run_daily_international_equity_job()
    # curator_job.run_daily_indian_macro_job()


    logger.info("Scheduler started. Waiting for scheduled jobs...")
    for job in schedule.jobs:
        logger.info(f"Scheduled job: {job.tags} next run at {job.next_run}")


    try:
        while True:
            schedule.run_pending()
            time.sleep(30) # Check every 30 seconds
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
    except Exception as e:
        logger.critical(f"A critical error occurred in the scheduler loop: {e}", exc_info=True)