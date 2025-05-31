import schedule
import time
import logging
from pathlib import Path

from .config_manager import ConfigManager
from .acquisition.yfinance_fetcher import fetch_stock_data
from .processing.cleaner import clean_stock_data
from .storage.file_handler import save_to_csv

logger = logging.getLogger("LocalQuantAgent")

class DataCuratorJob:
    def __init__(self, config: ConfigManager):
        self.config = config
        self.yfinance_settings = self.config.get_setting("yfinance", {})
        self.default_period = self.yfinance_settings.get("default_period", "1y")
        self.default_interval = self.yfinance_settings.get("default_interval", "1d")

    def run_daily_indian_equity_job(self):
        logger.info("Starting daily Indian equity data collection job...")
        tickers = self.config.get_tickers("indian_equity")
        if not tickers:
            logger.warning("No Indian equity tickers configured. Skipping job.")
            return

        base_data_path = self.config.get_data_path()
        output_dir = base_data_path / "indian" / "equity" / "daily"

        for ticker in tickers:
            logger.info(f"Processing {ticker}...")
            raw_data = fetch_stock_data(
                ticker,
                period=self.default_period,
                interval=self.default_interval
            )

            if raw_data.empty:
                logger.warning(f"No data fetched for {ticker}. Skipping processing and saving.")
                continue

            cleaned_data = clean_stock_data(raw_data, ticker)

            if cleaned_data.empty:
                logger.warning(f"Data for {ticker} became empty after cleaning. Skipping saving.")
                continue

            # Construct file name, e.g., RELIANCE.NS.csv
            # Sanitize ticker for filename if necessary (though yfinance tickers are usually safe)
            safe_ticker_filename = ticker.replace(":", "_") # Example sanitization
            file_path = output_dir / f"{safe_ticker_filename}.csv"
            save_to_csv(cleaned_data, str(file_path), ticker)

        logger.info("Daily Indian equity data collection job finished.")


def run_scheduler(config: ConfigManager):
    curator_job = DataCuratorJob(config)

    # Schedule the job
    # For testing, you might want to run it more frequently:
    # schedule.every(1).minutes.do(curator_job.run_daily_indian_equity_job)
    schedule.every().day.at("18:00").do(curator_job.run_daily_indian_equity_job) # Example: Run daily at 6 PM
    # schedule.every().day.at("01:00").do(curator_job.run_daily_indian_equity_job) # After market close (US time) for India EOD

    logger.info("Scheduler started. Waiting for scheduled jobs...")
    logger.info(f"Next job run at: {schedule.next_run()}")


    try:
        while True:
            schedule.run_pending()
            time.sleep(60) # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")