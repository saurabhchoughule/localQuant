# LocalQuant - AI Data Curator Agent

LocalQuant is an AI agent designed to collect, process, and store Indian and International micro and macro-economic data for stock market analysis.

## Phase 1: Indian Equity Daily Data

This phase focuses on:
- Setting up the basic project structure.
- Configuring tickers and settings.
- Implementing a daily scheduler.
- Fetching daily OHLCV data for configured Indian stocks using `yfinance`.
- Performing basic data cleaning.
- Storing the curated data as CSV files.
- Basic logging.

## Setup

1.  **Clone the repository (if applicable) or create the structure.**
2.  **Create and activate a virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Configure:**
    - Edit `config/settings.yaml` for data paths and log settings.
    - Edit `config/tickers.json` to list the Indian equity tickers you want to track.

## Running the Agent

```bash
python -m app.agent