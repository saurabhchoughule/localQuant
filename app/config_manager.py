import yaml
import json
from pathlib import Path
import logging
import os
from dotenv import load_dotenv # New import

logger = logging.getLogger("LocalQuantAgent")

class ConfigManager:
    def __init__(self, settings_file="config/settings.yaml", tickers_file="config/tickers.json"):
        self.base_path = Path(__file__).resolve().parent.parent
        self.settings_path = self.base_path / settings_file
        self.tickers_path = self.base_path / tickers_file

        # Load .env file from project root
        env_path = self.base_path / '.env'
        load_dotenv(dotenv_path=env_path)

        self.settings = self._load_yaml(self.settings_path)
        self.tickers = self._load_json(self.tickers_path)


        if not self.settings or not self.tickers:
            logger.error("Failed to load configuration. Exiting.")
            raise RuntimeError("Configuration loading failed.")

        logger.info("Configuration loaded successfully.")

    def _load_yaml(self, file_path):
        try:
            with open(file_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.error(f"YAML configuration file not found: {file_path}")
            return None
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {file_path}: {e}")
            return None

    def _load_json(self, file_path):
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"JSON configuration file not found: {file_path}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON file {file_path}: {e}")
            return None

    def get_setting(self, key, default=None):
        # First, check environment variables (e.g., for API keys)
        env_var = os.getenv(key.upper().replace('.', '_')) # FRED_API_KEY from fred.api_key
        if env_var is not None:
            return env_var

        # Then, check settings file
        keys = key.split('.')
        val = self.settings
        try:
            for k in keys:
                val = val[k]
            return val
        except (KeyError, TypeError):
            logger.debug(f"Setting '{key}' not found in settings file, returning default: {default}")
            return default

    def get_tickers(self, category="indian_equity"):
        return self.tickers.get(category, [])

    def get_data_path(self):
        relative_path = self.get_setting("data_path", "data")
        return self.base_path / relative_path

    def get_log_file_path(self):
        relative_path = self.get_setting("log_file_path", "logs/agent.log")
        return str(self.base_path / relative_path)