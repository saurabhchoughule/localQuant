import yaml
import json
from pathlib import Path
import logging

logger = logging.getLogger("LocalQuantAgent")

class ConfigManager:
    def __init__(self, settings_file="config/settings.yaml", tickers_file="config/tickers.json"):
        self.base_path = Path(__file__).resolve().parent.parent # Project root
        self.settings_path = self.base_path / settings_file
        self.tickers_path = self.base_path / tickers_file

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
        keys = key.split('.')
        val = self.settings
        try:
            for k in keys:
                val = val[k]
            return val
        except KeyError:
            logger.warning(f"Setting '{key}' not found, returning default: {default}")
            return default
        except TypeError: # Happens if a parent key is not a dict
             logger.warning(f"Setting path for '{key}' is invalid, returning default: {default}")
             return default


    def get_tickers(self, category="indian_equity"):
        return self.tickers.get(category, [])

    def get_data_path(self):
        relative_path = self.get_setting("data_path", "data")
        return self.base_path / relative_path

    def get_log_file_path(self):
        relative_path = self.get_setting("log_file_path", "logs/agent.log")
        return str(self.base_path / relative_path) # Logger expects string

# Initialize config globally for easy access in other modules if needed (or pass around)
# config = ConfigManager()