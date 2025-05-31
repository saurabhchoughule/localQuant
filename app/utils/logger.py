import logging
import sys
from pathlib import Path

def setup_logging(log_file_path_str: str, level: str = "INFO"):
    log_file_path = Path(log_file_path_str)
    log_file_path.parent.mkdir(parents=True, exist_ok=True) # Ensure log directory exists

    log_level = getattr(logging, level.upper(), logging.INFO)

    logger = logging.getLogger("LocalQuantAgent")
    logger.setLevel(log_level)

    # Prevent duplicate handlers if called multiple times (e.g., in testing)
    if logger.hasHandlers():
        logger.handlers.clear()

    # Console Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(log_level)

    # File Handler
    fh = logging.FileHandler(log_file_path)
    fh.setLevel(log_level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger

# Example of how to get the logger in other modules:
# import logging
# logger = logging.getLogger("LocalQuantAgent")