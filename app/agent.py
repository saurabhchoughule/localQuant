import logging
from app.utils.logger import setup_logging
from app.config_manager import ConfigManager
from app.scheduler import run_scheduler

def main():
    try:
        # 1. Initialize Configuration
        config = ConfigManager()
    except RuntimeError:
        # ConfigManager already logs the error, just exit.
        print("Critical error during configuration loading. Agent cannot start.")
        return

    # 2. Setup Logging (after config is loaded to get log path and level)
    log_file = config.get_log_file_path()
    log_level = config.get_setting("log_level", "INFO")
    setup_logging(log_file_path_str=log_file, level=log_level)

    logger = logging.getLogger("LocalQuantAgent")
    logger.info("LocalQuant Agent is starting...")

    # 3. Start the Scheduler
    try:
        run_scheduler(config)
    except Exception as e:
        logger.critical(f"An unhandled exception occurred in the scheduler: {e}", exc_info=True)
    finally:
        logger.info("LocalQuant Agent is shutting down.")

if __name__ == "__main__":
    # Create __init__.py in app/ subdirectories if you haven't
    # e.g., app/acquisition/__init__.py (can be empty)
    # This allows Python to treat them as packages for relative imports
    import os
    subdirs = ["acquisition", "processing", "storage", "utils"]
    for subdir in subdirs:
        init_file = os.path.join("app", subdir, "__init__.py")
        if not os.path.exists(init_file):
            with open(init_file, "w") as f:
                pass # Create empty __init__.py

    main()