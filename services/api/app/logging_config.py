import logging
import os


def configure_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    fmt = os.getenv(
        "LOG_FORMAT",
        "%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    if not logging.getLogger().handlers:
        logging.basicConfig(level=level, format=fmt)
    else:
        logging.getLogger().setLevel(level)

