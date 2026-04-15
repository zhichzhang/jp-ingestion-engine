# src/utils/logger.py

import logging
import os
import colorlog


def setup_logger():
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    handler = colorlog.StreamHandler()

    handler.setFormatter(
        colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        )
    )

    logger = logging.getLogger()
    logger.setLevel(level)
    logger.handlers = []
    logger.addHandler(handler)


logger = logging.getLogger("ingestion")