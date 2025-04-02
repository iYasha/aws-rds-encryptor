import logging
import os
import sys
import time
from datetime import UTC, datetime

DEBUG = bool(int(os.getenv("DEBUG", "0")))
MIGRATION_SEED = datetime.now(tz=UTC).strftime("%Y-%m-%d")


def get_logger(name: str = "deploy", log_level: str = logging.INFO) -> logging.Logger:
    if DEBUG:
        log_level = logging.DEBUG
    logger = logging.getLogger(name)
    stream_handler = logging.StreamHandler(sys.stdout)
    log_formatter = logging.Formatter(
        "[%(name)s] [%(levelname)s] %(asctime)s: %(message)s",
    )
    log_formatter.converter = time.gmtime
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)
    logger.setLevel(log_level)
    return logger


def normalize_aws_id(db_name: str) -> str:
    """
    Identifiers must begin with a letter and must contain only ASCII letters, digits, and hyphens.
    They can't end with a hyphen, or contain two consecutive hyphens.
    """
    return "".join(c for c in db_name.replace("_", "-") if c.isalnum() or c == "-").strip("-")
