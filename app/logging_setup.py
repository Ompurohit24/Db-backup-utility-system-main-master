"""Central logging setup for the FastAPI backup utility.

This module configures RotatingFileHandler-based loggers for:
- app.log     → general application events
- backup.log  → backup operations
- restore.log → restore operations
- error.log   → errors across the app
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


# Root logs directory (project_root/logs)
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"

# Rotation settings
MAX_BYTES = 5 * 1024 * 1024  # 5 MB per file
BACKUP_COUNT = 5             # keep last 5 files per logger

# Shared formatter for readability
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def _ensure_log_dir() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def _build_handler(filename: str, level: int) -> RotatingFileHandler:
    handler = RotatingFileHandler(
        LOG_DIR / filename,
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return handler


def setup_logging() -> None:
    """Idempotent logger setup used across the app."""
    if getattr(setup_logging, "_configured", False):
        return

    _ensure_log_dir()

    logger_map = {
        "app": ("app.log", logging.INFO),
        "backup": ("backup.log", logging.INFO),
        "restore": ("restore.log", logging.INFO),
        "scheduler": ("backup.log", logging.INFO),
        "error": ("error.log", logging.ERROR),
    }

    for name, (filename, level) in logger_map.items():
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = False

        if not logger.handlers:
            logger.addHandler(_build_handler(filename, level))

    setup_logging._configured = True


def get_logger(name: str = "app") -> logging.Logger:
    setup_logging()
    return logging.getLogger(name)

