"""Convenience wrapper around the shared logging setup.

This module simply re-exports the helpers defined in ``logging_setup.py``
so ``from app.logger import get_logger`` works regardless of import style.
"""

from app.logging_setup import get_logger, setup_logging  # re-export

__all__ = ["get_logger", "setup_logging"]
