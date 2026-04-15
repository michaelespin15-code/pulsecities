"""
Structured logging configuration for scrapers and API.
Every module gets a named logger via logging.getLogger(__name__).
Call configure_logging() once at app startup to set the root handler.
"""

import logging
import sys

_FORMATTER = logging.Formatter(
    fmt="%(asctime)s | %(name)-30s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure the root logger once at application startup.
    Sets a consistent format for all loggers in the project.
    Safe to call multiple times — idempotent.
    """
    root = logging.getLogger()
    if root.handlers:
        return  # Already configured
    root.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(_FORMATTER)
    root.addHandler(handler)


def setup_logging(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Return a named logger with consistent formatting.
    Kept for backwards compatibility — prefer logging.getLogger(__name__).
    """
    configure_logging(level)
    return logging.getLogger(name)
