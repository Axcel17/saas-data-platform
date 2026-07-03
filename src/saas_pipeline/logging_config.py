"""Centralised logging configuration.

A single ``get_logger`` entry point keeps log formatting consistent across the
pipeline and avoids scattering ``print`` calls through the transformation code.
"""

from __future__ import annotations

import logging
import os

_CONFIGURED = False


def configure_logging(level: str | None = None) -> None:
    """Configure the root logger once, idempotently.

    Level can be overridden with the ``LOG_LEVEL`` environment variable so the
    same code path works in local runs, CI and Databricks jobs.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    resolved = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(
        level=resolved,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a module-scoped logger with logging configured."""
    configure_logging()
    return logging.getLogger(name)
