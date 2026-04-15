"""
Scheduler manager — APScheduler removed (Phase 5).

Nightly pipeline now runs via system cron (see /etc/cron.d/pulsecities).
This module retains a no-op lifespan so api/main.py needs no changes.

To invoke the pipeline manually:
    /root/pulsecities/venv/bin/python -m scheduler.main
"""

import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    """
    No-op lifespan. APScheduler removed — scheduling handled by system cron.
    api/main.py imports this symbol; it must remain a valid asynccontextmanager.
    """
    logger.info("API starting — nightly pipeline runs via system cron at 2:00 AM UTC")
    yield
    logger.info("API shutting down")
