"""
Scheduler manager — APScheduler removed (Phase 5).

Nightly pipeline now runs via system cron (see /etc/cron.d/pulsecities).
This module retains a no-op lifespan so api/main.py needs no changes.

To invoke the pipeline manually:
    /root/pulsecities/venv/bin/python -m scheduler.main
"""

import logging
from contextlib import asynccontextmanager
from fastapi.responses import Response
from starlette.requests import Request as StarletteRequest

logger = logging.getLogger(__name__)

# Minimal ASGI scope that satisfies slowapi's get_remote_address check.
_WARMUP_SCOPE = {
    "type": "http",
    "method": "GET",
    "path": "/api/neighborhoods/top-risk",
    "query_string": b"",
    "headers": [],
    "client": ("127.0.0.1", 0),
}


def _warm_caches() -> None:
    """Pre-fill in-process caches that are expensive on first hit."""
    from models.database import SessionLocal
    from api.routes.neighborhoods import get_top_risk_neighborhoods

    db = SessionLocal()
    try:
        get_top_risk_neighborhoods(
            request=StarletteRequest(scope=_WARMUP_SCOPE),
            response=Response(),
            limit=10,
            db=db,
        )
        logger.info("Cache warm-up complete: top-risk neighborhoods")
    except Exception:
        logger.warning("Cache warm-up failed — first request will be slow", exc_info=True)
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app):
    """
    No-op lifespan. APScheduler removed — scheduling handled by system cron.
    api/main.py imports this symbol; it must remain a valid asynccontextmanager.
    """
    logger.info("API starting — nightly pipeline runs via system cron at 2:00 AM UTC")
    _warm_caches()
    yield
    logger.info("API shutting down")
