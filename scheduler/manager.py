"""
Scheduler manager — APScheduler removed (Phase 5).

Nightly pipeline now runs via system cron (see /etc/cron.d/pulsecities).
This module retains a no-op lifespan so api/main.py needs no changes.

To invoke the pipeline manually:
    /root/pulsecities/venv/bin/python -m scheduler.main
"""

import logging
import threading
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
    """Pre-fill in-process caches that are expensive on first hit.

    Runs in a daemon thread so worker startup is not blocked. The top-risk
    query takes ~15s cold; any request that arrives before warm-up completes
    will trigger its own fill and then benefit from the cached result.
    """
    from models.database import SessionLocal
    from api.routes.neighborhoods import (
        get_top_risk_neighborhoods,
        list_neighborhoods_geojson,
    )

    db = SessionLocal()
    try:
        get_top_risk_neighborhoods(
            request=StarletteRequest(scope=_WARMUP_SCOPE),
            response=Response(),
            limit=10,
            db=db,
        )
        logger.info("cache warm: top-risk neighborhoods complete")
    except Exception:
        logger.warning("cache warm: top-risk failed", exc_info=True)

    try:
        _geojson_scope = {**_WARMUP_SCOPE, "path": "/api/neighborhoods",
                          "headers": []}
        list_neighborhoods_geojson(
            request=StarletteRequest(scope=_geojson_scope),
            response=Response(),
            db=db,
        )
        logger.info("cache warm: neighborhoods GeoJSON complete")
    except Exception:
        logger.warning("cache warm: GeoJSON failed", exc_info=True)

    try:
        from api.routes.stats import get_citywide_stats
        _stats_scope = {**_WARMUP_SCOPE, "path": "/api/stats", "headers": []}
        get_citywide_stats(
            request=StarletteRequest(scope=_stats_scope),
            response=Response(),
            db=db,
        )
        logger.info("cache warm: stats complete")
    except Exception:
        logger.warning("cache warm: stats failed", exc_info=True)
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app):
    """
    No-op lifespan. APScheduler removed — scheduling handled by system cron.
    api/main.py imports this symbol; it must remain a valid asynccontextmanager.
    """
    logger.info("API starting — nightly pipeline runs via system cron at 2:00 AM UTC")
    threading.Thread(target=_warm_caches, daemon=True, name="cache-warm").start()
    yield
    logger.info("API shutting down")
