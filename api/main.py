"""
PulseCities FastAPI application entry point.

Run (development):
    uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

Run (production):
    gunicorn -w 2 -k uvicorn.workers.UvicornWorker api.main:app
"""

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from api.routes import health, neighborhoods, properties, blocks, score_history, pulse, search, subscribe, stats, operators, ops, frontend, og_images, schedule, briefs, status, flips, radar, ai_summary, badges
from config.logging_config import configure_logging
from scheduler.manager import lifespan

configure_logging()
logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address, headers_enabled=True)

app = FastAPI(
    title="PulseCities API",
    description="NYC displacement intelligence — powered by public data",
    version="0.1.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Gunicorn listens on a unix socket, so the peer has no IP address and an
# IP allowlist can never match. Only nginx can reach the socket, so trusting
# every peer is equivalent to trusting nginx. Without this, X-Forwarded-Proto
# is ignored and trailing-slash redirects downgrade to http:// (mixed content).
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten to specific origins before launch
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/api")
app.include_router(status.router, prefix="/api")
app.include_router(schedule.router, prefix="/api")
app.include_router(neighborhoods.router, prefix="/api")
app.include_router(properties.router, prefix="/api")
app.include_router(blocks.router, prefix="/api")
app.include_router(score_history.router, prefix="/api")
app.include_router(pulse.router, prefix="/api")
app.include_router(flips.router, prefix="/api")
app.include_router(radar.router, prefix="/api")
app.include_router(ai_summary.router, prefix="/api")
app.include_router(search.router, prefix="/api")
app.include_router(subscribe.router, prefix="/api")
app.include_router(stats.router, prefix="/api")
app.include_router(operators.router, prefix="/api")
app.include_router(ops.router)
app.include_router(og_images.router)
app.include_router(badges.router)
app.include_router(briefs.router)
app.include_router(frontend.router)
