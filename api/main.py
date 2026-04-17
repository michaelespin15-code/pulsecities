"""
PulseCities FastAPI application entry point.

Run (development):
    uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

Run (production — single worker required for APScheduler):
    gunicorn -w 1 -k uvicorn.workers.UvicornWorker api.main:app
"""

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from api.routes import health, neighborhoods, properties, blocks, score_history, pulse
from config.logging_config import configure_logging
from scheduler.manager import lifespan

configure_logging()
logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="PulseCities API",
    description="NYC displacement intelligence — powered by public data",
    version="0.1.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten to specific origins before launch
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/api")
app.include_router(neighborhoods.router, prefix="/api")
app.include_router(properties.router, prefix="/api")
app.include_router(blocks.router, prefix="/api")
app.include_router(score_history.router, prefix="/api")
app.include_router(pulse.router, prefix="/api")

# Static file serving — MUST come after all API routes
# FastAPI route matching is first-match; mounting at "/" before API routes
# would intercept /api/* requests and return 404 from StaticFiles.
from starlette.staticfiles import StaticFiles  # noqa: E402
app.mount("/", StaticFiles(directory="frontend", html=True), name="static")
