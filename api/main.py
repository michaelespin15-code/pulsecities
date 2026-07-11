"""
PulseCities FastAPI application entry point.

Run (development):
    uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

Run (production):
    gunicorn -w 2 -k uvicorn.workers.UvicornWorker api.main:app
"""

import hashlib
import logging
import time

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
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

# Partner API keys (see /developers). The public tier is keyless; when a
# request carries X-API-Key the key must resolve to an active row or the
# request fails loudly. A silently ignored bad key would look like public
# access to the caller and like no partner traffic to us. Lookups cache
# for a minute so keyed traffic doesn't add a query per request, and
# last_used_at advances at most once per cache window.
_API_KEY_CACHE: dict[str, tuple[dict | None, float]] = {}
_API_KEY_TTL = 60.0

# Junk-key flood guard. Unique bogus keys bypass the result cache (every one
# is a miss), so cap DB lookups per client IP per window; past the cap the
# 401 is served without touching the database.
_KEY_FAIL_WINDOW = 60.0
_KEY_FAIL_LIMIT = 20
_key_fail_counts: dict[str, tuple[int, float]] = {}


def _key_lookups_exhausted(client_ip: str) -> bool:
    now = time.monotonic()
    count, reset_at = _key_fail_counts.get(client_ip, (0, 0.0))
    if now >= reset_at:
        count, reset_at = 0, now + _KEY_FAIL_WINDOW
    if count >= _KEY_FAIL_LIMIT:
        return True
    if len(_key_fail_counts) > 1024:
        for ip, (_, r) in list(_key_fail_counts.items()):
            if now >= r:
                del _key_fail_counts[ip]
    _key_fail_counts[client_ip] = (count + 1, reset_at)
    return False


def _prune_key_cache() -> None:
    """Drop expired entries; if still over the cap, drop the oldest. Never
    wholesale-clears, so a junk-key flood can't evict valid partner entries.
    Iterates over snapshots throughout: other threadpool threads mutate the
    cache concurrently, and min() over a live dict can raise mid-iteration."""
    if len(_API_KEY_CACHE) <= 256:
        return
    now = time.monotonic()
    for k, (_, exp) in list(_API_KEY_CACHE.items()):
        if now >= exp:
            _API_KEY_CACHE.pop(k, None)
    while len(_API_KEY_CACHE) > 256:
        snapshot = list(_API_KEY_CACHE.items())
        if not snapshot:
            break
        oldest = min(snapshot, key=lambda kv: kv[1][1])[0]
        _API_KEY_CACHE.pop(oldest, None)


def _resolve_api_key(raw_key: str) -> dict | None:
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    cached = _API_KEY_CACHE.get(key_hash)
    if cached and time.monotonic() < cached[1]:
        return cached[0]

    from sqlalchemy import text
    from models.database import SessionLocal
    try:
        db = SessionLocal()
        try:
            row = db.execute(
                text("SELECT label, tier FROM api_keys WHERE key_hash = :h AND active"),
                {"h": key_hash},
            ).fetchone()
            info = {"label": row.label, "tier": row.tier} if row else None
            if row:
                db.execute(
                    text("UPDATE api_keys SET last_used_at = now() WHERE key_hash = :h"),
                    {"h": key_hash},
                )
                db.commit()
        finally:
            db.close()
    except Exception:
        # A DB outage must degrade keyed requests to 401, not surface a raw
        # 500 from inside middleware. Not cached: the next attempt retries.
        logger.exception("api-key lookup failed; treating key as unresolvable")
        return None

    _prune_key_cache()
    _API_KEY_CACHE[key_hash] = (info, time.monotonic() + _API_KEY_TTL)
    return info


@app.middleware("http")
async def api_key_middleware(request, call_next):
    raw_key = request.headers.get("x-api-key")
    if raw_key:
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        cached = _API_KEY_CACHE.get(key_hash)
        if cached and time.monotonic() < cached[1]:
            info = cached[0]
        else:
            client_ip = (request.client.host if request.client else "") or "unknown"
            if _key_lookups_exhausted(client_ip):
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Too many key lookups. Slow down."},
                )
            # Sync SQLAlchemy off the event loop, so a cache-miss lookup
            # can't stall every other request on this worker.
            from starlette.concurrency import run_in_threadpool
            info = await run_in_threadpool(_resolve_api_key, raw_key)
        if info is None:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or revoked API key."},
            )
        request.state.api_key = info
    return await call_next(request)


# Gunicorn listens on a unix socket, so the peer has no IP address and an
# IP allowlist can never match. Only nginx can reach the socket, so trusting
# every peer is equivalent to trusting nginx. Without this, X-Forwarded-Proto
# is ignored and trailing-slash redirects downgrade to http:// (mixed content).
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
app.add_middleware(
    CORSMiddleware,
    # Deliberately open: this is a public, credential-free read API and
    # /developers invites third-party pages to call it from the browser.
    allow_origins=["*"],
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
