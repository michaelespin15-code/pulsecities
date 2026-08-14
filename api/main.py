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
from fastapi.responses import HTMLResponse, JSONResponse, Response
from slowapi import Limiter
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
    description="NYC displacement signals from public records",
    version="0.1.0",
    lifespan=lifespan,
)

app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def rate_limit_exceeded(request, exc: RateLimitExceeded):
    """slowapi's stock handler answers a rejection with Retry-After: 0 and
    reports the full quota as X-RateLimit-Remaining. Both are backwards: the
    first tells a well-behaved client to retry immediately, which is how a
    throttle turns into a hot loop, and the second says there is budget left on
    the request we just refused. Answer with the window length and a remaining
    of zero, which is what the caller needs to back off correctly."""
    window = 60
    try:
        window = int(exc.limit.limit.get_expiry())
    except Exception:  # pragma: no cover - shape depends on the limits release
        logger.warning("could not read rate-limit window; defaulting Retry-After to 60s")
    return JSONResponse(
        status_code=429,
        content={"error": f"Rate limit exceeded: {exc.detail}"},
        headers={
            "Retry-After": str(window),
            "X-RateLimit-Remaining": "0",
        },
    )

# Catch-all 500 page. Without it an unhandled DB error on an SSR route serves
# Starlette's bare "Internal Server Error" text to a browser. Deliberately
# self-contained: no fonts, no assets, nothing that can fail with it.
_ERROR_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="robots" content="noindex">
<title>Something went wrong | PulseCities</title>
<style>
body{margin:0;background:#0f172a;color:#f1f5f9;font-family:system-ui,-apple-system,sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;text-align:center;line-height:1.6}
main{padding:24px;max-width:420px}
h1{font-size:1.25rem;font-weight:600;margin:0 0 10px}
p{font-size:.9rem;color:#94a3b8;margin:0 0 24px}
a{display:inline-block;background:#f97316;color:#fff;font-size:.85rem;font-weight:500;padding:10px 20px;border-radius:6px;text-decoration:none}
</style>
</head>
<body>
<main>
<h1>Something went wrong on our end</h1>
<p>The error is logged. Give it a minute and try again.</p>
<a href="/">Back to PulseCities</a>
</main>
</body>
</html>"""


@app.exception_handler(Exception)
async def unhandled_error(request, exc):
    # Starlette re-raises after this handler runs, so the traceback still lands
    # in the error log; this only shapes what the client sees.
    if "text/html" in request.headers.get("accept", ""):
        return HTMLResponse(_ERROR_HTML, status_code=500)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})

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
async def html_no_stale_cache(request, call_next):
    """SSR responses shipped with no cache headers, so browsers heuristically
    cached them and served stale pages after deploys. HTML must revalidate;
    API responses and anything that sets its own policy are left alone."""
    response = await call_next(request)
    ctype = response.headers.get("content-type", "")
    if ctype.startswith("text/html") and "cache-control" not in response.headers:
        response.headers["Cache-Control"] = "no-cache"
    return response


@app.middleware("http")
async def strip_bogus_retry_after(request, call_next):
    """slowapi's headers_enabled stamps Retry-After on every response it counts,
    including 200s, and fills it with the remaining request count rather than a
    delay. Retry-After is only defined for 429/503 and 3xx; on a 200 it is noise
    that well-behaved clients and CDNs are entitled to act on. The X-RateLimit-*
    trio already carries the quota, so drop the header everywhere it has no
    meaning and leave it intact where it does."""
    response = await call_next(request)
    if response.status_code not in (429, 503) and "retry-after" in response.headers:
        del response.headers["retry-after"]
    return response


@app.middleware("http")
async def head_as_get(request, call_next):
    """Uptime bots and link checkers probe with HEAD, which FastAPI's GET-only
    routes answer with 405. Serve the GET internally and drop the body.
    Content-Length is kept as-is; HEAD advertises the body it suppresses."""
    if request.method != "HEAD":
        return await call_next(request)
    request.scope["method"] = "GET"
    response = await call_next(request)
    # Drain the wrapped response so the downstream app task completes.
    async for _ in response.body_iterator:
        pass
    return Response(
        status_code=response.status_code,
        headers=dict(response.headers),
        background=response.background,
    )


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
