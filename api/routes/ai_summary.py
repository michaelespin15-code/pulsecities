"""
AI neighborhood summary — a plain-English read of the displacement signals for a ZIP.

GET /api/neighborhoods/{zip}/summary

The model is handed only the figures already computed for the ZIP (the same numbers
the /score endpoint serves) and told to explain them in two or three sentences. It
never sees addresses, owner names, or anything it could turn into an unfounded claim,
and it is instructed to work strictly from the provided counts. This is the editorial
layer on top of the deterministic score, not a replacement for it.

Cost and abuse controls, in order of importance:
  1. Per-IP rate limit. A miss costs a model call, so the endpoint is gated per client.
  2. Per-ZIP cache keyed on the score. Each neighborhood is generated at most once per
     nightly scoring run; a changed score regenerates. Cache hits are free.
  3. A daily generation cap across all clients as a hard ceiling on spend.
  4. Graceful 503 when ANTHROPIC_API_KEY is absent or the model call fails, so the page
     degrades to the deterministic summary instead of erroring.
"""

import logging
import os
import threading
import time
from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from models.database import get_db
from models.neighborhoods import Neighborhood
from models.scores import DisplacementScore
from api.routes.neighborhoods import _fetch_raw_counts, _borough_from_zip, _SIGNAL_LABELS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/neighborhoods", tags=["ai-summary"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)

MODEL = "claude-opus-4-8"
MAX_TOKENS = 400
REQUEST_TIMEOUT = 20.0  # seconds — this is user-facing, fail fast rather than hang

# Hard ceiling on model calls per UTC day, across every client. A miss past this point
# falls back to the deterministic summary. Generous for real traffic, cheap insurance
# against a cache-busting loop running up a bill.
DAILY_GENERATION_CAP = 500

_SYSTEM_PROMPT = (
    "You write one short, plain-English paragraph explaining what a neighborhood's "
    "displacement-pressure data shows, for a resident or journalist reading it for the "
    "first time. You are given the figures for one NYC ZIP code. Work strictly from "
    "those figures. Do not invent addresses, landlord names, dollar amounts, dates, or "
    "any number you were not given, and do not speculate about causes or intent. Name "
    "the one or two signals that are most elevated and say what they mean in everyday "
    "terms. Two to three sentences. Write like an experienced housing reporter: direct, "
    "specific, no hedging filler and no marketing tone. Never use an em dash; write two "
    "sentences or rephrase instead. Output only the paragraph: no preamble, no heading, "
    "no bullet points, no closing question."
)

# Bands mirror the map legend and weekly digest (the canonical thresholds) so the
# summary's tier word never contradicts the color a reader sees on the map.
_TIERS = [
    (85, "Critical"),
    (67, "High"),
    (34, "Moderate"),
    (0, "Low"),
]


def _tier(score: float) -> str:
    for threshold, label in _TIERS:
        if score >= threshold:
            return label
    return "Low"


# --- caching -----------------------------------------------------------------
# zip -> (summary, score_key, generated_at_iso). score_key pins the cache to the
# scoring run, so a re-scored ZIP regenerates on its next view.
_cache: dict[str, tuple[str, int, str]] = {}
_cache_lock = threading.Lock()

# Daily spend cap state.
_gen_day: date | None = None
_gen_count = 0
_gen_lock = threading.Lock()

# After a model-call failure (exhausted credits, provider outage), skip the
# API entirely for a while: every panel open was otherwise paying a ~3.5s
# doomed round-trip, and the panel flashed a spinner before hiding.
_FAILURE_COOLDOWN = 600.0  # seconds
_cooldown_until = 0.0

# Lazily constructed Anthropic client (reused across requests).
_client = None
_client_lock = threading.Lock()


def _score_key(score: float) -> int:
    # Round to one decimal so a re-score that nudges the number regenerates, but
    # floating-point noise on an unchanged score does not.
    return int(round(score * 10))


def _get_client():
    global _client
    if _client is not None:
        return _client
    if not os.getenv("ANTHROPIC_API_KEY"):
        return None
    with _client_lock:
        if _client is None:
            import anthropic
            _client = anthropic.Anthropic(timeout=REQUEST_TIMEOUT, max_retries=1)
    return _client


def _under_daily_cap() -> bool:
    global _gen_day, _gen_count
    with _gen_lock:
        today = datetime.now(timezone.utc).date()
        if _gen_day != today:
            _gen_day = today
            _gen_count = 0
        if _gen_count >= DAILY_GENERATION_CAP:
            return False
        _gen_count += 1
        return True


def _build_facts(name, borough, zip_code, score, breakdown, raw_counts) -> str:
    """Render the grounding numbers as a compact, unambiguous block for the model."""
    lines = [
        f"Neighborhood: {name or 'Unknown'}",
        f"Borough: {borough or 'Unknown'}",
        f"ZIP: {zip_code}",
        f"Composite displacement-pressure score: {round(score, 1)} out of 100 ({_tier(score)} pressure)",
        "",
        "Signal contributions to the score (0 to 100, higher means more pressure from that signal):",
    ]
    for key, value in sorted(breakdown.items(), key=lambda kv: kv[1] or 0, reverse=True):
        label = _SIGNAL_LABELS.get(key, key)
        lines.append(f"  - {label}: {round(float(value or 0), 1)}")
    lines.append("")
    lines.append("Raw event counts over the past 365 days:")
    raw_label = {
        "llc_acquisitions": "LLC property acquisitions",
        "evictions": "residential eviction filings",
        "permits": "renovation permit filings on 3+ unit buildings",
        "complaint_rate": "displacement-related tenant complaints",
    }
    for key, value in raw_counts.items():
        lines.append(f"  - {raw_label.get(key, key)}: {int(value)}")
    return "\n".join(lines)


@router.get("/{zip_code}/summary")
@limiter.limit("20/hour")
def get_neighborhood_summary(
    request: Request,
    response: Response,
    zip_code: str,
    db: Session = Depends(get_db),
):
    """Plain-English AI read of a ZIP's displacement signals. Cached per scoring run."""
    global _cooldown_until
    if not (len(zip_code) == 5 and zip_code.isdigit()):
        raise HTTPException(status_code=400, detail="zip_code must be 5 digits")

    score_row = (
        db.query(DisplacementScore)
        .filter(DisplacementScore.zip_code == zip_code)
        .first()
    )
    if not score_row or score_row.score is None:
        raise HTTPException(status_code=404, detail=f"No score data for ZIP {zip_code}.")

    score = float(score_row.score)
    key = _score_key(score)

    with _cache_lock:
        cached = _cache.get(zip_code)
    if cached and cached[1] == key:
        response.headers["Cache-Control"] = "public, max-age=3600"
        return {"zip_code": zip_code, "summary": cached[0], "model": MODEL,
                "generated_at": cached[2], "cached": True}

    client = _get_client()
    if client is None:
        # No key configured — the page falls back to the deterministic summary.
        raise HTTPException(status_code=503, detail="AI summary is not available right now.")

    if time.monotonic() < _cooldown_until:
        raise HTTPException(status_code=503, detail="AI summary is not available right now.")

    if not _under_daily_cap():
        logger.warning("AI summary daily cap reached; serving 503 for %s", zip_code)
        raise HTTPException(status_code=503, detail="AI summary is not available right now.")

    hood = db.query(Neighborhood).filter(Neighborhood.zip_code == zip_code).first()
    facts = _build_facts(
        hood.name if hood else None,
        _borough_from_zip(zip_code),
        zip_code,
        score,
        score_row.signal_breakdown or {},
        _fetch_raw_counts(db, zip_code),
    )

    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": facts}],
        )
    except Exception as exc:  # noqa: BLE001 — any SDK/network failure degrades gracefully
        _cooldown_until = time.monotonic() + _FAILURE_COOLDOWN
        logger.warning("AI summary generation failed for %s (cooling down %ds): %r",
                       zip_code, int(_FAILURE_COOLDOWN), exc)
        raise HTTPException(status_code=503, detail="AI summary is not available right now.")

    if message.stop_reason == "refusal":
        logger.warning("AI summary refused for %s", zip_code)
        raise HTTPException(status_code=503, detail="AI summary is not available right now.")

    summary = "".join(b.text for b in message.content if b.type == "text").strip()
    if not summary:
        raise HTTPException(status_code=503, detail="AI summary is not available right now.")

    generated_at = datetime.now(timezone.utc).isoformat()
    with _cache_lock:
        _cache[zip_code] = (summary, key, generated_at)

    response.headers["Cache-Control"] = "public, max-age=3600"
    return {"zip_code": zip_code, "summary": summary, "model": MODEL,
            "generated_at": generated_at, "cached": False}
