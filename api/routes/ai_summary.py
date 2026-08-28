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
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.freshness import feed_anchor, staleness_days, window_sql
from api.permit_kinds import DECONVERSION_PARAMS, deconversion_sql
from api.ratelimit import per_worker
from models.database import get_db
from scoring.tiers import tier
from models.neighborhoods import Neighborhood
from models.scores import DisplacementScore
from api.routes.neighborhoods import (DISPLACEMENT_COMPLAINT_TYPES, _fetch_raw_counts,
                                      _borough_from_zip, _SIGNAL_LABELS)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/neighborhoods", tags=["ai-summary"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)

MODEL = "claude-opus-5"
EFFORT = "medium"

# Thinking tokens are charged against max_tokens, so the old 400 would have starved the
# paragraph rather than shortened it. The length limit that matters is in the prompt.
MAX_TOKENS = 2000
REQUEST_TIMEOUT = 30.0  # seconds — user-facing, but a cold read is worth waiting out

# Hard ceiling on model calls per UTC day, across every client. A miss past this point
# falls back to the deterministic summary. A generation measures ~1,600 input and ~260
# output tokens, so the ceiling is worth about seven dollars of a runaway day and a
# sweep of all 177 ZIPs costs under three. Real traffic never approaches it: a
# ZIP is generated once per scoring run and every later view is a cache hit.
DAILY_GENERATION_CAP = 500

_SYSTEM_PROMPT = (
    "You write the short read that sits under a neighborhood's displacement numbers on "
    "PulseCities. The reader is usually a tenant, an organizer, or a reporter who has just "
    "typed in a ZIP code and has no way of knowing whether the numbers in front of them are "
    "normal for New York.\n\n"
    "You are given the figures for one NYC ZIP: the score and its citywide rank, where each "
    "signal ranks against the other ZIPs, counts for the past year with a rate per 1,000 "
    "apartments, the same counts against a year earlier, and a few named specifics. Your job "
    "is the part the numbers cannot do for themselves. Say what kind of pressure this is, "
    "whether it is unusual, and which way it is moving.\n\n"
    "- Open with the most useful thing you can say about this place. That is almost never the "
    "composite score, and it is never the neighborhood's name followed by its score.\n"
    "- Compare, do not restate. A count on its own means nothing to a reader. A rank among "
    "the city's ZIP codes, a rate per 1,000 apartments, or a change against last year is what "
    "tells them where they stand, and at least one of those belongs in the answer.\n"
    "- Signal scores and per-1,000 rates are different scales. Never compare a rate against a "
    "median signal score, and never call one a multiple of the other.\n"
    "- Say what the pressure looks like on the ground. Evictions, complaints and violations "
    "land on the people already living there. Purchases, renovation permits and filings to "
    "remove homes land on the buildings. Those are different stories and the reader should "
    "learn which one they are in.\n"
    "- If the signals here are quiet or falling, say so plainly. A calm ZIP is a useful answer "
    "and must not be dressed up as a warning.\n"
    "- Work strictly from those figures. Do not invent addresses, dollar amounts, dates, "
    "landlord names, or any number you were not given, and do not speculate about causes or "
    "intent.\n"
    "- Say nothing at all about a figure marked not measured. Not measured is not zero.\n"
    "- Where a buyer is named, a clause on it usually earns its space. Report only what the "
    "record says, which is that the company took title to that many properties.\n"
    "- Never describe a window as running to today. Each window ends where its records end.\n"
    "- Three or four sentences, under 110 words. Plain words, the way an experienced housing "
    "reporter writes: direct, specific, no hedging filler and no marketing tone. Never use an "
    "em dash; write two sentences or rephrase instead.\n"
    "- Output only the paragraph. No preamble, no heading, no bullet points, no closing "
    "question, and no advice about who to call."
)

# Bands come from scoring.tiers so the summary's tier word can never contradict
# the colour a reader sees on the map.
def _tier(score: float) -> str:
    return tier(score)


# --- caching -----------------------------------------------------------------
# Two layers, and the second one is why a reader does not wait.
#
# L1 is this dict: zip -> (summary, score_key, generated_at_iso), per process.
# L2 is the ai_summaries table, which every worker shares and which survives a
# reload. The read used to live only in L1, so each of the two workers paid its
# own cold generation per ZIP and a deploy threw all of it away. On a day with
# eight deploys that is eight waits per ZIP for whoever arrived first.
#
# score_key pins both layers to the scoring run, so a re-scored ZIP regenerates
# and an unchanged one never does. scripts/precompute_reads.py fills L2 straight
# after the nightly scoring, which is what makes a cold read rare rather than
# merely cheap.
_cache: dict[str, tuple[str, int, str]] = {}
_cache_lock = threading.Lock()


# How far a score may drift before the read is worth regenerating. Regenerating
# on any change at all costs about $45 a month and buys nothing: a ZIP that moves
# from 41.3 to 41.5 produces the same paragraph, because the read speaks in ranks,
# rates and direction of travel and none of those turn on a tenth of a point.
# Measured over 21 nights: 105 ZIPs a night move by 0.1 or more and 14 move by a
# full point, so this threshold is the difference between $45 a month and $6.
#
# A tier crossing always regenerates whatever the size of the move, because that
# is the one change that reframes the whole paragraph.
SCORE_DRIFT_TOLERANCE = 1.0


def is_fresh(stored_score: float | None, current_score: float) -> bool:
    """Would the read still say the same thing at this score?"""
    if stored_score is None:
        return False
    if tier(stored_score) != tier(current_score):
        return False
    return abs(current_score - stored_score) < SCORE_DRIFT_TOLERANCE


def _read_stored(db: Session, zip_code: str, score: float) -> tuple[str, str] | None:
    """The stored read for this ZIP, when it is still true of this score."""
    row = db.execute(text(
        "SELECT summary, generated_at, score FROM ai_summaries WHERE zip_code = :zip"
    ), {"zip": zip_code}).first()
    if not row or not is_fresh(row[2], score):
        return None
    return row[0], row[1].isoformat()


def store_summary(db: Session, zip_code: str, key: int, summary: str, model: str,
                  score: float | None = None) -> str:
    """Write a generated read where every worker can find it. Returns its stamp.

    Never raises into the request: a failed write costs a regeneration later,
    which is a great deal better than a 503 in front of a reader.
    """
    generated_at = datetime.now(timezone.utc)
    try:
        db.execute(text("""
            INSERT INTO ai_summaries (zip_code, score_key, score, summary, model, generated_at)
            VALUES (:zip, :key, :score, :summary, :model, :at)
            ON CONFLICT (zip_code) DO UPDATE SET
                score_key = EXCLUDED.score_key,
                score = EXCLUDED.score,
                summary = EXCLUDED.summary,
                model = EXCLUDED.model,
                generated_at = EXCLUDED.generated_at
        """), {"zip": zip_code, "key": key, "score": score, "summary": summary,
               "model": model, "at": generated_at})
        db.commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning("could not store the read for %s: %r", zip_code, exc)
        db.rollback()
    return generated_at.isoformat()

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


def _rank(values: list[float], mine: float) -> int:
    """Competition rank, 1 = highest. Ties share a rank rather than inventing an order."""
    return 1 + sum(1 for v in values if v > mine)


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    n = len(ordered)
    if not n:
        return 0.0
    return ordered[n // 2] if n % 2 else (ordered[n // 2 - 1] + ordered[n // 2]) / 2


def _gather_context(db: Session, zip_code: str, breakdown: dict) -> dict:
    """The comparative figures, which are the only ones a reader cannot work out alone.

    A count is unreadable on its own: nobody knows whether 441 evictions is a lot. A rank
    among the city's ZIPs, a rate per 1,000 apartments and the same window a year earlier
    are what make it mean something, so they are what the model is handed.

    Every query here is bounded to one ZIP or to the 177-row scores table, and the whole
    set measured between 0.4s and 0.9s. Anything slower belongs in the nightly run, not
    on a page load.
    """
    ctx: dict = {}

    rows = db.execute(text(
        "SELECT zip_code, score, signal_breakdown FROM displacement_scores "
        "WHERE score IS NOT NULL"
    )).fetchall()
    scores = [(str(r[0]), float(r[1]), r[2] or {}) for r in rows]
    ctx["zip_total"] = len(scores)
    mine = next((s for s in scores if s[0] == zip_code), None)
    if mine:
        ctx["composite_rank"] = _rank([s[1] for s in scores], mine[1])
    ctx["signal_standing"] = {}
    for key, value in (breakdown or {}).items():
        column = [float((s[2] or {}).get(key) or 0.0) for s in scores]
        # A signal that is zero for every ZIP is dormant, not calm. rs_unit_loss has been
        # zero citywide since the DHCR multi-year dataset was decommissioned, and ranking
        # it would hand every ZIP a rank of 1 for a signal nobody is measuring.
        ctx["signal_standing"][key] = {
            "dormant": not any(v > 0 for v in column),
            "rank": _rank(column, float(value or 0.0)),
            "median": _median(column),
        }

    stock = db.execute(text(
        "SELECT COUNT(*) FILTER (WHERE units_res > 0), COALESCE(SUM(units_res), 0) "
        "FROM parcels WHERE zip_code = :zip"
    ), {"zip": zip_code}).fetchone()
    ctx["buildings"] = int(stock[0] or 0)
    ctx["apartments"] = int(stock[1] or 0)

    # The oldest snapshot at least 80 days back, so the comparison is a season rather
    # than yesterday's noise. Absent for a ZIP scored for the first time this quarter.
    past = db.execute(text(
        "SELECT composite_score, scored_at FROM score_history WHERE zip_code = :zip "
        "AND scored_at <= CURRENT_DATE - INTERVAL '80 days' ORDER BY scored_at DESC LIMIT 1"
    ), {"zip": zip_code}).fetchone()
    if past and past[0] is not None:
        ctx["past_score"] = float(past[0])
        ctx["past_score_date"] = past[1]

    params = {"zip": zip_code, "ctypes": list(DISPLACEMENT_COMPLAINT_TYPES),
              "anchor": feed_anchor(db)}
    recent = db.execute(text("""
        SELECT
          (SELECT COUNT(*) FROM evictions_raw WHERE zip_code = :zip AND eviction_type ILIKE 'R%'
             AND executed_date >= CURRENT_DATE - INTERVAL '90 days'),
          (SELECT COUNT(*) FROM evictions_raw WHERE zip_code = :zip AND eviction_type ILIKE 'R%'
             AND executed_date >= CURRENT_DATE - INTERVAL '455 days'
             AND executed_date <  CURRENT_DATE - INTERVAL '365 days'),
          (SELECT COUNT(*) FROM complaints_raw WHERE zip_code = :zip AND complaint_type = ANY(:ctypes)
             AND created_date >= CURRENT_DATE - INTERVAL '90 days'),
          (SELECT COUNT(*) FROM complaints_raw WHERE zip_code = :zip AND complaint_type = ANY(:ctypes)
             AND created_date >= CURRENT_DATE - INTERVAL '455 days'
             AND created_date <  CURRENT_DATE - INTERVAL '365 days')
    """), params).fetchone()
    ctx["evictions_90"], ctx["evictions_90_prior"] = int(recent[0] or 0), int(recent[1] or 0)
    ctx["complaints_90"], ctx["complaints_90_prior"] = int(recent[2] or 0), int(recent[3] or 0)

    # Deed windows end at the last published deed, never at today. api.freshness.window_sql.
    ctx["llc_90"] = int(db.execute(text(f"""
        SELECT COUNT(*) FROM ownership_raw o JOIN parcels p ON o.bbl = p.bbl
        WHERE p.zip_code = :zip AND o.party_type = '2'
          AND o.doc_type IN ('DEED', 'DEEDP', 'ASST')
          AND {window_sql('o.doc_date', 90)} AND p.units_res > 0
          AND o.party_name_normalized LIKE '%LLC%'
          AND NOT EXISTS (
              SELECT 1 FROM ownership_raw seller
              WHERE seller.document_id = o.document_id AND seller.party_type = '1'
                AND seller.party_name_normalized LIKE '%LLC%')
    """), params).scalar() or 0)
    ctx["deed_lag_days"] = staleness_days("acris")

    buyer = db.execute(text(f"""
        SELECT o.party_name_normalized, COUNT(*) AS cnt
        FROM ownership_raw o JOIN parcels p ON o.bbl = p.bbl
        WHERE p.zip_code = :zip AND o.party_type = '2'
          AND o.doc_type IN ('DEED', 'DEEDP', 'ASST')
          AND {window_sql('o.doc_date', 365)} AND p.units_res > 0
          AND o.party_name_normalized LIKE '%LLC%'
          AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
          AND o.party_name_normalized NOT ILIKE '%LENDING%'
          AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
          AND NOT EXISTS (
              SELECT 1 FROM ownership_raw seller
              WHERE seller.document_id = o.document_id AND seller.party_type = '1'
                AND seller.party_name_normalized LIKE '%LLC%')
        GROUP BY 1 ORDER BY cnt DESC, 1 LIMIT 1
    """), params).fetchone()
    if buyer and int(buyer[1]) > 1:
        ctx["top_buyer"] = (str(buyer[0]), int(buyer[1]))

    # Filings that propose fewer homes than the building holds. api.permit_kinds owns the
    # rule; read raw these counts are unusable, and the comment there says why.
    dec = db.execute(text(f"""
        SELECT COUNT(*), COALESCE(SUM(pr.units_existing - pr.units_proposed), 0)
        FROM permits_raw pr JOIN parcels p ON p.bbl = pr.bbl
        WHERE p.zip_code = :zip AND pr.filing_date >= CURRENT_DATE - INTERVAL '365 days'
          AND {deconversion_sql('pr', 'p')}
    """), {**params, **DECONVERSION_PARAMS}).fetchone()
    ctx["deconversions"] = (int(dec[0] or 0), int(dec[1] or 0))

    return ctx


def _build_facts(name, borough, zip_code, score, breakdown, raw_counts, context=None) -> str:
    """Render the grounding numbers as a compact, unambiguous block for the model.

    Pure by design: _gather_context does the database work, this turns it into text, and
    an empty context still renders a valid block. Anything the context could not supply
    is simply absent rather than guessed at.
    """
    ctx = context or {}
    apartments = int(ctx.get("apartments") or 0)
    total = int(ctx.get("zip_total") or 0)

    def per_k(n: int) -> str:
        return f"{n * 1000.0 / apartments:.1f} per 1,000 apartments" if apartments else "rate unavailable"

    lines = [
        f"Neighborhood: {name or 'Unknown'}",
        f"Borough: {borough or 'Unknown'}",
        f"ZIP: {zip_code}",
    ]
    if apartments:
        lines.append(f"Housing stock: {ctx['buildings']:,} residential buildings, "
                     f"{apartments:,} apartments")
    rank_note = f", rank {ctx['composite_rank']} of {total} scored NYC ZIP codes" if total and ctx.get("composite_rank") else ""
    lines.append(f"Composite displacement-pressure score: {round(score, 1)} out of 100 "
                 f"({_tier(score)} pressure){rank_note}")
    if ctx.get("past_score") is not None:
        lines.append(f"Score on {ctx['past_score_date']}: {ctx['past_score']:.1f} "
                     f"(change since then: {score - ctx['past_score']:+.1f})")

    standing = ctx.get("signal_standing") or {}
    header = "Signal contributions to the score (0 to 100, higher means more pressure from that signal)"
    if standing:
        header += (". Each is already adjusted for how many apartments this ZIP holds, so a "
                   "rank compares like with like and rank 1 is the most pressured ZIP in the city")
    lines += ["", header + ":"]
    for key, value in sorted((breakdown or {}).items(), key=lambda kv: kv[1] or 0, reverse=True):
        label = _SIGNAL_LABELS.get(key, key)
        stat = standing.get(key) or {}
        if stat.get("dormant"):
            lines.append(f"  - {label}: not measured, no ZIP in the city has a figure for this")
        elif stat:
            lines.append(f"  - {label}: {round(float(value or 0), 1)}, rank {stat['rank']} of "
                         f"{total}, median signal score across all ZIPs {stat['median']:.1f}")
        else:
            lines.append(f"  - {label}: {round(float(value or 0), 1)}")

    lines += ["", "Raw event counts over the past 365 days:"]
    raw_label = {
        "llc_acquisitions": "LLC purchases of residential property",
        "evictions": "residential evictions executed by a marshal",
        "permits": "renovation permits on buildings with 3+ apartments",
        "complaint_rate": "displacement-related tenant complaints",
    }
    for key, value in (raw_counts or {}).items():
        lines.append(f"  - {raw_label.get(key, key)}: {int(value):,} ({per_k(int(value))})")

    if "evictions_90" in ctx:
        lines += ["", "Direction of travel, the most recent 90 days against the same 90 days "
                      "one year earlier:",
                  f"  - evictions executed: {ctx['evictions_90']:,} now against "
                  f"{ctx['evictions_90_prior']:,} a year ago",
                  f"  - tenant complaints: {ctx['complaints_90']:,} now against "
                  f"{ctx['complaints_90_prior']:,} a year ago",
                  f"  - LLC purchases in the most recent 90 days of deed records: "
                  f"{ctx['llc_90']:,}. Deeds are published about {ctx['deed_lag_days']} days "
                  f"behind, so that window ends where the records end, not today"]

    specifics = []
    if ctx.get("top_buyer"):
        specifics.append(f"  - Most active LLC buyer here in the past year: "
                         f"{ctx['top_buyer'][0]}, {ctx['top_buyer'][1]} properties taken title to")
    elif "top_buyer" in ctx or ctx.get("deconversions") is not None:
        specifics.append("  - No single LLC took title to more than one property here in the past year")
    if ctx.get("deconversions"):
        filings, homes = ctx["deconversions"]
        specifics.append(
            f"  - Filings proposing to remove homes: {filings} in the past year, {homes} homes"
            if filings else
            "  - Filings proposing to remove homes: none in the past year")
    if specifics:
        lines += ["", "Named specifics:"] + specifics

    return "\n".join(lines)


@router.get("/{zip_code}/summary")
# This route calls Anthropic on a cache miss, so the limit is a spend control.
# Declared as the whole-process ceiling; per_worker() divides it by the worker
# count, because slowapi would otherwise allow it once per worker.
@limiter.limit(per_worker(20, "hour"))
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
    if cached and cached[1] == key:  # L1 is exact; L2 below carries the drift rule
        response.headers["Cache-Control"] = "public, max-age=3600"
        return {"zip_code": zip_code, "summary": cached[0], "model": MODEL,
                "generated_at": cached[2], "cached": True}

    stored = _read_stored(db, zip_code, score)
    if stored:
        with _cache_lock:
            _cache[zip_code] = (stored[0], key, stored[1])
        response.headers["Cache-Control"] = "public, max-age=3600"
        return {"zip_code": zip_code, "summary": stored[0], "model": MODEL,
                "generated_at": stored[1], "cached": True}

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
    breakdown = score_row.signal_breakdown or {}
    facts = _build_facts(
        hood.name if hood else None,
        _borough_from_zip(zip_code),
        zip_code,
        score,
        breakdown,
        _fetch_raw_counts(db, zip_code),
        _gather_context(db, zip_code, breakdown),
    )

    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            thinking={"type": "adaptive"},
            output_config={"effort": EFFORT},
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

    usage = getattr(message, "usage", None)
    if usage is not None:
        # Spend visibility: `journalctl -u pulsecities | grep 'summary usage'` audits real
        # token consumption rather than estimating it. The app's root handler writes to
        # stdout, which under systemd is the journal, not gunicorn's error file. Output
        # tokens include thinking.
        logger.info("summary usage zip=%s in=%s out=%s model=%s effort=%s",
                    zip_code, usage.input_tokens, usage.output_tokens, MODEL, EFFORT)

    summary = "".join(b.text for b in message.content if b.type == "text").strip()
    if not summary:
        raise HTTPException(status_code=503, detail="AI summary is not available right now.")

    generated_at = store_summary(db, zip_code, key, summary, MODEL, score)
    with _cache_lock:
        _cache[zip_code] = (summary, key, generated_at)

    response.headers["Cache-Control"] = "public, max-age=3600"
    return {"zip_code": zip_code, "summary": summary, "model": MODEL,
            "generated_at": generated_at, "cached": False}
