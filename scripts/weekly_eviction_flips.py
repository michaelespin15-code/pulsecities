"""
Weekly eviction-flip scan — the recurring finding machine.

Finds the full arc across all ACRIS data: a residential eviction, an LLC
purchase within 12 months of it, then a resale at a 25%+ markup. Diffs
against last week's state file and emails Michael the new arcs with draft
social copy for review. Nothing posts anywhere on its own; this generates
material, a human publishes it.

State lives in scripts/eviction_flips_state.json (arc keys seen so far).
First run seeds the state and reports everything as new.

Usage:
    python -m scripts.weekly_eviction_flips            # scan, email, update state
    python -m scripts.weekly_eviction_flips --dry-run  # scan and print, no email, no state write
"""

import argparse
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

import resend
from sqlalchemy import text

from config.logging_config import configure_logging
from models.database import get_scraper_db  # imports load_dotenv() as a side effect

resend.api_key = os.getenv("RESEND_API_KEY", "")

configure_logging()
logger = logging.getLogger(__name__)

STATE_PATH = Path(__file__).parent / "eviction_flips_state.json"

# Each run with new arcs appends an edition here, approved: false. The future
# public /flips editions page renders approved editions only; flipping the
# flag after review is the human gate between scan output and publication.
EDITIONS_PATH = Path(__file__).parent / "eviction_flips_editions.json"

REPORT_TO = "michaelespin15@gmail.com"

# Arc thresholds. Purchases under $100k are usually partial-interest or
# family transfers; markups under 25% are ordinary market movement.
MIN_PURCHASE = 100_000
MIN_MARKUP = 0.25
MONTHS_AFTER_EVICTION = 12

ARC_SQL = """
WITH ev AS (
    SELECT bbl, MAX(executed_date) AS ev_date, COUNT(*) AS ev_count
    FROM evictions_raw
    WHERE eviction_type = 'Residential' AND bbl IS NOT NULL
    GROUP BY bbl
),
buy AS (
    SELECT o.bbl, o.document_id AS buy_doc, o.doc_date AS buy_date,
           o.doc_amount AS buy_amt, o.party_name AS buyer,
           e.ev_date, e.ev_count,
           ROW_NUMBER() OVER (PARTITION BY o.bbl ORDER BY o.doc_date) AS rn
    FROM ownership_raw o
    JOIN ev e
      ON e.bbl = o.bbl
     AND o.doc_date > e.ev_date
     AND o.doc_date <= e.ev_date + INTERVAL ':months months'
    WHERE o.party_type = '2'
      AND o.doc_type IN ('DEED', 'DEEDP')
      AND o.doc_amount >= :min_purchase
      AND o.party_name_normalized LIKE '%LLC%'
)
SELECT b.bbl, p.address, p.zip_code, b.ev_date, b.ev_count,
       b.buy_doc, b.buy_date, b.buy_amt, b.buyer,
       s.document_id AS sell_doc, s.doc_date AS sell_date, s.doc_amount AS sell_amt
FROM buy b
JOIN LATERAL (
    SELECT document_id, doc_date, doc_amount FROM ownership_raw
    WHERE bbl = b.bbl AND party_type = '2' AND doc_type IN ('DEED', 'DEEDP')
      AND doc_date > b.buy_date AND doc_amount > b.buy_amt
    ORDER BY doc_date LIMIT 1
) s ON TRUE
LEFT JOIN parcels p ON p.bbl = b.bbl
WHERE b.rn = 1
  AND (s.doc_amount - b.buy_amt) / b.buy_amt >= :min_markup
ORDER BY s.doc_date DESC
"""


def scan(db) -> list[dict]:
    sql = ARC_SQL.replace(":months", str(MONTHS_AFTER_EVICTION))
    rows = db.execute(
        text(sql), {"min_purchase": MIN_PURCHASE, "min_markup": MIN_MARKUP}
    ).fetchall()
    arcs = []
    for r in rows:
        gain = float(r.sell_amt - r.buy_amt) / float(r.buy_amt)
        arcs.append({
            "key": f"{r.bbl}:{r.buy_doc}:{r.sell_doc}",
            "bbl": r.bbl,
            "address": r.address,
            "zip_code": r.zip_code,
            "eviction_date": r.ev_date.isoformat(),
            "eviction_count": int(r.ev_count),
            "buy_doc": r.buy_doc,
            "buy_date": r.buy_date.isoformat(),
            "buy_amt": float(r.buy_amt),
            "buyer": r.buyer,
            "sell_doc": r.sell_doc,
            "sell_date": r.sell_date.isoformat(),
            "sell_amt": float(r.sell_amt),
            "gain_pct": round(gain * 100),
        })
    return arcs


def buyer_scale(db, buyer: str) -> int:
    """How many BBLs has this buyer's exact entity acquired? Context for the report."""
    return db.execute(text("""
        SELECT COUNT(DISTINCT bbl) FROM ownership_raw
        WHERE party_type = '2' AND doc_type IN ('DEED', 'DEEDP')
          AND party_name_normalized = :name
    """), {"name": (buyer or "").upper()}).scalar() or 0


def _money(n: float) -> str:
    return f"${n:,.0f}"


def format_arc(arc: dict, scale: int) -> str:
    addr = arc["address"] or f"lot {arc['bbl']}"
    lines = [
        f"{addr}, {arc['zip_code'] or 'no zip on record'} (BBL {arc['bbl']})",
        f"  eviction executed {arc['eviction_date']}"
        + (f" ({arc['eviction_count']} dockets)" if arc["eviction_count"] > 1 else ""),
        f"  bought {arc['buy_date']} for {_money(arc['buy_amt'])} by {arc['buyer']}"
        + (f" (entity holds {scale} BBLs)" if scale > 1 else ""),
        f"  sold {arc['sell_date']} for {_money(arc['sell_amt'])}, a {arc['gain_pct']}% gain",
        f"  ACRIS docs: {arc['buy_doc']} / {arc['sell_doc']}",
    ]
    return "\n".join(lines)


def _article(pct: int) -> str:
    """'an' before gains that read as a vowel sound (80s, 11, 18), else 'a'."""
    return "an" if str(pct).startswith("8") or pct in (11, 18) else "a"


def _months_held(arc: dict) -> int:
    days = (date.fromisoformat(arc["sell_date"]) - date.fromisoformat(arc["buy_date"])).days
    return max(1, round(days / 30.4))


def _months_phrase(n: int) -> str:
    return f"{n} month" + ("s" if n != 1 else "")


def draft_post(arc: dict) -> str:
    """Draft social copy for one arc. Reviewed by a human before posting anywhere."""
    addr = arc["address"] or f"lot {arc['bbl']}"
    pct = arc["gain_pct"]
    return (
        f"Tenants were evicted at {addr.title()} in "
        f"{date.fromisoformat(arc['eviction_date']).strftime('%B %Y')}. "
        f"An LLC then bought the building for {_money(arc['buy_amt'])} and resold it "
        f"{_months_phrase(_months_held(arc))} later for "
        f"{_money(arc['sell_amt'])}. That is {_article(pct)} {pct}% gain. "
        f"Every step is public record: ACRIS docs {arc['buy_doc']} and {arc['sell_doc']}. "
        f"Found with pulsecities.com"
    )


# ---------------------------------------------------------------------------
# Ready-to-post pack: turns the week's arcs into copy that ships as-is.
# Nothing here posts on its own; it lands in the review email so a human can
# paste it. Char budgets: X 280, Bluesky 300. Each block is length-annotated so
# a too-long line is obvious before it goes out.
# ---------------------------------------------------------------------------

MAX_TWEET = 280
MAX_BLUESKY = 300

# Outlets that run displacement/landlord-accountability tips. Order is a rough
# fit-for-this-beat ranking, not a mandate.
TIP_OUTLETS = "Hell Gate, THE CITY, Gothamist, City Limits, Documented"


def _loc(arc: dict) -> str:
    addr = (arc["address"] or f"lot {arc['bbl']}").title()
    zip_ = arc["zip_code"] or ""
    return f"{addr}, {zip_}".rstrip(", ").strip()


def _top_arc(new_arcs: list[dict]) -> dict:
    """The most newsworthy arc: biggest gain, then biggest sale price."""
    return max(new_arcs, key=lambda a: (a["gain_pct"], a["sell_amt"]))


def _arc_tweet(arc: dict, reserve: int = 0) -> str:
    """One arc as a tweet body. `reserve` is room to leave for the (k/N) suffix
    the thread builder appends, so the numbered tweet still fits inside 280."""
    ev = date.fromisoformat(arc["eviction_date"]).strftime("%b %Y")
    buy = date.fromisoformat(arc["buy_date"]).strftime("%b %Y")
    pct = arc["gain_pct"]
    core = (
        f"{_loc(arc)}. Tenants evicted {ev}. An LLC bought it {buy} for "
        f"{_money(arc['buy_amt'])}, resold it {_months_phrase(_months_held(arc))} "
        f"later for {_money(arc['sell_amt'])}. That is {_article(pct)} {pct}% gain."
    )
    docs = f" ACRIS: {arc['buy_doc']}, {arc['sell_doc']}."
    # Keep the receipts when they fit; they are the credibility. Drop them first
    # if the address pushes the tweet over the limit.
    return core + docs if len(core + docs) <= MAX_TWEET - reserve else core


_THREAD_LEAD = (
    "{n} NYC buildings this week, one playbook: evict the tenants, buy the "
    "building through an LLC, flip it for a fast markup. Every step is public "
    "record. Here is what the deeds show."
)
_THREAD_CLOSE = (
    "Every eviction and deed here comes straight from NYC open records. I built "
    "pulsecities.com to surface these patterns automatically, every night. "
    "Search any block."
)


def build_x_thread(new_arcs: list[dict]) -> list[str]:
    # Order the bodies first, then number and budget together: the (k/N) suffix
    # is known only once the thread length is, and it has to count against 280.
    bodies: list[dict | str] = []
    if len(new_arcs) > 1:
        bodies.append(_THREAD_LEAD.format(n=len(new_arcs)))
    bodies.extend(new_arcs)
    bodies.append(_THREAD_CLOSE)

    total = len(bodies)
    tweets = []
    for i, body in enumerate(bodies, 1):
        suffix = f" ({i}/{total})"
        text_body = _arc_tweet(body, reserve=len(suffix)) if isinstance(body, dict) else body
        tweets.append(text_body + suffix)
    return tweets


def build_bluesky_post(new_arcs: list[dict]) -> str:
    arc = _top_arc(new_arcs)
    ev = date.fromisoformat(arc["eviction_date"]).strftime("%B %Y")
    pct = arc["gain_pct"]
    full = (
        f"Tenants were evicted at {_loc(arc)} in {ev}. An LLC bought the building "
        f"for {_money(arc['buy_amt'])} and flipped it {_months_phrase(_months_held(arc))} "
        f"later for {_money(arc['sell_amt'])}. That is {_article(pct)} {pct}% gain, "
        f"every step in NYC public record. Found with pulsecities.com"
    )
    if len(full) <= MAX_BLUESKY:
        return full
    # Shed the eviction month first, then fall back to the shorter draft copy.
    trimmed = full.replace(f" in {ev}", "")
    return trimmed if len(trimmed) <= MAX_BLUESKY else draft_post(arc)


def build_reporter_pitch(new_arcs: list[dict], scales: dict, total: int) -> str:
    arc = _top_arc(new_arcs)
    pct = arc["gain_pct"]
    ev_full = date.fromisoformat(arc["eviction_date"]).strftime("%B %Y")
    dockets = f" ({arc['eviction_count']} dockets)" if arc["eviction_count"] > 1 else ""
    scale = scales.get(arc["key"], 0)
    buyer = arc["buyer"] or "an LLC"
    buyer_clause = (
        f"An LLC, {buyer} (an entity that holds {scale} NYC buildings), "
        if scale > 1
        else f"An LLC, {buyer}, "
    )
    where = f"{arc['zip_code']} " if arc["zip_code"] else ""
    subject = f"Tip: {where}building resold for {pct}% after its tenants were evicted (public record)"
    body = (
        "Hi [name],\n\n"
        "A quick tip from NYC public records that may be worth a look.\n\n"
        f"{_loc(arc)}. Tenants were evicted in {ev_full}{dockets}. "
        f"{buyer_clause}bought the building on {arc['buy_date']} for {_money(arc['buy_amt'])} "
        f"and resold it on {arc['sell_date']} for {_money(arc['sell_amt'])}, "
        f"{_months_phrase(_months_held(arc))} later. That is {_article(pct)} {pct}% gain. "
        f"The paper trail is two ACRIS deeds: {arc['buy_doc']} and {arc['sell_doc']}.\n\n"
        "I run pulsecities.com, which reads NYC open data nightly and surfaces "
        f"eviction-to-resale patterns like this citywide. There are {total} such arcs "
        "on record so far. Happy to send the full list or walk through the records.\n\n"
        "Best,\nMichael\npulsecities.com"
    )
    return f"Suggested outlets: {TIP_OUTLETS}\n\nSubject: {subject}\n\n{body}"


def build_post_pack(new_arcs: list[dict], scales: dict, total: int) -> str:
    thread = build_x_thread(new_arcs)
    bsky = build_bluesky_post(new_arcs)
    parts = [
        "=" * 60,
        "READY-TO-POST PACK (copy-paste, review before sending)",
        "=" * 60,
        "",
        "X / TWITTER THREAD",
        "-" * 20,
    ]
    for i, t in enumerate(thread, 1):
        flag = "" if len(t) <= MAX_TWEET else "  <- over 280, trim before posting"
        parts.append(f"[{len(t)} chars]{flag}")
        parts.append(t)
        parts.append("")
    parts.append("BLUESKY")
    parts.append("-" * 20)
    bsky_flag = "" if len(bsky) <= MAX_BLUESKY else "  <- over 300, trim before posting"
    parts.append(f"[{len(bsky)} chars]{bsky_flag}")
    parts.append(bsky)
    parts.append("")
    parts.append("REPORTER TIP")
    parts.append("-" * 20)
    parts.append(build_reporter_pitch(new_arcs, scales, total))
    parts.append("")
    return "\n".join(parts)


def build_report(new_arcs: list[dict], scales: dict, total: int) -> str:
    parts = [
        f"Eviction-flip scan, week of {date.today().isoformat()}.",
        f"{len(new_arcs)} new arc{'s' if len(new_arcs) != 1 else ''} since last run, {total} on record.",
        "",
    ]
    for arc in new_arcs:
        parts.append(format_arc(arc, scales.get(arc["key"], 0)))
        parts.append("")
        parts.append("Draft post (review before publishing):")
        parts.append(f"  {draft_post(arc)}")
        parts.append("")
    parts.append(build_post_pack(new_arcs, scales, total))
    return "\n".join(parts)


def run(dry_run: bool = False) -> None:
    if not dry_run and not resend.api_key:
        logger.error("RESEND_API_KEY not set. Aborting flips scan email.")
        sys.exit(1)

    seen: set = set()
    if STATE_PATH.exists():
        try:
            seen = set(json.loads(STATE_PATH.read_text()).get("seen_keys", []))
        except (json.JSONDecodeError, OSError) as exc:
            # A torn or corrupt state file must not kill the scan permanently.
            # Starting from an empty seen-set re-reports old arcs once, which is
            # recoverable; a weekly crash loop is not.
            logger.warning("State file unreadable (%s); rescanning from scratch", exc)

    with get_scraper_db() as db:
        arcs = scan(db)
        new_arcs = [a for a in arcs if a["key"] not in seen]
        scales = {a["key"]: buyer_scale(db, a["buyer"]) for a in new_arcs}

    logger.info("Scan complete: %d arcs total, %d new", len(arcs), len(new_arcs))

    if not new_arcs:
        logger.info("No new arcs this week. No email sent.")
    else:
        report = build_report(new_arcs, scales, total=len(arcs))
        if dry_run:
            print(report)
        else:
            resend.Emails.send({
                "from": "PulseCities <alerts@pulsecities.com>",
                "to": [REPORT_TO],
                "subject": f"Eviction flips: {len(new_arcs)} new arc{'s' if len(new_arcs) != 1 else ''} this week",
                "text": report,
            })
            logger.info("Report emailed to %s", REPORT_TO)

    if not dry_run:
        if new_arcs:
            editions = []
            if EDITIONS_PATH.exists():
                try:
                    editions = json.loads(EDITIONS_PATH.read_text()).get("editions", [])
                except (json.JSONDecodeError, OSError) as exc:
                    # Never overwrite the editions archive based on a bad read:
                    # appending to [] would silently drop every approved edition.
                    logger.error("Editions file unreadable (%s); aborting so the "
                                 "archive is not clobbered", exc)
                    sys.exit(1)
            editions.append({
                "week": date.today().strftime("%G-W%V"),
                "generated": date.today().isoformat(),
                "approved": False,
                "arcs": new_arcs,
            })
            # Atomic replace: the editions API and the /flips/editions page
            # read this file live, and a torn read parses as "no editions".
            tmp = EDITIONS_PATH.with_suffix(".json.tmp")
            tmp.write_text(json.dumps({"editions": editions}, indent=1))
            os.replace(tmp, EDITIONS_PATH)
            logger.info("Edition %s recorded (%d arcs, awaiting review)",
                        editions[-1]["week"], len(new_arcs))

        # Atomic replace, same as the editions file: a crash mid-write must not
        # tear the state and kill every future weekly run.
        tmp = STATE_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({
            "updated": date.today().isoformat(),
            "seen_keys": sorted(seen | {a["key"] for a in arcs}),
        }, indent=1))
        os.replace(tmp, STATE_PATH)
        logger.info("State updated: %d keys", len(seen | {a['key'] for a in arcs}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weekly eviction-flip scan")
    parser.add_argument("--dry-run", action="store_true", help="print report, no email, no state write")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
