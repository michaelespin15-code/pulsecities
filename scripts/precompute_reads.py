"""Generate the panel read for every scored ZIP, straight after the scoring run.

The read was generated on a cache miss, in front of a reader, and cost five to
twelve seconds while they watched a "generating..." line. That is long enough to
lose them, and it was paid twice per ZIP because each gunicorn worker held its
own copy of the cache, and again after every reload because the cache lived in
the process.

So it moves off the request path entirely. Scoring finishes, this runs, and by
the time anyone opens a panel the read is a row in ai_summaries. A reader now
waits for a database round trip.

Only ZIPs whose read would now say something different are regenerated: a move
of a full point, or a tier crossing. Regenerating on any change at all was
measured at 105 ZIPs a night against 14, which is $45 a month against $6, and it
buys nothing because a tenth of a point does not change a paragraph written in
ranks and directions. api.routes.ai_summary.is_fresh owns that rule so the
request path and this script cannot disagree about it.

    PYTHONPATH=. venv/bin/python -m scripts.precompute_reads [--limit N] [--all]

Cost, measured 2026-08-28: ~1,600 input and ~260 output tokens per ZIP, about
$0.015. A full sweep of 177 is $2.51, paid once. A normal night is about 14 ZIPs,
or 21 cents.
"""

import argparse
import logging
import sys
import time

from sqlalchemy import text

from api.routes.ai_summary import (MODEL, SCORE_DRIFT_TOLERANCE, _build_facts,
                                   _gather_context, _get_client, _score_key,
                                   is_fresh, store_summary, EFFORT, MAX_TOKENS,
                                   _SYSTEM_PROMPT)
from api.routes.neighborhoods import _borough_from_zip, _fetch_raw_counts
from config.logging_config import configure_logging
from models.database import SessionLocal

configure_logging()
logger = logging.getLogger(__name__)

# A full sweep is 177 calls. The cap is a spend ceiling for a run that goes wrong,
# not a target: the normal night regenerates only what re-scored.
DEFAULT_LIMIT = 200


def _stale_zips(db, regenerate_all: bool) -> list[tuple[str, float, dict, str | None]]:
    """Scored ZIPs whose stored read is missing or was written against an older
    score. Ordered by score so that if a run is cut short, the neighborhoods
    under the most pressure are the ones already done."""
    rows = db.execute(text("""
        SELECT ds.zip_code, ds.score, ds.signal_breakdown, n.name, a.score AS stored_score
        FROM displacement_scores ds
        LEFT JOIN neighborhoods n ON n.zip_code = ds.zip_code
        LEFT JOIN ai_summaries a ON a.zip_code = ds.zip_code
        WHERE ds.score IS NOT NULL
        ORDER BY ds.score DESC
    """)).fetchall()
    out = []
    for r in rows:
        if regenerate_all or not is_fresh(r.stored_score, float(r.score)):
            out.append((r.zip_code, float(r.score), r.signal_breakdown or {}, r.name))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                    help="stop after this many generations")
    ap.add_argument("--all", action="store_true",
                    help="regenerate every ZIP, not only the re-scored ones")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would be generated and call nothing")
    args = ap.parse_args()

    client = _get_client()
    if client is None and not args.dry_run:
        logger.warning("No ANTHROPIC_API_KEY; nothing to do. The panel falls back "
                       "to the deterministic summary, which is the designed behaviour.")
        return 0

    db = SessionLocal()
    try:
        todo = _stale_zips(db, args.all)
        logger.info("%d ZIP(s) need a read%s", len(todo),
                    " (--all)" if args.all else " since their last score change")
        if args.dry_run:
            for zip_code, score, _, name in todo[:20]:
                logger.info("  would generate %s %s (%.1f)", zip_code, name or "", score)
            return 0

        done = failed = 0
        for zip_code, score, breakdown, name in todo[:args.limit]:
            started = time.monotonic()
            try:
                facts = _build_facts(
                    name, _borough_from_zip(zip_code), zip_code, score, breakdown,
                    _fetch_raw_counts(db, zip_code),
                    _gather_context(db, zip_code, breakdown),
                )
                message = client.messages.create(
                    model=MODEL, max_tokens=MAX_TOKENS,
                    thinking={"type": "adaptive"},
                    output_config={"effort": EFFORT},
                    system=_SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": facts}],
                )
                if message.stop_reason == "refusal":
                    logger.warning("%s: refused", zip_code)
                    failed += 1
                    continue
                summary = "".join(b.text for b in message.content
                                  if b.type == "text").strip()
                if not summary:
                    logger.warning("%s: empty response", zip_code)
                    failed += 1
                    continue
                store_summary(db, zip_code, _score_key(score), summary, MODEL, score)
                usage = getattr(message, "usage", None)
                logger.info("%s %s %.1fs in=%s out=%s", zip_code, name or "",
                            time.monotonic() - started,
                            getattr(usage, "input_tokens", "?"),
                            getattr(usage, "output_tokens", "?"))
                done += 1
            except Exception as exc:  # noqa: BLE001 — one bad ZIP must not end the run
                logger.warning("%s failed: %r", zip_code, exc)
                failed += 1

        logger.info("Precompute complete: %d written, %d failed, %d left",
                    done, failed, max(0, len(todo) - args.limit))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
