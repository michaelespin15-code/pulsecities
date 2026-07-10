"""
Monthly operator-directory refresh.

The 37-vs-42 drift happened because operator_network_analysis + backfill only
ran when someone remembered. This wrapper runs both on a schedule, then holds
the result to invariants before declaring success:

  1. every allowlisted operator (MTEK, PHANTOM, BREDIF) still has a public row
  2. each public operator's parcel rows exactly match its headline count
  3. no public operator collapsed below the promotion floor (5 properties)

On success it emails a before/after diff so number changes on public pages are
never a surprise. On any failure it emails the error and exits 1; the DB may
hold a partial refresh at that point, which the invariant email calls out so
it gets fixed the same day rather than silently drifting.

Usage:
    python -m scripts.refresh_operator_directory
"""

import logging
import sys

from sqlalchemy import text

from models.database import get_scraper_db
from scheduler.alerts import send_ops_email
from scoring.operator_classification import KNOWN_OPERATOR_ALLOWLIST

logger = logging.getLogger(__name__)

_PROMOTION_FLOOR = 5


def _public_snapshot(db) -> dict[str, dict]:
    rows = db.execute(text("""
        SELECT o.operator_root, o.slug, o.total_properties, o.total_acquisitions,
               jsonb_array_length(o.llc_entities) AS llc_count,
               (SELECT COUNT(*) FROM operator_parcels op WHERE op.operator_id = o.id) AS parcel_rows
        FROM operators o
        WHERE o.operator_class = 'operator'
        ORDER BY o.operator_root
    """)).fetchall()
    return {
        r.operator_root: {
            "slug": r.slug,
            "properties": r.total_properties,
            "acquisitions": r.total_acquisitions,
            "llcs": r.llc_count,
            "parcels": r.parcel_rows,
        }
        for r in rows
    }


def _check_invariants(after: dict[str, dict]) -> list[str]:
    problems = []
    for root in sorted(KNOWN_OPERATOR_ALLOWLIST):
        if root not in after:
            problems.append(f"{root}: allowlisted operator missing from public directory")
    for root, s in after.items():
        if s["parcels"] != s["properties"]:
            problems.append(
                f"{root}: parcel rows ({s['parcels']}) != headline properties ({s['properties']})"
            )
        if s["properties"] < _PROMOTION_FLOOR:
            problems.append(f"{root}: {s['properties']} properties, below promotion floor")
    return problems


def _diff_lines(before: dict, after: dict) -> list[str]:
    lines = []
    for root in sorted(set(before) | set(after)):
        b, a = before.get(root), after.get(root)
        if b is None:
            lines.append(f"{root}: NEW public operator ({a['properties']} properties, {a['llcs']} LLCs)")
        elif a is None:
            lines.append(f"{root}: REMOVED from public directory (was {b['properties']} properties)")
        elif (b["properties"], b["acquisitions"], b["llcs"]) != (a["properties"], a["acquisitions"], a["llcs"]):
            lines.append(
                f"{root}: {b['properties']}/{b['acquisitions']}/{b['llcs']} -> "
                f"{a['properties']}/{a['acquisitions']}/{a['llcs']} (properties/acquisitions/LLCs)"
            )
        else:
            lines.append(f"{root}: unchanged ({a['properties']} properties, {a['llcs']} LLCs)")
    return lines


def run() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    with get_scraper_db() as db:
        before = _public_snapshot(db)

    try:
        from scripts.operator_network_analysis import main as run_analysis_main
        run_analysis_main()

        from scripts.backfill_operators import backfill
        backfill()
    except Exception as exc:
        logger.exception("Directory refresh failed")
        send_ops_email(
            "Operator directory refresh FAILED",
            f"The monthly analysis/backfill run raised:\n\n{exc}\n\n"
            "The operators table may hold a partial refresh. Rerun\n"
            "scripts/refresh_operator_directory.py after fixing.",
        )
        return 1

    with get_scraper_db() as db:
        after = _public_snapshot(db)

    problems = _check_invariants(after)
    if problems:
        body = "Refresh completed but failed invariants:\n\n" + "\n".join(
            f"  - {p}" for p in problems
        ) + "\n\nPublic pages may be showing inconsistent numbers right now."
        logger.error(body)
        send_ops_email("Operator directory refresh INVARIANT FAILURE", body)
        return 1

    diff = _diff_lines(before, after)
    logger.info("Refresh clean:\n%s", "\n".join(diff))
    send_ops_email(
        "Operator directory refreshed",
        "Monthly refresh completed and passed all invariants.\n\n"
        + "\n".join(diff)
        + "\n\nThese numbers are now live on the operator pages.",
    )
    return 0


if __name__ == "__main__":
    sys.exit(run())
