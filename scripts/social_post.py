"""
Nightly social-media findings generator for PulseCities.

Runs after the 2am pipeline to surface notable displacement signals:
  1. Score spikes  — zip codes where composite rose > 5 pts vs. prior snapshot
  2. Reno-flips    — LLC acquisition followed by a permit within 90 days, newly ingested
  3. LLC surges    — zip codes with > 3 LLC acquisitions ingested in the last 24 hours
  4. Dual signal   — addresses with a new LLC acquisition AND a new permit within 7 days

Findings are appended to /var/log/pulsecities/social_findings.log.

Twitter API wiring is intentionally absent — review finding quality before automating.

Usage:
    python scripts/social_post.py           # write to log (and print)
    python scripts/social_post.py --dry-run # print to stdout only, no log write
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timezone

# Resolve project root regardless of working directory when invoked
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text

from models.database import SessionLocal

LOG_PATH = "/var/log/pulsecities/social_findings.log"
SITE_URL = "pulsecities.com"
MAX_TWEET = 280

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_date(d: date) -> str:
    """'Apr 15' style — no leading zero on the day."""
    return d.strftime("%b %-d")


def _neighborhood_label(name: str | None, zip_code: str) -> str:
    if name:
        return f"{name} ({zip_code})"
    return zip_code


def _tweet(body: str) -> str:
    """Append site URL, hard-truncating body with ellipsis if needed."""
    suffix = f" {SITE_URL}"
    available = MAX_TWEET - len(suffix)
    if len(body) > available:
        body = body[: available - 1].rstrip() + "\u2026"
    return body + suffix


# ---------------------------------------------------------------------------
# Finding 1: Score spike > 5 points overnight
# ---------------------------------------------------------------------------

def find_score_spikes(db) -> list[dict]:
    """
    Compare the two most recent score_history rows per zip.
    When the delta is > 5 points, surface a finding.
    Also report how many LLC acquisitions the nightly scraper just ingested for
    that zip — gives the tweet a concrete data point.
    """
    rows = db.execute(text("""
        WITH ranked AS (
            SELECT zip_code,
                   composite_score,
                   scored_at,
                   ROW_NUMBER() OVER (PARTITION BY zip_code ORDER BY scored_at DESC) AS rn
            FROM score_history
        ),
        spikes AS (
            SELECT curr.zip_code,
                   curr.composite_score                                           AS current_score,
                   prev.composite_score                                           AS previous_score,
                   ROUND((curr.composite_score - prev.composite_score)::numeric, 1) AS delta
            FROM ranked curr
            JOIN ranked prev
              ON curr.zip_code = prev.zip_code AND prev.rn = 2
            WHERE curr.rn = 1
              AND curr.composite_score - prev.composite_score > 5
        )
        SELECT s.zip_code,
               s.current_score,
               s.previous_score,
               s.delta,
               n.name AS neighborhood,
               (
                   SELECT COUNT(*)
                   FROM ownership_raw o
                   JOIN parcels p ON o.bbl = p.bbl
                   WHERE o.party_type = '2'
                     AND o.party_name_normalized LIKE '%LLC%'
                     AND o.created_at >= NOW() - INTERVAL '24 hours'
                     AND p.zip_code = s.zip_code
                     AND p.units_res > 0
                     AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
                     AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
                     AND o.party_name_normalized NOT ILIKE '%LOAN SERVICE%'
                     AND o.party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
                     AND o.party_name_normalized NOT ILIKE '%CREDIT UNION%'
               ) AS llc_count_24h
        FROM spikes s
        LEFT JOIN neighborhoods n ON n.zip_code = s.zip_code
        ORDER BY s.delta DESC
    """)).fetchall()

    findings = []
    for r in rows:
        label = _neighborhood_label(r.neighborhood, r.zip_code)
        delta = int(r.delta) if r.delta == int(r.delta) else float(r.delta)
        llc = r.llc_count_24h or 0

        if llc > 0:
            unit = "acquisitions" if llc != 1 else "acquisition"
            body = (
                f"{label} displacement pressure rose {delta} points overnight — "
                f"{llc} new LLC {unit} recorded via ACRIS"
            )
        else:
            body = (
                f"{label} displacement risk jumped {delta} pts overnight — "
                f"signal spike across multiple indicators"
            )

        findings.append({"type": "SCORE_SPIKE", "zip": r.zip_code, "tweet": _tweet(body)})

    return findings


# ---------------------------------------------------------------------------
# Finding 2: Renovation-flip pattern (LLC acquisition → permit within 90 days)
# ---------------------------------------------------------------------------

def find_renovation_flips(db) -> list[dict]:
    """
    Detect the classic flip pattern: LLC buys a residential parcel, then pulls
    an alteration permit within 90 days.  Only surfaces combos where either the
    ownership record or the permit was ingested by last night's scraper run.
    """
    rows = db.execute(text("""
        SELECT DISTINCT ON (par.bbl)
            par.address,
            par.zip_code,
            n.name                        AS neighborhood,
            o.party_name,
            o.doc_date                    AS acquisition_date,
            pr.filing_date                AS permit_date,
            (pr.filing_date - o.doc_date) AS days_between,
            pr.work_type
        FROM ownership_raw o
        JOIN parcels par ON par.bbl = o.bbl
        JOIN permits_raw pr ON pr.bbl = o.bbl
        LEFT JOIN neighborhoods n ON n.zip_code = par.zip_code
        WHERE o.party_type = '2'
          AND o.party_name_normalized LIKE '%LLC%'
          AND par.units_res > 0
          AND par.zip_code IS NOT NULL
          AND pr.filing_date > o.doc_date
          AND pr.filing_date <= o.doc_date + INTERVAL '90 days'
          AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
          AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
          AND o.party_name_normalized NOT ILIKE '%LOAN SERVICE%'
          AND o.party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
          AND o.party_name_normalized NOT ILIKE '%CREDIT UNION%'
          AND (
              o.created_at  >= NOW() - INTERVAL '24 hours'
              OR pr.created_at >= NOW() - INTERVAL '24 hours'
          )
        ORDER BY par.bbl, (pr.filing_date - o.doc_date) ASC
        LIMIT 5
    """)).fetchall()

    findings = []
    for r in rows:
        addr = (r.address or "unknown address").title()
        neigh = r.neighborhood or r.zip_code
        acq = _fmt_date(r.acquisition_date)
        pmt = _fmt_date(r.permit_date)
        days = r.days_between.days if r.days_between else 0

        body = (
            f"Renovation-flip pattern detected at {addr}, {neigh} — "
            f"LLC acquired {acq}, renovation permit filed {pmt} ({days} days later)"
        )
        findings.append({"type": "RENO_FLIP", "zip": r.zip_code, "tweet": _tweet(body)})

    return findings


# ---------------------------------------------------------------------------
# Finding 3: LLC surge (> 3 new acquisitions in one zip in the last 24 hours)
# ---------------------------------------------------------------------------

def find_llc_surges(db) -> list[dict]:
    """
    Flag zip codes where the nightly ACRIS scraper ingested more than 3 LLC
    acquisitions on residential parcels in a single run.
    """
    rows = db.execute(text("""
        SELECT p.zip_code,
               n.name  AS neighborhood,
               COUNT(*) AS llc_count
        FROM ownership_raw o
        JOIN parcels p ON o.bbl = p.bbl
        LEFT JOIN neighborhoods n ON n.zip_code = p.zip_code
        WHERE o.party_type = '2'
          AND o.party_name_normalized LIKE '%LLC%'
          AND o.created_at >= NOW() - INTERVAL '24 hours'
          AND p.zip_code IS NOT NULL
          AND p.units_res > 0
          AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
          AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
          AND o.party_name_normalized NOT ILIKE '%LOAN SERVICE%'
          AND o.party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
          AND o.party_name_normalized NOT ILIKE '%CREDIT UNION%'
        GROUP BY p.zip_code, n.name
        HAVING COUNT(*) > 3
        ORDER BY llc_count DESC
    """)).fetchall()

    findings = []
    for r in rows:
        label = _neighborhood_label(r.neighborhood, r.zip_code)
        count = r.llc_count
        body = (
            f"{label} recorded {count} LLC property acquisitions overnight — "
            f"unusually high single-day transfer volume"
        )
        findings.append({"type": "LLC_SURGE", "zip": r.zip_code, "tweet": _tweet(body)})

    return findings


# ---------------------------------------------------------------------------
# Finding 4: Dual signal (new LLC acquisition + new permit within 7 days)
# ---------------------------------------------------------------------------

def find_dual_signals(db) -> list[dict]:
    """
    Surface individual addresses where an LLC transfer and a permit filing both
    landed within 7 days of each other and at least one event is new (doc date
    or filing date within the last 24 hours).
    """
    rows = db.execute(text("""
        SELECT DISTINCT ON (par.bbl)
            par.address,
            par.zip_code,
            n.name                            AS neighborhood,
            o.doc_date                        AS acquisition_date,
            pr.filing_date                    AS permit_date,
            ABS(pr.filing_date - o.doc_date)  AS days_apart
        FROM ownership_raw o
        JOIN parcels par ON par.bbl = o.bbl
        JOIN permits_raw pr ON pr.bbl = o.bbl
        LEFT JOIN neighborhoods n ON n.zip_code = par.zip_code
        WHERE o.party_type = '2'
          AND o.party_name_normalized LIKE '%LLC%'
          AND par.units_res > 0
          AND par.zip_code IS NOT NULL
          AND ABS(pr.filing_date - o.doc_date) <= 7
          AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
          AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
          AND o.party_name_normalized NOT ILIKE '%LOAN SERVICE%'
          AND o.party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
          AND o.party_name_normalized NOT ILIKE '%CREDIT UNION%'
          AND (
              o.doc_date       >= CURRENT_DATE - INTERVAL '1 day'
              OR pr.filing_date >= CURRENT_DATE - INTERVAL '1 day'
          )
        ORDER BY par.bbl, ABS(pr.filing_date - o.doc_date) ASC
        LIMIT 5
    """)).fetchall()

    findings = []
    for r in rows:
        addr = (r.address or "unknown address").title()
        neigh = r.neighborhood or r.zip_code
        days = r.days_apart.days if r.days_apart else 0

        if days == 0:
            timing = "same day"
        elif days == 1:
            timing = "1 day apart"
        else:
            timing = f"{days} days apart"

        body = (
            f"Dual displacement signal at {addr}, {neigh} — "
            f"LLC transfer and permit filing {timing}"
        )
        findings.append({"type": "DUAL_SIGNAL", "zip": r.zip_code, "tweet": _tweet(body)})

    return findings


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _print_findings(findings: list[dict], run_date: date) -> None:
    print(f"\n=== PulseCities social findings — {run_date.isoformat()} ===\n")
    if not findings:
        print("No notable findings today.")
        return
    for f in findings:
        print(f"[{f['type']}]")
        print(f["tweet"])
        print(f"  ({len(f['tweet'])} chars)")
        print()


def _write_log(findings: list[dict], run_date: date) -> None:
    log_dir = os.path.dirname(LOG_PATH)
    os.makedirs(log_dir, exist_ok=True)

    lines = [f"\n=== {run_date.isoformat()} ==="]
    if not findings:
        lines.append("No notable findings.")
    else:
        for f in findings:
            lines.append(f"[{f['type']}] {f['tweet']}")

    with open(LOG_PATH, "a") as fh:
        fh.write("\n".join(lines) + "\n")

    logger.info("Wrote %d finding(s) to %s", len(findings), LOG_PATH)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate nightly PulseCities social media findings."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print findings to stdout only — do not write to the log file.",
    )
    args = parser.parse_args()

    run_date = datetime.now(timezone.utc).date()

    db = SessionLocal()
    try:
        findings: list[dict] = []
        findings.extend(find_score_spikes(db))
        findings.extend(find_renovation_flips(db))
        findings.extend(find_llc_surges(db))
        findings.extend(find_dual_signals(db))
    except Exception:
        logger.exception("Failed to query findings — aborting")
        sys.exit(1)
    finally:
        db.close()

    _print_findings(findings, run_date)

    if not args.dry_run:
        _write_log(findings, run_date)


if __name__ == "__main__":
    main()
