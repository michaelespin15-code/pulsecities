"""
PulseCities pipeline health report.

Checks scraper freshness, signal quality, and scoring sanity in one pass.
Prints a human-readable summary and exits nonzero when a production risk
threshold is breached.

Exit-nonzero conditions:
  - Any key scraper has status='failure' on its latest run
  - ACRIS source frozen > 14 days (ownership_raw not updated)
  - displacement_scores max < 40 (scoring collapsed)
  - live avg vs latest score_history avg diverges by > 30 %
  - latest score_history avg < 50 % of previous day avg

Does NOT exit nonzero for:
  - DOB rolling-average warning after a bulk recovery run
  - ACRIS frozen <= 14 days (warn only)
  - Any non-critical scraper in degraded state

Usage:
    python -m scripts.pipeline_health
    python scripts/pipeline_health.py
"""

import sys
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import text

from models.database import get_scraper_db

# Key scrapers for which status=failure triggers a nonzero exit.
# Excludes auxiliary scrapers (dcwp_licenses, dof_assessments, dhcr_rs, mappluto).
KEY_SCRAPERS = [
    "acris_ownership",
    "dob_permits",
    "311_complaints",
    "evictions",
    "hpd_violations",
]

ACRIS_FROZEN_CRITICAL_DAYS = 14  # exit nonzero above this threshold
SCORE_MAX_FLOOR = 40.0            # displacement_scores max must exceed this
LIVE_HISTORY_DRIFT_PCT = 0.30     # live avg vs history avg tolerance
HISTORY_DAY_OVER_DAY_PCT = 0.50  # today's history avg must be >= 50 % of yesterday


# ---------------------------------------------------------------------------
# Health label helpers
# ---------------------------------------------------------------------------

def scraper_health_label(status: str, records: int, expected_min: int | None) -> str:
    """Assign a health label for a single scraper run."""
    if status == "failure":
        return "FAILED"
    if status == "running":
        return "RUNNING"
    if records == 0 and status == "warning":
        # Distinguish a genuinely frozen source (ACRIS) from a transient zero run.
        # The caller adds context; this function labels the run itself.
        return "FROZEN"
    if expected_min and records < expected_min * 0.50:
        return "DEGRADED"
    return "OK"


def acris_frozen_days(max_doc_date: date | None) -> int | None:
    """Return days since ownership_raw was last updated, or None if unknown."""
    if max_doc_date is None:
        return None
    return (date.today() - max_doc_date).days


def score_drift_pct(live_avg: float, history_avg: float) -> float | None:
    """Percentage difference between live displacement_scores avg and history avg."""
    if not history_avg:
        return None
    return abs(live_avg - history_avg) / history_avg


def history_day_over_day_ratio(today_avg: float | None, yesterday_avg: float | None) -> float | None:
    if not today_avg or not yesterday_avg:
        return None
    return today_avg / yesterday_avg


def build_exit_flags(
    scraper_rows: list[dict],
    acris_days: int | None,
    scores_max: float | None,
    drift: float | None,
    dod_ratio: float | None,
) -> list[str]:
    """
    Return a list of human-readable exit reasons.
    Empty list means healthy exit (0).
    """
    flags: list[str] = []

    for s in scraper_rows:
        if s["name"] in KEY_SCRAPERS and s["status"] == "failure":
            flags.append(f"{s['name']}: status=failure")

    if acris_days is not None and acris_days > ACRIS_FROZEN_CRITICAL_DAYS:
        flags.append(
            f"acris_ownership: source frozen {acris_days}d "
            f"(>{ACRIS_FROZEN_CRITICAL_DAYS}d threshold)"
        )

    if scores_max is not None and scores_max < SCORE_MAX_FLOOR:
        flags.append(
            f"displacement_scores: max={scores_max:.1f} below floor {SCORE_MAX_FLOOR}"
        )

    if drift is not None and drift > LIVE_HISTORY_DRIFT_PCT:
        flags.append(
            f"live vs history avg drift {drift*100:.1f}% "
            f"(>{LIVE_HISTORY_DRIFT_PCT*100:.0f}% threshold)"
        )

    if dod_ratio is not None and dod_ratio < HISTORY_DAY_OVER_DAY_PCT:
        flags.append(
            f"score_history today avg is {dod_ratio*100:.1f}% of yesterday "
            f"(<{HISTORY_DAY_OVER_DAY_PCT*100:.0f}% threshold)"
        )

    return flags


# ---------------------------------------------------------------------------
# DB queries
# ---------------------------------------------------------------------------

def fetch_scraper_latest(db) -> list[dict]:
    """Latest run per scraper, ordered by name."""
    rows = db.execute(text("""
        SELECT DISTINCT ON (scraper_name)
            scraper_name AS name,
            status,
            started_at,
            records_processed,
            records_failed,
            watermark_timestamp,
            warning_message,
            error_message
        FROM scraper_runs
        ORDER BY scraper_name, started_at DESC
    """)).fetchall()
    return [dict(r._mapping) for r in rows]


def fetch_scores_summary(db) -> dict[str, Any]:
    r = db.execute(text("""
        SELECT COUNT(*) AS n, MAX(score) AS mx, AVG(score) AS av
        FROM displacement_scores
        WHERE score IS NOT NULL
    """)).fetchone()
    return {
        "count": int(r.n or 0),
        "max":   float(r.mx) if r.mx is not None else None,
        "avg":   float(r.av) if r.av is not None else None,
    }


def fetch_history_summary(db, scored_at: date) -> dict[str, Any]:
    r = db.execute(text("""
        SELECT COUNT(*) AS n, MAX(composite_score) AS mx, AVG(composite_score) AS av
        FROM score_history
        WHERE scored_at = :d
    """), {"d": scored_at}).fetchone()
    return {
        "date":  scored_at,
        "count": int(r.n or 0),
        "max":   float(r.mx) if r.mx is not None else None,
        "avg":   float(r.av) if r.av is not None else None,
    }


def fetch_acris_max_doc_date(db) -> date | None:
    r = db.execute(text("SELECT MAX(doc_date) FROM ownership_raw")).scalar()
    return r


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return "never"
    # Render in UTC, trim seconds
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _label_color(label: str) -> str:
    return {
        "OK": "OK",
        "DEGRADED": "DEGRADED",
        "FROZEN": "FROZEN",
        "FAILED": "FAILED",
        "RUNNING": "RUNNING",
    }.get(label, label)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def run_health_report(db) -> int:
    """
    Build and print the health report.  Returns the exit code (0 = healthy).
    """
    today = date.today()
    yesterday = date.fromordinal(today.toordinal() - 1)

    scrapers = fetch_scraper_latest(db)
    scores = fetch_scores_summary(db)
    hist_today = fetch_history_summary(db, today)
    hist_yest  = fetch_history_summary(db, yesterday)
    acris_max  = fetch_acris_max_doc_date(db)
    acris_days = acris_frozen_days(acris_max)

    drift = None
    if scores["avg"] and hist_today["avg"]:
        drift = score_drift_pct(scores["avg"], hist_today["avg"])

    dod = history_day_over_day_ratio(hist_today["avg"], hist_yest["avg"])

    flags = build_exit_flags(
        scraper_rows=scrapers,
        acris_days=acris_days,
        scores_max=scores["max"],
        drift=drift,
        dod_ratio=dod,
    )

    # ---- Print report ----
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n=== PulseCities Pipeline Health — {now_str} ===\n")

    print("SCRAPERS")
    for s in scrapers:
        name     = s["name"]
        status   = s["status"] or "unknown"
        recs     = s["records_processed"] or 0
        started  = _fmt_dt(s["started_at"])
        warn     = s["warning_message"] or ""
        err      = s["error_message"] or ""

        label = scraper_health_label(status, recs, None)

        # ACRIS staleness context
        extra = ""
        if name == "acris_ownership" and acris_days is not None:
            extra = f"  [source frozen {acris_days}d — ownership_raw max {acris_max}]"
            if acris_days > ACRIS_FROZEN_CRITICAL_DAYS:
                label = "FROZEN"

        # DOB: note that a post-bulk-recovery rolling-average warning is expected
        if name == "dob_permits" and warn and "rolling average" in warn and recs <= 50:
            extra = "  [post-bulk-recovery; rolling-avg warning expected]"

        print(f"  {name:<22} {label:<10} {recs:>6} recs   {started}{extra}")
        if err:
            print(f"    ERROR: {err[:100]}")

    print()
    print("SCORES")

    ds_label = "OK"
    if scores["max"] is None:
        ds_label = "NO DATA"
    elif scores["max"] < SCORE_MAX_FLOOR:
        ds_label = "COLLAPSED"

    ds_max = f"{scores['max']:.1f}" if scores["max"] is not None else "N/A"
    ds_avg = f"{scores['avg']:.2f}" if scores["avg"] is not None else "N/A"
    print(f"  displacement_scores  {ds_label:<10}  n={scores['count']}  max={ds_max}  avg={ds_avg}")

    def _hist_line(label: str, h: dict) -> str:
        if h["count"] == 0:
            return f"  score_history {label:<8}  NO DATA"
        return (f"  score_history {label:<8}  "
                f"n={h['count']}  max={h['max']:.1f}  avg={h['avg']:.2f}")

    print(_hist_line("today   ", hist_today))
    print(_hist_line("yesterday", hist_yest))

    if drift is not None:
        drift_label = "OK" if drift <= LIVE_HISTORY_DRIFT_PCT else "DRIFT"
        print(f"  live vs history avg  {drift_label:<10}  Δ={drift*100:.1f}%")

    if dod is not None:
        dod_label = "OK" if dod >= HISTORY_DAY_OVER_DAY_PCT else "COLLAPSED"
        print(f"  history day-over-day {dod_label:<10}  ratio={dod:.2f}  "
              f"(today {hist_today['avg']:.2f} / yest {hist_yest['avg']:.2f})")

    print()

    if flags:
        verdict = "CRITICAL — exit nonzero"
        print(f"VERDICT: {verdict}")
        for f in flags:
            print(f"  ✗ {f}")
        print()
        return 1
    else:
        print("VERDICT: HEALTHY")
        print()
        return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    with get_scraper_db() as db:
        exit_code = run_health_report(db)
    sys.exit(exit_code)
