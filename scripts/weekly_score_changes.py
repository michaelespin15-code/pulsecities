"""
Weekly displacement score delta analysis.

Compares each zip code's most recent score against its score from ~7 days
prior, surfaces any zip that moved ±5 points or more, and attributes the
change to the signals that drove it.

Output: scripts/weekly_score_changes.json

Usage:
    python scripts/weekly_score_changes.py
    python scripts/weekly_score_changes.py --min-delta 3   # lower threshold
    python scripts/weekly_score_changes.py --lookback 14   # wider prior window
"""

import argparse
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from sqlalchemy import text

from models.database import get_scraper_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

OUTPUT = Path(__file__).parent / "weekly_score_changes.json"

# Nominal signal weights from compute.py — used to rank which signal
# contributed most to the composite delta.  Dormant signals (assessment_spike,
# rs_unit_loss) retain their nominal weight so attribution still appears when
# they eventually activate.
SIGNAL_WEIGHTS: dict[str, float] = {
    "llc_acquisition_rate": 0.26,
    "permit_intensity":      0.21,
    "complaint_rate":        0.17,
    "eviction_rate":         0.13,
    "rs_unit_loss":          0.15,
    "assessment_spike":      0.08,
}

SIGNAL_LABELS: dict[str, str] = {
    "llc_acquisition_rate": "LLC acquisitions",
    "permit_intensity":      "permit filings",
    "complaint_rate":        "311 complaints",
    "eviction_rate":         "eviction filings",
    "rs_unit_loss":          "rent-stabilized unit loss",
    "assessment_spike":      "tax assessment spike",
}


# ─── DB queries ──────────────────────────────────────────────────────────────

# Grabs the most recent snapshot per zip AND the closest snapshot between
# 6 and 21 days before that (wide window tolerates gaps in nightly runs).
# Returns one row per zip where both snapshots exist.
DELTA_QUERY = text("""
WITH latest AS (
    SELECT DISTINCT ON (zip_code)
        zip_code,
        scored_at              AS current_date,
        composite_score        AS current_score,
        permit_intensity       AS curr_permits,
        eviction_rate          AS curr_evictions,
        llc_acquisition_rate   AS curr_llc,
        assessment_spike       AS curr_assessment,
        complaint_rate         AS curr_complaints,
        rs_unit_loss           AS curr_rs_loss
    FROM score_history
    ORDER BY zip_code, scored_at DESC
),
prior AS (
    SELECT DISTINCT ON (sh.zip_code)
        sh.zip_code,
        sh.scored_at           AS prior_date,
        sh.composite_score     AS prior_score,
        sh.permit_intensity    AS prior_permits,
        sh.eviction_rate       AS prior_evictions,
        sh.llc_acquisition_rate AS prior_llc,
        sh.assessment_spike    AS prior_assessment,
        sh.complaint_rate      AS prior_complaints,
        sh.rs_unit_loss        AS prior_rs_loss
    FROM score_history sh
    JOIN latest l ON sh.zip_code = l.zip_code
    WHERE sh.scored_at <= l.current_date - INTERVAL '6 days'
      AND sh.scored_at >= l.current_date - INTERVAL '21 days'
    ORDER BY sh.zip_code, sh.scored_at DESC
)
SELECT
    l.zip_code,
    l.current_date,
    l.current_score,
    l.curr_permits,
    l.curr_evictions,
    l.curr_llc,
    l.curr_assessment,
    l.curr_comments,
    l.curr_rs_loss,
    p.prior_date,
    p.prior_score,
    p.prior_permits,
    p.prior_evictions,
    p.prior_llc,
    p.prior_assessment,
    p.prior_comments,
    p.prior_rs_loss,
    (l.current_score - p.prior_score) AS delta
FROM (
    SELECT
        zip_code,
        current_date,
        current_score,
        curr_permits,
        curr_evictions,
        curr_llc,
        curr_assessment,
        curr_complaints AS curr_comments,
        curr_rs_loss
    FROM latest
) l
JOIN (
    SELECT
        zip_code,
        prior_date,
        prior_score,
        prior_permits,
        prior_evictions,
        prior_llc,
        prior_assessment,
        prior_complaints AS prior_comments,
        prior_rs_loss
    FROM prior
) p ON l.zip_code = p.zip_code
ORDER BY ABS(l.current_score - p.prior_score) DESC, l.zip_code
""")

# Cleaner version without the alias aliasing mess
DELTA_QUERY = text("""
WITH latest AS (
    SELECT DISTINCT ON (zip_code)
        zip_code,
        scored_at              AS current_date,
        composite_score        AS current_score,
        permit_intensity       AS curr_permits,
        eviction_rate          AS curr_evictions,
        llc_acquisition_rate   AS curr_llc,
        assessment_spike       AS curr_assessment,
        complaint_rate         AS curr_complaints,
        rs_unit_loss           AS curr_rs_loss
    FROM score_history
    ORDER BY zip_code, scored_at DESC
),
prior AS (
    SELECT DISTINCT ON (sh.zip_code)
        sh.zip_code,
        sh.scored_at              AS prior_date,
        sh.composite_score        AS prior_score,
        sh.permit_intensity       AS prior_permits,
        sh.eviction_rate          AS prior_evictions,
        sh.llc_acquisition_rate   AS prior_llc,
        sh.assessment_spike       AS prior_assessment,
        sh.complaint_rate         AS prior_complaints,
        sh.rs_unit_loss           AS prior_rs_loss
    FROM score_history sh
    JOIN latest l ON sh.zip_code = l.zip_code
    WHERE sh.scored_at <= l.current_date - INTERVAL '6 days'
      AND sh.scored_at >= l.current_date - INTERVAL '21 days'
    ORDER BY sh.zip_code, sh.scored_at DESC
)
SELECT
    l.zip_code,
    l.current_date,
    l.current_score,
    l.curr_permits,
    l.curr_evictions,
    l.curr_llc,
    l.curr_assessment,
    l.curr_complaints,
    l.curr_rs_loss,
    p.prior_date,
    p.prior_score,
    p.prior_permits,
    p.prior_evictions,
    p.prior_llc,
    p.prior_assessment,
    p.prior_complaints,
    p.prior_rs_loss,
    (l.current_score - p.prior_score) AS delta
FROM latest l
JOIN prior p ON l.zip_code = p.zip_code
ORDER BY ABS(l.current_score - p.prior_score) DESC, l.zip_code
""")


# ─── signal attribution ───────────────────────────────────────────────────────

def _signal_drivers(row: dict) -> list[dict]:
    """
    Rank signals by their estimated contribution to the composite delta.

    Contribution approximation: weight * abs(signal_delta).
    Since score_history stores the raw per-unit rate (not the 0-100 scaled
    value), deltas across signals aren't directly comparable — we use the
    weight-scaled absolute delta as a relative importance proxy, which is
    consistent enough across the same week's snapshots.

    Returns list of signal dicts sorted by abs contribution, highest first.
    Only includes signals where at least one of current/prior is non-null.
    """
    signal_pairs = [
        ("llc_acquisition_rate", row["curr_llc"],        row["prior_llc"]),
        ("permit_intensity",      row["curr_permits"],    row["prior_permits"]),
        ("complaint_rate",        row["curr_complaints"], row["prior_complaints"]),
        ("eviction_rate",         row["curr_evictions"],  row["prior_evictions"]),
        ("rs_unit_loss",          row["curr_rs_loss"],    row["prior_rs_loss"]),
        ("assessment_spike",      row["curr_assessment"], row["prior_assessment"]),
    ]

    drivers = []
    for key, curr, prior in signal_pairs:
        if curr is None and prior is None:
            continue
        curr_val  = curr  if curr  is not None else 0.0
        prior_val = prior if prior is not None else 0.0
        delta     = curr_val - prior_val
        weight    = SIGNAL_WEIGHTS[key]
        drivers.append({
            "signal":      key,
            "label":       SIGNAL_LABELS[key],
            "current":     round(curr_val,  6),
            "prior":       round(prior_val, 6),
            "delta":       round(delta,     6),
            "direction":   "up" if delta > 0 else ("down" if delta < 0 else "flat"),
            "weight":      weight,
            # weighted abs delta — higher = bigger driver of composite change
            "contribution": round(weight * abs(delta), 8),
        })

    drivers.sort(key=lambda d: -d["contribution"])
    return drivers


def _primary_driver_labels(drivers: list[dict], top_n: int = 2) -> list[str]:
    """Human-readable list of the top contributing signals."""
    active = [d for d in drivers if d["contribution"] > 0][:top_n]
    return [d["label"] for d in active]


# ─── main ────────────────────────────────────────────────────────────────────

def run(min_delta: float = 5.0) -> dict:
    logger.info("Fetching score deltas from score_history …")

    with get_scraper_db() as db:
        rows = db.execute(DELTA_QUERY).mappings().all()

    rows = [dict(r) for r in rows]
    total_zips = len(rows)
    logger.info("Loaded %d zip codes with both current and prior snapshots", total_zips)

    notable = [r for r in rows if abs(r["delta"]) >= min_delta]
    logger.info(
        "%d zip(s) moved ±%.1f+ points (threshold: %.1f)",
        len(notable), min_delta, min_delta,
    )

    # Reference dates from the full dataset (not just notable)
    all_current_dates = {r["current_date"].isoformat() for r in rows}
    all_prior_dates   = {r["prior_date"].isoformat()   for r in rows}

    changes = []
    for r in notable:
        delta   = r["delta"]
        drivers = _signal_drivers(r)
        changes.append({
            "zip_code":        r["zip_code"],
            "current_score":   round(r["current_score"], 2),
            "prior_score":     round(r["prior_score"],   2),
            "delta":           round(delta, 2),
            "direction":       "rising" if delta > 0 else "falling",
            "current_date":    r["current_date"].isoformat(),
            "prior_date":      r["prior_date"].isoformat(),
            "primary_drivers": _primary_driver_labels(drivers),
            "signal_deltas":   drivers,
        })

    rising  = [c for c in changes if c["direction"] == "rising"]
    falling = [c for c in changes if c["direction"] == "falling"]

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "min_delta_threshold": min_delta,
        "analysis_window": {
            "current_dates": sorted(all_current_dates),
            "prior_dates":   sorted(all_prior_dates),
        },
        "summary": {
            "total_zips_analyzed":     total_zips,
            "zips_with_notable_change": len(notable),
            "rising":                  len(rising),
            "falling":                 len(falling),
        },
        "changes": changes,
    }

    OUTPUT.write_text(json.dumps(output, indent=2))
    logger.info("Saved → %s", OUTPUT)
    return output


def _print_summary(result: dict) -> None:
    s       = result["summary"]
    changes = result["changes"]

    print(f"\nWeekly Score Delta Analysis  (threshold: ±{result['min_delta_threshold']} pts)")
    print(f"  Zips analyzed:  {s['total_zips_analyzed']}")
    print(f"  Notable moves:  {s['zips_with_notable_change']}  "
          f"({s['rising']} rising, {s['falling']} falling)")

    if not changes:
        print("\n  No zip codes crossed the threshold this week.")
        return

    print(f"\n{'ZIP':<7} {'Prev':>6} {'Now':>6} {'Δ':>7}  Direction   Primary drivers")
    print("─" * 72)
    for c in changes:
        sign    = "+" if c["delta"] > 0 else ""
        drivers = ", ".join(c["primary_drivers"]) if c["primary_drivers"] else "—"
        print(
            f"{c['zip_code']:<7} "
            f"{c['prior_score']:>6.1f} "
            f"{c['current_score']:>6.1f} "
            f"{sign}{c['delta']:>6.1f}  "
            f"{'↑ rising' if c['direction'] == 'rising' else '↓ falling':<11} "
            f"{drivers}"
        )

    print(f"\n  Output: {OUTPUT}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--min-delta", type=float, default=5.0,
        help="Minimum absolute score change to surface (default: 5.0)",
    )
    args = parser.parse_args()

    result = run(min_delta=args.min_delta)
    _print_summary(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
