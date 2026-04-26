"""
Displacement score computation — full six-signal composite.

Weights (ANHD Displacement Alert Project methodology, rebalanced):
  LLC acquisitions:  26%
  Permits:           21%
  Complaint rate:    17%
  Evictions:         13%
  HPD violations:     8%  (Class B+C, 90-day window on multifamily parcels)
  RS unit loss:      15%  (DHCR — dormant until second annual scraper run, 2027)

  0.26 + 0.21 + 0.17 + 0.13 + 0.08 + 0.15 = 1.00 ✓

Assessment spike removed from active weight roster — replaced by HPD violations.
Will be reconsidered when assessment_history has ≥2 distinct tax years (2027+).

Per-unit normalization uses DOF residential unit counts from the parcels table.
Zero-unit and null-unit parcels fall back to the borough median units_res.
RS unit loss signal is not per-unit (it is already a percentage rate).

compute_scores(db) is the main entry point:
  1. Aggregate all six signals per zip code (raw counts, past 365 days).
  2. Pre-compute borough median units_res for the fallback denominator.
  3. Divide each signal count by per-unit denominator (borough median fallback).
  4. Normalize each per-unit signal to 0–100 via independent linear min-max.
  5. Redistribute dormant-signal weight proportionally to active signals (Step 5.5).
  5b. Compute weighted sum using effective weights → clamp to [1, 100].
  6. Upsert to displacement_scores on uq_displacement_scores_zip_code.
  7. Propagate scores to neighborhoods.current_score.
  8. Return the number of zip codes scored.

Run standalone:
    python scoring/compute.py
"""

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class ScoringGuardError(RuntimeError):
    """Raised when a batch of new scores fails the pre-commit sanity guard."""

from config.nyc import DISPLACEMENT_COMPLAINT_TYPES

# ---------------------------------------------------------------------------
# Signal weights — ANHD Displacement Alert Project methodology.
# Rebalanced in Phase 6-06 to add RS unit loss as 6th signal.
# Existing 5 signals scaled by 0.85 so all 6 sum to 1.0.
# ---------------------------------------------------------------------------
WEIGHT_LLC_ACQUISITIONS = 0.26
WEIGHT_PERMITS          = 0.21
WEIGHT_COMPLAINTS       = 0.17
WEIGHT_EVICTIONS        = 0.13
WEIGHT_HPD_VIOLATIONS   = 0.08   # Class B+C on 3+ unit parcels, 90-day inspection window
WEIGHT_RS_UNIT_LOSS     = 0.15   # DHCR — dormant until 2027 second annual scraper run
# Sum: 0.26 + 0.21 + 0.17 + 0.13 + 0.08 + 0.15 = 1.00 ✓

# Expected signal keys in every displacement score breakdown.
# If any key is missing, the score is invalid and must not be written to DB.
EXPECTED_SIGNAL_KEYS: frozenset = frozenset({
    "permits",
    "evictions",
    "llc_acquisitions",
    "hpd_violations",
    "complaint_rate",
    "rs_unit_loss",
})


def _assert_score_valid(zip_code: str, score: float, breakdown: dict) -> None:
    """
    Raise ValueError if the computed score or signal breakdown fails pre-commit invariants.

    Called once per zip code inside compute_scores() before the DB upsert.
    A ValueError means this zip's score is invalid and should be skipped.

    Invariants checked:
    1. score in [1.0, 100.0] — enforced by max(1, min(100, ...)) but asserted explicitly
    2. All EXPECTED_SIGNAL_KEYS present in breakdown
    3. Every signal value in [0.0, 100.0]
    """
    if not (1.0 <= score <= 100.0):
        raise ValueError(
            f"Score out of range for {zip_code}: {score} (must be in [1.0, 100.0])"
        )
    missing = EXPECTED_SIGNAL_KEYS - set(breakdown.keys())
    if missing:
        raise ValueError(
            f"Missing signal keys for {zip_code}: {missing}"
        )
    for key, val in breakdown.items():
        if key in EXPECTED_SIGNAL_KEYS and not (0.0 <= float(val) <= 100.0):
            raise ValueError(
                f"Signal {key}={val} out of [0.0, 100.0] for {zip_code}"
            )


# ---------------------------------------------------------------------------
# Signal aggregators — each returns (zip_code, count) tuples.
# All filter zip_code IS NOT NULL to prevent NULL grouping artifacts.
# ---------------------------------------------------------------------------

def _aggregate_permits(db: Session, cutoff: date | None = None) -> List[Tuple[str, int]]:
    """
    Return (zip_code, count) for alteration permits filed in the past 365 days,
    restricted to residential parcels (parcels.units_res > 0).
    Only permit_type = 'AL' (alterations to occupied buildings) on parcels with
    3+ residential units is counted. New Building (NB), Foundation (FO), Demolition
    (DM), and equipment/plumbing permits are excluded — they reflect construction on
    already-cleared sites or routine maintenance, not active tenant displacement.
    Single- and two-family parcels (units_res < 3) are excluded because alteration
    permits there are almost always owner-driven renovations, not landlord harassment.
    Permits without a BBL match in parcels are excluded (can't confirm residential).
    cutoff: explicit start-of-window date; defaults to today - 365 days when None.
    """
    effective = cutoff if cutoff is not None else (date.today() - timedelta(days=365))
    rows = db.execute(
        text(
            """
            SELECT pr.zip_code, COUNT(*) AS permit_count
            FROM permits_raw pr
            JOIN parcels p ON pr.bbl = p.bbl
            WHERE pr.zip_code IS NOT NULL
              AND pr.bbl IS NOT NULL
              AND pr.filing_date >= :cutoff
              AND p.units_res >= 3
              AND pr.permit_type = 'AL'
            GROUP BY pr.zip_code
            ORDER BY permit_count DESC
            """
        ),
        {"cutoff": effective},
    ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


def _aggregate_evictions(db: Session, cutoff: date | None = None) -> List[Tuple[str, int]]:
    """
    Return (zip_code, count) for residential evictions executed in the past 365 days.
    Note: OCA data lags executed_date by 2–4 weeks (documented in evictions_raw model).
    eviction_type = 'R' (residential) excludes commercial tenant evictions.
    ILIKE 'R%' handles both 'R' and 'Residential' across OCA dataset versions.
    cutoff: explicit start-of-window date; defaults to today - 365 days when None.
    """
    effective = cutoff if cutoff is not None else (date.today() - timedelta(days=365))
    rows = db.execute(
        text(
            """
            SELECT zip_code, COUNT(*) AS eviction_count
            FROM evictions_raw
            WHERE zip_code IS NOT NULL
              AND executed_date >= :cutoff
              AND eviction_type ILIKE 'R%'
            GROUP BY zip_code
            ORDER BY eviction_count DESC
            """
        ),
        {"cutoff": effective},
    ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


def _aggregate_llc_acquisitions(db: Session, cutoff: date | None = None) -> List[Tuple[str, int]]:
    """
    Return (zip_code, count) of LLC acquisitions (GRANTEE with LLC in normalized name)
    in the past 365 days, restricted to residential parcels (units_res > 0).
    Commercial REIT transfers (e.g. Goldman Sachs CMBS vehicles) are excluded —
    they inflate scores for commercial-heavy zips like 10018 (Garment District).

    Mortgage servicer exclusion: national servicers that acquire properties via
    foreclosure (e.g. Nationstar Mortgage LLC, Rocket Mortgage LLC, Lakeview Loan
    Servicing LLC, Newrez LLC) are excluded by SQL pattern matching on party name.
    These are not speculative investor purchases and would otherwise inflate LLC
    scores in lower-income neighborhoods, inverting the displacement signal.
    Excluded patterns (case-insensitive substring): MORTGAGE, LOAN SERVICING,
    LOAN SERVICE, LOAN FUNDER, FEDERAL SAVINGS, CREDIT UNION, LENDING,
    [ ]FINANCIAL[ ], [ ]FINANCIAL LLC, REVERSE LLC, GUIDANCE RESIDENTIAL.
    cutoff: explicit start-of-window date; defaults to today - 365 days when None.
    """
    effective = cutoff if cutoff is not None else (date.today() - timedelta(days=365))
    rows = db.execute(
        text(
            """
            SELECT p.zip_code, COUNT(*) AS llc_count
            FROM ownership_raw o
            JOIN parcels p ON o.bbl = p.bbl
            WHERE o.party_type = '2'
              AND o.doc_type IN ('DEED', 'DEEDP', 'ASST')
              AND o.party_name_normalized LIKE '%LLC%'
              AND o.doc_date >= :cutoff
              AND p.zip_code IS NOT NULL
              AND p.units_res > 0
              AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
              AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
              AND o.party_name_normalized NOT ILIKE '%LOAN SERVICE%'
              AND o.party_name_normalized NOT ILIKE '%LOAN FUNDER%'
              AND o.party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
              AND o.party_name_normalized NOT ILIKE '%CREDIT UNION%'
              AND o.party_name_normalized NOT ILIKE '%LENDING%'
              AND o.party_name_normalized NOT ILIKE '% FINANCIAL %'
              AND o.party_name_normalized NOT ILIKE '% FINANCIAL LLC'
              AND o.party_name_normalized NOT ILIKE '%REVERSE LLC'
              AND o.party_name_normalized NOT ILIKE '%GUIDANCE RESIDENTIAL%'
              AND NOT EXISTS (
                SELECT 1 FROM ownership_raw seller
                WHERE seller.document_id = o.document_id
                  AND seller.party_type = '1'
                  AND seller.party_name_normalized LIKE '%LLC%'
              )
            GROUP BY p.zip_code
            ORDER BY llc_count DESC
            """
        ),
        {"cutoff": effective},
    ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


def _aggregate_complaints(db: Session, cutoff: date | None = None) -> List[Tuple[str, int]]:
    """
    Return (zip_code, count) for displacement-relevant 311 complaints
    (filtered to DISPLACEMENT_COMPLAINT_TYPES at query time) in the past 365 days.
    Raw complaints_raw table retains all complaint types — filtering here only.
    Named parameter :types is passed as a list; SQLAlchemy maps it to a PostgreSQL
    array, preventing any SQL injection from the constant values.
    cutoff: explicit start-of-window date; defaults to today - 365 days when None.
    """
    effective = cutoff if cutoff is not None else (date.today() - timedelta(days=365))
    rows = db.execute(
        text(
            """
            SELECT zip_code, COUNT(*) AS complaint_count
            FROM complaints_raw
            WHERE zip_code IS NOT NULL
              AND complaint_type = ANY(:types)
              AND created_date >= :cutoff
            GROUP BY zip_code
            ORDER BY complaint_count DESC
            """
        ),
        {"types": list(DISPLACEMENT_COMPLAINT_TYPES), "cutoff": effective},
    ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]




def _aggregate_violations(db: Session, cutoff: date | None = None) -> List[Tuple[str, int]]:
    """
    Return (zip_code, count) of Class B and C HPD violations filed in the past
    90 days on 3+ unit residential parcels.

    Uses a 90-day window (not 365) so the signal captures recent active
    degradation rather than accumulated historical conditions. A building with
    steady old violations scores low; one with a recent cluster scores high.

    inspection_date is the temporal anchor — no current_status filter — so
    backfill runs are historically accurate regardless of how violations were
    later resolved.

    Restricted to parcels with 3+ residential units to focus on multifamily
    rental contexts and reduce noise from lower-density housing.

    cutoff: start of the 365-day scoring window (as_of_date - 365). The 90-day
    recent window is derived as cutoff + 275 days (= as_of_date - 90 days).
    """
    if cutoff is not None:
        recent_cutoff = cutoff + timedelta(days=275)
    else:
        recent_cutoff = date.today() - timedelta(days=90)

    rows = db.execute(
        text(
            """
            SELECT p.zip_code, COUNT(*) AS violation_count
            FROM violations_raw v
            JOIN parcels p ON v.bbl = p.bbl
            WHERE v.violation_class IN ('B', 'C')
              AND v.inspection_date >= :cutoff
              AND p.units_res >= 3
              AND p.zip_code IS NOT NULL
            GROUP BY p.zip_code
            ORDER BY violation_count DESC
            """
        ),
        {"cutoff": recent_cutoff},
    ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]

def _aggregate_assessment_spike(db: Session) -> List[Tuple[str, float]]:
    """
    Compute YoY assessment spike per ZIP, weighted by units_res.

    Joins assessment_history for the two most recent tax years on the same BBL,
    computes (current - prior) / prior as spike_pct per BBL, then aggregates
    to ZIP level as a units_res-weighted average (COALESCE zero-unit parcels to 1).

    Only positive spikes are counted — assessment decreases do not contribute.
    Returns [] if fewer than 2 distinct tax years exist in assessment_history,
    keeping the signal dormant until the second annual MapPLUTO run.
    """
    year_count = db.execute(
        text("SELECT COUNT(DISTINCT tax_year) FROM assessment_history")
    ).scalar() or 0

    if year_count < 2:
        return []

    rows = db.execute(
        text("""
            WITH two_years AS (
                SELECT tax_year,
                       ROW_NUMBER() OVER (ORDER BY tax_year DESC) AS rn
                FROM (SELECT DISTINCT tax_year FROM assessment_history) t
            ),
            current_h AS (
                SELECT bbl, assessed_total
                FROM assessment_history
                WHERE tax_year = (SELECT tax_year FROM two_years WHERE rn = 1)
            ),
            prior_h AS (
                SELECT bbl, assessed_total
                FROM assessment_history
                WHERE tax_year = (SELECT tax_year FROM two_years WHERE rn = 2)
            ),
            spike_per_bbl AS (
                SELECT c.bbl,
                       GREATEST(0.0,
                           (c.assessed_total - p.assessed_total)::float / p.assessed_total
                       ) AS spike_pct
                FROM current_h c
                JOIN prior_h p ON c.bbl = p.bbl
                WHERE p.assessed_total > 0
                  AND c.assessed_total > p.assessed_total
            )
            SELECT par.zip_code,
                   SUM(s.spike_pct * COALESCE(NULLIF(par.units_res, 0), 1)) /
                   NULLIF(SUM(COALESCE(NULLIF(par.units_res, 0), 1)), 0) AS weighted_spike
            FROM spike_per_bbl s
            JOIN parcels par ON par.bbl = s.bbl
            WHERE par.zip_code IS NOT NULL
            GROUP BY par.zip_code
            ORDER BY weighted_spike DESC
        """)
    ).fetchall()

    return [(str(r[0]), float(r[1])) for r in rows]


def _aggregate_rs_unit_loss(db: Session) -> List[Tuple[str, float]]:
    """
    Compute YoY RS unit loss as pct per ZIP.
    Joins rs_buildings (current and prior year per BBL) to parcels for ZIP.
    Returns (zip_code, avg_pct_loss) for ZIPs with >= 1 building showing loss.
    Uses CURRENT_DATE year and year-1 for comparison.

    Query is scoped to current and prior year only; parcels join is indexed on bbl
    (T-06-06-04: DoS mitigation — bounded scan, not full-table).

    DORMANCY NOTE: The DHCR scraper (scrapers/dhcr_rs.py) writes the current
    calendar year as the snapshot year.  The original multi-year dataset
    (yn95-5t2d) was decommissioned 2026-04-12; the replacement dataset
    (kj4p-ruqc) is a live snapshot with no year field, so only one year of data
    accumulates per annual run.  Until the second annual scraper run completes
    (expected 2027), the prior_year CTE will always be empty, this function
    returns [], and rs_norm will be 0.0 for all ZIPs.  compute_scores() emits a
    WARNING when this condition is detected.
    """
    rows = db.execute(
        text("""
            WITH current_year AS (
                SELECT bbl, rs_unit_count AS current_units
                FROM rs_buildings
                WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)::int
            ),
            prior_year AS (
                SELECT bbl, rs_unit_count AS prior_units
                FROM rs_buildings
                WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)::int - 1
            ),
            loss_per_bbl AS (
                SELECT c.bbl,
                       CASE WHEN p.prior_units > 0
                            THEN GREATEST(0, (p.prior_units - c.current_units)::float / p.prior_units)
                            ELSE 0 END AS loss_pct
                FROM current_year c
                JOIN prior_year p ON c.bbl = p.bbl
                WHERE p.prior_units > c.current_units
            )
            SELECT par.zip_code, AVG(l.loss_pct) AS avg_loss_pct
            FROM loss_per_bbl l
            JOIN parcels par ON par.bbl = l.bbl
            WHERE par.zip_code IS NOT NULL
            GROUP BY par.zip_code
            ORDER BY avg_loss_pct DESC
        """)
    ).fetchall()
    return [(str(r[0]), float(r[1])) for r in rows]


# ---------------------------------------------------------------------------
# Per-unit normalization helpers
# ---------------------------------------------------------------------------

def _compute_borough_medians(db: Session) -> Dict[str, float]:
    """
    Pre-compute median units_res per borough from parcels where units_res > 0.
    Returns {"1": median, "2": median, ...} for Manhattan through Staten Island.
    Used as fallback denominator when a zip code has no positive-unit parcels.
    Fallback: missing boroughs default to 1.0 to prevent ZeroDivisionError.
    """
    rows = db.execute(
        text(
            """
            SELECT
                borough::text,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY units_res) AS median_units
            FROM parcels
            WHERE units_res > 0
              AND borough IS NOT NULL
            GROUP BY borough
            """
        )
    ).fetchall()
    result = {str(r[0]): float(r[1]) for r in rows}
    # Ensure all five boroughs have a fallback — prevents KeyError in _per_unit
    for b in ("1", "2", "3", "4", "5"):
        if b not in result:
            result[b] = 1.0
    return result


def _get_zip_units(db: Session) -> Dict[str, Optional[float]]:
    """
    Return total units_res per zip code from parcels where units_res > 0.
    Uses SUM (total housing units in zip) not AVG — AVG produces the wrong
    denominator: dense zips with large buildings get inflated AVG, collapsing
    their per-unit signals to near-zero and inverting the displacement signal.
    Zip codes absent from result have no parcels with positive unit counts.
    """
    rows = db.execute(
        text(
            """
            SELECT zip_code, SUM(units_res::float) AS total_units
            FROM parcels
            WHERE zip_code IS NOT NULL
              AND units_res > 0
            GROUP BY zip_code
            """
        )
    ).fetchall()
    return {str(r[0]): float(r[1]) for r in rows}


def _get_zip_borough(db: Session) -> Dict[str, int]:
    """
    Return the most common borough for each zip code (DISTINCT ON, ordered by borough).
    Used to look up the borough median fallback when a zip has no positive-unit parcels.
    """
    rows = db.execute(
        text(
            """
            SELECT DISTINCT ON (zip_code) zip_code, borough
            FROM parcels
            WHERE zip_code IS NOT NULL
              AND borough IS NOT NULL
            ORDER BY zip_code, borough
            """
        )
    ).fetchall()
    return {str(r[0]): int(r[1]) for r in rows}


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def _normalize(count: float, min_count: float, max_count: float) -> float:
    """
    Linear min-max normalization to [0, 100].
    Returns 0.0 when min == max == 0 (no signal data).
    Returns 50.0 when min == max != 0 (single non-zero data point or all equal).
    """
    if max_count == min_count:
        return 0.0 if max_count == 0 else 50.0
    return round((count - min_count) / (max_count - min_count) * 100, 1)


def _per_unit(count: int, zip_code: str,
              zip_units: Dict[str, Optional[float]],
              zip_borough: Dict[str, int],
              borough_medians: Dict[str, float]) -> float:
    """
    Divide raw signal count by residential unit count for a zip code.
    Falls back to borough median when units_res is null, zero, or absent.
    Never raises ZeroDivisionError — always has a positive denominator.
    """
    units = zip_units.get(zip_code)
    if not units or units <= 0:
        borough = zip_borough.get(zip_code)
        if borough:
            units = borough_medians.get(str(borough), 1.0)
        else:
            units = 1.0  # final fallback: unknown borough → 1 unit
    return count / units


# ---------------------------------------------------------------------------
# Batch sanity guard
# ---------------------------------------------------------------------------

def _count_active_signals(signal_norms: dict[str, dict]) -> int:
    """
    Count signals where the mean normalized value across all scored ZIPs is > 1.0.
    A signal with mean <= 1.0 is effectively zero across the whole city — dormant or broken.
    """
    active = 0
    for norm_dict in signal_norms.values():
        vals = list(norm_dict.values())
        if vals and (sum(vals) / len(vals)) > 1.0:
            active += 1
    return active


def _batch_sanity_check(
    db: Session,
    computed_scores: dict,
    signal_norms: dict[str, dict],
    force: bool = False,
) -> None:
    """
    Compare the new score batch against the current live displacement_scores.
    Raises ScoringGuardError if any threshold is violated and force is False.
    Logs details on both pass and block.

    Thresholds (any one triggers block):
      1. new max score < 50% of previous max
      2. new average score < 50% of previous average
      3. > 50% of ZIPs have score <= 5
      4. active signal count collapses (>= 4 previously, <= 2 now)
      5. scored ZIP count < 170
    """
    if force:
        logger.warning("scoring guard bypassed — --force flag active. Writing scores unconditionally.")
        return

    new_scores = list(computed_scores.values())
    new_count  = len(new_scores)
    new_max    = max(new_scores) if new_scores else 0.0
    new_avg    = sum(new_scores) / new_count if new_count else 0.0
    new_active = _count_active_signals(signal_norms)
    near_floor = sum(1 for s in new_scores if s <= 5)

    # Read current live baseline
    prev_rows = db.execute(text(
        "SELECT score, signal_breakdown FROM displacement_scores WHERE score IS NOT NULL"
    )).fetchall()

    if prev_rows:
        prev_scores = [float(r[0]) for r in prev_rows]
        prev_max    = max(prev_scores)
        prev_avg    = sum(prev_scores) / len(prev_scores)

        # Count previously active signals from JSONB breakdown (sample all rows)
        prev_signal_sums: dict[str, float] = {}
        prev_signal_counts: dict[str, int] = {}
        for r in prev_rows:
            breakdown = r[1] if isinstance(r[1], dict) else {}
            for sig, val in breakdown.items():
                if isinstance(val, (int, float)):
                    prev_signal_sums[sig]   = prev_signal_sums.get(sig, 0.0) + float(val)
                    prev_signal_counts[sig] = prev_signal_counts.get(sig, 0) + 1
        prev_active = sum(
            1 for sig in prev_signal_sums
            if prev_signal_counts[sig] > 0
            and prev_signal_sums[sig] / prev_signal_counts[sig] > 1.0
        )
    else:
        # No prior data — allow first-ever run without comparison
        logger.info("scoring guard: no prior displacement_scores — skipping comparison thresholds")
        prev_max = prev_avg = 0.0
        prev_active = 0

    def _block(reason: str) -> None:
        logger.error(
            "scoring guard blocked live score update: %s | "
            "prev max=%.1f new max=%.1f | prev avg=%.1f new avg=%.1f | "
            "prev active signals=%d new active signals=%d | scored ZIPs=%d",
            reason,
            prev_max, new_max, prev_avg, new_avg,
            prev_active, new_active, new_count,
        )
        raise ScoringGuardError(reason)

    # Threshold 1: scored ZIP count
    if new_count < 170:
        _block(f"ZIP count too low: {new_count} (threshold: 170)")

    # Threshold 2: max score collapse
    if prev_max > 0 and new_max < prev_max * 0.50:
        _block(f"max score collapsed: {prev_max:.1f} -> {new_max:.1f} (<50% of previous)")

    # Threshold 3: average score collapse
    if prev_avg > 0 and new_avg < prev_avg * 0.50:
        _block(f"avg score collapsed: {prev_avg:.1f} -> {new_avg:.1f} (<50% of previous)")

    # Threshold 4: > 50% of ZIPs near floor
    if new_count > 0 and near_floor > new_count * 0.50:
        _block(f"{near_floor}/{new_count} ZIPs have score <= 5 (>50%)")

    # Threshold 5: active signal collapse
    if prev_active >= 4 and new_active <= 2:
        _block(
            f"active signal count collapsed: {prev_active} -> {new_active} "
            f"(was >=4 active signals, now <=2)"
        )

    logger.info(
        "scoring guard passed: max=%.1f avg=%.1f active_signals=%d zips=%d",
        new_max, new_avg, new_active, new_count,
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def compute_scores(db: Session, as_of_date: date | None = None, force: bool = False) -> int:
    """
    Compute full six-signal composite displacement scores for all zip codes.
    Returns the number of zip codes scored and upserted.

    as_of_date: when provided, scores are computed as if today were that date
    (signals use a 365-day window ending on as_of_date). Used by the backfill
    script to populate score_history for historical dates. When None (default),
    the current date is used — standard nightly pipeline behavior.

    The 6th signal (rs_unit_loss) is stored in signal_breakdown JSONB only —
    no separate float column added to displacement_scores (JSONB is sufficient
    for the API response and score panel).
    """
    cutoff = (as_of_date - timedelta(days=365)) if as_of_date is not None else None

    # --- Step 1: Aggregate all signals ---
    permit_rows = _aggregate_permits(db, cutoff=cutoff)
    eviction_rows = _aggregate_evictions(db, cutoff=cutoff)
    llc_rows = _aggregate_llc_acquisitions(db, cutoff=cutoff)
    complaint_rows = _aggregate_complaints(db, cutoff=cutoff)
    violation_rows        = _aggregate_violations(db, cutoff=cutoff)
    assessment_spike_rows = _aggregate_assessment_spike(db)
    rs_loss_rows          = _aggregate_rs_unit_loss(db)
    # Dormancy check: warn when assessment spike has fewer than 2 tax years.
    # _aggregate_assessment_spike already ran the year count internally and
    # returned [] — no need for a second COUNT query here.
    if not violation_rows:
        logger.warning(
            "HPD violations signal is dormant: violations_raw table is empty."
            " Run the violations scraper to activate this signal."
        )
    if not assessment_spike_rows:
        logger.warning(
            "Assessment spike signal is dormant: assessment_history has <2 tax years."
            " Signal activates after the second annual MapPLUTO run."
        )
    # Dormancy check: warn when prior-year DHCR data is absent (expected until 2027).
    if not rs_loss_rows:
        # Only fire when current-year data exists — confirms the gap is a data issue,
        # not a schema/connection problem.
        current_year_count = db.execute(
            text(
                "SELECT COUNT(*) FROM rs_buildings"
                " WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)::int"
            )
        ).scalar() or 0
        if current_year_count > 0:
            prior_year = datetime.now(timezone.utc).year - 1
            logger.warning(
                "RS unit loss signal is dormant: no prior-year (%d) data in"
                " rs_buildings. Signal becomes active after second annual DHCR"
                " scraper run.",
                prior_year,
            )
    # Assessment spike: dormant Phase 4 — no YoY DOF baseline yet.
    # TODO: Phase 6+ — implement YoY comparison once 2+ months of DOF history exists.

    # Bail early if no data at all
    if not (permit_rows or eviction_rows or llc_rows or complaint_rows):
        return 0

    # --- Step 2: Pre-compute per-unit denominators ---
    borough_medians = _compute_borough_medians(db)
    zip_units = _get_zip_units(db)
    zip_borough = _get_zip_borough(db)

    # --- Step 3: Build union of all zip codes across signals ---
    permit_map: Dict[str, int] = dict(permit_rows)
    eviction_map: Dict[str, int] = dict(eviction_rows)
    llc_map: Dict[str, int] = dict(llc_rows)
    complaint_map: Dict[str, int] = dict(complaint_rows)
    violation_map: Dict[str, int] = dict(violation_rows)
    # assessment_spike_map: (zip_code -> weighted avg spike pct) — dormant until 2 years exist
    assessment_spike_map: Dict[str, float] = dict(assessment_spike_rows)
    # rs_loss_map: (zip_code -> avg_pct_loss float 0.0–1.0) — not per-unit
    rs_loss_map: Dict[str, float] = dict(rs_loss_rows)

    # Only score residential zip codes — zips absent from zip_units have no
    # residential units in parcels and would use the 1.0 fallback denominator,
    # making them dominate per-unit normalization over genuine neighbourhoods.
    # Guard: if zip_units is empty (parcels not loaded, or test mock), skip the
    # filter so scoring still runs rather than silently producing zero rows.
    residential_zips = set(zip_units.keys())
    all_zips_raw = (
        set(permit_map) | set(eviction_map) | set(llc_map) | set(complaint_map)
        | set(violation_map) | set(rs_loss_map)
    )
    all_zips = (all_zips_raw & residential_zips) if residential_zips else all_zips_raw
    if not all_zips:
        return 0

    # --- Step 4: Compute per-unit values for each signal across all zips ---
    permit_pu: Dict[str, float] = {}
    eviction_pu: Dict[str, float] = {}
    llc_pu: Dict[str, float] = {}
    complaint_pu: Dict[str, float] = {}
    violation_pu: Dict[str, float] = {}
    # Assessment spike is already a weighted-avg rate — no per-unit division needed
    assessment_spike_pu: Dict[str, float] = {z: assessment_spike_map.get(z, 0.0) for z in all_zips}
    # RS unit loss is already a rate (pct 0.0–1.0) — no per-unit division needed
    rs_loss_pu: Dict[str, float] = {z: rs_loss_map.get(z, 0.0) for z in all_zips}

    for z in all_zips:
        permit_pu[z] = _per_unit(
            permit_map.get(z, 0), z, zip_units, zip_borough, borough_medians
        )
        eviction_pu[z] = _per_unit(
            eviction_map.get(z, 0), z, zip_units, zip_borough, borough_medians
        )
        llc_pu[z] = _per_unit(
            llc_map.get(z, 0), z, zip_units, zip_borough, borough_medians
        )
        complaint_pu[z] = _per_unit(
            complaint_map.get(z, 0), z, zip_units, zip_borough, borough_medians
        )
        violation_pu[z] = _per_unit(
            violation_map.get(z, 0), z, zip_units, zip_borough, borough_medians
        )

    # --- Step 5: Normalize each signal independently to [0, 100] ---
    # Uses 5th–95th percentile range instead of absolute min–max.
    # Pure min-max collapses all scores when signals peak in different zips
    # (the absolute max is one outlier zip; everyone else scores <30).
    # Percentile normalization ensures the top 5% of zips fill the high end
    # of each signal's scale, producing a spread that fills 1–100 in practice.
    def _norm_map(pu_dict: Dict[str, float]) -> Dict[str, float]:
        vals = sorted(pu_dict.values())
        n = len(vals)
        if n == 0:
            return {}
        if n == 1:
            # Single data point: zero means no activity (stay at 0.0 so dormancy
            # detection works correctly); non-zero gets 50.0 (can't rank it).
            return {z: (0.0 if vals[0] == 0.0 else 50.0) for z in pu_dict}
        p5  = vals[max(0, int(n * 0.05))]
        p95 = vals[min(n - 1, int(n * 0.95))]
        # Degenerate range: happens when the signal is sparse (95%+ of ZIPs have
        # zero activity) or when both percentiles land on the same value.
        # Fall back to absolute min-max so ZIPs with real activity still score
        # above zero instead of silently collapsing the entire signal to dormant.
        if p5 == p95:
            p5, p95 = vals[0], vals[-1]
        return {
            z: max(0.0, min(100.0, _normalize(v, p5, p95)))
            for z, v in pu_dict.items()
        }

    permit_norm = _norm_map(permit_pu)
    eviction_norm = _norm_map(eviction_pu)
    llc_norm = _norm_map(llc_pu)
    complaint_norm = _norm_map(complaint_pu)
    violation_norm = _norm_map(violation_pu)
    assessment_spike_norm = _norm_map(assessment_spike_pu)
    rs_loss_norm = _norm_map(rs_loss_pu)

    # --- Step 5.5: Effective weights — redistribute dormant signal weight ---
    # A signal is dormant when every zip has a normalized value of 0.0 (no data).
    # Without redistribution, dormant signals (hpd_violations when no data,
    # rs_unit_loss=15%) would cap the achievable composite, deflating all scores
    # by ~23% versus the ANHD methodology intent. Active-signal weights are rescaled
    # to sum to 1.0 so the full 1–100 range is reachable with whatever data exists.
    # When a dormant signal comes online the redistribution shrinks automatically.
    _raw_weights: Dict[str, float] = {
        "llc_acquisitions":  WEIGHT_LLC_ACQUISITIONS,
        "permits":           WEIGHT_PERMITS,
        "complaint_rate":    WEIGHT_COMPLAINTS,
        "evictions":         WEIGHT_EVICTIONS,
        "hpd_violations":    WEIGHT_HPD_VIOLATIONS,
        "rs_unit_loss":      WEIGHT_RS_UNIT_LOSS,
    }
    _norms_by_key: Dict[str, Dict[str, float]] = {
        "llc_acquisitions":  llc_norm,
        "permits":           permit_norm,
        "complaint_rate":    complaint_norm,
        "evictions":         eviction_norm,
        "hpd_violations":    violation_norm,
        "rs_unit_loss":      rs_loss_norm,
    }
    _active_signals: set = {
        k for k, norm in _norms_by_key.items()
        if any(v > 0.0 for v in norm.values())
    }
    _active_weight_sum: float = sum(_raw_weights[k] for k in _active_signals) or 1.0
    effective_weights: Dict[str, float] = {
        k: (_raw_weights[k] / _active_weight_sum) if k in _active_signals else 0.0
        for k in _raw_weights
    }
    if len(_active_signals) < len(_raw_weights):
        _dormant = sorted(set(_raw_weights) - _active_signals)
        logger.info(
            "Weight redistribution: %d dormant signal(s) %s (%.0f%% of mass) "
            "redistributed proportionally to %d active signals.",
            len(_dormant), _dormant,
            (1.0 - _active_weight_sum) * 100,
            len(_active_signals),
        )

    # --- Step 6: Upsert to displacement_scores ---
    now = datetime.now(timezone.utc)
    sig_updated = {
        "permits": now.isoformat(),
        "evictions": now.isoformat(),
        "llc_acquisitions": now.isoformat(),
        "hpd_violations": now.isoformat(),
        "complaint_rate": now.isoformat(),
        "rs_unit_loss": now.isoformat(),
    }

    # Track validation failures to detect systemic scoring issues
    skipped_zips: list = []
    # Cache computed scores so Step 7 (score_history) reuses them instead of
    # re-deriving the composite — prevents silent divergence if weights change.
    computed_scores: Dict[str, float] = {}

    for zip_code in sorted(all_zips):
        p_norm = permit_norm.get(zip_code, 0.0)
        e_norm = eviction_norm.get(zip_code, 0.0)
        l_norm = llc_norm.get(zip_code, 0.0)
        c_norm = complaint_norm.get(zip_code, 0.0)
        v_norm = violation_norm.get(zip_code, 0.0)
        a_norm = assessment_spike_norm.get(zip_code, 0.0)
        rs_norm = rs_loss_norm.get(zip_code, 0.0)

        composite = (
            effective_weights["llc_acquisitions"] * l_norm
            + effective_weights["permits"] * p_norm
            + effective_weights["complaint_rate"] * c_norm
            + effective_weights["evictions"] * e_norm
            + effective_weights["hpd_violations"] * v_norm
            + effective_weights["rs_unit_loss"] * rs_norm
        )
        # Clamp to [1, 100] — minimum 1 so no zip ever shows "0 risk"
        score = max(1.0, min(100.0, round(composite, 1)))
        computed_scores[zip_code] = score

        breakdown = {
            "permits": p_norm,
            "evictions": e_norm,
            "llc_acquisitions": l_norm,
            "hpd_violations": v_norm,
            "complaint_rate": c_norm,
            "rs_unit_loss": rs_norm,
        }

        # --- Pre-commit sanity check ---
        try:
            _assert_score_valid(zip_code, score, breakdown)
        except ValueError as val_err:
            logger.warning(
                "Score sanity check failed for %s — skipping upsert: %s",
                zip_code,
                val_err,
            )
            skipped_zips.append(zip_code)
            continue  # skip this zip, do not write to DB

        # Backfill runs skip the current-score tables — writing historical scores
        # to displacement_scores would overwrite live data.
        if as_of_date is None:
            db.execute(
                text(
                    """
                    INSERT INTO displacement_scores
                        (zip_code, score, signal_breakdown, permit_intensity,
                         eviction_rate, llc_acquisition_rate, assessment_spike,
                         complaint_rate, cache_generated_at, signal_last_updated,
                         created_at, updated_at)
                    VALUES
                        (:zip_code, :score, CAST(:breakdown AS jsonb), :permit_intensity,
                         :eviction_rate, :llc_acquisition_rate, :assessment_spike,
                         :complaint_rate, :now, CAST(:sig_updated AS jsonb),
                         :now, :now)
                    ON CONFLICT ON CONSTRAINT uq_displacement_scores_zip_code
                    DO UPDATE SET
                        score                = EXCLUDED.score,
                        signal_breakdown     = EXCLUDED.signal_breakdown,
                        permit_intensity     = EXCLUDED.permit_intensity,
                        eviction_rate        = EXCLUDED.eviction_rate,
                        llc_acquisition_rate = EXCLUDED.llc_acquisition_rate,
                        assessment_spike     = EXCLUDED.assessment_spike,
                        complaint_rate       = EXCLUDED.complaint_rate,
                        cache_generated_at   = EXCLUDED.cache_generated_at,
                        signal_last_updated  = EXCLUDED.signal_last_updated,
                        updated_at           = EXCLUDED.updated_at
                    """
                ),
                {
                    "zip_code": zip_code,
                    "score": score,
                    "breakdown": json.dumps(breakdown),
                    "permit_intensity": p_norm,
                    "eviction_rate": e_norm,
                    "llc_acquisition_rate": l_norm,
                    "assessment_spike": a_norm,
                    "complaint_rate": c_norm,
                    "now": now,
                    "sig_updated": json.dumps(sig_updated),
                },
            )

    # Systemic failure detection: if >50% of zips failed validation,
    # something is wrong with normalization — abort and return 0.
    total_attempted = len(all_zips)
    if skipped_zips and len(skipped_zips) > total_attempted * 0.5:
        logger.error(
            "Scoring aborted: %d/%d zips failed sanity check (>50%%). "
            "Possible normalization or signal aggregation bug. "
            "No scores written to displacement_scores.",
            len(skipped_zips),
            total_attempted,
        )
        db.rollback()
        return 0

    # --- Batch sanity guard ---
    # Compares the full new batch against live displacement_scores before committing.
    # Rolls back and raises ScoringGuardError if any threshold is violated.
    # Skipped during backfill runs (as_of_date is set) — those never touch live tables.
    if as_of_date is None:
        signal_norms = {
            "permits":        permit_norm,
            "evictions":      eviction_norm,
            "llc_acquisitions": llc_norm,
            "hpd_violations": violation_norm,
            "complaint_rate": complaint_norm,
            "rs_unit_loss":   rs_loss_norm,
        }
        try:
            _batch_sanity_check(db, computed_scores, signal_norms, force=force)
        except ScoringGuardError:
            db.rollback()
            raise  # propagate so cron/CLI exits nonzero

    db.commit()

    if skipped_zips:
        logger.warning(
            "Scoring complete with %d skipped zip(s): %s",
            len(skipped_zips),
            skipped_zips[:10],  # log up to 10 skipped zips
        )

    scored_count = total_attempted - len(skipped_zips)

    # --- Step 7: Write daily snapshot to score_history ---
    # ON CONFLICT DO NOTHING — if scoring runs multiple times on the same date, only keep first.
    # When as_of_date is provided (backfill), use it as scored_at; otherwise use today.
    scored_at = as_of_date if as_of_date is not None else date.today()
    skipped_set = set(skipped_zips)
    for zip_code in sorted(all_zips):
        if zip_code in skipped_set:   # don't snapshot invalid scores
            continue
        p_norm = permit_norm.get(zip_code, 0.0)
        e_norm = eviction_norm.get(zip_code, 0.0)
        l_norm = llc_norm.get(zip_code, 0.0)
        c_norm = complaint_norm.get(zip_code, 0.0)
        v_norm = violation_norm.get(zip_code, 0.0)
        rs_norm = rs_loss_norm.get(zip_code, 0.0)
        score = computed_scores[zip_code]
        db.execute(
            text(
                """
                INSERT INTO score_history
                    (zip_code, scored_at, composite_score, permit_intensity,
                     eviction_rate, llc_acquisition_rate, hpd_violations,
                     complaint_rate, rs_unit_loss, created_at, updated_at)
                VALUES
                    (:zip_code, :scored_at, :composite_score, :permit_intensity,
                     :eviction_rate, :llc_acquisition_rate, :hpd_violations,
                     :complaint_rate, :rs_unit_loss, :now, :now)
                ON CONFLICT ON CONSTRAINT uq_score_history_zip_date DO NOTHING
                """
            ),
            {
                "zip_code": zip_code,
                "scored_at": scored_at,
                "composite_score": score,
                "permit_intensity": p_norm,
                "eviction_rate": e_norm,
                "llc_acquisition_rate": l_norm,
                "hpd_violations": v_norm,
                "complaint_rate": c_norm,
                "rs_unit_loss": rs_norm,
                "now": now,
            },
        )
    db.commit()

    # --- Step 8: Propagate scores to neighborhoods.current_score ---
    # Skipped during backfill — historical scores must not overwrite live data.
    if as_of_date is None:
        db.execute(
            text(
                """
                UPDATE neighborhoods n
                SET current_score = ds.score
                FROM displacement_scores ds
                WHERE n.zip_code = ds.zip_code
                """
            )
        )
        db.commit()

    # --- Step 9: Remove orphan ZIPs from displacement_scores ---
    # ZIPs that appear in raw data but have no ZCTA geometry in neighborhoods
    # accumulate across scoring runs (suburban LI ZIPs, commercial midtown ZIPs,
    # test ZIPs, occasional out-of-state BBLs). They are invisible to the map API
    # (which JOINs on neighborhoods) but inflate rowcount and make table stats
    # misleading. Skipped during backfill — only the live table is affected.
    if as_of_date is None:
        result = db.execute(text("""
            DELETE FROM displacement_scores
            WHERE zip_code NOT IN (SELECT zip_code FROM neighborhoods)
        """))
        db.commit()
        if result.rowcount:
            logger.info(
                "Removed %d orphan ZIP(s) from displacement_scores "
                "(no matching ZCTA geometry in neighborhoods)",
                result.rowcount,
            )

    return scored_count


# ---------------------------------------------------------------------------
# snapshot_scores — standalone daily snapshot, safe to call outside compute_scores()
# ---------------------------------------------------------------------------

def snapshot_scores(db) -> None:
    """
    Copy current displacement_scores into score_history for today's date.
    ON CONFLICT DO NOTHING makes this idempotent — safe to call multiple times per day.
    """
    from datetime import date, datetime, timezone
    db.execute(
        text(
            """
            INSERT INTO score_history
                (zip_code, scored_at, composite_score, permit_intensity,
                 eviction_rate, llc_acquisition_rate, hpd_violations,
                 complaint_rate, rs_unit_loss, created_at, updated_at)
            SELECT
                zip_code,
                :scored_at,
                score,
                permit_intensity,
                eviction_rate,
                llc_acquisition_rate,
                NULL,
                complaint_rate,
                NULL,
                :now,
                :now
            FROM displacement_scores
            WHERE score IS NOT NULL
            ON CONFLICT ON CONSTRAINT uq_score_history_zip_date DO NOTHING
            """
        ),
        {"scored_at": date.today(), "now": datetime.now(timezone.utc)},
    )
    db.commit()


# ---------------------------------------------------------------------------
# Standalone execution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    from config.logging_config import configure_logging
    from models.database import get_scraper_db

    configure_logging()

    parser = argparse.ArgumentParser(description="PulseCities displacement scoring engine")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass the batch sanity guard and write scores unconditionally. "
             "Use only for manual recovery — never in cron.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute scores and run the sanity guard but do not write to any table.",
    )
    args = parser.parse_args()

    if args.dry_run:
        print("DRY RUN — scores will be computed and guard evaluated but nothing written.")

    try:
        with get_scraper_db() as db:
            if args.dry_run:
                # Run computation and guard check, then rollback all writes.
                n = compute_scores(db, force=args.force)
                db.rollback()
                print(f"DRY RUN complete. Would have scored {n} zip codes.")
            else:
                n = compute_scores(db, force=args.force)
                print(f"Scored {n} zip codes.")
        sys.exit(0)
    except ScoringGuardError as exc:
        print(f"ERROR: scoring guard blocked write — {exc}", file=sys.stderr)
        sys.exit(1)
