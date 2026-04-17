"""
Displacement score computation — full six-signal composite.

Weights (ANHD Displacement Alert Project methodology, rebalanced Phase 6-06):
  LLC acquisitions:  26%  (was 30%; rebalanced to add RS unit loss signal)
  Permits:           21%  (was 25%; rebalanced proportionally)
  Complaint rate:    17%  (was 20%; rebalanced proportionally)
  Evictions:         13%  (was 15%; rebalanced proportionally)
  Assessment spike:   8%  (was 10%; rebalanced proportionally) <- DORMANT: always 0.0
  RS unit loss:      15%  (NEW Phase 6-06: YoY RS unit loss per BBL via DHCR)

Rebalancing: existing 5 signals scaled by 0.85 multiplier so all 6 sum to 1.0.
  0.26 + 0.21 + 0.17 + 0.13 + 0.08 + 0.15 = 1.00 ✓

TODO: Phase 6+ — implement assessment_spike YoY comparison once 2+ months
      of DOF history exists (successive annual assessments per parcel).

Per-unit normalization uses DOF residential unit counts from the parcels table.
Zero-unit and null-unit parcels fall back to the borough median units_res.
RS unit loss signal is not per-unit (it is already a percentage rate).

compute_scores(db) is the main entry point:
  1. Aggregate all six signals per zip code (raw counts, past 365 days).
  2. Pre-compute borough median units_res for the fallback denominator.
  3. Divide each signal count by per-unit denominator (borough median fallback).
  4. Normalize each per-unit signal to 0–100 via independent linear min-max.
  5. Compute weighted sum → clamp to [1, 100].
  6. Upsert to displacement_scores on uq_displacement_scores_zip_code.
  7. Propagate scores to neighborhoods.current_score.
  8. Return the number of zip codes scored.

Run standalone:
    python scoring/compute.py
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

from config.nyc import DISPLACEMENT_COMPLAINT_TYPES

# ---------------------------------------------------------------------------
# Signal weights — ANHD Displacement Alert Project methodology.
# Rebalanced in Phase 6-06 to add RS unit loss as 6th signal.
# Existing 5 signals scaled by 0.85 so all 6 sum to 1.0.
# ---------------------------------------------------------------------------
WEIGHT_LLC_ACQUISITIONS = 0.26   # was 0.30; scaled × 0.85
WEIGHT_PERMITS          = 0.21   # was 0.25; scaled × 0.85
WEIGHT_COMPLAINTS       = 0.17   # was 0.20; scaled × 0.85
WEIGHT_EVICTIONS        = 0.13   # was 0.15; scaled × 0.85
WEIGHT_ASSESSMENT_SPIKE = 0.08   # was 0.10; scaled × 0.85 — dormant Phase 4 (always 0.0)
WEIGHT_RS_UNIT_LOSS     = 0.15   # NEW Phase 6-06: YoY RS unit loss per BBL (DHCR dataset)
# Sum: 0.26 + 0.21 + 0.17 + 0.13 + 0.08 + 0.15 = 1.00 ✓

# Expected signal keys in every displacement score breakdown.
# If any key is missing, the score is invalid and must not be written to DB.
EXPECTED_SIGNAL_KEYS: frozenset = frozenset({
    "permits",
    "evictions",
    "llc_acquisitions",
    "assessment_spike",
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

def _aggregate_permits(db: Session) -> List[Tuple[str, int]]:
    """
    Return (zip_code, count) for permits filed in the past 365 days,
    restricted to residential parcels (parcels.units_res > 0).
    Commercial permits (office towers, retail) are excluded — they inflate
    scores for mixed-use and already-gentrified zips (e.g. 10018, 10013).
    Permits without a BBL match in parcels are excluded (can't confirm residential).
    """
    rows = db.execute(
        text(
            """
            SELECT pr.zip_code, COUNT(*) AS permit_count
            FROM permits_raw pr
            JOIN parcels p ON pr.bbl = p.bbl
            WHERE pr.zip_code IS NOT NULL
              AND pr.bbl IS NOT NULL
              AND pr.filing_date >= CURRENT_DATE - INTERVAL '365 days'
              AND p.units_res > 0
            GROUP BY pr.zip_code
            ORDER BY permit_count DESC
            """
        )
    ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


def _aggregate_evictions(db: Session) -> List[Tuple[str, int]]:
    """
    Return (zip_code, count) for residential evictions executed in the past 365 days.
    Note: OCA data lags executed_date by 2–4 weeks (documented in evictions_raw model).
    eviction_type = 'R' (residential) excludes commercial tenant evictions.
    ILIKE 'R%' handles both 'R' and 'Residential' across OCA dataset versions.
    """
    rows = db.execute(
        text(
            """
            SELECT zip_code, COUNT(*) AS eviction_count
            FROM evictions_raw
            WHERE zip_code IS NOT NULL
              AND executed_date >= CURRENT_DATE - INTERVAL '365 days'
              AND eviction_type ILIKE 'R%'
            GROUP BY zip_code
            ORDER BY eviction_count DESC
            """
        )
    ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


def _aggregate_llc_acquisitions(db: Session) -> List[Tuple[str, int]]:
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
    LOAN SERVICE, FEDERAL SAVINGS, CREDIT UNION.
    """
    rows = db.execute(
        text(
            """
            SELECT p.zip_code, COUNT(*) AS llc_count
            FROM ownership_raw o
            JOIN parcels p ON o.bbl = p.bbl
            WHERE o.party_type = '2'
              AND o.party_name_normalized LIKE '%LLC%'
              AND o.doc_date >= CURRENT_DATE - INTERVAL '365 days'
              AND p.zip_code IS NOT NULL
              AND p.units_res > 0
              AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
              AND o.party_name_normalized NOT ILIKE '%LOAN SERVICING%'
              AND o.party_name_normalized NOT ILIKE '%LOAN SERVICE%'
              AND o.party_name_normalized NOT ILIKE '%FEDERAL SAVINGS%'
              AND o.party_name_normalized NOT ILIKE '%CREDIT UNION%'
            GROUP BY p.zip_code
            ORDER BY llc_count DESC
            """
        )
    ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


def _aggregate_complaints(db: Session) -> List[Tuple[str, int]]:
    """
    Return (zip_code, count) for displacement-relevant 311 complaints
    (filtered to DISPLACEMENT_COMPLAINT_TYPES at query time) in the past 365 days.
    Raw complaints_raw table retains all complaint types — filtering here only.
    Named parameter :types is passed as a list; SQLAlchemy maps it to a PostgreSQL
    array, preventing any SQL injection from the constant values.
    """
    rows = db.execute(
        text(
            """
            SELECT zip_code, COUNT(*) AS complaint_count
            FROM complaints_raw
            WHERE zip_code IS NOT NULL
              AND complaint_type = ANY(:types)
              AND created_date >= CURRENT_DATE - INTERVAL '365 days'
            GROUP BY zip_code
            ORDER BY complaint_count DESC
            """
        ),
        {"types": list(DISPLACEMENT_COMPLAINT_TYPES)},
    ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


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
# Main entry point
# ---------------------------------------------------------------------------

def compute_scores(db: Session) -> int:
    """
    Compute full six-signal composite displacement scores for all zip codes.
    Returns the number of zip codes scored and upserted.

    The 6th signal (rs_unit_loss) is stored in signal_breakdown JSONB only —
    no separate float column added to displacement_scores (JSONB is sufficient
    for the API response and score panel).
    """
    # --- Step 1: Aggregate all signals ---
    permit_rows = _aggregate_permits(db)
    eviction_rows = _aggregate_evictions(db)
    llc_rows = _aggregate_llc_acquisitions(db)
    complaint_rows = _aggregate_complaints(db)
    rs_loss_rows = _aggregate_rs_unit_loss(db)
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
        | set(rs_loss_map)
    )
    all_zips = (all_zips_raw & residential_zips) if residential_zips else all_zips_raw
    if not all_zips:
        return 0

    # --- Step 4: Compute per-unit values for each signal across all zips ---
    permit_pu: Dict[str, float] = {}
    eviction_pu: Dict[str, float] = {}
    llc_pu: Dict[str, float] = {}
    complaint_pu: Dict[str, float] = {}
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

    # --- Step 5: Normalize each signal independently to [0, 100] ---
    # Uses 5th–95th percentile range instead of absolute min–max.
    # Pure min-max collapses all scores when signals peak in different zips
    # (the absolute max is one outlier zip; everyone else scores <30).
    # Percentile normalization ensures the top 5% of zips fill the high end
    # of each signal's scale, producing a spread that fills 1–100 in practice.
    def _norm_map(pu_dict: Dict[str, float]) -> Dict[str, float]:
        vals = sorted(pu_dict.values())
        n = len(vals)
        if n < 2:
            return {z: 50.0 for z in pu_dict}
        p5  = vals[max(0, int(n * 0.05))]
        p95 = vals[min(n - 1, int(n * 0.95))]
        return {
            z: max(0.0, min(100.0, _normalize(v, p5, p95)))
            for z, v in pu_dict.items()
        }

    permit_norm = _norm_map(permit_pu)
    eviction_norm = _norm_map(eviction_pu)
    llc_norm = _norm_map(llc_pu)
    complaint_norm = _norm_map(complaint_pu)
    rs_loss_norm = _norm_map(rs_loss_pu)

    # --- Step 6: Upsert to displacement_scores ---
    now = datetime.now(timezone.utc)
    sig_updated = {
        "permits": now.isoformat(),
        "evictions": now.isoformat(),
        "llc_acquisitions": now.isoformat(),
        "assessment_spike": now.isoformat(),
        "complaint_rate": now.isoformat(),
        "rs_unit_loss": now.isoformat(),
    }

    # Track validation failures to detect systemic scoring issues
    skipped_zips: list = []

    for zip_code in sorted(all_zips):
        p_norm = permit_norm.get(zip_code, 0.0)
        e_norm = eviction_norm.get(zip_code, 0.0)
        l_norm = llc_norm.get(zip_code, 0.0)
        c_norm = complaint_norm.get(zip_code, 0.0)
        a_norm = 0.0  # assessment spike dormant
        rs_norm = rs_loss_norm.get(zip_code, 0.0)

        composite = (
            WEIGHT_LLC_ACQUISITIONS * l_norm
            + WEIGHT_PERMITS * p_norm
            + WEIGHT_COMPLAINTS * c_norm
            + WEIGHT_EVICTIONS * e_norm
            + WEIGHT_ASSESSMENT_SPIKE * a_norm
            + WEIGHT_RS_UNIT_LOSS * rs_norm
        )
        # Clamp to [1, 100] — minimum 1 so no zip ever shows "0 risk"
        score = max(1.0, min(100.0, round(composite, 1)))

        breakdown = {
            "permits": p_norm,
            "evictions": e_norm,
            "llc_acquisitions": l_norm,
            "assessment_spike": a_norm,
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
        return 0

    db.commit()

    if skipped_zips:
        logger.warning(
            "Scoring complete with %d skipped zip(s): %s",
            len(skipped_zips),
            skipped_zips[:10],  # log up to 10 skipped zips
        )

    scored_count = total_attempted - len(skipped_zips)

    # --- Step 7: Write daily snapshot to score_history ---
    # ON CONFLICT DO NOTHING — if scoring runs multiple times today, only keep first.
    skipped_set = set(skipped_zips)
    for zip_code in sorted(all_zips):
        if zip_code in skipped_set:   # don't snapshot invalid scores
            continue
        p_norm = permit_norm.get(zip_code, 0.0)
        e_norm = eviction_norm.get(zip_code, 0.0)
        l_norm = llc_norm.get(zip_code, 0.0)
        c_norm = complaint_norm.get(zip_code, 0.0)
        a_norm = 0.0
        rs_norm = rs_loss_norm.get(zip_code, 0.0)
        composite = (
            WEIGHT_LLC_ACQUISITIONS * l_norm
            + WEIGHT_PERMITS * p_norm
            + WEIGHT_COMPLAINTS * c_norm
            + WEIGHT_EVICTIONS * e_norm
            + WEIGHT_ASSESSMENT_SPIKE * a_norm
            + WEIGHT_RS_UNIT_LOSS * rs_norm
        )
        score = max(1.0, min(100.0, round(composite, 1)))
        db.execute(
            text(
                """
                INSERT INTO score_history
                    (zip_code, scored_at, composite_score, permit_intensity,
                     eviction_rate, llc_acquisition_rate, assessment_spike,
                     complaint_rate, created_at, updated_at)
                VALUES
                    (:zip_code, CURRENT_DATE, :composite_score, :permit_intensity,
                     :eviction_rate, :llc_acquisition_rate, :assessment_spike,
                     :complaint_rate, :now, :now)
                ON CONFLICT ON CONSTRAINT uq_score_history_zip_date DO NOTHING
                """
            ),
            {
                "zip_code": zip_code,
                "composite_score": score,
                "permit_intensity": p_norm,
                "eviction_rate": e_norm,
                "llc_acquisition_rate": l_norm,
                "assessment_spike": a_norm,
                "complaint_rate": c_norm,
                "now": now,
            },
        )
    db.commit()

    # --- Step 8: Propagate scores to neighborhoods.current_score ---
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

    return scored_count


# ---------------------------------------------------------------------------
# Standalone execution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from models.database import get_scraper_db

    print("Computing displacement scores from six signals (RS unit loss dormant until 2027)...")
    with get_scraper_db() as db:
        n = compute_scores(db)
    print(f"Scored {n} zip codes.")
    sys.exit(0)
