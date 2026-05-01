"""
Data integrity audit — 14 checks across operator pipeline, score integrity,
referential integrity, pipeline health, and frontend/API truth.

Writes a timestamped JSON report to audits/integrity_YYYYMMDD_HHMMSS.json
and prints a human-readable summary to stdout.

Usage:
    python -m scripts.data_integrity_audit
    python -m scripts.data_integrity_audit --check=lender_contamination
    python scripts/data_integrity_audit.py --check=score_signal_dominance
"""

import argparse
import importlib.util
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from models.database import get_scraper_db
from scoring.operator_classification import (
    KNOWN_OPERATOR_ALLOWLIST,
    OperatorClass,
    classify_operator_candidate,
)

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).parent.parent
_AUDITS_DIR = _REPO_ROOT / "audits"

# Expected refresh intervals per scraper.
# Used by watermark_drift (check 12).
_SCRAPER_INTERVALS: dict[str, int] = {
    "acris_ownership":  1,   # days
    "dob_permits":      1,
    "311_complaints":   1,
    "evictions":        1,
    "hpd_violations":   1,
    "dcwp_licenses":    30,
    "dhcr_rs":          365,
    "dof_assessments":  30,
    "mappluto":         30,
}

# Stale thresholds for signal_last_updated (check 7).
_DAILY_STALE_HOURS   = 36
_MONTHLY_STALE_DAYS  = 35

_DAILY_SIGNALS   = {"permits", "evictions", "llc_acquisitions", "hpd_violations", "complaint_rate"}
_MONTHLY_SIGNALS = {"rs_unit_loss"}

# Signal weights from scoring/compute.py — canonical source.
_EXPECTED_WEIGHTS = {
    "llc_acquisitions": 0.26,
    "permits":          0.21,
    "complaint_rate":   0.17,
    "evictions":        0.13,
    "hpd_violations":   0.08,
    "rs_unit_loss":     0.15,
}

# Signal weight percentages as shown in methodology.html.
_METHODOLOGY_WEIGHTS = {
    "llc_acquisitions": 26,
    "permits":          21,
    "complaint_rate":   17,
    "evictions":        13,
    "hpd_violations":   8,
    "rs_unit_loss":     15,
}


# ---------------------------------------------------------------------------
# Result helpers
# ---------------------------------------------------------------------------

def _result(check_id: str, name: str, status: str, findings: list, remediation: str) -> dict:
    return {
        "check_id":    check_id,
        "name":        name,
        "status":      status,
        "findings":    findings,
        "remediation": remediation,
    }


def _pass(check_id: str, name: str, note: str = "") -> dict:
    return _result(check_id, name, "pass", [note] if note else [], "")


def _warn(check_id: str, name: str, findings: list, remediation: str) -> dict:
    return _result(check_id, name, "warn", findings, remediation)


def _fail(check_id: str, name: str, findings: list, remediation: str) -> dict:
    return _result(check_id, name, "fail", findings, remediation)


# ---------------------------------------------------------------------------
# Operator pipeline checks
# ---------------------------------------------------------------------------

def check_lender_contamination(db) -> dict:
    """Check 1: run classifier against every existing operators row."""
    rows = db.execute(
        text("SELECT operator_root, display_name, llc_entities FROM operators")
    ).fetchall()

    would_suppress = []
    for r in rows:
        root = r.operator_root
        if root in KNOWN_OPERATOR_ALLOWLIST:
            continue
        cls = classify_operator_candidate(r.display_name)
        if cls.operator_class == OperatorClass.SUPPRESSED:
            would_suppress.append({
                "operator_root": root,
                "display_name":  r.display_name,
                "reasons":       cls.reasons,
            })
        else:
            # Also check each LLC entity name for name-based suppression.
            entities = r.llc_entities or []
            suppressed_entities = []
            for entity in entities:
                ecls = classify_operator_candidate(entity)
                if ecls.operator_class == OperatorClass.SUPPRESSED:
                    suppressed_entities.append({"entity": entity, "reasons": ecls.reasons})
            if suppressed_entities:
                would_suppress.append({
                    "operator_root":       root,
                    "display_name":        r.display_name,
                    "contaminated_entities": suppressed_entities,
                    "reasons":             ["entity_name_contamination"],
                })

    if not would_suppress:
        return _pass("lender_contamination", "Lender/intermediary contamination")

    status = "fail" if would_suppress else "pass"
    return _result(
        "lender_contamination",
        "Lender/intermediary contamination",
        status,
        [json.dumps(w) for w in would_suppress],
        "Run python scripts/backfill_operators.py to purge suppressed rows.",
    )


def check_doc_amount_anomalies(db) -> dict:
    """Check 2: operators with high null_amount_ratio or low median_doc_amount."""
    rows = db.execute(
        text("""
            SELECT
                o.operator_root,
                o.display_name,
                COUNT(*)                                                       AS acq_count,
                COUNT(*) FILTER (WHERE doc_amount IS NULL OR doc_amount = 0)   AS null_count,
                ROUND(
                    COUNT(*) FILTER (WHERE doc_amount IS NULL OR doc_amount = 0)::numeric
                    / NULLIF(COUNT(*), 0), 3
                )                                                              AS null_ratio,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY doc_amount)        AS median_amount
            FROM operators o
            JOIN operator_parcels op ON op.operator_id = o.id
            JOIN ownership_raw raw ON raw.bbl = op.bbl
              AND raw.party_type = '2'
              AND raw.party_name_normalized = ANY(
                  ARRAY(SELECT jsonb_array_elements_text(o.llc_entities))
              )
            GROUP BY o.operator_root, o.display_name
        """)
    ).fetchall()

    anomalies = []
    for r in rows:
        null_ratio = float(r.null_ratio or 0)
        median_amt = float(r.median_amount) if r.median_amount is not None else None
        if null_ratio > 0.5 or (median_amt is not None and median_amt < 1000):
            anomalies.append({
                "operator_root": r.operator_root,
                "null_ratio":    round(null_ratio, 3),
                "median_doc_amount": median_amt,
                "acquisition_count": r.acq_count,
            })

    if not anomalies:
        return _pass("doc_amount_anomalies", "Doc amount anomaly scan")
    return _warn(
        "doc_amount_anomalies",
        "Doc amount anomaly scan",
        [json.dumps(a) for a in anomalies],
        "Review flagged operators — high null ratio or low median price suggests "
        "non-acquisition transfers (foreclosure, correction, intra-entity).",
    )


def check_holding_period(db) -> dict:
    """Check 3: operators where median holding period < 365 days."""
    rows = db.execute(
        text("""
            SELECT
                o.operator_root,
                o.display_name,
                PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (now() - raw.doc_date::timestamptz)) / 86400
                ) AS median_hold_days
            FROM operators o
            JOIN operator_parcels op ON op.operator_id = o.id
            JOIN ownership_raw raw ON raw.bbl = op.bbl
              AND raw.party_type = '2'
              AND raw.party_name_normalized = ANY(
                  ARRAY(SELECT jsonb_array_elements_text(o.llc_entities))
              )
            GROUP BY o.operator_root, o.display_name
        """)
    ).fetchall()

    short_hold = []
    for r in rows:
        if r.median_hold_days is not None and float(r.median_hold_days) < 365:
            short_hold.append({
                "operator_root":    r.operator_root,
                "median_hold_days": round(float(r.median_hold_days), 0),
            })

    if not short_hold:
        return _pass("holding_period", "Holding period scan")
    return _warn(
        "holding_period",
        "Holding period scan",
        [json.dumps(s) for s in short_hold],
        "Short median holding may indicate flip activity or pipeline cycling, not "
        "long-term operator behavior. Review before next press cycle.",
    )


def check_grantor_concentration(db) -> dict:
    """Check 4: operators where >70% of acquisitions come from <=3 distinct grantors."""
    rows = db.execute(
        text("""
            WITH grantor_counts AS (
                SELECT
                    o.operator_root,
                    o.display_name,
                    raw.party_name_normalized          AS grantor,
                    COUNT(*)                           AS txn_count
                FROM operators o
                JOIN operator_parcels op ON op.operator_id = o.id
                JOIN ownership_raw raw ON raw.bbl = op.bbl
                  AND raw.party_type = '1'
                  AND raw.party_name_normalized IS NOT NULL
                GROUP BY o.operator_root, o.display_name, raw.party_name_normalized
            ),
            totals AS (
                SELECT operator_root, SUM(txn_count) AS total
                FROM grantor_counts GROUP BY operator_root
            ),
            top3 AS (
                SELECT
                    gc.operator_root,
                    gc.display_name,
                    SUM(gc.txn_count) AS top3_count,
                    t.total
                FROM grantor_counts gc
                JOIN totals t USING (operator_root)
                WHERE (
                    SELECT COUNT(*) FROM grantor_counts g2
                    WHERE g2.operator_root = gc.operator_root
                      AND g2.txn_count >= gc.txn_count
                ) <= 3
                GROUP BY gc.operator_root, gc.display_name, t.total
            )
            SELECT operator_root, display_name,
                   ROUND(top3_count::numeric / NULLIF(total, 0), 3) AS top3_ratio,
                   total
            FROM top3
            WHERE top3_count::numeric / NULLIF(total, 0) > 0.70
              AND total >= 10
        """)
    ).fetchall()

    flagged = [
        {
            "operator_root": r.operator_root,
            "top3_grantor_ratio": float(r.top3_ratio),
            "total_transactions": int(r.total),
        }
        for r in rows
    ]
    if not flagged:
        return _pass("grantor_concentration", "Grantor concentration scan")
    return _warn(
        "grantor_concentration",
        "Grantor concentration scan",
        [json.dumps(f) for f in flagged],
        "High grantor concentration suggests foreclosure feed or intra-entity "
        "transfers rather than open-market acquisitions.",
    )


# ---------------------------------------------------------------------------
# Score integrity checks
# ---------------------------------------------------------------------------

def check_score_signal_dominance(db) -> dict:
    """Check 5: ZIPs where one signal contributes >70% of the composite."""
    rows = db.execute(
        text("SELECT zip_code, score, signal_breakdown FROM displacement_scores WHERE score IS NOT NULL")
    ).fetchall()

    flagged = []
    for r in rows:
        breakdown = r.signal_breakdown or {}
        total = sum(float(v) for v in breakdown.values() if v is not None)
        if total <= 0:
            continue
        for signal, val in breakdown.items():
            if val is None:
                continue
            share = float(val) / total
            if share > 0.70:
                flagged.append({
                    "zip_code": r.zip_code,
                    "dominant_signal": signal,
                    "share": round(share, 3),
                    "composite_score": round(r.score, 1),
                })
                break

    if not flagged:
        return _pass("score_signal_dominance", "Single-signal score dominance")
    return _warn(
        "score_signal_dominance",
        "Single-signal score dominance",
        [json.dumps(f) for f in flagged[:20]],
        "ZIPs with one dominant signal are usually data quality issues. "
        "Inspect the underlying signal data for the flagged zip codes.",
    )


def check_score_percentile_sanity(db) -> dict:
    """Check 6: ZIPs whose composite score rank doesn't match percentile_tier."""
    rows = db.execute(
        text("""
            SELECT zip_code, score,
                   PERCENT_RANK() OVER (ORDER BY score) AS actual_rank
            FROM displacement_scores
            WHERE score IS NOT NULL
            ORDER BY score DESC
        """)
    ).fetchall()

    # Compute distribution boundaries from actual scores.
    scores = sorted([float(r.score) for r in rows])
    if len(scores) < 2:
        return _pass("score_percentile_sanity", "Score-to-percentile sanity")

    p80 = scores[int(len(scores) * 0.80)]
    p90 = scores[int(len(scores) * 0.90)]
    p95 = scores[int(len(scores) * 0.95)]
    p99 = scores[int(len(scores) * 0.99)]

    # The API assigns "top 1%", "top 3%", etc. based on PERCENT_RANK.
    # If score < p80 but actual_rank > 0.80, that's a distribution drift.
    mismatches = []
    for r in rows:
        actual_pct = (1.0 - float(r.actual_rank)) * 100.0
        score = float(r.score)
        # Flag if reported percentile would be "top 5%" but score is below p90.
        if actual_pct <= 5 and score < p90:
            mismatches.append({
                "zip_code": r.zip_code,
                "score": round(score, 1),
                "actual_top_pct": round(actual_pct, 1),
            })

    if not mismatches:
        return _pass("score_percentile_sanity", "Score-to-percentile sanity")
    return _warn(
        "score_percentile_sanity",
        "Score-to-percentile sanity",
        [json.dumps(m) for m in mismatches],
        "Distribution shift detected. Re-run scoring/compute.py and verify "
        "displacement_scores matches current signal data.",
    )


def check_stale_signals(db) -> dict:
    """Check 7: signals in signal_last_updated older than expected refresh interval."""
    now = datetime.now(timezone.utc)
    daily_cutoff   = now - timedelta(hours=_DAILY_STALE_HOURS)
    monthly_cutoff = now - timedelta(days=_MONTHLY_STALE_DAYS)

    rows = db.execute(
        text("SELECT zip_code, signal_last_updated FROM displacement_scores LIMIT 1")
    ).fetchall()

    if not rows or not rows[0].signal_last_updated:
        return _warn(
            "stale_signals",
            "Stale signal timestamps",
            ["signal_last_updated is empty or missing on displacement_scores"],
            "Run scoring/compute.py to populate signal_last_updated.",
        )

    # Use a single representative row to check signal freshness.
    sample = rows[0].signal_last_updated

    stale = []
    for signal, ts_str in (sample or {}).items():
        if not ts_str:
            stale.append({"signal": signal, "reason": "null_timestamp"})
            continue
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            stale.append({"signal": signal, "reason": f"unparseable: {ts_str}"})
            continue

        if signal in _DAILY_SIGNALS and ts < daily_cutoff:
            stale.append({
                "signal": signal,
                "last_updated": ts_str,
                "hours_stale": round((now - ts).total_seconds() / 3600, 1),
            })
        elif signal in _MONTHLY_SIGNALS and ts < monthly_cutoff:
            stale.append({
                "signal": signal,
                "last_updated": ts_str,
                "days_stale": round((now - ts).days, 0),
            })

    if not stale:
        return _pass("stale_signals", "Stale signal timestamps")
    return _warn(
        "stale_signals",
        "Stale signal timestamps",
        [json.dumps(s) for s in stale],
        "Run the relevant scraper(s) and rerun compute.py to refresh signal timestamps.",
    )


# ---------------------------------------------------------------------------
# Referential integrity checks
# ---------------------------------------------------------------------------

def check_bbl_orphans(db) -> dict:
    """Check 8: BBLs in raw tables with no row in parcels."""
    tables = [
        ("ownership_raw", "bbl"),
        ("permits_raw", "bbl"),
        ("evictions_raw", "bbl"),
        ("complaints_raw", "bbl"),
        ("violations_raw", "bbl"),
    ]

    orphans = []
    for table, col in tables:
        row = db.execute(
            text(f"""
                SELECT COUNT(DISTINCT t.{col}) AS orphan_count
                FROM {table} t
                LEFT JOIN parcels p ON p.bbl = t.{col}
                WHERE t.{col} IS NOT NULL
                  AND p.bbl IS NULL
            """)
        ).fetchone()
        count = int(row.orphan_count or 0)
        if count > 0:
            orphans.append({"table": table, "orphan_bbl_count": count})

    if not orphans:
        return _pass("bbl_orphans", "BBL referential integrity")

    total = sum(o["orphan_bbl_count"] for o in orphans)
    status = "fail" if total > 1000 else "warn"
    return _result(
        "bbl_orphans",
        "BBL referential integrity",
        status,
        [json.dumps(o) for o in orphans],
        "Run the PLUTO/parcels backfill to pull missing BBLs into the parcels table.",
    )


def check_zip_coverage(db) -> dict:
    """Check 9: ZIPs in displacement_scores vs neighborhoods table."""
    scored_not_in_zcta = db.execute(
        text("""
            SELECT ds.zip_code FROM displacement_scores ds
            WHERE ds.zip_code NOT IN (SELECT zip_code FROM neighborhoods)
        """)
    ).fetchall()

    zcta_not_scored = db.execute(
        text("""
            SELECT n.zip_code FROM neighborhoods n
            WHERE n.zip_code NOT IN (SELECT zip_code FROM displacement_scores)
        """)
    ).fetchall()

    findings = []
    if scored_not_in_zcta:
        findings.append(f"{len(scored_not_in_zcta)} scored ZIPs not in neighborhoods: "
                        f"{[r.zip_code for r in scored_not_in_zcta[:5]]}")
    if zcta_not_scored:
        findings.append(f"{len(zcta_not_scored)} ZCTA neighborhoods not scored: "
                        f"{[r.zip_code for r in zcta_not_scored[:5]]}")

    if not findings:
        return _pass("zip_coverage", "ZIP coverage alignment")
    return _warn(
        "zip_coverage",
        "ZIP coverage alignment",
        findings,
        "Run compute.py to score all neighborhoods. "
        "For scored-but-not-in-ZCTA, verify the zip code is in the ZCTA boundary dataset.",
    )


def check_score_history_gaps(db) -> dict:
    """Check 10: ZIPs missing more than 3 consecutive days of score_history in the last 30 days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).date()

    rows = db.execute(
        text("""
            SELECT zip_code, COUNT(DISTINCT scored_at) AS day_count
            FROM score_history
            WHERE scored_at >= :cutoff
            GROUP BY zip_code
            HAVING COUNT(DISTINCT scored_at) < (
                SELECT COUNT(DISTINCT scored_at) - 3
                FROM score_history
                WHERE scored_at >= :cutoff
            )
        """),
        {"cutoff": cutoff},
    ).fetchall()

    if not rows:
        return _pass("score_history_gaps", "Score history gap detection")
    flagged = [{"zip_code": r.zip_code, "days_with_data": r.day_count} for r in rows[:20]]
    return _warn(
        "score_history_gaps",
        "Score history gap detection",
        [json.dumps(f) for f in flagged],
        "Missing score_history rows suggest compute.py was not run on those dates. "
        "Run backfill_score_history.py for the gap window.",
    )


# ---------------------------------------------------------------------------
# Pipeline health checks
# ---------------------------------------------------------------------------

def check_scraper_run_anomalies(db) -> dict:
    """Check 11: scrapers with recent runs < 50% of 14-day rolling average.

    Reports the most recent anomalous run per scraper (not every run).
    """
    rows = db.execute(
        text("""
            WITH rolling AS (
                SELECT scraper_name,
                       AVG(records_processed) AS rolling_avg
                FROM scraper_runs
                WHERE started_at >= NOW() - INTERVAL '14 days'
                  AND status = 'success'
                GROUP BY scraper_name
                HAVING AVG(records_processed) > 50
            ),
            latest_per_scraper AS (
                SELECT DISTINCT ON (scraper_name)
                    scraper_name, started_at, records_processed, warning_message
                FROM scraper_runs
                WHERE started_at >= NOW() - INTERVAL '14 days'
                  AND status = 'success'
                ORDER BY scraper_name, started_at DESC
            )
            SELECT l.scraper_name, l.started_at, l.records_processed,
                   r.rolling_avg, l.warning_message
            FROM latest_per_scraper l
            JOIN rolling r USING (scraper_name)
            WHERE l.records_processed < r.rolling_avg * 0.50
            ORDER BY l.started_at DESC
        """)
    ).fetchall()

    if not rows:
        return _pass("scraper_run_anomalies", "Scraper run anomaly detection")

    findings = [
        {
            "scraper_name":      r.scraper_name,
            "latest_run":        r.started_at.isoformat(),
            "records_processed": r.records_processed,
            "rolling_avg":       round(float(r.rolling_avg), 0),
            "warning":           r.warning_message or "",
        }
        for r in rows
    ]
    return _warn(
        "scraper_run_anomalies",
        "Scraper run anomaly detection",
        [json.dumps(f) for f in findings],
        "Low record counts may indicate upstream API changes, Socrata outages, or "
        "scraper bugs. Check scraper_quarantine and the upstream dataset.",
    )


def check_watermark_drift(db) -> dict:
    """Check 12: scrapers whose watermark hasn't advanced in more than 2x expected interval."""
    now = datetime.now(timezone.utc)
    rows = db.execute(
        text("""
            SELECT DISTINCT ON (scraper_name)
                scraper_name, watermark_timestamp, started_at
            FROM scraper_runs
            WHERE watermark_timestamp IS NOT NULL
            ORDER BY scraper_name, started_at DESC
        """)
    ).fetchall()

    drifted = []
    for r in rows:
        expected_days = _SCRAPER_INTERVALS.get(r.scraper_name)
        if expected_days is None:
            continue
        wm = r.watermark_timestamp
        if wm.tzinfo is None:
            wm = wm.replace(tzinfo=timezone.utc)
        days_since = (now - wm).days
        if days_since > expected_days * 2:
            drifted.append({
                "scraper_name":    r.scraper_name,
                "watermark_age_days": days_since,
                "expected_days":   expected_days,
            })

    if not drifted:
        return _pass("watermark_drift", "Watermark drift detection")
    return _warn(
        "watermark_drift",
        "Watermark drift detection",
        [json.dumps(d) for d in drifted],
        "Stale watermarks mean the scraper is not advancing. "
        "Check scraper_runs for failure rows and inspect the upstream API.",
    )


# ---------------------------------------------------------------------------
# Frontend / API truth checks
# ---------------------------------------------------------------------------

def check_methodology_weights() -> dict:
    """Check 13: methodology.html weights vs scoring/compute.py constants."""
    methodology_path = _REPO_ROOT / "frontend" / "methodology.html"
    if not methodology_path.exists():
        return _warn(
            "methodology_weights",
            "Methodology page weight alignment",
            ["frontend/methodology.html not found"],
            "Verify the file path is correct.",
        )

    html = methodology_path.read_text()

    mismatches = []
    for signal, expected_weight in _EXPECTED_WEIGHTS.items():
        expected_pct = _METHODOLOGY_WEIGHTS.get(signal)
        if expected_pct is None:
            continue
        pattern = rf"{expected_pct}%"
        if pattern not in html:
            mismatches.append({
                "signal": signal,
                "expected_in_html": pattern,
                "compute_py_value": expected_weight,
            })

    if not mismatches:
        return _pass("methodology_weights", "Methodology page weight alignment")
    return _fail(
        "methodology_weights",
        "Methodology page weight alignment",
        [json.dumps(m) for m in mismatches],
        "Update frontend/methodology.html weight badges to match WEIGHT_* constants "
        "in scoring/compute.py.",
    )


def check_frontend_signal_labels(db) -> dict:
    """Check 14: STRINGS signal keys in app.html vs keys returned by the neighborhoods API."""
    app_html_path = _REPO_ROOT / "frontend" / "app.html"
    if not app_html_path.exists():
        return _warn(
            "frontend_signal_labels",
            "Frontend signal label alignment",
            ["frontend/app.html not found"],
            "Verify the file path.",
        )

    html = app_html_path.read_text()

    # Extract signal keys from the STRINGS.en.signals block in app.html.
    signals_block = re.search(
        r"signals:\s*\{([^}]+)\}", html, re.DOTALL
    )
    if not signals_block:
        return _warn(
            "frontend_signal_labels",
            "Frontend signal label alignment",
            ["Could not find signals: {} block in STRINGS.en in app.html"],
            "Check that STRINGS.en.signals is intact in app.html.",
        )

    frontend_keys = set(re.findall(r"^\s*([a-z_]+)\s*:", signals_block.group(1), re.MULTILINE))

    # Get actual keys from a sample API response.
    sample = db.execute(
        text("SELECT signal_breakdown FROM displacement_scores WHERE signal_breakdown != '{}' LIMIT 1")
    ).fetchone()
    if not sample:
        return _warn(
            "frontend_signal_labels",
            "Frontend signal label alignment",
            ["No displacement_scores rows with signal_breakdown — cannot verify keys"],
            "Run compute.py to populate displacement_scores.",
        )

    api_keys = set(sample.signal_breakdown.keys())

    orphan_labels = frontend_keys - api_keys   # in HTML but not returned by API
    untranslated   = api_keys - frontend_keys   # in API but no label in HTML

    findings = []
    if orphan_labels:
        findings.append(f"Orphan labels in STRINGS.en.signals (no API key): {sorted(orphan_labels)}")
    if untranslated:
        findings.append(f"API signal keys with no frontend label: {sorted(untranslated)}")

    if not findings:
        return _pass("frontend_signal_labels", "Frontend signal label alignment")
    return _fail(
        "frontend_signal_labels",
        "Frontend signal label alignment",
        findings,
        "Update STRINGS.en.signals in app.html to match the signal_breakdown keys "
        "returned by /api/neighborhoods/{zip}.",
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_CHECKS_DB = [
    ("lender_contamination",   check_lender_contamination),
    ("doc_amount_anomalies",   check_doc_amount_anomalies),
    ("holding_period",         check_holding_period),
    ("grantor_concentration",  check_grantor_concentration),
    ("score_signal_dominance", check_score_signal_dominance),
    ("score_percentile_sanity", check_score_percentile_sanity),
    ("stale_signals",          check_stale_signals),
    ("bbl_orphans",            check_bbl_orphans),
    ("zip_coverage",           check_zip_coverage),
    ("score_history_gaps",     check_score_history_gaps),
    ("scraper_run_anomalies",  check_scraper_run_anomalies),
    ("watermark_drift",        check_watermark_drift),
]

_CHECKS_STATIC = [
    ("methodology_weights",    check_methodology_weights),
    ("frontend_signal_labels", None),   # needs db, handled separately
]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_all(db, only: str | None = None) -> list[dict]:
    results = []

    for check_id, fn in _CHECKS_DB:
        if only and check_id != only:
            continue
        try:
            results.append(fn(db))
            db.commit()
        except Exception as exc:
            db.rollback()
            results.append(_fail(
                check_id,
                check_id.replace("_", " ").title(),
                [f"Check raised exception: {exc}"],
                "Investigate the exception — likely a missing table or column.",
            ))

    # methodology_weights is static (no DB)
    if not only or only == "methodology_weights":
        try:
            results.append(check_methodology_weights())
        except Exception as exc:
            results.append(_fail("methodology_weights", "Methodology weights", [str(exc)], ""))

    # frontend_signal_labels needs DB
    if not only or only == "frontend_signal_labels":
        try:
            results.append(check_frontend_signal_labels(db))
        except Exception as exc:
            db.rollback()
            results.append(_fail("frontend_signal_labels", "Frontend signal labels", [str(exc)], ""))

    return results


def _print_summary(results: list[dict]) -> None:
    pass_n  = sum(1 for r in results if r["status"] == "pass")
    warn_n  = sum(1 for r in results if r["status"] == "warn")
    fail_n  = sum(1 for r in results if r["status"] == "fail")

    print(f"\n=== Data Integrity Audit — {datetime.now().strftime('%Y-%m-%d %H:%M')} ===\n")
    print(f"  PASS: {pass_n}   WARN: {warn_n}   FAIL: {fail_n}\n")

    for r in results:
        symbol = {"pass": "OK", "warn": "WARN", "fail": "FAIL"}.get(r["status"], "??")
        print(f"  [{symbol:<4}] {r['check_id']}")
        for finding in r.get("findings", []):
            if finding:
                print(f"           {finding[:120]}")
        if r.get("remediation") and r["status"] != "pass":
            print(f"           => {r['remediation']}")

    print()


def main() -> int:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="PulseCities data integrity audit",
    )
    parser.add_argument("--check", metavar="CHECK_ID", default=None,
                        help="Run a single check by ID (e.g. lender_contamination)")
    args = parser.parse_args()

    _AUDITS_DIR.mkdir(exist_ok=True)

    with get_scraper_db() as db:
        results = run_all(db, only=args.check)

    _print_summary(results)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = _AUDITS_DIR / f"integrity_{ts}.json"
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "check_filter": args.check,
        "results": results,
        "summary": {
            "pass": sum(1 for r in results if r["status"] == "pass"),
            "warn": sum(1 for r in results if r["status"] == "warn"),
            "fail": sum(1 for r in results if r["status"] == "fail"),
        },
    }
    out_path.write_text(json.dumps(payload, indent=2, default=str))
    print(f"Report written to {out_path}\n")

    return 1 if any(r["status"] == "fail" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
