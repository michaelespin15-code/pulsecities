"""
Weekly digest mailer — runs every Sunday at 9:00 AM UTC via cron.

Sends per-ZIP displacement updates only when meaningful public-record changes
occurred during the past 7 days. Quiet weeks are skipped; reasons are logged.
"""

import argparse
import logging
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import resend
from sqlalchemy import text

from config.logging_config import configure_logging
from config.schedule import DIGEST_SEND_DAY
from models.database import SessionLocal

configure_logging()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

SCORE_DELTA_MIN  = 3.0   # absolute point change over 7 days
BASELINE_RATIO   = 0.50  # fraction above 8-week average that triggers send

# Absolute weekly event counts that always trigger a send
HPD_ABS      = 5
EVICTION_ABS = 2
PERMIT_ABS   = 3
LLC_ABS      = 1
COMPLAINT_ABS = 8

_TIER_ORDER = ["Low", "Watch", "Elevated", "High"]

SIGNAL_LABELS = {
    "permit_intensity":     "Permit Filings",
    "llc_acquisition_rate": "LLC Acquisitions",
    "eviction_rate":        "Evictions",
    "complaint_rate":       "Complaints",
    "rs_unit_loss":         "RS Unit Loss",
    "hpd_violations":       "HPD Violations",
}

_RISK_DISPLAY = [
    (85, "CRITICAL",      "#ef4444"),
    (67, "HIGH RISK",     "#f97316"),
    (34, "MODERATE RISK", "#eab308"),
    ( 0, "LOW RISK",      "#22c55e"),
]


def _display_risk(score: float) -> tuple[str, str]:
    for threshold, label, color in _RISK_DISPLAY:
        if score >= threshold:
            return label, color
    return "LOW RISK", "#22c55e"


def _score_color(score: float) -> str:
    if score >= 85: return "#ef4444"
    if score >= 67: return "#f97316"
    if score >= 34: return "#eab308"
    return "#22c55e"


def _send_tier(score: float) -> str:
    if score < 25: return "Low"
    if score < 50: return "Watch"
    if score < 75: return "Elevated"
    return "High"


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------

def load_active_subscriptions(db) -> list[dict]:
    """Return confirmed ZIP-based subscribers only."""
    rows = db.execute(text("""
        SELECT email, zip_code, unsubscribe_token
        FROM subscribers
        WHERE confirmed = true AND is_citywide = false AND zip_code IS NOT NULL
        ORDER BY zip_code, email
    """)).fetchall()
    return [{"email": r[0], "zip_code": r[1], "unsubscribe_token": r[2]} for r in rows]


def load_citywide_subscriptions(db) -> list[dict]:
    """Return confirmed citywide subscribers."""
    rows = db.execute(text("""
        SELECT email, unsubscribe_token
        FROM subscribers
        WHERE confirmed = true AND is_citywide = true
        ORDER BY email
    """)).fetchall()
    return [{"email": r[0], "unsubscribe_token": r[1]} for r in rows]


def build_weekly_zip_summaries(db, zip_codes: set[str]) -> dict[str, dict]:
    """
    Batch-fetch all per-ZIP digest data for a set of zip codes.
    One query per signal type rather than one expensive query per subscriber.
    Returns a dict keyed by zip_code.
    """
    if not zip_codes:
        return {}

    zips = list(zip_codes)
    today         = date.today()
    week_ago      = today - timedelta(days=7)
    baseline_start = today - timedelta(days=64)   # 9 weeks back
    baseline_end   = week_ago                      # exclude current window

    # -- Score history: need current + one prior-week snapshot ---------------
    score_rows = db.execute(text("""
        SELECT zip_code, scored_at, composite_score,
               permit_intensity, eviction_rate, llc_acquisition_rate,
               complaint_rate, rs_unit_loss, hpd_violations
        FROM score_history
        WHERE zip_code = ANY(:zips)
          AND scored_at >= :cutoff
        ORDER BY zip_code, scored_at DESC
    """), {"zips": zips, "cutoff": week_ago - timedelta(days=14)}).fetchall()

    score_by_zip: dict[str, list] = {}
    for row in score_rows:
        score_by_zip.setdefault(row[0], []).append(row)

    # -- Neighborhood names --------------------------------------------------
    name_map = dict(db.execute(text(
        "SELECT zip_code, name FROM neighborhoods WHERE zip_code = ANY(:zips)"
    ), {"zips": zips}).fetchall())

    # -- Current-week event counts (batch, one query per signal) -------------
    hpd_counts = dict(db.execute(text("""
        SELECT zip_code, COUNT(*) FROM violations_raw
        WHERE zip_code = ANY(:zips)
          AND inspection_date >= :cutoff
          AND violation_class IN ('B', 'C')
        GROUP BY zip_code
    """), {"zips": zips, "cutoff": week_ago}).fetchall())

    eviction_counts = dict(db.execute(text("""
        SELECT zip_code, COUNT(*) FROM evictions_raw
        WHERE zip_code = ANY(:zips)
          AND executed_date >= :cutoff
          AND eviction_type ILIKE 'R%'
        GROUP BY zip_code
    """), {"zips": zips, "cutoff": week_ago}).fetchall())

    permit_counts = dict(db.execute(text("""
        SELECT p.zip_code, COUNT(*) FROM permits_raw pr
        JOIN parcels p ON pr.bbl = p.bbl
        WHERE p.zip_code = ANY(:zips)
          AND pr.filing_date >= :cutoff
          AND pr.permit_type = 'AL'
          AND p.units_res >= 3
        GROUP BY p.zip_code
    """), {"zips": zips, "cutoff": week_ago}).fetchall())

    llc_counts = dict(db.execute(text("""
        SELECT p.zip_code, COUNT(DISTINCT o.bbl) FROM ownership_raw o
        JOIN parcels p ON o.bbl = p.bbl
        WHERE p.zip_code = ANY(:zips)
          AND o.doc_date >= :cutoff
          AND o.party_type = '2'
          AND o.doc_type IN ('DEED','DEEDP','ASST')
          AND o.party_name_normalized LIKE '%LLC%'
          AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
          AND o.party_name_normalized NOT ILIKE '%LENDING%'
          AND o.party_name_normalized NOT ILIKE '%FINANCIAL %'
        GROUP BY p.zip_code
    """), {"zips": zips, "cutoff": week_ago}).fetchall())

    complaint_counts = dict(db.execute(text("""
        SELECT zip_code, COUNT(*) FROM complaints_raw
        WHERE zip_code = ANY(:zips)
          AND created_date >= :cutoff
          AND (
              complaint_type ILIKE '%HEAT%'
           OR complaint_type ILIKE '%PLUMBING%'
           OR complaint_type ILIKE '%PAINT%'
           OR complaint_type ILIKE '%MOLD%'
           OR complaint_type ILIKE '%RODENT%'
           OR complaint_type ILIKE '%ELEVATOR%'
          )
        GROUP BY zip_code
    """), {"zips": zips, "cutoff": week_ago}).fetchall())

    # -- 8-week baselines (average weekly count over prior 8 weeks) ----------
    hpd_baselines: dict[str, float] = {}
    eviction_baselines: dict[str, float] = {}
    permit_baselines: dict[str, float] = {}
    complaint_baselines: dict[str, float] = {}

    if baseline_start < baseline_end:
        hpd_baselines = dict(db.execute(text("""
            SELECT zip_code, AVG(weekly_cnt) FROM (
                SELECT zip_code,
                       date_trunc('week', inspection_date) AS wk,
                       COUNT(*) AS weekly_cnt
                FROM violations_raw
                WHERE zip_code = ANY(:zips)
                  AND inspection_date >= :start AND inspection_date < :end
                  AND violation_class IN ('B', 'C')
                GROUP BY zip_code, wk
            ) sub GROUP BY zip_code
        """), {"zips": zips, "start": baseline_start, "end": baseline_end}).fetchall())

        eviction_baselines = dict(db.execute(text("""
            SELECT zip_code, AVG(weekly_cnt) FROM (
                SELECT zip_code,
                       date_trunc('week', executed_date) AS wk,
                       COUNT(*) AS weekly_cnt
                FROM evictions_raw
                WHERE zip_code = ANY(:zips)
                  AND executed_date >= :start AND executed_date < :end
                  AND eviction_type ILIKE 'R%'
                GROUP BY zip_code, wk
            ) sub GROUP BY zip_code
        """), {"zips": zips, "start": baseline_start, "end": baseline_end}).fetchall())

        permit_baselines = dict(db.execute(text("""
            SELECT zip_code, AVG(weekly_cnt) FROM (
                SELECT p.zip_code,
                       date_trunc('week', pr.filing_date) AS wk,
                       COUNT(*) AS weekly_cnt
                FROM permits_raw pr JOIN parcels p ON pr.bbl = p.bbl
                WHERE p.zip_code = ANY(:zips)
                  AND pr.filing_date >= :start AND pr.filing_date < :end
                  AND pr.permit_type = 'AL' AND p.units_res >= 3
                GROUP BY p.zip_code, wk
            ) sub GROUP BY zip_code
        """), {"zips": zips, "start": baseline_start, "end": baseline_end}).fetchall())

        complaint_baselines = dict(db.execute(text("""
            SELECT zip_code, AVG(weekly_cnt) FROM (
                SELECT zip_code,
                       date_trunc('week', created_date) AS wk,
                       COUNT(*) AS weekly_cnt
                FROM complaints_raw
                WHERE zip_code = ANY(:zips)
                  AND created_date >= :start AND created_date < :end
                  AND (
                      complaint_type ILIKE '%HEAT%'
                   OR complaint_type ILIKE '%PLUMBING%'
                   OR complaint_type ILIKE '%PAINT%'
                   OR complaint_type ILIKE '%MOLD%'
                   OR complaint_type ILIKE '%RODENT%'
                   OR complaint_type ILIKE '%ELEVATOR%'
                  )
                GROUP BY zip_code, wk
            ) sub GROUP BY zip_code
        """), {"zips": zips, "start": baseline_start, "end": baseline_end}).fetchall())

    # -- Assemble per-ZIP summary dicts -------------------------------------
    summaries: dict[str, dict] = {}
    for zip_code in zip_codes:
        rows = score_by_zip.get(zip_code, [])
        if not rows:
            continue

        current = rows[0]
        prior   = next(
            (r for r in rows if (current[1] - r[1]).days >= 6),
            None,
        )

        score_now  = float(current[2]) if current[2] is not None else 0.0
        score_prev = float(prior[2])   if prior and prior[2] is not None else score_now
        delta      = round(score_now - score_prev, 1)

        tier_now      = _send_tier(score_now)
        tier_prev     = _send_tier(score_prev)
        tier_increased = _TIER_ORDER.index(tier_now) > _TIER_ORDER.index(tier_prev)

        signal_map = {
            "permit_intensity":     float(current[3] or 0),
            "eviction_rate":        float(current[4] or 0),
            "llc_acquisition_rate": float(current[5] or 0),
            "complaint_rate":       float(current[6] or 0),
            "rs_unit_loss":         float(current[7] or 0),
            "hpd_violations":       float(current[8] or 0),
        }
        elevated = sorted(
            [(k, v) for k, v in signal_map.items() if v > 20],
            key=lambda x: -x[1],
        )

        raw_name = name_map.get(zip_code)
        name = raw_name if raw_name and raw_name != zip_code else zip_code

        summaries[zip_code] = {
            "zip":             zip_code,
            "name":            name,
            "score_now":       score_now,
            "score_prev":      score_prev,
            "delta":           delta,
            "tier_now":        tier_now,
            "tier_prev":       tier_prev,
            "tier_increased":  tier_increased,
            "elevated":        elevated,
            "hpd_count":       int(hpd_counts.get(zip_code, 0)),
            "eviction_count":  int(eviction_counts.get(zip_code, 0)),
            "permit_count":    int(permit_counts.get(zip_code, 0)),
            "llc_count":       int(llc_counts.get(zip_code, 0)),
            "complaint_count": int(complaint_counts.get(zip_code, 0)),
            "hpd_avg":      float(hpd_baselines[zip_code])       if zip_code in hpd_baselines      else None,
            "eviction_avg": float(eviction_baselines[zip_code])   if zip_code in eviction_baselines  else None,
            "permit_avg":   float(permit_baselines[zip_code])     if zip_code in permit_baselines    else None,
            "complaint_avg":float(complaint_baselines[zip_code])  if zip_code in complaint_baselines else None,
        }

    return summaries


def _fetch_event_detail(db, zip_code: str) -> dict:
    """Address-level event rows for the email body — only called for ZIPs that pass threshold."""
    cutoff = date.today() - timedelta(days=7)

    llc_rows = db.execute(text("""
        SELECT DISTINCT ON (o.bbl)
               par.address, o.party_name_normalized, o.doc_date
        FROM ownership_raw o
        JOIN parcels par ON o.bbl = par.bbl
        JOIN parcels p   ON o.bbl = p.bbl
        WHERE p.zip_code = :zip
          AND o.doc_date >= :cutoff
          AND o.party_type = '2'
          AND o.doc_type IN ('DEED','DEEDP','ASST')
          AND o.party_name_normalized LIKE '%LLC%'
          AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
          AND o.party_name_normalized NOT ILIKE '%LENDING%'
          AND o.party_name_normalized NOT ILIKE '%FINANCIAL %'
        ORDER BY o.bbl, o.doc_date DESC
        LIMIT 5
    """), {"zip": zip_code, "cutoff": cutoff}).fetchall()

    eviction_rows = db.execute(text("""
        SELECT address, executed_date FROM evictions_raw
        WHERE zip_code = :zip
          AND executed_date >= :cutoff
          AND eviction_type ILIKE 'R%'
        ORDER BY executed_date DESC
        LIMIT 5
    """), {"zip": zip_code, "cutoff": cutoff}).fetchall()

    permit_rows = db.execute(text("""
        SELECT pr.address, pr.filing_date, pr.work_type
        FROM permits_raw pr
        JOIN parcels p ON pr.bbl = p.bbl
        WHERE p.zip_code = :zip
          AND pr.filing_date >= :cutoff
          AND pr.permit_type = 'AL'
          AND p.units_res >= 3
        ORDER BY pr.filing_date DESC
        LIMIT 5
    """), {"zip": zip_code, "cutoff": cutoff}).fetchall()

    hpd_rows = db.execute(text("""
        SELECT address, inspection_date, violation_class, description
        FROM violations_raw
        WHERE zip_code = :zip
          AND inspection_date >= :cutoff
          AND violation_class IN ('B', 'C')
        ORDER BY inspection_date DESC
        LIMIT 5
    """), {"zip": zip_code, "cutoff": cutoff}).fetchall()

    return {
        "llc_rows":      llc_rows,
        "eviction_rows": eviction_rows,
        "permit_rows":   permit_rows,
        "hpd_rows":      hpd_rows,
    }


# ---------------------------------------------------------------------------
# Threshold logic
# ---------------------------------------------------------------------------

def is_meaningful_zip_update(summary: dict) -> tuple[bool, list[str]]:
    """
    Return (should_send, reasons).

    reasons is a list of plain-English strings describing what moved.
    Empty reasons means no meaningful change — skip the email.
    """
    reasons: list[str] = []

    # A. Score movement
    if abs(summary["delta"]) >= SCORE_DELTA_MIN:
        reasons.append(f"score moved {summary['delta']:+.1f} points this week")

    # B. Tier increase
    if summary["tier_increased"]:
        reasons.append(
            f"risk tier increased from {summary['tier_prev']} to {summary['tier_now']}"
        )

    # C. Signal movement — absolute threshold first, then vs. 8-week baseline
    hpd      = summary["hpd_count"]
    hpd_avg  = summary.get("hpd_avg")
    if hpd >= HPD_ABS:
        reasons.append(f"{hpd} class B/C HPD violations recorded")
    elif hpd_avg and hpd > hpd_avg * (1 + BASELINE_RATIO):
        reasons.append(f"{hpd} HPD violations ({int(hpd / hpd_avg * 100)}% of 8-week average)")

    evictions     = summary["eviction_count"]
    eviction_avg  = summary.get("eviction_avg")
    if evictions >= EVICTION_ABS:
        reasons.append(f"{evictions} residential eviction filing{'s' if evictions > 1 else ''}")
    elif eviction_avg and evictions > eviction_avg * (1 + BASELINE_RATIO):
        reasons.append(f"{evictions} eviction filings ({int(evictions / eviction_avg * 100)}% of 8-week average)")

    permits    = summary["permit_count"]
    permit_avg = summary.get("permit_avg")
    if permits >= PERMIT_ABS:
        reasons.append(f"{permits} alteration permit{'s' if permits > 1 else ''} filed")
    elif permit_avg and permits > permit_avg * (1 + BASELINE_RATIO):
        reasons.append(f"{permits} permits ({int(permits / permit_avg * 100)}% of 8-week average)")

    llc = summary["llc_count"]
    if llc >= LLC_ABS:
        reasons.append(f"{llc} LLC-linked acquisition{'s' if llc > 1 else ''} recorded")

    complaints    = summary["complaint_count"]
    complaint_avg = summary.get("complaint_avg")
    if complaints >= COMPLAINT_ABS:
        reasons.append(f"{complaints} housing complaints filed")
    elif complaint_avg and complaints > complaint_avg * (1 + BASELINE_RATIO):
        reasons.append(f"{complaints} housing complaints ({int(complaints / complaint_avg * 100)}% of 8-week average)")

    return bool(reasons), reasons


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _area_label(zip_code: str, name: str) -> str:
    return f"{name} / {zip_code}" if (name and name != zip_code) else zip_code


def _driver_sentence(reasons: list[str], name: str) -> str:
    if not reasons:
        return ""
    top = reasons[:2]
    if len(top) == 1:
        return f"Public records show {top[0].lower()} in {name}."
    return f"Public records show {top[0].lower()} and {top[1].lower()} in {name}."


def _delta_text(delta: float) -> str:
    if delta >= SCORE_DELTA_MIN:  return f"+{delta:.1f} this week"
    if delta <= -SCORE_DELTA_MIN: return f"{delta:.1f} this week"
    return "Stable this week"


def _delta_color(delta: float) -> str:
    if delta >= SCORE_DELTA_MIN:  return "#ef4444"
    if delta <= -SCORE_DELTA_MIN: return "#22c55e"
    return "#94a3b8"


def _bullet_html(reasons: list[str]) -> str:
    items = "".join(
        f'<li style="margin:0 0 6px;font-size:13px;color:#cbd5e1;">{r.capitalize()}</li>'
        for r in reasons[:5]
    )
    return f'<ul style="margin:0;padding:0 0 0 18px;list-style:disc;">{items}</ul>'


def _signal_bars_html(elevated: list[tuple[str, float]]) -> str:
    if not elevated:
        return (
            '<p style="margin:0;font-size:12px;color:rgba(148,163,184,0.4);'
            'font-style:italic;">No signals above baseline.</p>'
        )
    rows = ""
    for key, val in elevated[:5]:
        label = SIGNAL_LABELS.get(key, key)
        color = _score_color(val)
        pct   = min(100, int(val))
        rows += (
            f'<tr style="vertical-align:middle;">'
            f'<td style="font-size:11px;color:#94a3b8;padding:4px 0;width:130px;">{label}</td>'
            f'<td style="padding:4px 8px;">'
            f'<div style="background:rgba(148,163,184,0.1);border-radius:2px;height:4px;width:100%;">'
            f'<div style="background:{color};height:4px;border-radius:2px;width:{pct}%;"></div>'
            f'</div></td>'
            f'<td style="font-size:12px;color:{color};font-family:\'JetBrains Mono\',monospace;'
            f'text-align:right;padding:4px 0;white-space:nowrap;">{val:.0f}</td>'
            f'</tr>'
        )
    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'


def _events_section_html(event_detail: dict) -> str:
    llc_rows      = event_detail.get("llc_rows", [])
    eviction_rows = event_detail.get("eviction_rows", [])
    permit_rows   = event_detail.get("permit_rows", [])
    hpd_rows      = event_detail.get("hpd_rows", [])

    if not any([llc_rows, eviction_rows, permit_rows, hpd_rows]):
        return ""

    def _addr_table(rows, date_idx, label_fn):
        html = ""
        for row in rows:
            addr  = str(row[0] or "").title()
            dt    = row[date_idx]
            label = label_fn(row)
            html += (
                f'<tr>'
                f'<td style="padding:5px 0;font-size:12px;color:#cbd5e1;'
                f'font-family:\'JetBrains Mono\',monospace;">{addr}</td>'
                f'<td style="padding:5px 0 5px 16px;font-size:11px;color:#94a3b8;white-space:nowrap;">{dt}</td>'
                f'<td style="padding:5px 0 5px 16px;font-size:11px;color:#94a3b8;white-space:nowrap;">{label}</td>'
                f'</tr>'
            )
        return f'<table width="100%" cellpadding="0" cellspacing="0">{html}</table>'

    sections = ""
    if llc_rows:
        sections += (
            '<tr><td style="padding-bottom:16px;">'
            '<span style="font-size:10px;color:#f59e0b;font-weight:600;'
            'text-transform:uppercase;letter-spacing:0.08em;">LLC Acquisitions</span>'
            '<div style="margin-top:8px;">'
            + _addr_table(llc_rows, 2, lambda r: str(r[1]).title().replace(" Llc", " LLC"))
            + '</div></td></tr>'
        )
    if eviction_rows:
        sections += (
            '<tr><td style="padding-bottom:16px;">'
            '<span style="font-size:10px;color:#ef4444;font-weight:600;'
            'text-transform:uppercase;letter-spacing:0.08em;">Eviction Filings</span>'
            '<div style="margin-top:8px;">'
            + _addr_table(eviction_rows, 1, lambda _: "Residential eviction")
            + '</div></td></tr>'
        )
    if permit_rows:
        sections += (
            '<tr><td style="padding-bottom:16px;">'
            '<span style="font-size:10px;color:#38bdf8;font-weight:600;'
            'text-transform:uppercase;letter-spacing:0.08em;">Permit Filings</span>'
            '<div style="margin-top:8px;">'
            + _addr_table(permit_rows, 1, lambda r: f"Alteration{(' ' + str(r[2])) if r[2] else ''}")
            + '</div></td></tr>'
        )
    if hpd_rows:
        sections += (
            '<tr><td>'
            '<span style="font-size:10px;color:#a78bfa;font-weight:600;'
            'text-transform:uppercase;letter-spacing:0.08em;">HPD Violations</span>'
            '<div style="margin-top:8px;">'
            + _addr_table(hpd_rows, 1, lambda r: f"Class {r[2] or '?'}")
            + '</div></td></tr>'
        )

    return (
        '<tr><td style="padding-top:24px;">'
        '<table width="100%" cellpadding="0" cellspacing="0" '
        'style="background:#0f172a;border-radius:8px;padding:20px;border:1px solid rgba(148,163,184,0.08);">'
        '<tr><td style="padding-bottom:16px;">'
        '<span style="font-size:10px;font-weight:600;color:rgba(148,163,184,0.5);'
        'text-transform:uppercase;letter-spacing:0.1em;">This Week</span>'
        '</td></tr>'
        + sections
        + '</table></td></tr>'
    )


def render_zip_digest(
    subscription: dict,
    summary: dict,
    reasons: list[str],
    event_detail: dict,
) -> dict:
    """Return {'subject': str, 'html': str}."""
    zip_code = summary["zip"]
    name     = summary["name"]
    score    = summary["score_now"]
    delta    = summary["delta"]
    elevated = summary["elevated"]
    token    = subscription["unsubscribe_token"]

    risk_label, risk_color = _display_risk(score)
    score_color     = _score_color(score)
    delta_text      = _delta_text(delta)
    delta_color_val = _delta_color(delta)
    driver          = _driver_sentence(reasons, name)
    area            = _area_label(zip_code, name)

    subject = f"PulseCities Weekly Watch: {area} update"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PulseCities Weekly Watch: {name}</title>
</head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Inter',system-ui,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:48px 24px;">
    <tr><td align="center">
      <table width="100%" cellpadding="0" cellspacing="0" style="max-width:540px;">

        <!-- Header -->
        <tr><td style="padding-bottom:28px;">
          <span style="font-family:'JetBrains Mono',monospace;font-size:16px;font-weight:600;color:#38bdf8;">PulseCities</span>
          <span style="font-size:12px;color:rgba(148,163,184,0.4);margin-left:10px;">Weekly Watch</span>
        </td></tr>

        <!-- Lede -->
        <tr><td style="padding-bottom:20px;">
          <p style="margin:0;font-size:14px;color:#94a3b8;line-height:1.6;">
            Public-record changes for <strong style="color:#f1f5f9;">{name}</strong> this week.
          </p>
        </td></tr>

        <!-- Score card -->
        <tr><td>
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="background:#1e293b;border-radius:12px;padding:28px;border:1px solid rgba(148,163,184,0.1);">

            <!-- Neighborhood header -->
            <tr><td style="padding-bottom:20px;border-bottom:1px solid rgba(148,163,184,0.08);">
              <div style="font-size:20px;font-weight:700;color:#f1f5f9;">{name}</div>
              <div style="font-size:12px;color:#94a3b8;margin-top:2px;font-family:'JetBrains Mono',monospace;">{zip_code} &middot; NYC</div>
            </td></tr>

            <!-- Score row -->
            <tr><td style="padding-top:20px;padding-bottom:20px;border-bottom:1px solid rgba(148,163,184,0.08);">
              <table cellpadding="0" cellspacing="0">
                <tr>
                  <td style="padding-right:20px;">
                    <div style="font-family:'JetBrains Mono',monospace;font-size:48px;font-weight:700;color:{score_color};letter-spacing:-0.02em;line-height:1;">{score:.1f}</div>
                    <div style="font-size:10px;font-weight:600;color:{risk_color};text-transform:uppercase;letter-spacing:0.1em;margin-top:4px;">{risk_label}</div>
                  </td>
                  <td style="vertical-align:bottom;padding-bottom:8px;">
                    <div style="font-size:13px;color:{delta_color_val};font-weight:500;">{delta_text}</div>
                    <div style="font-size:11px;color:rgba(148,163,184,0.4);margin-top:4px;">vs. last week: {summary['score_prev']:.1f}</div>
                  </td>
                </tr>
              </table>
            </td></tr>

            <!-- What changed -->
            <tr><td style="padding-top:20px;padding-bottom:20px;border-bottom:1px solid rgba(148,163,184,0.08);">
              <div style="font-size:10px;font-weight:600;color:rgba(148,163,184,0.5);text-transform:uppercase;letter-spacing:0.1em;margin-bottom:12px;">What Changed</div>
              {_bullet_html(reasons)}
              {f'<p style="margin:12px 0 0;font-size:12px;color:#94a3b8;line-height:1.6;">{driver}</p>' if driver else ''}
            </td></tr>

            <!-- Signal breakdown -->
            <tr><td style="padding-top:20px;">
              <div style="font-size:10px;font-weight:600;color:rgba(148,163,184,0.5);text-transform:uppercase;letter-spacing:0.1em;margin-bottom:12px;">Signal Breakdown</div>
              {_signal_bars_html(elevated)}
            </td></tr>

            {_events_section_html(event_detail)}

            <!-- CTA -->
            <tr><td style="padding-top:24px;">
              <a href="https://pulsecities.com/neighborhood/{zip_code}"
                 style="display:inline-block;background:#f97316;color:#fff;font-size:13px;font-weight:600;padding:11px 22px;border-radius:6px;text-decoration:none;margin-right:12px;">
                View full data for {zip_code}
              </a>
              <a href="https://pulsecities.com/methodology.html"
                 style="display:inline-block;font-size:12px;color:#94a3b8;text-decoration:underline;vertical-align:middle;">
                Methodology
              </a>
            </td></tr>

          </table>
        </td></tr>

        <!-- Footer -->
        <tr><td style="padding-top:24px;">
          <p style="margin:0 0 10px;font-size:11px;color:rgba(148,163,184,0.5);line-height:1.7;border-top:1px solid rgba(148,163,184,0.08);padding-top:16px;">
            <strong style="color:rgba(148,163,184,0.6);">Why you're getting this:</strong>
            You're watching {area}. Public records changed enough this week to trigger an update.
          </p>
          <p style="margin:0 0 8px;font-size:11px;color:rgba(148,163,184,0.35);line-height:1.7;">
            PulseCities uses public records. Scores are risk indicators, not claims of wrongdoing.
          </p>
          <p style="margin:0;font-size:11px;color:rgba(148,163,184,0.35);line-height:1.7;">
            <a href="https://pulsecities.com/api/unsubscribe?token={token}"
               style="color:rgba(148,163,184,0.5);">Unsubscribe</a>
          </p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

    return {"subject": subject, "html": html}


# ---------------------------------------------------------------------------
# Sending
# ---------------------------------------------------------------------------

def send_digest_email(subscription: dict, rendered: dict, dry_run: bool = False) -> bool:
    """Send via Resend. Returns True on success, False on failure."""
    if dry_run:
        logger.info("[DRY RUN] Would send '%s' to %s", rendered["subject"], subscription["email"])
        return True
    try:
        resend.Emails.send({
            "from":    "PulseCities <alerts@pulsecities.com>",
            "to":      [subscription["email"]],
            "subject": rendered["subject"],
            "html":    rendered["html"],
        })
        return True
    except Exception:
        logger.exception("Resend failed for %s", subscription["email"])
        return False


# ---------------------------------------------------------------------------
# Citywide digest
# ---------------------------------------------------------------------------

def build_citywide_summary(db) -> dict:
    """Top-risk ZIPs and citywide signal snapshot for the citywide digest."""
    top_rows = db.execute(text("""
        SELECT ds.zip_code, n.name, ds.score,
               ds.permit_intensity, ds.eviction_rate,
               ds.llc_acquisition_rate, ds.complaint_rate
        FROM displacement_scores ds
        LEFT JOIN neighborhoods n ON ds.zip_code = n.zip_code
        WHERE ds.score IS NOT NULL
        ORDER BY ds.score DESC
        LIMIT 5
    """)).fetchall()

    top_zips = [
        {
            "zip":   r[0],
            "name":  (r[1] if r[1] and r[1] != r[0] else r[0]),
            "score": float(r[2]),
        }
        for r in top_rows
    ]

    avg_row = db.execute(text(
        "SELECT AVG(score), MAX(score), COUNT(*) FROM displacement_scores WHERE score IS NOT NULL"
    )).fetchone()

    return {
        "top_zips":  top_zips,
        "avg_score": round(float(avg_row[0] or 0), 1),
        "max_score": round(float(avg_row[1] or 0), 1),
        "zip_count": int(avg_row[2] or 0),
    }


def render_citywide_digest(subscription: dict, summary: dict) -> dict:
    """Subject and HTML for a citywide subscriber."""
    token    = subscription["unsubscribe_token"]
    top_zips = summary["top_zips"]
    max_score = summary["max_score"]
    risk_label, risk_color = _display_risk(max_score)

    rows_html = ""
    for z in top_zips:
        color = _score_color(z["score"])
        label = f"{z['name']} / {z['zip']}" if z["name"] != z["zip"] else z["zip"]
        rows_html += (
            f'<tr>'
            f'<td style="padding:8px 0;font-size:13px;color:#cbd5e1;">{label}</td>'
            f'<td style="padding:8px 0 8px 16px;font-family:\'JetBrains Mono\',monospace;'
            f'font-size:13px;color:{color};text-align:right;">{z["score"]:.1f}</td>'
            f'</tr>'
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PulseCities Weekly Watch: NYC</title>
</head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Inter',system-ui,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:48px 24px;">
    <tr><td align="center">
      <table width="100%" cellpadding="0" cellspacing="0" style="max-width:540px;">

        <tr><td style="padding-bottom:28px;">
          <span style="font-family:'JetBrains Mono',monospace;font-size:16px;font-weight:600;color:#38bdf8;">PulseCities</span>
          <span style="font-size:12px;color:rgba(148,163,184,0.4);margin-left:10px;">Weekly Watch</span>
        </td></tr>

        <tr><td style="padding-bottom:20px;">
          <p style="margin:0;font-size:14px;color:#94a3b8;line-height:1.6;">
            NYC displacement overview for this week.
          </p>
        </td></tr>

        <tr><td>
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="background:#1e293b;border-radius:12px;padding:28px;border:1px solid rgba(148,163,184,0.1);">

            <tr><td style="padding-bottom:20px;border-bottom:1px solid rgba(148,163,184,0.08);">
              <div style="font-size:10px;font-weight:600;color:rgba(148,163,184,0.5);text-transform:uppercase;letter-spacing:0.1em;margin-bottom:12px;">Highest Risk This Week</div>
              <table width="100%" cellpadding="0" cellspacing="0">
                {rows_html}
              </table>
            </td></tr>

            <tr><td style="padding-top:24px;">
              <a href="https://pulsecities.com"
                 style="display:inline-block;background:#f97316;color:#fff;font-size:13px;font-weight:600;padding:11px 22px;border-radius:6px;text-decoration:none;margin-right:12px;">
                Explore the map
              </a>
              <a href="https://pulsecities.com/methodology.html"
                 style="display:inline-block;font-size:12px;color:#94a3b8;text-decoration:underline;vertical-align:middle;">
                Methodology
              </a>
            </td></tr>

          </table>
        </td></tr>

        <tr><td style="padding-top:24px;">
          <p style="margin:0 0 10px;font-size:11px;color:rgba(148,163,184,0.5);line-height:1.7;border-top:1px solid rgba(148,163,184,0.08);padding-top:16px;">
            <strong style="color:rgba(148,163,184,0.6);">Why you're getting this:</strong>
            You're watching NYC-wide displacement activity.
          </p>
          <p style="margin:0 0 8px;font-size:11px;color:rgba(148,163,184,0.35);line-height:1.7;">
            PulseCities uses public records. Scores are risk indicators, not claims of wrongdoing.
          </p>
          <p style="margin:0;font-size:11px;color:rgba(148,163,184,0.35);line-height:1.7;">
            <a href="https://pulsecities.com/api/unsubscribe?token={token}"
               style="color:rgba(148,163,184,0.5);">Unsubscribe</a>
          </p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

    return {
        "subject": "PulseCities Weekly Watch: NYC displacement overview",
        "html":    html,
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run(dry_run: bool = False, limit: int | None = None, email_filter: str | None = None) -> None:
    if not dry_run and not resend.api_key:
        logger.error("RESEND_API_KEY not set. Aborting digest.")
        return

    db = SessionLocal()
    try:
        subscriptions = load_active_subscriptions(db)

        if email_filter:
            subscriptions = [s for s in subscriptions if s["email"] == email_filter]
        if limit is not None:
            subscriptions = subscriptions[:limit]

        if not subscriptions:
            logger.info("No active subscriptions. Nothing to send.")
            return

        logger.info(
            "Digest run: %d confirmed subscriber(s)%s%s",
            len(subscriptions),
            " [DRY RUN]" if dry_run else "",
            f" [limit={limit}]" if limit is not None else "",
        )

        zip_codes = {s["zip_code"] for s in subscriptions}
        summaries = build_weekly_zip_summaries(db, zip_codes)

        sent = skipped = failed = 0

        for sub in subscriptions:
            zip_code = sub["zip_code"]
            summary  = summaries.get(zip_code)

            if not summary:
                logger.warning("No score history for %s, skipping %s", zip_code, sub["email"])
                skipped += 1
                continue

            should_send, reasons = is_meaningful_zip_update(summary)
            if not should_send:
                logger.info(
                    "SKIP %s (%s): delta=%.1f hpd=%d evictions=%d permits=%d llc=%d complaints=%d",
                    zip_code, sub["email"],
                    summary["delta"], summary["hpd_count"], summary["eviction_count"],
                    summary["permit_count"], summary["llc_count"], summary["complaint_count"],
                )
                skipped += 1
                continue

            event_detail = _fetch_event_detail(db, zip_code)
            rendered     = render_zip_digest(sub, summary, reasons, event_detail)

            if send_digest_email(sub, rendered, dry_run=dry_run):
                logger.info("SENT %s -> %s", zip_code, sub["email"])
                sent += 1
            else:
                failed += 1

        logger.info("ZIP digest complete. sent=%d skipped=%d failed=%d", sent, skipped, failed)

        # --- Citywide subscribers ---
        citywide_subs = load_citywide_subscriptions(db)
        if email_filter:
            citywide_subs = [s for s in citywide_subs if s["email"] == email_filter]
        if limit is not None:
            remaining = max(0, limit - len(subscriptions))
            citywide_subs = citywide_subs[:remaining]

        if citywide_subs:
            citywide_summary = build_citywide_summary(db)
            c_sent = c_failed = 0
            for sub in citywide_subs:
                rendered = render_citywide_digest(sub, citywide_summary)
                if send_digest_email(sub, rendered, dry_run=dry_run):
                    logger.info("SENT citywide -> %s", sub["email"])
                    c_sent += 1
                else:
                    c_failed += 1
            logger.info("Citywide digest complete. sent=%d failed=%d", c_sent, c_failed)
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PulseCities weekly digest mailer")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without calling Resend")
    parser.add_argument("--limit",   type=int, metavar="N", help="Process at most N subscribers")
    parser.add_argument("--email",   metavar="ADDR",        help="Only process this email address")
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit, email_filter=args.email)
