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
from html import escape as _html_escape

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import resend
from sqlalchemy import text

from config.logging_config import configure_logging
from config.schedule import DIGEST_SEND_DAY
from models.database import SessionLocal  # imports load_dotenv() as a side effect
from scripts.digest_narrative import generate_narrative, generate_citywide_narrative

# The API process sets this in api/routes/subscribe.py; this script runs
# standalone from cron and must set it itself or every send aborts.
resend.api_key = os.getenv("RESEND_API_KEY", "")

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

# Canonical tier palette (see test_frontend_routes.py tripwires). On the
# email's light paper these muted inks also read far better than the old
# bright green/gold ever did.
_RISK_DISPLAY = [
    (85, "CRITICAL",      "#ef4444"),
    (67, "HIGH RISK",     "#f97316"),
    (34, "MODERATE RISK", "#C08B2D"),
    ( 0, "LOW RISK",      "#3E6B54"),
]


def _display_risk(score: float) -> tuple[str, str]:
    for threshold, label, color in _RISK_DISPLAY:
        if score >= threshold:
            return label, color
    return "LOW RISK", "#3E6B54"


def _score_color(score: float) -> str:
    if score >= 85: return "#ef4444"
    if score >= 67: return "#f97316"
    if score >= 34: return "#C08B2D"
    return "#3E6B54"


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
    # Windows key on created_at (ingest time), not event dates: the city feeds
    # publish with a lag, so "this week" by event date always undercounts and
    # would read as a fake quiet week. "This week" means newly on the record,
    # the same convention the operator digest uses.
    hpd_counts = dict(db.execute(text("""
        SELECT zip_code, COUNT(*) FROM violations_raw
        WHERE zip_code = ANY(:zips)
          AND created_at >= :cutoff
          AND violation_class IN ('B', 'C')
        GROUP BY zip_code
    """), {"zips": zips, "cutoff": week_ago}).fetchall())

    eviction_counts = dict(db.execute(text("""
        SELECT zip_code, COUNT(*) FROM evictions_raw
        WHERE zip_code = ANY(:zips)
          AND created_at >= :cutoff
          AND eviction_type ILIKE 'R%'
        GROUP BY zip_code
    """), {"zips": zips, "cutoff": week_ago}).fetchall())

    permit_counts = dict(db.execute(text("""
        SELECT p.zip_code, COUNT(*) FROM permits_raw pr
        JOIN parcels p ON pr.bbl = p.bbl
        WHERE p.zip_code = ANY(:zips)
          AND pr.created_at >= :cutoff
          AND pr.permit_type = 'AL'
          AND p.units_res >= 3
        GROUP BY p.zip_code
    """), {"zips": zips, "cutoff": week_ago}).fetchall())

    llc_counts = dict(db.execute(text("""
        SELECT p.zip_code, COUNT(DISTINCT o.bbl) FROM ownership_raw o
        JOIN parcels p ON o.bbl = p.bbl
        WHERE p.zip_code = ANY(:zips)
          AND o.created_at >= :cutoff
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
          AND created_at >= :cutoff
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
                       date_trunc('week', created_at) AS wk,
                       COUNT(*) AS weekly_cnt
                FROM violations_raw
                WHERE zip_code = ANY(:zips)
                  AND created_at >= :start AND created_at < :end
                  AND violation_class IN ('B', 'C')
                GROUP BY zip_code, wk
            ) sub GROUP BY zip_code
        """), {"zips": zips, "start": baseline_start, "end": baseline_end}).fetchall())

        eviction_baselines = dict(db.execute(text("""
            SELECT zip_code, AVG(weekly_cnt) FROM (
                SELECT zip_code,
                       date_trunc('week', created_at) AS wk,
                       COUNT(*) AS weekly_cnt
                FROM evictions_raw
                WHERE zip_code = ANY(:zips)
                  AND created_at >= :start AND created_at < :end
                  AND eviction_type ILIKE 'R%'
                GROUP BY zip_code, wk
            ) sub GROUP BY zip_code
        """), {"zips": zips, "start": baseline_start, "end": baseline_end}).fetchall())

        permit_baselines = dict(db.execute(text("""
            SELECT zip_code, AVG(weekly_cnt) FROM (
                SELECT p.zip_code,
                       date_trunc('week', pr.created_at) AS wk,
                       COUNT(*) AS weekly_cnt
                FROM permits_raw pr JOIN parcels p ON pr.bbl = p.bbl
                WHERE p.zip_code = ANY(:zips)
                  AND pr.created_at >= :start AND pr.created_at < :end
                  AND pr.permit_type = 'AL' AND p.units_res >= 3
                GROUP BY p.zip_code, wk
            ) sub GROUP BY zip_code
        """), {"zips": zips, "start": baseline_start, "end": baseline_end}).fetchall())

        complaint_baselines = dict(db.execute(text("""
            SELECT zip_code, AVG(weekly_cnt) FROM (
                SELECT zip_code,
                       date_trunc('week', created_at) AS wk,
                       COUNT(*) AS weekly_cnt
                FROM complaints_raw
                WHERE zip_code = ANY(:zips)
                  AND created_at >= :start AND created_at < :end
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
    """Address-level event rows for the email body — only called for ZIPs that
    pass threshold. Windows key on created_at (newly on the record this week);
    displayed dates remain the event dates from the records themselves."""
    cutoff = date.today() - timedelta(days=7)

    llc_rows = db.execute(text("""
        SELECT DISTINCT ON (o.bbl)
               par.address, o.party_name_normalized, o.doc_date
        FROM ownership_raw o
        JOIN parcels par ON o.bbl = par.bbl
        JOIN parcels p   ON o.bbl = p.bbl
        WHERE p.zip_code = :zip
          AND o.created_at >= :cutoff
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
          AND created_at >= :cutoff
          AND eviction_type ILIKE 'R%'
        ORDER BY executed_date DESC
        LIMIT 5
    """), {"zip": zip_code, "cutoff": cutoff}).fetchall()

    permit_rows = db.execute(text("""
        SELECT pr.address, pr.filing_date, pr.work_type
        FROM permits_raw pr
        JOIN parcels p ON pr.bbl = p.bbl
        WHERE p.zip_code = :zip
          AND pr.created_at >= :cutoff
          AND pr.permit_type = 'AL'
          AND p.units_res >= 3
        ORDER BY pr.filing_date DESC
        LIMIT 5
    """), {"zip": zip_code, "cutoff": cutoff}).fetchall()

    hpd_rows = db.execute(text("""
        SELECT address, inspection_date, violation_class, description
        FROM violations_raw
        WHERE zip_code = :zip
          AND created_at >= :cutoff
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
    if delta <= -SCORE_DELTA_MIN: return "#3E6B54"
    return "#94a3b8"


# --- Paper tokens for the ZIP email ------------------------------------------
# The site is the dark instrument; the email is the printed record it produces.
# Set on paper in email-safe system faces: a serif for prose, a monospace for
# figures and record entries. Web fonts don't load in most clients anyway.
_MONO  = "Menlo,Consolas,'Courier New',monospace"
_SERIF = "Georgia,'Times New Roman',serif"
_INK   = "#1C2430"
_BODY  = "#3A4352"
_MUTED = "#6D7480"
_FAINT = "#9A948A"
_RULE  = "#D9D4C9"
_PULSE = "#E4590F"


def _tier_ink(value: float) -> str:
    """Canonical bands (85/67/34) in ink weights that hold contrast on paper."""
    if value >= 85: return "#B3261E"
    if value >= 67: return "#C2410C"
    if value >= 34: return "#966A08"
    return "#1F7A44"


def _delta_ink(delta: float) -> str:
    if delta >= SCORE_DELTA_MIN:  return "#B3261E"
    if delta <= -SCORE_DELTA_MIN: return "#1F7A44"
    return _MUTED


def _stamp_date(v) -> str:
    try:
        return v.strftime("%b %-d")
    except (AttributeError, ValueError):
        return str(v)


def _field_label(label: str) -> str:
    return (
        f'<div style="font-family:{_MONO};font-size:10px;font-weight:700;color:{_FAINT};'
        f'text-transform:uppercase;letter-spacing:0.18em;margin-bottom:12px;">{label}</div>'
    )


def _bullet_html(reasons: list[str]) -> str:
    rows = "".join(
        f'<tr>'
        f'<td style="padding:0 10px 7px 0;font-family:{_MONO};font-size:12px;color:{_PULSE};vertical-align:top;">+</td>'
        f'<td width="100%" style="padding:0 0 7px;font-size:13px;color:{_BODY};line-height:1.55;">{r.capitalize()}</td>'
        f'</tr>'
        for r in reasons[:5]
    )
    return f'<table width="100%" cellpadding="0" cellspacing="0">{rows}</table>'


def _signal_bars_html(elevated: list[tuple[str, float]]) -> str:
    if not elevated:
        return (
            f'<p style="margin:0;font-family:{_SERIF};font-size:13px;color:{_FAINT};'
            f'font-style:italic;">No signals above baseline this week.</p>'
        )
    rows = ""
    for key, val in elevated[:5]:
        label = SIGNAL_LABELS.get(key, key)
        rows += (
            f'<tr>'
            f'<td style="padding:6px 8px 0 0;font-family:{_MONO};font-size:12px;color:{_BODY};white-space:nowrap;">{label}</td>'
            f'<td width="100%" style="border-bottom:1px dotted #B9B2A4;"></td>'
            f'<td style="padding:6px 0 0 10px;font-family:{_MONO};font-size:12px;font-weight:700;color:{_tier_ink(val)};white-space:nowrap;">{val:.0f}</td>'
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

    def _entries(rows, date_idx, label_fn):
        # Identical (date, address, label) lines collapse to one entry with a
        # count, so three same-day violations at one building read as a fact,
        # not a copy-paste mistake.
        collapsed: dict[tuple, int] = {}
        for row in rows:
            key = (_stamp_date(row[date_idx]), str(row[0] or "").title(), label_fn(row))
            collapsed[key] = collapsed.get(key, 0) + 1
        html = ""
        for (stamp, addr, label), count in collapsed.items():
            # Addresses and labels come from ACRIS/DOB free text; escape them
            # like the citywide section already does.
            addr = _html_escape(addr)
            label = _html_escape(label)
            shown = f"{addr} &times;{count}" if count > 1 else addr
            html += (
                f'<tr>'
                f'<td style="padding:5px 12px 5px 0;font-family:{_MONO};font-size:10px;color:{_FAINT};'
                f'white-space:nowrap;text-transform:uppercase;vertical-align:top;">{stamp}</td>'
                f'<td width="100%" style="padding:5px 12px 5px 0;font-family:{_MONO};font-size:12px;color:{_INK};">{shown}</td>'
                f'<td style="padding:5px 0;font-size:11px;color:{_MUTED};white-space:nowrap;text-align:right;vertical-align:top;">{label}</td>'
                f'</tr>'
            )
        return f'<table width="100%" cellpadding="0" cellspacing="0">{html}</table>'

    def _group(color, label, table):
        return (
            '<tr><td style="padding-bottom:14px;">'
            f'<span style="font-family:{_MONO};font-size:10px;color:{color};font-weight:700;'
            f'text-transform:uppercase;letter-spacing:0.14em;">{label}</span>'
            '<div style="margin-top:6px;">' + table + '</div></td></tr>'
        )

    sections = ""
    if llc_rows:
        sections += _group("#C2410C", "LLC Acquisitions",
                           _entries(llc_rows, 2, lambda r: str(r[1]).title().replace(" Llc", " LLC")))
    if eviction_rows:
        sections += _group("#B3261E", "Eviction Filings",
                           _entries(eviction_rows, 1, lambda _: "Residential eviction"))
    if permit_rows:
        sections += _group("#1F5D8A", "Permit Filings",
                           _entries(permit_rows, 1, lambda r: f"Alteration{(' ' + str(r[2])) if r[2] else ''}"))
    if hpd_rows:
        sections += _group("#6B4FA1", "HPD Violations",
                           _entries(hpd_rows, 1, lambda r: f"Class {r[2] or '?'}"))

    return (
        f'<tr><td style="padding:22px 0 2px;border-top:1px solid {_RULE};">'
        + _field_label("The Record")
        + '<table width="100%" cellpadding="0" cellspacing="0">' + sections + '</table>'
        + f'<p style="margin:4px 0 0;font-family:{_MONO};font-size:10px;color:{_FAINT};">'
        + 'Up to five entries per category, newly on record this week. Dates shown are from the records themselves.</p>'
        + '</td></tr>'
    )


def render_zip_digest(
    subscription: dict,
    summary: dict,
    reasons: list[str],
    event_detail: dict,
    narrative: str | None = None,
) -> dict:
    """Return {'subject': str, 'html': str}."""
    zip_code = summary["zip"]
    name     = summary["name"]
    score    = summary["score_now"]
    delta    = summary["delta"]
    elevated = summary["elevated"]
    token    = subscription["unsubscribe_token"]

    tier_label = ("CRITICAL" if score >= 85 else "HIGH" if score >= 67
                  else "MODERATE" if score >= 34 else "LOW")
    tier_color = _tier_ink(score)
    delta_text = _delta_text(delta)
    delta_ink  = _delta_ink(delta)
    driver     = _driver_sentence(reasons, name)
    area       = _area_label(zip_code, name)
    filed      = date.today().strftime("%b %-d, %Y")
    issue_no   = date.today().isocalendar()[1]

    narrative_html = ""
    if narrative:
        narrative_html = f"""
            <!-- Plain-English read -->
            <tr><td style="padding:22px 0;border-top:1px solid {_RULE};">
              {_field_label("The Week, In Plain English")}
              <p style="margin:0;font-family:{_SERIF};font-size:15px;color:{_INK};line-height:1.75;">{_html_escape(narrative)}</p>
              <p style="margin:10px 0 0;font-family:{_MONO};font-size:10px;color:{_FAINT};">Written by AI from this week's exact counts. The numbers below are the record.</p>
            </td></tr>"""

    subject = f"PulseCities Weekly Watch: {area} update"
    preheader = f"{name} is at {score:.1f}, {delta_text.lower()}. This week's public record, one page."

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PulseCities Weekly Watch: {name}</title>
</head>
<body style="margin:0;padding:0;background:#EFEBE2;">
  <div style="display:none;max-height:0;overflow:hidden;mso-hide:all;">{preheader}</div>
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#EFEBE2;padding:36px 16px;">
    <tr><td align="center">
      <table width="100%" cellpadding="0" cellspacing="0" style="max-width:560px;">

        <!-- The sheet -->
        <tr><td style="background:#FBFAF7;border:1px solid {_RULE};padding:30px 28px 26px;">
          <table width="100%" cellpadding="0" cellspacing="0">

            <!-- Masthead -->
            <tr><td style="padding-bottom:10px;border-bottom:2px solid {_INK};">
              <table width="100%" cellpadding="0" cellspacing="0"><tr>
                <td style="font-family:{_MONO};font-size:14px;font-weight:700;color:{_INK};letter-spacing:0.2em;">PULSECITIES</td>
                <td align="right" style="font-family:{_MONO};font-size:10px;color:{_MUTED};letter-spacing:0.14em;">WEEKLY WATCH &middot; NO. {issue_no}</td>
              </tr></table>
            </td></tr>

            <!-- File line -->
            <tr><td style="padding:10px 0 22px;">
              <span style="font-family:{_MONO};font-size:10px;color:{_FAINT};letter-spacing:0.14em;text-transform:uppercase;">Filed {filed} &middot; {area} &middot; NYC public records</span>
            </td></tr>

            <!-- Area name -->
            <tr><td style="padding-bottom:18px;">
              <div style="font-family:{_SERIF};font-size:27px;color:{_INK};line-height:1.15;">{name}</div>
            </td></tr>

            <!-- Pressure reading -->
            <tr><td style="padding:0 0 22px;">
              {_field_label("Displacement pressure")}
              <table width="100%" cellpadding="0" cellspacing="0"><tr>
                <td style="font-family:{_MONO};font-size:42px;font-weight:700;color:{tier_color};letter-spacing:-0.02em;line-height:1;white-space:nowrap;">{score:.1f}</td>
                <td width="100%" style="padding-left:14px;vertical-align:bottom;">
                  <span style="display:inline-block;font-family:{_MONO};font-size:10px;font-weight:700;color:{tier_color};border:1px solid {tier_color};padding:3px 7px;letter-spacing:0.14em;">{tier_label}</span>
                  <div style="font-family:{_MONO};font-size:11px;color:{delta_ink};margin-top:6px;white-space:nowrap;">{delta_text} &middot; last week {summary['score_prev']:.1f}</div>
                </td>
              </tr></table>
              <img src="https://pulsecities.com/og/spark/{zip_code}.png" width="504" alt="90-day pressure trace for {zip_code}" style="display:block;width:100%;height:auto;margin-top:14px;border:0;">
              <p style="margin:6px 0 0;font-family:{_MONO};font-size:10px;color:{_FAINT};letter-spacing:0.1em;text-transform:uppercase;">Pressure score, past 90 days</p>
            </td></tr>
{narrative_html}
            <!-- What changed -->
            <tr><td style="padding:22px 0;border-top:1px solid {_RULE};">
              {_field_label("What Changed")}
              {_bullet_html(reasons)}
              {f'<p style="margin:10px 0 0;font-family:{_SERIF};font-size:13px;color:{_MUTED};font-style:italic;line-height:1.6;">{driver}</p>' if driver else ''}
            </td></tr>

            <!-- Signal levels -->
            <tr><td style="padding:22px 0;border-top:1px solid {_RULE};">
              {_field_label("Signal Levels")}
              {_signal_bars_html(elevated)}
            </td></tr>

            {_events_section_html(event_detail)}

            <!-- CTA -->
            <tr><td style="padding-top:26px;">
              <a href="https://pulsecities.com/neighborhood/{zip_code}"
                 style="display:inline-block;background:{_INK};color:#FBFAF7;font-family:{_MONO};font-size:12px;font-weight:700;letter-spacing:0.06em;padding:12px 20px;text-decoration:none;">
                Open the full file for {zip_code} &rarr;
              </a>
              <a href="https://pulsecities.com/methodology.html"
                 style="display:inline-block;font-family:{_MONO};font-size:11px;color:{_MUTED};text-decoration:underline;vertical-align:middle;margin-left:14px;">
                Methodology
              </a>
            </td></tr>

          </table>
        </td></tr>

        <!-- Footer, off the sheet -->
        <tr><td style="padding:18px 6px 0;">
          <p style="margin:0 0 8px;font-family:{_MONO};font-size:10px;color:#8A8578;line-height:1.7;">
            Why you're getting this: you're watching {area}. Public records changed enough this week to trigger an update.
          </p>
          <p style="margin:0 0 8px;font-family:{_MONO};font-size:10px;color:#8A8578;line-height:1.7;">
            PulseCities reads NYC public records. Scores are risk indicators, not claims of wrongdoing.
          </p>
          <p style="margin:0;font-family:{_MONO};font-size:10px;line-height:1.7;">
            <a href="https://pulsecities.com/api/unsubscribe?token={token}" style="color:#8A8578;">Unsubscribe</a>
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
        payload = {
            "from":    "PulseCities <alerts@pulsecities.com>",
            "to":      [subscription["email"]],
            "subject": rendered["subject"],
            "html":    rendered["html"],
        }
        token = subscription.get("unsubscribe_token")
        if token:
            payload["headers"] = {
                "List-Unsubscribe": f"<https://pulsecities.com/api/unsubscribe?token={token}>",
                "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
            }
        resend.Emails.send(payload)
        return True
    except Exception:
        logger.exception("Resend failed for %s", subscription["email"])
        return False


# ---------------------------------------------------------------------------
# Citywide send threshold
# ---------------------------------------------------------------------------

def is_meaningful_citywide_update(db) -> tuple[bool, list[str]]:
    """
    Return (should_send, reasons).

    Conditions — any one is sufficient to trigger a citywide send:
    A. 3+ ZIPs moved by >= 3 score points this week.
    B. Any ZIP entered High tier (>= 75) this week from below.
    C. Top-10 ZIP ranking changed vs. last week.
    D. Pulse feed has >= 5 notable events (LLC acquisitions + evictions) this week.

    Conditions requiring data that isn't available are silently skipped.
    """
    reasons: list[str] = []
    today    = date.today()
    week_ago = today - timedelta(days=7)

    # A + B: score movement — one batch query for both conditions
    try:
        rows = db.execute(text("""
            SELECT cur.zip_code,
                   cur.composite_score  AS score_now,
                   prev.composite_score AS score_prev
            FROM score_history cur
            JOIN score_history prev ON cur.zip_code = prev.zip_code
            WHERE cur.scored_at  = :today
              AND prev.scored_at = (
                  SELECT MAX(scored_at) FROM score_history
                  WHERE zip_code = cur.zip_code AND scored_at < :today
              )
        """), {"today": today}).fetchall()

        movers = sum(1 for r in rows if abs(float(r[1] or 0) - float(r[2] or 0)) >= 3.0)
        if movers >= 3:
            reasons.append(f"{movers} ZIP codes moved by 3+ points this week")

        # 67 is the canonical High threshold (map legend, _tier_info, ai_summary).
        new_high = [r for r in rows if float(r[1] or 0) >= 67 and float(r[2] or 0) < 67]
        if new_high:
            names = ", ".join(r[0] for r in new_high[:3])
            reasons.append(f"{len(new_high)} ZIP(s) entered High tier: {names}")
    except Exception:
        logger.warning("Citywide condition A/B query failed — skipping", exc_info=True)

    # C: top-10 ranking changed
    try:
        cur_top = db.execute(text("""
            SELECT zip_code FROM score_history
            WHERE scored_at = :today
            ORDER BY composite_score DESC LIMIT 10
        """), {"today": today}).fetchall()

        prev_top = db.execute(text("""
            SELECT zip_code FROM score_history
            WHERE scored_at = (
                SELECT MAX(scored_at) FROM score_history WHERE scored_at < :today
            )
            ORDER BY composite_score DESC LIMIT 10
        """), {"today": today}).fetchall()

        if cur_top and prev_top:
            cur_set  = [r[0] for r in cur_top]
            prev_set = [r[0] for r in prev_top]
            if cur_set != prev_set:
                reasons.append("top-risk neighborhood ranking changed since last week")
    except Exception:
        logger.warning("Citywide condition C query failed — skipping", exc_info=True)

    # D: notable pulse events this week (LLC acquisitions + residential evictions)
    try:
        llc_count = db.execute(text("""
            SELECT COUNT(DISTINCT o.bbl)
            FROM ownership_raw o
            JOIN parcels p ON o.bbl = p.bbl
            WHERE o.created_at >= :cutoff
              AND o.party_type = '2'
              AND o.doc_type IN ('DEED','DEEDP','ASST')
              AND o.party_name_normalized LIKE '%LLC%'
              AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
              AND o.party_name_normalized NOT ILIKE '%LENDING%'
              AND o.party_name_normalized NOT ILIKE '%FINANCIAL %'
        """), {"cutoff": week_ago}).scalar() or 0

        eviction_count = db.execute(text("""
            SELECT COUNT(*) FROM evictions_raw
            WHERE created_at >= :cutoff AND eviction_type ILIKE 'R%'
        """), {"cutoff": week_ago}).scalar() or 0

        total_events = int(llc_count) + int(eviction_count)
        if total_events >= 5:
            reasons.append(
                f"{total_events} notable public-record events this week "
                f"({llc_count} LLC acquisitions, {eviction_count} evictions)"
            )
    except Exception:
        logger.warning("Citywide condition D query failed — skipping", exc_info=True)

    return bool(reasons), reasons


# ---------------------------------------------------------------------------
# Citywide digest
# ---------------------------------------------------------------------------

def build_citywide_summary(db) -> dict:
    """The city's week: movers, counts vs baselines, fresh buying clusters,
    and the standings, for the citywide digest."""
    today          = date.today()
    week_ago       = today - timedelta(days=7)
    baseline_start = today - timedelta(days=64)

    # 7-day score deltas per ZIP — the movement layer under both the movers
    # section and the standings.
    delta_rows = db.execute(text("""
        SELECT cur.zip_code, n.name,
               cur.composite_score  AS score_now,
               prev.composite_score AS score_prev
        FROM score_history cur
        JOIN score_history prev ON prev.zip_code = cur.zip_code
        LEFT JOIN neighborhoods n ON n.zip_code = cur.zip_code
        WHERE cur.scored_at = (SELECT MAX(scored_at) FROM score_history)
          AND prev.scored_at = (
              SELECT MAX(scored_at) FROM score_history s
              WHERE s.zip_code = cur.zip_code
                AND s.scored_at <= (SELECT MAX(scored_at) FROM score_history) - INTERVAL '6 days'
          )
    """)).fetchall()

    deltas: dict[str, float] = {}
    movers = []
    for r in delta_rows:
        if r.score_now is None or r.score_prev is None:
            continue
        delta = round(float(r.score_now) - float(r.score_prev), 1)
        deltas[r.zip_code] = delta
        movers.append({
            "zip":   r.zip_code,
            "name":  (r.name if r.name and r.name != r.zip_code else r.zip_code),
            "score": round(float(r.score_now), 1),
            "delta": delta,
        })
    movers = sorted((m for m in movers if abs(m["delta"]) >= 1.0),
                    key=lambda m: -abs(m["delta"]))[:5]

    top_rows = db.execute(text("""
        SELECT ds.zip_code, n.name, ds.score
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
            "delta": deltas.get(r[0], 0.0),
        }
        for r in top_rows
    ]

    avg_row = db.execute(text(
        "SELECT AVG(score), MAX(score), COUNT(*) FROM displacement_scores WHERE score IS NOT NULL"
    )).fetchone()

    # This week's citywide counts against the 8-week average week. Same signal
    # definitions as the per-ZIP queries in build_weekly_zip_summaries.
    week_row = db.execute(text("""
        SELECT
            (SELECT COUNT(*) FROM evictions_raw
             WHERE created_at >= :week_ago AND eviction_type ILIKE 'R%')                          AS ev,
            (SELECT COALESCE(AVG(c), 0) FROM (
                SELECT COUNT(*) AS c FROM evictions_raw
                WHERE created_at >= :baseline_start AND created_at < :week_ago
                  AND eviction_type ILIKE 'R%'
                GROUP BY date_trunc('week', created_at)) s)                                       AS ev_avg,
            (SELECT COUNT(DISTINCT o.bbl) FROM ownership_raw o
             WHERE o.created_at >= :week_ago AND o.party_type = '2'
               AND o.doc_type IN ('DEED','DEEDP','ASST')
               AND o.party_name_normalized LIKE '%LLC%'
               AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
               AND o.party_name_normalized NOT ILIKE '%LENDING%'
               AND o.party_name_normalized NOT ILIKE '%FINANCIAL %')                              AS llc,
            (SELECT COALESCE(AVG(c), 0) FROM (
                SELECT COUNT(DISTINCT o.bbl) AS c FROM ownership_raw o
                WHERE o.created_at >= :baseline_start AND o.created_at < :week_ago
                  AND o.party_type = '2' AND o.doc_type IN ('DEED','DEEDP','ASST')
                  AND o.party_name_normalized LIKE '%LLC%'
                  AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
                  AND o.party_name_normalized NOT ILIKE '%LENDING%'
                  AND o.party_name_normalized NOT ILIKE '%FINANCIAL %'
                GROUP BY date_trunc('week', o.created_at)) s)                                      AS llc_avg,
            (SELECT COUNT(*) FROM permits_raw pr JOIN parcels p ON pr.bbl = p.bbl
             WHERE pr.created_at >= :week_ago AND pr.permit_type = 'AL' AND p.units_res >= 3)     AS pm,
            (SELECT COALESCE(AVG(c), 0) FROM (
                SELECT COUNT(*) AS c FROM permits_raw pr JOIN parcels p ON pr.bbl = p.bbl
                WHERE pr.created_at >= :baseline_start AND pr.created_at < :week_ago
                  AND pr.permit_type = 'AL' AND p.units_res >= 3
                GROUP BY date_trunc('week', pr.created_at)) s)                                    AS pm_avg,
            (SELECT COUNT(*) FROM violations_raw
             WHERE created_at >= :week_ago AND violation_class IN ('B','C'))                      AS hpd,
            (SELECT COALESCE(AVG(c), 0) FROM (
                SELECT COUNT(*) AS c FROM violations_raw
                WHERE created_at >= :baseline_start AND created_at < :week_ago
                  AND violation_class IN ('B','C')
                GROUP BY date_trunc('week', created_at)) s)                                       AS hpd_avg
    """), {"week_ago": week_ago, "baseline_start": baseline_start}).fetchone()

    week = {
        "evictions":      int(week_row.ev or 0),
        "evictions_avg":  float(week_row.ev_avg or 0),
        "llc":            int(week_row.llc or 0),
        "llc_avg":        float(week_row.llc_avg or 0),
        "permits":        int(week_row.pm or 0),
        "permits_avg":    float(week_row.pm_avg or 0),
        "violations":     int(week_row.hpd or 0),
        "violations_avg": float(week_row.hpd_avg or 0),
    }

    # Speculation Radar clusters whose most recent deed landed this week —
    # concentrated buying is citywide news, and the radar already computes it.
    try:
        from api.routes.radar import query_radar
        week_ago_iso = week_ago.isoformat()
        clusters = [
            c for c in query_radar(db)
            if c["last_deed"] and c["last_deed"] >= week_ago_iso
        ][:3]
    except Exception:
        logger.warning("Radar clusters unavailable for citywide digest", exc_info=True)
        clusters = []

    return {
        "top_zips":  top_zips,
        "movers":    movers,
        "week":      week,
        "clusters":  clusters,
        "avg_score": round(float(avg_row[0] or 0), 1),
        "max_score": round(float(avg_row[1] or 0), 1),
        "zip_count": int(avg_row[2] or 0),
    }


def _money(v) -> str:
    if not v:
        return ""
    v = float(v)
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M".replace(".0M", "M")
    if v >= 1_000:
        return f"${round(v / 1_000)}K"
    return f"${int(v)}"


def render_citywide_digest(subscription: dict, summary: dict, narrative: str | None = None) -> dict:
    """Subject and HTML for a citywide subscriber — the city's week on paper."""
    token     = subscription["unsubscribe_token"]
    week      = summary.get("week") or {}
    movers    = summary.get("movers") or []
    clusters  = summary.get("clusters") or []
    avg_score = summary["avg_score"]
    filed     = date.today().strftime("%b %-d, %Y")
    issue_no  = date.today().isocalendar()[1]

    narrative_html = ""
    if narrative:
        narrative_html = f"""
            <tr><td style="padding:22px 0;border-top:1px solid {_RULE};">
              {_field_label("The Week, In Plain English")}
              <p style="margin:0;font-family:{_SERIF};font-size:15px;color:{_INK};line-height:1.75;">{_html_escape(narrative)}</p>
              <p style="margin:10px 0 0;font-family:{_MONO};font-size:10px;color:{_FAINT};">Written by AI from this week's exact counts. The numbers below are the record.</p>
            </td></tr>"""

    if movers:
        mover_rows = ""
        for m in movers:
            label = f"{m['name']} / {m['zip']}" if m["name"] != m["zip"] else m["zip"]
            mover_rows += (
                f'<tr>'
                f'<td style="padding:6px 8px 0 0;font-family:{_MONO};font-size:12px;color:{_BODY};white-space:nowrap;">{label}</td>'
                f'<td width="100%" style="border-bottom:1px dotted #B9B2A4;"></td>'
                f'<td style="padding:6px 0 0 10px;font-family:{_MONO};font-size:12px;font-weight:700;color:{_delta_ink(m["delta"])};white-space:nowrap;">{m["delta"]:+.1f}</td>'
                f'<td style="padding:6px 0 0 10px;font-family:{_MONO};font-size:11px;color:{_MUTED};white-space:nowrap;">now {m["score"]:.1f}</td>'
                f'</tr>'
            )
        movers_html = f'<table width="100%" cellpadding="0" cellspacing="0">{mover_rows}</table>'
    else:
        movers_html = (
            f'<p style="margin:0;font-family:{_SERIF};font-size:13px;color:{_FAINT};'
            f'font-style:italic;">No neighborhood moved by more than a point this week.</p>'
        )

    number_rows = ""
    for label, val, avg in (
        ("Eviction records added",      week.get("evictions"),  week.get("evictions_avg")),
        ("LLC acquisitions recorded",   week.get("llc"),        week.get("llc_avg")),
        ("Permit filings, 3+ units",    week.get("permits"),    week.get("permits_avg")),
        ("B/C violations recorded",     week.get("violations"), week.get("violations_avg")),
    ):
        if val is None:
            continue
        number_rows += (
            f'<tr>'
            f'<td style="padding:6px 8px 0 0;font-family:{_MONO};font-size:12px;color:{_BODY};white-space:nowrap;">{label}</td>'
            f'<td width="100%" style="border-bottom:1px dotted #B9B2A4;"></td>'
            f'<td style="padding:6px 0 0 10px;font-family:{_MONO};font-size:12px;font-weight:700;color:{_INK};white-space:nowrap;">{val:,}</td>'
            f'<td style="padding:6px 0 0 10px;font-family:{_MONO};font-size:11px;color:{_MUTED};white-space:nowrap;">typical {(avg or 0):,.0f}</td>'
            f'</tr>'
        )
    numbers_html = f'<table width="100%" cellpadding="0" cellspacing="0">{number_rows}</table>'

    clusters_html = ""
    if clusters:
        entries = ""
        for c in clusters:
            where = c.get("neighborhood") or c.get("zip_code")
            amount = _money(c.get("total_amount"))
            detail = f"{c['building_count']} buildings &middot; {where}"
            if amount:
                detail += f" &middot; {amount}"
            entries += (
                f'<tr><td style="padding-bottom:10px;">'
                f'<div style="font-family:{_MONO};font-size:12px;color:{_INK};">{_html_escape(str(c["buyer"]))}</div>'
                f'<div style="font-family:{_MONO};font-size:11px;color:{_MUTED};margin-top:2px;">{detail}</div>'
                f'</td></tr>'
            )
        clusters_html = f"""
            <tr><td style="padding:22px 0;border-top:1px solid {_RULE};">
              {_field_label("Concentrated Buying")}
              <table width="100%" cellpadding="0" cellspacing="0">{entries}</table>
              <p style="margin:4px 0 0;font-family:{_MONO};font-size:10px;color:{_FAINT};">One buyer, 3 or more buildings, one ZIP, with deed activity this week. <a href="https://pulsecities.com/radar" style="color:{_FAINT};">Full radar &rarr;</a></p>
            </td></tr>"""

    standings_rows = ""
    for z in summary["top_zips"]:
        label = f"{z['name']} / {z['zip']}" if z["name"] != z["zip"] else z["zip"]
        delta = z.get("delta") or 0.0
        delta_txt = f"{delta:+.1f}" if abs(delta) >= 0.1 else "flat"
        standings_rows += (
            f'<tr>'
            f'<td style="padding:6px 8px 0 0;font-family:{_MONO};font-size:12px;color:{_BODY};white-space:nowrap;">{label}</td>'
            f'<td width="100%" style="border-bottom:1px dotted #B9B2A4;"></td>'
            f'<td style="padding:6px 0 0 10px;font-family:{_MONO};font-size:12px;font-weight:700;color:{_tier_ink(z["score"])};white-space:nowrap;">{z["score"]:.1f}</td>'
            f'<td style="padding:6px 0 0 10px;font-family:{_MONO};font-size:11px;color:{_MUTED};white-space:nowrap;">{delta_txt}</td>'
            f'</tr>'
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PulseCities Weekly Watch: NYC</title>
</head>
<body style="margin:0;padding:0;background:#EFEBE2;">
  <div style="display:none;max-height:0;overflow:hidden;mso-hide:all;">The city's week in the public record: what moved, this week's counts, and who's buying in bulk.</div>
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#EFEBE2;padding:36px 16px;">
    <tr><td align="center">
      <table width="100%" cellpadding="0" cellspacing="0" style="max-width:560px;">

        <tr><td style="background:#FBFAF7;border:1px solid {_RULE};padding:30px 28px 26px;">
          <table width="100%" cellpadding="0" cellspacing="0">

            <tr><td style="padding-bottom:10px;border-bottom:2px solid {_INK};">
              <table width="100%" cellpadding="0" cellspacing="0"><tr>
                <td style="font-family:{_MONO};font-size:14px;font-weight:700;color:{_INK};letter-spacing:0.2em;">PULSECITIES</td>
                <td align="right" style="font-family:{_MONO};font-size:10px;color:{_MUTED};letter-spacing:0.14em;">WEEKLY WATCH &middot; NO. {issue_no}</td>
              </tr></table>
            </td></tr>

            <tr><td style="padding:10px 0 22px;">
              <span style="font-family:{_MONO};font-size:10px;color:{_FAINT};letter-spacing:0.14em;text-transform:uppercase;">Filed {filed} &middot; Citywide &middot; NYC public records</span>
            </td></tr>

            <tr><td style="padding-bottom:18px;">
              <div style="font-family:{_SERIF};font-size:27px;color:{_INK};line-height:1.15;">New York City</div>
            </td></tr>

            <tr><td style="padding:0 0 22px;">
              {_field_label("City pressure")}
              <table width="100%" cellpadding="0" cellspacing="0"><tr>
                <td style="font-family:{_MONO};font-size:42px;font-weight:700;color:{_tier_ink(avg_score)};letter-spacing:-0.02em;line-height:1;white-space:nowrap;">{avg_score:.1f}</td>
                <td width="100%" style="padding-left:14px;vertical-align:bottom;">
                  <div style="font-family:{_MONO};font-size:11px;color:{_MUTED};white-space:nowrap;">average across {summary['zip_count']} neighborhoods</div>
                  <div style="font-family:{_MONO};font-size:11px;color:{_MUTED};margin-top:4px;white-space:nowrap;">highest {summary['max_score']:.1f}</div>
                </td>
              </tr></table>
              <img src="https://pulsecities.com/og/spark/nyc.png" width="504" alt="90-day citywide pressure trace" style="display:block;width:100%;height:auto;margin-top:14px;border:0;">
              <p style="margin:6px 0 0;font-family:{_MONO};font-size:10px;color:{_FAINT};letter-spacing:0.1em;text-transform:uppercase;">Citywide average, past 90 days</p>
            </td></tr>
{narrative_html}
            <tr><td style="padding:22px 0;border-top:1px solid {_RULE};">
              {_field_label("The Week's Movers")}
              {movers_html}
            </td></tr>

            <tr><td style="padding:22px 0;border-top:1px solid {_RULE};">
              {_field_label("The Week's Numbers")}
              {numbers_html}
              <p style="margin:8px 0 0;font-family:{_MONO};font-size:10px;color:{_FAINT};">Records newly published this week. City feeds lag event dates.</p>
            </td></tr>
{clusters_html}
            <tr><td style="padding:22px 0;border-top:1px solid {_RULE};">
              {_field_label("Highest Pressure")}
              <table width="100%" cellpadding="0" cellspacing="0">{standings_rows}</table>
            </td></tr>

            <tr><td style="padding-top:26px;">
              <a href="https://pulsecities.com/this-week"
                 style="display:inline-block;background:{_INK};color:#FBFAF7;font-family:{_MONO};font-size:12px;font-weight:700;letter-spacing:0.06em;padding:12px 20px;text-decoration:none;">
                Read this week's full report &rarr;
              </a>
              <a href="https://pulsecities.com/methodology.html"
                 style="display:inline-block;font-family:{_MONO};font-size:11px;color:{_MUTED};text-decoration:underline;vertical-align:middle;margin-left:14px;">
                Methodology
              </a>
            </td></tr>

          </table>
        </td></tr>

        <tr><td style="padding:18px 6px 0;">
          <p style="margin:0 0 8px;font-family:{_MONO};font-size:10px;color:#8A8578;line-height:1.7;">
            Why you're getting this: you're watching NYC-wide displacement activity. Public records changed enough this week to trigger an update.
          </p>
          <p style="margin:0 0 8px;font-family:{_MONO};font-size:10px;color:#8A8578;line-height:1.7;">
            PulseCities reads NYC public records. Scores are risk indicators, not claims of wrongdoing.
          </p>
          <p style="margin:0;font-family:{_MONO};font-size:10px;line-height:1.7;">
            <a href="https://pulsecities.com/api/unsubscribe?token={token}" style="color:#8A8578;">Unsubscribe</a>
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
# Operator follows
# ---------------------------------------------------------------------------

def load_operator_follows(db) -> list[dict]:
    """Return confirmed operator-follow subscribers."""
    rows = db.execute(text("""
        SELECT email, operator_slug, unsubscribe_token
        FROM subscribers
        WHERE confirmed = true AND operator_slug IS NOT NULL
        ORDER BY operator_slug, email
    """)).fetchall()
    return [{"email": r[0], "operator_slug": r[1], "unsubscribe_token": r[2]} for r in rows]


def build_operator_updates(db, slugs: set[str]) -> dict[str, dict]:
    """New acquisitions per followed operator, keyed on ingest time.

    created_at, not doc date: ACRIS publishes with a lag, so "new this
    week" means "newly on the record", same as everywhere on the site.
    Operators with nothing new are absent, which skips their send.
    """
    if not slugs:
        return {}
    rows = db.execute(text("""
        SELECT o.slug, o.display_name, o.operator_root,
               op.bbl, op.acquisition_date, op.acquisition_price,
               p.address, p.zip_code
        FROM operators o
        JOIN operator_parcels op ON op.operator_id = o.id
        LEFT JOIN parcels p ON p.bbl = op.bbl
        WHERE o.slug = ANY(:slugs)
          AND o.operator_class = 'operator'
          AND op.created_at >= NOW() - INTERVAL '7 days'
        ORDER BY o.slug, op.acquisition_date DESC NULLS LAST
    """), {"slugs": list(slugs)}).fetchall()

    updates: dict[str, dict] = {}
    for r in rows:
        u = updates.setdefault(r.slug, {
            "slug":         r.slug,
            "display_name": r.display_name or r.operator_root,
            "acquisitions": [],
        })
        u["acquisitions"].append({
            "address": (r.address or f"BBL {r.bbl}").title(),
            "zip":     r.zip_code or "",
            "date":    r.acquisition_date.isoformat() if r.acquisition_date else "",
            "price":   float(r.acquisition_price) if r.acquisition_price else None,
        })
    return updates


def render_operator_digest(subscription: dict, update: dict) -> dict:
    """Subject and HTML for one operator-follow alert."""
    token = subscription["unsubscribe_token"]
    name  = update["display_name"]
    slug  = update["slug"]
    acqs  = update["acquisitions"]
    n     = len(acqs)

    rows_html = ""
    for a in acqs[:15]:
        price = f"${a['price']:,.0f}" if a["price"] else ""
        place = _html_escape(f"{a['address']}" + (f" ({a['zip']})" if a["zip"] else ""))
        rows_html += (
            f'<tr>'
            f'<td style="padding:8px 0;font-size:13px;color:#cbd5e1;">{place}</td>'
            f'<td style="padding:8px 0 8px 16px;font-family:\'JetBrains Mono\',monospace;'
            f'font-size:12px;color:#94a3b8;text-align:right;white-space:nowrap;">{a["date"]}</td>'
            f'<td style="padding:8px 0 8px 16px;font-family:\'JetBrains Mono\',monospace;'
            f'font-size:12px;color:#f97316;text-align:right;white-space:nowrap;">{price}</td>'
            f'</tr>'
        )
    more_html = ""
    if n > 15:
        more_html = (
            f'<p style="margin:12px 0 0;font-size:12px;color:rgba(148,163,184,0.6);">'
            f'And {n - 15} more on the profile page.</p>'
        )

    plural = "acquisitions" if n != 1 else "acquisition"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PulseCities: {name} update</title>
</head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Inter',system-ui,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:48px 24px;">
    <tr><td align="center">
      <table width="100%" cellpadding="0" cellspacing="0" style="max-width:540px;">

        <tr><td style="padding-bottom:28px;">
          <span style="font-family:'JetBrains Mono',monospace;font-size:16px;font-weight:600;color:#38bdf8;">PulseCities</span>
          <span style="font-size:12px;color:rgba(148,163,184,0.4);margin-left:10px;">Operator Watch</span>
        </td></tr>

        <tr><td style="padding-bottom:20px;">
          <p style="margin:0;font-size:14px;color:#94a3b8;line-height:1.6;">
            <strong style="color:#f1f5f9;">{name}</strong> recorded {n} new {plural} in NYC public records this week.
          </p>
        </td></tr>

        <tr><td>
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="background:#1e293b;border-radius:12px;padding:28px;border:1px solid rgba(148,163,184,0.1);">

            <tr><td style="padding-bottom:20px;border-bottom:1px solid rgba(148,163,184,0.08);">
              <div style="font-size:10px;font-weight:600;color:rgba(148,163,184,0.5);text-transform:uppercase;letter-spacing:0.1em;margin-bottom:12px;">Newly Recorded Acquisitions</div>
              <table width="100%" cellpadding="0" cellspacing="0">
                {rows_html}
              </table>
              {more_html}
            </td></tr>

            <tr><td style="padding-top:24px;">
              <a href="https://pulsecities.com/operator/{slug}"
                 style="display:inline-block;background:#f97316;color:#fff;font-size:13px;font-weight:600;padding:11px 22px;border-radius:6px;text-decoration:none;margin-right:12px;">
                View the full profile
              </a>
              <a href="https://pulsecities.com/brief/operator/{slug}"
                 style="display:inline-block;font-size:12px;color:#94a3b8;text-decoration:underline;vertical-align:middle;">
                Evidence brief
              </a>
            </td></tr>

          </table>
        </td></tr>

        <tr><td style="padding-top:24px;">
          <p style="margin:0 0 10px;font-size:11px;color:rgba(148,163,184,0.5);line-height:1.7;border-top:1px solid rgba(148,163,184,0.08);padding-top:16px;">
            <strong style="color:rgba(148,163,184,0.6);">Why you're getting this:</strong>
            You follow {name} on PulseCities. Quiet weeks send nothing.
          </p>
          <p style="margin:0 0 8px;font-size:11px;color:rgba(148,163,184,0.35);line-height:1.7;">
            PulseCities uses public records. Grouping reflects LLC naming patterns, not claims of wrongdoing.
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
        "subject": f"{name} recorded {n} new {plural} this week",
        "html":    html,
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run(dry_run: bool = False, limit: int | None = None, email_filter: str | None = None) -> None:
    if not dry_run and not resend.api_key:
        logger.error("RESEND_API_KEY not set. Aborting digest.")
        sys.exit(1)

    db = SessionLocal()
    try:
        subscriptions = load_active_subscriptions(db)

        if email_filter:
            subscriptions = [s for s in subscriptions if s["email"] == email_filter]
        if limit is not None:
            subscriptions = subscriptions[:limit]

        # No early return here: citywide and operator sends below must still
        # run when there are zero ZIP subscribers.
        if not subscriptions:
            logger.info("No active ZIP subscriptions.")

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
            narrative    = generate_narrative(summary, reasons)
            rendered     = render_zip_digest(sub, summary, reasons, event_detail, narrative=narrative)

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
            should_send_citywide, citywide_reasons = is_meaningful_citywide_update(db)
            if not should_send_citywide:
                logger.info(
                    "SKIP citywide digest: no threshold met for %d subscriber(s). "
                    "Conditions checked: score movement, tier change, ranking shift, pulse events.",
                    len(citywide_subs),
                )
            else:
                logger.info("Citywide threshold met: %s", "; ".join(citywide_reasons))
                citywide_summary   = build_citywide_summary(db)
                citywide_narrative = generate_citywide_narrative(citywide_summary)
                c_sent = c_failed = 0
                for sub in citywide_subs:
                    rendered = render_citywide_digest(sub, citywide_summary, narrative=citywide_narrative)
                    if send_digest_email(sub, rendered, dry_run=dry_run):
                        logger.info("SENT citywide -> %s", sub["email"])
                        c_sent += 1
                    else:
                        c_failed += 1
                logger.info("Citywide digest complete. sent=%d failed=%d", c_sent, c_failed)

        # --- Operator follows ---
        follows = load_operator_follows(db)
        if email_filter:
            follows = [s for s in follows if s["email"] == email_filter]
        if limit is not None:
            remaining = max(0, limit - len(subscriptions) - len(citywide_subs))
            follows = follows[:remaining]

        if follows:
            updates = build_operator_updates(db, {s["operator_slug"] for s in follows})
            o_sent = o_skipped = o_failed = 0
            for sub in follows:
                update = updates.get(sub["operator_slug"])
                if not update:
                    logger.info("SKIP operator %s (%s): nothing newly recorded",
                                sub["operator_slug"], sub["email"])
                    o_skipped += 1
                    continue
                rendered = render_operator_digest(sub, update)
                if send_digest_email(sub, rendered, dry_run=dry_run):
                    logger.info("SENT operator %s -> %s", sub["operator_slug"], sub["email"])
                    o_sent += 1
                else:
                    o_failed += 1
            logger.info("Operator digest complete. sent=%d skipped=%d failed=%d",
                        o_sent, o_skipped, o_failed)
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PulseCities weekly digest mailer")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without calling Resend")
    parser.add_argument("--limit",   type=int, metavar="N", help="Process at most N subscribers")
    parser.add_argument("--email",   metavar="ADDR",        help="Only process this email address")
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit, email_filter=args.email)
