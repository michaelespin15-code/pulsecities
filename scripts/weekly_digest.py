"""
Weekly digest mailer — runs every Sunday at 9:00 AM UTC.

For each subscriber, sends a one-email summary of their zip code:
  - Score this week vs last week
  - Which signals are elevated
  - Recent LLC acquisitions, evictions, and permit filings (past 7 days)
  - Link back to the neighborhood page

Cron entry (add to /etc/cron.d/pulsecities or crontab):
  0 9 * * 0 root /root/pulsecities/venv/bin/python -m scripts.weekly_digest >> /var/log/pulsecities/digest.log 2>&1
"""

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import resend
from sqlalchemy import text

from config.logging_config import configure_logging
from models.database import SessionLocal

configure_logging()
logger = logging.getLogger(__name__)

resend.api_key = os.getenv("RESEND_API_KEY", "")

SIGNAL_LABELS = {
    "permit_intensity":    "Permit Filings",
    "llc_acquisition_rate":"LLC Acquisitions",
    "eviction_rate":       "Evictions",
    "complaint_rate":      "Complaints",
    "rs_unit_loss":        "RS Unit Loss",
    "assessment_spike":    "Assessment Spike",
}

RISK_LABEL = {
    (0,  33):  ("LOW RISK",      "#22c55e"),
    (34, 66):  ("MODERATE RISK", "#eab308"),
    (67, 84):  ("HIGH RISK",     "#f97316"),
    (85, 100): ("CRITICAL",      "#ef4444"),
}

def _risk(score):
    for (lo, hi), (label, color) in RISK_LABEL.items():
        if lo <= score <= hi:
            return label, color
    return "MODERATE RISK", "#eab308"


def _score_color(score):
    if score >= 85: return "#ef4444"
    if score >= 67: return "#f97316"
    if score >= 34: return "#eab308"
    return "#22c55e"


def _fetch_digest_data(db, zip_code: str) -> dict | None:
    # Current and prior week scores
    r = db.execute(text("""
        SELECT scored_at, composite_score,
               permit_intensity, eviction_rate,
               llc_acquisition_rate, complaint_rate, rs_unit_loss
        FROM score_history
        WHERE zip_code = :zip
        ORDER BY scored_at DESC
        LIMIT 14
    """), {"zip": zip_code}).fetchall()

    if not r:
        return None

    current = r[0]
    prior   = next((row for row in r if (current[0] - row[0]).days >= 6), None)

    score_now  = float(current[1]) if current[1] is not None else 0.0
    score_prev = float(prior[1])   if prior and prior[1] is not None else score_now
    delta      = round(score_now - score_prev, 1)

    # Neighborhood name
    name_row = db.execute(text(
        "SELECT name FROM neighborhoods WHERE zip_code = :zip"
    ), {"zip": zip_code}).fetchone()
    name = (name_row[0] if name_row and name_row[0] and name_row[0] != zip_code
            else zip_code)

    # Top elevated signals (score > 40)
    signal_map = {
        "permit_intensity":    float(current[2] or 0),
        "eviction_rate":       float(current[3] or 0),
        "llc_acquisition_rate":float(current[4] or 0),
        "complaint_rate":      float(current[5] or 0),
        "rs_unit_loss":        float(current[6] or 0),
    }
    elevated = sorted(
        [(k, v) for k, v in signal_map.items() if v > 40],
        key=lambda x: -x[1]
    )

    # Recent events — last 7 days
    llc_rows = db.execute(text("""
        SELECT DISTINCT ON (o.bbl)
               par.address, o.party_name_normalized, o.doc_date
        FROM ownership_raw o
        JOIN parcels par ON o.bbl = par.bbl
        JOIN parcels p   ON o.bbl = p.bbl
        WHERE p.zip_code = :zip
          AND o.doc_date >= CURRENT_DATE - INTERVAL '7 days'
          AND o.party_type = '2'
          AND o.doc_type IN ('DEED','DEEDP','ASST')
          AND o.party_name_normalized LIKE '%LLC%'
          AND o.party_name_normalized NOT ILIKE '%MORTGAGE%'
          AND o.party_name_normalized NOT ILIKE '%LENDING%'
          AND o.party_name_normalized NOT ILIKE '%FINANCIAL %'
        ORDER BY o.bbl, o.doc_date DESC
        LIMIT 5
    """), {"zip": zip_code}).fetchall()

    eviction_rows = db.execute(text("""
        SELECT address, executed_date FROM evictions_raw
        WHERE zip_code = :zip
          AND executed_date >= CURRENT_DATE - INTERVAL '7 days'
          AND eviction_type ILIKE 'R%'
        ORDER BY executed_date DESC
        LIMIT 5
    """), {"zip": zip_code}).fetchall()

    permit_rows = db.execute(text("""
        SELECT pr.address, pr.filing_date, pr.work_type
        FROM permits_raw pr
        JOIN parcels p ON pr.bbl = p.bbl
        WHERE p.zip_code = :zip
          AND pr.filing_date >= CURRENT_DATE - INTERVAL '7 days'
          AND pr.permit_type = 'AL'
          AND p.units_res >= 3
        ORDER BY pr.filing_date DESC
        LIMIT 5
    """), {"zip": zip_code}).fetchall()

    return {
        "zip":          zip_code,
        "name":         name,
        "score_now":    score_now,
        "score_prev":   score_prev,
        "delta":        delta,
        "elevated":     elevated,
        "llc_rows":     llc_rows,
        "eviction_rows":eviction_rows,
        "permit_rows":  permit_rows,
    }


def _direction_html(delta: float) -> str:
    if delta > 0.5:
        return f'<span style="color:#ef4444;font-size:13px;">&#8593; {delta:+.1f} this week</span>'
    if delta < -0.5:
        return f'<span style="color:#22c55e;font-size:13px;">&#8595; {abs(delta):.1f} this week</span>'
    return '<span style="color:#94a3b8;font-size:13px;">Stable this week</span>'


def _event_rows_html(rows, date_field_idx: int, label_fn) -> str:
    if not rows:
        return '<p style="margin:0;font-size:12px;color:rgba(148,163,184,0.4);font-style:italic;">No activity this week.</p>'
    html = ""
    for row in rows:
        date  = row[date_field_idx]
        label = label_fn(row)
        addr  = str(row[0]).title()
        html += (
            f'<tr>'
            f'<td style="padding:6px 0;font-size:12px;color:#cbd5e1;font-family:\'JetBrains Mono\',monospace;">{addr}</td>'
            f'<td style="padding:6px 0 6px 16px;font-size:11px;color:#94a3b8;white-space:nowrap;">{date}</td>'
            f'<td style="padding:6px 0 6px 16px;font-size:11px;color:#94a3b8;white-space:nowrap;">{label}</td>'
            f'</tr>'
        )
    return f'<table width="100%" cellpadding="0" cellspacing="0">{html}</table>'


def _signal_bar_html(label: str, value: float) -> str:
    color = _score_color(value)
    pct   = min(100, int(value))
    return (
        f'<tr style="vertical-align:middle;">'
        f'<td style="font-size:11px;color:#94a3b8;padding:4px 0;width:130px;">{label}</td>'
        f'<td style="padding:4px 8px;">'
        f'  <div style="background:rgba(148,163,184,0.1);border-radius:2px;height:4px;width:100%;">'
        f'    <div style="background:{color};height:4px;border-radius:2px;width:{pct}%;"></div>'
        f'  </div>'
        f'</td>'
        f'<td style="font-size:12px;color:{color};font-family:\'JetBrains Mono\',monospace;text-align:right;padding:4px 0;white-space:nowrap;">{value:.0f}</td>'
        f'</tr>'
    )


def _build_email_html(data: dict) -> str:
    zip_code   = data["zip"]
    name       = data["name"]
    score      = data["score_now"]
    delta      = data["delta"]
    elevated   = data["elevated"]
    risk_label, risk_color = _risk(score)
    score_color = _score_color(score)

    signal_rows_html = "".join(
        _signal_bar_html(SIGNAL_LABELS.get(k, k), v)
        for k, v in elevated
    ) if elevated else '<tr><td colspan="3" style="font-size:12px;color:rgba(148,163,184,0.4);font-style:italic;padding:4px 0;">No signals above average this week.</td></tr>'

    llc_html = _event_rows_html(
        data["llc_rows"], 2,
        lambda r: str(r[1]).title().replace(" Llc", " LLC")
    )
    eviction_html = _event_rows_html(
        data["eviction_rows"], 1,
        lambda r: "Residential eviction"
    )
    permit_html = _event_rows_html(
        data["permit_rows"], 1,
        lambda r: f"Alteration filed{(' ' + str(r[2])) if r[2] else ''}"
    )

    has_events = data["llc_rows"] or data["eviction_rows"] or data["permit_rows"]
    events_section = ""
    if has_events:
        events_section = f"""
          <tr><td style="padding-top:24px;">
            <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;border-radius:8px;padding:20px;border:1px solid rgba(148,163,184,0.08);">
              <tr><td style="padding-bottom:16px;">
                <span style="font-size:10px;font-weight:600;color:rgba(148,163,184,0.5);text-transform:uppercase;letter-spacing:0.1em;">This Week</span>
              </td></tr>
              {'<tr><td style="padding-bottom:16px;"><span style="font-size:10px;color:#f59e0b;font-weight:600;text-transform:uppercase;letter-spacing:0.08em;">LLC Acquisitions</span><div style="margin-top:8px;">' + llc_html + '</div></td></tr>' if data['llc_rows'] else ''}
              {'<tr><td style="padding-bottom:16px;"><span style="font-size:10px;color:#ef4444;font-weight:600;text-transform:uppercase;letter-spacing:0.08em;">Evictions</span><div style="margin-top:8px;">' + eviction_html + '</div></td></tr>' if data['eviction_rows'] else ''}
              {'<tr><td><span style="font-size:10px;color:#38bdf8;font-weight:600;text-transform:uppercase;letter-spacing:0.08em;">Permit Filings</span><div style="margin-top:8px;">' + permit_html + '</div></td></tr>' if data['permit_rows'] else ''}
            </table>
          </td></tr>
        """

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PulseCities Weekly: {name} ({zip_code})</title>
</head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Inter',system-ui,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:48px 24px;">
    <tr><td align="center">
      <table width="100%" cellpadding="0" cellspacing="0" style="max-width:540px;">

        <!-- Header -->
        <tr><td style="padding-bottom:28px;">
          <span style="font-family:'JetBrains Mono',monospace;font-size:16px;font-weight:600;color:#38bdf8;">PulseCities</span>
          <span style="font-size:12px;color:rgba(148,163,184,0.4);margin-left:10px;">Weekly Digest</span>
        </td></tr>

        <!-- Score card -->
        <tr><td>
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#1e293b;border-radius:12px;padding:28px;border:1px solid rgba(148,163,184,0.1);">

            <!-- Neighborhood -->
            <tr><td style="padding-bottom:20px;border-bottom:1px solid rgba(148,163,184,0.08);">
              <div style="font-size:20px;font-weight:700;color:#f1f5f9;">{name}</div>
              <div style="font-size:12px;color:#94a3b8;margin-top:2px;font-family:'JetBrains Mono',monospace;">{zip_code} &middot; NYC</div>
            </td></tr>

            <!-- Score -->
            <tr><td style="padding-top:20px;padding-bottom:20px;border-bottom:1px solid rgba(148,163,184,0.08);">
              <table cellpadding="0" cellspacing="0">
                <tr>
                  <td style="padding-right:20px;">
                    <div style="font-family:'JetBrains Mono',monospace;font-size:48px;font-weight:700;color:{score_color};letter-spacing:-0.02em;line-height:1;">{score:.1f}</div>
                    <div style="font-size:10px;font-weight:600;color:{risk_color};text-transform:uppercase;letter-spacing:0.1em;margin-top:4px;">{risk_label}</div>
                  </td>
                  <td style="vertical-align:bottom;padding-bottom:8px;">
                    {_direction_html(delta)}
                    <div style="font-size:11px;color:rgba(148,163,184,0.4);margin-top:4px;">vs. last week: {data['score_prev']:.1f}</div>
                  </td>
                </tr>
              </table>
            </td></tr>

            <!-- Signal breakdown -->
            <tr><td style="padding-top:20px;">
              <div style="font-size:10px;font-weight:600;color:rgba(148,163,184,0.5);text-transform:uppercase;letter-spacing:0.1em;margin-bottom:12px;">Signal Breakdown</div>
              <table width="100%" cellpadding="0" cellspacing="0">
                {signal_rows_html}
              </table>
            </td></tr>

            {events_section}

            <!-- CTA -->
            <tr><td style="padding-top:24px;">
              <a href="https://pulsecities.com/neighborhood/{zip_code}"
                 style="display:inline-block;background:#f97316;color:#fff;font-size:13px;font-weight:600;padding:11px 22px;border-radius:6px;text-decoration:none;">
                View full data for {zip_code}
              </a>
            </td></tr>

          </table>
        </td></tr>

        <!-- Footer -->
        <tr><td style="padding-top:24px;">
          <p style="margin:0;font-size:11px;color:rgba(148,163,184,0.35);line-height:1.7;">
            PulseCities tracks displacement pressure across NYC using public records from ACRIS, DOB, and NYC Open Data.<br>
            You subscribed at pulsecities.com. To unsubscribe, reply with "unsubscribe" and your ZIP code.
          </p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


def _subject(data: dict) -> str:
    name  = data["name"]
    score = data["score_now"]
    delta = data["delta"]
    risk_label, _ = _risk(score)

    if delta > 0.5:
        trend = f"score up {delta:+.1f}"
    elif delta < -0.5:
        trend = f"score down {abs(delta):.1f}"
    else:
        trend = "score stable"

    events = []
    if data["llc_rows"]:
        events.append(f"{len(data['llc_rows'])} LLC buy{'s' if len(data['llc_rows']) > 1 else ''}")
    if data["eviction_rows"]:
        events.append(f"{len(data['eviction_rows'])} eviction{'s' if len(data['eviction_rows']) > 1 else ''}")
    if data["permit_rows"]:
        events.append(f"{len(data['permit_rows'])} permit{'s' if len(data['permit_rows']) > 1 else ''}")

    suffix = ", ".join(events[:2])
    if suffix:
        return f"{name} this week: {trend} · {suffix}"
    return f"{name} this week: {trend} · {risk_label}"


def run_digest() -> None:
    if not resend.api_key:
        logger.error("RESEND_API_KEY not set. Aborting digest.")
        return

    db = SessionLocal()
    try:
        subscribers = db.execute(text(
            "SELECT email, zip_code FROM subscribers ORDER BY zip_code"
        )).fetchall()

        if not subscribers:
            logger.info("No subscribers. Nothing to send.")
            return

        logger.info("Sending weekly digest to %d subscribers", len(subscribers))
        sent = failed = 0

        for email, zip_code in subscribers:
            try:
                data = _fetch_digest_data(db, zip_code)
                if not data:
                    logger.warning("No score history for zip %s, skipping %s", zip_code, email)
                    continue

                resend.Emails.send({
                    "from":    "PulseCities <alerts@pulsecities.com>",
                    "to":      [email],
                    "subject": _subject(data),
                    "html":    _build_email_html(data),
                })
                logger.info("Digest sent: %s -> %s", zip_code, email)
                sent += 1
            except Exception:
                logger.exception("Failed to send digest to %s for zip %s", email, zip_code)
                failed += 1

        logger.info("Digest complete. Sent: %d, Failed: %d", sent, failed)
    finally:
        db.close()


if __name__ == "__main__":
    run_digest()
