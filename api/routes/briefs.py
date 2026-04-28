"""
Shareable evidence brief pages — summarize public-record signals for a ZIP
code or operator cluster in a clean, printable format.

GET /brief/zip/{zip}        — displacement-pressure summary for a ZIP code
GET /brief/operator/{slug}  — acquisition summary for an operator cluster

Designed to be shared by link, printed, or saved as PDF by journalists,
tenants, advocates, or researchers.  No LLM calls.  No PDF generation.
Reuses existing query patterns from neighborhoods.py and operators.py.
"""

import html as _html
import logging
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(tags=["briefs"])

_BRIEF_TTL = 3600
_zip_brief_cache: dict[str, tuple[str, float]] = {}
_op_brief_cache: dict[str, tuple[str, float]] = {}

_ANALYSIS_WINDOW_DAYS = 548


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%B %-d, %Y at %H:%M UTC")


def _score_tier(score: float) -> tuple[str, str]:
    """Return (label, hex_color) for a displacement score."""
    if score >= 76:
        return "Critical", "#ef4444"
    if score >= 56:
        return "High", "#f97316"
    if score >= 34:
        return "Moderate", "#eab308"
    return "Low", "#64748b"


def _idx_color(v: float) -> str:
    if v >= 70:
        return "#f97316"
    if v >= 45:
        return "#eab308"
    return "rgba(148,163,184,0.45)"


_CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#0f172a;--border:rgba(148,163,184,.1);--text:#f1f5f9;--muted:rgba(148,163,184,.65);--faint:rgba(148,163,184,.35);--accent:#f97316}
body{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;line-height:1.6;overflow-x:hidden}
a{color:inherit;text-decoration:none}
nav{border-bottom:1px solid var(--border);padding:12px 0}
.nav-inner{max-width:740px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between}
.nav-links a{font-size:.78rem;color:var(--muted);margin-left:16px;transition:color .15s}
.nav-links a:hover{color:var(--text)}
.container{max-width:740px;margin:0 auto;padding:32px 20px 80px}
.brief-label{font-size:.66rem;font-weight:600;letter-spacing:.12em;text-transform:uppercase;color:var(--accent);margin-bottom:8px}
h1{font-size:1.42rem;font-weight:600;line-height:1.3;margin-bottom:4px}
.generated{font-size:.74rem;color:var(--faint);margin-bottom:28px}
.summary{font-size:.9rem;color:var(--muted);line-height:1.7;margin-bottom:32px;padding:16px 20px;border-left:2px solid var(--accent);background:rgba(249,115,22,.04)}
h2{font-size:.67rem;font-weight:600;letter-spacing:.09em;text-transform:uppercase;color:var(--faint);margin-bottom:10px}
section{margin-bottom:32px}
table{width:100%;border-collapse:collapse;margin-bottom:8px}
th{font-size:.63rem;font-weight:500;text-transform:uppercase;letter-spacing:.06em;color:var(--faint);padding:6px 0;border-bottom:1px solid var(--border);text-align:left}
th.tr{text-align:right}
td{padding:10px 0;border-bottom:1px solid rgba(148,163,184,.06);vertical-align:top;font-size:.86rem}
.td-r{text-align:right;font-family:'JetBrains Mono',monospace;white-space:nowrap}
.td-label{color:var(--text)}
.td-sub{display:block;font-size:.7rem;color:var(--faint);margin-top:2px}
.score-row{display:flex;align-items:baseline;gap:6px;flex-wrap:wrap;padding:16px 20px;border:1px solid var(--border);border-radius:6px;background:rgba(255,255,255,.02);margin-bottom:20px}
.score-big{font-size:2.4rem;font-weight:700;font-family:'JetBrains Mono',monospace;line-height:1}
.score-denom{font-size:.9rem;color:var(--muted);font-family:'JetBrains Mono',monospace;align-self:flex-end;padding-bottom:3px}
.score-tier-lbl{font-size:.63rem;font-weight:600;letter-spacing:.09em;text-transform:uppercase;align-self:flex-end;padding-bottom:5px;margin-left:10px}
.metrics{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:10px;margin-bottom:8px}
.metric{padding:12px 16px;border:1px solid var(--border);border-radius:6px;background:rgba(255,255,255,.02)}
.metric-val{font-family:'JetBrains Mono',monospace;font-size:1.25rem;font-weight:600;margin-bottom:2px}
.metric-key{font-size:.72rem;color:var(--muted)}
.entity-list{list-style:none;padding:0;margin:0;columns:2;column-gap:24px}
.entity-list li{font-family:'JetBrains Mono',monospace;font-size:.73rem;color:var(--muted);padding:3px 0;break-inside:avoid}
.source-note{font-size:.77rem;color:var(--faint);line-height:1.65;margin-bottom:8px}
.disclaimer{font-size:.77rem;color:var(--faint);line-height:1.65;padding:14px 18px;border:1px solid var(--border);border-radius:6px;margin-bottom:24px;margin-top:24px}
.btn-primary{display:inline-flex;align-items:center;gap:6px;padding:10px 20px;background:var(--accent);color:#fff;border-radius:6px;font-size:.85rem;font-weight:500;text-decoration:none;transition:opacity .15s}
.btn-primary:hover{opacity:.88}
.btn-ghost{display:inline-flex;align-items:center;gap:6px;padding:10px 20px;background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:6px;font-size:.85rem;text-decoration:none;transition:color .15s,border-color .15s;cursor:pointer;font-family:inherit}
.btn-ghost:hover{color:var(--text);border-color:rgba(148,163,184,.3)}
.cta-row{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
footer{border-top:1px solid var(--border);padding:24px 20px;text-align:center}
.footer-links{max-width:740px;margin:0 auto;display:flex;justify-content:center;gap:24px;flex-wrap:wrap}
.footer-links a{font-size:.75rem;color:var(--faint)}
.footer-links a:hover{color:var(--muted)}
@media(max-width:600px){h1{font-size:1.2rem}.score-big{font-size:2rem}.container{padding:24px 16px 60px}.metrics{grid-template-columns:1fr 1fr}.entity-list{columns:1}.cta-row{flex-direction:column;align-items:flex-start}}
@media print{
  nav,footer,.cta-row{display:none!important}
  body{background:#fff;color:#1e293b}
  :root{--bg:#fff;--text:#1e293b;--muted:#475569;--faint:#94a3b8;--border:#e2e8f0;--accent:#ea580c}
  .summary{border-left-color:#ea580c;background:#fff7ed}
  .score-row,.metric{border-color:#e2e8f0;background:#f8fafc}
  .container{padding:20px 0 40px}
  a[href]{color:#1e293b}
  .disclaimer{border-color:#e2e8f0}
}
"""

_FONTS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600&family=JetBrains+Mono:wght@400&display=swap" rel="stylesheet">'
)


def _nav_html() -> str:
    return """<nav><div class="nav-inner">
  <a href="/" style="display:flex;align-items:center;gap:8px;">
    <svg width="20" height="20" viewBox="0 0 32 32" fill="none" aria-hidden="true"><rect width="32" height="32" rx="6" fill="#1a1a2e"/><polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none" stroke="#f97316" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
    <span style="font-size:.85rem;color:rgba(148,163,184,.55);">PulseCities</span>
  </a>
  <div class="nav-links"><a href="/map">Map</a><a href="/methodology">Methodology</a><a href="/about">About</a></div>
</div></nav>"""


def _footer_html() -> str:
    return """<footer><div class="footer-links">
  <a href="/">Home</a><a href="/map">Map</a><a href="/methodology">Methodology</a><a href="/about">About</a><a href="mailto:michaelespin15@gmail.com">Contact</a>
</div></footer>"""


def _copy_js(url: str) -> str:
    return f"""<script>
function copyBrief() {{
  var url = '{_html.escape(url)}';
  var btn = document.getElementById('copy-btn');
  function done() {{ btn.textContent = 'Copied!'; setTimeout(function() {{ btn.textContent = 'Copy link'; }}, 2000); }}
  if (navigator.clipboard && navigator.clipboard.writeText) {{
    navigator.clipboard.writeText(url).then(done).catch(function() {{ fallback(url, done); }});
  }} else {{ fallback(url, done); }}
}}
function fallback(url, done) {{
  var el = document.createElement('textarea');
  el.value = url; el.style.cssText = 'position:fixed;opacity:0';
  document.body.appendChild(el); el.select(); el.setSelectionRange(0,99999);
  try {{ document.execCommand('copy'); done(); }} catch(e) {{}}
  document.body.removeChild(el);
}}
</script>"""


# ---------------------------------------------------------------------------
# ZIP code evidence brief
# ---------------------------------------------------------------------------

@router.get("/brief/zip/{zip_code}", include_in_schema=False)
def zip_brief(zip_code: str, db: Session = Depends(get_db)):
    if not (len(zip_code) == 5 and zip_code.isdigit()):
        return Response(status_code=400, content="Invalid ZIP code")

    cached = _zip_brief_cache.get(zip_code)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    from api.routes.neighborhoods import _borough_from_zip, _build_summary, _fetch_raw_counts

    row = db.execute(text("""
        SELECT n.name, ds.score, ds.signal_breakdown, ds.cache_generated_at
        FROM neighborhoods n
        LEFT JOIN displacement_scores ds ON n.zip_code = ds.zip_code
        WHERE n.zip_code = :zip
    """), {"zip": zip_code}).fetchone()

    if not row:
        return Response(status_code=404, content="ZIP code not found")

    name       = row.name or zip_code
    score      = float(row.score) if row.score is not None else None
    borough    = _borough_from_zip(zip_code) or "New York City"
    breakdown  = dict(row.signal_breakdown) if row.signal_breakdown else {}
    last_upd   = row.cache_generated_at.date().isoformat() if row.cache_generated_at else None

    raw_counts = _fetch_raw_counts(db, zip_code)

    hpd_row = db.execute(text("""
        SELECT COUNT(*) FROM violations_raw
        WHERE zip_code = :zip
          AND violation_class IN ('B', 'C')
          AND inspection_date >= CURRENT_DATE - INTERVAL '90 days'
    """), {"zip": zip_code}).fetchone()
    raw_hpd = int(hpd_row[0] or 0)

    summary = _build_summary(score, breakdown, raw_counts)
    page_html = _build_zip_brief(
        zip_code, name, borough, score, breakdown, raw_counts, raw_hpd, summary, last_upd
    )
    _zip_brief_cache[zip_code] = (page_html, time.monotonic() + _BRIEF_TTL)
    return HTMLResponse(page_html)


def _build_zip_brief(
    zip_code: str,
    name: str,
    borough: str,
    score: float | None,
    breakdown: dict,
    raw_counts: dict,
    raw_hpd: int,
    summary: str | None,
    last_updated: str | None,
) -> str:
    e = _html.escape
    ts     = _now_str()
    title  = f"Evidence brief for {zip_code} | PulseCities"
    brief_url = f"https://pulsecities.com/brief/zip/{zip_code}"

    if score is not None:
        tier_label, tier_color = _score_tier(score)
        score_block = (
            f'<div class="score-row">'
            f'<span class="score-big" style="color:{tier_color};">{score:.1f}</span>'
            f'<span class="score-denom">/100</span>'
            f'<span class="score-tier-lbl" style="color:{tier_color};">{tier_label.upper()} DISPLACEMENT PRESSURE</span>'
            f'</div>'
        )
    else:
        score_block = '<p style="color:var(--faint);font-size:.88rem;margin-bottom:20px;">Score data not yet available for this ZIP code.</p>'

    _signals = [
        ("llc_acquisitions", "LLC property acquisitions",               "past 365 days",     raw_counts.get("llc_acquisitions", 0)),
        ("permits",          "Building permits (residential, 3+ units)", "past 365 days",     raw_counts.get("permits", 0)),
        ("evictions",        "Residential eviction filings",             "past 365 days",     raw_counts.get("evictions", 0)),
        ("hpd_violations",   "HPD violations (Class B and C)",           "past 90 days",      raw_hpd),
        ("complaint_rate",   "311 housing complaints",                   "past 365 days",     raw_counts.get("complaint_rate", 0)),
        ("rs_unit_loss",     "Rent-stabilized unit loss",                "annual comparison", None),
    ]

    rows_html = ""
    for key, label, window, count in _signals:
        idx   = breakdown.get(key)
        idx_s = f"{idx:.1f}" if idx is not None else "N/A"
        i_col = _idx_color(float(idx) if idx is not None else 0.0)
        if key == "rs_unit_loss":
            cnt_s = "No annual loss recorded in current data"
            c_col = "rgba(148,163,184,0.38)"
        elif count == 0:
            cnt_s = "0"
            c_col = "rgba(148,163,184,0.38)"
        else:
            cnt_s = f"{count:,}"
            c_col = "var(--text)"
        rows_html += (
            f'<tr>'
            f'<td class="td-label">{e(label)}<span class="td-sub">{e(window)}</span></td>'
            f'<td class="td-r" style="color:{c_col};">{cnt_s}</td>'
            f'<td class="td-r" style="color:{i_col};">{idx_s}</td>'
            f'</tr>'
        )

    upd_note = f" Last updated {last_updated}." if last_updated else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="robots" content="noindex">
<title>{e(title)}</title>
<meta property="og:title" content="{e(title)}">
<meta property="og:description" content="Public-record displacement-pressure signals for {e(zip_code)} ({e(name)}), {e(borough)}.">
<meta property="og:url" content="{e(brief_url)}">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
{_FONTS}
<style>{_CSS}</style>
</head>
<body>
{_nav_html()}
<main><div class="container">

  <p class="brief-label">Evidence Brief</p>
  <h1>Evidence brief for {e(zip_code)}</h1>
  <p class="generated">Generated {ts}.{e(upd_note)} Data from NYC public records.</p>

  <p class="summary">{e(summary) if summary else "PulseCities summarizes public records associated with displacement pressure in this area, including permits, complaints, eviction filings, ownership transfers, HPD violations, and rent-stabilized housing records."}</p>

  <section>
    <h2>Displacement pressure score</h2>
    {score_block}
    <p class="source-note">Score is a 0 to 100 index normalized across 178 NYC ZIP codes. Higher values indicate more elevated displacement-pressure signals relative to citywide averages, not an absolute measure of risk.</p>
  </section>

  <section>
    <h2>Signal breakdown</h2>
    <p style="font-size:.82rem;color:var(--muted);margin-bottom:14px;">{e(name)}, {e(borough)}. All counts from NYC public records.</p>
    <table>
      <thead>
        <tr>
          <th>Signal</th>
          <th class="tr">Count</th>
          <th class="tr">Index (0-100)</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
    <p class="source-note">Index values are PERCENT_RANK scores computed across all 178 tracked NYC ZIP codes. Count windows noted per signal. Rent-stabilized unit loss uses annual DHCR data; counts are point-in-time, not windowed.</p>
  </section>

  <section>
    <h2>Sources</h2>
    <p class="source-note">
      LLC property acquisitions: ACRIS deed records (NYC Department of Finance).<br>
      Eviction filings: NYC housing court data via NYC Open Data.<br>
      Building permits: NYC Department of Buildings (DOB).<br>
      HPD violations: NYC Department of Housing Preservation and Development.<br>
      311 complaints: NYC 311 Service Requests.<br>
      Rent-stabilized units: DHCR via NYCDB.
    </p>
    <p class="source-note" style="margin-top:8px;">
      <a href="/methodology" style="color:var(--accent);">Read the full methodology</a> for signal definitions, scoring logic, and data limitations.
    </p>
  </section>

  <p class="disclaimer">This brief summarizes public-record signals. It is not an allegation of wrongdoing.</p>

  <div class="cta-row">
    <a href="/neighborhood/{e(zip_code)}" class="btn-primary">View {e(zip_code)} neighborhood page</a>
    <a href="/map?q={e(zip_code)}" class="btn-ghost">Open on map</a>
    <button class="btn-ghost" id="copy-btn" onclick="copyBrief()">Copy link</button>
  </div>

</div></main>
{_footer_html()}
{_copy_js(brief_url)}
</body>
</html>"""


# ---------------------------------------------------------------------------
# Operator evidence brief
# ---------------------------------------------------------------------------

@router.get("/brief/operator/{slug}", include_in_schema=False)
def operator_brief(slug: str, db: Session = Depends(get_db)):
    if not re.match(r"^[a-z0-9-]+$", slug):
        return Response(status_code=400, content="Invalid slug")

    from api.routes.operators import OPERATOR_NOISE_ROOTS, OPERATOR_NOISE_SLUGS

    if slug in OPERATOR_NOISE_SLUGS:
        return Response(status_code=404)

    cached = _op_brief_cache.get(slug)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    op_row = db.execute(
        text("SELECT * FROM operators WHERE slug = :slug"),
        {"slug": slug},
    ).fetchone()
    if op_row is None:
        return Response(status_code=404, content="Operator not found")

    if op_row.operator_root in OPERATOR_NOISE_ROOTS:
        return Response(status_code=404)

    operator_id   = op_row.id
    operator_root = op_row.operator_root
    llc_names     = op_row.llc_entities or []

    # BBL list
    bbl_rows  = db.execute(
        text("SELECT bbl FROM operator_parcels WHERE operator_id = :oid"),
        {"oid": operator_id},
    ).fetchall()
    bbl_list = [r.bbl for r in bbl_rows]

    # Top 10 properties by displacement score
    prop_rows = db.execute(text("""
        SELECT op.bbl, p.address, p.zip_code, ds.score
        FROM operator_parcels op
        JOIN parcels p ON p.bbl = op.bbl
        LEFT JOIN displacement_scores ds ON ds.zip_code = p.zip_code
        WHERE op.operator_id = :oid
        ORDER BY ds.score DESC NULLS LAST
        LIMIT 10
    """), {"oid": operator_id}).fetchall()

    # Eviction-then-buy count
    etb_count = 0
    if bbl_list and llc_names:
        etb_row = db.execute(text("""
            SELECT COUNT(DISTINCT (e.bbl, e.executed_date))
            FROM evictions_raw e
            JOIN ownership_raw o
                ON o.bbl = e.bbl
               AND o.party_type = '2'
               AND o.party_name_normalized = ANY(:llc_names)
               AND o.doc_date > e.executed_date
               AND o.doc_date <= e.executed_date + INTERVAL '365 days'
        """), {"llc_names": llc_names}).fetchone()
        etb_count = int(etb_row[0] or 0)

    # Recent acquisitions (last 10)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=_ANALYSIS_WINDOW_DAYS)).date()
    acq_rows = db.execute(text("""
        SELECT o.bbl, p.address, p.zip_code, o.party_name_normalized AS buyer,
               o.doc_date, o.doc_amount
        FROM ownership_raw o
        JOIN parcels p ON p.bbl = o.bbl
        WHERE o.party_type = '2'
          AND o.party_name_normalized = ANY(:names)
          AND o.doc_date >= :cutoff
        ORDER BY o.doc_date DESC NULLS LAST
        LIMIT 10
    """), {"names": llc_names, "cutoff": cutoff}).fetchall()

    # Acquisition timeline by year
    tl_rows = db.execute(text("""
        SELECT EXTRACT(YEAR FROM o.doc_date)::int AS yr, COUNT(*) AS cnt
        FROM ownership_raw o
        WHERE o.party_type = '2'
          AND o.party_name_normalized = ANY(:names)
          AND o.doc_date IS NOT NULL
        GROUP BY yr
        ORDER BY yr
    """), {"names": llc_names}).fetchall() if llc_names else []

    # ZIP count
    zip_count = op_row.borough_spread or 0
    if not zip_count and bbl_list:
        zip_row = db.execute(text("""
            SELECT COUNT(DISTINCT p.zip_code)
            FROM operator_parcels op
            JOIN parcels p ON p.bbl = op.bbl
            WHERE op.operator_id = :oid AND p.zip_code IS NOT NULL
        """), {"oid": operator_id}).fetchone()
        zip_count = int(zip_row[0] or 0)

    page_html = _build_operator_brief(
        operator_root   = operator_root,
        slug            = slug,
        display_name    = op_row.display_name,
        llc_entities    = llc_names,
        total_properties= op_row.total_properties or 0,
        total_acqs      = op_row.total_acquisitions or 0,
        borough_spread  = op_row.borough_spread,
        highest_score   = float(op_row.highest_displacement_score) if op_row.highest_displacement_score else None,
        prop_rows       = prop_rows,
        acq_rows        = acq_rows,
        tl_rows         = tl_rows,
        etb_count       = etb_count,
        zip_count       = zip_count,
    )
    _op_brief_cache[slug] = (page_html, time.monotonic() + _BRIEF_TTL)
    return HTMLResponse(page_html)


def _build_operator_brief(
    operator_root: str,
    slug: str,
    display_name: str,
    llc_entities: list,
    total_properties: int,
    total_acqs: int,
    borough_spread: int | None,
    highest_score: float | None,
    prop_rows: list,
    acq_rows: list,
    tl_rows: list,
    etb_count: int,
    zip_count: int,
) -> str:
    e    = _html.escape
    ts   = _now_str()
    name = display_name or operator_root
    title = f"Evidence brief for {name} | PulseCities"
    brief_url = f"https://pulsecities.com/brief/operator/{slug}"
    op_url    = f"/operator/{slug}"

    # Key metrics grid
    metrics_html = ""
    for val, label in [
        (f"{total_acqs:,}",            "Acquisitions on record"),
        (f"{len(llc_entities):,}",     "LLC entities"),
        (f"{total_properties:,}",      "Properties"),
        (f"{zip_count:,}" if zip_count else "N/A", "ZIP codes"),
    ]:
        metrics_html += (
            f'<div class="metric">'
            f'<div class="metric-val">{e(str(val))}</div>'
            f'<div class="metric-key">{e(label)}</div>'
            f'</div>'
        )

    # Top properties table
    prop_html = ""
    for r in prop_rows:
        addr  = e((r.address or f"BBL {r.bbl}").title())
        zip_  = e(r.zip_code or "")
        score = f"{r.score:.1f}" if r.score is not None else "N/A"
        sc_color = _idx_color(float(r.score) if r.score is not None else 0.0)
        prop_html += (
            f'<tr>'
            f'<td class="td-label"><a href="/property/{e(r.bbl)}" style="color:var(--text);">{addr}</a></td>'
            f'<td class="td-r">{zip_}</td>'
            f'<td class="td-r" style="color:{sc_color};">{score}</td>'
            f'</tr>'
        )
    if not prop_html:
        prop_html = '<tr><td colspan="3" style="color:var(--faint);font-size:.82rem;">No properties on record.</td></tr>'

    # Recent acquisitions table
    any_amount = any(r.doc_amount for r in acq_rows)
    acq_html   = ""
    for r in acq_rows:
        addr   = e((r.address or f"BBL {r.bbl}").title())
        zip_   = e(r.zip_code or "")
        buyer  = e(r.buyer or "")
        dt     = e(r.doc_date.strftime("%Y-%m-%d") if r.doc_date else "")
        acq_html += (
            f'<tr>'
            f'<td class="td-label">{addr}</td>'
            f'<td class="td-r" style="font-family:monospace;">{zip_}</td>'
            f'<td class="td-label" style="font-size:.78rem;">{buyer}</td>'
            f'<td class="td-r">{dt}</td>'
            f'</tr>'
        )
    if not acq_html:
        acq_html = '<tr><td colspan="4" style="color:var(--faint);font-size:.82rem;">No acquisitions found in the 18-month window.</td></tr>'
    amount_note = '' if any_amount else '<p class="source-note" style="margin-top:8px;">Transaction amounts are not reported in public records for this cluster.</p>'

    # Acquisition timeline
    tl_html = ""
    if tl_rows:
        for r in tl_rows:
            yr  = r.yr
            cnt = r.cnt
            tl_html += f'<tr><td class="td-label">{yr}</td><td class="td-r">{int(cnt):,}</td></tr>'
    else:
        tl_html = '<tr><td colspan="2" style="color:var(--faint);font-size:.82rem;">Timeline data not available.</td></tr>'

    # Eviction signal note
    if etb_count > 0:
        etb_html = (
            f'<section>'
            f'<h2>Eviction filings before acquisition</h2>'
            f'<p style="font-size:.86rem;color:var(--muted);line-height:1.65;">'
            f'Public records show {etb_count} '
            f'{"instance" if etb_count == 1 else "instances"} where an eviction filing at a property '
            f'was followed by an acquisition from an entity in this cluster within 365 days. '
            f'Sourced from NYC housing court records and ACRIS deed records.'
            f'</p>'
            f'</section>'
        )
    else:
        etb_html = ""

    # LLC entity list
    entity_items = "".join(f'<li>{e(name)}</li>' for name in sorted(llc_entities))
    if not entity_items:
        entity_items = '<li style="color:var(--faint);">No LLC entities on record.</li>'

    # Highest neighborhood score note
    score_note = ""
    if highest_score is not None:
        hs_label, hs_color = _score_tier(highest_score)
        score_note = (
            f'<p style="font-size:.82rem;color:var(--muted);margin-top:10px;">'
            f'Highest neighborhood displacement-pressure score in this portfolio: '
            f'<span style="color:{hs_color};font-family:\'JetBrains Mono\',monospace;font-weight:600;">'
            f'{highest_score:.1f}/100 ({hs_label})</span>.</p>'
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="robots" content="noindex">
<title>{e(title)}</title>
<meta property="og:title" content="{e(title)}">
<meta property="og:description" content="Public-record acquisition summary for {e(name)} in NYC. Sourced from ACRIS deed records.">
<meta property="og:url" content="{e(brief_url)}">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
{_FONTS}
<style>{_CSS}</style>
</head>
<body>
{_nav_html()}
<main><div class="container">

  <p class="brief-label">Evidence Brief</p>
  <h1>Evidence brief for {e(name)}</h1>
  <p class="generated">Generated {ts}. Data from NYC public records.</p>

  <p class="summary">PulseCities groups public deed records by operator naming patterns and acquisition activity. This brief summarizes the records currently associated with this cluster. Each operator is shown as the public records describe them.</p>

  <section>
    <h2>Key metrics</h2>
    <div class="metrics">{metrics_html}</div>
    {score_note}
  </section>

  <section>
    <h2>LLC entities on record</h2>
    <p style="font-size:.82rem;color:var(--muted);margin-bottom:12px;">The following LLC names appear as grantees in ACRIS deed records and are grouped under this cluster by naming pattern.</p>
    <ul class="entity-list">{entity_items}</ul>
  </section>

  <section>
    <h2>Portfolio sample</h2>
    <p style="font-size:.82rem;color:var(--muted);margin-bottom:12px;">Up to 10 properties, sorted by neighborhood displacement score (highest first).</p>
    <table>
      <thead>
        <tr>
          <th>Address</th>
          <th class="tr">ZIP</th>
          <th class="tr">Neighborhood score</th>
        </tr>
      </thead>
      <tbody>{prop_html}</tbody>
    </table>
    <p class="source-note">Neighborhood scores reflect ZIP-level displacement pressure, not a property-level measure. Source: PulseCities scoring model.</p>
  </section>

  {etb_html}

  <section>
    <h2>Acquisition timeline</h2>
    <p style="font-size:.82rem;color:var(--muted);margin-bottom:12px;">Annual counts of deed transfers in ACRIS attributed to entities in this cluster.</p>
    <table>
      <thead>
        <tr><th>Year</th><th class="tr">Acquisitions</th></tr>
      </thead>
      <tbody>{tl_html}</tbody>
    </table>
  </section>

  <section>
    <h2>Recent acquisitions (up to 10)</h2>
    <p style="font-size:.82rem;color:var(--muted);margin-bottom:12px;">Most recent deed records in the 18-month window.</p>
    <div style="overflow-x:auto;">
    <table style="min-width:500px;">
      <thead>
        <tr>
          <th>Address</th>
          <th class="tr">ZIP</th>
          <th>Buyer entity</th>
          <th class="tr">Date</th>
        </tr>
      </thead>
      <tbody>{acq_html}</tbody>
    </table>
    </div>
    {amount_note}
  </section>

  <section>
    <h2>Sources</h2>
    <p class="source-note">
      Acquisition records: ACRIS (NYC Department of Finance public deed records).<br>
      Eviction records: NYC housing court data via NYC Open Data.<br>
      Neighborhood displacement scores: PulseCities scoring model, derived from six public-record signals.
    </p>
    <p class="source-note" style="margin-top:8px;">
      <a href="/methodology" style="color:var(--accent);">Read the full methodology</a> for cluster identification logic, signal definitions, and data limitations.
    </p>
  </section>

  <p class="disclaimer">This brief summarizes public-record signals. It is not an allegation of wrongdoing.</p>

  <div class="cta-row">
    <a href="{e(op_url)}" class="btn-primary">View full operator profile</a>
    <button class="btn-ghost" id="copy-btn" onclick="copyBrief()">Copy link</button>
  </div>

</div></main>
{_footer_html()}
{_copy_js(brief_url)}
</body>
</html>"""
