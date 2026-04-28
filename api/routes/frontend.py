"""
Server-side rendered pages for neighborhood and operator deep links, plus the
operators directory.

GET /neighborhood/{zip_code}  — full SSR civic intelligence card (score, signals, FAQ, CTA)
GET /operator/{root}          — per-operator OG/meta injection into operator.html
GET /operators                — server-side rendered directory of all tracked operators
"""

import html as _html
import json
import logging
import re
import time
from datetime import date
from pathlib import Path

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse, HTMLResponse, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(tags=["frontend"])

_FRONTEND = Path(__file__).parent.parent.parent / "frontend"
_app_html: str | None = None
_operator_html: str | None = None
_page_cache: dict[str, tuple[str, float]] = {}   # zip -> (html, expires_at)
_op_page_cache: dict[str, tuple[str, float]] = {}  # root -> (html, expires_at)
_prop_page_cache: dict[str, tuple[str, float]] = {}  # bbl -> (html, expires_at)
_PAGE_TTL = 3600


def _template() -> str:
    global _app_html
    if _app_html is None:
        _app_html = (_FRONTEND / "app.html").read_text()
    return _app_html


def _operator_template() -> str:
    global _operator_html
    if _operator_html is None:
        _operator_html = (_FRONTEND / "operator.html").read_text()
    return _operator_html


def _set_meta(html: str, attr: str, attr_val: str, new_content: str) -> str:
    """Replace content="..." on the meta tag identified by attr=attr_val.

    Matches by attribute name and value rather than by the current content
    string, so the replacement survives changes to the default meta values
    in app.html without needing to be updated here.
    """
    pattern = rf'<meta\b[^>]*\b{re.escape(attr)}="{re.escape(attr_val)}"[^>]*>'

    def _swap(m: re.Match) -> str:
        return re.sub(r'content="[^"]*"', f'content="{new_content}"', m.group(0), count=1)

    return re.sub(pattern, _swap, html)


# ---------------------------------------------------------------------------
# Neighborhood page \u2014 SSR civic intelligence card
# ---------------------------------------------------------------------------

_FAQ_Q1 = "What does this displacement score mean?"
_FAQ_A1 = (
    "The score is a 0 to 100 index showing where multiple public-record displacement signals "
    "are elevated at the ZIP level. Each signal is normalized across all 178 NYC ZIP codes so "
    "dense areas are not scored by raw counts alone."
)
_FAQ_Q2 = "What public records are included?"
_FAQ_A2 = (
    "PulseCities uses NYC public records: DOB building permits, HPD housing violations, "
    "311 housing complaints, eviction filings, ACRIS property deed transfers, DHCR "
    "rent-stabilized housing data, and MapPLUTO residential unit counts."
)
_FAQ_Q3 = "Is this a prediction of eviction?"
_FAQ_A3 = (
    "No. PulseCities does not predict individual evictions and is not legal advice. "
    "The score shows neighborhood-level public-record indicators that may be worth reviewing."
)


def _tier_info(score: float) -> tuple[str, str]:
    """Returns (display_label, hex_color) for the score tier."""
    if score >= 76: return "Critical", "#ef4444"
    if score >= 56: return "High",     "#f97316"
    if score >= 34: return "Moderate", "#eab308"
    return "Low", "#64748b"


def _idx_color(v: float) -> str:
    if v >= 70: return "#f97316"
    if v >= 45: return "#eab308"
    return "rgba(148,163,184,0.55)"


def _build_neighborhood_page(
    zip_code: str,
    name: str,
    borough: str | None,
    score: float | None,
    breakdown: dict,
    raw_counts: dict,
    raw_hpd: int,
    summary: str | None,
    last_updated: str | None,
) -> str:
    e = _html.escape

    borough_disp = borough or "New York City"
    canonical    = f"https://pulsecities.com/neighborhood/{zip_code}"
    og_image     = f"https://pulsecities.com/og/{zip_code}.png"

    if last_updated:
        try:
            updated_disp = date.fromisoformat(last_updated).strftime("%B %-d, %Y")
        except ValueError:
            updated_disp = last_updated
    else:
        updated_disp = "recently"

    if score is not None:
        tier_label, tier_color = _tier_info(score)
        score_str    = f"{score:.1f}"
        page_title   = f"{name} ({zip_code}), {borough_disp} | Displacement Score {score_str}/100 | PulseCities"
        social_title = f"{name} ({zip_code}) | Displacement Score {score_str}/100 | PulseCities"
        meta_desc    = (
            f"{name} shows {tier_label.lower()} displacement-pressure signals based on NYC public records, "
            f"including LLC acquisitions, eviction filings, 311 complaints, HPD violations, "
            f"permits, and rent-stabilized housing data."
        )
    else:
        tier_label, tier_color = "Unknown", "#64748b"
        score_str    = "N/A"
        page_title   = f"{name} ({zip_code}), {borough_disp} | NYC Displacement Signals | PulseCities"
        social_title = page_title
        meta_desc    = (
            f"Track displacement-pressure signals in {name} ({zip_code}) from NYC public records: LLC acquisitions, "
            f"eviction filings, 311 complaints, HPD violations, permits, and rent-stabilized housing data."
        )

    # (breakdown_key, display_label, window_label, raw_count or None for dormant signals)
    _signals = [
        ("llc_acquisitions", "LLC property acquisitions",               "past 365 days",     raw_counts.get("llc_acquisitions", 0)),
        ("permits",          "Building permits (residential, 3+ units)", "past 365 days",     raw_counts.get("permits", 0)),
        ("evictions",        "Residential eviction filings",             "past 365 days",     raw_counts.get("evictions", 0)),
        ("hpd_violations",   "HPD violations (Class B+C)",               "past 90 days",      raw_hpd),
        ("complaint_rate",   "311 housing complaints",                   "past 365 days",     raw_counts.get("complaint_rate", 0)),
        ("rs_unit_loss",     "Rent-stabilized unit loss",                "annual comparison", None),
    ]

    rows_html = ""
    for key, label, window, count in _signals:
        idx   = breakdown.get(key)
        idx_s = f"{idx:.1f}" if idx is not None else "&mdash;"
        i_col = _idx_color(float(idx) if idx is not None else 0.0)
        if key == "rs_unit_loss":
            cnt_s = "No annual loss recorded in current data"
            c_col = "rgba(148,163,184,0.4)"
        elif count == 0:
            cnt_s = "0"
            c_col = "rgba(148,163,184,0.4)"
        else:
            cnt_s = f"{count:,}"
            c_col = "#f1f5f9"
        rows_html += (
            f'<tr>'
            f'<td class="sc">{e(label)}<span class="sw">{e(window)}</span></td>'
            f'<td class="sr" style="color:{c_col};">{cnt_s}</td>'
            f'<td class="si" style="color:{i_col};">{idx_s}</td>'
            f'</tr>'
        )

    score_block = (
        f'<div class="score-block">'
        f'<span class="score-num" style="color:{tier_color};">{score_str}</span>'
        f'<span class="score-denom">/100</span>'
        f'<span class="score-tier" style="color:{tier_color};">{tier_label.upper()} DISPLACEMENT PRESSURE</span>'
        f'</div>'
        if score is not None
        else '<div class="score-block"><p style="color:rgba(148,163,184,0.5);font-size:0.9rem;">Score data not yet available.</p></div>'
    )
    summary_html = f'<p class="summary">{e(summary)}</p>' if summary else ""

    dataset_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": f"Displacement Signals -- {name} ({zip_code}), {borough_disp}, NYC",
        "description": meta_desc,
        "url": canonical,
        "spatialCoverage": {
            "@type": "Place",
            "name": f"{name}, New York City",
            "address": {
                "@type": "PostalAddress",
                "postalCode": zip_code,
                "addressRegion": "NY",
                "addressCountry": "US",
            },
        },
        "creator": {"@type": "Person", "name": "Michael Espin", "url": "https://pulsecities.com"},
        "license": "https://creativecommons.org/licenses/by/4.0/",
        "isBasedOn": [
            "https://data.cityofnewyork.us/City-Government/ACRIS-Real-Property-Master/bnx9-e6tj",
            "https://data.cityofnewyork.us/Housing-Development/Evictions/6z8x-wfk4",
            "https://data.cityofnewyork.us/Housing-Development/Building-Permits/ipu4-2q9a",
        ],
        **({"dateModified": last_updated} if last_updated else {}),
        **({"variableMeasured": {
            "@type": "PropertyValue",
            "name": "Displacement Risk Score",
            "value": round(score, 1),
            "minValue": 0,
            "maxValue": 100,
        }} if score is not None else {}),
    }, indent=2)

    faq_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {"@type": "Question", "name": _FAQ_Q1, "acceptedAnswer": {"@type": "Answer", "text": _FAQ_A1}},
            {"@type": "Question", "name": _FAQ_Q2, "acceptedAnswer": {"@type": "Answer", "text": _FAQ_A2}},
            {"@type": "Question", "name": _FAQ_Q3, "acceptedAnswer": {"@type": "Answer", "text": _FAQ_A3}},
        ],
    }, indent=2)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="robots" content="index, follow">
<title>{e(page_title)}</title>
<meta name="description" content="{e(meta_desc)}">
<link rel="canonical" href="{e(canonical)}">
<meta property="og:title" content="{e(social_title)}">
<meta property="og:description" content="{e(meta_desc)}">
<meta property="og:url" content="{e(canonical)}">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="{e(og_image)}">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{e(social_title)}">
<meta name="twitter:description" content="{e(meta_desc)}">
<meta name="twitter:image" content="{e(og_image)}">
<script type="application/ld+json">{dataset_ld}</script>
<script type="application/ld+json">{faq_ld}</script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600&family=JetBrains+Mono:wght@400&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
:root{{--bg:#0f172a;--border:rgba(148,163,184,.1);--text:#f1f5f9;--muted:rgba(148,163,184,.65);--faint:rgba(148,163,184,.35);--accent:#f97316}}
body{{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;line-height:1.6;overflow-x:hidden}}
a{{color:inherit;text-decoration:none}}
nav{{border-bottom:1px solid var(--border);padding:12px 0}}
.nav-inner{{max-width:720px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between}}
.nav-links a{{font-size:.78rem;color:var(--muted);margin-left:16px;transition:color .15s}}
.nav-links a:hover{{color:var(--text)}}
.container{{max-width:720px;margin:0 auto;padding:32px 20px 80px}}
.breadcrumb{{font-size:.78rem;color:var(--muted);margin-bottom:20px}}
.breadcrumb a{{color:var(--muted)}}
.breadcrumb a:hover{{color:var(--text)}}
h1{{font-size:1.45rem;font-weight:600;line-height:1.3;margin-bottom:6px}}
.subline{{font-size:.82rem;color:var(--muted);margin-bottom:28px}}
.score-block{{display:flex;align-items:baseline;gap:6px;flex-wrap:wrap;padding:20px 24px;background:rgba(255,255,255,.02);border:1px solid var(--border);border-radius:8px;margin-bottom:20px}}
.score-num{{font-size:2.8rem;font-weight:700;font-family:'JetBrains Mono',monospace;line-height:1}}
.score-denom{{font-size:1rem;color:var(--muted);font-family:'JetBrains Mono',monospace;align-self:flex-end;padding-bottom:4px}}
.score-tier{{font-size:.68rem;font-weight:600;letter-spacing:.08em;align-self:flex-end;padding-bottom:6px;margin-left:8px}}
.summary{{font-size:.92rem;color:var(--muted);line-height:1.7;margin-bottom:32px}}
h2{{font-size:.68rem;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--faint);margin-bottom:8px}}
.section-sub{{font-size:.82rem;color:var(--muted);margin-bottom:14px}}
table{{width:100%;border-collapse:collapse;margin-bottom:12px}}
th{{font-size:.64rem;font-weight:500;text-transform:uppercase;letter-spacing:.06em;color:var(--faint);padding:6px 0;border-bottom:1px solid var(--border)}}
th:not(:first-child){{text-align:right}}
td{{padding:12px 0;border-bottom:1px solid rgba(148,163,184,.06);vertical-align:top}}
.sc{{font-size:.87rem}}
.sw{{display:block;font-size:.71rem;color:var(--faint);margin-top:2px}}
.sr,.si{{font-size:.87rem;font-family:'JetBrains Mono',monospace;text-align:right;white-space:nowrap}}
.data-note{{font-size:.74rem;color:var(--faint);margin-top:10px;margin-bottom:36px;line-height:1.55}}
.faq-list{{margin-bottom:36px}}
.faq-item{{padding:16px 0;border-bottom:1px solid var(--border)}}
.faq-item:first-child{{border-top:1px solid var(--border)}}
.faq-q{{font-size:.88rem;font-weight:600;margin-bottom:6px}}
.faq-a{{font-size:.83rem;color:var(--muted);line-height:1.65}}
.meth-link{{font-size:.82rem;margin-bottom:28px}}
.meth-link a{{color:var(--accent)}}
.meth-link a:hover{{text-decoration:underline}}
.cta-row{{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-top:4px}}
.btn-map{{display:inline-flex;align-items:center;gap:6px;padding:10px 20px;background:var(--accent);color:#fff;border-radius:6px;font-size:.85rem;font-weight:500;transition:opacity .15s}}
.btn-map:hover{{opacity:.88}}
.btn-copy{{display:inline-flex;align-items:center;gap:6px;padding:10px 20px;background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:6px;font-size:.85rem;cursor:pointer;font-family:inherit;transition:color .15s,border-color .15s}}
.btn-copy:hover{{color:var(--text);border-color:rgba(148,163,184,.3)}}
footer{{border-top:1px solid var(--border);padding:24px 20px calc(env(safe-area-inset-bottom) + 24px);text-align:center}}
.footer-links{{max-width:720px;margin:0 auto;display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
.footer-links a{{font-size:.75rem;color:var(--faint)}}
.footer-links a:hover{{color:var(--muted)}}
@media(max-width:600px){{h1{{font-size:1.2rem}}.score-num{{font-size:2.2rem}}.container{{padding:24px 16px 60px}}.cta-row{{flex-direction:column;align-items:flex-start}}}}
</style>
</head>
<body>
<nav><div class="nav-inner">
  <a href="/" style="display:flex;align-items:center;gap:8px;">
    <svg width="20" height="20" viewBox="0 0 32 32" fill="none" aria-hidden="true"><rect width="32" height="32" rx="6" fill="#1a1a2e"/><polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none" stroke="#f97316" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
    <span style="font-size:.85rem;color:rgba(148,163,184,.55);">PulseCities</span>
  </a>
  <div class="nav-links"><a href="/map">Map</a><a href="/methodology">Methodology</a><a href="/about">About</a></div>
</div></nav>
<main><div class="container">
  <p class="breadcrumb"><a href="/map">&#8592; Back to map</a></p>
  <h1>Displacement Signals | {e(name)} ({zip_code})</h1>
  <p class="subline">{e(borough_disp)}. Updated {e(updated_disp)}.</p>
  {score_block}
  {summary_html}
  <section style="margin-bottom:32px;">
    <h2>Signal breakdown</h2>
    <p class="section-sub">Public-record signals used in the neighborhood score.</p>
    <table>
      <thead><tr><th>Signal</th><th>Count</th><th>Index</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    <p class="data-note">All counts from NYC public records. Index values are normalized across 178 NYC ZIP codes. Data is refreshed nightly.</p>
  </section>
  <section style="margin-bottom:32px;">
    <h2>About this data</h2>
    <div class="faq-list">
      <div class="faq-item"><p class="faq-q">{e(_FAQ_Q1)}</p><p class="faq-a">{e(_FAQ_A1)}</p></div>
      <div class="faq-item"><p class="faq-q">{e(_FAQ_Q2)}</p><p class="faq-a">{e(_FAQ_A2)}</p></div>
      <div class="faq-item"><p class="faq-q">{e(_FAQ_Q3)}</p><p class="faq-a">{e(_FAQ_A3)}</p></div>
    </div>
  </section>
  <p class="meth-link"><a href="/methodology">Read the methodology &#8594;</a></p>
  <div class="cta-row">
    <a href="/map?q={zip_code}" class="btn-map">Open {zip_code} on the map &#8594;</a>
    <button class="btn-copy" id="copy-btn" onclick="copyLink()">Copy link</button>
  </div>
</div></main>
<footer><div class="footer-links">
  <a href="/">Home</a><a href="/map">Map</a><a href="/methodology">Methodology</a><a href="/about">About</a><a href="mailto:michaelespin15@gmail.com">Contact</a>
</div></footer>
<script>
function copyLink() {{
  var url = 'https://pulsecities.com/neighborhood/{zip_code}';
  var btn = document.getElementById('copy-btn');
  function onDone() {{
    btn.textContent = 'Copied!';
    setTimeout(function() {{ btn.textContent = 'Copy link'; }}, 2000);
  }}
  if (navigator.clipboard && navigator.clipboard.writeText) {{
    navigator.clipboard.writeText(url).then(onDone).catch(function() {{ fallback(url, onDone); }});
  }} else {{
    fallback(url, onDone);
  }}
}}
function fallback(url, onDone) {{
  var el = document.createElement('textarea');
  el.value = url;
  el.style.cssText = 'position:fixed;opacity:0';
  document.body.appendChild(el);
  el.select();
  el.setSelectionRange(0, 99999);
  try {{ document.execCommand('copy'); onDone(); }} catch(err) {{}}
  document.body.removeChild(el);
}}
</script>
</body>
</html>"""


@router.get("/map", include_in_schema=False)
def map_page():
    return FileResponse(_FRONTEND / "app.html")


@router.get("/methodology", include_in_schema=False)
def methodology_page():
    return FileResponse(_FRONTEND / "methodology.html")


@router.get("/about", include_in_schema=False)
def about_page():
    return FileResponse(_FRONTEND / "about.html")


@router.get("/neighborhood/{zip_code}", include_in_schema=False)
def neighborhood_page(zip_code: str, db: Session = Depends(get_db)):
    if not (len(zip_code) == 5 and zip_code.isdigit()):
        return HTMLResponse(_template(), status_code=200)

    cached = _page_cache.get(zip_code)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    row = db.execute(text("""
        SELECT n.name, ds.score, ds.signal_breakdown, ds.cache_generated_at
        FROM neighborhoods n
        LEFT JOIN displacement_scores ds ON n.zip_code = ds.zip_code
        WHERE n.zip_code = :zip
    """), {"zip": zip_code}).fetchone()

    if not row:
        return HTMLResponse(_template(), status_code=200)

    from api.routes.neighborhoods import _borough_from_zip, _build_summary, _fetch_raw_counts

    name         = row.name or zip_code
    score        = float(row.score) if row.score is not None else None
    borough      = _borough_from_zip(zip_code)
    breakdown    = dict(row.signal_breakdown) if row.signal_breakdown else {}
    last_updated = row.cache_generated_at.date().isoformat() if row.cache_generated_at else None

    raw_counts = _fetch_raw_counts(db, zip_code)

    hpd_row = db.execute(text("""
        SELECT COUNT(*) FROM violations_raw
        WHERE zip_code = :zip
          AND violation_class IN ('B', 'C')
          AND inspection_date >= CURRENT_DATE - INTERVAL '90 days'
    """), {"zip": zip_code}).fetchone()
    raw_hpd = int(hpd_row[0] or 0)

    summary = _build_summary(score, breakdown, raw_counts)

    page_html = _build_neighborhood_page(
        zip_code, name, borough, score, breakdown, raw_counts, raw_hpd, summary, last_updated,
    )
    _page_cache[zip_code] = (page_html, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page_html)


@router.get("/property/{bbl}", include_in_schema=False)
def property_page(bbl: str, db: Session = Depends(get_db)):
    clean = bbl.strip()
    if not clean.isdigit():
        return HTMLResponse(_template(), status_code=200)

    cached = _prop_page_cache.get(clean)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    row = db.execute(text("""
        SELECT p.address, p.zip_code,
               CASE
                   WHEN CAST(p.zip_code AS INTEGER) BETWEEN 10001 AND 10282 THEN 'Manhattan'
                   WHEN CAST(p.zip_code AS INTEGER) BETWEEN 10301 AND 10314 THEN 'Staten Island'
                   WHEN CAST(p.zip_code AS INTEGER) BETWEEN 10451 AND 10475 THEN 'Bronx'
                   WHEN CAST(p.zip_code AS INTEGER) BETWEEN 11201 AND 11239 THEN 'Brooklyn'
                   WHEN CAST(p.zip_code AS INTEGER) BETWEEN 11001 AND 11109 THEN 'Queens'
                   WHEN CAST(p.zip_code AS INTEGER) BETWEEN 11354 AND 11697 THEN 'Queens'
                   ELSE NULL
               END AS borough,
               ds.score
        FROM parcels p
        LEFT JOIN displacement_scores ds ON p.zip_code = ds.zip_code
        WHERE p.bbl = :bbl
        LIMIT 1
    """), {"bbl": clean}).fetchone()

    if not row:
        return HTMLResponse(_template(), status_code=200)

    address  = row.address.title() if row.address else clean
    zip_code = row.zip_code or ""
    borough  = row.borough or "NYC"
    score    = float(row.score) if row.score is not None else None

    url = f"https://pulsecities.com/property/{clean}"
    score_part = f" | Displacement Score {score:.1f}/100" if score is not None else ""
    title = f"{address}, {borough}{score_part} | PulseCities"

    if score is not None:
        desc = (
            f"{address} in {borough} shows {_risk_tier(score)} with a displacement score of "
            f"{score:.1f}/100. View eviction filings, construction permits, and ownership "
            f"transfers from NYC public records."
        )
    else:
        desc = (
            f"View displacement risk data for {address} in {borough}, NYC. "
            f"Eviction filings, construction permits, and ownership transfers from public records."
        )

    e_title = _html.escape(title, quote=True)
    e_desc  = _html.escape(desc,  quote=True)
    e_url   = _html.escape(url,   quote=True)

    og_image_url = (
        f"https://pulsecities.com/og/{zip_code}.png?d={date.today().strftime('%Y%m%d')}"
        if zip_code else "https://pulsecities.com/og-image.png"
    )
    e_og_image = _html.escape(og_image_url, quote=True)

    html = _template()
    html = html.replace('<title>Explore | PulseCities</title>', f'<title>{title}</title>', 1)
    html = html.replace('<link rel="canonical" href="https://pulsecities.com/map">', f'<link rel="canonical" href="{e_url}">', 1)
    html = _set_meta(html, "name",     "description",          e_desc)
    html = _set_meta(html, "property", "og:title",             e_title)
    html = _set_meta(html, "property", "og:description",       e_desc)
    html = _set_meta(html, "property", "og:url",               e_url)
    html = _set_meta(html, "property", "og:image",             e_og_image)
    html = _set_meta(html, "name",     "twitter:title",        e_title)
    html = _set_meta(html, "name",     "twitter:description",  e_desc)
    html = _set_meta(html, "name",     "twitter:image",        e_og_image)

    _prop_page_cache[clean] = (html, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(html)


@router.head("/operator/{root}", include_in_schema=False)
def operator_page_head(root: str):
    return Response(status_code=200)


@router.get("/operator/{root}", include_in_schema=False)
def operator_page(root: str, db: Session = Depends(get_db)):
    root_upper = root.upper().strip()
    if len(root_upper) < 2:
        return HTMLResponse(_operator_template())

    cached = _op_page_cache.get(root_upper)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    from api.routes.operators import OPERATOR_NOISE_ROOTS, OPERATOR_NOISE_SLUGS, _load_audit

    # Block finance/lender noise operators — they have DB entries but should not
    # render public profiles.  Return 404 so search engines don't index them.
    if root.lower() in OPERATOR_NOISE_SLUGS or root_upper in OPERATOR_NOISE_ROOTS:
        return Response(status_code=404)

    clusters = _load_audit()["clusters"]

    # The path param may be a slug (e.g. "mtek-nyc") or an operator_root (e.g. "MTEK").
    # Look up both directions so title/meta always use the canonical operator_root.
    op_row = db.execute(
        text(
            "SELECT operator_root, slug FROM operators "
            "WHERE operator_root = :root OR slug = :slug LIMIT 1"
        ),
        {"root": root_upper, "slug": root.lower()},
    ).fetchone()
    if op_row:
        root_upper   = op_row.operator_root  # canonical root for title/meta
        canonical_id = op_row.slug
    else:
        canonical_id = root_upper.lower()

    cluster = clusters.get(root_upper)
    url = f"https://pulsecities.com/operator/{canonical_id}"

    if cluster:
        entity_count = len(cluster.get("llc_entities") or [])
        acq_count    = cluster.get("total_acquisitions", 0)
        title = f"{root_upper} LLC Network | NYC Property Acquisitions | PulseCities"
        if acq_count and entity_count:
            desc = (
                f"{root_upper}: {acq_count} property "
                f"{'acquisition' if acq_count == 1 else 'acquisitions'} in NYC, "
                f"tracked across {entity_count} LLC "
                f"{'entity' if entity_count == 1 else 'entities'}. "
                "Sourced from ACRIS public deed records."
            )
        elif acq_count:
            desc = (
                f"{root_upper}: {acq_count} property "
                f"{'acquisition' if acq_count == 1 else 'acquisitions'} in NYC. "
                "Sourced from ACRIS public deed records."
            )
        else:
            desc = f"{root_upper} LLC network in NYC. Sourced from ACRIS public deed records."
    else:
        title = f"{root_upper} | NYC Operator Profile | PulseCities"
        desc  = f"{root_upper} LLC network in NYC. Sourced from ACRIS public deed records."

    e_title = _html.escape(title, quote=True)
    e_desc  = _html.escape(desc,  quote=True)
    e_url   = _html.escape(url,   quote=True)

    html = _operator_template()
    html = html.replace('<title>Operator Profile | PulseCities</title>', f'<title>{title}</title>', 1)
    html = html.replace(
        'content="LLC portfolio and affiliated operator network for a NYC acquisition cluster, sourced from ACRIS public records."',
        f'content="{e_desc}"',
    )
    html = html.replace(
        '<link rel="canonical" id="canonical-url" href="https://pulsecities.com/">',
        f'<link rel="canonical" id="canonical-url" href="{e_url}">',
        1,
    )

    og_block = (
        f'    <meta property="og:title" content="{e_title}">\n'
        f'    <meta property="og:description" content="{e_desc}">\n'
        f'    <meta property="og:url" content="{e_url}">\n'
        f'    <meta property="og:type" content="website">\n'
        f'    <meta property="og:site_name" content="PulseCities">\n'
        f'    <meta property="og:image" content="https://pulsecities.com/og-image.png">\n'
        f'    <meta property="og:image:width" content="1200">\n'
        f'    <meta property="og:image:height" content="630">\n'
        f'    <meta name="twitter:card" content="summary_large_image">\n'
        f'    <meta name="twitter:title" content="{e_title}">\n'
        f'    <meta name="twitter:description" content="{e_desc}">\n'
        f'    <meta name="twitter:image" content="https://pulsecities.com/og-image.png">'
    )
    html = html.replace('</head>', f'{og_block}\n</head>', 1)

    _op_page_cache[root_upper] = (html, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(html)


_SCRIPTS = Path(__file__).parent.parent.parent / "scripts"
_operators_cache: tuple[str, float] | None = None  # cleared on restart


@router.get("/operators", include_in_schema=False)
def operators_directory(db: Session = Depends(get_db)):
    global _operators_cache
    if _operators_cache and time.monotonic() < _operators_cache[1]:
        return HTMLResponse(_operators_cache[0])

    from api.routes.operators import OPERATOR_NOISE_ROOTS, _load_audit
    clusters = _load_audit()["clusters"]

    # Build root → slug map from the operators table so links use canonical slugs
    slug_rows = db.execute(text("SELECT operator_root, slug FROM operators")).fetchall()
    root_to_slug: dict[str, str] = {r.operator_root: r.slug for r in slug_rows}

    def _zip_to_borough(z: str) -> str | None:
        try:
            n = int(z)
        except ValueError:
            return None
        if 10001 <= n <= 10282: return "Manhattan"
        if 10301 <= n <= 10314: return "Staten Island"
        if 10451 <= n <= 10475: return "Bronx"
        if 11201 <= n <= 11239: return "Brooklyn"
        if (11001 <= n <= 11109) or (11354 <= n <= 11697): return "Queens"
        return None

    # Only include operators that:
    #   1. have LLC entities (measurable footprint)
    #   2. have a DB entry (valid profile page — no dead links)
    #   3. are not finance/lender noise (ASST-only activity, not DEED transfers)
    operators = sorted(
        [
            {"root": r, **c}
            for r, c in clusters.items()
            if len(c.get("llc_entities") or []) > 0
            and r in root_to_slug
            and r not in OPERATOR_NOISE_ROOTS
        ],
        key=lambda x: x.get("total_acquisitions", 0),
        reverse=True,
    )

    rows_html = ""
    list_items = []
    for i, op in enumerate(operators, 1):
        root = op["root"]
        entities = len(op.get("llc_entities") or [])
        acqs = op.get("total_acquisitions", 0)
        zips = op.get("zip_codes") or []
        boroughs = list(dict.fromkeys(b for z in zips if (b := _zip_to_borough(z))))
        extra = len(boroughs) - 2
        borough_str = ", ".join(boroughs[:2]) + (f" +{extra}" if extra > 0 else "")
        slug = root_to_slug[root]
        op_link = f"/operator/{_html.escape(slug)}"
        zip_count = len(zips)
        meta_parts = []
        if acqs:     meta_parts.append(f'{acqs} <span class="op-label-acq">acquisitions</span>')
        if entities: meta_parts.append(f'{entities} LLC{"s" if entities != 1 else ""}')
        if zip_count: meta_parts.append(f'{zip_count} ZIP code{"s" if zip_count != 1 else ""}')
        meta_line = f'<div class="op-meta" data-count="{acqs}">{" · ".join(meta_parts)}</div>' if meta_parts else ""
        geo_html  = f'<div class="op-geo">{_html.escape(borough_str)}</div>' if borough_str else ""
        rows_html += (
            f'<li class="op-row" onclick="location.href=\'{op_link}\'">'
            f'<a href="{op_link}">'
            f'<div class="op-rank">#{i}</div>'
            f'<div class="op-body">'
            f'<div class="op-name">{_html.escape(root)}</div>'
            f'{meta_line}'
            f'{geo_html}'
            f'<div class="op-cta">View profile →</div>'
            f'</div>'
            f'</a>'
            f'</li>\n'
        )
        list_items.append({
            "@type": "ListItem",
            "position": i,
            "name": f"{root} LLC Network",
            "url": f"https://pulsecities.com/operator/{slug}",
        })

    n_visible = len(operators)
    title = "NYC Operator Networks | PulseCities"
    desc = (
        f"{n_visible} public-record operator clusters with measurable NYC acquisition activity, "
        "sourced from ACRIS deed records."
    )
    jsonld = json.dumps({
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": "NYC Operator Networks",
        "description": desc,
        "url": "https://pulsecities.com/operators",
        "numberOfItems": n_visible,
        "itemListElement": list_items,
    }, indent=2)

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{_html.escape(title)}</title>
<meta name="description" content="{_html.escape(desc)}">
<link rel="canonical" href="https://pulsecities.com/operators">
<meta property="og:title" content="NYC Operator Networks | PulseCities">
<meta property="og:description" content="{_html.escape(desc)}">
<meta property="og:url" content="https://pulsecities.com/operators">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="NYC Operator Networks | PulseCities">
<meta name="twitter:description" content="{_html.escape(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<script type="application/ld+json">{jsonld}</script>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'DM Sans',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh}}
nav{{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}}
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px;border-top:1px solid rgba(148,163,184,0.08);margin-top:32px;font-size:12px;color:#64748b}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
.op-list{{list-style:none;padding:0;margin:0}}
.op-row{{border-bottom:1px solid rgba(148,163,184,0.07);cursor:pointer;}}
.op-row:hover{{background:rgba(148,163,184,0.04)}}
.op-row a{{display:flex;align-items:flex-start;gap:12px;padding:14px 0;text-decoration:none;color:inherit;}}
.op-rank{{font-family:'JetBrains Mono',monospace;font-size:0.68rem;color:rgba(148,163,184,0.28);min-width:24px;padding-top:3px;flex-shrink:0;}}
.op-body{{display:flex;flex-direction:column;gap:3px;}}
.op-name{{font-family:'JetBrains Mono',monospace;font-size:0.88rem;color:#e2e8f0;letter-spacing:0.04em;font-weight:500;}}
.op-row:hover .op-name{{color:#f97316;}}
.op-meta{{font-size:0.78rem;color:rgba(148,163,184,0.6);}}
.op-geo{{font-size:0.73rem;color:rgba(148,163,184,0.38);}}
.op-cta{{font-size:0.72rem;color:rgba(249,115,22,0.55);font-family:'JetBrains Mono',monospace;margin-top:2px;}}
.op-row:hover .op-cta{{color:rgba(249,115,22,0.85);}}
</style>
</head>
<body>
<nav>
  <div class="nav-inner">
    <a href="/" style="display:flex;align-items:center;gap:8px;color:#f1f5f9;">
      <svg width="22" height="22" viewBox="0 0 32 32" fill="none" aria-hidden="true"><rect width="32" height="32" rx="6" fill="#1a1a2e"/><polyline points="2,16 7,16 10,9 13,23 16,13 19,19 22,16 30,16" fill="none" stroke="#f97316" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      <span style="font-size:0.85rem;color:rgba(148,163,184,0.6);">PulseCities</span>
    </a>
    <div style="display:flex;align-items:center;gap:16px;">
      <a href="/map" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">Map</a>
      <a href="/methodology" style="font-size:0.78rem;color:rgba(148,163,184,0.5);" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='rgba(148,163,184,0.5)'">About</a>
      <button id="lang-toggle" style="font-family:'JetBrains Mono',monospace;font-size:0.72rem;color:rgba(148,163,184,0.5);background:none;border:none;cursor:pointer;padding:4px 2px;min-height:32px;">EN / ES</button>
    </div>
  </div>
</nav>
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:rgba(148,163,184,0.5);">&#8592; Home</a>
  </div>
  <h1 id="dir-heading" style="font-size:1.4rem;font-weight:600;margin-bottom:6px;">NYC Operator Networks</h1>
  <p id="dir-desc" style="font-size:0.82rem;color:rgba(148,163,184,0.55);margin-bottom:28px;line-height:1.6;">
    Ownership clusters identified in NYC deed records. Each groups LLCs by naming patterns and acquisition activity. Public records only.
  </p>
  <ul class="op-list">
{rows_html}  </ul>
</div>
<footer>
  <div class="footer-links">
    <a href="/" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Home</a>
    <a href="/map" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Map</a>
    <a href="/methodology" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">About</a>
    <a href="mailto:michaelespin15@gmail.com" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Contact</a>
  </div>
</footer>
<script>
(function() {{
  var lang = localStorage.getItem('pc-lang') || 'en';
  var i18n = {{
    en: {{
      heading: 'NYC Operator Networks',
      desc: 'Ownership clusters identified in NYC deed records. Each groups LLCs by naming patterns and acquisition activity. Public records only.',
      acq: 'acquisitions',
      toggle: 'EN / ES'
    }},
    es: {{
      heading: 'Redes de operadores de NYC',
      desc: 'Grupos de propiedad identificados en registros de escrituras de NYC. Cada uno agrupa LLC por patrones de nombres y actividad de adquisición. Solo registros públicos.',
      acq: 'adquisiciones',
      toggle: 'ES / EN'
    }}
  }};
  function applyLang(l) {{
    var s = i18n[l] || i18n.en;
    var h = document.getElementById('dir-heading');
    if (h) h.textContent = s.heading;
    var d = document.getElementById('dir-desc');
    if (d) d.textContent = s.desc;
    document.querySelectorAll('.op-label-acq').forEach(function(el) {{
      el.textContent = s.acq;
    }});
    var btn = document.getElementById('lang-toggle');
    if (btn) btn.textContent = s.toggle;
  }}
  applyLang(lang);
  var btn = document.getElementById('lang-toggle');
  if (btn) btn.addEventListener('click', function() {{
    lang = lang === 'en' ? 'es' : 'en';
    localStorage.setItem('pc-lang', lang);
    applyLang(lang);
  }});
}})();
</script>
</body>
</html>"""

    _operators_cache = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)
