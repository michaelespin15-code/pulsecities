"""
Server-side rendered pages for neighborhood and operator deep links, plus the
operators directory.

GET /neighborhood/{zip_code}  — per-neighborhood OG/meta injection into app.html
GET /operator/{root}          — per-operator OG/meta injection into operator.html
GET /operators                — server-side rendered directory of all tracked operators
"""

import html as _html
import json
import logging
import time
from datetime import date
from pathlib import Path

from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
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


def _risk_tier(score: float) -> str:
    if score >= 70: return "critical displacement pressure"
    if score >= 55: return "high displacement pressure"
    if score >= 35: return "moderate displacement pressure"
    return "low displacement pressure relative to other NYC neighborhoods"


def _description(name: str, zip_code: str, borough: str | None, score: float | None) -> str:
    loc = f"{name} ({zip_code}{', ' + borough if borough else ''})"
    if score is None:
        return (
            f"Track displacement pressure in {loc}, NYC. "
            f"Updated daily from ACRIS deed transfers, eviction filings, DOB permits, and rent stabilization data."
        )
    return (
        f"{loc} shows {_risk_tier(score)} with a displacement score of {score:.1f}/100. "
        f"Updated daily from NYC public records: LLC deed transfers, eviction filings, "
        f"DOB permits, and rent-stabilized unit loss."
    )


def _build_html(zip_code: str, name: str, borough: str | None, score: float | None, last_updated: str | None) -> str:
    html = _template()

    url   = f"https://pulsecities.com/neighborhood/{zip_code}"
    borough_suffix = f", {borough}" if borough else ""
    title = (
        f"{name} ({zip_code}{borough_suffix}) | Displacement Score {score:.1f}/100 | PulseCities"
        if score is not None
        else f"{name} ({zip_code}{borough_suffix}) | NYC Displacement Risk | PulseCities"
    )
    desc  = _description(name, zip_code, borough, score)

    # Escape values for HTML attribute context
    e_title = _html.escape(title, quote=True)
    e_desc  = _html.escape(desc,  quote=True)
    e_url   = _html.escape(url,   quote=True)

    html = html.replace('<title>Explore \u2014 PulseCities</title>', f'<title>{title}</title>', 1)
    html = html.replace(
        'content="NYC displacement risk scores for 178 neighborhoods, updated daily from ACRIS deeds, HPD violations, DOB permits, evictions, and rent stabilization data."',
        f'content="{e_desc}"',
    )
    html = html.replace(
        '<link rel="canonical" href="https://pulsecities.com/map">',
        f'<link rel="canonical" href="{e_url}">',
        1,
    )
    # og:title and twitter:title share the same content string — replace both
    html = html.replace(
        'content="PulseCities: NYC Displacement Risk Intelligence"',
        f'content="{e_title}"',
    )
    # og:description and twitter:description share the same content string — replace both
    html = html.replace(
        'content="Real-time displacement risk scores for all 178 NYC neighborhoods, built on six public data signals: ACRIS deed transfers, HPD violations, DOB permits, eviction filings, rent stabilization loss, and assessment spikes."',
        f'content="{e_desc}"',
    )
    # og:url — was pointing to /map, must be the specific neighborhood URL
    html = html.replace(
        'content="https://pulsecities.com/map"',
        f'content="{e_url}"',
        1,
    )

    # Point og:image and twitter:image at the per-neighborhood dynamic image
    og_image_url = f"https://pulsecities.com/og/{zip_code}.png?d={date.today().strftime('%Y%m%d')}"
    e_og_image = _html.escape(og_image_url, quote=True)
    html = html.replace(
        'content="https://pulsecities.com/og-image.png"',
        f'content="{e_og_image}"',
    )

    # Inject neighborhood Dataset JSON-LD before </head>
    jsonld = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": f"Displacement Risk \u2014 {name} ({zip_code}), NYC",
        "description": desc,
        "url": url,
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
    }
    if last_updated:
        jsonld["dateModified"] = last_updated
    if score is not None:
        jsonld["variableMeasured"] = {
            "@type": "PropertyValue",
            "name": "Displacement Risk Score",
            "value": round(score, 1),
            "minValue": 0,
            "maxValue": 100,
        }

    script = f'    <script type="application/ld+json">\n    {json.dumps(jsonld, indent=4)}\n    </script>'
    html = html.replace('</head>', f'{script}\n</head>', 1)

    return html


@router.get("/neighborhood/{zip_code}", include_in_schema=False)
def neighborhood_page(zip_code: str, db: Session = Depends(get_db)):
    if not (len(zip_code) == 5 and zip_code.isdigit()):
        return HTMLResponse(_template(), status_code=200)

    cached = _page_cache.get(zip_code)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    row = db.execute(text("""
        SELECT n.name, ds.score, ds.cache_generated_at,
               CASE
                   WHEN CAST(n.zip_code AS INTEGER) BETWEEN 10001 AND 10282 THEN 'Manhattan'
                   WHEN CAST(n.zip_code AS INTEGER) BETWEEN 10301 AND 10314 THEN 'Staten Island'
                   WHEN CAST(n.zip_code AS INTEGER) BETWEEN 10451 AND 10475 THEN 'Bronx'
                   WHEN CAST(n.zip_code AS INTEGER) BETWEEN 11201 AND 11239 THEN 'Brooklyn'
                   WHEN CAST(n.zip_code AS INTEGER) BETWEEN 11001 AND 11109 THEN 'Queens'
                   WHEN CAST(n.zip_code AS INTEGER) BETWEEN 11354 AND 11697 THEN 'Queens'
                   ELSE NULL
               END AS borough
        FROM neighborhoods n
        LEFT JOIN displacement_scores ds ON n.zip_code = ds.zip_code
        WHERE n.zip_code = :zip
    """), {"zip": zip_code}).fetchone()

    if not row:
        return HTMLResponse(_template(), status_code=200)

    name         = row.name or zip_code
    score        = float(row.score) if row.score is not None else None
    borough      = row.borough
    last_updated = row.cache_generated_at.date().isoformat() if row.cache_generated_at else None

    page_html = _build_html(zip_code, name, borough, score, last_updated)
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
    html = html.replace('<title>Explore — PulseCities</title>', f'<title>{title}</title>', 1)
    html = html.replace('content="PulseCities: NYC Displacement Risk Intelligence"', f'content="{e_title}"')
    html = html.replace(
        'content="NYC displacement risk scores for 178 neighborhoods, updated daily from ACRIS deeds, HPD violations, DOB permits, evictions, and rent stabilization data."',
        f'content="{e_desc}"',
    )
    html = html.replace(
        'content="Real-time displacement risk scores for all 178 NYC neighborhoods, built on six public data signals: ACRIS deed transfers, HPD violations, DOB permits, eviction filings, rent stabilization loss, and assessment spikes."',
        f'content="{e_desc}"',
    )
    html = html.replace('<link rel="canonical" href="https://pulsecities.com/map">', f'<link rel="canonical" href="{e_url}">', 1)
    html = html.replace('content="https://pulsecities.com/map"', f'content="{e_url}"', 1)
    html = html.replace('content="https://pulsecities.com/og-image.png"', f'content="{e_og_image}"')

    _prop_page_cache[clean] = (html, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(html)


@router.get("/operator/{root}", include_in_schema=False)
def operator_page(root: str):
    root_upper = root.upper().strip()
    if len(root_upper) < 2:
        return HTMLResponse(_operator_template())

    cached = _op_page_cache.get(root_upper)
    if cached and time.monotonic() < cached[1]:
        return HTMLResponse(cached[0])

    from api.routes.operators import _load_audit
    cluster = _load_audit()["clusters"].get(root_upper)

    url = f"https://pulsecities.com/operator/{root_upper}"

    if cluster:
        entity_count = len(cluster.get("llc_entities") or [])
        prop_count   = cluster.get("total_properties", 0)
        parts = []
        if entity_count:
            parts.append(f"{entity_count} LLC {'entity' if entity_count == 1 else 'entities'}")
        if prop_count:
            parts.append(f"{prop_count} {'property' if prop_count == 1 else 'properties'} in NYC")
        title = f"{root_upper} LLC Network | NYC Acquisition Cluster | PulseCities"
        desc  = (", ".join(parts) + ". Tracked via ACRIS public records on PulseCities.") if parts else \
                f"NYC property acquisition cluster {root_upper}, tracked via ACRIS public records."
    else:
        title = f"{root_upper} | NYC Operator Profile | PulseCities"
        desc  = f"NYC property acquisition cluster {root_upper}, tracked via ACRIS public records."

    e_title = _html.escape(title, quote=True)
    e_desc  = _html.escape(desc,  quote=True)
    e_url   = _html.escape(url,   quote=True)

    html = _operator_template()
    html = html.replace('<title>Operator Profile — PulseCities</title>', f'<title>{title}</title>', 1)
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
_operators_cache: tuple[str, float] | None = None


@router.get("/operators", include_in_schema=False)
def operators_directory():
    global _operators_cache
    if _operators_cache and time.monotonic() < _operators_cache[1]:
        return HTMLResponse(_operators_cache[0])

    from api.routes.operators import _load_audit
    clusters = _load_audit()["clusters"]
    operators = sorted(
        [{"root": r, **c} for r, c in clusters.items()],
        key=lambda x: x.get("total_acquisitions", 0),
        reverse=True,
    )

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

    rows_html = ""
    list_items = []
    for i, op in enumerate(operators, 1):
        root = op["root"]
        entities = len(op.get("llc_entities") or [])
        props = op.get("total_properties", 0)
        acqs = op.get("total_acquisitions", 0)
        zips = op.get("zip_codes") or []
        boroughs = list(dict.fromkeys(b for z in zips if (b := _zip_to_borough(z))))
        borough_str = ", ".join(boroughs[:3]) + ("…" if len(boroughs) > 3 else "")
        detail = f"{entities} LLC{'s' if entities != 1 else ''}"
        if acqs:
            detail += f" &middot; {acqs} acquisition{'s' if acqs != 1 else ''}"
        if borough_str:
            detail += f" &middot; {borough_str}"
        rows_html += (
            f'<tr>'
            f'<td style="padding:10px 12px;font-family:\'JetBrains Mono\',monospace;font-size:0.8rem;">'
            f'<a href="/operator/{_html.escape(root)}" '
            f'style="color:#f97316;text-decoration:none;font-weight:500;">'
            f'{_html.escape(root)}</a></td>'
            f'<td style="padding:10px 12px;font-size:0.78rem;color:rgba(148,163,184,0.8);">{detail}</td>'
            f'</tr>\n'
        )
        list_items.append({
            "@type": "ListItem",
            "position": i,
            "name": f"{root} LLC Network",
            "url": f"https://pulsecities.com/operator/{root}",
        })

    title = "NYC LLC Acquisition Networks | Operator Directory | PulseCities"
    desc = (
        f"{len(operators)} tracked LLC acquisition networks in NYC, "
        "sourced from ACRIS public deed records. Each cluster maps shell companies "
        "to a common operator and shows acquisition counts, active ZIP codes, and related networks."
    )
    jsonld = json.dumps({
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": "NYC LLC Property Acquisition Networks",
        "description": desc,
        "url": "https://pulsecities.com/operators",
        "numberOfItems": len(operators),
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
<meta property="og:title" content="NYC LLC Acquisition Networks — PulseCities">
<meta property="og:description" content="{_html.escape(desc)}">
<meta property="og:url" content="https://pulsecities.com/operators">
<meta property="og:type" content="website">
<meta property="og:site_name" content="PulseCities">
<meta property="og:image" content="https://pulsecities.com/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="NYC LLC Acquisition Networks — PulseCities">
<meta name="twitter:description" content="{_html.escape(desc)}">
<meta name="twitter:image" content="https://pulsecities.com/og-image.png">
<script type="application/ld+json">{jsonld}</script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Inter',sans-serif;background:#0f172a;color:#f1f5f9;min-height:100vh}}
nav{{border-bottom:1px solid rgba(148,163,184,0.08);padding:12px 0}}
.nav-inner{{max-width:860px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between}}
.container{{max-width:860px;margin:0 auto;padding:32px 20px 80px}}
table{{width:100%;border-collapse:collapse}}
tr{{border-bottom:1px solid rgba(148,163,184,0.07)}}
tr:hover{{background:rgba(148,163,184,0.04)}}
a{{color:inherit;text-decoration:none}}
footer{{text-align:center;padding:24px 16px;border-top:1px solid rgba(148,163,184,0.08);margin-top:32px;font-size:12px;color:#64748b}}
.footer-links{{display:flex;justify-content:center;gap:24px;flex-wrap:wrap}}
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
    </div>
  </div>
</nav>
<div class="container">
  <div style="margin-bottom:8px;">
    <a href="/" style="font-size:0.75rem;color:rgba(148,163,184,0.5);">&#8592; Home</a>
  </div>
  <h1 style="font-size:1.4rem;font-weight:600;margin-bottom:8px;">NYC LLC Acquisition Networks</h1>
  <p style="font-size:0.85rem;color:rgba(148,163,184,0.65);margin-bottom:24px;line-height:1.6;">
    {len(operators)} operator clusters identified in ACRIS deed records. Each cluster groups LLC entities
    operating under a common name pattern with a measurable acquisition footprint in NYC.
    Sourced from public records — no proprietary data.
  </p>
  <table>
    <thead>
      <tr style="border-bottom:1px solid rgba(148,163,184,0.15);">
        <th style="padding:8px 12px;text-align:left;font-size:0.7rem;color:rgba(148,163,184,0.5);font-weight:500;text-transform:uppercase;letter-spacing:0.06em;">Operator</th>
        <th style="padding:8px 12px;text-align:left;font-size:0.7rem;color:rgba(148,163,184,0.5);font-weight:500;text-transform:uppercase;letter-spacing:0.06em;">Portfolio</th>
      </tr>
    </thead>
    <tbody>
{rows_html}    </tbody>
  </table>
</div>
<footer>
  <div class="footer-links">
    <a href="/" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Home</a>
    <a href="/map" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Map</a>
    <a href="/methodology" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">About</a>
    <a href="mailto:michaelespin15@gmail.com" style="color:#64748b;" onmouseover="this.style.color='#94a3b8'" onmouseout="this.style.color='#64748b'">Contact</a>
  </div>
</footer>
</body>
</html>"""

    _operators_cache = (page, time.monotonic() + _PAGE_TTL)
    return HTMLResponse(page)
