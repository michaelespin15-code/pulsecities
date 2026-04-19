"""
Server-side meta tag injection for neighborhood deep-link pages.

GET /neighborhood/{zip_code}
  Serves app.html with neighborhood-specific title, description, canonical,
  og:*, twitter:*, and Dataset JSON-LD injected — making each of the 178
  neighborhood URLs a distinct, indexable page for search engines and AI crawlers.

  Falls back to the generic app.html if the zip has no score data yet.
"""

import html as _html
import json
import logging
import time
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
_page_cache: dict[str, tuple[str, float]] = {}  # zip -> (html, expires_at)
_PAGE_TTL = 3600


def _template() -> str:
    global _app_html
    if _app_html is None:
        _app_html = (_FRONTEND / "app.html").read_text()
    return _app_html


def _risk_tier(score: float) -> str:
    if score >= 85: return "critical displacement pressure"
    if score >= 67: return "high displacement pressure"
    if score >= 34: return "moderate displacement pressure"
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
    title = (
        f"{name} ({zip_code}) \u2014 Displacement Risk {score:.1f} \u00b7 PulseCities"
        if score is not None
        else f"{name} ({zip_code}) \u2014 NYC Displacement Risk \u00b7 PulseCities"
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
        '<link rel="canonical" href="https://pulsecities.com/">',
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
    html = html.replace(
        'content="https://pulsecities.com/"',
        f'content="{e_url}"',
        1,
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
