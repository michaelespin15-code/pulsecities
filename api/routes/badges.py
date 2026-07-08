"""
Embeddable score badge.

GET /badge/{zip_code}.svg
  Self-contained SVG showing the ZIP, neighborhood name, current score,
  and tier. Meant to be dropped into press articles and community sites
  as a plain <img> linking back to the neighborhood page, so it uses
  system font stacks only: browsers block external fetches for SVGs
  loaded through <img>.

Rate-limited to 60/minute per IP. Cached for an hour; scores move daily.
"""

import html as _html
import logging

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import Response as RawResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(tags=["badges"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)

_MONO = "SFMono-Regular, Menlo, Consolas, 'Liberation Mono', monospace"
_SANS = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"


def _badge_svg(zip_code: str, name: str | None, score: float | None) -> str:
    from api.routes.frontend import _tier_info

    if score is not None:
        tier_label, tier_color = _tier_info(score)
        score_str = f"{score:.1f}"
        tier_text = f"{tier_label} displacement pressure".upper()
    else:
        tier_color = "#64748b"
        score_str = "--"
        tier_text = "NO SCORE YET"

    place = _html.escape(name or "", quote=True)
    e_zip = _html.escape(zip_code, quote=True)

    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="320" height="64" viewBox="0 0 320 64" role="img" aria-label="PulseCities displacement score for {e_zip}">
  <rect width="320" height="64" rx="8" fill="#0f172a" stroke="#334155" stroke-width="1"/>
  <rect x="12" y="16" width="32" height="32" rx="6" fill="#1a1a2e"/>
  <polyline points="14,32 19,32 22,25 25,39 28,29 31,35 34,32 42,32" fill="none" stroke="#f97316" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
  <text x="56" y="26" font-family="{_SANS}" font-size="12" font-weight="600" fill="#e2e8f0">{e_zip}{(' ' + chr(183) + ' ' + place) if place else ''}</text>
  <text x="56" y="44" font-family="{_MONO}" font-size="8.5" letter-spacing="0.4" fill="{tier_color}">{tier_text}</text>
  <text x="308" y="32" text-anchor="end" font-family="{_MONO}" font-weight="600"><tspan font-size="20" fill="{tier_color}">{score_str}</tspan><tspan font-size="10" fill="#94a3b8">/100</tspan></text>
  <text x="308" y="48" text-anchor="end" font-family="{_MONO}" font-size="9" fill="#94a3b8">pulsecities.com</text>
</svg>'''


@router.get("/badge/{zip_code}.svg", include_in_schema=False)
@limiter.limit("60/minute")
def badge(
    request: Request,
    response: Response,
    zip_code: str,
    db: Session = Depends(get_db),
):
    if not (len(zip_code) == 5 and zip_code.isdigit()):
        raise HTTPException(status_code=400, detail="zip_code must be a 5-digit numeric string")

    row = db.execute(text("""
        SELECT n.zip_code, n.name, ds.score
        FROM neighborhoods n
        LEFT JOIN displacement_scores ds ON ds.zip_code = n.zip_code
        WHERE n.zip_code = :zip
        LIMIT 1
    """), {"zip": zip_code}).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="ZIP not covered")

    svg = _badge_svg(row.zip_code, row.name, float(row.score) if row.score is not None else None)
    return RawResponse(
        content=svg,
        media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=3600"},
    )
