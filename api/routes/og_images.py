"""
Dynamic per-neighborhood OG images.

GET /og/{zip_code}.png  — generates a 1200x630 PNG showing "As of [date]"
with current eviction and permit counts for the neighborhood. Cached to disk
by (zip, date) so each day's image is generated once and served as bytes
on subsequent scrapes.
"""

import io
import logging
from datetime import date, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends
from fastapi.responses import Response
from PIL import Image, ImageDraw, ImageFont
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(tags=["og-images"])

_CACHE_DIR = Path(__file__).parent.parent.parent / "og_cache"
_CACHE_DIR.mkdir(exist_ok=True)

_FONTS_BOLD    = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
_FONTS_REGULAR = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
_FONTS_MONO    = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"

_DEFAULT_IMAGE = Path(__file__).parent.parent.parent / "frontend" / "og-image.png"

_BG     = (15, 23, 42)
_ORANGE = (249, 115, 22)
_WHITE  = (241, 245, 249)
_MUTED  = (100, 116, 139)
_DIM    = (20, 32, 56)


def _score_color(score: float) -> tuple:
    if score >= 70: return (220, 60, 60)
    if score >= 55: return (249, 115, 22)
    if score >= 35: return (210, 160, 10)
    return (30, 170, 80)


def _score_tier(score: float) -> str:
    if score >= 70: return "CRITICAL"
    if score >= 55: return "HIGH"
    if score >= 35: return "MODERATE"
    return "LOW"


def _render(
    name: str,
    borough: str | None,
    zip_code: str,
    score: float | None,
    evictions_30d: int,
    permits_30d: int,
    today_label: str,
) -> bytes:
    W, H = 1200, 630
    img  = Image.new("RGB", (W, H), _BG)
    draw = ImageDraw.Draw(img)

    for x in range(0, W, 60):
        draw.line([(x, 0), (x, H)], fill=_DIM, width=1)
    for y in range(0, H, 60):
        draw.line([(0, y), (W, y)], fill=_DIM, width=1)

    f_name  = ImageFont.truetype(_FONTS_BOLD,    62)
    f_zip   = ImageFont.truetype(_FONTS_REGULAR, 22)
    f_date  = ImageFont.truetype(_FONTS_MONO,    17)
    f_count = ImageFont.truetype(_FONTS_BOLD,    44)
    f_label = ImageFont.truetype(_FONTS_REGULAR, 19)
    f_tier  = ImageFont.truetype(_FONTS_BOLD,    22)
    f_score = ImageFont.truetype(_FONTS_BOLD,   110)
    f_denom = ImageFont.truetype(_FONTS_REGULAR, 20)
    f_brand = ImageFont.truetype(_FONTS_MONO,    16)

    # Left — neighborhood identity
    loc = f"{name}, {borough}" if borough else name
    draw.text((80, 90),  loc,          font=f_name, fill=_WHITE)
    draw.text((80, 170), f"ZIP {zip_code}", font=f_zip,  fill=_MUTED)

    draw.line([(80, 210), (680, 210)], fill=(35, 50, 75), width=1)

    draw.text((80, 225), f"As of {today_label}", font=f_date, fill=_MUTED)

    # Evictions
    ev_color = _ORANGE if evictions_30d >= 5 else _MUTED
    draw.text((80, 270), str(evictions_30d), font=f_count, fill=ev_color)
    draw.text((80, 326), "eviction filings this month", font=f_label, fill=_MUTED)

    # Permits
    pm_color = _ORANGE if permits_30d >= 10 else _MUTED
    draw.text((80, 375), str(permits_30d), font=f_count, fill=pm_color)
    draw.text((80, 431), "construction permits filed", font=f_label, fill=_MUTED)

    # Right — score
    if score is not None:
        sc = _score_color(score)
        tier = _score_tier(score)

        score_str = f"{score:.0f}"
        sb = draw.textbbox((0, 0), score_str, font=f_score)
        sw = sb[2] - sb[0]
        draw.text((W - sw - 90, 160), score_str, font=f_score, fill=sc)
        draw.text((W - 120, 290),     "/100",    font=f_denom, fill=_MUTED)

        tb = draw.textbbox((0, 0), tier, font=f_tier)
        tw = tb[2] - tb[0]
        draw.text((W - tw - 90, 340), tier, font=f_tier, fill=sc)

        draw.text((W - 220, 375), "PRESSURE", font=f_label, fill=_MUTED)

    # Brand
    draw.text((82, H - 50), "pulsecities.com", font=f_brand, fill=(60, 75, 100))

    # Orange bottom bar
    draw.rectangle([0, H - 5, W, H], fill=_ORANGE)

    buf = io.BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


def _clean_old_cache(zip_code: str, today_key: str) -> None:
    keep = {today_key, f"{zip_code}_{(date.today() - timedelta(days=1)).strftime('%Y%m%d')}"}
    for f in _CACHE_DIR.glob(f"{zip_code}_*.png"):
        if f.stem not in keep:
            f.unlink(missing_ok=True)


@router.get("/og/{zip_code}.png", include_in_schema=False)
def og_image(zip_code: str, db: Session = Depends(get_db)):
    if not (len(zip_code) == 5 and zip_code.isdigit()):
        return Response(content=_DEFAULT_IMAGE.read_bytes(), media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})

    today      = date.today()
    cache_key  = f"{zip_code}_{today.strftime('%Y%m%d')}"
    cache_path = _CACHE_DIR / f"{cache_key}.png"

    if cache_path.exists():
        return Response(
            content=cache_path.read_bytes(),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=3600"},
        )

    row = db.execute(text("""
        SELECT n.name,
               CASE
                   WHEN CAST(n.zip_code AS INTEGER) BETWEEN 10001 AND 10282 THEN 'Manhattan'
                   WHEN CAST(n.zip_code AS INTEGER) BETWEEN 10301 AND 10314 THEN 'Staten Island'
                   WHEN CAST(n.zip_code AS INTEGER) BETWEEN 10451 AND 10475 THEN 'Bronx'
                   WHEN CAST(n.zip_code AS INTEGER) BETWEEN 11201 AND 11239 THEN 'Brooklyn'
                   WHEN CAST(n.zip_code AS INTEGER) BETWEEN 11001 AND 11109 THEN 'Queens'
                   WHEN CAST(n.zip_code AS INTEGER) BETWEEN 11354 AND 11697 THEN 'Queens'
                   ELSE NULL
               END AS borough,
               ds.score
        FROM neighborhoods n
        LEFT JOIN displacement_scores ds ON n.zip_code = ds.zip_code
        WHERE n.zip_code = :zip
    """), {"zip": zip_code}).fetchone()

    if not row:
        return Response(content=_DEFAULT_IMAGE.read_bytes(), media_type="image/png",
                        headers={"Cache-Control": "public, max-age=3600"})

    counts = db.execute(text("""
        SELECT
            (SELECT COUNT(*) FROM evictions_raw
             WHERE zip_code = :zip
               AND executed_date >= CURRENT_DATE - INTERVAL '30 days') AS ev,
            (SELECT COUNT(*) FROM permits_raw
             WHERE zip_code = :zip
               AND filing_date >= CURRENT_DATE - INTERVAL '30 days') AS pm
    """), {"zip": zip_code}).fetchone()

    today_label = today.strftime("%b %-d, %Y")
    png = _render(
        name         = row.name or zip_code,
        borough      = row.borough,
        zip_code     = zip_code,
        score        = float(row.score) if row.score is not None else None,
        evictions_30d = int(counts.ev) if counts else 0,
        permits_30d   = int(counts.pm) if counts else 0,
        today_label  = today_label,
    )

    cache_path.write_bytes(png)
    _clean_old_cache(zip_code, cache_key)

    return Response(
        content=png,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=3600"},
    )
