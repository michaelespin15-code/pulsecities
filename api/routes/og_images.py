"""
Dynamic per-neighborhood OG images.

GET /og/{zip_code}.png  — generates a 1200x630 PNG showing "As of [date]"
with current eviction and permit counts for the neighborhood. Cached to disk
by (zip, date) so each day's image is generated once and served as bytes
on subsequent scrapes.
"""

import io
import logging
import math
import re
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


# Canonical bands: Low 0-33, Moderate 34-66, High 67-84, Critical 85+.
# Must match the map legend, panel, summaries, and digest.
def _score_color(score: float) -> tuple:
    if score >= 85: return (239, 68, 68)
    if score >= 67: return (249, 115, 22)
    if score >= 34: return (192, 139, 45)
    return (62, 107, 84)


def _score_tier(score: float) -> str:
    if score >= 85: return "CRITICAL"
    if score >= 67: return "HIGH"
    if score >= 34: return "MODERATE"
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


def _render_operator(
    display_name: str,
    acquisitions: int,
    llc_count: int,
    zip_count: int,
    today_label: str,
) -> bytes:
    W, H = 1200, 630
    img  = Image.new("RGB", (W, H), _BG)
    draw = ImageDraw.Draw(img)

    for x in range(0, W, 60):
        draw.line([(x, 0), (x, H)], fill=_DIM, width=1)
    for y in range(0, H, 60):
        draw.line([(0, y), (W, y)], fill=_DIM, width=1)

    f_name  = ImageFont.truetype(_FONTS_BOLD,    64)
    f_sub   = ImageFont.truetype(_FONTS_REGULAR, 22)
    f_date  = ImageFont.truetype(_FONTS_MONO,    17)
    f_count = ImageFont.truetype(_FONTS_BOLD,    44)
    f_label = ImageFont.truetype(_FONTS_REGULAR, 19)
    f_brand = ImageFont.truetype(_FONTS_MONO,    16)

    # Left — cluster identity
    draw.text((80, 90),  display_name.upper(), font=f_name, fill=_ORANGE)
    draw.text((80, 172), "LLC network in NYC property records", font=f_sub, fill=_MUTED)

    draw.line([(80, 214), (640, 214)], fill=(35, 50, 75), width=1)
    draw.text((80, 228), f"As of {today_label}", font=f_date, fill=_MUTED)

    stats = [
        (str(acquisitions), "recorded acquisitions"),
        (str(llc_count),    "LLC entities"),
        (str(zip_count),    "ZIP codes"),
    ]
    y = 280
    for value, label in stats:
        draw.text((80, y),      value, font=f_count, fill=_WHITE)
        draw.text((80, y + 56), label, font=f_label, fill=_MUTED)
        y += 100

    # Right — the shell constellation: one node per LLC around the operator
    cx, cy = W - 300, 300
    spokes = max(3, min(llc_count, 12))
    for i in range(spokes):
        a = (i / spokes) * 2 * math.pi - math.pi / 2
        x = cx + math.cos(a) * 150
        yy = cy + math.sin(a) * 130
        draw.line([(cx, cy), (x, yy)], fill=(55, 70, 95), width=2)
        draw.ellipse([x - 11, yy - 11, x + 11, yy + 11], fill=(100, 116, 139))
    draw.ellipse([cx - 22, cy - 22, cx + 22, cy + 22], fill=_ORANGE)

    draw.text((82, H - 50), "pulsecities.com", font=f_brand, fill=(60, 75, 100))
    draw.rectangle([0, H - 5, W, H], fill=_ORANGE)

    buf = io.BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


# --- Digest sparkline -------------------------------------------------------
# The weekly email is set on paper, so the trace is drawn on the email's paper
# tokens rather than the site-dark OG palette above.

_SPARK_PAPER = (251, 250, 247)
_SPARK_RULE  = (217, 212, 201)
_SPARK_PULSE = (228, 89, 15)


def _render_spark(scores: list[float]) -> bytes:
    """90-day score trace, 1120x160 (rendered 2x for a 560x80 slot)."""
    W, H = 1120, 160
    PAD_X, PAD_Y = 10, 16
    img  = Image.new("RGB", (W, H), _SPARK_PAPER)
    draw = ImageDraw.Draw(img)

    if not scores:
        scores = [50.0, 50.0]
    if len(scores) == 1:
        scores = scores * 2

    lo, hi = min(scores), max(scores)
    if hi - lo < 1.0:
        # A flat run still deserves a visible line, not a degenerate scale.
        mid = (hi + lo) / 2
        lo, hi = mid - 1.0, mid + 1.0

    n = len(scores)
    pts = [
        (
            PAD_X + (W - 2 * PAD_X) * i / (n - 1),
            PAD_Y + (H - 2 * PAD_Y) * (1 - (s - lo) / (hi - lo)),
        )
        for i, s in enumerate(scores)
    ]

    draw.line([(0, H - 1), (W, H - 1)], fill=_SPARK_RULE, width=2)
    draw.line(pts, fill=_SPARK_PULSE, width=5, joint="curve")
    ex, ey = pts[-1]
    r = 9
    draw.ellipse([ex - r, ey - r, ex + r, ey + r], fill=_SPARK_PULSE)

    buf = io.BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()


@router.get("/og/spark/{zip_code}.png", include_in_schema=False)
def spark_image(zip_code: str, db: Session = Depends(get_db)):
    """Personal 90-day pulse trace for the weekly email. Never 404s: a ZIP
    without history gets a flat placeholder so no client renders a broken box.
    The special key 'nyc' traces the citywide average for the citywide digest."""
    if zip_code != "nyc" and not (len(zip_code) == 5 and zip_code.isdigit()):
        png = _render_spark([])
        return Response(content=png, media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})

    today      = date.today()
    cache_key  = f"spark-{zip_code}_{today.strftime('%Y%m%d')}"
    cache_path = _CACHE_DIR / f"{cache_key}.png"

    if cache_path.exists():
        return Response(content=cache_path.read_bytes(), media_type="image/png",
                        headers={"Cache-Control": "public, max-age=3600"})

    if zip_code == "nyc":
        rows = db.execute(text("""
            SELECT scored_at::date AS d, AVG(composite_score) AS s
            FROM score_history
            WHERE scored_at >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY scored_at::date
            ORDER BY d
        """)).fetchall()
    else:
        rows = db.execute(text("""
            SELECT scored_at::date AS d, AVG(composite_score) AS s
            FROM score_history
            WHERE zip_code = :zip
              AND scored_at >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY scored_at::date
            ORDER BY d
        """), {"zip": zip_code}).fetchall()

    scores = [float(r.s) for r in rows if r.s is not None]
    png = _render_spark(scores)

    cache_path.write_bytes(png)
    _clean_old_cache(f"spark-{zip_code}", cache_key)

    return Response(content=png, media_type="image/png",
                    headers={"Cache-Control": "public, max-age=3600"})


@router.get("/og/operator/{slug}.png", include_in_schema=False)
def operator_og_image(slug: str, db: Session = Depends(get_db)):
    if not re.match(r"^[a-z0-9-]+$", slug):
        return Response(content=_DEFAULT_IMAGE.read_bytes(), media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})

    today      = date.today()
    cache_key  = f"op_{slug}_{today.strftime('%Y%m%d')}"
    cache_path = _CACHE_DIR / f"{cache_key}.png"

    if cache_path.exists():
        return Response(content=cache_path.read_bytes(), media_type="image/png",
                        headers={"Cache-Control": "public, max-age=3600"})

    row = db.execute(text("""
        SELECT o.display_name, o.operator_root, o.operator_class,
               o.total_acquisitions, o.llc_entities,
               (SELECT COUNT(DISTINCT p.zip_code)
                FROM operator_parcels op JOIN parcels p ON p.bbl = op.bbl
                WHERE op.operator_id = o.id) AS zip_count,
               (SELECT COUNT(DISTINCT op.acquiring_entity)
                FROM operator_parcels op
                WHERE op.operator_id = o.id) AS llc_count
        FROM operators o
        WHERE o.slug = :slug
    """), {"slug": slug}).fetchone()

    # Classification gate: no branded card for lenders, GSEs, or HDFCs
    if not row or row.operator_class != "operator":
        return Response(content=_DEFAULT_IMAGE.read_bytes(), media_type="image/png",
                        headers={"Cache-Control": "public, max-age=3600"})

    png = _render_operator(
        display_name = row.display_name or row.operator_root,
        acquisitions = int(row.total_acquisitions or 0),
        llc_count    = int(row.llc_count or 0),
        zip_count    = int(row.zip_count or 0),
        today_label  = today.strftime("%b %-d, %Y"),
    )

    cache_path.write_bytes(png)
    _clean_old_cache(f"op_{slug}", cache_key)

    return Response(content=png, media_type="image/png",
                    headers={"Cache-Control": "public, max-age=3600"})


# ---------------------------------------------------------------------------
# Headline cards for the section landing pages (borough, this-week). One
# generic renderer: an eyebrow, a wrapped title, a date line, and up to three
# stat columns, in the same visual language as the neighborhood card.
# ---------------------------------------------------------------------------

_BOROUGH_SLUGS = {
    "brooklyn": "Brooklyn", "manhattan": "Manhattan", "queens": "Queens",
    "bronx": "Bronx", "staten-island": "Staten Island",
}


def _wrap(draw, text_str: str, font, max_w: int, max_lines: int = 2) -> list[str]:
    words, lines, cur = text_str.split(), [], ""
    for w in words:
        trial = f"{cur} {w}".strip()
        if draw.textlength(trial, font=font) <= max_w or not cur:
            cur = trial
        else:
            lines.append(cur)
            cur = w
            if len(lines) == max_lines - 1:
                break
    rest = " ".join(words[sum(len(l.split()) for l in lines):]) if lines else cur
    lines.append(rest if lines else cur)
    return lines[:max_lines]


def _render_headline(eyebrow: str, title: str, date_label: str,
                     stats: list[tuple], accent: tuple = _ORANGE) -> bytes:
    W, H = 1200, 630
    img  = Image.new("RGB", (W, H), _BG)
    draw = ImageDraw.Draw(img)

    for x in range(0, W, 60):
        draw.line([(x, 0), (x, H)], fill=_DIM, width=1)
    for y in range(0, H, 60):
        draw.line([(0, y), (W, y)], fill=_DIM, width=1)

    f_eyebrow = ImageFont.truetype(_FONTS_MONO,    20)
    f_title   = ImageFont.truetype(_FONTS_BOLD,    58)
    f_date    = ImageFont.truetype(_FONTS_MONO,    17)
    f_count   = ImageFont.truetype(_FONTS_BOLD,    52)
    f_label   = ImageFont.truetype(_FONTS_REGULAR, 19)
    f_brand   = ImageFont.truetype(_FONTS_MONO,    16)

    draw.text((80, 82), eyebrow.upper(), font=f_eyebrow, fill=accent)

    lines = _wrap(draw, title, f_title, W - 160)
    ty = 128
    for ln in lines:
        draw.text((80, ty), ln, font=f_title, fill=_WHITE)
        ty += 68

    draw.line([(80, ty + 8), (680, ty + 8)], fill=(35, 50, 75), width=1)
    draw.text((80, ty + 22), f"As of {date_label}", font=f_date, fill=_MUTED)

    sx = 80
    for value, label in stats[:3]:
        draw.text((sx, 400), str(value), font=f_count, fill=_WHITE)
        draw.text((sx, 462), label, font=f_label, fill=_MUTED)
        sx += 370

    draw.text((82, H - 50), "pulsecities.com", font=f_brand, fill=(60, 75, 100))
    draw.rectangle([0, H - 5, W, H], fill=accent)

    buf = io.BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


@router.get("/og/borough/{slug}.png", include_in_schema=False)
def borough_og_image(slug: str, db: Session = Depends(get_db)):
    borough = _BOROUGH_SLUGS.get(slug)
    if not borough:
        return Response(content=_DEFAULT_IMAGE.read_bytes(), media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})

    today      = date.today()
    cache_key  = f"borough-{slug}_{today.strftime('%Y%m%d')}"
    cache_path = _CACHE_DIR / f"{cache_key}.png"
    if cache_path.exists():
        return Response(content=cache_path.read_bytes(), media_type="image/png",
                        headers={"Cache-Control": "public, max-age=3600"})

    from api.routes.neighborhoods import _borough_from_zip
    rows = db.execute(text("""
        SELECT n.zip_code, n.name, ds.score
        FROM neighborhoods n JOIN displacement_scores ds ON ds.zip_code = n.zip_code
        WHERE ds.score IS NOT NULL AND n.name IS NOT NULL
    """)).fetchall()
    members = [(r.name, float(r.score)) for r in rows if _borough_from_zip(r.zip_code) == borough]
    if not members:
        return Response(content=_DEFAULT_IMAGE.read_bytes(), media_type="image/png",
                        headers={"Cache-Control": "public, max-age=3600"})

    n = len(members)
    avg = sum(s for _, s in members) / n
    top_name, top_score = max(members, key=lambda x: x[1])
    png = _render_headline(
        eyebrow="NYC displacement pressure",
        title=f"{borough}",
        date_label=today.strftime("%b %-d, %Y"),
        stats=[
            (n, "neighborhoods tracked"),
            (f"{avg:.0f}", "average score"),
            (f"{top_score:.0f}", f"highest: {top_name}"),
        ],
        accent=_score_color(top_score),
    )
    cache_path.write_bytes(png)
    _clean_old_cache(f"borough-{slug}", cache_key)
    return Response(content=png, media_type="image/png",
                    headers={"Cache-Control": "public, max-age=3600"})


# Slashed path on purpose: a single-segment /og/this-week.png would be captured
# by the /og/{zip_code}.png route (zip_code="this-week") and fall back to the
# default image. The extra segment keeps this route reachable.
@router.get("/og/this-week/card.png", include_in_schema=False)
def this_week_og_image(db: Session = Depends(get_db)):
    today      = date.today()
    cache_key  = f"thisweek_{today.strftime('%Y%m%d')}"
    cache_path = _CACHE_DIR / f"{cache_key}.png"
    if cache_path.exists():
        return Response(content=cache_path.read_bytes(), media_type="image/png",
                        headers={"Cache-Control": "public, max-age=3600"})

    # Score movers, not raw filing counts: the record tables lag ~1-2 weeks, so a
    # 7-day eviction/deed count reads 0 mid-lag and looks broken. The score deltas
    # are snapshotted nightly, so they are always current and genuinely this-week.
    risers = db.execute(text("""
        WITH now_s AS (
            SELECT DISTINCT ON (zip_code) zip_code, composite_score AS s
            FROM score_history WHERE scored_at <= CURRENT_DATE
            ORDER BY zip_code, scored_at DESC
        ),
        then_s AS (
            SELECT DISTINCT ON (zip_code) zip_code, composite_score AS s
            FROM score_history WHERE scored_at <= CURRENT_DATE - 7
            ORDER BY zip_code, scored_at DESC
        )
        SELECT COUNT(*) FILTER (WHERE now_s.s - then_s.s >= 0.5) AS n
        FROM now_s JOIN then_s ON then_s.zip_code = now_s.zip_code
    """)).scalar() or 0

    agg = db.execute(text("""
        SELECT COUNT(*) FILTER (WHERE score >= 67) AS high, ROUND(AVG(score)) AS avg
        FROM displacement_scores WHERE score IS NOT NULL
    """)).fetchone()

    png = _render_headline(
        eyebrow="This week in NYC displacement",
        title="What the public record moved this week",
        date_label=today.strftime("%b %-d, %Y"),
        stats=[
            (int(risers), "neighborhoods rising, 7d"),
            (int(agg.high or 0) if agg else 0, "at high pressure"),
            (int(agg.avg or 0) if agg else 0, "citywide average score"),
        ],
    )
    cache_path.write_bytes(png)
    _clean_old_cache("thisweek", cache_key)
    return Response(content=png, media_type="image/png",
                    headers={"Cache-Control": "public, max-age=3600"})
