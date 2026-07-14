"""
Regenerate frontend/sitemap.xml from the live database.

Core pages are listed first, then the tracked operator profiles under their
canonical slugs, then every neighborhood page that has a score. Neighborhood
pages carry today's lastmod because scores refresh nightly.

Run manually or from cron after the nightly scoring pass:
    python -m scripts.generate_sitemap
"""

import os
import tempfile
from datetime import date
from pathlib import Path

from sqlalchemy import text

from models.database import SessionLocal

_OUT = Path(__file__).resolve().parent.parent / "frontend" / "sitemap.xml"

# (path, changefreq, priority, lastmod or None for today)
_CORE = [
    ("/",            "daily",   "1.0",  None),
    ("/displacement", "daily",  "0.9",  None),
    ("/map",         "daily",   "0.9",  None),
    ("/methodology", "weekly",  "0.8",  "2026-07-09"),
    ("/about",       "weekly",  "0.8",  "2026-04-29"),
    ("/press",       "weekly",  "0.75", "2026-07-10"),
    ("/developers",  "weekly",  "0.7",  "2026-07-10"),
    ("/operators",   "weekly",  "0.75", "2026-04-29"),
    ("/neighborhoods", "daily", "0.8",  None),
    ("/brooklyn",      "daily", "0.8",  None),
    ("/manhattan",     "daily", "0.8",  None),
    ("/queens",        "daily", "0.8",  None),
    ("/bronx",         "daily", "0.8",  None),
    ("/staten-island", "daily", "0.8",  None),
    ("/this-week",         "daily",  "0.75", None),
    ("/this-week/archive", "weekly", "0.65", None),
    ("/flips",       "daily",   "0.75", None),
    ("/flips/editions", "weekly", "0.7", None),
    ("/radar",       "daily",   "0.75", None),
]


def _completed_week_slugs(db) -> list[tuple[str, str]]:
    """(slug, sunday_iso) for every fully-elapsed ISO week we can score, matching
    the /week/{slug} route's availability. One week after history begins so a
    prior-week baseline exists; up to the last week whose Sunday is already past."""
    from datetime import timedelta

    row = db.execute(text("SELECT MIN(scored_at), MAX(scored_at) FROM score_history")).fetchone()
    if not row or not row[0]:
        return []
    hist_min = row[0]
    today = date.today()

    anchor = hist_min + timedelta(days=7)
    y, w, _ = anchor.isocalendar()
    monday = date.fromisocalendar(y, w, 1)

    out: list[tuple[str, str]] = []
    while True:
        sunday = monday + timedelta(days=6)
        if sunday >= today:
            break
        iy, iw, _ = monday.isocalendar()
        out.append((f"{iy}-W{iw:02d}", sunday.isoformat()))
        monday += timedelta(days=7)
    return out

# Canonical operator slugs (the /operator/{ROOT} form redirects its meta here)
_OPERATORS = ["mtek-nyc", "phantom-capital", "bredif"]


def build() -> str:
    today = date.today().isoformat()

    with SessionLocal() as db:
        zips = [r.zip_code for r in db.execute(text("""
            SELECT n.zip_code
            FROM neighborhoods n
            JOIN displacement_scores ds ON ds.zip_code = n.zip_code
            WHERE ds.score IS NOT NULL
            ORDER BY n.zip_code
        """)).fetchall()]

        week_slugs = _completed_week_slugs(db)

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]

    def entry(path: str, changefreq: str, priority: str, lastmod: str) -> None:
        lines.append(
            f"  <url>\n"
            f"    <loc>https://pulsecities.com{path}</loc>\n"
            f"    <lastmod>{lastmod}</lastmod>\n"
            f"    <changefreq>{changefreq}</changefreq>\n"
            f"    <priority>{priority}</priority>\n"
            f"  </url>"
        )

    for path, freq, prio, lastmod in _CORE:
        entry(path, freq, prio, lastmod or today)
    for slug in _OPERATORS:
        entry(f"/operator/{slug}", "weekly", "0.6", today)
    for z in zips:
        entry(f"/neighborhood/{z}", "daily", "0.7", today)
    # Historical weekly editions never change once past; lastmod = their Sunday.
    for slug, sunday_iso in week_slugs:
        entry(f"/week/{slug}", "monthly", "0.5", sunday_iso)

    lines.append("</urlset>")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    xml = build()
    # Atomic replace: nginx serves this file straight from disk, so a crawler
    # must never catch it half-written.
    fd, tmp_path = tempfile.mkstemp(dir=_OUT.parent, prefix=".sitemap.", suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(xml)
    # mkstemp creates 0600; nginx workers need world-read or they serve 403.
    os.chmod(tmp_path, 0o644)
    os.replace(tmp_path, _OUT)
    count = xml.count("<url>")
    print(f"wrote {_OUT} with {count} urls")
