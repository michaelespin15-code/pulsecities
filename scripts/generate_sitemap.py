"""
Regenerate frontend/sitemap.xml from the live database.

Core pages are listed first, then the tracked operator profiles under their
canonical slugs, then every neighborhood page that has a score. Neighborhood
pages carry today's lastmod because scores refresh nightly.

Run manually or from cron after the nightly scoring pass:
    python -m scripts.generate_sitemap
"""

from datetime import date
from pathlib import Path

from sqlalchemy import text

from models.database import SessionLocal

_OUT = Path(__file__).resolve().parent.parent / "frontend" / "sitemap.xml"

# (path, changefreq, priority, lastmod or None for today)
_CORE = [
    ("/",            "daily",   "1.0",  None),
    ("/map",         "daily",   "0.9",  None),
    ("/methodology", "weekly",  "0.8",  "2026-07-09"),
    ("/about",       "weekly",  "0.8",  "2026-04-29"),
    ("/operators",   "weekly",  "0.75", "2026-04-29"),
    ("/neighborhoods", "daily", "0.8",  None),
    ("/brooklyn",      "daily", "0.8",  None),
    ("/manhattan",     "daily", "0.8",  None),
    ("/queens",        "daily", "0.8",  None),
    ("/bronx",         "daily", "0.8",  None),
    ("/staten-island", "daily", "0.8",  None),
    ("/this-week",   "daily",   "0.75", None),
    ("/flips",       "daily",   "0.75", None),
    ("/radar",       "daily",   "0.75", None),
]

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

    lines.append("</urlset>")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    xml = build()
    _OUT.write_text(xml)
    count = xml.count("<url>")
    print(f"wrote {_OUT} with {count} urls")
