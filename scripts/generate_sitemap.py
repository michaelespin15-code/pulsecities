"""
Regenerate the sitemap set from the live database.

Writes a sitemap index at frontend/sitemap.xml (the URL robots.txt and Search
Console already point at) plus the child files it names. The split is forced by
volume: the sitemaps spec caps a single file at 50,000 URLs.

    sitemap.xml             index
    sitemap-core.xml        hubs, neighborhoods, boroughs, weeks, operators, LLCs
    sitemap-property-N.xml  property pages, 45,000 per file

**The gate that matters is not here.** A page is indexed because
`_build_property_page` renders `index, follow`, not because this file lists it.
That rule used to pass on the ZIP-level displacement score, so 596,432 parcels
carrying no deed, eviction, violation or permit told Google to index ~429 words
of boilerplate running 81% identical page to page. It now requires a
building-level record, and this file's gate is set to match rather than to
compensate for it.

Sitemapped property pages are the ones with a deed or an eviction: the
ownership-and-displacement story the site is actually about, and the shape of
the address queries in the search exports. Measured 5-gram overlap by record
profile, against /neighborhood at 68-69% as the known-good benchmark:

    deed + eviction   52% mean    sitemapped, priority 0.6
    deed only         69% mean    sitemapped, priority 0.5
    eviction only     68% mean    sitemapped, priority 0.5
    violations only   66% mean    indexable, not sitemapped
    no records        75% mean    noindex

lastmod is the date of that page's newest record, not the date this ran. 2,111
of the old 2,159 URLs claimed the same lastmod, which tells a crawler nothing
and costs credibility on the ones that did change.

Run manually or from cron after the nightly scoring pass:
    python -m scripts.generate_sitemap
"""

import gzip
import os
import tempfile
from datetime import date
from pathlib import Path

from sqlalchemy import text

from models.database import SessionLocal

_FRONTEND = Path(__file__).resolve().parent.parent / "frontend"
_OUT = _FRONTEND / "sitemap.xml"
_BASE = "https://pulsecities.com"

# The spec caps a sitemap at 50,000 URLs; leave headroom so a growth spurt
# between runs cannot silently push a file over the line.
_URLS_PER_FILE = 45_000

# (path, changefreq, priority, lastmod or None for today)
_CORE = [
    ("/",            "daily",   "1.0",  None),
    ("/displacement", "daily",  "0.9",  None),
    # The lookup form only; ?q= results are noindex and belong to no one.
    ("/eviction-case", "weekly", "0.6",  None),
    ("/network",      "weekly", "0.7",  None),
    ("/map",         "daily",   "0.6",  None),
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
    ("/evictions",   "daily",   "0.75", None),
    ("/who-owns-my-building", "monthly", "0.7", None),
    ("/is-my-building-rent-stabilized", "monthly", "0.7", None),
    ("/llc",         "weekly",  "0.6",  None),
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


def build() -> dict[str, str]:
    """Returns {filename: xml}. sitemap.xml is the index; the rest are children."""
    today = date.today().isoformat()

    with SessionLocal() as db:
        zips = [r.zip_code for r in db.execute(text("""
            SELECT n.zip_code
            FROM neighborhoods n
            JOIN displacement_scores ds ON ds.zip_code = n.zip_code
            WHERE ds.score IS NOT NULL
            ORDER BY n.zip_code
        """)).fetchall()]

        # Property pages carrying a deed or an eviction, with the date of the
        # newest of the two as lastmod and a priority that reflects whether the
        # page tells the full arc or half of it. ORDER BY keeps nightly output
        # stable so the file does not churn in git.
        # Aggregate once per table, then hash-join. The LATERAL form of this
        # query ran max() per parcel: ~1.75M index probes, planner cost 3.3M
        # against 116k for this shape, in the middle of the 03:15 cron pile-up
        # on two vCPUs.
        property_rows = db.execute(text("""
            WITH d AS (
                SELECT bbl, max(doc_date) AS last_deed
                FROM ownership_raw WHERE doc_type = 'DEED' GROUP BY bbl
            ),
            e AS (
                SELECT bbl, max(executed_date) AS last_evict
                FROM evictions_raw GROUP BY bbl
            )
            SELECT p.bbl,
                   GREATEST(COALESCE(d.last_deed, DATE '1900-01-01'),
                            COALESCE(e.last_evict, DATE '1900-01-01')) AS lastmod,
                   (d.last_deed IS NOT NULL AND e.last_evict IS NOT NULL) AS full_arc
            FROM parcels p
            JOIN neighborhoods n ON n.zip_code = p.zip_code
            JOIN displacement_scores ds ON ds.zip_code = p.zip_code
            LEFT JOIN d ON d.bbl = p.bbl
            LEFT JOIN e ON e.bbl = p.bbl
            WHERE p.address IS NOT NULL AND n.name IS NOT NULL AND ds.score IS NOT NULL
              AND (d.last_deed IS NOT NULL OR e.last_evict IS NOT NULL)
            ORDER BY p.bbl
        """)).fetchall()

        week_slugs = _completed_week_slugs(db)

        # Per-neighbourhood eviction pages, gated at the same floor the route
        # uses for index,follow so the sitemap never lists a noindex page.
        from api.routes.frontend import _EV_AREA_MIN, _ev_area_slug

        # Entity families, gated in api/entity_families.py. A family whose
        # entities are already covered by a curated /operator profile 301s
        # there, so it must not be listed here as its own URL.
        from api.entity_families import compute_families
        from api.routes.frontend import _is_buyer_entity

        curated_entities = set()
        for row in db.execute(text(
            "SELECT jsonb_array_elements_text(llc_entities) AS name FROM operators "
            "WHERE operator_class = 'operator' AND llc_entities IS NOT NULL"
        )).fetchall():
            curated_entities.add(row.name)
        families = [
            (f["slug"], f["last_deed"].isoformat() if f["last_deed"] else today)
            for f in compute_families(db, _is_buyer_entity).values()
            if not (set(f["entities"]) & curated_entities)
        ]

        eviction_areas = [
            (_ev_area_slug(r.name), r.last.isoformat() if r.last else today)
            for r in db.execute(text("""
                SELECT n.name, max(e.executed_date) AS last
                FROM evictions_raw e
                JOIN neighborhoods n ON n.zip_code = e.zip_code
                WHERE e.eviction_type = 'Residential' AND n.name IS NOT NULL
                GROUP BY 1 HAVING count(*) >= :floor
                ORDER BY 1
            """), {"floor": _EV_AREA_MIN}).fetchall()
            if _ev_area_slug(r.name)
        ]

        # LLC entity pages, gated exactly as the route's robots policy is, by
        # importing the rule rather than restating it so the two cannot drift.
        # _BUILDING_KEY_SQL collapses a condominium's unit lots to the one
        # building they are; counting tax blocks instead used to exclude
        # NORWORTH HOLDINGS LLC, three buildings on one block, which had earned
        # 3 of the site's 5 total clicks while marked noindex.
        from api.routes.frontend import (
            _BUILDING_KEY_SQL, _LLC_MIN_BUILDINGS, _LLC_MIN_LOTS, _LLC_SLUG_RE,
        )

        llc_rows = db.execute(text(f"""
            SELECT party_name_normalized AS name,
                   btrim(regexp_replace(lower(party_name_normalized),
                         '[^a-z0-9]+', '-', 'g'), '-') AS slug,
                   max(doc_date) AS last_deed
            FROM ownership_raw
            WHERE doc_type = 'DEED' AND party_type = '2'
              AND party_name_normalized LIKE '%LLC%'
            GROUP BY 1, 2
            HAVING count(DISTINCT bbl) >= {_LLC_MIN_LOTS}
               AND count(DISTINCT ({_BUILDING_KEY_SQL})) >= {_LLC_MIN_BUILDINGS}
        """)).fetchall()
        llcs = sorted(
            {(r.slug, r.last_deed.isoformat() if r.last_deed else today)
             for r in llc_rows
             if r.slug and _LLC_SLUG_RE.match(r.slug) and _is_buyer_entity(r.name)}
        )

        # Newest record anywhere, so the hub pages claim a date they can defend.
        hub_lastmod = db.execute(text("""
            SELECT max(d) FROM (
                SELECT max(doc_date) AS d FROM ownership_raw
                UNION ALL SELECT max(executed_date) FROM evictions_raw
            ) t
        """)).scalar()
        hub_lastmod = hub_lastmod.isoformat() if hub_lastmod else today

    def urlset(entries) -> str:
        lines = ['<?xml version="1.0" encoding="UTF-8"?>',
                 '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
        for path, freq, prio, lastmod in entries:
            lines.append(
                f"  <url>\n"
                f"    <loc>{_BASE}{path}</loc>\n"
                f"    <lastmod>{lastmod}</lastmod>\n"
                f"    <changefreq>{freq}</changefreq>\n"
                f"    <priority>{prio}</priority>\n"
                f"  </url>"
            )
        lines.append("</urlset>")
        return "\n".join(lines) + "\n"

    core: list[tuple[str, str, str, str]] = []
    for path, freq, prio, lastmod in _CORE:
        core.append((path, freq, prio, lastmod or hub_lastmod))
    for slug in _OPERATORS:
        core.append((f"/operator/{slug}", "weekly", "0.6", hub_lastmod))
    for z in zips:
        core.append((f"/neighborhood/{z}", "daily", "0.7", today))
    # Historical weekly editions never change once past; lastmod = their Sunday.
    for slug, sunday_iso in week_slugs:
        core.append((f"/week/{slug}", "monthly", "0.5", sunday_iso))
    # An entity ledger moves only when a deed lands, so say when that was.
    for slug, last_deed in llcs:
        core.append((f"/llc/{slug}", "monthly", "0.5", last_deed))
    # The eviction record refreshes nightly, and these answer the largest block
    # of unserved demand in the search exports.
    for slug, last_evict in eviction_areas:
        core.append((f"/evictions/{slug}", "weekly", "0.7", last_evict))
    # Entity families. Substantial by construction and the only pages that
    # reassemble a portfolio held one LLC at a time.
    for slug, last_deed in families:
        core.append((f"/network/{slug}", "monthly", "0.7", last_deed))

    # changefreq is "monthly" because a property page changes when a record
    # lands, which for most lots is never. The old blanket "weekly" was a claim
    # the data did not support on 1,792 URLs.
    prop = [
        (f"/property/{r.bbl}", "monthly", "0.6" if r.full_arc else "0.5",
         r.lastmod.isoformat())
        for r in property_rows
    ]

    files: dict[str, str] = {"sitemap-core.xml": urlset(core)}
    chunks = [prop[i:i + _URLS_PER_FILE] for i in range(0, len(prop), _URLS_PER_FILE)] or [[]]
    for i, chunk in enumerate(chunks, 1):
        files[f"sitemap-property-{i}.xml"] = urlset(chunk)

    index = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for name in files:
        index.append(f"  <sitemap>\n"
                     f"    <loc>{_BASE}/{name}</loc>\n"
                     f"    <lastmod>{today}</lastmod>\n"
                     f"  </sitemap>")
    index.append("</sitemapindex>")
    files["sitemap.xml"] = "\n".join(index) + "\n"
    return files


def _write_atomic(path: Path, body: str) -> None:
    """nginx serves these straight from disk, so a crawler must never catch one
    half-written. A .gz twin is written alongside for gzip_static: 11.5 MB of
    sitemap XML was otherwise recompressed on every crawler fetch."""
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(body)
    # mkstemp creates 0600; nginx workers need world-read or they serve 403.
    os.chmod(tmp_path, 0o644)
    os.replace(tmp_path, path)

    gz_fd, gz_tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".gz.tmp")
    with os.fdopen(gz_fd, "wb") as fh:
        fh.write(gzip.compress(body.encode("utf-8"), compresslevel=9))
    os.chmod(gz_tmp, 0o644)
    os.replace(gz_tmp, str(path) + ".gz")


if __name__ == "__main__":
    files = build()
    # Children first: the index must never name a file that is not there yet.
    for name in sorted(files, key=lambda n: n == "sitemap.xml"):
        _write_atomic(_FRONTEND / name, files[name])
        print(f"wrote {name} with {files[name].count('<url>') or files[name].count('<sitemap>')} entries")

    # Old single-file runs left no other children, but a shrinking property set
    # would: drop any chunk this run did not write, or the index and the disk
    # disagree.
    for stale in _FRONTEND.glob("sitemap-property-*.xml"):
        if stale.name not in files:
            stale.unlink()
            print(f"removed stale {stale.name}")
