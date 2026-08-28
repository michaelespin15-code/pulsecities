"""
Guards for /evictions/{neighborhood}.

These pages exist to answer the largest block of unserved demand in the search
exports: ~200 impressions and zero clicks across ~35 phrasings of "eviction
marshal {place}", with nothing on the site targeting any of them.

Keyed on the neighbourhood NAME, not the ZIP. Three separate ZIPs are called
Bushwick, and three pages titled "Bushwick evictions" would compete with each
other for the one query they all want, so the page aggregates a name's ZIPs.

The plan's warning applies here specifically: do not ship these at
property-page depth or they become the next thin-content problem. Hence the
word floor, and hence the near-duplicate ceiling, which is the measure that
actually tracks thin content: n-gram containment over digit-bearing tokens.
Measured across the largest and smallest qualifying areas, 61-63% mean and 66%
max, against /neighborhood at 68-69% as the known-good benchmark.
"""

import json
import re
import warnings

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from api.main import app
from api.routes.frontend import _EV_AREA_MIN, _ev_area_slug
from models.database import SessionLocal

warnings.filterwarnings("ignore")
client = TestClient(app)

# The smallest qualifying areas measure 515-544 words and are no more
# duplicative than the largest, so the floor sits where the data sits rather
# than at a round number the thin end would have to be padded to reach.
MIN_WORDS = 500
MAX_OVERLAP = 0.70
SHINGLE = 5

_CHROME = re.compile(r"<(script|style|nav|footer|head)\b.*?</\1>", re.S | re.I)
_TAG = re.compile(r"<[^>]+>")
_WORD = re.compile(r"[A-Za-z][A-Za-z'-]+")
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9'$%,.-]*")
_ROBOTS = re.compile(r'name="robots" content="([^"]+)"')


def _shingles(html: str) -> set:
    toks = [t.lower() for t in _TOKEN.findall(_text(html))]
    return {tuple(toks[i:i + SHINGLE]) for i in range(max(len(toks) - SHINGLE + 1, 0))}


def _overlap(a: set, b: set) -> float:
    return len(a & b) / min(len(a), len(b)) if a and b else 1.0


def _text(html: str) -> str:
    body = _CHROME.sub(" ", html.split("<body", 1)[-1])
    return " ".join(_TAG.sub(" ", body).split())


def _areas(limit: int, smallest_first: bool = False):
    """Named neighbourhoods that clear the index floor. Smallest first is where
    a word floor actually bites."""
    db = SessionLocal()
    try:
        order = "count(*) ASC" if smallest_first else "count(*) DESC"
        rows = db.execute(text(f"""
            SELECT n.name, count(*) AS n
            FROM evictions_raw e
            JOIN neighborhoods n ON n.zip_code = e.zip_code
            WHERE e.eviction_type = 'Residential' AND n.name IS NOT NULL
            GROUP BY 1 HAVING count(*) >= :floor
            ORDER BY {order}, 1
            LIMIT :lim
        """), {"floor": _EV_AREA_MIN, "lim": limit}).fetchall()
        return [(r.name, _ev_area_slug(r.name)) for r in rows]
    finally:
        db.close()


class TestEvictionAreaPages:
    @pytest.mark.needs_data
    def test_pages_exist_and_are_indexable(self):
        areas = _areas(6)
        assert areas, "no neighbourhood clears the eviction floor"
        for name, slug in areas:
            resp = client.get(f"/evictions/{slug}")
            assert resp.status_code == 200, f"/evictions/{slug} -> {resp.status_code}"
            robots = _ROBOTS.search(resp.text)
            assert robots and robots.group(1).startswith("index"), \
                f"/evictions/{slug} has {_EV_AREA_MIN}+ evictions but is {robots.group(1)}"

    def test_pages_clear_the_word_floor(self):
        # Smallest-first: the thin end is what a floor is for.
        thin = []
        for name, slug in _areas(6, smallest_first=True):
            n = len(_WORD.findall(_text(client.get(f"/evictions/{slug}").text)))
            if n < MIN_WORDS:
                thin.append(f"/evictions/{slug}: {n} words")
        assert not thin, (
            f"eviction pages under the {MIN_WORDS}-word floor:\n  " + "\n  ".join(thin)
        )

    def test_pages_are_not_near_duplicates(self):
        """Largest and smallest together: a template that only holds up on the
        big neighbourhoods is a template that fails on 100 of the 127."""
        picked = _areas(3) + _areas(3, smallest_first=True)
        grams = {slug: _shingles(client.get(f"/evictions/{slug}").text)
                 for _, slug in picked}
        slugs = list(grams)
        dupes = []
        for i, a in enumerate(slugs):
            for b in slugs[i + 1:]:
                o = _overlap(grams[a], grams[b])
                if o >= MAX_OVERLAP:
                    dupes.append(f"{a} vs {b}: {o:.0%}")
        assert not dupes, (
            f"eviction pages are near-duplicates (limit {MAX_OVERLAP:.0%}):\n  "
            + "\n  ".join(dupes)
        )

    def test_thin_areas_are_noindex(self):
        db = SessionLocal()
        try:
            row = db.execute(text("""
                SELECT n.name FROM evictions_raw e
                JOIN neighborhoods n ON n.zip_code = e.zip_code
                WHERE e.eviction_type = 'Residential' AND n.name IS NOT NULL
                GROUP BY 1 HAVING count(*) < :floor AND count(*) > 0
                ORDER BY count(*) DESC LIMIT 1
            """), {"floor": _EV_AREA_MIN}).first()
        finally:
            db.close()
        if not row:
            pytest.skip("no below-floor neighbourhood in current data")
        resp = client.get(f"/evictions/{_ev_area_slug(row.name)}")
        assert resp.status_code == 200
        assert _ROBOTS.search(resp.text).group(1).startswith("noindex"), \
            f"{row.name} is under the floor and must not be indexed"

    def test_multi_zip_names_aggregate_rather_than_compete(self):
        """Bushwick is 11206, 11221 and 11237. One page, all three ZIPs."""
        db = SessionLocal()
        try:
            row = db.execute(text("""
                SELECT n.name, count(DISTINCT n.zip_code) AS zips
                FROM neighborhoods n
                JOIN evictions_raw e ON e.zip_code = n.zip_code
                WHERE e.eviction_type = 'Residential' AND n.name IS NOT NULL
                GROUP BY 1 HAVING count(DISTINCT n.zip_code) > 1
                ORDER BY count(*) DESC LIMIT 1
            """)).first()
            zips = [r.zip_code for r in db.execute(text(
                "SELECT zip_code FROM neighborhoods WHERE name = :n"
            ), {"n": row.name}).fetchall()] if row else []
        finally:
            db.close()
        if not row:
            pytest.skip("no multi-ZIP neighbourhood name in current data")
        html = client.get(f"/evictions/{_ev_area_slug(row.name)}").text
        for z in zips:
            assert f"/neighborhood/{z}" in html, \
                f"{row.name} page omits its own ZIP {z}"

    def test_unknown_slug_is_404_not_a_blank_page(self):
        assert client.get("/evictions/nosuchplacetown").status_code == 404

    @pytest.mark.needs_data
    def test_schema_declares_faq_and_dataset(self):
        name, slug = _areas(1)[0]
        html = client.get(f"/evictions/{slug}").text
        types = []
        for block in re.findall(
            r'<script type="application/ld\+json">(.*?)</script>', html, re.S
        ):
            obj = json.loads(block)
            for node in obj.get("@graph", [obj]) if isinstance(obj, dict) else obj:
                if isinstance(node, dict) and node.get("@type"):
                    types.append(node["@type"])
        assert "FAQPage" in types
        assert "Dataset" in types
        assert "BreadcrumbList" in types


class TestDiscoverability:
    """A sitemap is a hint. A link is a path."""

    @pytest.mark.needs_data
    def test_hub_links_every_qualifying_area(self):
        expected = {slug for _, slug in _areas(500)}
        for lang in ("en", "es"):
            html = client.get(f"/evictions?lang={lang}").text
            found = set(re.findall(r'href="/evictions/([a-z0-9-]+)"', html))
            missing = expected - found
            assert not missing, (
                f"/evictions?lang={lang} omits {len(missing)} area pages, "
                f"e.g. {sorted(missing)[:5]}"
            )

    @pytest.mark.needs_data
    def test_area_page_links_back_to_hub_and_out_to_buildings(self):
        name, slug = _areas(1)[0]
        html = client.get(f"/evictions/{slug}").text
        assert 'href="/evictions"' in html, "no link back to the citywide tracker"
        assert re.search(r'href="/property/\d+"', html), \
            "no link to any of the buildings it names"


class TestBoroughContext:
    """neighborhoods.borough is NULL on all 178 rows. Reading it made borough
    always None, which silently dropped the borough from every lede and removed
    the only links between these 127 pages. Derive it from the ZIP instead."""

    def test_pages_name_their_borough(self):
        missing = []
        for name, slug in _areas(6):
            body = _text(client.get(f"/evictions/{slug}").text)
            if not re.search(rf"evictions in {re.escape(name)}, \w", body):
                missing.append(slug)
        assert not missing, (
            "eviction pages with no borough in the lede: " + ", ".join(missing)
        )

    def test_pages_link_to_other_areas_in_the_same_borough(self):
        bare = []
        for name, slug in _areas(6):
            html = client.get(f"/evictions/{slug}").text
            peers = set(re.findall(r'href="/evictions/([a-z0-9-]+)"', html)) - {slug}
            if len(peers) < 2:
                bare.append(f"{slug}: {len(peers)} peer links")
        assert not bare, (
            "eviction pages that link to no siblings, so the set is 127 "
            "dead ends:\n  " + "\n  ".join(bare)
        )


class TestBoroughTier:
    """The tier between the citywide tracker and the 127 leaves.

    The search exports carry 60+ place variants of "eviction marshal {place}"
    ranking 2 to 43 with almost no clicks. Nothing on the site was *about* a
    borough, so a query naming one landed on a page that did not answer it.
    """

    def _boroughs(self):
        from api.routes.frontend import _EV_BOROUGHS
        return _EV_BOROUGHS

    @pytest.mark.needs_data
    def test_all_five_render_and_are_indexable(self):
        for slug in self._boroughs():
            resp = client.get(f"/evictions/{slug}")
            assert resp.status_code == 200, f"/evictions/{slug} -> {resp.status_code}"
            robots = _ROBOTS.search(resp.text)
            assert robots and robots.group(1).startswith("index"), \
                f"/evictions/{slug} is {robots.group(1) if robots else 'missing robots'}"

    @pytest.mark.needs_data
    def test_they_clear_the_word_floor(self):
        thin = []
        for slug in self._boroughs():
            n = len(_WORD.findall(_text(client.get(f"/evictions/{slug}").text)))
            if n < MIN_WORDS:
                thin.append(f"/evictions/{slug}: {n} words")
        assert not thin, "borough pages under the floor:\n  " + "\n  ".join(thin)

    def test_each_borough_lists_every_neighbourhood_page_it_parents(self):
        """The whole point of the tier. A borough page that lists four of its
        twenty-two neighbourhoods is a hub that leaks."""
        from api.routes.frontend import _EV_BOROUGHS, _borough_zips
        db = SessionLocal()
        try:
            for slug, name in _EV_BOROUGHS.items():
                zips = _borough_zips(name, db)
                expected = {
                    _ev_area_slug(r.name) for r in db.execute(text("""
                        SELECT n.name FROM evictions_raw e
                        JOIN neighborhoods n ON n.zip_code = e.zip_code
                        WHERE e.eviction_type = 'Residential' AND n.name IS NOT NULL
                          AND n.zip_code = ANY(:zips)
                        GROUP BY 1 HAVING count(*) >= :floor
                    """), {"zips": zips, "floor": _EV_AREA_MIN}).fetchall()
                    if _ev_area_slug(r.name)
                }
                found = set(re.findall(r'href="/evictions/([a-z0-9-]+)"',
                                       client.get(f"/evictions/{slug}").text))
                missing = expected - found
                assert not missing, (
                    f"/evictions/{slug} omits {len(missing)} of its own "
                    f"neighbourhood pages, e.g. {sorted(missing)[:5]}"
                )
        finally:
            db.close()

    @pytest.mark.needs_data
    def test_the_citywide_hub_reaches_all_five(self):
        html = client.get("/evictions").text
        found = set(re.findall(r'href="/evictions/([a-z0-9-]+)"', html))
        missing = set(self._boroughs()) - found
        assert not missing, f"/evictions does not link {sorted(missing)}"

    def test_leaves_link_up_to_their_borough(self):
        from api.routes.frontend import _EV_BOROUGHS
        orphans = []
        for name, slug in _areas(6):
            found = set(re.findall(r'href="/evictions/([a-z0-9-]+)"',
                                   client.get(f"/evictions/{slug}").text))
            if not found & set(_EV_BOROUGHS):
                orphans.append(slug)
        assert not orphans, (
            "neighbourhood pages with no path up to their borough: "
            + ", ".join(orphans)
        )

    @pytest.mark.needs_data
    def test_they_are_not_near_duplicates_of_each_other(self):
        grams = {s: _shingles(client.get(f"/evictions/{s}").text)
                 for s in self._boroughs()}
        worst = []
        keys = sorted(grams)
        for i, a in enumerate(keys):
            for b in keys[i + 1:]:
                ov = _overlap(grams[a], grams[b])
                if ov > MAX_OVERLAP:
                    worst.append(f"{a} vs {b}: {ov:.0%}")
        assert not worst, "borough pages too similar:\n  " + "\n  ".join(worst)

    @pytest.mark.needs_data
    def test_totals_match_the_record(self):
        from api.routes.frontend import _EV_BOROUGHS, _borough_zips
        db = SessionLocal()
        try:
            for slug, name in _EV_BOROUGHS.items():
                expected = int(db.execute(text("""
                    SELECT count(*) FROM evictions_raw
                    WHERE eviction_type = 'Residential' AND zip_code = ANY(:zips)
                """), {"zips": _borough_zips(name, db)}).scalar() or 0)
                body = _text(client.get(f"/evictions/{slug}").text)
                assert f"{expected:,} residential eviction" in body, (
                    f"/evictions/{slug} does not state its own total of {expected:,}"
                )
        finally:
            db.close()

    @pytest.mark.needs_data
    def test_the_bronx_takes_its_article(self):
        body = _text(client.get("/evictions/bronx").text)
        assert "in the Bronx" in body
        assert "evictions in Bronx" not in body
