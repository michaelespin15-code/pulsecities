"""
Guards for /eviction-case, the docket and index-number lookup.

Built because the search export carries "nyc marshal docket number search" and a
run of address queries ending in "eviction cases": people holding a piece of
paper with a number on it. The site had all 42,567 of those numbers and no way
to type one in.

Three things here are easy to break and expensive to break.

The number formats. Marshal dockets are stored with inconsistent leading zeros,
065592 and 64865 in the same export, so a tenant typing what they see has to
match either way. Index numbers arrive as 312756/24, sometimes with a hyphen or
a stray space.

The robots rule. The empty form is a landing page and indexes; a result is one
row of a public dataset, and letting a crawler walk 37,905 docket numbers would
be a doorway flood with a search box on it.

And the shared cache. nginx keys SSR pages on path plus ?lang alone, so this
route opts out in its location block. Nothing here can test nginx, but the test
below documents why the opt-out exists so it is not deleted as clutter.
"""

import re
import warnings
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from api.main import app
from models.database import SessionLocal

warnings.filterwarnings("ignore")
client = TestClient(app)
ROOT = Path(__file__).parent.parent
_TAG = re.compile(r"<[^>]+>")


def _text(html: str) -> str:
    return " ".join(_TAG.sub(" ", html.split("<body", 1)[-1]).split())


@pytest.fixture(scope="module")
def sample():
    """A real executed eviction, so the format tests use live data shapes."""
    db = SessionLocal()
    try:
        row = db.execute(text("""
            SELECT docket_number, court_index_number, address
            FROM evictions_raw
            WHERE docket_number IS NOT NULL AND court_index_number IS NOT NULL
              AND address IS NOT NULL
            ORDER BY executed_date DESC LIMIT 1
        """)).first()
    finally:
        db.close()
    if not row:
        pytest.skip("no eviction records in this database")
    return row


class TestTheForm:
    def test_landing_page_renders_and_indexes(self):
        r = client.get("/eviction-case")
        assert r.status_code == 200
        assert 'name="robots" content="index, follow"' in r.text
        assert len(_text(r.text).split()) >= 420, "landing page is thin"

    def test_landing_page_canonical_has_no_query(self):
        r = client.get("/eviction-case")
        assert 'rel="canonical" href="https://pulsecities.com/eviction-case"' in r.text

    def test_result_pages_are_noindex(self, sample):
        r = client.get("/eviction-case", params={"q": sample.docket_number})
        assert 'name="robots" content="noindex, follow"' in r.text, (
            "a result page must not be indexable; 37,905 dockets is a doorway flood"
        )

    def test_no_match_says_so_without_pretending_certainty(self):
        body = _text(client.get("/eviction-case", params={"q": "999999999"}).text)
        assert "Nothing on 999999999" in body
        assert "never reached an execution" in body, (
            "an empty result must explain that most cases end without one"
        )


class TestNumberFormats:
    def test_docket_matches_with_and_without_leading_zeros(self, sample):
        bare = sample.docket_number.lstrip("0")
        padded = sample.docket_number
        a = _text(client.get("/eviction-case", params={"q": bare}).text)
        b = _text(client.get("/eviction-case", params={"q": padded}).text)
        assert "on this number" in a and "on this number" in b
        assert "Nothing on" not in a and "Nothing on" not in b

    def test_index_number_matches_slash_hyphen_and_spaces(self, sample):
        idx = sample.court_index_number
        for variant in (idx, idx.replace("/", "-"), idx.replace("/", " / ")):
            body = _text(client.get("/eviction-case", params={"q": variant}).text)
            assert "on this number" in body, f"index variant not matched: {variant!r}"

    def test_a_match_links_to_the_building(self, sample):
        r = client.get("/eviction-case", params={"q": sample.court_index_number})
        assert "/property/" in r.text or "no tax lot number" in _text(r.text)


class TestSafety:
    def test_query_is_escaped(self):
        r = client.get("/eviction-case", params={"q": "<script>alert(1)</script>"})
        assert "<script>alert(1)</script>" not in r.text
        assert "&lt;script&gt;" in r.text

    def test_no_tenant_names_are_promised_or_shown(self):
        """The dataset carries no names. The page says so, and that claim is
        worth a test because it is the first thing a tenant will worry about."""
        body = _text(client.get("/eviction-case").text)
        assert "no tenant names" in body.lower()

    def test_long_input_is_truncated_not_rejected(self):
        r = client.get("/eviction-case", params={"q": "9" * 500})
        assert r.status_code == 200


class TestWiring:
    def test_nginx_opts_this_route_out_of_the_shared_cache(self):
        """The SSR cache keys on path plus ?lang, so without this the form's
        cached empty state would be served for every lookup."""
        conf = (ROOT / "deploy" / "nginx-pulsecities.conf").read_text()
        block = conf.split("location = /eviction-case {", 1)
        assert len(block) == 2, "no nginx location for /eviction-case"
        assert "proxy_cache off;" in block[1].split("}", 1)[0]

    def test_the_evictions_page_points_at_it(self):
        for params in ({}, {"lang": "es"}):
            html = client.get("/evictions", params=params).text
            assert 'href="/eviction-case"' in html, (
                f"the evictions page does not link the lookup ({params})"
            )

    def test_every_surface_that_shows_an_eviction_offers_the_lookup(self):
        """Someone holding case paperwork lands on one of these, not on the
        lookup itself. /evictions/{name} is 127 pages and /property is where an
        address search ends, so both carry it; the two sibling tenant tools
        cross-link it the way they already cross-link each other."""
        surfaces = {
            "/evictions": {},
            "/evictions/bushwick": {},
            "/who-owns-my-building": {},
            "/is-my-building-rent-stabilized": {},
        }
        for path, params in surfaces.items():
            html = client.get(path, params=params).text
            assert 'href="/eviction-case"' in html, f"{path} does not offer the lookup"

    def test_a_property_with_evictions_offers_the_lookup(self):
        db = SessionLocal()
        try:
            bbl = db.execute(text("""
                SELECT bbl FROM evictions_raw WHERE bbl IS NOT NULL
                GROUP BY bbl ORDER BY count(*) DESC LIMIT 1
            """)).scalar()
        finally:
            db.close()
        if not bbl:
            pytest.skip("no eviction has a BBL in this database")
        assert 'href="/eviction-case"' in client.get(f"/property/{bbl}").text

    def test_nginx_301s_the_trailing_slash(self):
        """Every other content route 301s its slash form. This one 404'd."""
        conf = (ROOT / "deploy" / "nginx-pulsecities.conf").read_text()
        rule = [l for l in conf.splitlines() if 'who-owns-my-building|' in l]
        assert rule, "trailing-slash redirect rule not found"
        assert "eviction-case|" in rule[0], "/eviction-case/ is not in the 301 list"

    def test_it_is_in_the_sitemap_source(self):
        src = (ROOT / "scripts" / "generate_sitemap.py").read_text()
        assert '"/eviction-case"' in src
