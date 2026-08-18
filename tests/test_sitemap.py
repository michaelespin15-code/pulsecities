"""
Guard: the sitemap and the robots tag agree, and neither one lets the ~596k
record-less parcels into the index.

This used to assert a flat cap of 10,000 property URLs, as a proxy for "don't
flood the index with doorway pages". The proxy was measuring the wrong thing:
indexing is decided by the `index, follow` that `_build_property_page` renders,
not by whether this file lists a URL, and that rule used to pass on the
ZIP-level score. So 596,432 parcels with no deed, eviction, violation or permit
were indexable the whole time the cap was green.

The checks here are therefore the direct ones. A record-less parcel must be
noindex and unsitemapped. Everything sitemapped must be indexable. Both gates
must come from the same rule, so widening one cannot silently outrun the other.
"""

import re
import warnings

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from api.main import app
from models.database import SessionLocal
from scripts.generate_sitemap import build

warnings.filterwarnings("ignore")
client = TestClient(app)

_FILES = build()
_INDEX = _FILES["sitemap.xml"]
_CHILDREN = {n: x for n, x in _FILES.items() if n != "sitemap.xml"}
_ALL_URLS = "\n".join(_CHILDREN.values())
_PROPERTY_LOCS = re.findall(
    r"<loc>https://pulsecities\.com(/property/\d+)</loc>", _ALL_URLS
)

# The sitemaps spec caps one file at 50,000 URLs. Exceeding it makes the file
# invalid, not merely large.
_SPEC_CAP = 50_000

_ROBOTS = re.compile(r'<meta name="robots" content="([^"]+)"')


def _robots(path: str) -> str:
    resp = client.get(path)
    assert resp.status_code == 200, f"{path} -> {resp.status_code}"
    m = _ROBOTS.search(resp.text)
    assert m, f"{path} renders no robots meta"
    return m.group(1)


class TestSitemapShape:
    def test_index_names_only_files_that_were_written(self):
        named = re.findall(r"<loc>https://pulsecities\.com/([^<]+)</loc>", _INDEX)
        assert named, "sitemap index names no children"
        assert set(named) == set(_CHILDREN), (
            f"index names {sorted(named)} but build produced {sorted(_CHILDREN)}"
        )

    def test_no_child_exceeds_the_spec_cap(self):
        for name, xml in _CHILDREN.items():
            n = xml.count("<url>")
            assert n <= _SPEC_CAP, f"{name} has {n} URLs, over the {_SPEC_CAP} cap"

    def test_property_pages_are_present(self):
        assert _PROPERTY_LOCS, "no /property pages in sitemap"

    def test_lastmod_is_per_url_not_a_blanket_stamp(self):
        # 2,111 of the old 2,159 URLs claimed the same date, which tells a
        # crawler nothing. Property lastmod is now the newest record on the lot.
        stamps = re.findall(r"<lastmod>([\d-]+)</lastmod>",
                            _CHILDREN.get("sitemap-property-1.xml", ""))
        if not stamps:
            pytest.skip("no property sitemap in this build")
        assert len(set(stamps)) > 50, (
            f"only {len(set(stamps))} distinct lastmod values across "
            f"{len(stamps)} property URLs; the stamp is not per-URL"
        )


class TestGatesAgree:
    """The sitemap must never disagree with the robots tag in either direction."""

    def test_sitemapped_properties_are_indexable(self):
        sample = _PROPERTY_LOCS[:: max(1, len(_PROPERTY_LOCS) // 8)][:8]
        assert sample
        for path in sample:
            assert "noindex" not in _robots(path), \
                f"{path} is in the sitemap but renders noindex"

    def test_recordless_parcels_are_noindex(self):
        """The 596,432-parcel case. These carry no building-level record at all,
        render ~429 words that run 81% identical to each other, and must never
        be offered to an index."""
        db = SessionLocal()
        try:
            rows = db.execute(text("""
                SELECT p.bbl FROM parcels p
                JOIN displacement_scores ds ON ds.zip_code = p.zip_code
                WHERE p.address IS NOT NULL AND ds.score IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM ownership_raw o WHERE o.bbl = p.bbl)
                  AND NOT EXISTS (SELECT 1 FROM evictions_raw e WHERE e.bbl = p.bbl)
                  AND NOT EXISTS (SELECT 1 FROM violations_raw v WHERE v.bbl = p.bbl)
                  AND NOT EXISTS (SELECT 1 FROM permits_raw pr WHERE pr.bbl = p.bbl)
                  AND NOT EXISTS (SELECT 1 FROM rs_buildings rs WHERE rs.bbl = p.bbl)
                ORDER BY p.zip_code, p.bbl LIMIT 5
            """)).fetchall()
        finally:
            db.close()
        if not rows:
            pytest.skip("no record-less parcel in current data")
        for r in rows:
            path = f"/property/{r.bbl}"
            assert _robots(path).startswith("noindex"), (
                f"{path} has no building record and is {_robots(path)}; "
                f"a ZIP score is not a reason to index a parcel"
            )
            assert path not in _PROPERTY_LOCS, f"{path} has no records but is sitemapped"

    def test_sitemapped_llcs_are_indexable(self):
        slugs = re.findall(r"<loc>https://pulsecities\.com(/llc/[a-z0-9-]+)</loc>", _ALL_URLS)
        assert slugs, "no /llc pages in sitemap"
        for path in slugs[:: max(1, len(slugs) // 6)][:6]:
            assert "noindex" not in _robots(path), \
                f"{path} is in the sitemap but renders noindex"

    def test_whole_condo_buyers_stay_out(self):
        """A condominium records one deed per unit. Counting lots would read 53
        unit deeds as a 53-property portfolio; the gate counts buildings."""
        db = SessionLocal()
        try:
            row = db.execute(text("""
                SELECT btrim(regexp_replace(lower(party_name_normalized),
                             '[^a-z0-9]+', '-', 'g'), '-') AS slug
                FROM ownership_raw
                WHERE doc_type = 'DEED' AND party_type = '2'
                GROUP BY 1, party_name_normalized
                HAVING count(DISTINCT bbl) >= 10
                   AND count(DISTINCT (substring(bbl, 1, 6) ||
                       CASE WHEN substring(bbl, 7, 4) >= '1001'
                            THEN '0000' ELSE substring(bbl, 7, 4) END)) = 1
                LIMIT 1
            """)).first()
        finally:
            db.close()
        if not row:
            pytest.skip("no whole-condo buyer in current data")
        assert f"/llc/{row.slug}" not in _ALL_URLS, \
            f"{row.slug} is one building of unit deeds and should not be sitemapped"
        assert _robots(f"/llc/{row.slug}").startswith("noindex")
