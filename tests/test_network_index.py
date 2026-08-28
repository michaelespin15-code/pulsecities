"""
Guards for /network, the index of the entity families.

The 26 hub pages shipped without a parent. Nothing linked them but the sitemap
and one line on /llc, so the most distinctive thing on the site was reachable
only by knowing the URL, and a reporter sent to /network/flgsp had no way to
learn the other 25 existed.

What these check is the wiring rather than the prose: that every family is
listed, that the hubs link back, and that the index itself does not become an
orphan the way its children did.
"""

import re
import warnings
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routes.frontend import _families
from models.database import SessionLocal

warnings.filterwarnings("ignore")
client = TestClient(app)
ROOT = Path(__file__).parent.parent
_TAG = re.compile(r"<[^>]+>")


@pytest.fixture(scope="module")
def fams():
    db = SessionLocal()
    try:
        return _families(db)
    finally:
        db.close()


class TestTheIndex:
    def test_it_exists_and_indexes(self):
        r = client.get("/network")
        assert r.status_code == 200
        assert 'name="robots" content="index, follow"' in r.text

    def test_every_family_is_listed(self, fams):
        html = client.get("/network").text
        missing = [s for s in fams if f'href="/network/{s}"' not in html]
        assert not missing, f"families absent from the index: {missing}"

    def test_it_carries_the_method_in_full(self):
        """The long explanation moved here so 26 hubs stop repeating it. If it
        is not here, it is nowhere."""
        body = " ".join(_TAG.sub(" ", client.get("/network").text).split())
        assert "seven in ten" in body, "the corroboration rule is not explained"
        assert "do not say who controls them" in body, "the ownership caveat is gone"

    def test_it_is_substantial(self):
        body = _TAG.sub(" ", client.get("/network").text.split("<body", 1)[-1])
        assert len(body.split()) >= 600


class TestItIsNotAnOrphan:
    @pytest.mark.needs_data
    def test_hubs_link_back_to_it(self, fams):
        slug = next(iter(fams))
        assert 'href="/network"' in client.get(f"/network/{slug}").text

    @pytest.mark.needs_data
    def test_the_llc_directory_links_it(self):
        assert 'href="/network"' in client.get("/llc").text

    def test_it_is_in_the_sitemap_source(self):
        src = (ROOT / "scripts" / "generate_sitemap.py").read_text()
        assert '("/network",' in src

    def test_nginx_serves_it_and_301s_the_slash(self):
        conf = (ROOT / "deploy" / "nginx-pulsecities.conf").read_text()
        assert "location = /network {" in conf, "no nginx location for /network"
        rule = [l for l in conf.splitlines() if "network/[a-z0-9-]+|" in l]
        assert rule and "|network|" in rule[0], "/network/ is not in the 301 list"


class TestTheNumbersAreTheFamilies:
    def test_counts_match_the_clustering(self, fams):
        """The headline stats are computed on the page; if they drift from the
        clustering they are decoration."""
        body = " ".join(_TAG.sub(" ", client.get("/network").text).split())
        n_ent = sum(len(f["entities"]) for f in fams.values())
        assert f"{len(fams)} networks" in body
        assert f"{n_ent} companies" in body
