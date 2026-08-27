"""
The property page links sideways to other buildings the same owner holds, and
only when the owner is a company.

Two independent measurements asked for this. The 2026-08-27 Search Console
export carries assistant follow-up turns logged as queries: "do they own any
other properties?" at position 2.0 and "what else do they own" at 8.0, both
against a page that carried one link to the owning company and no list of what
it held. Separately, 73% of organic visitors read one page and leave, while the
27% who stay go to another property page 720 times in fifteen days, which they
were doing by returning to Google because the page offered no path.

The same change is the crawl fix. 69,846 property pages were reachable from the
sitemap and almost nothing else, and Googlebot's rate fell from 31,573 requests
a day to 78 once the discovery crawl finished. A sitemap says a page exists;
links say it matters.

The gate is the part to protect. A natural person who bought two houses is not a
portfolio, and a page listing every building a named individual owns is a
people-search directory wearing a displacement-research mission statement. Person
names are the best-converting queries the site has, at 12.3% against a 2.5%
average, which is exactly why the rule needs a test rather than good intentions.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from api.main import app
from api.routes.frontend import _is_buyer_entity, _sibling_buildings
from models.database import SessionLocal


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


@pytest.fixture(scope="module")
def db():
    s = SessionLocal()
    yield s
    s.close()


def _bbl_bought_by(db, entity_like: str, min_addr: int = 3):
    row = db.execute(text("""
        SELECT min(o.bbl) AS bbl
        FROM ownership_raw o JOIN parcels p ON p.bbl = o.bbl
        WHERE o.doc_type = 'DEED' AND o.party_type = '2'
          AND p.address IS NOT NULL
          AND o.party_name_normalized LIKE :pat
        GROUP BY o.party_name_normalized
        HAVING count(DISTINCT p.address) >= :n
        LIMIT 1
    """), {"pat": entity_like, "n": min_addr}).first()
    return row.bbl if row else None


class TestCompanyOwnersGetSideways:
    def test_a_company_portfolio_lists_its_other_buildings(self, db):
        bbl = _bbl_bought_by(db, "%LLC%")
        if not bbl:
            pytest.skip("no multi-address LLC buyer in current data")
        out = _sibling_buildings(bbl, None, db)
        assert out["rows"], f"{bbl} has an LLC buyer with other buildings and no siblings"
        assert out["source"] == "entity"
        assert out["entity"]

    def test_the_page_renders_links_to_those_buildings(self, client, db):
        """The crawl half. A sibling the page computes and does not link is
        worth nothing to a reader or to Googlebot."""
        bbl = _bbl_bought_by(db, "%LLC%")
        if not bbl:
            pytest.skip("no multi-address LLC buyer in current data")
        html = client.get(f"/property/{bbl}").text
        import re
        others = {m for m in re.findall(r'href="/property/(\d+)"', html) if m != bbl}
        assert others, "/property links to no other property page"

    def test_the_section_names_the_record_it_used(self, client, db):
        """A shared deed buyer is a fact about a filing. It is weaker than common
        control and the page must not let a reader round it up."""
        bbl = _bbl_bought_by(db, "%LLC%")
        if not bbl:
            pytest.skip("no multi-address LLC buyer in current data")
        html = client.get(f"/property/{bbl}").text
        assert "What else this owner holds" in html
        assert "not proof that one operation runs them all" in html, (
            "the sideways section dropped its caveat about what a shared buyer means"
        )


class TestPeopleAreNotAPortfolio:
    """The rule that keeps this a landlord tool instead of a people directory."""

    def _non_company_multi_buyer(self, db):
        rows = db.execute(text("""
            SELECT o.party_name_normalized AS name, min(o.bbl) AS bbl
            FROM ownership_raw o JOIN parcels p ON p.bbl = o.bbl
            WHERE o.doc_type = 'DEED' AND o.party_type = '2' AND p.address IS NOT NULL
            GROUP BY 1 HAVING count(DISTINCT p.address) BETWEEN 2 AND 8
            LIMIT 400
        """)).fetchall()
        return [(r.name, r.bbl) for r in rows if not _is_buyer_entity(r.name)]

    def test_a_person_or_trust_gets_no_sibling_list(self, db):
        cases = self._non_company_multi_buyer(db)
        if not cases:
            pytest.skip("no non-company multi-address buyer in current data")
        leaked = []
        for name, bbl in cases[:12]:
            if _sibling_buildings(bbl, None, db)["rows"]:
                leaked.append(f"{bbl} ({name})")
        assert not leaked, (
            "sideways links built for buyers that are not companies. Trusts, "
            "trustees, servicers and private individuals must not get a "
            "portfolio page:\n  " + "\n  ".join(leaked)
        )

    def test_the_rendered_page_shows_nothing_either(self, client, db):
        cases = self._non_company_multi_buyer(db)
        if not cases:
            pytest.skip("no non-company multi-address buyer in current data")
        for _name, bbl in cases[:6]:
            html = client.get(f"/property/{bbl}").text
            assert "What else this owner holds" not in html, (
                f"/property/{bbl} renders an owner portfolio for a non-company buyer"
            )

    def test_the_gate_is_the_same_one_llc_indexing_uses(self):
        """Two tests for "is this a company" would eventually disagree, and the
        disagreement would show up as an indexed page for a private person."""
        assert _is_buyer_entity("SOMETHING HOLDINGS LLC")
        assert not _is_buyer_entity("SMITH, JOHN")
        assert not _is_buyer_entity("ABOUHAMRA TRUSTEE IBRAHIM")
