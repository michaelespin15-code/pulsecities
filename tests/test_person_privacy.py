"""
A private homeowner's name does not belong on an indexable page.

Republishing ACRIS is lawful and that was never the question. The question is
whether this site should print a private individual's name next to their street
address, the price they paid and the date, on a page carrying `index, follow`.
The 2026-08-28 audit found it doing exactly that on 43,212 sitemapped /property
pages, on 57 /evictions pages ranked beside an eviction count, and on 143 /llc
pages that also printed the person's street address.

**It costs almost nothing to stop.** /property ranks on the address: the title is
"{address}, {borough} {zip}: deeds, evictions, permits" and the meta description
is address plus BBL. No owner name appears in either. Person-name queries were 81
impressions and 10 clicks against 29,700 and 731, so 0.27% of impressions.

**The portfolio threshold was tested and abandoned**, and this is the finding
worth not re-deriving. The theory was that a person holding many buildings is a
landlord and fair to name. Of the 102 person-shaped names on /evictions, only 6
hold 5+ buildings citywide and all 6 are misclassified organisations: US Bank
Trust, RCF 2 Acquisition Trust, and four Housing Development Fund Corporations,
which are affordable-housing nonprofits. Every genuine individual holds one
building. There was no landlord whose naming needed protecting.

api/person_privacy.py owns the rule. `_is_buyer_entity` is not it and must not be
substituted: that function answers "may I link this to /llc/{slug}", so it is
False for servicers, trustees, and any name at or over 48 characters where the
source truncates. Ten real organisations on /evictions read as people under it
purely for being long.
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent


class TestTheRule:
    def test_individuals_are_people(self):
        from api.person_privacy import is_natural_person
        for n in ("HUSSAIN SYED", "DRAME ABDOULAYE", "YIN XIAO", "SCHWARTZ CHANAH",
                  "WALKER DELANO", "RITZIU WILLIAM F", "LAURA BEATRICE PATOU TRUSTEE"):
            assert is_natural_person(n), n

    def test_organisations_are_not(self):
        from api.person_privacy import is_natural_person
        for n in ("NORWORTH HOLDINGS LLC", "2 CRESTON LLC",
                  "WELLS FARGO BANK NATIONAL ASSOCIATION",
                  "BFH HOUSING DEVELOPMENT FUND CORPORATION",
                  "US BANK TRUST NATIONAL ASSOCIATION",
                  "RCF 2 ACQUISITION TRUST",
                  "THE WEN JIE CHEN AND SU LING CHEN FAMILY TRUST"):
            assert not is_natural_person(n), n

    def test_truncated_names_are_treated_as_organisations(self):
        """The source cuts at 48 characters and only organisation names get there."""
        from api.person_privacy import is_natural_person
        assert not is_natural_person("HP THIRD STREET PR1 HOUSING DEVELOPMENT FUND COMPA")

    def test_unknown_resolves_to_person(self):
        """Being wrong should redact a company, never publish a person."""
        from api.person_privacy import is_natural_person
        assert is_natural_person("ZZZQQ XYLOPHONE")

    def test_public_name_redacts_only_people(self):
        from api.person_privacy import public_name
        assert public_name("HUSSAIN SYED") == "Private individual"
        assert public_name("NORWORTH HOLDINGS LLC") == "NORWORTH HOLDINGS LLC"


class TestEverySurfaceUsesIt:
    """Three render sites printed person names. Each must consult the rule."""

    def test_frontend_imports_the_rule(self):
        src = (REPO / "api" / "routes" / "frontend.py").read_text()
        assert "person_privacy import" in src, (
            "api/routes/frontend.py renders /property deed rows, the /evictions "
            "owner list and the /llc registered agent, and must consult "
            "api/person_privacy.py before printing any of those names"
        )

    def test_the_rule_has_one_definition(self):
        hits = []
        for d in ("api", "scripts", "scoring", "scheduler"):
            root = REPO / d
            if not root.exists():
                continue
            for path in sorted(root.rglob("*.py")):
                if "__pycache__" in path.as_posix():
                    continue
                if re.search(r"^_ORG_TOKEN\s*=\s*re\.compile", path.read_text(errors="ignore"), re.M):
                    hits.append(path.relative_to(REPO).as_posix())
        assert hits == ["api/person_privacy.py"], f"one owner only. Found: {hits}"

    def test_is_buyer_entity_is_not_used_as_the_privacy_gate(self):
        """
        It answers a linking question, not a privacy one, and returns False for
        every name at or over 48 characters. Using it here would redact ten real
        organisations on /evictions and still miss the point.
        """
        src = (REPO / "api" / "routes" / "frontend.py").read_text()
        assert "public_name" in src or "is_natural_person" in src, (
            "the privacy gate must come from api/person_privacy.py"
        )


@pytest.mark.needs_data
class TestNoPersonSurvivesOnAPage:
    """
    The rule is only worth what the rendered page says, and the first pass at
    this fixed three call sites and left four. A grep cannot find the next one;
    only rendering can. Measured progression on random draws: 16 leaks of 41
    pages after the first three sites were gated, 1 of 48 after the DOF
    owner-of-record line, 0 of 71 once that was a false positive from substring
    matching rather than a real leak.

    Sites this found that the audit's file:line list did not:
      - the "Who owns {address}" prose sentence
      - the visible FAQ answer and its FAQPage JSON-LD twin
      - parcels.owner_name, DOF's owner of record, a different source from
        ACRIS that keeps its comma ("KARIM, ABDOOL")
    """

    def test_no_person_shaped_name_renders_on_a_property_page(self):
        import re
        from sqlalchemy import text
        from fastapi.testclient import TestClient
        from models.database import SessionLocal
        from api.main import app
        from api.person_privacy import is_natural_person

        db, client = SessionLocal(), TestClient(app)
        try:
            rows = db.execute(text("""
                SELECT p.bbl, p.owner_name,
                       array_agg(DISTINCT o.party_name_normalized) AS names
                FROM parcels p
                LEFT JOIN ownership_raw o ON o.bbl = p.bbl
                GROUP BY p.bbl, p.owner_name
                ORDER BY md5(p.bbl || 'privacy')
                LIMIT 60
            """)).fetchall()
        finally:
            db.close()

        leaks, checked = [], 0
        for r in rows:
            names = [n for n in (list(r.names or []) + [r.owner_name])
                     if n and is_natural_person(n)]
            if not names:
                continue
            checked += 1
            page = re.sub(r"<[^>]+>", " ", client.get(f"/property/{r.bbl}").text)
            # Organisations legitimately render, and a person's name is often a
            # substring of one: BBL 3009760019 carries both "SUN GERALD T" and
            # "GERALD T SUN LIVING TRUST", and the trust is correctly published.
            # Remove what may be shown before looking for what may not.
            for org in (n for n in (list(r.names or []) + [r.owner_name])
                        if n and not is_natural_person(n)):
                page = re.sub(re.escape(org), " ", page, flags=re.I)
                page = re.sub(r"\s+".join(map(re.escape, org.split())), " ", page, flags=re.I)
            for n in names:
                # Word-boundary, not substring: "CHAN" matches inside "chance"
                # and produced the other false leak.
                toks = [t for t in re.split(r"[,\s]+", n) if len(t) > 3][:2]
                if len(toks) >= 2 and all(
                        re.search(rf"\b{re.escape(t)}\b", page, re.I) for t in toks):
                    leaks.append((r.bbl, n))
                    break

        assert checked >= 10, f"draw only found {checked} pages with a person; widen it"
        assert not leaks, (
            "these /property pages still print a private individual's name:\n  "
            + "\n  ".join(f"/property/{b}  {n}" for b, n in leaks)
        )
