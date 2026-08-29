"""
There is no page here about a private individual.

/llc/{slug} rendered a profile for any deed party, and 40,532 of the 64,279
distinct deed grantees are person-shaped. Each of those pages ran about 1,500
words: every deed, every address, the prices, the chain of title, an FAQ headed
"Who owns or controls <PERSON>?", and for 200 of them a paragraph noting an
eviction executed in the year before their purchase.

The comment on the route explains why they existed: "Any entity in the deed
record renders on demand so those searches always land." **That intent cannot
work as built.** The pages are noindex, so a search for a person's name has
never been able to land on one.

Measured 2026-08-29 before removing them:

    human hits on /llc/* today          23, and every one was a company slug
    human organic hits on a person page  0
    internal links to a person slug      0, across /llc, /operators,
                                         /displacement, /property, /evictions
                                         and /radar
    person-shaped slugs reachable        40,532

So: forty thousand dossiers on named private individuals, linked from nowhere,
reached by nobody, indexable by no one, each carrying an eviction sequence and a
question about who controls them.

**The decisive argument is not traffic.** /privacy now tells every reader that
the names of private individuals are among the four things this site withholds.
While these pages answered, that was not true. A stated policy the code
contradicts is worse than no policy, because it is the claim a reader relies on.

Companies are unaffected. A company is named in full wherever it appears, which
is the entire point of the site.
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent


class TestTheRouteRefuses:
    def test_the_route_refuses_rather_than_merely_consulting(self):
        """
        Asserted structurally. Two earlier versions of this guard passed with the
        check deleted: a regex bounded on the next top-level def over-captured
        981 lines, and llc_entity_page is genuinely 848 lines and names
        is_natural_person elsewhere inside itself, so "the name appears in the
        body" proves nothing. What has to exist is the statement: a bare
        `if is_natural_person(name): return _not_found()`.

        Sixth false-passing guard in this session, and the reason the behavioural
        test below issues a real request rather than trusting any of this.
        """
        import ast
        src = (REPO / "api" / "routes" / "frontend.py").read_text()
        fn = next((n for n in ast.walk(ast.parse(src))
                   if isinstance(n, ast.FunctionDef) and n.name == "llc_entity_page"), None)
        assert fn, "could not find llc_entity_page; the route was renamed"

        def _is_the_refusal(node) -> bool:
            if not isinstance(node, ast.If) or not isinstance(node.test, ast.Call):
                return False
            fname = getattr(node.test.func, "id", None)
            if fname != "is_natural_person":
                return False
            return any(
                isinstance(st, ast.Return)
                and isinstance(st.value, ast.Call)
                and getattr(st.value.func, "id", None) == "_not_found"
                for st in node.body
            )

        assert any(_is_the_refusal(n) for n in ast.walk(fn)), (
            "llc_entity_page must refuse a person-shaped party outright: "
            "`if is_natural_person(name): return _not_found()`. Consulting the "
            "rule to decide whether to hyperlink a name is a different question "
            "and leaves the dossier rendered."
        )

    @pytest.mark.needs_data
    def test_a_person_slug_404s(self):
        from fastapi.testclient import TestClient
        from api.main import app
        r = TestClient(app).get("/llc/adelman-yaakov")
        assert r.status_code == 404, (
            f"/llc/adelman-yaakov returned {r.status_code}; it renders a dossier "
            f"on a named private individual"
        )

    @pytest.mark.needs_data
    def test_a_company_slug_still_renders(self):
        """The site exists to name companies. This must not touch them."""
        from fastapi.testclient import TestClient
        from api.main import app
        r = TestClient(app).get("/llc/norworth-holdings-llc")
        assert r.status_code == 200
        assert "NORWORTH" in r.text.upper()

    @pytest.mark.needs_data
    def test_the_sitemapped_llc_pages_all_still_resolve(self):
        """
        A 404 on a sitemapped URL is worse than the page it replaced. The /llc
        sitemap gate already admits only entity-form slugs, so this should hold,
        and it is cheap to prove rather than assume.
        """
        import random
        from fastapi.testclient import TestClient
        from api.main import app
        xml = (REPO / "frontend" / "sitemap-core.xml").read_text()
        slugs = re.findall(r"<loc>https://pulsecities\.com(/llc/[a-z0-9-]+)</loc>", xml)
        assert len(slugs) > 100, f"only {len(slugs)} /llc URLs in the sitemap"
        client = TestClient(app)
        random.seed(29)
        for path in random.sample(slugs, 12):
            assert client.get(path).status_code == 200, f"{path} now 404s"
