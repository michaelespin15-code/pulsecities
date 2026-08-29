"""
An apartment number is a household, and it must be stripped once, in one place.

HPD writes the unit the inspector stood in into the violation description:
"REPAIR THE SMOKE DETECTOR ... LOCATED AT APT 2, 2ND STORY". On a building page
that clause names a specific home, and the building's street address is already
on the page beside it, so the pair identifies a household.

`scripts/block_digest.py` decided this on 2026-08-28 and wrote the reason down:
"printing a stranger's apartment number serves nobody". It stripped the clause in
the email pipeline and nowhere else. The 2026-08-28 PII audit found the web
renderer had no equivalent: `_violation_text` removed the statute citation and
truncated at 110 characters, and passed the unit through.

Measured then: 1,383,988 of 2,093,270 violation rows carry a unit locator across
44,535 BBLs, and a 400-BBL random draw rendered a visible apartment number on
26.5% of pages. 49,684 BBLs carrying violations are in the property sitemap, so
roughly 13,000 indexable pages showed one. Verified live before the fix on
/property/3048710035, which rendered "located at apt 2" under `index, follow`.

The rule now lives in `api/unit_privacy.py` and every surface imports it, so a
third reader cannot quietly reintroduce it. That is the same shape as
api/freshness.py and api/permit_kinds.py, and the reason both of those exist.

The module was called violation_text.py for about an hour, until /eviction-case
turned out to need the same rule in a different shape. The apartment reaches that
page through `evictions_raw.address` instead, because the scraper appends it
("456 FLATBUSH AVE Apt 3B", asserted in tests/test_evictions_scraper.py). Address
plus unit plus an exact execution date identifies one household, across 894 rows.
A file named for violation text is not where anyone would look for that, so it
was renamed for the rule rather than for its first caller.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


class TestTheWebPageStripsIt:
    def test_violation_text_drops_the_unit_clause(self):
        from api.routes.frontend import _violation_text
        cases = [
            "§ 27-2045 HMC: REPAIR OR REPLACE THE SMOKE DETECTOR DEFECTIVE LOCATED AT APT 2, 2ND STORY",
            "ABATE THE INFESTATION CONSISTING OF ROACHES IN THE ENTIRE APARTMENT LOCATED AT APT 1L, 1ST STORY",
            "REPAIR THE BROKEN WINDOW IN APT 6J, 6TH STORY",
            "FILE A REPORT at apt. 3B",
        ]
        for desc in cases:
            out = _violation_text(desc)
            assert "apt" not in out.lower(), f"unit survived: {out!r}"
            assert out.strip(), f"stripping emptied the row for {desc!r}"

    def test_it_still_says_what_the_violation_is(self):
        """Stripping the unit must not strip the instruction."""
        from api.routes.frontend import _violation_text
        out = _violation_text(
            "§ 27-2045 HMC: REPAIR OR REPLACE THE SMOKE DETECTOR DEFECTIVE LOCATED AT APT 2, 2ND STORY"
        )
        assert "smoke detector" in out.lower(), out

    def test_descriptions_without_a_unit_are_untouched(self):
        from api.routes.frontend import _violation_text
        out = _violation_text("§ 27-2005 HMC: PROPERLY REPAIR THE BROKEN OR DEFECTIVE WOOD FLOOR")
        assert "wood floor" in out.lower(), out


class TestOneRuleOneOwner:
    def test_the_regex_is_defined_exactly_once(self):
        """Two copies of a privacy rule is how one of them rots."""
        hits = []
        for d in ("api", "scripts", "scoring", "scheduler"):
            root = REPO / d
            if not root.exists():
                continue
            for path in sorted(root.rglob("*.py")):
                if "__pycache__" in path.as_posix():
                    continue
                src = path.read_text(errors="ignore")
                if re.search(r"^_?UNIT_TAIL\s*=\s*re\.compile", src, re.M):
                    hits.append(path.relative_to(REPO).as_posix())
        assert hits == ["api/unit_privacy.py"], (
            f"the unit-strip regex must be defined once, in api/unit_privacy.py. Found: {hits}"
        )

    def test_both_surfaces_import_it(self):
        for rel in ("api/routes/frontend.py", "scripts/block_digest.py"):
            src = (REPO / rel).read_text()
            assert "unit_privacy import" in src, (
                f"{rel} must import the unit-strip rule rather than carry its own"
            )


class TestTheEvictionCaseAddress:
    """
    /eviction-case takes the unit through a different door. The scraper appends
    it into evictions_raw.address (scrapers/evictions.py, asserted in
    tests/test_evictions_scraper.py), so the page rendered "262 West 136th Street
    Apt #2 Triplex ... Executed August 24, 2026". Address plus unit plus an exact
    date is one household, and 894 rows carry one.

    The strip is at render, not at ingest: the stored row is the city's record
    and other things read it, but nothing needs to publish the unit.
    """

    def test_strip_apartment_removes_the_unit(self):
        from api.unit_privacy import strip_apartment
        for addr, want in [
            ("456 FLATBUSH AVE Apt 3B", "456 FLATBUSH AVE"),
            ("262 West 136th Street Apt #2 Triplex", "262 West 136th Street"),
            ("100 MAIN ST APT. 12F", "100 MAIN ST"),
            ("55 BROADWAY #4A", "55 BROADWAY"),
            ("12 OCEAN PKWY UNIT 6", "12 OCEAN PKWY"),
        ]:
            assert strip_apartment(addr) == want, f"{addr!r} -> {strip_apartment(addr)!r}"

    def test_addresses_without_a_unit_are_untouched(self):
        from api.unit_privacy import strip_apartment
        for addr in ("456 FLATBUSH AVE", "1 WORLD TRADE CENTER", "100 W 12 ST"):
            assert strip_apartment(addr) == addr

    def test_both_renders_use_it(self):
        """
        Grepping for the import alone passes on an unused import, which is what
        the first version of this test did. Both places that print
        evictions_raw.address have to be wrapped.
        """
        src = (REPO / "api" / "routes" / "frontend.py").read_text()
        wrapped = src.count("strip_apartment(r.address)")
        assert wrapped >= 2, (
            "/eviction-case prints evictions_raw.address in two places, the recent "
            f"list and the result card, and both carry the unit (found {wrapped})"
        )
