"""
The robots tag and the sitemap have to admit the same pages.

They never did. Measured 2026-08-28:

    routable (HTTP 200)        922,737
    rendering "index, follow"  229,286
    in the sitemap              97,790
    the gap                    131,496

The sitemap admits a building carrying a deed, an eviction, or five or more
violations. `has_signals` admitted any document, any eviction, any violation at
all, a rent-stabilization registration, or anything in the twelve-month permit,
deed or eviction windows. Two predicates for one question, written apart.

What sat in the gap, and why it matters more than its size:

    90,001  1-4 violations only    all index,follow. 66.3% mean 5-gram overlap
                                   with 30 of 190 pairs over 70% and a max of
                                   80.7%, which is worse than every tier the
                                   site actually publishes, and worse than the
                                   deed-only tier it debated cutting
    20,494  permit only (365d)
    10,995  non-deed ACRIS doc only
     3,531  rent-stab registration only, carrying ZERO data rows

**This is the second narrowing, not the first.** The predicate used to pass on
`score is not None`, which is ZIP-level, so 596,432 parcels with no record of
their own were telling Google to index 429 words of boilerplate. That was found
and fixed. The fix was directionally right and stopped short of the standard the
sitemap already applied, which is the same one-rule-two-enforcers shape as the
eviction label, the score_history conflict policy and the apartment-number strip.

The window to do this quietly was open: Googlebot had crawled 63,487 distinct
/property BBLs over ~14 days and only 348 of them, 0.5%, were outside the
sitemap. Property pages emit no outbound /property links, so the shadow corpus
was real but essentially uncrawled.

The threshold now has one owner, because a gate that drifts from the sitemap it
is supposed to mirror is how this happened in the first place.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


class TestOneThreshold:
    def test_the_violation_floor_has_a_single_owner(self):
        from config.nyc import INDEX_MIN_VIOLATIONS
        assert INDEX_MIN_VIOLATIONS == 5

    def test_the_sitemap_reads_it_rather_than_defining_it(self):
        src = (REPO / "scripts" / "generate_sitemap.py").read_text()
        assert not re.search(r"^MIN_VIOLATIONS\s*=\s*\d", src, re.M), (
            "the sitemap must not carry its own copy of the floor; the robots "
            "predicate mirrors it and a second definition is how they diverge"
        )
        assert "INDEX_MIN_VIOLATIONS" in src

    def test_the_robots_predicate_reads_it_too(self):
        src = (REPO / "api" / "routes" / "frontend.py").read_text()
        assert "INDEX_MIN_VIOLATIONS" in src, (
            "has_signals must gate on the same floor the sitemap uses"
        )


class TestTheGateMatchesTheSitemap:
    def _predicate(self) -> str:
        src = (REPO / "api" / "routes" / "frontend.py").read_text()
        m = re.search(r"has_signals = bool\(.*?\n    \)", src, re.S)
        assert m, "could not find the has_signals predicate; the grep has rotted"
        return m.group(0)

    def test_it_no_longer_admits_a_bare_violation(self):
        p = self._predicate()
        assert "INDEX_MIN_VIOLATIONS" in p, (
            "one violation is a fact about a building; five is a history. The "
            "90,001 pages carrying 1-4 measured worse than anything sitemapped."
        )

    def test_it_no_longer_admits_a_rent_stab_registration_alone(self):
        p = self._predicate()
        assert '"rs"' not in p and "'rs'" not in p, (
            "3,531 pages qualified on a rent-stabilization registration alone "
            "and carried zero data rows"
        )

    def test_it_no_longer_admits_a_twelve_month_window_alone(self):
        """
        owners / evicts / permits are the 12-month lists. A page indexable
        because a permit was filed 11 months ago is the permit-only block, and
        robots policy should not flip as a window rolls anyway.
        """
        p = self._predicate()
        for name in ("owners", "evicts", "permits"):
            assert not re.search(rf"\bor {name}\b", p), (
                f"{name} is a twelve-month window, not a building-level record"
            )

    def test_it_still_admits_a_deed_and_an_eviction(self):
        p = self._predicate()
        assert "documents" in p and "evictions" in p, (
            "a deed or an eviction is exactly what the sitemap admits"
        )
