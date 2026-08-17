"""
Guard on the sort key every paginating scraper walks.

Socrata pages with $limit/$offset, which only returns each row once when the
sort key is stable and unique. scrapers/base.py defaults to :id for that reason.
dob_permits overrode it with `filing_date ASC`, a TEXT column upstream that
thousands of permits share, so rows could shift across a page boundary and be
skipped. It never fired because the window was ~30k rows against a 50k page: a
bug waiting on a row count, which would have surfaced as missing permits and
never as an error.

Failure shape 4 in docs/ops/failure_patterns.md, a fix applied to one site
instead of the class. This test is the class.
"""

import re
from pathlib import Path

import pytest

import scrapers.base as base

_SCRAPERS_DIR = Path(base.__file__).parent

# :id is Socrata's immutable row identifier. A composite ending in :id is also
# stable, since the tiebreak makes the whole key unique.
_STABLE = re.compile(r":id\s*(asc|desc)?\s*$", re.IGNORECASE)

# `paginate(...)` / `_fetch_page(...)` calls that pass an explicit order=.
_ORDER_ARG = re.compile(r"(?:paginate|_fetch_page)\s*\([^)]*?order\s*=\s*([^,)]+)", re.DOTALL)


def _scraper_sources():
    for path in sorted(_SCRAPERS_DIR.glob("*.py")):
        if path.name in {"__init__.py", "base.py"}:
            continue
        yield path


class TestPaginationOrder:
    def test_base_default_is_the_stable_key(self):
        """If this default moves, every scraper inherits the bug at once."""
        import inspect

        sig = inspect.signature(base.BaseScraper.paginate)
        assert sig.parameters["order"].default == ":id"

    @pytest.mark.parametrize("path", list(_scraper_sources()), ids=lambda p: p.name)
    def test_no_scraper_paginates_on_an_unstable_key(self, path):
        source = path.read_text()
        for match in _ORDER_ARG.finditer(source):
            order = match.group(1).strip().strip("\"'")
            if order in {"None", "order"}:
                # None is the aggregate-select case; `order` is a passthrough.
                continue
            assert _STABLE.search(order), (
                f"{path.name} paginates ordered by {order}. Offset pagination "
                f"needs a unique, stable sort key or rows move between pages. "
                f"Drop the override to inherit :id, or append :id as a tiebreak."
            )

    def test_permits_inherits_the_default(self):
        """The site that had the bug, pinned by name so it cannot regress."""
        source = (_SCRAPERS_DIR / "permits.py").read_text()
        assert "order=f\"{DATE_FIELD} ASC\"" not in source
        assert "filing_date ASC" not in source


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
