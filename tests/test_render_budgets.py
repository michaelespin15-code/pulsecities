"""Cold-render budgets for the pages a visitor can land on.

/flips took **38 seconds** to render cold and nothing noticed for weeks. The
cause is worth stating because it is not the usual one: the page did not get
slower by being neglected, it got slower by being *fixed*. The DOB NOW backfill
made the renovation-permit rule match 96% of the record instead of 4%, and the
flip query, which scanned a year of permits_raw and threw away everything that
was not an LLC purchase, went from cheap-and-wrong to correct-and-unusable.

Every layer of caching hid it. The in-process cache is an hour, nginx holds ten
minutes, so a developer clicking the page sees 60ms and only an unlucky visitor
or a crawler ever pays. The one measurement that shows it is a cold render, and
nothing was taking one.

These budgets are deliberately loose. This box has two vCPUs and a concurrent
test run or a nightly scrape inflates every timing here, so a tight budget would
be a flaky test that gets deleted, which is worse than no test. What they catch
is the order-of-magnitude regression: 1 second becoming 30.
"""

import time
import warnings

import pytest
from fastapi.testclient import TestClient

from api.main import app

warnings.filterwarnings("ignore")
client = TestClient(app)

# Generous on purpose. See the module docstring.
BUDGET_SECONDS = 8.0


def _clear_caches():
    """Every page here memoises itself; a budget on a warm render measures a
    dict lookup.

    The module keeps both shapes: a page with one body holds a tuple or None, a
    page with one body per key holds a dict. Emptying a dict by assigning None
    does not clear it, it breaks the next request, so each is cleared the way its
    own type wants.
    """
    import api.routes.flips as flips
    import api.routes.frontend as fe

    flips._cache = None
    for attr in [a for a in dir(fe) if a.endswith("_cache")]:
        current = getattr(fe, attr)
        if isinstance(current, dict):
            current.clear()
        else:
            setattr(fe, attr, None)


def _live_zip():
    from sqlalchemy import text

    from models.database import SessionLocal
    db = SessionLocal()
    try:
        row = db.execute(text(
            "SELECT zip_code FROM displacement_scores WHERE score IS NOT NULL "
            "ORDER BY score DESC LIMIT 1")).first()
        return row[0] if row else None
    finally:
        db.close()


@pytest.mark.integration
@pytest.mark.needs_data
@pytest.mark.parametrize("path", ["/flips", "/this-week", "/evictions",
                                  "/evictions/brooklyn", "/displacement"])
def test_page_renders_cold_inside_the_budget(path):
    _clear_caches()
    started = time.monotonic()
    resp = client.get(path)
    elapsed = time.monotonic() - started
    assert resp.status_code == 200, f"{path} returned {resp.status_code}"
    assert elapsed < BUDGET_SECONDS, (
        f"{path} took {elapsed:.1f}s to render cold, over the {BUDGET_SECONDS}s "
        f"budget. Nobody sees this until the caches expire, and then one visitor "
        f"pays all of it."
    )


@pytest.mark.integration
@pytest.mark.needs_data
def test_the_neighbourhood_page_renders_cold_inside_the_budget():
    zip_code = _live_zip()
    if not zip_code:
        pytest.skip("no scored ZIPs")
    _clear_caches()
    started = time.monotonic()
    resp = client.get(f"/neighborhood/{zip_code}")
    elapsed = time.monotonic() - started
    assert resp.status_code == 200
    assert elapsed < BUDGET_SECONDS, (
        f"/neighborhood/{zip_code} took {elapsed:.1f}s cold, over {BUDGET_SECONDS}s"
    )


@pytest.mark.integration
@pytest.mark.needs_data
def test_the_flip_query_itself_stays_cheap():
    """The query behind /flips, /this-week, the homepage docket and both pulse
    endpoints. It is the one that regressed, so it is measured directly rather
    than through a page that could mask it with a cache."""
    from models.database import SessionLocal

    import api.routes.flips as flips
    flips._cache = None
    db = SessionLocal()
    try:
        flips._deed_anchor(db)          # warm the anchor; it is its own query
        started = time.monotonic()
        rows = flips.query_flips(db)
        elapsed = time.monotonic() - started
    finally:
        db.close()
    assert rows, "the flip feed came back empty"
    assert elapsed < 5.0, (
        f"query_flips took {elapsed:.1f}s. It reads a year of permits_raw, so it "
        f"must stay scoped to the LLC purchases it will actually join against."
    )
