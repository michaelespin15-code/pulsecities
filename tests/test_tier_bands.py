"""
Guards the displacement score bands against being written down twice.

Low 0-33, Moderate 34-44, High 45-54, Critical 55+ had been hand-written in six
places: frontend.py, briefs.py, og_images.py, neighborhoods.py, ops.py and
weekly_digest.py. They agreed, with nothing keeping them that way. That is
failure shape 1 from docs/ops/failure_patterns.md, and its consequence here is
specific and embarrassing: the same ZIP reads "High" on its neighbourhood page
and "Critical" in Sunday's email, on a site whose entire pitch is that you can
check it.

ops.py was the copy most at risk. Its bands lived in hand-written SQL, so a
change to every Python tier function would have left the ops dashboard counting
to different cut points than the pages it reports on, silently.

Colour is deliberately not unified and is not checked here beyond completeness:
the dark page uses the grey ramp, the map card and the digest use the risk ramp,
and the print surfaces use ink weights that hold contrast on paper. Each surface
keeps its own palette and maps it through the shared band.
"""

import re
from pathlib import Path

import pytest

from scoring.tiers import BANDS, ORDER, sql_tier_counts, tier

ROOT = Path(__file__).parent.parent

# Every value where a band could be got wrong, plus the ordinary middles.
EDGES = [0, 1, 33, 33.9, 34, 34.1, 40, 44.9, 45, 45.1, 50, 54.9, 55, 55.1, 63, 100]


def test_bands_are_ordered_and_complete():
    floors = [f for f, _ in BANDS]
    assert floors == sorted(floors, reverse=True), "BANDS must be descending by floor"
    assert len(ORDER) == len(BANDS) + 1, "ORDER must name one more label than BANDS"
    assert set(ORDER) == {label for _, label in BANDS} | {"Low"}


@pytest.mark.parametrize("score,expected", [
    (0, "Low"), (33.9, "Low"), (34, "Moderate"), (44.9, "Moderate"),
    (45, "High"), (54.9, "High"), (55, "Critical"), (100, "Critical"),
])
def test_canonical_boundaries(score, expected):
    assert tier(score) == expected


def test_every_surface_agrees_with_the_shared_band():
    """Each module keeps its own palette; none may keep its own thresholds."""
    from api.routes.ai_summary import _tier as ai_tier
    from api.routes.briefs import _score_tier as brief_tier
    from api.routes.frontend import _tier_info
    from api.routes.og_images import _score_tier as og_tier
    from scripts.weekly_digest import _score_color, _tier_ink

    for s in EDGES:
        want = tier(s)
        assert _tier_info(s)[0] == want, f"frontend disagrees at {s}"
        assert brief_tier(s)[0] == want, f"briefs disagrees at {s}"
        assert og_tier(s) == want.upper(), f"og_images disagrees at {s}"
        assert ai_tier(s) == want, f"ai_summary disagrees at {s}"
        # The colour surfaces have no label to compare, so check they at least
        # resolve every band to a distinct colour rather than falling through.
        assert _score_color(s), f"digest colour missing at {s}"
        assert _tier_ink(s), f"digest ink missing at {s}"


def test_digest_palettes_cover_every_band():
    from scripts.weekly_digest import _score_color, _tier_ink

    for fn in (_score_color, _tier_ink):
        colours = {fn(s) for s in EDGES}
        assert len(colours) == len(ORDER), (
            f"{fn.__name__} produced {len(colours)} colours for {len(ORDER)} bands; "
            f"a band is sharing or missing a colour"
        )


def test_generated_sql_matches_the_bands():
    sql = sql_tier_counts("score")
    assert "score < 34" in sql
    assert "score >= 34 AND score < 45" in sql
    assert "score >= 45 AND score < 55" in sql
    assert "score >= 55" in sql
    for label in ORDER:
        assert f"AS {label.lower()}" in sql


def test_no_module_hardcodes_the_bands_again():
    """The point of the module is that these numbers appear in one file."""
    pattern = re.compile(r">=\s*(85|67|34)\b")
    offenders = []
    for rel in ["api/routes/frontend.py", "api/routes/briefs.py",
                "api/routes/og_images.py", "api/routes/neighborhoods.py",
                "api/routes/ops.py", "api/routes/ai_summary.py",
                "scripts/weekly_digest.py"]:
        path = ROOT / rel
        for i, line in enumerate(path.read_text().splitlines(), 1):
            if pattern.search(line) and "tiers" not in line:
                offenders.append(f"{rel}:{i}: {line.strip()[:80]}")
    assert not offenders, (
        "score bands written out again instead of imported from scoring.tiers:\n"
        + "\n".join(offenders)
    )


@pytest.mark.integration
@pytest.mark.needs_data
def test_every_band_is_reachable_by_a_real_zip():
    """The check that was missing for as long as the bands were wrong.

    They were 34 / 67 / 85 while the highest score New York has ever produced is
    63.2, so the top two bands could not be occupied. The map rendered in two
    colours, the og card printed "0 ZIPs at High risk", and the weekly digest
    could not report a crossing into High because the floor sat above the
    maximum. Every one of those surfaces was working exactly as written.

    A band with nobody in it is either a band set against the wrong distribution
    or a signal that has died. Both are worth a red test.
    """
    from sqlalchemy import text

    from models.database import SessionLocal
    db = SessionLocal()
    try:
        scores = [float(r[0]) for r in db.execute(text(
            "SELECT score FROM displacement_scores WHERE score IS NOT NULL"))]
    finally:
        db.close()
    if len(scores) < 50:
        pytest.skip("not enough scored ZIPs to judge the distribution")

    counts = {label: sum(1 for s in scores if tier(s) == label) for label in ORDER}
    empty = [label for label, n in counts.items() if n == 0]
    assert not empty, (
        f"no ZIP falls in {empty}. Highest score is {max(scores):.1f} against a "
        f"{[f for f, _ in BANDS][0]} floor for {BANDS[0][1]}. Distribution: {counts}"
    )

    # The top band is meant to be the sharp end, not a third of the city.
    top = counts[ORDER[-1]] / len(scores)
    assert top <= 0.15, (
        f"{top:.0%} of ZIPs are {ORDER[-1]}, which makes the label meaningless"
    )
