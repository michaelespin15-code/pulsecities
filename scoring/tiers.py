"""One definition of the displacement score bands.

Low 0-33, Moderate 34-66, High 67-84, Critical 85+. Those four cut points had
been written out by hand in six places: api/routes/frontend.py, briefs.py,
og_images.py, neighborhoods.py, ops.py and scripts/weekly_digest.py. They all
agreed, with nothing keeping them that way, which is failure shape 1 from
docs/ops/failure_patterns.md and the same shape that let `--muted` mean two
different greys and ACRIS staleness mean three different numbers. A drift here
is quiet and ugly: the same score reads "High" on the neighbourhood page and
"Critical" in the Sunday digest, on a site whose whole claim is checkability.

What is deliberately NOT centralised is colour. Each surface maps the same tier
to a different value on purpose, and flattening them would be a regression:
the dark page uses the grey ramp, the map and the digest use the risk ramp
(Low #3E6B54), and the printable brief and digest surfaces use ink weights that
hold contrast on paper. So callers import the band, then apply their own
palette. tests/test_tier_bands.py checks that every one of them still agrees
with this module about which band a score falls in.
"""

CRITICAL = "Critical"
HIGH = "High"
MODERATE = "Moderate"
LOW = "Low"

# Descending by floor. Anything below the last floor is LOW.
BANDS: tuple[tuple[int, str], ...] = (
    (85, CRITICAL),
    (67, HIGH),
    (34, MODERATE),
)

ORDER = (LOW, MODERATE, HIGH, CRITICAL)


def tier(score: float) -> str:
    """Band label for a score, title case: Critical, High, Moderate or Low."""
    for floor, label in BANDS:
        if score >= floor:
            return label
    return LOW


def floor_for(label: str) -> int:
    """Lowest score that still counts as `label`.

    For the queries that ask "how many are High or worse" or "which ZIPs
    crossed into High this week" rather than "which band is this".
    """
    for floor, name in BANDS:
        if name == label:
            return floor
    if label == LOW:
        return 0
    raise KeyError(f"unknown tier: {label!r}")


def sql_tier_counts(column: str = "score") -> str:
    """`COUNT(*) FILTER (...)` expressions, one per band, lowest first.

    ops.py counted the distribution in hand-written SQL, which is the copy most
    likely to be forgotten: it is the only one a Python-side change would not
    obviously touch. Generated from BANDS so it cannot fall out of step.
    """
    parts = []
    floors = [floor for floor, _ in BANDS]
    for i, label in enumerate(ORDER):
        alias = label.lower()
        if i == 0:
            cond = f"{column} < {floors[-1]}"
        elif i == len(ORDER) - 1:
            cond = f"{column} >= {floors[0]}"
        else:
            # ORDER is ascending, BANDS descending; walk BANDS from the bottom.
            lo = floors[len(floors) - i]
            hi = floors[len(floors) - i - 1]
            cond = f"{column} >= {lo} AND {column} < {hi}"
        parts.append(f"COUNT(*) FILTER (WHERE {cond}) AS {alias}")
    return ",\n            ".join(parts)
