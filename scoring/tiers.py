"""One definition of the displacement score bands.

Low 0-33, Moderate 34-44, High 45-54, Critical 55+. Those cut points had been
written out by hand in six places: api/routes/frontend.py, briefs.py,
og_images.py, neighborhoods.py, ops.py and scripts/weekly_digest.py. They all
agreed, with nothing keeping them that way, which is failure shape 1 from
docs/ops/failure_patterns.md and the same shape that let `--muted` mean two
different greys and ACRIS staleness mean three different numbers. A drift here
is quiet and ugly: the same score reads "High" on the neighbourhood page and
"Critical" in the Sunday digest, on a site whose whole claim is checkability.

**The bands were recalibrated on 2026-08-28, and the reason is worth keeping.**
They had been 34 / 67 / 85, and no ZIP in New York has ever scored above 63.2.
The top two bands were unreachable, so the map rendered in two colours, 73 of
177 ZIPs sat in one amber block, and the South Bronx read the same as mid-pack
Queens. /og cards printed "0 ZIPs at High risk" citywide, and the weekly digest
could never report a crossing into High because the floor was above the maximum.

The ceiling is not a bug in the scoring. The composite is a weighted blend of
five signals that are each normalised across the city, and no neighbourhood is
at the extreme of all five at once: 10457 ranks first citywide on both evictions
and HPD violations and still lands at 54.9, because LLC acquisitions and permits
carry 55% of the weight and it is mid-pack on both. A blend of imperfectly
correlated signals compresses toward the middle. That is arithmetic, not a
defect, and 85 was never a score the composite could produce.

The cut points below come from the live distribution rather than from a
hypothetical 0-100 spread (n=177: min 1.0, max 63.2, median 31.6, p75 41.5,
p90 50.0, p95 54.4):

    Low       under 34   104 ZIPs, 59%   the calmer half of the city
    Moderate  34 to 44    40 ZIPs, 23%
    High      45 to 54    26 ZIPs, 15%
    Critical  55 and up    7 ZIPs,  4%   Wakefield, Fordham, University Heights,
                                         Longwood, Harlem, and two Bed-Stuy ZIPs

34 is kept from the old set deliberately: it is the one boundary already
published in the legend, the digests and the SSR copy, and the half of the city
below it has genuinely less measured pressure. Only the two unreachable cut
points moved.

**If the signal set changes, these move again.** tests/test_tier_bands.py has a
guard that fails when the top band is empty against live data, which is the
check that was missing for as long as this was wrong. Colour is deliberately
NOT centralised: each surface maps the same band to its own palette on purpose
(the dark page uses the grey ramp, the map and digest use the risk ramp, print
surfaces use ink weights that hold contrast on paper), so callers import the
band and apply their own colours.
"""

CRITICAL = "Critical"
HIGH = "High"
MODERATE = "Moderate"
LOW = "Low"

# Descending by floor. Anything below the last floor is LOW.
BANDS: tuple[tuple[int, str], ...] = (
    (55, CRITICAL),
    (45, HIGH),
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
