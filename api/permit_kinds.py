"""One answer to "is this a permit to renovate an existing building".

There were four readers and one answer, and the answer stopped being true.

`permits_raw` is written by two scrapers now. Legacy BIS (ipu4-2q9a) carries a
DOB job classification in `raw_data->>'job_type'` (A1 major alteration, A2 minor,
A3 minor, NB new building) and, in a separate column, the permit's *trade*
(EW equipment work, PL plumbing, AL alteration). DOB NOW (w9ak-ipjd) carries
neither vocabulary: it spells the job type out, and scrapers/dob_now_permits.py
maps that onto the short codes in `permit_type`.

So `permit_type` holds a trade for a BIS row and a job type for a DOB NOW row.
Both resolve to "AL means work on an existing building", which is why
scoring/compute.py reads it unchanged across the two, but it is an overload and
this module is where it gets written down rather than rediscovered.

**What it cost before anyone wrote it down.** Four queries hand-rolled
`raw_data->>'job_type' IN ('A1', 'A2')`, which matches BIS rows and nothing else.
Once DOB NOW carried 96% of permits, those four were reading 5.5% of the
renovation record: 4,474 permits in a year against 81,486. Flip Watch is one of
them, and it is live at /flips, feeds the homepage docket and generates the
weekly editions. **It found 15 flips in the last 365 days. It should have found
639.**

Same shape as api/freshness.py and scripts/lib/mailer.py: a rule with more
readers than enforcers drifts, so the rule lives here and
tests/test_permit_kind_guards.py greps for the next hand-rolled copy.
"""

# BIS job codes that mean "alteration of an existing building". A3 is
# deliberately absent: it is the minor-work class (a sign, a fence, a
# curb cut), and the four call sites this module replaced all excluded it.
BIS_RENOVATION_JOB_TYPES = ("A1", "A2")
BIS_NEW_BUILDING_JOB_TYPES = ("NB",)

# Short codes scrapers/dob_now_permits.py writes into permit_type. Kept as
# literals rather than imported from the scraper so the API does not depend on
# a scraper module; tests/test_permit_kind_guards.py asserts the two agree.
NOW_RENOVATION_CODES = ("AL",)
NOW_NEW_BUILDING_CODES = ("NB",)


def _in(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


def _clause(alias: str, bis: tuple[str, ...], now: tuple[str, ...]) -> str:
    a = f"{alias}." if alias else ""
    return (
        f"(({a}source = 'dob_now' AND {a}permit_type IN ({_in(now)}))"
        f" OR ({a}source = 'dob_bis' AND {a}raw_data->>'job_type' IN ({_in(bis)})))"
    )


def renovation_sql(alias: str = "") -> str:
    """Predicate: this permit is work on a building that already exists.

    Pass the table alias a query uses, or nothing for an unaliased
    `FROM permits_raw`. Both branches are needed: neither source's vocabulary
    can express the other's, so a single-column predicate silently drops one.

        WHERE {renovation_sql('pr')} AND pr.filing_date >= ...
    """
    return _clause(alias, BIS_RENOVATION_JOB_TYPES, NOW_RENOVATION_CODES)


def new_building_sql(alias: str = "") -> str:
    """Predicate: this permit is for a new building on the lot."""
    return _clause(alias, BIS_NEW_BUILDING_JOB_TYPES, NOW_NEW_BUILDING_CODES)


def renovation_or_new_sql(alias: str = "") -> str:
    """Either of the above, for feeds that show both as construction activity."""
    return _clause(
        alias,
        BIS_RENOVATION_JOB_TYPES + BIS_NEW_BUILDING_JOB_TYPES,
        tuple(dict.fromkeys(NOW_RENOVATION_CODES + NOW_NEW_BUILDING_CODES)),
    )


# What to call each code in front of a reader. BIS distinguishes major from
# minor alteration and DOB NOW does not, so a DOB NOW row cannot honestly claim
# "Major Renovation" and says the thing both sources can support.
KIND_LABELS = {
    "A1": "Major renovation",
    "A2": "Renovation",
    "NB": "New building",
    "AL": "Renovation",
    "DM": "Demolition",
    "NW": "No work",
}


def label(code: str | None) -> str:
    return KIND_LABELS.get((code or "").strip().upper(), "Permit")


def kind_select(alias: str = "") -> str:
    """Expression yielding a label-able code for a row from either source.

    A BIS row keeps its finer A1/A2 split; a DOB NOW row yields its short code.
    Feed the result to `label()`.
    """
    a = f"{alias}." if alias else ""
    return (f"CASE WHEN {a}source = 'dob_bis' THEN {a}raw_data->>'job_type' "
            f"ELSE {a}permit_type END")


# The trade a permit covers, in words. Two vocabularies again: BIS uses its own
# two-letter codes and DOB NOW uses the ones scrapers/dob_now_permits.py builds
# from the per-trade flags, joined with "+" when a job carries several.
#
# This exists because the codes were reaching readers. A building alert said
# "Permit filed (PMM), Mar 23" and a block digest said "(SHD)", which tells a
# tenant nothing. The codes only started surfacing when DOB NOW arrived and
# permits began appearing in those emails at all.
TRADE_LABELS = {
    # DOB NOW
    "GC": "general construction", "STR": "structural", "FND": "foundation",
    "DEM": "demolition", "EW": "earth work", "SOE": "excavation support",
    "PLM": "plumbing", "MECH": "mechanical", "SPR": "sprinklers",
    "BLR": "boiler", "SOL": "solar", "GRN": "green roof",
    "SHD": "sidewalk shed", "SCF": "scaffold",
    "PMM": "site protection", "POA": "place of assembly",
    # Legacy BIS
    "OT": "other", "PL": "plumbing", "EQ": "equipment", "MH": "manual hoist",
    "SP": "sprinklers", "BL": "boiler", "FP": "fire protection",
    "FB": "fuel burning", "SD": "standpipe", "FS": "fuel storage",
    "CC": "curtain wall", "SF": "scaffold", "AN": "antenna",
}


def trade_label(code: str | None) -> str:
    """Readable trade for a work_type, or "" when there is nothing to say.

    Handles the DOB NOW composite form ("GC+STR" -> "general construction and
    structural"). An unrecognised code returns "" rather than itself: a reader
    is better served by "Permit filed." than by "Permit filed (XZ)."
    """
    parts = [p.strip().upper() for p in (code or "").split("+") if p.strip()]
    words = [TRADE_LABELS[p] for p in parts if p in TRADE_LABELS]
    if not words:
        return ""
    if len(words) == 1:
        return words[0]
    return ", ".join(words[:-1]) + " and " + words[-1]
