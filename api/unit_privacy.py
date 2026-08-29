"""
One owner for the rule that a published page must not name a household.

HPD writes the apartment the inspector stood in into the description text:

    § 27-2045 HMC: REPAIR OR REPLACE THE SMOKE DETECTOR DEFECTIVE
    LOCATED AT APT 2, 2ND STORY

On a building page the street address is already printed beside that row, so
the pair identifies one home. The block digest reached this conclusion first and
wrote the reason down plainly: printing a stranger's apartment number serves
nobody. It then stripped the clause in the email pipeline and nowhere else, and
the web renderer went on publishing units on roughly 13,000 indexable pages
until the 2026-08-28 audit read one off /property/3048710035.

That is the failure this module exists to prevent, and it is the same one
api/freshness.py and api/permit_kinds.py were written for: a rule with more
readers than enforcers drifts. The apartment reaches a page in two shapes and both live here:

  strip_unit       the trailing clause inside an HPD violation description
  strip_apartment  the unit appended to evictions_raw.address by the scraper
                   ("456 FLATBUSH AVE Apt 3B"), which /eviction-case renders
                   beside an exact execution date. 894 rows carry one, and
                   address plus unit plus date is a household.

The strip is at render, not at ingest. The stored row is the city's record and
other code reads it; nothing needs to publish the unit.

This file was called violation_text.py for about an hour, until /eviction-case
turned out to need the same rule in a different shape. It is named for the rule
now rather than for its first caller. Do not copy either pattern into a third
file; tests/test_violation_unit_privacy.py fails if you do.
"""

import re

# "located at apt 1L, 1st story", "in apt 6J", "at apt. 3B". Everything from the
# locator to the end of the string goes: HPD puts the location clause last, and
# what precedes it is the instruction to the landlord, which is the half worth
# printing.
UNIT_TAIL = re.compile(r"\s+(located at|at apt\.?|in apt\.?)\b.*$", re.I | re.S)


def strip_unit(text: str) -> str:
    """Drop the trailing apartment/location clause from an HPD description."""
    return UNIT_TAIL.sub("", text or "").strip()


# The unit as it is appended to an address rather than to a sentence:
# "456 FLATBUSH AVE Apt 3B", "55 BROADWAY #4A", "12 OCEAN PKWY UNIT 6".
# A bare "#" needs its own branch because it is not a word character, so \b
# after it does not mean what it means after "apt".
_APT_TAIL = re.compile(r"\s+(?:(?:apt|apartment|unit|ste|suite)\b\.?|#).*$", re.I)


def strip_apartment(address: str) -> str:
    """An address without the apartment appended to it."""
    return _APT_TAIL.sub("", address or "").strip()
