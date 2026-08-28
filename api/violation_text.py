"""
One owner for the rule that an HPD violation must not name a household.

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
readers than enforcers drifts. Import UNIT_TAIL or strip_unit from here. Do not
copy the pattern into a third file; tests/test_violation_unit_privacy.py fails
if you do.
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
