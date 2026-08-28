"""
A title that never survives truncation is a title nobody reads.

Google renders roughly 580px of a title, which is about 60 characters at typical
widths, and drops the rest. Measured 2026-08-28 over a random 20,000-parcel draw
(ORDER BY md5, per the sampling rule that caught the hotel and the dorm):

    /property titles    mean 71, min 65, max 78, and 100.0% over 60

Not most of them. All of them, by construction: the fixed tail
", {borough} NY {zip}: deeds, evictions, permits | PulseCities" is 41 characters
before the address is added, so no address in New York can fit. Two consequences,
and the second is the one that matters:

  - "| PulseCities" was invisible on every one of ~97,790 pages already, so
    removing it costs nothing that was being displayed.
  - "deeds, evictions, permits" was invisible too, and that is the entire
    differentiator against Zillow and StreetEasy for an address query.

This matters here more than it would elsewhere. The diagnosis in the 2026-08-27
search read is that the site ranks and does not get clicked: 731 clicks on 29,700
impressions, 2.5% CTR, average position 8 to 11. /property is 88% of traffic.

The budget is 60 with a little headroom for the long tail of addresses. This
guard reads the real template rather than a copy of it, so a template change
that reintroduces a fixed suffix fails here rather than six weeks later in a
search export.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BUDGET = 60
MEDIAN_ADDRESS = 17   # measured over parcels; p75 is 19, p90 is 21, max 36

# The literal tail of the /property <title>, everything after the address.
_TITLE_LINE = re.compile(
    r'title\s*=\s*f"\{title_name\}(?P<tail>[^"]*)"'
)


def _property_title_tail() -> str:
    src = (REPO / "api" / "routes" / "frontend.py").read_text()
    m = _TITLE_LINE.search(src)
    assert m, "could not find the /property title template; the grep has rotted"
    # Resolve the interpolations to their realistic widths.
    return (m.group("tail")
            .replace("{borough}", "Brooklyn")       # 8, the modal borough
            .replace("{zip_part}", " 11233"))       # 6, always this shape


def test_property_title_tail_leaves_room_for_an_address():
    """
    The fixed tail plus a median NYC address has to land inside the budget.
    Measured over the parcels table: median address is 17 characters, p75 is 19,
    p90 is 21, max is 36. The median is the right test because the tail is fixed
    and the long tail of addresses will always overflow a little.
    """
    tail = _property_title_tail()
    median_address = "x" * MEDIAN_ADDRESS
    total = len(median_address) + len(tail)
    assert total <= BUDGET, (
        f"a median address renders a {total}-char title against a {BUDGET} budget. "
        f"Fixed tail is {len(tail)} chars: {tail!r}. Everything past ~60 is dropped "
        f"by the search result, so whatever sits at the end is never read."
    )


def test_the_brand_suffix_is_not_in_the_property_title():
    """
    It was never visible on any of the ~97,790 pages, and it cost 14 characters
    that the differentiator needed.
    """
    tail = _property_title_tail()
    assert "PulseCities" not in tail, (
        "the brand suffix is 14 characters that no /property search result has "
        "ever displayed, because the title is over budget before it is reached"
    )


def test_the_differentiator_survives():
    """The words that distinguish this page from Zillow must be inside the budget."""
    tail = _property_title_tail()
    for word in ("deeds", "evictions", "permits"):
        pos = MEDIAN_ADDRESS + tail.index(word) + len(word)
        assert pos <= BUDGET, (
            f"{word!r} ends at character {pos} of a median title, past the {BUDGET} "
            f"the search result renders, so it is never read"
        )
