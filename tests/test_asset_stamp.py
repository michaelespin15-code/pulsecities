"""
Guards the Tailwind build stamp.

The map app is the one page on the site that links a stylesheet instead of
inlining it, and that stylesheet is written in place to a fixed filename. So
the URL stayed constant while the bytes changed, and a returning visitor could
hold the previous CSS against freshly served HTML. nginx had been paying for
that with a one-hour must-revalidate on the file while every other static asset
got thirty days immutable.

`npm run build:css` now appends the build's content hash to the link, which is
what lets nginx treat a stamped request as immutable. The failure mode that
replaces the old one is a stamp that no longer matches the file: someone runs
tailwindcss directly, or edits the CSS by hand, and ships HTML pointing at a
build that no longer exists at that hash. This is the check for that.
"""

import hashlib
import re
from pathlib import Path

ROOT = Path(__file__).parent.parent
CSS = ROOT / "frontend" / "tailwind.min.css"
APP = ROOT / "frontend" / "app.html"

LINK = re.compile(r'href="/tailwind\.min\.css(\?v=([0-9a-f]+))?"')


def test_app_links_a_stamped_stylesheet():
    m = LINK.search(APP.read_text())
    assert m, "app.html no longer links /tailwind.min.css"
    assert m.group(2), (
        "app.html links the stylesheet without a ?v= stamp. Run `npm run build:css`, "
        "which rebuilds and stamps in one step."
    )


def test_stamp_matches_the_built_css():
    m = LINK.search(APP.read_text())
    stamped = m.group(2)
    actual = hashlib.sha256(CSS.read_bytes()).hexdigest()[: len(stamped)]
    assert stamped == actual, (
        f"app.html points at build {stamped} but tailwind.min.css hashes to {actual}. "
        f"The CSS changed without the stamp being updated; run `npm run build:css`."
    )


def test_no_page_links_the_bare_stylesheet():
    """A bare link would be served with the conservative revalidate policy,
    which is the cost the stamp exists to avoid."""
    offenders = [
        p.name for p in (ROOT / "frontend").glob("*.html")
        if re.search(r'href="/tailwind\.min\.css"', p.read_text())
    ]
    assert not offenders, f"unstamped stylesheet link in {offenders}"
