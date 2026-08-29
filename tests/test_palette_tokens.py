"""
A page may not reference a colour token it never declares.

Measured 2026-08-29: api/routes/frontend.py used var(--...) 195 times and
declared :root twice. Eight templates referenced tokens nothing on the page
defined, and the browser does not fall back to "some sensible colour". It falls
back per property:

    color: var(--dim)           invalid at computed-value time, so the property
                                inherits: dim grey text rendered as body white
    background: var(--surface)  background does not inherit, so it takes its
                                initial value: a panel meant to be #16202d
                                rendered fully transparent
    border-color: var(--line)   initial is currentColor, so a hairline meant to
                                be barely visible rendered at text brightness

That is why /radar and /flips read as washed out beside the landing page while
both bodies were the same #111823. Nothing was broken in a way any test could
see: the markup was valid, the colours were named, and the names meant nothing.

tests/test_text_contrast.py guards the ratios between the palette's colours. It
cannot catch this, because a token that resolves to nothing has no ratio.

Rendered, not grepped. The tokens and the declarations live in different string
literals in different functions, so only the assembled page can be asked.
"""
import re

import pytest

# One page per head template that interpolates _HEAD_TAIL.
PAGES = [
    "/radar", "/flips", "/this-week", "/this-week/archive", "/week/2026-W34",
    "/displacement", "/llc", "/operators", "/evictions", "/neighborhoods",
    "/neighborhood/10457", "/neighborhood/10457?lang=es", "/borough/bronx",
    "/property/1013691075", "/who-owns-my-building", "/eviction-case", "/network",
]

_USED = re.compile(r"var\(\s*(--[a-zA-Z0-9_-]+)")
_DECLARED = re.compile(r"(--[a-zA-Z0-9_-]+)\s*:")


@pytest.fixture(scope="module")
def rendered():
    from fastapi.testclient import TestClient
    from api.main import app

    client = TestClient(app)
    out = {}
    for path in PAGES:
        r = client.get(path)
        assert r.status_code == 200, f"{path} returned {r.status_code}"
        out[path] = r.text
    return out


@pytest.mark.integration
@pytest.mark.needs_data
class TestEveryTokenResolves:
    def test_no_page_uses_a_token_it_does_not_declare(self, rendered):
        offenders = {}
        for path, markup in rendered.items():
            missing = sorted(set(_USED.findall(markup)) - set(_DECLARED.findall(markup)))
            if missing:
                offenders[path] = missing
        assert not offenders, (
            "these pages name colours nothing declares, so each property falls "
            "back on its own terms and the page renders lighter than intended:\n  "
            + "\n  ".join(f"{p}: {', '.join(v)}" for p, v in sorted(offenders.items())))

    def test_the_sweep_actually_reads_tokens(self, rendered):
        """A guard that measures nothing passes forever."""
        counts = {p: len(set(_USED.findall(m))) for p, m in rendered.items()}
        empty = [p for p, n in counts.items() if n == 0]
        assert not empty, f"no var() found at all on {empty}; the pattern has rotted"

    def test_the_palette_reaches_every_page(self, rendered):
        """The tokens arrive through _HEAD_TAIL. If a new template interpolates
        _PLAUSIBLE directly again, it renders without a palette and this says so."""
        missing = [p for p, m in rendered.items() if "--faint:" not in m]
        assert not missing, (
            "no palette declaration on these, so they interpolate the analytics "
            f"snippet without _HEAD_TAIL: {missing}")
