"""
Guard: the borough and this-week pages get dynamic OG cards, not the static
default. Catches route-precedence regressions (a single-segment /og/x.png is
swallowed by /og/{zip_code}.png) and broken renders.
"""

import re
import warnings

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routes.og_images import _DEFAULT_IMAGE

warnings.filterwarnings("ignore")
client = TestClient(app)

_DEFAULT_BYTES = _DEFAULT_IMAGE.read_bytes()


def _png(path: str):
    r = client.get(path)
    assert r.status_code == 200, f"{path} -> {r.status_code}"
    assert r.headers["content-type"] == "image/png", f"{path} not a png"
    return r.content


def test_borough_card_is_dynamic():
    body = _png("/og/borough/brooklyn.png")
    assert body != _DEFAULT_BYTES, "borough card fell back to the default image"


def test_this_week_card_is_dynamic():
    body = _png("/og/this-week/card.png")
    assert body != _DEFAULT_BYTES, "this-week card fell back to the default image"


def test_invalid_borough_slug_uses_default():
    body = _png("/og/borough/not-a-borough.png")
    assert body == _DEFAULT_BYTES, "unknown borough should serve the default image"


@pytest.mark.parametrize("route,expected", [
    # /borough/brooklyn is the app path; nginx exposes it at /brooklyn.
    ("/borough/brooklyn", "https://pulsecities.com/og/borough/brooklyn.png"),
    ("/this-week", "https://pulsecities.com/og/this-week/card.png"),
])
def test_page_references_dynamic_card(route, expected):
    html = client.get(route).text
    og = re.search(r'<meta property="og:image" content="([^"]+)"', html)
    tw = re.search(r'<meta name="twitter:image" content="([^"]+)"', html)
    assert og and og.group(1) == expected, f"{route} og:image = {og and og.group(1)}"
    assert tw and tw.group(1) == expected, f"{route} twitter:image = {tw and tw.group(1)}"
