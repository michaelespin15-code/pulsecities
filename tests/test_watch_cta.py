"""
Guard: the neighborhood SSR page carries a working watch-this-block CTA.

This is the search -> view -> subscribe conversion point on the organic landing
pages, so it must post to /api/subscribe for the page's own ZIP, fire the
Plausible conversion event, and render in both languages.
"""

import warnings

from fastapi.testclient import TestClient

from api.main import app

warnings.filterwarnings("ignore")
client = TestClient(app)

ZIP = "11216"


def test_watch_card_present_and_wired_en():
    body = client.get(f"/neighborhood/{ZIP}").text
    assert 'class="watch-card"' in body
    assert 'id="watch-btn"' in body and 'id="watch-email"' in body
    assert "/api/subscribe" in body
    assert f'"{ZIP}"' in body, "the page ZIP must be baked into the subscribe payload"
    assert "plausible('Subscribe'" in body
    assert "Watch this block" in body


def test_watch_card_localized_es():
    body = client.get(f"/neighborhood/{ZIP}?lang=es").text
    assert 'class="watch-card"' in body
    assert "Observa esta zona" in body
    assert "/api/subscribe" in body
    assert "Watch this block" not in body  # no English leaking into the ES page


def test_watch_card_avoids_stale_green():
    # The "no green in risk display" palette rule: the success color must not be
    # one of the retired bright greens.
    body = client.get(f"/neighborhood/{ZIP}").text
    for stale in ("#4ade80", "#22c55e", "#16a34a"):
        assert stale not in body, f"stale green {stale} rendered on the page"
