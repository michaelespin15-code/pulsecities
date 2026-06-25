"""
Flip Watch feed — regression guards for GET /api/flips and the SSR /flips page.

The feed promises a specific, defensible pattern: an LLC took the deed and filed an
A1/A2 renovation permit on the same lot within FLIP_WINDOW_DAYS. Two failure modes
would quietly erode that promise, so they are pinned here:

  1. Window drift — a query change lets in transfers whose permit is outside the
     60-day window (or before the transfer), turning the feed into a generic
     acquisitions list that contradicts the published methodology.
  2. Lender noise — a bank, servicer, or GSE taking title by deed is not an investor
     flip. If the noise filter regresses, those entities leak into a page that reads
     as "operators repositioning buildings."

The unit checks run in CI without a database. The integration checks run against the
live DB and skip gracefully when no flips exist yet.
"""

import pytest

from api.routes.flips import (
    _NOISE_TERMS,
    _NOISE_SQL,
    FLIP_WINDOW_DAYS,
    LOOKBACK_DAYS,
    FEED_LIMIT,
)


def _get_client():
    from fastapi.testclient import TestClient
    from api.main import app
    return TestClient(app)


# ---------------------------------------------------------------------------
# Unit — no database required
# ---------------------------------------------------------------------------

class TestFlipFeedConstants:
    def test_flip_window_matches_published_methodology(self):
        # The README and /methodology both state "within 60 days". The per-ZIP
        # renovation-flip endpoint uses the same bound. Keep them in lockstep.
        assert FLIP_WINDOW_DAYS == 60

    def test_lookback_is_sane(self):
        assert LOOKBACK_DAYS >= FLIP_WINDOW_DAYS
        assert FEED_LIMIT > 0

    def test_every_noise_term_is_compiled_into_the_filter(self):
        # The SQL is generated from _NOISE_TERMS. If a term is added to the list
        # but the SQL is built some other way, this catches the drift.
        for term in _NOISE_TERMS:
            assert f"'%{term}%'" in _NOISE_SQL, f"{term} missing from noise SQL"
        assert _NOISE_SQL.count("NOT ILIKE") == len(_NOISE_TERMS)

    def test_known_lender_terms_are_excluded(self):
        # Regression anchor for the specific entities seen leaking in the data:
        # loan servicers and funding vehicles taking title by deed.
        for term in ("MORTGAGE", "LOAN", "LENDER", "FUNDING", "SERVICING", "BANK"):
            assert term in _NOISE_TERMS


# ---------------------------------------------------------------------------
# Integration — requires a live PostgreSQL database
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestFlipFeedAPI:
    def test_endpoint_returns_200_with_documented_shape(self):
        client = _get_client()
        resp = client.get("/api/flips")
        assert resp.status_code == 200, f"{resp.status_code}: {resp.text}"
        body = resp.json()
        for key in ("window_days", "flip_window_days", "count", "flips"):
            assert key in body, f"missing key {key}"
        assert body["flip_window_days"] == FLIP_WINDOW_DAYS
        assert body["window_days"] == LOOKBACK_DAYS
        assert isinstance(body["flips"], list)
        assert body["count"] == len(body["flips"])
        assert body["count"] <= FEED_LIMIT

    def test_each_flip_has_required_keys(self):
        client = _get_client()
        flips = client.get("/api/flips").json()["flips"]
        if not flips:
            pytest.skip("No flips detected in the current window")
        required = {
            "bbl", "address", "zip_code", "neighborhood", "buyer",
            "doc_amount", "transfer_date", "permit_date", "days_between",
        }
        for f in flips:
            assert required.issubset(f.keys()), f"missing keys: {required - f.keys()}"

    def test_every_flip_is_inside_the_flip_window(self):
        # Core guard: permit filed strictly after the deed, within 60 days.
        client = _get_client()
        flips = client.get("/api/flips").json()["flips"]
        if not flips:
            pytest.skip("No flips detected in the current window")
        for f in flips:
            d = f["days_between"]
            assert d is not None, f"null days_between for {f['address']}"
            assert 0 < d <= FLIP_WINDOW_DAYS, (
                f"{f['address']} buy->permit gap {d}d outside (0, {FLIP_WINDOW_DAYS}]"
            )
            assert f["permit_date"] > f["transfer_date"], (
                f"{f['address']} permit not after transfer"
            )

    def test_no_lender_or_servicer_noise_in_feed(self):
        # Core guard: the buyer is an operator, not a debt entity.
        client = _get_client()
        flips = client.get("/api/flips").json()["flips"]
        if not flips:
            pytest.skip("No flips detected in the current window")
        for f in flips:
            name = (f["buyer"] or "").upper()
            leaked = [t for t in _NOISE_TERMS if t in name]
            assert not leaked, f"lender noise leaked: {f['buyer']} matched {leaked}"

    def test_zip_codes_are_five_digits(self):
        client = _get_client()
        flips = client.get("/api/flips").json()["flips"]
        if not flips:
            pytest.skip("No flips detected in the current window")
        for f in flips:
            assert len(str(f["zip_code"])) == 5 and str(f["zip_code"]).isdigit()


@pytest.mark.integration
class TestFlipWatchPage:
    def test_ssr_page_renders_with_core_markers(self):
        client = _get_client()
        resp = client.get("/flips")
        assert resp.status_code == 200, f"{resp.status_code}: {resp.text[:200]}"
        html = resp.text
        assert "Flip Watch" in html
        assert '<link rel="canonical" href="https://pulsecities.com/flips">' in html
        # The defensibility disclaimer must be present so the page never reads as
        # an accusation about any owner.
        assert "not wrongdoing" in html

    def test_cards_link_to_property_pages(self):
        client = _get_client()
        html = client.get("/flips").text
        # When the feed has rows, each card deep-links to the building record.
        if "flip-row" in html:
            assert "/property/" in html
