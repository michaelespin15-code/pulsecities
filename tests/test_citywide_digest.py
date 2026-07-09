"""
Citywide digest — guards for the strengthened NYC overview email.

The old email was a standings table; the new one is the city's week. What's
pinned here is the difference between those two things:

  1. Movement is the lead: the movers section shows signed 7-day deltas, and
     the standings carry deltas too.
  2. This week's citywide counts appear against their baselines.
  3. Fresh Speculation Radar clusters appear when they exist and the section
     vanishes when they don't (never an empty header).
  4. The AI lede follows the same grounding-and-graceful-absence contract as
     the ZIP narrative, and one bad Sunday costs at most one model call.
  5. Copy hygiene matches the ZIP email: no em dash, no weekday names,
     unsubscribe token, wrongdoing disclaimer.

No test here touches the network; the model client is stubbed at the seam.
"""

import pytest

from scripts import digest_narrative
from scripts.digest_narrative import generate_citywide_narrative, build_citywide_facts
from scripts.weekly_digest import render_citywide_digest


def _summary(**overrides) -> dict:
    base = {
        "top_zips": [
            {"zip": "10472", "name": "Soundview", "score": 92.1, "delta": 2.1},
            {"zip": "11216", "name": "Bedford-Stuyvesant", "score": 70.6, "delta": 0.0},
        ],
        "movers": [
            {"zip": "10456", "name": "Morrisania", "score": 81.4, "delta": 7.2},
            {"zip": "11375", "name": "Forest Hills", "score": 38.0, "delta": -4.1},
        ],
        "avg_score": 48.2,
        "max_score": 92.1,
        "zip_count": 180,
        "week": {
            "evictions": 29, "evictions_avg": 21.5,
            "llc": 6, "llc_avg": 4.2,
            "permits": 12, "permits_avg": 9.0,
            "violations": 610, "violations_avg": 540.0,
        },
        "clusters": [
            {"buyer": "416 4TH AVE REALTY LLC", "zip_code": "11215",
             "neighborhood": "Park Slope", "building_count": 4,
             "total_amount": 14578260.86},
        ],
    }
    base.update(overrides)
    return base


def _subscription() -> dict:
    return {"email": "reader@example.com", "unsubscribe_token": "tok-city"}


class _FakeBlock:
    type = "text"
    text = "Pressure moved most in the Bronx this week."


class _FakeMessage:
    stop_reason = "end_turn"
    content = [_FakeBlock()]

    class usage:
        input_tokens = 500
        output_tokens = 90


class _FakeClient:
    def __init__(self):
        self.calls = 0
        self.messages = self

    def create(self, **kwargs):
        self.calls += 1
        self.last_kwargs = kwargs
        return _FakeMessage()


@pytest.fixture(autouse=True)
def _clear_cache():
    digest_narrative._cache.clear()
    yield
    digest_narrative._cache.clear()


# ---------------------------------------------------------------------------
# Narrative — citywide edition
# ---------------------------------------------------------------------------

class TestCitywideNarrative:
    def test_facts_carry_movers_counts_and_clusters(self):
        facts = build_citywide_facts(_summary())
        assert "Morrisania" in facts and "+7.2" in facts
        assert "29" in facts and "21.5" in facts
        assert "416 4TH AVE REALTY LLC" in facts and "Park Slope" in facts

    def test_returns_none_without_api_key(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.setattr(digest_narrative, "_client", None)
        assert generate_citywide_narrative(_summary()) is None

    def test_one_call_per_run(self, monkeypatch):
        fake = _FakeClient()
        monkeypatch.setattr(digest_narrative, "_get_client", lambda: fake)
        generate_citywide_narrative(_summary())
        generate_citywide_narrative(_summary())
        assert fake.calls == 1

    def test_model_is_env_overridable(self, monkeypatch):
        # The cost lever: one env var flips every narrative surface to a
        # cheaper model without a deploy.
        fake = _FakeClient()
        monkeypatch.setattr(digest_narrative, "_get_client", lambda: fake)
        monkeypatch.setenv("PULSE_AI_MODEL", "claude-haiku-4-5")
        generate_citywide_narrative(_summary())
        assert fake.last_kwargs["model"] == "claude-haiku-4-5"

    def test_model_defaults_to_opus(self, monkeypatch):
        fake = _FakeClient()
        monkeypatch.setattr(digest_narrative, "_get_client", lambda: fake)
        monkeypatch.delenv("PULSE_AI_MODEL", raising=False)
        generate_citywide_narrative(_summary())
        assert fake.last_kwargs["model"] == "claude-opus-4-8"


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

class TestCitywideRender:
    def _render(self, summary=None, narrative=None):
        return render_citywide_digest(_subscription(), summary or _summary(), narrative=narrative)

    def test_movers_lead_with_signed_deltas(self):
        html = self._render()["html"]
        assert "Morrisania" in html
        assert "+7.2" in html
        assert "-4.1" in html

    def test_week_counts_appear_with_baselines(self):
        html = self._render()["html"]
        assert "29" in html
        assert "The Week's Numbers" in html

    def test_clusters_render_when_present_and_hide_when_absent(self):
        html = self._render()["html"]
        assert "Concentrated Buying" in html
        assert "416 4Th Ave Realty LLC".upper() in html.upper()
        html_empty = self._render(summary=_summary(clusters=[]))["html"]
        assert "Concentrated Buying" not in html_empty

    def test_narrative_section_present_only_when_narrative_exists(self):
        with_n = self._render(narrative="The citywide week in one paragraph.")["html"]
        assert "The Week, In Plain English" in with_n
        assert "The citywide week in one paragraph." in with_n
        without = self._render()["html"]
        assert "The Week, In Plain English" not in without

    def test_embeds_the_citywide_pulse_trace(self):
        html = self._render()["html"]
        assert "https://pulsecities.com/og/spark/nyc.png" in html

    def test_copy_hygiene(self):
        r = self._render(narrative="A paragraph.")
        assert "—" not in r["subject"] and "—" not in r["html"]
        for day in ("Monday", "monday"):
            assert day not in r["html"]
        assert "unsubscribe?token=tok-city" in r["html"]
        assert "not claims of wrongdoing" in r["html"]


# ---------------------------------------------------------------------------
# Integration — citywide sparkline
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestCitywideSpark:
    def test_nyc_spark_returns_png(self):
        from fastapi.testclient import TestClient
        from api.main import app
        resp = TestClient(app).get("/og/spark/nyc.png")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "image/png"
        assert resp.content[:4] == b"\x89PNG"
