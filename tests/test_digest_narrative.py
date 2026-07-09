"""
Digest narrative — the AI-written paragraph in the weekly ZIP digest.

The paragraph is editorial glaze over deterministic numbers, so the guards here
are about discipline, not prose quality:

  1. Grounding — the facts block handed to the model must carry the exact
     figures the email already shows. The model never sees anything the
     subscriber doesn't.
  2. Graceful absence — no API key, an SDK failure, a refusal, or an empty
     completion must all degrade to "no paragraph", never to a broken email.
  3. One call per ZIP per run — subscribers sharing a ZIP share the paragraph.

No test here touches the network. The client is stubbed at the module seam.
"""

import pytest

from scripts import digest_narrative
from scripts.digest_narrative import build_facts, generate_narrative
from scripts.weekly_digest import render_zip_digest


def _summary(**overrides) -> dict:
    base = {
        "zip": "11216",
        "name": "Bedford-Stuyvesant",
        "score_now": 72.4,
        "score_prev": 66.1,
        "delta": 6.3,
        "tier_now": "high",
        "tier_prev": "moderate",
        "tier_increased": True,
        "elevated": [("eviction_rate", 81.0), ("llc_acquisition_rate", 44.5)],
        "hpd_count": 12,
        "eviction_count": 4,
        "permit_count": 2,
        "llc_count": 3,
        "complaint_count": 9,
        "hpd_avg": 6.5,
        "eviction_avg": 1.2,
        "permit_avg": 2.1,
        "complaint_avg": 8.8,
    }
    base.update(overrides)
    return base


def _subscription() -> dict:
    return {"email": "reader@example.com", "unsubscribe_token": "tok123"}


class _FakeBlock:
    type = "text"
    text = "Pressure in Bedford-Stuyvesant climbed this week."


class _FakeMessage:
    stop_reason = "end_turn"
    content = [_FakeBlock()]


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
# Grounding
# ---------------------------------------------------------------------------

class TestBuildFacts:
    def test_carries_the_exact_figures_the_email_shows(self):
        facts = build_facts(_summary(), ["4 evictions executed"])
        for needle in ("Bedford-Stuyvesant", "11216", "72.4", "66.1"):
            assert needle in facts, f"{needle} missing from facts block"

    def test_carries_weekly_counts_against_their_baselines(self):
        facts = build_facts(_summary(), [])
        # The week-vs-typical comparison is the one thing a template can't
        # phrase well; the model must receive both sides of it.
        assert "4" in facts and "1.2" in facts   # evictions: count vs 8-week avg
        assert "12" in facts and "6.5" in facts  # HPD violations

    def test_includes_the_threshold_reasons(self):
        facts = build_facts(_summary(), ["Risk score rose 6.3 points"])
        assert "Risk score rose 6.3 points" in facts


# ---------------------------------------------------------------------------
# Graceful absence
# ---------------------------------------------------------------------------

class TestGenerateNarrative:
    def test_returns_none_when_no_api_key(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.setattr(digest_narrative, "_client", None)
        assert generate_narrative(_summary(), []) is None

    def test_returns_text_from_the_model(self, monkeypatch):
        fake = _FakeClient()
        monkeypatch.setattr(digest_narrative, "_get_client", lambda: fake)
        out = generate_narrative(_summary(), [])
        assert out == "Pressure in Bedford-Stuyvesant climbed this week."

    def test_one_call_per_zip_per_run(self, monkeypatch):
        fake = _FakeClient()
        monkeypatch.setattr(digest_narrative, "_get_client", lambda: fake)
        generate_narrative(_summary(), [])
        generate_narrative(_summary(), [])
        assert fake.calls == 1

    def test_sdk_failure_degrades_to_none(self, monkeypatch):
        class _Boom:
            def __init__(self):
                self.messages = self
            def create(self, **kwargs):
                raise RuntimeError("network down")
        monkeypatch.setattr(digest_narrative, "_get_client", lambda: _Boom())
        assert generate_narrative(_summary(), []) is None

    def test_refusal_degrades_to_none(self, monkeypatch):
        class _Refusal(_FakeClient):
            def create(self, **kwargs):
                msg = _FakeMessage()
                msg.stop_reason = "refusal"
                return msg
        monkeypatch.setattr(digest_narrative, "_get_client", lambda: _Refusal())
        assert generate_narrative(_summary(), []) is None

    def test_failure_is_cached_so_a_bad_run_makes_at_most_one_call_per_zip(self, monkeypatch):
        calls = {"n": 0}
        class _Boom:
            def __init__(self):
                self.messages = self
            def create(self, **kwargs):
                calls["n"] += 1
                raise RuntimeError("network down")
        boom = _Boom()
        monkeypatch.setattr(digest_narrative, "_get_client", lambda: boom)
        generate_narrative(_summary(), [])
        generate_narrative(_summary(), [])
        assert calls["n"] == 1


# ---------------------------------------------------------------------------
# Email integration
# ---------------------------------------------------------------------------

class TestRenderWithNarrative:
    def test_narrative_appears_in_the_email_when_present(self):
        rendered = render_zip_digest(
            _subscription(), _summary(), ["4 evictions executed"],
            {"llc_rows": [], "eviction_rows": [], "permit_rows": [], "hpd_rows": []},
            narrative="Pressure climbed on the strength of eviction filings.",
        )
        assert "Pressure climbed on the strength of eviction filings." in rendered["html"]
        assert "The Week, In Plain English" in rendered["html"]

    def test_email_is_unchanged_when_narrative_is_absent(self):
        rendered = render_zip_digest(
            _subscription(), _summary(), ["4 evictions executed"],
            {"llc_rows": [], "eviction_rows": [], "permit_rows": [], "hpd_rows": []},
            narrative=None,
        )
        assert "The Week, In Plain English" not in rendered["html"]
