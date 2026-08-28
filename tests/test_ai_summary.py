"""
AI neighborhood summary — regression guards for GET /api/neighborhoods/{zip}/summary.

The model call is mocked everywhere here: these tests pin the contract around it
(grounding, caching, rate-limit/abuse controls, graceful degradation) without spending
a token. The one thing they deliberately do NOT mock is the prompt — the system prompt
text is asserted directly, because the grounding and no-em-dash rules are the whole
reason this endpoint is safe to expose.

Unit checks run in CI without a database. Integration checks need a live DB row and
mock the Anthropic client, so no key or credits are required.
"""

import pytest

import api.routes.ai_summary as ai


def _get_client():
    from fastapi.testclient import TestClient
    from api.main import app
    return TestClient(app)


class _FakeBlock:
    type = "text"
    def __init__(self, text):
        self.text = text


class _FakeMessage:
    def __init__(self, text, stop_reason="end_turn"):
        self.content = [_FakeBlock(text)]
        self.stop_reason = stop_reason


class _FakeMessages:
    def __init__(self, text, stop_reason="end_turn"):
        self._text = text
        self._stop = stop_reason
        self.calls = 0
        self.last_kwargs = None

    def create(self, **kwargs):
        self.calls += 1
        self.last_kwargs = kwargs
        return _FakeMessage(self._text, self._stop)


class _FakeClient:
    def __init__(self, text="Renovation permits and LLC acquisitions are both running high here, the two clearest signs of repositioning pressure.", stop_reason="end_turn"):
        self.messages = _FakeMessages(text, stop_reason)


@pytest.fixture(autouse=True)
def _reset_state():
    # Each test starts with an empty cache and a fresh daily counter.
    ai._cache.clear()
    ai._gen_day = None
    ai._gen_count = 0
    yield
    ai._cache.clear()


# ---------------------------------------------------------------------------
# Unit — no database, no client
# ---------------------------------------------------------------------------

class TestPromptAndFacts:
    def test_system_prompt_enforces_grounding_and_no_em_dash(self):
        p = ai._SYSTEM_PROMPT
        assert "strictly from those figures" in p
        assert "Never use an em dash" in p
        assert "Do not invent" in p

    def test_pins_the_model_and_leaves_room_for_thinking(self):
        # Thinking tokens are charged against max_tokens; at the old 400 a thinking model
        # returns an empty paragraph and the panel silently hides itself.
        assert ai.MODEL == "claude-opus-5"
        assert ai.MAX_TOKENS >= 1000

    def test_prompt_demands_comparison_not_restatement(self):
        # The whole point of the read is the judgement a reader cannot make from the
        # panel: where this ZIP stands, and which way it is moving.
        p = ai._SYSTEM_PROMPT
        assert "Compare, do not restate" in p
        assert "rate per 1,000 apartments" in p
        assert "not measured" in p

    def test_build_facts_includes_score_tier_and_signals(self):
        facts = ai._build_facts(
            "Bedford-Stuyvesant", "Brooklyn", "11216", 71.1,
            {"permits": 100.0, "llc_acquisitions": 82.1, "rs_unit_loss": 0.0},
            {"llc_acquisitions": 111, "evictions": 238},
        )
        assert "71.1 out of 100" in facts
        assert "High pressure" in facts
        assert "renovation permit filings" in facts  # signal label, not raw key
        assert "111" in facts

    def test_build_facts_renders_the_comparative_block(self):
        facts = ai._build_facts(
            "Bedford-Stuyvesant", "Brooklyn", "11216", 71.1,
            {"permits": 100.0, "rs_unit_loss": 0.0},
            {"evictions": 238},
            {"zip_total": 177, "composite_rank": 12, "buildings": 2000, "apartments": 20000,
             "signal_standing": {"permits": {"dormant": False, "rank": 3, "median": 16.9},
                                 "rs_unit_loss": {"dormant": True, "rank": 1, "median": 0.0}},
             "evictions_90": 40, "evictions_90_prior": 61, "complaints_90": 5,
             "complaints_90_prior": 4, "llc_90": 7, "deed_lag_days": 21,
             "top_buyer": ("SOME HOLDINGS LLC", 4), "deconversions": (2, 5)},
        )
        assert "rank 12 of 177" in facts
        assert "rank 3 of 177" in facts
        assert "11.9 per 1,000 apartments" in facts          # 238 evictions / 20,000 homes
        assert "40 now against 61 a year ago" in facts
        assert "SOME HOLDINGS LLC" in facts
        # A signal nobody measures must never be handed over as a zero. Every ZIP would
        # otherwise rank 1 for rent-stabilized loss, which reads as the worst in the city.
        assert "rent-stabilized unit loss: not measured" in facts
        assert "window ends where the records end" in facts

    def test_build_facts_survives_an_empty_context(self):
        # The renderer is pure; a context the database could not supply drops out rather
        # than rendering a guess.
        facts = ai._build_facts("X", "Brooklyn", "11216", 40.0, {"permits": 10.0},
                                {"evictions": 5})
        assert "rank" not in facts
        assert "per 1,000" not in facts

    def test_tier_bands_match_map_legend(self):
        # Canonical bands: Low 0-33, Moderate 34-66, High 67-84, Critical 85+.
        assert ai._tier(90) == "Critical"
        assert ai._tier(75) == "High"
        assert ai._tier(50) == "Moderate"
        assert ai._tier(10) == "Low"

    def test_score_key_pins_cache_to_scoring_run(self):
        assert ai._score_key(71.1) == ai._score_key(71.14)   # noise -> same cache
        assert ai._score_key(71.1) != ai._score_key(71.3)    # real move -> regenerate


# ---------------------------------------------------------------------------
# Integration — live DB row, mocked model
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestSummaryEndpoint:
    def _live_zip(self):
        from models.database import SessionLocal
        from models.scores import DisplacementScore
        db = SessionLocal()
        row = (
            db.query(DisplacementScore)
            .filter(DisplacementScore.score.isnot(None))
            .first()
        )
        db.close()
        if not row:
            pytest.skip("No scored ZIPs in the database")
        return row.zip_code

    def test_generates_summary_then_serves_from_cache(self, monkeypatch):
        fake = _FakeClient()
        monkeypatch.setattr(ai, "_get_client", lambda: fake)
        zip_code = self._live_zip()
        client = _get_client()

        r1 = client.get(f"/api/neighborhoods/{zip_code}/summary")
        assert r1.status_code == 200, r1.text
        body = r1.json()
        assert body["summary"] and body["model"] == "claude-opus-5"
        assert body["cached"] is False
        assert fake.messages.calls == 1

        # Second call must hit the cache, not the model.
        r2 = client.get(f"/api/neighborhoods/{zip_code}/summary")
        assert r2.status_code == 200
        assert r2.json()["cached"] is True
        assert fake.messages.calls == 1, "cache miss — model was called twice"

    def test_grounding_facts_reach_the_model(self, monkeypatch):
        fake = _FakeClient()
        monkeypatch.setattr(ai, "_get_client", lambda: fake)
        zip_code = self._live_zip()
        _get_client().get(f"/api/neighborhoods/{zip_code}/summary")
        sent = fake.messages.last_kwargs
        assert sent["model"] == "claude-opus-5"
        assert sent["output_config"]["effort"] == ai.EFFORT
        content = sent["messages"][0]["content"]
        assert "displacement-pressure score" in content
        assert "scored NYC ZIP codes" in content   # the comparative block reached the model
        assert "Never use an em dash" in sent["system"]

    def test_refusal_degrades_to_503(self, monkeypatch):
        monkeypatch.setattr(ai, "_get_client", lambda: _FakeClient(stop_reason="refusal"))
        zip_code = self._live_zip()
        r = _get_client().get(f"/api/neighborhoods/{zip_code}/summary")
        assert r.status_code == 503

    def test_missing_api_key_degrades_to_503(self, monkeypatch):
        monkeypatch.setattr(ai, "_get_client", lambda: None)
        zip_code = self._live_zip()
        r = _get_client().get(f"/api/neighborhoods/{zip_code}/summary")
        assert r.status_code == 503

    def test_daily_cap_degrades_to_503(self, monkeypatch):
        monkeypatch.setattr(ai, "_get_client", lambda: _FakeClient())
        monkeypatch.setattr(ai, "DAILY_GENERATION_CAP", 0)
        zip_code = self._live_zip()
        r = _get_client().get(f"/api/neighborhoods/{zip_code}/summary")
        assert r.status_code == 503

    def test_invalid_zip_returns_400(self):
        r = _get_client().get("/api/neighborhoods/ABCDE/summary")
        assert r.status_code == 400

    def test_unknown_zip_returns_404(self, monkeypatch):
        monkeypatch.setattr(ai, "_get_client", lambda: _FakeClient())
        r = _get_client().get("/api/neighborhoods/00000/summary")
        assert r.status_code == 404
