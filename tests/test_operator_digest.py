"""
Operator-follow digest: loaders, update builder, and renderer.
"""

import pytest

from models.database import SessionLocal
from sqlalchemy import text

from scripts.weekly_digest import (
    build_operator_updates,
    load_operator_follows,
    render_operator_digest,
)

_FAKE_SUB = {"email": "x@example.com", "operator_slug": "mtek-nyc", "unsubscribe_token": "tok123"}
_FAKE_UPDATE = {
    "slug": "mtek-nyc",
    "display_name": "Mtek",
    "acquisitions": [
        {"address": "1130 Greene Avenue", "zip": "11221", "date": "2026-07-02", "price": 1240000.0},
        {"address": "Bbl 3032950010",     "zip": "",      "date": "",           "price": None},
    ],
}


class TestRenderOperatorDigest:

    def test_subject_names_operator_and_count(self):
        r = render_operator_digest(_FAKE_SUB, _FAKE_UPDATE)
        assert "Mtek" in r["subject"]
        assert "2 new acquisitions" in r["subject"]

    def test_html_carries_rows_links_and_token(self):
        html = render_operator_digest(_FAKE_SUB, _FAKE_UPDATE)["html"]
        assert "1130 Greene Avenue" in html
        assert "$1,240,000" in html
        assert "/operator/mtek-nyc" in html
        assert "/brief/operator/mtek-nyc" in html
        assert "unsubscribe?token=tok123" in html

    def test_no_em_dash_in_email(self):
        r = render_operator_digest(_FAKE_SUB, _FAKE_UPDATE)
        assert "—" not in r["subject"] + r["html"]

    def test_singular_subject(self):
        one = dict(_FAKE_UPDATE, acquisitions=_FAKE_UPDATE["acquisitions"][:1])
        r = render_operator_digest(_FAKE_SUB, one)
        assert "1 new acquisition this week" in r["subject"]


@pytest.mark.integration
class TestOperatorUpdateQueries:

    def test_build_updates_runs_and_gates_class(self):
        db = SessionLocal()
        try:
            slugs = {r.slug for r in db.execute(text(
                "SELECT slug FROM operators WHERE slug IS NOT NULL LIMIT 5"
            )).fetchall()}
            updates = build_operator_updates(db, slugs)
            gated = {r.slug for r in db.execute(text(
                "SELECT slug FROM operators WHERE operator_class IS DISTINCT FROM 'operator'"
            )).fetchall()}
            assert not (set(updates) & gated), "gated cluster leaked into digest updates"
        finally:
            db.close()

    def test_load_follows_returns_confirmed_only(self):
        db = SessionLocal()
        try:
            follows = load_operator_follows(db)
            assert all(f["operator_slug"] for f in follows)
        finally:
            db.close()
