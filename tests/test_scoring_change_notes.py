"""
A step the algorithm caused must not read as a step the city caused.

On 2026-08-28 the permit signal was recomputed against DOB NOW, which had been
missing 96% of the renovation record. Every score moved at once. Measured against
score_history, the night of 08-27 to 08-28 moved the mean ZIP **4.2 points with a
maximum of 20.8, and 87 of 177 ZIPs moved 3 or more**. A normal night moves the
mean ZIP 0.24 points with a maximum of 3.1 and one ZIP over 3, so that night was
seventeen times a normal one.

Rockaway Park reads 50.0 then 30.6. The Upper West Side reads 10.0 then 30.8.
A reader looking at a ninety-day chart sees a cliff and has no way to know it was
our arithmetic.

The weekly digest was taught to disclose this (0140f12) because it was about to
report the recompute as the city's news. `scoring_changes` holds the row. The
chart on the homepage and in the map panel was never told.

The history endpoint returns a bare array and three consumers index into it
(frontend/app.html, frontend/index.html, and the documented contract on
/developers), so the note is served beside it rather than folded into it.
"""

import pytest

from fastapi.testclient import TestClient


def _client():
    from api.main import app
    return TestClient(app)


@pytest.mark.integration
class TestTheNotesEndpoint:
    def test_it_returns_changes_in_the_window(self):
        r = _client().get("/api/score-history/notes?days=365")
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body, list)
        if body:
            assert {"date", "summary"} <= set(body[0])

    def test_days_is_clamped(self):
        """Same clamp as the series endpoint, so a note cannot outrun its chart."""
        r = _client().get("/api/score-history/notes?days=99999")
        assert r.status_code == 200

    def test_the_series_endpoint_still_returns_a_bare_array(self):
        """
        Three consumers index into this. Wrapping it in an object to carry the
        note would have broken all of them silently.
        """
        r = _client().get("/api/score-history/10458?days=30")
        assert r.status_code == 200
        assert isinstance(r.json(), list)


class TestTheChartAsksForIt:
    def test_the_panel_chart_fetches_the_notes(self):
        """
        app.html only. index.html draws the same series as a decorative hero
        sparkline with no axes, no labels and no scale, which a reader does not
        interpret as a measurement, so a disclosure sentence under it would be
        clutter rather than context. The panel chart has a date axis, a score
        axis and a window selector, and is read.
        """
        from pathlib import Path
        repo = Path(__file__).resolve().parent.parent
        src = (repo / "frontend" / "app.html").read_text()
        assert "score-history/notes" in src, (
            "the panel history chart must disclose a scoring change inside the "
            "window it is drawing"
        )
        assert "history-change-note" in src, "the note element is missing"

    def test_the_note_window_follows_the_chart_window(self):
        """A note outside the drawn range explains a step the reader cannot see."""
        from pathlib import Path
        repo = Path(__file__).resolve().parent.parent
        src = (repo / "frontend" / "app.html").read_text()
        assert "score-history/notes?days=${days}" in src, (
            "the note must be fetched for the same window as the series, not a "
            "fixed one, or the 30-day view explains a step 80 days back"
        )
