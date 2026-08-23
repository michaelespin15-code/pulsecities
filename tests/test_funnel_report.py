"""
Guard: the funnel report counts the right things.

The numbers it prints decide where build effort goes, so the failure that
matters is a silent miscount, not a crash. The two ways it could lie are
counting requests as visitors (which overstates by ~20x on this box, where
crawlers and vulnerability scanners are most of the traffic) and attributing a
landing to the wrong template.
"""

from datetime import datetime, timezone

import pytest

from scripts.funnel_report import (
    TEMPLATES, _ASSET, _LINE, _SEARCH_REF, _parse_ts, _template_for, render,
)


def _line(ip="1.2.3.4", ts="23/Aug/2026:05:04:27 +0000", path="/property/3034100059",
          status="200", ref="https://www.google.com/", ua="Mozilla/5.0"):
    return (f'{ip} - - [{ts}] "GET {path} HTTP/1.1" {status} 1234 "{ref}" "{ua}"')


class TestLineParsing:

    def test_parses_a_normal_line(self):
        m = _LINE.match(_line())
        assert m is not None
        assert m["ip"] == "1.2.3.4"
        assert m["path"] == "/property/3034100059"
        assert m["ref"] == "https://www.google.com/"

    def test_parses_timestamp_to_utc(self):
        assert _parse_ts("23/Aug/2026:05:04:27 +0000") == \
            datetime(2026, 8, 23, 5, 4, 27, tzinfo=timezone.utc)

    def test_bad_timestamp_returns_none_not_raise(self):
        for bad in ("", "nonsense", "32/Xxx/2026:99:99:99 +0000", "23/Aug/2026"):
            assert _parse_ts(bad) is None

    def test_malformed_line_does_not_match(self):
        for bad in ("", "garbage", '1.2.3.4 - - [] "" 200'):
            assert _LINE.match(bad) is None


class TestSearchReferrer:

    @pytest.mark.parametrize("ref", [
        "https://www.google.com/", "https://google.co.uk/search?q=x",
        "https://www.bing.com/", "https://duckduckgo.com/",
        "https://yandex.com/", "https://search.brave.com/",
    ])
    def test_recognises_search_engines(self, ref):
        assert _SEARCH_REF.match(ref)

    @pytest.mark.parametrize("ref", [
        "", "-", "https://pulsecities.com/map", "https://104.236.87.19",
        "https://notgoogle.example.com/",
    ])
    def test_rejects_everything_else(self, ref):
        assert not _SEARCH_REF.match(ref)

    def test_self_referral_is_not_a_landing(self):
        """Internal navigation carries our own host as the referrer. Counting
        it would turn one visitor into a whole session."""
        assert not _SEARCH_REF.match("https://pulsecities.com/property/1")


class TestTemplateAttribution:

    @pytest.mark.parametrize("path,expected", [
        ("/property/3034100059", "property"),
        ("/property/3034100059?lang=es", "property"),
        ("/llc/go-dey-llc", "llc"),
        ("/neighborhood/11207", "neighborhood"),
        ("/network/flgsp", "network"),
        ("/operator/mtek-nyc", "operator"),
    ])
    def test_maps_paths_to_templates(self, path, expected):
        assert _template_for(path) == expected

    @pytest.mark.parametrize("path", [
        "/", "/map", "/evictions", "/who-owns-my-building",
        "/property", "/property/", "/llc", "/llc/",
        "/.env", "/wp-admin/install.php",
    ])
    def test_index_pages_and_junk_are_not_templates(self, path):
        """A bare /llc is the directory, not an entity page, and it converts
        through nothing. Attributing it to the llc template would inflate that
        row's landings with traffic no card can catch."""
        assert _template_for(path) is None

    def test_every_template_names_a_real_subscribers_column(self):
        from models.subscribers import Subscriber
        cols = set(Subscriber.__table__.columns.keys())
        for _prefix, name, col in TEMPLATES:
            assert col in cols, f"{name} maps to {col}, which is not a subscribers column"


class TestAssetFilter:

    @pytest.mark.parametrize("path", [
        "/static/app.css", "/fonts/dm-sans-latin.woff2", "/og/spark/11370.png",
        "/sitemap.xml", "/robots.txt", "/tiles/1/2/3.pbf",
    ])
    def test_assets_are_excluded(self, path):
        assert _ASSET.search(path)

    @pytest.mark.parametrize("path", ["/property/3034100059", "/llc/go-dey-llc", "/"])
    def test_pages_are_not_assets(self, path):
        assert not _ASSET.search(path)


class TestRender:

    def _report(self, rows, **kw):
        base = {"days": 14, "oldest_log_entry": "2026-08-10T00:00:00+00:00",
                "rows": rows, "subscribers_total": 8, "subscribers_in_window": 0}
        base.update(kw)
        return base

    def test_zero_landings_shows_na_not_division_error(self):
        out = render(self._report([
            {"template": "operator", "column": "operator_slug",
             "landings": 0, "signups": 0, "rate_pct": None}]))
        assert "n/a" in out

    def test_totals_add_up(self):
        out = render(self._report([
            {"template": "property", "column": "bbl",
             "landings": 400, "signups": 4, "rate_pct": 1.0},
            {"template": "llc", "column": "entity_slug",
             "landings": 100, "signups": 1, "rate_pct": 1.0}]))
        assert "500" in out and "1.00%" in out

    def test_no_em_dash_in_output(self):
        out = render(self._report([
            {"template": "property", "column": "bbl",
             "landings": 1, "signups": 0, "rate_pct": 0.0}]))
        assert "—" not in out
