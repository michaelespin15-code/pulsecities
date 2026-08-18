"""
Guards for IndexNow submission.

The failure mode is silent: a key file that stops matching robots.txt, or that
nginx does not serve, gets every submission rejected with a 403 that nobody
reads because the run is in cron. These check the three things that have to
agree, and that the delta logic does not resubmit an unchanged URL, which is
what gets a host throttled.
"""

import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from scripts.indexnow_submit import (
    BATCH,
    KEY,
    KEY_FILE,
    MAX_PER_RUN,
    changed,
    sitemap_urls,
)

ROOT = Path(__file__).resolve().parent.parent
ROBOTS = ROOT / "frontend" / "robots.txt"


class TestKeyMaterial:
    def test_key_file_exists_and_holds_exactly_the_key(self):
        assert KEY_FILE.exists(), f"{KEY_FILE} is missing; submissions 403"
        assert KEY_FILE.read_text().strip() == KEY

    def test_key_is_the_shape_the_protocol_accepts(self):
        assert 8 <= len(KEY) <= 128
        assert all(c in "0123456789abcdefABCDEF-" for c in KEY)

    def test_robots_names_the_same_key(self):
        line = [l for l in ROBOTS.read_text().splitlines()
                if l.lower().startswith("indexnow:")]
        assert line, "robots.txt does not reference the IndexNow key"
        assert line[0].split(":", 1)[1].strip() == KEY

    def test_key_file_is_world_readable(self):
        """nginx workers serve it from disk; 0600 means a 403 and a dead key."""
        assert KEY_FILE.stat().st_mode & 0o044


class TestDelta:
    def test_unchanged_urls_are_not_resubmitted(self):
        urls = {"https://x/a": "2026-08-01", "https://x/b": "2026-08-02"}
        assert changed(urls, dict(urls)) == []

    def test_new_and_moved_urls_are_submitted(self):
        urls = {"https://x/a": "2026-08-03", "https://x/b": "2026-08-02"}
        state = {"https://x/a": "2026-08-01"}
        assert changed(urls, state) == ["https://x/a", "https://x/b"]

    def test_batch_fits_the_protocol_limit(self):
        assert BATCH <= 10_000
        assert MAX_PER_RUN >= BATCH


class TestAgainstTheLiveSitemap:
    def test_core_urls_come_first(self):
        """Order decides what gets sent when a run hits the cap, and a changed
        hub is worth more than one more property page."""
        index = ROOT / "frontend" / "sitemap.xml"
        if not index.exists():
            pytest.skip("no sitemap generated in this tree")
        urls = sitemap_urls()
        if not urls:
            pytest.skip("sitemap has no URLs")
        paths = list(urls)
        first_property = next(
            (i for i, u in enumerate(paths) if "/property/" in u), len(paths))
        last_core = max(
            (i for i, u in enumerate(paths) if "/property/" not in u), default=0)
        assert last_core < first_property or first_property == len(paths)

    def test_every_url_is_on_this_host(self):
        """A URL from another host is a 422 for the whole batch."""
        index = ROOT / "frontend" / "sitemap.xml"
        if not index.exists():
            pytest.skip("no sitemap generated in this tree")
        bad = [u for u in sitemap_urls() if not u.startswith("https://pulsecities.com/")]
        assert not bad, f"off-host URLs in the sitemap: {bad[:5]}"
