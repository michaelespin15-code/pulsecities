"""
The weekly feed, and the two ways a generated artifact goes quiet.

There was no feed of any kind until 2026-08-29: /feed.xml and /rss.xml both
404'd and no page carried a `<link rel="alternate">`, on a site whose README
names journalists, tenant organisations and planners as its audience. Every one
of those is a subscriber by habit.

Two failure shapes are guarded here, and both are silent.

A page can advertise a feed that does not exist. Nothing breaks, nothing logs,
the reader's app just reports an error the site never sees.

A generated artifact can lose its cron. The file stays on disk at whatever it
last said, so the site keeps serving a feed whose newest item is six weeks old
and looks fine to everything except a reader.

The `cd` assertion is here because it was broken twice while this file was
being written. `python -m scripts.generate_feed` without it is
ModuleNotFoundError, every night, into a log nobody reads.
"""
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
FEED = REPO / "frontend" / "feed.xml"
CRON = REPO / "deploy" / "pulsecities.cron"

_ALTERNATE = re.compile(
    r'rel="alternate"\s+type="application/rss\+xml"[^>]*href="([^"]+)"')
_WEEK_URL = re.compile(r"^https://pulsecities\.com/week/\d{4}-W\d{2}$")


def _advertised_hrefs():
    """Every feed URL any page hands a reader."""
    sources = [REPO / "api" / "routes" / "frontend.py"]
    sources += sorted((REPO / "frontend").glob("*.html"))
    for path in sources:
        for href in _ALTERNATE.findall(path.read_text()):
            yield path.relative_to(REPO).as_posix(), href


class TestWhatIsAdvertisedExists:
    def test_the_grep_finds_something(self):
        """A guard that matches nothing passes forever."""
        found = list(_advertised_hrefs())
        assert found, (
            "no page advertises a feed. Either autodiscovery was removed, or "
            "this pattern stopped matching how the tag is written."
        )

    def test_every_advertised_feed_is_a_file_that_exists(self):
        missing = []
        for src, href in _advertised_hrefs():
            rel = href.replace("https://pulsecities.com/", "")
            if not (REPO / "frontend" / rel).is_file():
                missing.append(f"{src} -> {href}")
        assert not missing, (
            "these pages hand readers a feed URL with nothing behind it:\n  "
            + "\n  ".join(missing))


@pytest.fixture(scope="module")
def channel():
    assert FEED.is_file(), "frontend/feed.xml is gone; run scripts.generate_feed"
    return ET.parse(FEED).getroot().find("channel")


class TestTheFeedItself:
    def test_it_has_items(self, channel):
        items = channel.findall("item")
        assert items, "an empty feed is worse than none; the generator refuses to write one"

    def test_every_item_points_at_a_completed_edition(self, channel):
        """Never /this-week. That URL is rewritten nightly, so a feed carrying
        it reissues the same item every day and trains people to ignore it."""
        bad = [i.find("link").text for i in channel.findall("item")
               if not _WEEK_URL.match(i.find("link").text or "")]
        assert not bad, f"items that are not a stable weekly edition: {bad}"

    def test_guids_are_unique(self, channel):
        guids = [i.find("guid").text for i in channel.findall("item")]
        assert len(guids) == len(set(guids)), "a repeated guid re-notifies every reader"

    def test_it_declares_where_it_lives(self, channel):
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        link = channel.find("atom:link", ns)
        assert link is not None and link.get("rel") == "self", (
            "no atom:link rel=self; aggregators use it to canonicalise the feed")


class TestItKeepsGettingWritten:
    def test_the_generator_has_a_cron_entry(self):
        assert "scripts.generate_feed" in CRON.read_text(), (
            "nothing regenerates the feed. The file stays on disk saying "
            "whatever it last said, which reads as a publication that stopped.")

    def test_every_module_cron_cds_into_the_repo_first(self):
        """`python -m scripts.x` resolves nothing from cron's working directory.
        This was broken twice while writing the feed entry, and the only
        symptom is a ModuleNotFoundError in a log nobody reads."""
        offenders = []
        for line in CRON.read_text().splitlines():
            if not line or line.startswith("#") or "python -m" not in line:
                continue
            if "cd /root/pulsecities" not in line:
                offenders.append(line.strip())
        assert not offenders, (
            "these run `python -m` without cd'ing into the repo first:\n  "
            + "\n  ".join(offenders))
