"""
Nothing leaves this system without passing one gate.

There were four independent `resend.Emails.send` call sites and four different
answers to "is this worth sending". The daily building alert guarded on
`if events:`, the weekly digest on per-arm thresholds, the flips report on
`if new_arcs:`, and the subscribe confirmation on nothing at all. Adding a fifth
sender meant inventing a fifth rule, and the block digest was about to be that
fifth sender.

Same shape as the freshness rule that had one reader and seventeen bypasses, so
the same fix: scripts/lib/mailer.py owns it, and this file greps for anyone
going around it. A test that only exercises today's four senders passes forever
while someone adds a fifth beside them.

The floor in that module is measured, not guessed. The first draft read 45 on a
guess that a one-event alert carries "60 to 90 words". A real one-event alert
renders 39, so that floor would have blocked "your building was sold", which is
the most important message the system sends. The numbers below are the measured
ones and they are pinned here so the next edit to a template has to notice.
"""

import re
from pathlib import Path

import pytest

from scripts.lib import mailer

REPO = Path(__file__).resolve().parent.parent
SEARCH_DIRS = ("scripts", "api", "scheduler")
ALLOWED = {"scripts/lib/mailer.py"}


class TestOneGateOnly:
    def test_nothing_calls_the_provider_directly(self):
        offenders = []
        for d in SEARCH_DIRS:
            for path in sorted((REPO / d).rglob("*.py")):
                rel = path.relative_to(REPO).as_posix()
                if rel in ALLOWED or "__pycache__" in rel:
                    continue
                for i, line in enumerate(path.read_text().splitlines()):
                    if re.search(r"resend\.Emails\.send", line):
                        offenders.append(f"{rel}:{i + 1}")
        assert not offenders, (
            "these call the Resend SDK directly and skip every content check. "
            "Use scripts.lib.mailer.send:\n  " + "\n  ".join(offenders)
        )


class TestItRefusesTheThingsItShould:
    def _shell_and_real(self):
        from scripts.building_alerts import build_email
        def mk(n):
            return {"email": "x@y.com", "bbl": "3008760024", "address": "460 17 Street",
                    "token": "t", "total": n,
                    "events": [{"kind": "violation", "line": f"Class B violation {i}"}
                               for i in range(n)]}
        return mk

    def test_zero_content_items_is_refused_however_full_the_page_looks(self):
        """The primary guarantee, and the one that does not depend on a
        heuristic. A caller with nothing to report cannot send."""
        problems = mailer.check("a@b.com", "Subject", "<p>" + "word " * 500 + "</p>",
                                content_items=0)
        assert any("nothing to say" in p for p in problems)

    def test_an_empty_render_is_refused_even_when_items_are_claimed(self):
        """The backstop. A caller that counted rows and then rendered none."""
        problems = mailer.check("a@b.com", "Subject", "<html><body></body></html>",
                                content_items=5)
        assert any("visible words" in p or "body is empty" in p for p in problems)

    def test_a_real_single_event_alert_is_not_blocked(self):
        """The regression this guard nearly caused. One event is a real email
        and the most important one the system sends."""
        mk = self._shell_and_real()
        from scripts.building_alerts import build_email
        subject, html, text = build_email(mk(1))
        assert mailer.check("a@b.com", subject, html, text, content_items=1) == [], (
            "the content floor is blocking a legitimate one-event alert"
        )

    def test_the_bare_shell_is_blocked(self):
        mk = self._shell_and_real()
        from scripts.building_alerts import build_email
        subject, html, text = build_email(mk(0))
        assert mailer.check("a@b.com", subject, html, text, content_items=0)

    def test_the_floor_sits_between_the_two(self):
        """Pinned so a template edit that fattens the chrome has to move it
        deliberately rather than silently swallowing the margin."""
        from scripts.building_alerts import build_email
        mk = self._shell_and_real()
        empty = len(mailer.visible_words(build_email(mk(0))[1]))
        one = len(mailer.visible_words(build_email(mk(1))[1]))
        assert empty < mailer.MIN_CONTENT_WORDS <= one, (
            f"shell renders {empty} words, one real event renders {one}, floor is "
            f"{mailer.MIN_CONTENT_WORDS}. The floor has to sit between them."
        )

    @pytest.mark.parametrize("field,body", [
        ("html", "<p>" + "word " * 60 + "{address} more words</p>"),
        ("html", "<p>" + "word " * 60 + "__LABEL__ more words</p>"),
    ])
    def test_unfilled_placeholders_are_refused(self, field, body):
        """.format() and str.replace() do not raise on a missing key, so these
        reach inboxes looking like a bug because they are one."""
        problems = mailer.check("a@b.com", "Subject", body, content_items=3)
        assert any("unfilled placeholders" in p for p in problems)

    def test_an_unfilled_placeholder_in_the_subject_is_refused(self):
        problems = mailer.check("a@b.com", "New at {address}",
                                "<p>" + "word " * 60 + "</p>", content_items=3)
        assert any("unfilled placeholders" in p for p in problems)

    @pytest.mark.parametrize("addr", ["", "   ", "not-an-email", "a@b", "a b@c.com"])
    def test_a_bad_recipient_is_refused(self, addr):
        problems = mailer.check(addr, "Subject", "<p>" + "word " * 60 + "</p>",
                                content_items=3)
        assert any("recipient" in p for p in problems)

    def test_an_empty_subject_is_refused(self):
        problems = mailer.check("a@b.com", "  ", "<p>" + "word " * 60 + "</p>",
                                content_items=3)
        assert any("subject is empty" in p for p in problems)

    def test_refusal_raises_rather_than_returning_false(self):
        """A content problem is a bug in the caller. Returning False would let
        it be logged as a delivery failure and ignored."""
        with pytest.raises(mailer.EmailRefused):
            mailer.send("a@b.com", "S", "<html></html>", content_items=0, dry_run=True)

    def test_a_good_message_passes_in_dry_run(self):
        assert mailer.send("a@b.com", "Subject", "<p>" + "word " * 60 + "</p>",
                           content_items=3, dry_run=True) is True


class TestVisibleWords:
    def test_markup_does_not_count_as_content(self):
        """An HTML email is 2KB of shell. A raw-length check would pass anything."""
        markup = '<table style="background:#EFEBE2;padding:36px"><tr><td></td></tr></table>'
        assert mailer.visible_words(markup) == []

    def test_style_and_script_blocks_are_stripped(self):
        html = "<style>.a{color:red}</style><script>var x=1</script><p>hello there</p>"
        assert mailer.visible_words(html) == ["hello", "there"]


class TestTheAlertScansWhatMatters:
    """311 was the missing feed, and it is the one this audience cares about.

    Across the twelve watched buildings the four original feeds produced 24
    events in 90 days; 311 alone produced 109. The subscribers are aol.com and
    msn.com residents, not investors, and heat, plumbing and mold is what they
    signed up to hear about. One watched building, 681 5 Avenue, alerts only
    because of this feed and would otherwise never have heard from us.
    """

    def test_complaints_are_scanned(self):
        from scripts.building_alerts import _EVENT_SQL, _EVENT_WHERE
        assert "complaint" in _EVENT_WHERE and "complaint" in _EVENT_SQL

    def test_only_housing_complaint_types_qualify(self):
        """A watcher does not want "Litter Basket Request" as a building alert,
        and at least one watched building files those alongside its plumbing."""
        from scripts.building_alerts import _EVENT_WHERE
        assert "housing_types" in _EVENT_WHERE["complaint"], (
            "the complaint scan is unfiltered and will alert on litter baskets"
        )

    def test_the_filter_is_the_same_list_the_site_counts_on(self):
        """Two lists of housing complaint types would drift, and the drift shows
        up as an alert about a subject the neighbourhood page does not count."""
        import scripts.building_alerts as ba
        from config.nyc import DISPLACEMENT_COMPLAINT_TYPES
        assert ba.DISPLACEMENT_COMPLAINT_TYPES is DISPLACEMENT_COMPLAINT_TYPES

    def test_a_complaint_renders_a_readable_line(self):
        from scripts.building_alerts import _describe
        from datetime import date

        class Row:
            ref = "1"; event_date = date(2026, 6, 15)
            complaint_type = "PLUMBING"; descriptor = "BASIN/SINK"; status = "Open"
        line = _describe("complaint", Row())
        assert "311 complaint" in line and "Plumbing" in line and "Basin/Sink" in line
        assert "{" not in line, "unfilled placeholder in an alert line"
