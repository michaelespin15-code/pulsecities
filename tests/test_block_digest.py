"""
The monthly block digest, and the one property that makes it worth sending.

A building watch is event-driven, and the measurement that motivated this
report is that the events are not there: across the twelve watched buildings
there were zero in thirty days. Widening to the tax block helps but does not
close it. By event date only 5 of 12 blocks had anything in thirty days, and by
ingest date 9 of 12, so a report that only says *what happened* is empty for a
quarter to a half of its readers and reproduces the hole one radius out.

That is why the report carries a standing-state section, and why the test that
matters here is `test_report_is_never_only_events`: the render must produce
content for a block where nothing at all was recorded. Everything else in this
file is the editorial machinery that got the report down to twelve legible
lines, each pinned to the input that broke it.

No database. The suite once committed deletes against score_history, so these
build their rows in Python.
"""

from datetime import date

import pytest

from scripts import block_digest as bd
from scripts.lib import mailer


def _row(**kw):
    """A stand-in for a SQLAlchemy result row."""
    base = {"bbl": "3044477502", "address": "475 LOCKE STREET",
            "event_date": date(2026, 8, 9), "party_name": None, "doc_amount": None,
            "a": None, "b": None, "c": None}
    base.update(kw)
    return type("Row", (), base)


def _event(**kw):
    base = {"kind": "violation", "bucket": "violation", "bbl": "3044477502",
            "address": "475 Locke Street", "mine": False,
            "date": date(2026, 8, 9), "line": "HPD violation issued (class B, hazardous), Aug 9."}
    base.update(kw)
    return base


def _block(events=(), total=None, **state):
    base_state = {"parcels": 59, "units": 248, "open_violations": 66,
                  "violation_buildings": 19, "deeds_12m": 7, "sold_buildings_12m": 7,
                  "evictions_12m": 1, "worst_address": "291 21 Street",
                  "worst_bbl": "3008910052", "worst_open": 15}
    base_state.update(state)
    return {"block": "300891", "bbls": ["3008910006"], "addresses": ["681 5 Avenue"],
            "label": "681 5 Avenue", "borough": "Brooklyn",
            "events": list(events), "state": base_state,
            "total": len(events) if total is None else total}


def _report(blocks=None, **kw):
    blocks = blocks or [_block()]
    base = {"email": "watcher@example.com", "token": "t" * 32, "blocks": blocks,
            "feeds": {"acris": date(2026, 7, 31), "violations": date(2026, 8, 25),
                      "evictions": date(2026, 8, 20), "complaints": date(2026, 8, 25),
                      "permits": date(2026, 8, 25)},
            "new_records": sum(b["total"] for b in blocks)}
    base.update(kw)
    return base


class TestBlockSpan:
    def test_span_covers_every_lot_on_the_block(self):
        lo, hi = bd.block_span("3044477501")
        assert (lo, hi) == ("3044470000", "3044479999")
        assert lo <= "3044477501" <= hi

    def test_span_excludes_the_neighbouring_block(self):
        lo, hi = bd.block_span("3044477501")
        assert not (lo <= "3044480001" <= hi)
        assert not (lo <= "3044460001" <= hi)

    @pytest.mark.parametrize("bbl", ["1000010001", "5033420021", "4006920116"])
    def test_span_is_a_range_not_a_prefix_match(self, bbl):
        """The range form is what lets Postgres use the plain btree on bbl.

        `bbl LIKE '304447%'` cannot use it outside the C collation: the same
        twelve-block scan ran in 1.9s as a range and had not finished in two
        minutes as a prefix match.
        """
        lo, hi = bd.block_span(bbl)
        assert len(lo) == len(hi) == 10 and lo < hi
        assert lo.endswith("0000") and hi.endswith("9999")


class TestViolationProse:
    def test_statute_preamble_is_dropped(self):
        raw = ("HMC ADM CODE: § 27-2017.4 ABATE THE INFESTATION CONSISTING OF "
               "ROACHES IN THE ENTIRE APARTMENT LOCATED AT APT 1L, 1st STORY")
        out = bd._plain(raw)
        assert out.startswith("Abate the infestation")
        assert "27-2017.4" not in out and "HMC" not in out

    def test_the_last_citation_wins_when_there_are_six(self):
        raw = ("§ 27-2005, 27-2007, 27-2041.1 HMC, §238, § 309; § 107 (2) ( C) "
               "MDL AND 28 RCNY §25-171: REPLACE OR REPAIR THE SELF-CLOSING DOOR")
        assert bd._plain(raw).startswith("Replace or repair the self-closing door")

    def test_the_apartment_number_is_not_printed(self):
        """A block report is about buildings, and the unit clause is both noise
        and somebody's front door."""
        out = bd._plain("HMC ADM CODE: § 27-2017 ABATE THE INFESTATION LOCATED AT APT 4R")
        assert "4R" not in out and "apt" not in out.lower()

    def test_shouting_is_taken_out(self):
        out = bd._plain("HMC ADM CODE: § 27-2005 REPAIR THE BROKEN FLOOR TILES")
        assert out == "Repair the broken floor tiles."

    def test_a_description_with_no_citation_survives(self):
        assert bd._plain("Boiler not operating") == "Boiler not operating."

    def test_empty_stays_empty(self):
        assert bd._plain("") == "" and bd._plain(None) == ""

    def test_long_text_cuts_on_a_word_boundary(self):
        out = bd._plain("REPLACE " + "widget " * 60, limit=40)
        assert len(out) <= 45 and not out.rstrip(".").endswith("wid")


class TestMergeRepeats:
    def test_same_building_same_kind_same_day_becomes_one_line(self):
        """HPD writes one violation per apartment, so an inspector's afternoon
        at a thirty-unit building arrives as thirty rows sharing a date. Printed
        straight, four identical lines took a third of the report."""
        merged = bd._merge_repeats([_event() for _ in range(4)])
        assert len(merged) == 1
        assert merged[0]["line"] == "4 HPD violations issued, Aug 9."
        assert merged[0]["rows"] == 4

    def test_different_days_stay_separate(self):
        merged = bd._merge_repeats([_event(), _event(date=date(2026, 8, 21))])
        assert len(merged) == 2

    def test_different_severity_stays_separate(self):
        merged = bd._merge_repeats([_event(), _event(bucket="hazard")])
        assert len(merged) == 2

    def test_a_lone_row_keeps_its_own_words(self):
        one = _event(line="Sold to WANG, SHU for $1,530,000, Jul 10.", kind="deed",
                     bucket="deed")
        assert bd._merge_repeats([one])[0]["line"] == one["line"]
        assert bd._merge_repeats([one])[0]["rows"] == 1

    def test_every_bucket_has_a_plural_line(self):
        """A bucket with no template would raise on the day it first repeats,
        which is a night nobody is watching."""
        for bucket in bd.WEIGHT:
            merged = bd._merge_repeats([_event(bucket=bucket, kind="violation")
                                        for _ in range(2)])
            assert merged[0]["line"].startswith("2 ")


class TestPerAddressCap:
    def test_one_neighbour_cannot_own_the_report(self):
        events = [_event(date=date(2026, 8, d)) for d in range(1, 9)]
        kept = bd._cap_per_address(events)
        assert len(kept) == bd.MAX_LINES_PER_ADDRESS

    def test_the_watched_building_is_exempt(self):
        """Someone who asked to be told about one building is not being told
        too much about that building."""
        events = [_event(mine=True, bbl="3008910006", date=date(2026, 8, d))
                  for d in range(1, 9)]
        assert len(bd._cap_per_address(events)) == 8

    def test_other_addresses_keep_their_own_budget(self):
        events = ([_event(bbl="A", date=date(2026, 8, d)) for d in range(1, 9)]
                  + [_event(bbl="B", date=date(2026, 8, d)) for d in range(1, 9)])
        kept = bd._cap_per_address(events)
        assert len(kept) == 2 * bd.MAX_LINES_PER_ADDRESS


class TestRender:
    def test_report_is_never_only_events(self):
        """The property the whole design rests on.

        A month with nothing recorded is the common case, not the edge: by
        event date only 5 of the 12 watched blocks had activity in thirty days.
        If the render produced nothing here, the mailer would refuse the send
        and the retention hole would be exactly where it started.
        """
        subject, html, text, items = bd.render(_report([_block(events=[], total=0)]))
        assert items > 0
        assert not mailer.check("watcher@example.com", subject, html, text,
                                content_items=items)
        assert "Nothing new was recorded" in text
        assert "66 open HPD violations" in text

    def test_a_quiet_month_still_leads_with_a_fact(self):
        subject, _, _, _ = bd.render(_report([_block(events=[], total=0)]))
        assert "66 open violations on record" in subject

    def test_a_busy_month_leads_with_the_count(self):
        subject, _, _, _ = bd.render(_report([_block(events=[_event()], total=12)]))
        assert subject.startswith("12 new records")

    def test_more_line_counts_records_not_lines(self):
        """A merged line stands for several rows. Counting lines here claimed
        six records were hidden when the four lines above covered all ten."""
        block = _block(events=[_event(rows=4), _event(rows=6, date=date(2026, 8, 21))],
                       total=10)
        _, _, text, _ = bd.render(_report([block]))
        assert "more record" not in text

    def test_the_hidden_remainder_is_reported(self):
        block = _block(events=[_event(rows=4)], total=10)
        _, _, text, _ = bd.render(_report([block]))
        assert "And 6 more records on this block." in text

    def test_the_watched_building_is_labelled(self):
        block = _block(events=[_event(mine=True, address="681 5 Avenue")])
        _, _, text, _ = bd.render(_report([block]))
        assert "681 5 Avenue (your building)" in text

    def test_every_section_names_its_source_and_date(self):
        """A number lifted out of this email travels without the page it came
        from. Over the fifteen days to 2026-08-27 the AI crawlers fetched more
        pages here than Googlebot did."""
        _, _, text, _ = bd.render(_report())
        assert text.count("Source: ") >= 2
        assert "current through August 25, 2026" in text

    def test_one_subscriber_two_blocks_is_one_email(self):
        """One reader follows three buildings across two blocks. A per-watch
        loop mailed them the same block twice."""
        second = _block()
        second["block"], second["label"] = "400679", "31-74 42 Street"
        subject, _, text, _ = bd.render(_report([_block(), second]))
        assert "one other block" in subject
        assert text.count("WHERE THE BLOCK STANDS") == 2

    def test_no_placeholder_survives_the_render(self):
        subject, html, text, items = bd.render(_report())
        assert "__TOKEN__" not in html and "__BODY__" not in html
        assert not mailer.check("watcher@example.com", subject, html, text,
                                content_items=items)

    def test_the_unsubscribe_token_is_in_the_footer(self):
        _, html, _, _ = bd.render(_report(token="abc123"))
        assert "unsubscribe?token=abc123" in html


class TestStandingSentences:
    def test_a_clean_block_says_so_rather_than_saying_nothing(self):
        block = _block(open_violations=0, violation_buildings=0, deeds_12m=0,
                       sold_buildings_12m=0, evictions_12m=0, worst_address=None,
                       worst_bbl=None, worst_open=0)
        lines = bd._state_sentences(block)
        assert any("No open HPD violations" in l for l in lines)
        assert any("has recorded a deed" in l for l in lines)
        assert len(lines) >= 3

    def test_more_deeds_than_buildings_is_called_out(self):
        """More deeds than addresses means one traded twice, which is the shape
        a flip leaves in the record."""
        lines = bd._state_sentences(_block(deeds_12m=9, sold_buildings_12m=4))
        assert any("across 9 recorded deeds" in l for l in lines)

    def test_one_deed_per_building_does_not_pad_the_sentence(self):
        lines = bd._state_sentences(_block(deeds_12m=4, sold_buildings_12m=4))
        assert any(l == "4 buildings changed hands in the last twelve months."
                   for l in lines)


class TestHouseStyle:
    def test_no_em_dashes_reach_the_reader(self):
        _, html, text, _ = bd.render(_report([_block(events=[_event()])]))
        assert "—" not in html and "—" not in text

    def test_no_template_constant_carries_an_em_dash(self):
        """The rendered check above only covers the paths this fixture walks.
        These are every string the renderer can reach."""
        templates = [bd._SHELL, bd._STAMP, bd._HEADING, bd._LINE, bd._PROSE, bd._CITE]
        templates += list(bd._PLURAL_LINE.values())
        for t in templates:
            assert "—" not in t, f"em dash in a template: {t[:60]}"

    def test_a_merged_line_is_not_a_dot_chain(self):
        """Prose takes commas. Dot chains are for code stamps, and the stamp
        line is the only place this report uses one."""
        for t in bd._PLURAL_LINE.values():
            assert " · " not in t and "&middot;" not in t
