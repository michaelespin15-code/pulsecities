"""
Guards on how the weekly report ages the offsite backup slots.

The offsite copy rotates through seven daily slots, each overwritten once a
week. Every backup check we had reported the newest object, which stays fresh
even when one weekday stops pushing, so the 'sat' slot went nine days stale in
full view of a report that said the backups were fine. That is shape 3 in
docs/ops/failure_patterns.md, a check that cannot fail.

These tests pin the per-slot ageing and, just as importantly, the quiet first
week: a section that cries wolf while it is still filling in is a section people
learn to skip.

No network, no database.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest

import scripts.weekly_ops_health as woh


def _state(**ages_in_days):
    """Slot state where each named weekday was pushed N days ago."""
    now = datetime.now(timezone.utc)
    return {
        f"daily/{day}.sql.gz": {
            "pushed_at": (now - timedelta(days=age)).isoformat(),
            "bytes": 1_700_000_000,
        }
        for day, age in ages_in_days.items()
    }


def _write(tmp_path, monkeypatch, state):
    path = tmp_path / "backup_offsite_slots.json"
    path.write_text(json.dumps(state))
    monkeypatch.setattr(woh, "OFFSITE_SLOTS", path)
    return path


class TestHealthyRotation:
    def test_a_full_fresh_rotation_raises_nothing(self, tmp_path, monkeypatch):
        _write(tmp_path, monkeypatch, _state(
            mon=6, tue=5, wed=4, thu=3, fri=2, sat=1, sun=0))
        attention = []
        lines = woh._offsite_lines(attention)
        assert attention == []
        assert any("daily/sat" in line for line in lines)


class TestStaleSlot:
    def test_one_rotting_weekday_is_reported(self, tmp_path, monkeypatch):
        """The exact shape of the incident: six fresh slots hide the seventh."""
        _write(tmp_path, monkeypatch, _state(
            mon=6, tue=5, wed=4, thu=3, fri=2, sat=9, sun=0))
        attention = []
        woh._offsite_lines(attention)
        assert any("sat" in item for item in attention), (
            "a nine-day-old slot passed unreported, which is the bug this exists for"
        )

    def test_fresh_slots_are_not_flagged_alongside_it(self, tmp_path, monkeypatch):
        _write(tmp_path, monkeypatch, _state(
            mon=6, tue=5, wed=4, thu=3, fri=2, sat=9, sun=0))
        attention = []
        woh._offsite_lines(attention)
        assert len(attention) == 1

    def test_the_boundary_is_not_off_by_one(self, tmp_path, monkeypatch):
        """A slot at exactly the limit is late, not yet failing."""
        _write(tmp_path, monkeypatch, _state(sat=woh.OFFSITE_SLOT_MAX_AGE_DAYS))
        attention = []
        woh._offsite_lines(attention)
        assert not any("sat" in item for item in attention)


class TestFirstWeekIsQuiet:
    def test_unseen_slots_are_not_faults_before_a_rotation(self, tmp_path, monkeypatch):
        """Day two of recording: six slots are simply not due yet."""
        _write(tmp_path, monkeypatch, _state(mon=1, tue=0))
        attention = []
        lines = woh._offsite_lines(attention)
        assert attention == []
        assert any("not yet recorded" in line for line in lines)

    def test_unseen_slots_become_faults_once_a_rotation_has_passed(self, tmp_path, monkeypatch):
        """After a full week, a slot that never landed is a real gap."""
        _write(tmp_path, monkeypatch, _state(mon=9, tue=1))
        attention = []
        woh._offsite_lines(attention)
        assert any("never been pushed" in item for item in attention)


class TestDegradedInputs:
    def test_missing_file_says_so_without_claiming_failure(self, tmp_path, monkeypatch):
        monkeypatch.setattr(woh, "OFFSITE_SLOTS", tmp_path / "absent.json")
        attention = []
        lines = woh._offsite_lines(attention)
        assert "not recorded yet" in attention[0]
        assert any("no slot pushes recorded" in line for line in lines)

    def test_corrupt_file_is_reported_not_raised(self, tmp_path, monkeypatch):
        path = tmp_path / "backup_offsite_slots.json"
        path.write_text("{not json")
        monkeypatch.setattr(woh, "OFFSITE_SLOTS", path)
        attention = []
        woh._offsite_lines(attention)  # must not raise
        assert any("unreadable" in item for item in attention)

    def test_unparseable_timestamp_does_not_kill_the_section(self, tmp_path, monkeypatch):
        _write(tmp_path, monkeypatch, {
            "daily/mon.sql.gz": {"pushed_at": "whenever"},
            **_state(tue=1),
        })
        attention = []
        lines = woh._offsite_lines(attention)
        assert any("daily/tue" in line for line in lines)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
