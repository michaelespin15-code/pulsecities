"""
The year printed beside an assessment has to be the year of that assessment.

/property renders "the city assessed this lot at $287K for the 2026 tax year"
from `max(tax_year)` in assessment_history. On 2026-08-29 that sentence was
wrong on 10,197 sitemapped pages. The figure was a 2014/15 assessment and the
year was the calendar year that scrapers/pluto.py happened to run, stamped onto
59,423 lots PLUTO has never seen.

Two rules came out of it, one per writer:

  a snapshot may only date the lots it read
  a loader must file each row under the fiscal year it read

Neither is checkable by reading the sentence, so both are checked here against
the database, and the loader's half runs the real statement.
"""
import pytest
from sqlalchemy import text

from models.database import SessionLocal

# A lot MapPLUTO has never described; see scripts/repair_assessment_years.py.
NEVER_SEEN_BY_PLUTO = """
    p.units_res IS NULL AND p.year_built IS NULL AND p.lot_area IS NULL
    AND p.bldg_area IS NULL AND p.land_use IS NULL
"""


@pytest.fixture()
def db():
    session = SessionLocal()
    yield session
    session.rollback()
    session.close()


@pytest.mark.integration
@pytest.mark.needs_data
class TestTheSyncTakesTheNewestYearAndOnlyWhereItMay:
    """Runs scrapers/dof.py's real statement, then rolls it back.

    `_sync_parcel_assessments` commits, which is how a test suite once
    committed deletes of two days of score_history. The commit is stubbed out
    rather than trusted.
    """

    def _dof_only_lot(self, db):
        row = db.execute(text(f"""
            SELECT p.bbl FROM parcels p
            WHERE {NEVER_SEEN_BY_PLUTO} AND p.assessed_total IS NOT NULL
            ORDER BY md5(p.bbl) LIMIT 1
        """)).first()
        assert row, "no DOF-only lot to test against"
        return row[0]

    def _pluto_lot(self, db):
        row = db.execute(text(f"""
            SELECT p.bbl FROM parcels p
            JOIN assessment_history ah ON ah.bbl = p.bbl
            WHERE NOT ({NEVER_SEEN_BY_PLUTO}) AND ah.assessed_total IS NOT NULL
              AND ah.tax_year = (SELECT MAX(tax_year) FROM assessment_history
                                  WHERE assessed_total IS NOT NULL)
            ORDER BY md5(p.bbl) LIMIT 1
        """)).first()
        assert row, "no MapPLUTO lot to test against"
        return row[0]

    def _file(self, db, bbl, year, total):
        db.execute(
            text("""INSERT INTO assessment_history (bbl, assessed_total, tax_year, created_at)
                    VALUES (:bbl, :total, :year, NOW())
                    ON CONFLICT (bbl, tax_year) DO UPDATE
                       SET assessed_total = EXCLUDED.assessed_total"""),
            {"bbl": bbl, "year": year, "total": total},
        )

    def test_the_newest_filed_year_wins_and_a_current_lot_is_left_alone(self, db, monkeypatch):
        from scrapers.dof import DOFScraper

        archive_lot = self._dof_only_lot(db)
        current_lot = self._pluto_lot(db)
        current_before = db.execute(
            text("SELECT assessed_total FROM parcels WHERE bbl = :b"), {"b": current_lot}
        ).scalar()

        # The archive lot as it will look once the repair has cleared the
        # fabricated snapshot row: only DOF years on file.
        db.execute(text("DELETE FROM assessment_history WHERE bbl = :b"), {"b": archive_lot})
        self._file(db, archive_lot, 1990, 111_111.0)
        self._file(db, archive_lot, 1991, 222_222.0)
        # And an archive year for the lot MapPLUTO owns, which must not move it.
        self._file(db, current_lot, 1991, 999_999.0)

        scraper = DOFScraper.__new__(DOFScraper)
        monkeypatch.setattr(db, "commit", lambda: None)
        scraper._sync_parcel_assessments(db, newest_year=1991)

        after = dict(db.execute(
            text("SELECT bbl, assessed_total FROM parcels WHERE bbl IN (:a, :c)"),
            {"a": archive_lot, "c": current_lot},
        ).fetchall())

        assert after[archive_lot] == 222_222.0, (
            "the sync took a year that is not the newest one on file. Pagination "
            "order picked the year before this existed, and it resolved to a "
            "different fiscal year per lot: 2011/12, 2014/15, 2016/17"
        )
        assert after[current_lot] == current_before, (
            "a 1991 assessment overwrote a lot that already carries a current "
            "one. That is how a 2014/15 figure came to be published as 2026"
        )

    def test_it_does_nothing_when_the_feed_yielded_no_year(self, db, monkeypatch):
        """A walk that parsed no fiscal year has nothing to say about which
        year wins, and must not fall back to touching every lot."""
        from scrapers.dof import DOFScraper

        scraper = DOFScraper.__new__(DOFScraper)
        monkeypatch.setattr(db, "commit", lambda: None)
        assert scraper._sync_parcel_assessments(db, newest_year=None) == 0


@pytest.mark.integration
@pytest.mark.needs_data
class TestNoLotIsDatedByAScraperThatNeverReadIt:
    def test_the_snapshot_year_covers_only_mappluto_lots(self, db):
        """The live assertion. Until scripts/repair_assessment_years.py runs,
        59,423 lots carry a snapshot-year row written by a scraper that has
        never seen them, and /property prints that year as fact."""
        stragglers = db.execute(text(f"""
            SELECT COUNT(*) FROM assessment_history ah
            JOIN parcels p ON p.bbl = ah.bbl
            WHERE {NEVER_SEEN_BY_PLUTO}
              AND ah.tax_year = (SELECT MAX(tax_year) FROM assessment_history
                                  WHERE assessed_total IS NOT NULL)
        """)).scalar()
        assert stragglers == 0, (
            f"{stragglers} lots MapPLUTO has never read carry a row dated with "
            "the snapshot year. Run:\n"
            "  venv/bin/python -m scripts.repair_assessment_years\n"
            "  venv/bin/python -m scrapers.dof"
        )
