"""
The second permit scraper, and the mapping the score depends on.

scrapers/permits.py reads ipu4-2q9a, the legacy DOB Permit Issuance dataset,
and DOB NOW superseded it. Measured 2026-08-28: we held 6,501 permits for the
last 365 days against roughly 170,000 upstream, and after the scoring filter
the permit signal was 414 records across 106 of 177 scored ZIPs, carrying 24.7%
of the composite score. Removing that term entirely and renormalising moved
ranks by a median of 13 places and changed half the top ten, so the sparsity
was never cosmetic.

The tests that matter here are the mapping ones. `scoring/compute.py` filters on
`permit_type = 'AL'` and was deliberately left unchanged, which only works
because this scraper maps DOB NOW's spelled-out `job_type` onto the BIS codes.
If that map drifts, the score moves and nothing else fails.

No database and no network. Everything below is a dict shaped like a Socrata row.
"""

import pytest

from scrapers import dob_now_permits as dn
from scrapers.dob_now_permits import DobNowPermitsScraper


def _raw(**kw):
    base = {
        "job_filing_number": "B01089478-I1",
        "bbl": "3034890001",
        "borough": "Brooklyn",
        "block": "3489",
        "lot": "1",
        "bin": "3086543",
        "house_no": "1062",
        "street_name": "ELTON STREET",
        "postcode": "11208",
        "job_type": "Alteration",
        "first_permit_date": "2026-08-03T00:00:00.000",
        "job_description": "Replacing existing metal stairs.",
        "owner_first_name": "JANE",
        "owner_last_name": "DOE",
        "general_construction_work_type_": "YES",
        "structural_work_type_": "NO",
    }
    base.update(kw)
    return base


class _Quarantine:
    """Stands in for the db session, capturing what would be quarantined."""
    def __init__(self):
        self.reasons = []


@pytest.fixture
def scraper(monkeypatch):
    s = DobNowPermitsScraper()
    captured = _Quarantine()
    monkeypatch.setattr(s, "quarantine",
                        lambda db, raw, reason: captured.reasons.append(reason))
    s.captured = captured
    return s


class TestJobTypeMapping:
    """The score reads these codes. A wrong one moves a number nothing checks."""

    @pytest.mark.parametrize("job_type,code", [
        ("Alteration", "AL"),
        ("Alteration CO", "AL"),
        ("New Building", "NB"),
        ("ALT-CO - New Building with Existing Elements to Remain", "NB"),
        ("Full Demolition", "DM"),
        ("No Work", "NW"),
    ])
    def test_every_published_job_type_maps(self, scraper, job_type, code):
        """These six are the complete set the dataset publishes, counted on
        2026-08-28. A seventh appearing is a quarantine, not a guess."""
        row = scraper._parse(None, _raw(job_type=job_type))
        assert row["permit_type"] == code

    def test_case_does_not_matter(self, scraper):
        assert scraper._parse(None, _raw(job_type="alteration"))["permit_type"] == "AL"

    def test_an_unknown_job_type_is_quarantined_not_guessed(self, scraper):
        assert scraper._parse(None, _raw(job_type="Sidewalk Widening")) is None
        assert scraper.captured.reasons[0].startswith("unmapped_job_type:")

    def test_alteration_co_is_not_downgraded(self):
        """An alteration that also changes the certificate of occupancy is a
        stronger displacement signal than a plain one, not a weaker one."""
        assert dn.JOB_TYPE_CODE["ALTERATION CO"] == "AL"

    def test_new_building_with_existing_elements_is_not_an_alteration(self):
        """It keeps a facade and builds behind it. BIS called that NB."""
        assert dn.JOB_TYPE_CODE[
            "ALT-CO - NEW BUILDING WITH EXISTING ELEMENTS TO REMAIN"] == "NB"

    def test_codes_fit_the_column(self):
        assert all(len(c) <= 10 for c in dn.JOB_TYPE_CODE.values())

    def test_codes_stay_in_the_bis_vocabulary(self):
        """scoring/compute.py filters `permit_type = 'AL'` and was left alone.
        That only holds while both sources speak the same short codes."""
        assert set(dn.JOB_TYPE_CODE.values()) <= {"AL", "NB", "DM", "NW"}


class TestWorkType:
    def test_flags_become_a_compact_string(self, scraper):
        row = scraper._parse(None, _raw(**{"general_construction_work_type_": "YES",
                                           "structural_work_type_": "YES"}))
        assert row["work_type"] == "GC+STR"

    def test_no_flags_is_empty_string_not_null(self):
        """The BIS half of the table keys on work_type, and NULL is distinct
        from NULL in a unique index, so a null here lets duplicates through."""
        assert dn._work_type({}) == ""

    def test_order_is_stable_not_dict_order(self):
        a = dn._work_type({"structural_work_type_": "YES",
                           "general_construction_work_type_": "YES"})
        b = dn._work_type({"general_construction_work_type_": "YES",
                           "structural_work_type_": "YES"})
        assert a == b == "GC+STR"

    def test_no_is_not_treated_as_set(self):
        assert dn._work_type({"general_construction_work_type_": "NO"}) == ""

    def test_result_fits_the_column(self):
        every = {field: "YES" for field, _ in dn.TRADES}
        assert len(dn._work_type(every)) <= 50


class TestBbl:
    def test_the_native_column_is_used(self, scraper):
        assert scraper._parse(None, _raw())["bbl"] == "3034890001"

    def test_a_float_serialisation_still_parses(self):
        """Socrata types bbl as a number, so it can arrive as 3034890001.0."""
        rec = dn.JobFilingInput.model_validate(_raw(bbl="3034890001.0"))
        assert dn._bbl(rec) == "3034890001"

    def test_borough_block_lot_is_the_fallback(self):
        rec = dn.JobFilingInput.model_validate(_raw(bbl=None))
        assert dn._bbl(rec) == "3034890001"

    def test_leading_zeros_in_block_and_lot_survive(self):
        rec = dn.JobFilingInput.model_validate(
            _raw(bbl=None, block="03489", lot="0001"))
        assert dn._bbl(rec) == "3034890001"

    def test_an_unknown_borough_yields_nothing(self):
        rec = dn.JobFilingInput.model_validate(_raw(bbl=None, borough="Yonkers"))
        assert dn._bbl(rec) is None

    def test_a_row_with_neither_bbl_nor_bin_is_quarantined(self, scraper):
        assert scraper._parse(None, _raw(bbl=None, borough=None, bin=None)) is None
        assert "missing_bbl_and_bin" in scraper.captured.reasons


class TestRowIdentity:
    def test_the_filing_number_becomes_the_source_id(self, scraper):
        row = scraper._parse(None, _raw())
        assert row["source"] == "dob_now"
        assert row["source_id"] == "B01089478-I1"

    def test_a_row_without_a_filing_number_is_quarantined(self, scraper):
        """Without it there is no identity, so the row dedupes against nothing
        and multiplies on every run."""
        assert scraper._parse(None, _raw(job_filing_number="")) is None
        assert "missing_job_filing_number" in scraper.captured.reasons

    def test_a_job_with_no_permit_yet_is_not_a_permit_event(self, scraper):
        assert scraper._parse(None, _raw(first_permit_date=None)) is None
        assert "missing_first_permit_date" in scraper.captured.reasons


class TestFieldMapping:
    def test_first_permit_date_becomes_filing_date(self, scraper):
        row = scraper._parse(None, _raw())
        assert row["filing_date"].isoformat() == "2026-08-03"

    def test_address_joins_house_and_street(self, scraper):
        assert scraper._parse(None, _raw())["address"] == "1062 ELTON STREET"

    def test_owner_falls_back_to_the_business_name(self, scraper):
        row = scraper._parse(None, _raw(owner_first_name=None, owner_last_name=None,
                                        owner_s_business_name="ELTON HOLDINGS LLC"))
        assert row["owner_name"] == "ELTON HOLDINGS LLC"

    def test_zip_comes_from_postcode(self, scraper):
        assert scraper._parse(None, _raw())["zip_code"] == "11208"

    def test_a_zip_plus_four_is_cut_back(self):
        assert dn._clean_zip("11208-1234") == "11208"

    def test_a_malformed_zip_is_dropped_not_stored(self):
        assert dn._clean_zip("ABCDE") is None and dn._clean_zip("112") is None

    @pytest.mark.parametrize("value,expected", [
        ("2026-08-03T00:00:00.000", "2026-08-03"),
        ("2026-08-03", "2026-08-03"),
        ("08/03/2026", "2026-08-03"),
    ])
    def test_date_formats(self, value, expected):
        assert dn._parse_date(value).isoformat() == expected

    def test_an_unparseable_date_is_none_not_an_exception(self):
        assert dn._parse_date("not a date") is None

    def test_the_full_raw_row_is_kept(self, scraper):
        raw = _raw(some_new_dob_column="value")
        assert scraper._parse(None, raw)["raw_data"]["some_new_dob_column"] == "value"


class TestBackfillIsolation:
    def test_a_windowed_run_is_marked_a_backfill(self):
        """Recorded as an ordinary success, a 485k-row historical walk poisons
        the 14-day rolling average: from the third night on, every normal run
        lands under 50% of it and emails ops. Eleven nights of false alarms is
        how a real one gets ignored."""
        from datetime import date
        assert DobNowPermitsScraper(since=date(2021, 1, 1)).is_backfill is True

    def test_a_normal_run_is_not(self):
        assert DobNowPermitsScraper().is_backfill is False

    def test_the_backfill_status_is_excluded_from_the_rolling_average(self):
        """_compute_rolling_avg counts only success and warning. This asserts
        the status is not one of those, which is the whole mechanism."""
        from scrapers.base import BaseScraper
        assert BaseScraper.BACKFILL_STATUS not in ("success", "warning", "failure")

    def test_the_backfill_status_fits_the_column(self):
        from models.scraper import ScraperRun
        from scrapers.base import BaseScraper
        assert len(BaseScraper.BACKFILL_STATUS) <= ScraperRun.__table__.c.status.type.length


class TestRegistration:
    def test_the_scraper_is_in_the_nightly_pipeline(self):
        """A scraper nobody calls is a file, not a feed."""
        from pathlib import Path
        src = Path("scheduler/pipeline.py").read_text()
        assert '("dob_now_permits", DobNowPermitsScraper)' in src

    def test_it_has_a_minimum_record_expectation(self):
        """The whole reason it exists is that dob_permits decayed to a trickle
        while reporting success every night. This one is measured against a floor."""
        from config.nyc import SCRAPER_EXPECTED_MIN_RECORDS
        assert SCRAPER_EXPECTED_MIN_RECORDS["dob_now_permits"] > 0

    def test_the_permits_feed_is_anchored_on_the_live_source(self):
        from api.freshness import FRESHNESS_SOURCES
        scrapers = {slug: name for slug, name, _t, _c, _d in FRESHNESS_SOURCES}
        assert scrapers["permits"] == "dob_now_permits"

    def test_the_old_scraper_name_still_resolves(self):
        """Consumers key on it; changing the anchor must not KeyError them."""
        from api.freshness import db_through_sql, staleness_days
        assert staleness_days("dob_permits") == staleness_days("dob_now_permits")
        assert db_through_sql("dob_permits") == db_through_sql("permits")

    def test_the_permits_feed_is_listed_once(self):
        """Every consumer renders one card per FRESHNESS_SOURCES row, so a
        second entry for the same table would draw the permits card twice."""
        from api.freshness import FRESHNESS_SOURCES
        slugs = [slug for slug, *_ in FRESHNESS_SOURCES]
        assert len(slugs) == len(set(slugs))
