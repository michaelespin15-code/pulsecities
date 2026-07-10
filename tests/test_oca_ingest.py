"""
OCA petition ingest — ZIP-month aggregation over the de-identified extract.

The source is ZIP-only by design and CC BY-NC-SA licensed, so the pipeline
stores aggregates alone and the signal stays off API surfaces. These tests
exercise the spill-and-aggregate path with fixture CSVs.
"""

from pathlib import Path

import pytest

from scripts.oca_ingest import aggregate, build_spill

ADDRESSES = """indexnumberid,city,state,postalcode
AAA,Bronx,New York,10467
BBB,Brooklyn,New York,11216
CCC,Port Jefferson,New York,11776
DDD,Bronx,New York,10467
"""

INDEX = """indexnumberid,court,fileddate,propertytype,classification,specialtydesignationtypes,status,disposeddate,disposedreason,firstpaper,primaryclaimtotal,dateofjurydemand
AAA,Bronx County Civil Court,2026-03-12,Residential,Non-Payment,,Active,,,Petition by Attorney,5123.63,
BBB,Kings County Civil Court,2026-03-20,Residential,Holdover,,Active,,,Petition by Attorney,,
CCC,Suffolk County Court,2026-03-01,Residential,Non-Payment,,Active,,,Petition by Attorney,1482.72,
DDD,Bronx County Civil Court,2026-04-02,Commercial,Non-Payment,,Active,,,Petition by Attorney,,
AAA,Bronx County Civil Court,2018-01-05,Residential,Non-Payment,,Disposed,,,Petition by Attorney,,
"""


@pytest.fixture()
def workdir(tmp_path: Path):
    (tmp_path / "addresses.csv").write_text(ADDRESSES)
    (tmp_path / "index.csv").write_text(INDEX)
    return tmp_path


class TestAggregation:

    def test_spill_keeps_only_watched_zips(self, workdir):
        n = build_spill(workdir / "addresses.csv", {"10467", "11216"}, workdir / "ids.sqlite")
        assert n == 3  # AAA, BBB, DDD; CCC is Suffolk

    def test_aggregate_filters_and_counts(self, workdir):
        build_spill(workdir / "addresses.csv", {"10467", "11216"}, workdir / "ids.sqlite")
        counts = aggregate(workdir / "index.csv", workdir / "ids.sqlite")
        # CCC dropped (not NYC), DDD dropped (Commercial),
        # AAA 2018 dropped (before MIN_FILED). Two rows survive.
        assert counts[("10467", "2026-03-01", "Non-Payment")] == 1
        assert counts[("11216", "2026-03-01", "Holdover")] == 1
        assert sum(counts.values()) == 2

    def test_month_floors_to_first(self, workdir):
        build_spill(workdir / "addresses.csv", {"10467"}, workdir / "ids.sqlite")
        counts = aggregate(workdir / "index.csv", workdir / "ids.sqlite")
        assert all(m.endswith("-01") for _, m, _ in counts)
