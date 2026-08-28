"""
A permit that removes homes, and the four things that had to be true before it
could be said out loud.

DOB NOW carries existing and proposed dwelling counts and the difference looks
like the most direct displacement signal in the dataset. Read raw it is not:
2,164 filings a year "remove" 44,406 homes, and the largest is a $1,500
gas-valve permit on a 792-unit building. Filers use `proposed_dwelling_units`
inconsistently, and blanks arrive as 0.

Each condition in api.permit_kinds.deconversion_sql exists because a specific
row got through without it, and each is pinned below. The rule is not a
classifier: everything it surfaces is rendered with the applicant's own
description beside it so a reader checks it rather than trusting us.
"""

import pytest

from api import permit_kinds as pk


class TestTheRule:
    def test_both_aliases_are_required(self):
        """The parcel join is the one that excludes hotels and dormitories.
        Making it a required argument means it cannot be dropped by writing one
        fewer parameter."""
        with pytest.raises(TypeError):
            pk.deconversion_sql()
        with pytest.raises(TypeError):
            pk.deconversion_sql("pr")

    def test_blank_proposed_counts_are_excluded(self):
        """A blank arrives as 0. Of the 596 such rows, 5% mention a conversion
        against ~30% of the rest, which is what identifies them as blanks."""
        assert "units_proposed > 0" in pk.deconversion_sql("pr", "p")

    def test_the_unit_count_is_bounded(self):
        """One filer typed 111111111, which by itself accounted for 111 million
        of a 111,115,979-unit citywide 'total'."""
        sql = pk.deconversion_sql("pr", "p")
        assert f"BETWEEN {pk.MIN_UNITS} AND {pk.MAX_UNITS}" in sql
        assert pk.MAX_UNITS < 100_000

    def test_the_building_must_be_residential(self):
        """The biggest hits without this were a hotel turning 606 rooms into
        312 apartments and a dormitory reconfiguring 267 suites into 141.
        Both reduce a count; neither removes a home."""
        assert f"p.units_res >= {pk.MIN_UNITS}" in pk.deconversion_sql("pr", "p")

    def test_the_description_must_corroborate_twice(self):
        """Once for the conversion, once for the dwelling. 'CONVERSION OF
        EXISTING WET STANDPIPE TO DRY STANDPIPE' passed the first alone."""
        sql = pk.deconversion_sql("pr", "p")
        assert ":conv_re" in sql and ":dwell_re" in sql

    def test_there_is_no_cost_floor(self):
        """Measured, not assumed. The cheapest rows are real: '$100, PROPOSED
        CONVERSION OF EXISTING 3-FAMILY BUILDING', three homes to one.
        Deconversions are often filed as nominal-cost sub-filings."""
        assert "job_cost" not in pk.deconversion_sql("pr", "p")

    def test_it_is_scoped_to_the_source_that_has_the_columns(self):
        """Legacy BIS records no dwelling counts, so an unscoped predicate would
        silently compare NULLs."""
        assert "pr.source = 'dob_now'" in pk.deconversion_sql("pr", "p")

    def test_the_regexes_are_bound_not_interpolated(self):
        assert set(pk.DECONVERSION_PARAMS) == {"conv_re", "dwell_re"}
        assert "convert" not in pk.deconversion_sql("pr", "p")

    def test_aliases_reach_every_column(self):
        sql = pk.deconversion_sql("x", "y")
        assert "x.units_proposed" in sql and "y.units_res" in sql
        assert "pr." not in sql and " p." not in sql


class TestTheRegexes:
    @pytest.mark.parametrize("desc", [
        "CONVERT MULTIPLE DWELLING TO SINGLE FAMILY DWELLING",
        "Combining two apartments and interior alteration",
        "HEREBY FILING TO CONVERT SRO TO SINGLE FAMILY",
        "combination of dwelling units",
        "ALT1 FILED TO CONVERT 2 FAMILY HOUSE INTO 1 FAMILY",
    ])
    def test_real_deconversions_match_both(self, desc):
        import re
        assert re.search(pk._CONVERSION_WORDS, desc, re.I)
        assert re.search(pk._DWELLING_WORDS, desc, re.I)

    def test_a_standpipe_conversion_fails_the_dwelling_test(self):
        """The row that made the second regex necessary: it matched 'conversion'
        on a 362-unit building and had nothing to do with homes."""
        import re
        desc = "CONVERSION OF EXISTING WET STANDPIPE TO DRY STANDPIPE"
        assert re.search(pk._CONVERSION_WORDS, desc, re.I)
        assert not re.search(pk._DWELLING_WORDS, desc, re.I)


class TestItIsSurfaced:
    def test_the_property_page_carries_the_block(self):
        from pathlib import Path
        src = Path("api/routes/frontend.py").read_text()
        assert 'deconversion_sql("pr", "p")' in src
        assert "Homes proposed for removal" in src

    def test_the_page_discloses_where_the_numbers_come_from(self):
        """The counts are the applicant's, not an inspector's, and nothing
        before 2021 is covered. Both belong beside the number."""
        from pathlib import Path
        src = Path("api/routes/frontend.py").read_text()
        assert "come from the applicant, not from an inspection" in src
        assert "before 2021" in src

    def test_the_block_digest_carries_it(self):
        from pathlib import Path
        src = Path("scripts/block_digest.py").read_text()
        assert 'deconversion_sql("pr", "pc")' in src
        assert "on the applicant's own description" in src
