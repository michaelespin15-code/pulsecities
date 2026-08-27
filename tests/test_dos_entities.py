"""
Who the state register says stands behind a company, and when it says nobody.

/llc ranked at position 5 to 9 for roughly 1,700 monthly impressions of queries
asking who controls an LLC and answered none of them, because a deed names the
grantee and stops. DOS adds the DOS ID, the filing date, the jurisdiction and
the name designated for service of process.

The agent field is the whole value and the whole risk. Two thirds of numbered
LLCs designate themselves, a further slice designate a paid registered-agent
service, and only the remainder name a real third party. Getting that split
wrong does not degrade quietly: it prints a placeholder or a service company on
a page as the answer to who controls a building. The first run of the ingest
did exactly that, classing 447 rows spelled "The Limited Liability Company" and
106 spelled "C T CORPORATION SYSTEM" as named third parties, which is why the
classifier is pinned here rather than trusted.
"""

import pytest
from sqlalchemy import text

from api.routes import frontend
from models.database import SessionLocal
from scripts.refresh_dos_entities import agent_kind, normalize


@pytest.fixture(scope="module")
def db():
    s = SessionLocal()
    yield s
    s.close()


class TestAgentClassification:
    def test_an_entity_designating_itself_is_not_a_finding(self):
        assert agent_kind("ACME HOLDINGS LLC", "ACME HOLDINGS LLC") == "self"
        assert agent_kind("ACME HOLDINGS LLC", "Acme Holdings, LLC") == "self"

    def test_the_c_o_prefix_does_not_make_a_company_its_own_third_party(self):
        """"C/O ACME HOLDINGS LLC" on ACME HOLDINGS LLC is the company itself."""
        assert agent_kind("ACME HOLDINGS LLC", "C/O ACME HOLDINGS LLC") == "self"

    @pytest.mark.parametrize("placeholder", [
        "The Limited Liability Company", "THE LLC", "The Company", "SELF", "NONE",
    ])
    def test_placeholders_are_not_third_parties(self, placeholder):
        """DOS accepts these words in the process-name field and they designate
        the filer. 447 rows of them were classed as named third parties on the
        first run, which would have printed "The Limited Liability Company" as
        the answer to who controls a building."""
        assert agent_kind("ACME HOLDINGS LLC", placeholder) == "self"

    @pytest.mark.parametrize("service", [
        "C T CORPORATION SYSTEM", "C/O C T CORPORATION SYSTEM",
        "CT Corporation System", "REGISTERED AGENT SOLUTIONS, INC.",
        "CORPORATION SERVICE COMPANY", "USACORP INC", "Corporate Creations Network Inc",
    ])
    def test_commercial_agents_are_labelled_as_services(self, service):
        """Spacing is the trap: the list read "CT CORPORATION" while DOS writes
        "C T CORPORATION", so 106 rows were called third parties."""
        assert agent_kind("ACME HOLDINGS LLC", service) == "commercial"

    def test_a_real_third_party_survives_all_of_that(self):
        assert agent_kind("FLGSP 1023 CARROLL ST LLC",
                          "C/O SUMMIT MALLS MANAGEMENT LLC") == "third_party"

    def test_a_missing_agent_says_nothing_rather_than_guessing(self):
        assert agent_kind("ACME HOLDINGS LLC", None) == "none"
        assert agent_kind("ACME HOLDINGS LLC", "   ") == "none"

    def test_the_page_and_the_ingest_share_one_classifier(self):
        """Two copies of this rule would drift, and the drift would show up as a
        shell's own name presented as a controlling party."""
        assert frontend._dos_agent_kind("ACME LLC", "ACME LLC") == "self"
        assert frontend._dos_agent_kind(
            "FLGSP 1023 CARROLL ST LLC", "C/O SUMMIT MALLS MANAGEMENT LLC"
        ) == "third_party"


class TestTheJoinKey:
    def test_normalize_matches_the_sql_the_page_uses(self, db):
        """The page joins with regexp_replace(upper(name), '[^A-Z0-9]+', ' ').
        Python and Postgres have to agree or every lookup silently misses."""
        for name in ("FLGSP 1023 CARROLL ST LLC", "A.B.C. Realty, L.L.C.",
                     "123  MAIN   STREET LLC"):
            pg = db.execute(
                text("SELECT btrim(regexp_replace(upper(:n), '[^A-Z0-9]+', ' ', 'g'))"),
                {"n": name},
            ).scalar()
            assert normalize(name) == pg, f"python and postgres disagree on {name!r}"


@pytest.mark.integration
class TestWhatWeActuallyHold:
    def test_the_table_has_rows(self, db):
        n = db.execute(text("SELECT count(*) FROM dos_entities")).scalar()
        if not n:
            pytest.skip("dos_entities not yet populated")
        assert n > 1000, f"only {n} DOS rows; the refresh may be failing quietly"

    def test_most_rows_resolve_to_a_deed_buyer(self, db):
        """This table is selective by design. A row that matches no deed party is
        wasted storage and a sign the ingest lost its filter."""
        n = db.execute(text("SELECT count(*) FROM dos_entities")).scalar()
        if not n:
            pytest.skip("dos_entities not yet populated")
        matched = db.execute(text("""
            SELECT count(*) FROM dos_entities d
            WHERE EXISTS (
                SELECT 1 FROM ownership_raw o
                WHERE o.doc_type = 'DEED' AND o.party_type = '2'
                  AND regexp_replace(upper(o.party_name_normalized),
                                     '[^A-Z0-9]+', ' ', 'g') = d.entity_name_normalized)
        """)).scalar()
        assert matched / n > 0.9, (
            f"only {matched}/{n} DOS rows join to a deed buyer; the ingest is "
            f"storing entities the site cannot use"
        )
