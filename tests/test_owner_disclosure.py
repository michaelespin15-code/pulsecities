"""
The rule that decides whether a registered owner may be named.

/privacy is a published promise: individuals' names, apartment numbers, tenant
names and private home addresses are withheld. Naming HPD's registered
responsible parties has to sit inside that promise, not beside it.

Two properties are guarded here and the first is structural.

**The service address is never stored.** 19.7% of IndividualOwner contacts and
14.7% of JointOwner contacts give the registered building as their business
address, so for roughly 23,000 people that field is a home address. The table
has no column for it. A gate that depends on every future SELECT remembering
something is the gate that failed on /property, where seven render sites named
private buyers and the audit had found three.

**An owner-occupant of a small building is a resident.** Where the service
address is the building and the building has one or two units, the name is
withheld: 4,023 contacts. At three or more units an owner living on site is a
landlord who happens to live there: 19,169 contacts, and a different thing.
"""
import pytest
from sqlalchemy import inspect, text

from api.owner_disclosure import OWNER_OCCUPIED_UNIT_CEILING, is_publishable
from models.database import SessionLocal


class TestTheRule:
    def test_an_organisation_is_always_nameable(self):
        assert is_publishable(is_organization=True, at_building=True, units_res=1)

    def test_a_person_who_lists_an_address_elsewhere_is_nameable(self):
        assert is_publishable(is_organization=False, at_building=False, units_res=1)

    def test_a_person_living_in_their_own_small_building_is_not(self):
        for units in range(1, OWNER_OCCUPIED_UNIT_CEILING + 1):
            assert not is_publishable(
                is_organization=False, at_building=True, units_res=units), units

    def test_a_landlord_living_in_a_larger_building_is_nameable(self):
        assert is_publishable(
            is_organization=False, at_building=True,
            units_res=OWNER_OCCUPIED_UNIT_CEILING + 1)

    def test_unknown_unit_count_withholds(self):
        """Absent data resolves against disclosure, not for it."""
        assert not is_publishable(is_organization=False, at_building=True, units_res=None)
        assert not is_publishable(is_organization=False, at_building=True, units_res=0)


@pytest.mark.integration
@pytest.mark.needs_data
class TestTheAddressIsNotStored:
    def test_the_table_has_no_address_column(self):
        cols = {c["name"] for c in inspect(SessionLocal().bind).get_columns("hpd_owner_contacts")}
        offenders = {c for c in cols
                     if any(w in c.lower() for w in
                            ("addr", "street", "house", "zip", "city", "state", "apartment"))}
        assert not offenders, (
            "the service address is back in the schema. For ~23,000 people it is "
            f"a home address: {sorted(offenders)}")

    def test_the_stored_verdict_matches_the_rule(self):
        """publishable is decided at ingest. If the stored column and the
        function disagree, one of them has been edited alone."""
        db = SessionLocal()
        try:
            wrong = db.execute(text("""
                SELECT COUNT(*) FROM hpd_owner_contacts c
                JOIN parcels p ON p.bbl = c.bbl
                WHERE c.publishable IS DISTINCT FROM (
                    c.is_organization
                    OR NOT c.at_building
                    OR COALESCE(p.units_res, 0) > :ceiling)
            """), {"ceiling": OWNER_OCCUPIED_UNIT_CEILING}).scalar()
        finally:
            db.close()
        assert wrong == 0, f"{wrong} rows disagree with api/owner_disclosure.py"

    def test_something_was_actually_withheld(self):
        """A gate that never fires is a gate nobody has tested."""
        db = SessionLocal()
        try:
            n = db.execute(text(
                "SELECT COUNT(*) FROM hpd_owner_contacts WHERE NOT publishable")).scalar()
        finally:
            db.close()
        assert n > 0, "no contact was withheld; the gate is not reaching the data"
