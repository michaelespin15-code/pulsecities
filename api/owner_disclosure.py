"""
Who may be named as responsible for a building, and who may not.

The deed data could not answer this. `/llc/{person-slug}` published 40,532
dossiers built from ACRIS grantee names, and the test that was supposed to
separate landlords from homeowners failed: of 102 person-shaped owners on
repeat-eviction buildings, the 6 holding five or more buildings were all
misclassified organisations, and every genuine individual held exactly one.
A deed records that someone bought a home. Nothing about it says they run one.

HPD registration is a different record with a different published purpose. The
city requires an owner to register a responsible party *so tenants and the
courts can reach them*, and it requires it only of multiple dwellings and of
one- and two-family homes the owner does not live in. Measured on the 2026-08-29
extract: 181,484 of 182,366 registered lots match a parcel, and 77.1% carry
three or more residential units. The registration requirement is itself the
filter the deed data never had.

Two rules, and the first is why this module exists rather than a WHERE clause.

**The service address is never stored.** Not withheld at query time: never
written. 19.7% of IndividualOwner contacts and 14.7% of JointOwner contacts give
the registered building as their business address, so for roughly 23,000 people
the "business address" is where they sleep. A column that holds it is a column
something eventually selects. `scrapers/hpd_registrations.py` compares it during
ingest, keeps the boolean, and discards the string.

**An owner-occupant of a small building is a resident, not an operation.** Where
the business address is the building and the building has one or two units, the
name is withheld. That is 4,023 contacts. On a building of three or more units
an owner living on site is a landlord who happens to live there, which is 19,169
contacts and is not the same thing.

Organisations are named without qualification. `/privacy` promises to withhold
individuals' names, apartment numbers, tenant names and private home addresses.
It does not promise to hide who owns a building, and a corporate owner has no
private home.

Related: api/person_privacy.py gates the ACRIS party names, which are a
different population under a different rule.
"""
from __future__ import annotations

# Roles HPD publishes. CorporateOwner and the organisation half of Agent are
# entities; the rest are people.
PERSON_ROLES = ("HeadOfficer", "IndividualOwner", "JointOwner", "Officer", "Shareholder")
ORG_ROLES = ("CorporateOwner",)
# Agent is both, decided per row by whether corporationname is set.
MIXED_ROLES = ("Agent",)

# A building this small, registered to someone whose business address is the
# building, is a home. Three or more units is a rental operation whatever the
# owner's address says.
OWNER_OCCUPIED_UNIT_CEILING = 2


def is_publishable(*, is_organization: bool, at_building: bool, units_res: int | None) -> bool:
    """May this contact's name appear on a public page?

    Computed once at ingest and stored, so no reader has to remember the rule.
    `at_building` is whether the registered service address matched the building
    itself; the address that produced it is not retained.
    """
    if is_organization:
        return True
    if not at_building:
        return True
    return (units_res or 0) > OWNER_OCCUPIED_UNIT_CEILING


def publishable_sql(contacts: str = "c") -> str:
    """The same rule for callers that filter in SQL rather than in Python.

    One expression, one owner. The ACRIS gate drifted to eight call sites with
    one enforcer before anyone noticed, so this returns the predicate rather
    than inviting each reader to write it again.
    """
    return f"{contacts}.publishable IS TRUE"
