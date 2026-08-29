"""
One owner for the question "is this name a private individual?".

ACRIS party names are public record and republishing them is lawful. That is not
the question. The question is whether this site should put a private homeowner's
name, their street address, the price they paid and the date, on a page that
tells Google to index it. The city's own record does not present it that way, and
nothing on this site needs it: /property ranks on the address, never the owner.

Measured 2026-08-28 before this module existed:

  - 43,212 sitemapped /property pages named a person-shaped ACRIS party.
  - 102 person-shaped names were printed across 57 /evictions pages under
    "who holds the deeds", ranked beside an eviction count.
  - Person-name queries were 81 impressions and 10 clicks of 29,700 and 731,
    so redacting them costs 0.27% of impressions.

**The portfolio-threshold idea was tested and abandoned.** The theory was that a
person holding many buildings is a landlord and fair to name. Of the 102 names on
/evictions, only 6 hold 5 or more buildings citywide, and every one of the 6 is
a misclassified organisation: US Bank Trust, RCF 2 Acquisition Trust, and four
Housing Development Fund Corporations, which are affordable-housing nonprofits.
The genuine individuals hold one building each. There was no landlord to protect
the naming of, so the threshold bought nothing and the gate is unconditional.

Why the test is org-detection rather than person-detection: `party_name_normalized`
has its comma stripped at ingest (scrapers/ownership.py), so "SMITH, JOHN" arrives
as "SMITH JOHN" and the strongest person signal is gone. What remains reliable is
the presence of an organisation token. **Unknown therefore resolves to person**,
which redacts a few organisations rather than publishing a few people.

`_is_buyer_entity` in api/routes/frontend.py is NOT this test and must not be
reused as it. It answers "may I link this name to /llc/{slug}", so it returns
False for a servicer, for a trustee, and for any name at or over 48 characters,
which is where the source truncates. Ten real organisations on /evictions were
person-shaped by that test purely because their names were long.
"""

import re

# The source truncates party_name_normalized at 48 characters. A name long
# enough to be cut is an organisation in every case checked; a person's name
# does not reach 48 characters.
_TRUNCATION_LEN = 48

# Organisation tokens as they actually appear in ACRIS party names. Deliberately
# broader than _ENTITY_FORM_RE, which knows only LLC|PLLC|LLP|CORP|INC|LTD|LP and
# therefore reads "WELLS FARGO BANK NATIONAL ASSOCIATION" and "BFH HOUSING
# DEVELOPMENT FUND CORPORATION" as people. "CORPORATION" does not match \bCORP\b.
# Organisation tokens as they actually appear in ACRIS party names. Deliberately
# broader than _ENTITY_FORM_RE, which knows only LLC|PLLC|LLP|CORP|INC|LTD|LP and
# therefore reads "WELLS FARGO BANK NATIONAL ASSOCIATION" and "BFH HOUSING
# DEVELOPMENT FUND CORPORATION" as people. "CORPORATION" does not match \bCORP\b.
#
# Kept as a list because two readers need it in two languages: this module
# compiles a Python regex from it, and org_sql() emits the Postgres equivalent
# for /api/search/landlord, which has to gate its summary aggregate in SQL so the
# counts describe the same population as the rows. Retyping it in SQL is exactly
# how the two would drift.
_ORG_WORDS = (
    "LLC", r"L\.L\.C", "PLLC", "LLP", r"L\.P", "LP", "INC", "INCORPORATED",
    "CORP", "CORPORATION", "LTD", "LIMITED", "CO", "COMPANY", "COMPANIES",
    "TRUST", "BANK", "BANKING", "ASSOCIATION", "ASSN", r"N\.A", "NA",
    "FUND", "HDFC", "HOUSING", "DEVELOPMENT", "PARTNERS", "PARTNERSHIP",
    "REALTY", "REALTIES", "PROPERTIES", "PROPERTY", "HOLDINGS", "HOLDING",
    "MANAGEMENT", "MGMT", "GROUP", "ENTERPRISES", "VENTURES", "CAPITAL",
    "EQUITIES", "INVESTORS", "INVESTMENT", "INVESTMENTS", "ACQUISITION",
    "ACQUISITIONS", "FOUNDATION", "INSTITUTE", "SOCIETY", "AUTHORITY", "AGENCY",
    "DEPARTMENT", "CHURCH", "TEMPLE", "SYNAGOGUE", "MOSQUE", "MINISTRIES",
    "CONGREGATION", "PARISH", "DIOCESE", "UNIVERSITY", "COLLEGE", "HOSPITAL",
    "CENTER", "CENTRE", "MUSEUM", "ACADEMY", "SCHOOL", "CITY", "STATE",
    "COUNTY", "BOARD", "COMMISSION", "NYCHA", "HPD", "MTA", "USA", "AMERICA",
    "CONDOMINIUM", "CONDO", "COOPERATIVE", "CO-OP", "COOP", "APARTMENTS",
    "TOWERS", "PLAZA", "ASSOCIATES", "SERVICES", "SOLUTIONS", "VENTURE",
    "PORTFOLIO", "PRESERVATION",
)

_ORG_TOKEN = re.compile(r"\b(" + "|".join(_ORG_WORDS) + r")\b")


def org_sql(column: str) -> str:
    """
    The same rule as a Postgres predicate, for callers that must filter in SQL.

    Postgres spells a word boundary \\y rather than \\b. The length arm mirrors
    _TRUNCATION_LEN: a name the source had to cut is an organisation.
    """
    pattern = r"\y(" + "|".join(_ORG_WORDS) + r")\y"
    return f"({column} ~* '{pattern}' OR length({column}) >= {_TRUNCATION_LEN})"


REDACTED = "Private individual"


def is_organisation(name: str) -> bool:
    """True when the name carries an organisation token, or is long enough that
    the source truncated it, which only happens to organisations."""
    if not name:
        return False
    return bool(_ORG_TOKEN.search(name.upper())) or len(name) >= _TRUNCATION_LEN


def is_natural_person(name: str) -> bool:
    """True when nothing marks this name as an organisation. Unknown resolves
    here, so the cost of being wrong is a redacted company, not a named person."""
    return bool(name) and not is_organisation(name)


def public_name(name: str, redacted: str = REDACTED) -> str:
    """The name this site may print. Organisations keep theirs; people do not."""
    return name if is_organisation(name) else redacted
