"""
The landlord search is a landlord search, not a people search.

`GET /api/search/landlord?q=<substring>` returned every deed party whose name
matched, including 36,733 natural persons, each with their address, ZIP, price,
date **and latitude/longitude**. It has no auth, only a rate limit, and
frontend/methodology.html advertises it in words: "You can also search any
landlord or LLC name to see their full NYC portfolio."

robots.txt disallows /api/, so this was never an indexing problem. It is a
people-search problem: a surname substring returned where those people live.

The filters it did carry excluded mortgage servicers, which is a different
concern (a servicer taking title in foreclosure is not a buyer) and does not
help here.

The gate is api/person_privacy.py, the same rule the pages use, expressed as SQL
so the summary aggregate and the result rows agree. Two readers of one rule, and
the SQL is generated from the same token list rather than retyped.
"""

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def test_the_sql_predicate_is_generated_from_the_python_rule():
    """Retyping the token list in SQL is how the two would drift apart."""
    from api.person_privacy import is_natural_person, _ORG_WORDS, org_sql
    assert _ORG_WORDS, "the token list must be the single source"
    for w in ("LLC", "TRUST", "BANK", "HOUSING"):
        assert w in _ORG_WORDS
    frag = org_sql("party_name_normalized")
    assert "party_name_normalized" in frag and "LLC" in frag


def test_the_endpoint_applies_it_to_rows_and_to_the_aggregate():
    src = (REPO / "api" / "routes" / "search.py").read_text()
    assert "org_sql" in src or "person_privacy" in src, (
        "/api/search/landlord must gate on api/person_privacy.py"
    )
    # Both the summary aggregate and the LIMIT 50 result set, or the counts
    # describe a different population than the rows.
    assert src.count("org_sql(") >= 2, (
        "the aggregate and the result query must both be gated, or the summary "
        f"counts people the rows do not show (found {src.count('org_sql(')})"
    )
