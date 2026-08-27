"""
Nightly Department of State lookup for entities in the deed record.

/llc ranks at position 5 to 9 for roughly 1,700 monthly impressions of queries
asking who controls a company, and converts none of them, because a deed names
the grantee and stops. New York does not require an LLC to name a member on one.
DOS carries the DOS ID, the filing date, the jurisdiction, and the name and
address designated for service of process.

Selective, not bulk. data.ny.gov n9v6-gdp6 holds 4.2M rows and we care about the
LLC-form names that appear as deed buyers. Those go up in batches of 400 through
a Socrata `in (...)` filter, which is about 60 requests and half a second each,
against a database already at 69% of its disk. 78% of sampled names resolve.

Reading the agent field, which is the whole point and the part that is easy to
get wrong:

    self       most numbered LLCs designate themselves; says nothing
    commercial a registered-agent service; says only that they paid for privacy
    third party a real other name, which is the answer people search for

The distinction is computed here rather than at render time so a page cannot
quietly start presenting a shell's own name as a finding.

Usage:
    python -m scripts.refresh_dos_entities              # names not yet looked up
    python -m scripts.refresh_dos_entities --all        # re-check everything
"""

import argparse
import json
import logging
import re
import sys
import urllib.parse
import urllib.request
from datetime import date

from sqlalchemy import text

from models.database import get_scraper_db

logger = logging.getLogger(__name__)

_DATASET = "https://data.ny.gov/resource/n9v6-gdp6.json"
_BATCH = 400
_TIMEOUT = 120

# Registered-agent services. An entity designating one of these has bought
# privacy, which is worth saying plainly and is not the same as naming a
# controlling party. Matched as substrings against an uppercased agent name.
# Matched against the NORMALIZED agent name, so "C T CORPORATION SYSTEM" and
# "CT Corporation System" are the same entry. Spelling out the spaces is how
# the first run let 106 CT Corporation rows through as third parties.
COMMERCIAL_AGENTS = (
    "REGISTERED AGENT", "CORPORATION SERVICE COMPANY", "CT CORPORATION",
    "C T CORPORATION", "COGENCY GLOBAL", "NATIONAL REGISTERED AGENTS",
    "INCORP", "LEGALINC", "NORTHWEST REGISTERED", "ZENBUSINESS",
    "HARBOR BUSINESS", "VCORP", "UNITED STATES CORPORATION AGENTS",
    "BUSINESS FILINGS", "LEGALZOOM", "PARACORP", "SPIEGEL UTRERA",
    "USA CORPORATE SERVICES", "INTERSTATE AGENT", "CAPITOL SERVICES",
    # Found by ranking third_party agents after the first run and reading the
    # top of the list. Anything appearing for dozens of unrelated single-
    # building LLCs is a service, not a controlling party.
    "USACORP", "CORPORATE CREATIONS", "METRO CORPORATE SERVICES",
    "CORPORATE FILING", "AGENTS AND CORPORATIONS", "RESIDENT AGENT",
)

# Placeholders DOS accepts in the process-name field that designate the filer
# itself in words. The first run classed 447 of these as a named third party,
# which would have put "The Limited Liability Company" on a page as the answer
# to who controls a building.
PLACEHOLDER_AGENTS = frozenset({
    "THE LIMITED LIABILITY COMPANY", "THE LLC", "THE COMPANY",
    "THE CORPORATION", "THE ENTITY", "SELF", "SAME", "NONE", "N A",
    "THE PARTNERSHIP", "THE LIMITED PARTNERSHIP",
})


def normalize(name: str) -> str:
    """The join key. ownership_raw.party_name_normalized is uppercase with
    punctuation collapsed, so DOS names are reduced the same way rather than
    the two sources being trusted to agree about commas and periods."""
    return re.sub(r"[^A-Z0-9]+", " ", (name or "").upper()).strip()


def agent_kind(entity_name: str, agent_name: str | None) -> str:
    """'self', 'commercial', 'third_party', or 'none'."""
    if not agent_name:
        return "none"
    a = normalize(agent_name)
    if not a:
        return "none"
    # "C/O SOMETHING LLC" designating the company itself reads as a third party
    # until the C/O prefix is stripped, so strip it before every comparison.
    stripped = re.sub(r"^C ?O ", "", a).strip()
    if a == normalize(entity_name) or stripped == normalize(entity_name):
        return "self"
    if a in PLACEHOLDER_AGENTS or stripped in PLACEHOLDER_AGENTS:
        return "self"
    if any(term in a or term in stripped for term in COMMERCIAL_AGENTS):
        return "commercial"
    return "third_party"


def _candidate_names(db, refresh_all: bool) -> list[str]:
    """LLC-form deed buyers, optionally only the ones never looked up."""
    seen = "" if refresh_all else """
        AND NOT EXISTS (
            SELECT 1 FROM dos_entities d
            WHERE d.entity_name_normalized = regexp_replace(
                  upper(o.party_name_normalized), '[^A-Z0-9]+', ' ', 'g')
        )"""
    rows = db.execute(text(f"""
        SELECT DISTINCT o.party_name_normalized AS name
        FROM ownership_raw o
        WHERE o.doc_type = 'DEED' AND o.party_type = '2'
          AND o.party_name_normalized LIKE '%LLC%'
          AND length(o.party_name_normalized) < 48
          {seen}
    """)).fetchall()
    return [r.name for r in rows if r.name]


def _fetch(names: list[str]) -> list[dict]:
    quoted = ",".join("'" + n.replace("'", "''") + "'" for n in names)
    where = urllib.parse.quote(f"upper(current_entity_name) in ({quoted})")
    url = (f"{_DATASET}?$select=dos_id,current_entity_name,entity_type,jurisdiction,"
           f"county,initial_dos_filing_date,dos_process_name,dos_process_address_1,"
           f"dos_process_city,dos_process_state,dos_process_zip"
           f"&$where={where}&$limit=5000")
    with urllib.request.urlopen(url, timeout=_TIMEOUT) as resp:
        return json.load(resp)


def _as_date(v: str | None) -> date | None:
    if not v:
        return None
    try:
        return date.fromisoformat(v[:10])
    except ValueError:
        return None


_UPSERT = text("""
    INSERT INTO dos_entities (
        dos_id, entity_name, entity_name_normalized, entity_type, jurisdiction,
        county, initial_filing_date, agent_name, agent_address, agent_city,
        agent_state, agent_zip, created_at, updated_at)
    VALUES (:dos_id, :entity_name, :norm, :entity_type, :jurisdiction, :county,
            :filed, :agent_name, :agent_address, :agent_city, :agent_state,
            :agent_zip, now(), now())
    ON CONFLICT (dos_id) DO UPDATE SET
        entity_name = EXCLUDED.entity_name,
        entity_name_normalized = EXCLUDED.entity_name_normalized,
        entity_type = EXCLUDED.entity_type,
        jurisdiction = EXCLUDED.jurisdiction,
        county = EXCLUDED.county,
        initial_filing_date = EXCLUDED.initial_filing_date,
        agent_name = EXCLUDED.agent_name,
        agent_address = EXCLUDED.agent_address,
        agent_city = EXCLUDED.agent_city,
        agent_state = EXCLUDED.agent_state,
        agent_zip = EXCLUDED.agent_zip,
        updated_at = now()
""")


def run(db, refresh_all: bool = False, commit: bool = True) -> dict:
    names = _candidate_names(db, refresh_all)
    if not names:
        logger.info("dos refresh: no names to look up")
        return {"looked_up": 0, "matched": 0, "batches": 0}

    matched = batches = 0
    for i in range(0, len(names), _BATCH):
        chunk = names[i:i + _BATCH]
        batches += 1
        try:
            rows = _fetch(chunk)
        except Exception as exc:  # noqa: BLE001 -- one bad batch must not lose the rest
            logger.warning("dos batch %d failed (%d names): %r", batches, len(chunk), exc)
            continue
        for r in rows:
            nm = r.get("current_entity_name") or ""
            db.execute(_UPSERT, {
                "dos_id": r.get("dos_id"),
                "entity_name": nm[:255],
                "norm": normalize(nm)[:255],
                "entity_type": (r.get("entity_type") or "")[:80] or None,
                "jurisdiction": (r.get("jurisdiction") or "")[:60] or None,
                "county": (r.get("county") or "")[:40] or None,
                "filed": _as_date(r.get("initial_dos_filing_date")),
                "agent_name": (r.get("dos_process_name") or "")[:255] or None,
                "agent_address": (r.get("dos_process_address_1") or "")[:255] or None,
                "agent_city": (r.get("dos_process_city") or "")[:80] or None,
                "agent_state": (r.get("dos_process_state") or "")[:2] or None,
                "agent_zip": (r.get("dos_process_zip") or "")[:10] or None,
            })
            matched += 1
    if commit:
        db.commit()
    logger.info("dos refresh: %d names in %d batches, %d rows written",
                len(names), batches, matched)
    return {"looked_up": len(names), "matched": matched, "batches": batches}


def main() -> int:
    from config.logging_config import configure_logging
    configure_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true",
                    help="re-check every LLC deed buyer, not only new names")
    args = ap.parse_args()
    try:
        with get_scraper_db() as db:
            result = run(db, refresh_all=args.all, commit=False)
    except Exception:
        logger.exception("dos refresh failed")
        return 1
    print(f"looked up {result['looked_up']} names, wrote {result['matched']} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
