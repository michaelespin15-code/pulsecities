"""
Backfill party_addr_1/city/state/zip on existing ownership_raw rows from ACRIS.

**This script did not work, and its failure produced a wrong conclusion that
stood for months.** It asked Socrata 636b-3b5g for `addr_1` and `addr_2`. The
dataset has neither: its columns are `address_1, city, state, zip, country`.
Every fetch therefore returned rows with the address fields absent, every
update wrote NULL, and the project recorded that "ACRIS 636b-3b5g has no party
address data" and parked entity resolution on that basis. The scraper had been
reading `address_1` correctly the whole time, which is why rows ingested from
May 2026 onward do carry addresses while the April backfill left 141,354 empty.

The second bug was quieter and worse. It fetched only grantee (party_type 2)
rows from Socrata, then matched them to database rows by `document_id` alone.
A deed has a grantor row and a grantee row sharing one document_id, and the
id_map dict kept whichever came last, so the buyer's mailing address could be
written onto the seller's row. Matching is now on (document_id, party_type,
name), which is what actually identifies a party.

Why it is worth running: 71% of LLC buyer entities have no filing address, and
those addresses are what lets api/entity_families.py tell one operation holding
forty buildings from forty unrelated owners.

    python -m scripts.backfill_party_addresses --limit 500   # try it first
    python -m scripts.backfill_party_addresses               # the real run
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
from sqlalchemy import text

from config.nyc import SOCRATA_BASE_URL
from models.database import get_scraper_db

logger = logging.getLogger(__name__)

PARTIES_DATASET_ID = "636b-3b5g"
PARTIES_URL = f"{SOCRATA_BASE_URL}/{PARTIES_DATASET_ID}.json"

# Documents per Socrata request. Each returns every party on the document, so
# the row count comes back larger than the batch.
BATCH_SIZE = 150
PAGE_LIMIT = 50_000
SLEEP = 0.15


def _fetch(session: requests.Session, doc_ids: list[str]) -> dict[tuple, dict]:
    """{(document_id, party_type, name): address fields} for a batch."""
    id_list = ", ".join("'" + d.replace("'", "''") + "'" for d in doc_ids)
    params = {
        "$where": f"document_id IN ({id_list})",
        # address_1, not addr_1. This one character is the whole bug.
        "$select": "document_id, party_type, name, address_1, city, state, zip",
        "$limit": PAGE_LIMIT,
    }
    token = os.environ.get("NYC_OPEN_DATA_APP_TOKEN")
    headers = {"X-App-Token": token} if token else {}
    resp = session.get(PARTIES_URL, params=params, headers=headers, timeout=90)
    resp.raise_for_status()

    out: dict[tuple, dict] = {}
    for row in resp.json():
        did = (row.get("document_id") or "").strip()
        ptype = (row.get("party_type") or "").strip()
        name = (row.get("name") or "").strip().upper()
        if not did or not ptype:
            continue
        addr = (row.get("address_1") or "").strip() or None
        if not addr:
            continue
        out[(did, ptype, name)] = {
            "addr_1": addr,
            "city": (row.get("city") or "").strip() or None,
            "state": (row.get("state") or "").strip() or None,
            "zip": (row.get("zip") or "").strip() or None,
        }
    return out


def run(limit: int | None = None, dry_run: bool = False) -> None:
    with get_scraper_db() as db:
        sql = ("SELECT id, document_id, party_type, "
               "       upper(coalesce(party_name, party_name_normalized)) AS name "
               "FROM ownership_raw "
               "WHERE party_addr_1 IS NULL AND document_id IS NOT NULL "
               "ORDER BY id")
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = db.execute(text(sql)).fetchall()

    if not rows:
        logger.info("Nothing to backfill.")
        return
    logger.info("Rows to backfill: %d%s", len(rows), " (dry run)" if dry_run else "")

    http = requests.Session()
    updated = unmatched = no_address = 0
    started = time.monotonic()

    for offset in range(0, len(rows), BATCH_SIZE):
        batch = rows[offset:offset + BATCH_SIZE]
        doc_ids = sorted({r.document_id for r in batch})
        try:
            found = _fetch(http, doc_ids)
        except Exception as exc:
            logger.warning("fetch failed at offset %d: %s", offset, exc)
            continue

        updates = []
        for r in batch:
            hit = found.get((r.document_id, r.party_type, (r.name or "").strip()))
            if hit is None:
                # ACRIS truncates party_name_normalized at 48 characters, so
                # fall back to the one party of that type on the document.
                same = [v for (d, p, _n), v in found.items()
                        if d == r.document_id and p == r.party_type]
                hit = same[0] if len(same) == 1 else None
            if hit is None:
                if any(k[0] == r.document_id for k in found):
                    no_address += 1      # document is in ACRIS, party has no address
                else:
                    unmatched += 1       # document not returned at all
                continue
            updates.append({**hit, "id": r.id})

        if updates and not dry_run:
            with get_scraper_db() as db:
                db.execute(text(
                    "UPDATE ownership_raw SET party_addr_1 = :addr_1, "
                    "party_city = :city, party_state = :state, party_zip = :zip "
                    "WHERE id = :id"
                ), updates)
                db.commit()
        updated += len(updates)

        if offset % (BATCH_SIZE * 20) == 0:
            done = offset + len(batch)
            rate = done / max(time.monotonic() - started, 1e-9)
            logger.info("%d/%d rows  updated=%d  no_address=%d  unmatched=%d  "
                        "(%.0f rows/s, ~%.0f min left)",
                        done, len(rows), updated, no_address, unmatched, rate,
                        (len(rows) - done) / max(rate, 1e-9) / 60)
        time.sleep(SLEEP)

    logger.info("Done. updated=%d no_address_in_acris=%d unmatched=%d",
                updated, no_address, unmatched)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    run(limit=a.limit, dry_run=a.dry_run)
