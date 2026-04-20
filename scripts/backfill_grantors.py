"""
Backfill grantor (party_type='1') rows for all document_ids already in ownership_raw.

The scraper historically only fetched grantees (party_type='2'). The LLC-to-LLC
transfer filter in the scoring engine requires grantor rows to be present so it
can exclude transfers where the seller was also an LLC.

Run once after deploying the updated scraper:
    python -m scripts.backfill_grantors

Safe to re-run: inserts use ON CONFLICT DO NOTHING on uq_ownership_raw_document_party.
"""

import logging
import os
import sys
import time

import requests
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models.database import get_scraper_db
from models.ownership import OwnershipRaw
from scrapers.ownership import GRANTOR_PARTY_TYPE, normalize_party_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PARTIES_URL = "https://data.cityofnewyork.us/resource/636b-3b5g.json"
# 40 IDs per batch keeps the GET URL under ~1200 chars (Socrata chokes at ~2000)
BATCH_SIZE = 40
RATE_LIMIT_SLEEP = 0.25  # seconds between batches


def _fetch_grantors(doc_ids: list[str], session: requests.Session) -> list[dict]:
    id_list = ", ".join(f"'{d}'" for d in doc_ids)
    params = {
        "$where": f"document_id IN ({id_list}) AND party_type = '1'",
        "$select": "document_id, name",
        "$limit": "5000",
    }
    resp = session.get(PARTIES_URL, params=params, timeout=60)
    if not resp.ok:
        raise requests.HTTPError(f"{resp.status_code}: {resp.text[:300]}", response=resp)
    return resp.json()


def main() -> None:
    app_token = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")

    session = requests.Session()
    if app_token:
        session.headers.update({"X-App-Token": app_token})

    with get_scraper_db() as db:
        rows = db.execute(text("""
            SELECT DISTINCT document_id
            FROM ownership_raw
            WHERE party_type = '2'
              AND document_id NOT IN (
                SELECT document_id FROM ownership_raw WHERE party_type = '1'
              )
            ORDER BY document_id
        """)).fetchall()

    doc_ids = [r[0] for r in rows]
    total = len(doc_ids)
    logger.info("Documents needing grantor backfill: %d", total)

    inserted = 0
    no_grantor = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch = doc_ids[batch_start: batch_start + BATCH_SIZE]
        pct = int((batch_start / total) * 100) if total else 100

        try:
            grantor_rows_raw = _fetch_grantors(batch, session)
        except Exception as e:
            logger.warning("Batch %d-%d fetch failed: %s", batch_start, batch_start + len(batch), e)
            time.sleep(2)
            continue

        # One row per document — LLC-named grantor preferred
        best: dict[str, dict] = {}
        for r in grantor_rows_raw:
            did = (r.get("document_id") or "").strip()
            name = (r.get("name") or "").strip()
            if not did or not name:
                continue
            norm = normalize_party_name(name)
            existing = best.get(did)
            if existing is None or (
                norm and "LLC" in norm
                and "LLC" not in (existing.get("party_name_normalized") or "")
            ):
                best[did] = {
                    "document_id": did,
                    "party_type": GRANTOR_PARTY_TYPE,
                    "party_name": name,
                    "party_name_normalized": norm,
                    "party_addr_1": None,
                    "party_addr_2": None,
                    "party_city": None,
                    "party_state": None,
                    "party_zip": None,
                }

        if not best:
            no_grantor += len(batch)
            time.sleep(RATE_LIMIT_SLEEP)
            continue

        doc_id_list = list(best.keys())
        with get_scraper_db() as db:
            existing_rows = db.execute(text("""
                SELECT document_id, bbl, doc_type, doc_date, doc_amount
                FROM ownership_raw
                WHERE document_id = ANY(:ids) AND party_type = '2'
            """), {"ids": doc_id_list}).fetchall()

            to_insert = []
            for row in existing_rows:
                grantor = best.get(row.document_id)
                if not grantor:
                    continue
                to_insert.append({
                    "bbl": row.bbl,
                    "document_id": row.document_id,
                    "doc_type": row.doc_type,
                    "doc_date": row.doc_date,
                    "doc_amount": row.doc_amount,
                    "raw_data": {},
                    **grantor,
                })

            if to_insert:
                result = db.execute(
                    insert(OwnershipRaw)
                    .values(to_insert)
                    .on_conflict_do_nothing(constraint="uq_ownership_raw_document_party")
                )
                db.commit()
                inserted += result.rowcount

        if batch_start % 2000 == 0:
            logger.info("[%d%%] inserted so far: %d", pct, inserted)

        time.sleep(RATE_LIMIT_SLEEP)

    logger.info("Done. Inserted: %d  |  Docs with no ACRIS grantor: %d", inserted, no_grantor)


if __name__ == "__main__":
    main()
