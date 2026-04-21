"""
Backfill party_addr_1/addr_2/city/state/zip for existing ownership_raw rows.

Queries all rows where party_addr_1 IS NULL, batches document_ids 400 at a
time to Socrata 636b-3b5g, and updates matched rows in place.

Run once after applying migration b2c3d4e5f6a7. Safe to re-run; rows that
already have party_addr_1 populated are skipped.

Usage:
    python scripts/backfill_party_addresses.py
"""

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
from sqlalchemy import text

from config.nyc import SOCRATA_BASE_URL
from models.database import get_scraper_db

logger = logging.getLogger(__name__)

PARTIES_DATASET_ID = "636b-3b5g"
PARTIES_URL = f"{SOCRATA_BASE_URL}/{PARTIES_DATASET_ID}.json"
GRANTEE_PARTY_TYPE = "2"
BATCH_SIZE = 20


def _fetch_addresses(session: requests.Session, doc_ids: list[str]) -> dict[str, dict]:
    id_list_sql = ", ".join(f"'{d}'" for d in doc_ids)
    params = {
        "$where": (
            f"document_id IN ({id_list_sql}) "
            f"AND party_type = '{GRANTEE_PARTY_TYPE}'"
        ),
        "$select": "document_id, addr_1, addr_2, city, state, zip",
        "$limit": 10_000,
    }
    app_token = os.environ.get("NYC_OPEN_DATA_APP_TOKEN")
    headers = {"X-App-Token": app_token} if app_token else {}
    resp = session.get(PARTIES_URL, params=params, headers=headers, timeout=60)
    resp.raise_for_status()

    result: dict[str, dict] = {}
    for row in resp.json():
        did = (row.get("document_id") or "").strip()
        if not did:
            continue
        result[did] = {
            "party_addr_1": (row.get("addr_1") or "").strip() or None,
            "party_addr_2": (row.get("addr_2") or "").strip() or None,
            "party_city": (row.get("city") or "").strip() or None,
            "party_state": (row.get("state") or "").strip() or None,
            "party_zip": (row.get("zip") or "").strip() or None,
        }
    return result


def run() -> None:
    with get_scraper_db() as db:
        rows = db.execute(
            text(
                "SELECT id, document_id FROM ownership_raw "
                "WHERE party_addr_1 IS NULL AND document_id IS NOT NULL "
                "ORDER BY id"
            )
        ).fetchall()

    if not rows:
        logger.info("Nothing to backfill — all rows already have party_addr_1.")
        return

    logger.info("Rows to backfill: %d", len(rows))

    http = requests.Session()
    total_updated = 0
    total_not_found = 0

    # Iterate in BATCH_SIZE chunks
    for offset in range(0, len(rows), BATCH_SIZE):
        batch = rows[offset : offset + BATCH_SIZE]
        id_map: dict[str, int] = {row.document_id: row.id for row in batch}
        doc_ids = list(id_map.keys())

        try:
            addresses = _fetch_addresses(http, doc_ids)
        except Exception as exc:
            logger.warning("Socrata fetch failed for batch at offset %d: %s", offset, exc)
            continue

        with get_scraper_db() as db:
            for doc_id, addrs in addresses.items():
                row_id = id_map.get(doc_id)
                if row_id is None:
                    continue
                db.execute(
                    text(
                        "UPDATE ownership_raw SET "
                        "party_addr_1 = :addr_1, "
                        "party_addr_2 = :addr_2, "
                        "party_city   = :city, "
                        "party_state  = :state, "
                        "party_zip    = :zip "
                        "WHERE id = :id"
                    ),
                    {
                        "addr_1": addrs["party_addr_1"],
                        "addr_2": addrs["party_addr_2"],
                        "city":   addrs["party_city"],
                        "state":  addrs["party_state"],
                        "zip":    addrs["party_zip"],
                        "id":     row_id,
                    },
                )
                total_updated += 1
            db.commit()

        not_found = len(doc_ids) - len(addresses)
        total_not_found += not_found
        logger.info(
            "Batch %d–%d: updated=%d not_found_in_socrata=%d",
            offset + 1,
            offset + len(batch),
            len(addresses),
            not_found,
        )

    logger.info(
        "Backfill complete. total_updated=%d total_not_found=%d",
        total_updated,
        total_not_found,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    run()
