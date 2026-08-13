"""
HPD building-registry scraper (formerly the DHCR rent-stabilization source).

Dataset: kj4p-ruqc — "Buildings Subject to HPD Jurisdiction" (NYC Open Data),
published by HPD, not DHCR.

IMPORTANT: what this ingests is NOT rent-stabilization data. The field read
here, legalclassa, is defined by HPD as "the number of apartments in a
multiple dwelling" — total apartments in every building under HPD
jurisdiction (~333k buildings), not registered rent-stabilized units (~32k
buildings in the real DHCR years).

History: the annual DHCR snapshot dataset yn95-5t2d was withdrawn from NYC
Open Data on 2026-04-12 and kj4p-ruqc was substituted on the assumption that
legalclassa equalled the RS unit count for DHCR-registered buildings. It does
not, and the 10x jump in building count is the tell. Rows written here are
therefore stamped source='hpd_jurisdiction' and are excluded from every
rent-stabilization surface and from the rs_unit_loss signal, which has had no
live source since the withdrawal.

Rows still land in rs_buildings because the apartment count is a useful
measure of building size; the source column keeps the two datasets apart.

Field name constants:
  boroid      — 1-digit borough code (1–5)
  block       — block number (zero-padded to 5 digits for BBL construction)
  lot         — lot number (zero-padded to 4 digits for BBL construction)
  legalclassa — Class A (residential apartment) unit count = RS unit count

Filter: recordstatus='Active' AND lifecycle='Building' — excludes inactive and
unit-level records. ~348k buildings total.

Full-refresh pattern: all Active Building records are fetched and upserted each run.
Returns None as watermark (snapshot data has no incremental date field).
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import text

from models.bbl import normalize_bbl
from models.database import get_scraper_db
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Field name constants
FIELD_BOROID = "boroid"
FIELD_BLOCK = "block"
FIELD_LOT = "lot"
FIELD_UNITS = "legalclassa"

# Where clause: active building-level registrations only
_WHERE_ACTIVE = 'recordstatus="Active" AND lifecycle="Building"'


class DhcrRsScraper(BaseScraper):
    SCRAPER_NAME = "dhcr_rs"
    DATASET_ID = "kj4p-ruqc"
    INITIAL_LOOKBACK_DAYS = 365 * 3  # not used (full-refresh), kept for base class

    def _run(self, db) -> tuple[int, int, datetime | None]:
        """Fetch all Active Building records and upsert; returns None as watermark (no incremental date field)."""
        records_processed = 0
        records_failed = 0
        snapshot_year = datetime.now(timezone.utc).year

        for raw in self.paginate(_WHERE_ACTIVE, order=":id"):
            parsed = self._parse(db, raw, snapshot_year)
            if parsed is None:
                records_failed += 1
                continue

            self._upsert(db, parsed, raw)
            records_processed += 1

        return records_processed, records_failed, None

    def _parse(self, db, raw: dict, snapshot_year: int) -> dict | None:
        """Parse a raw DHCR Building Registration record; BBL from boroid+block+lot, rs_unit_count from legalclassa."""
        # Construct BBL from boroid + block + lot
        boroid_raw = raw.get(FIELD_BOROID)
        block_raw = raw.get(FIELD_BLOCK)
        lot_raw = raw.get(FIELD_LOT)

        if not boroid_raw or not block_raw or not lot_raw:
            self.quarantine(db, raw, "missing_required_field:bbl_components")
            return None

        try:
            boroid = str(int(float(str(boroid_raw).strip())))
            block = str(int(float(str(block_raw).strip()))).zfill(5)
            lot = str(int(float(str(lot_raw).strip()))).zfill(4)
        except (ValueError, TypeError):
            self.quarantine(db, raw, "invalid_bbl_component")
            return None

        bbl = normalize_bbl(f"{boroid}{block}{lot}")
        if bbl is None:
            self.quarantine(db, raw, "missing_required_field:bbl")
            return None

        # RS unit count — legalclassa; treat 0 as None (unrecorded, not zero units)
        rs_unit_count = _parse_int(raw.get(FIELD_UNITS))
        if rs_unit_count == 0:
            rs_unit_count = None

        return {
            "bbl": bbl,
            "year": snapshot_year,
            "rs_unit_count": rs_unit_count,
            "source": "hpd_jurisdiction",
        }

    def _upsert(self, db, parsed: dict, raw: dict) -> None:
        """Upsert a DHCR RS row; (bbl, year) is the natural key, rs_unit_count updated on conflict."""
        now = datetime.now(timezone.utc)
        db.execute(
            text("""
                INSERT INTO rs_buildings (
                    bbl, year, rs_unit_count, source, raw_data, created_at, updated_at
                )
                VALUES (
                    :bbl, :year, :rs_unit_count, :source, CAST(:raw_data AS jsonb), :now, :now
                )
                ON CONFLICT ON CONSTRAINT uq_rs_buildings_bbl_year
                DO UPDATE SET
                    rs_unit_count = EXCLUDED.rs_unit_count,
                    source = EXCLUDED.source,
                    raw_data      = EXCLUDED.raw_data,
                    updated_at    = EXCLUDED.updated_at
            """),
            {
                **parsed,
                "raw_data": __import__("json").dumps(raw),
                "now": now,
            },
        )
        db.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_int(value) -> int | None:
    """Parse a string or numeric value to int, or return None."""
    if value is None:
        return None
    try:
        # Handle float-formatted strings like "45.0"
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    try:
        with get_scraper_db() as db:
            scraper = DhcrRsScraper()
            run = scraper.run(db)
            print(f"Status: {run.status} | Processed: {run.records_processed}")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
