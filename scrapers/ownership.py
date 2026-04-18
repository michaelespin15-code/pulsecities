"""
NYC ACRIS Property Ownership scraper.
Sources (three datasets, joined client-side on document_id):
  Master:  bnx9-e6tj  (document-level records: doc type, date, amount)
  Parties: 636b-3b5g  (buyer/seller names per document)
  Legals:  8h5j-fqxa  (BBL: borough/block/lot per document)

Watermark field: recorded_datetime (from master dataset) — tracks when the deed
  appeared in ACRIS, not when it was dated.  Late filings arrive with old
  document_date values; filtering on recorded_datetime ensures they are never skipped.

JOIN STRATEGY:
1. Paginate master → filter by deed doc types → collect (document_id → master_fields)
2. For each batch of 400 document_ids, query parties (party_type='2' = buyer)
3. For each batch of 400 document_ids, query legals (non-null borough/block/lot)
4. Join master + parties + legals in memory → write to ownership_raw

CRITICAL: Filter by broader deed types, not just 'DEED'.
LLC acquisitions often appear as ASST (assignment) or DEEDP.
See ACRIS_TRANSFER_DOC_TYPES in config/nyc.py.

BBL CONSTRUCTION from legals:
  borough (1 digit) + block.zfill(5) + lot.zfill(4)
  This is the canonical 10-digit format used throughout the platform.

LLC NORMALIZATION:
  party_name_normalized strips punctuation and unifies LLC variant spellings.
  Enables portfolio aggregation across 'LLC', 'L.L.C.', 'LIMITED LIABILITY CO.'
"""

import logging
import re
from datetime import date, datetime, timezone

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.dialects.postgresql import insert

from config.nyc import ACRIS_TRANSFER_DOC_TYPES, SOCRATA_BASE_URL
from models.bbl import normalize_bbl
from models.database import get_scraper_db
from models.ownership import OwnershipRaw
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

MASTER_DATASET_ID = "bnx9-e6tj"
PARTIES_DATASET_ID = "636b-3b5g"
LEGALS_DATASET_ID = "8h5j-fqxa"

# Maximum document_ids per IN clause; keep under 400 to stay within URL length limits
BATCH_SIZE = 400

# party_type = '2' is the Grantee (buyer/transferee) in ACRIS
GRANTEE_PARTY_TYPE = "2"


class AcrisMasterInput(BaseModel):
    """Pydantic model for ACRIS master records from Socrata bnx9-e6tj."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    document_id: str = ""
    document_date: str | None = None
    recorded_datetime: str | None = None
    doc_type: str | None = None
    document_amt: str | None = None


class AcrisPartyInput(BaseModel):
    """Pydantic model for ACRIS party records from Socrata 636b-3b5g."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    document_id: str = ""
    party_type: str | None = None
    name: str | None = None
    addr_1: str | None = None
    addr_2: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None


class AcrisLegalInput(BaseModel):
    """Pydantic model for ACRIS legal records from Socrata 8h5j-fqxa."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    document_id: str = ""
    borough: str | None = None
    block: str | None = None
    lot: str | None = None


class OwnershipScraper(BaseScraper):
    SCRAPER_NAME = "acris_ownership"
    DATASET_ID = MASTER_DATASET_ID

    def __init__(self) -> None:
        super().__init__()
        # Override base_url: set per-dataset in helpers
        self._parties_url = f"{SOCRATA_BASE_URL}/{PARTIES_DATASET_ID}.json"
        self._legals_url = f"{SOCRATA_BASE_URL}/{LEGALS_DATASET_ID}.json"

    def _run(self, db) -> tuple[int, int, datetime | None]:
        where = self._build_master_where(db)
        logger.info("ACRIS master query: %s", where)

        records_processed = 0
        records_failed = 0
        new_watermark: datetime | None = None

        # Accumulate master records page by page, batch-join, persist
        master_batch: dict[str, dict] = {}  # document_id → master fields

        for raw in self.paginate(where, order="recorded_datetime ASC"):
            try:
                master_rec = AcrisMasterInput.model_validate(raw)
            except ValidationError as exc:
                records_failed += 1
                self.quarantine(db, raw, f"schema_validation_error:{exc}")
                continue

            doc_id = master_rec.document_id.strip()
            if not doc_id:
                records_failed += 1
                self.quarantine(db, raw, "missing_required_field:document_id")
                continue

            doc_date = _parse_date(master_rec.document_date)
            # Watermark on recorded_datetime, not document_date — deeds frequently
            # appear in ACRIS weeks after the document date (late filings), so
            # filtering on document_date would silently skip backdated records.
            recorded_dt = _parse_date(master_rec.recorded_datetime)
            if recorded_dt and (
                new_watermark is None
                or _date_to_dt(recorded_dt) > new_watermark
            ):
                new_watermark = _date_to_dt(recorded_dt)

            master_batch[doc_id] = {
                "doc_type": (master_rec.doc_type or "").strip(),
                "doc_date": doc_date,
                "doc_amount": _parse_decimal(master_rec.document_amt),
            }

            if len(master_batch) >= BATCH_SIZE:
                p, f = self._join_and_persist(db, master_batch)
                records_processed += p
                records_failed += f
                master_batch = {}

        if master_batch:
            p, f = self._join_and_persist(db, master_batch)
            records_processed += p
            records_failed += f

        return records_processed, records_failed, new_watermark

    def _build_master_where(self, db) -> str:
        watermark = self.get_watermark(db)
        from datetime import timedelta

        if watermark:
            since = watermark - timedelta(minutes=10)
        else:
            from datetime import timedelta
            since = datetime.now(timezone.utc) - timedelta(days=self.INITIAL_LOOKBACK_DAYS)

        since_str = since.strftime("%Y-%m-%dT%H:%M:%S.000")
        # Build doc_type IN clause
        doc_types_sql = ", ".join(f"'{t}'" for t in ACRIS_TRANSFER_DOC_TYPES)
        return (
            f"recorded_datetime > '{since_str}' "
            f"AND doc_type IN ({doc_types_sql})"
        )

    def _join_and_persist(
        self, db, master_batch: dict[str, dict]
    ) -> tuple[int, int]:
        """
        For a batch of document_ids:
        1. Fetch parties (party_type=2) → buyer names
        2. Fetch legals → BBLs
        3. Join all three → write to ownership_raw
        """
        doc_ids = list(master_batch.keys())
        id_list_sql = ", ".join(f"'{d}'" for d in doc_ids)

        # --- Fetch parties (buyers) ---
        parties: dict[str, str] = {}  # document_id → normalized party name
        try:
            page = self._fetch_parties(id_list_sql)
            for r in page:
                try:
                    party_rec = AcrisPartyInput.model_validate(r)
                except ValidationError:
                    continue
                did = party_rec.document_id.strip()
                name = (party_rec.name or "").strip()
                if did and name:
                    parties[did] = name
        except Exception as e:
            logger.warning("ACRIS parties fetch failed for batch: %s", e)
            # Count entire batch as failed; can't score LLC acquisitions without party names
            return 0, len(master_batch)

        # --- Fetch legals (BBLs) ---
        legals: dict[str, str] = {}  # document_id → canonical BBL
        try:
            page = self._fetch_legals(id_list_sql)
            for r in page:
                try:
                    legal_rec = AcrisLegalInput.model_validate(r)
                except ValidationError:
                    continue
                did = legal_rec.document_id.strip()
                bbl = _bbl_from_legals(r)
                if did and bbl:
                    legals[did] = bbl
        except Exception as e:
            logger.warning("ACRIS legals fetch failed for batch: %s", e)

        # --- Join and write ---
        rows = []
        failed = 0
        for doc_id, master in master_batch.items():
            raw_party_name = parties.get(doc_id)
            bbl = legals.get(doc_id)

            if not bbl:
                # Missing BBL means we can't join this record to a property
                failed += 1
                continue

            rows.append(
                {
                    "bbl": bbl,
                    "document_id": doc_id,
                    "doc_type": master["doc_type"],
                    "party_type": GRANTEE_PARTY_TYPE,
                    "party_name": raw_party_name,
                    "party_name_normalized": normalize_party_name(raw_party_name),
                    "doc_date": master["doc_date"],
                    "doc_amount": master["doc_amount"],
                    "raw_data": {
                        "doc_type": master["doc_type"],
                        "doc_date": master["doc_date"].isoformat() if master["doc_date"] is not None else None,
                        "doc_amount": str(master["doc_amount"]) if master["doc_amount"] is not None else None,
                    },
                }
            )

        processed = 0
        if rows:
            stmt = (
                insert(OwnershipRaw)
                .values(rows)
                .on_conflict_do_nothing(
                    constraint="uq_ownership_raw_document_party"
                )
            )
            result = db.execute(stmt)
            db.commit()
            processed = result.rowcount

        return processed, failed

    def _fetch_parties(self, id_list_sql: str) -> list[dict]:
        """Fetch buyer (party_type=2) rows for the given document_ids."""
        params = {
            "$where": (
                f"document_id IN ({id_list_sql}) "
                f"AND party_type = '{GRANTEE_PARTY_TYPE}'"
            ),
            "$select": "document_id, party_type, name",
            "$limit": 10_000,
        }
        resp = self._http.get(self._parties_url, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _fetch_legals(self, id_list_sql: str) -> list[dict]:
        """Fetch property (borough/block/lot) rows for the given document_ids."""
        params = {
            "$where": (
                f"document_id IN ({id_list_sql}) "
                f"AND borough IS NOT NULL "
                f"AND block IS NOT NULL "
                f"AND lot IS NOT NULL"
            ),
            "$select": "document_id, borough, block, lot",
            "$limit": 10_000,
        }
        resp = self._http.get(self._legals_url, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()


# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #

def _bbl_from_legals(row: dict) -> str | None:
    """Construct canonical 10-digit BBL from legals borough/block/lot."""
    try:
        borough = str(int(float(row["borough"]))).strip()
        block = str(int(float(row["block"]))).zfill(5)
        lot = str(int(float(row["lot"]))).zfill(4)
        bbl = f"{borough}{block}{lot}"
        return normalize_bbl(bbl)
    except (TypeError, ValueError, KeyError):
        return None


def normalize_party_name(name: str | None) -> str | None:
    """Normalize an ACRIS party name for LLC portfolio aggregation."""
    if not name:
        return None
    n = name.upper().strip()
    # Normalize LLC variants: must handle L L C (spaces between letters) BEFORE
    # collapsing whitespace, since that variant has interior spaces.
    # Order matters: check "L L C" (spaced) before the tighter dot/no-dot pattern.
    n = re.sub(
        r"\bL\s+L\s+C\b"
        r"|"
        r"\bL\.?L\.?C\.?\b"
        r"|"
        r"LIMITED\s+LIABILITY\s+CO(?:MPANY)?\.?",
        "LLC",
        n,
    )
    # Remove trailing/leading punctuation
    n = re.sub(r"[.,;:'\"]", "", n)
    # Collapse whitespace
    n = re.sub(r"\s+", " ", n).strip()
    return n or None


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_decimal(value: str | None):
    if not value:
        return None
    try:
        from decimal import Decimal
        return Decimal(str(value).strip())
    except Exception:
        return None


def _date_to_dt(d: date | None) -> datetime | None:
    if d is None:
        return None
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with get_scraper_db() as db:
        scraper = OwnershipScraper()
        run = scraper.run(db)
        print(f"Status: {run.status} | Processed: {run.records_processed}")
