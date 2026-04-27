"""
NYC DOB (Department of Buildings) Permit Issuance scraper.
Dataset: ipu4-2q9a (NYC Open Data)
Update frequency: daily
Watermark field: filing_date

Permit types and what they signal for displacement scoring:
  NB  = New Building       → strongest signal (demolition + rebuild)
  DM  = Demolition         → strongest signal
  A1  = Major Alteration   → strong signal (gut renovation)
  A2  = Minor Alteration   → moderate signal (units being combined)
  A3  = Minor Alteration   → low signal
  SG  = Sign               → ignore for displacement
  EW  = Equipment Work     → ignore for displacement
  FP  = Fire Protection    → ignore for displacement

Field mapping (Socrata → model):
  NOTE: Dataset ipu4-2q9a has NO bbl field. BBL is constructed from
        separate borough (name text), block (text), and lot (text) columns
        using BOROUGH_NAME_TO_CODE mapping + normalize_bbl().
  bin__            → bin           (7-digit building ID)
  house__          → address       (combined with street_name below)
  street_name      → address
  zip_code         → zip_code
  borough          → borough name (MANHATTAN, BRONX, etc.) — NOT a borough code
  block            → block number (text, zero-padded to 5 digits in normalize_bbl)
  lot              → lot number (text, zero-padded to 4 digits in normalize_bbl)
  permit_type      → permit_type
  work_type        → work_type
  filing_date      → filing_date   (text type in Socrata — stored as MM/DD/YYYY)
  expiration_date  → expiration_date
  owner_s_first_name + owner_s_last_name → owner_name
  job_description1 → job_description
"""

import logging
from datetime import date, datetime, timezone

from pydantic import BaseModel, ConfigDict, ValidationError, model_validator
from sqlalchemy.dialects.postgresql import insert

from models.bbl import normalize_bbl
from models.database import get_scraper_db
from models.permits import PermitRaw
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

DATASET_ID = "ipu4-2q9a"
DATE_FIELD = "filing_date"

# Permit types relevant to displacement — None = ingest all (filter at scoring)
RELEVANT_PERMIT_TYPES: list[str] | None = None

# Borough name to single-digit code mapping (matches Socrata "borough" field values).
# The DOB permits dataset stores borough as a text name, not a numeric code.
BOROUGH_NAME_TO_CODE: dict[str, str] = {
    "MANHATTAN": "1",
    "BRONX": "2",
    "BROOKLYN": "3",
    "QUEENS": "4",
    "STATEN ISLAND": "5",
}


class PermitRawInput(BaseModel):
    """Pydantic model for raw DOB permit records from Socrata ipu4-2q9a.

    Handles two API fragilities:
    1. bin__ vs bin_ (double underscore variant appears in some dataset snapshots)
    2. house__ vs house_ (same double-underscore fragility)
    3. job_description1 vs job_description (field renamed in API at some point)

    The @model_validator normalizes these to canonical field names before use.
    extra="allow" is REQUIRED — DOB adds fields without notice.
    """
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    borough: str | None = None
    block: str | None = None
    lot: str | None = None
    bin_: str | None = None          # canonical — set by model_validator
    house_: str | None = None        # canonical — set by model_validator
    street_name: str | None = None
    zip_code: str | None = None
    permit_type: str | None = None
    work_type: str | None = None
    filing_date: str | None = None
    expiration_date: str | None = None
    owner_s_first_name: str | None = None
    owner_s_last_name: str | None = None
    job_description: str | None = None  # canonical — set by model_validator

    @model_validator(mode="before")
    @classmethod
    def normalize_alternate_field_names(cls, data: dict) -> dict:
        """Normalize double-underscore and renamed field variants to canonical names."""
        if isinstance(data, dict):
            data = dict(data)  # copy to avoid mutating original raw dict
            # bin__ → bin_
            if "bin__" in data and "bin_" not in data:
                data["bin_"] = data.pop("bin__")
            # house__ → house_
            if "house__" in data and "house_" not in data:
                data["house_"] = data.pop("house__")
            # job_description1 → job_description
            if "job_description1" in data and "job_description" not in data:
                data["job_description"] = data.pop("job_description1")
        return data


def _build_bbl(raw: dict) -> str | None:
    """Construct BBL from separate borough/block/lot text fields.

    Dataset ipu4-2q9a has NO bbl field — it stores location as
    separate borough (name), block, and lot text columns.

    The borough name is validated against the BOROUGH_NAME_TO_CODE allowlist
    (exactly 5 values) to reject unexpected/tampered input before passing to
    normalize_bbl() which enforces the canonical 10-digit format.

    Returns None if any required field is missing or borough is unrecognized.
    """
    borough_name = (raw.get("borough") or "").strip().upper()
    block = (raw.get("block") or "").strip()
    lot = (raw.get("lot") or "").strip()
    borough_code = BOROUGH_NAME_TO_CODE.get(borough_name)
    if not borough_code or not block or not lot:
        return None
    # Strip leading zeros before handing to normalize_bbl — the Socrata dataset
    # stores block/lot with arbitrary leading zeros (e.g. "00025" for lot 25).
    # normalize_bbl re-pads to canonical 5-digit block + 4-digit lot widths,
    # but its hyphenated regex only accepts up to \d{1,5} block and \d{1,4} lot.
    try:
        block_int = int(block)
        lot_int = int(lot)
    except ValueError:
        return None
    return normalize_bbl(f"{borough_code}-{block_int}-{lot_int}")


class PermitsScraper(BaseScraper):
    SCRAPER_NAME = "dob_permits"
    DATASET_ID = DATASET_ID

    def _run(self, db) -> tuple[int, int, datetime | None]:
        # filing_date is stored as text (MM/DD/YYYY) in Socrata ipu4-2q9a —
        # ISO comparison fails, to_floating_timestamp() is unsupported on text
        # columns, and dobrundate stopped being populated after 2020. Instead,
        # filter by year using LIKE '%/YEAR' on the last two years. This
        # reduces the download from 3.9M records to ~12k per run and makes
        # the nightly scraper viable. on_conflict_do_nothing ensures idempotency.
        current_year = datetime.now(timezone.utc).year
        # Three-year lookback: include current_year - 2 so permits added to the
        # Socrata dataset after the calendar-year boundary are not silently dropped.
        # When current_year rolls over (e.g. 2026 → 2027) the 2-year window
        # stops fetching the prior year, leaving a gap for late-arriving records.
        # on_conflict_do_nothing ensures already-ingested rows are deduplicated.
        year_conditions = " OR ".join(
            f"filing_date LIKE '%/{y}'" for y in [current_year, current_year - 1, current_year - 2]
        )
        where = f"permit_type IS NOT NULL AND ({year_conditions})"
        if RELEVANT_PERMIT_TYPES:
            types_sql = ", ".join(f"'{t}'" for t in RELEVANT_PERMIT_TYPES)
            where += f" AND permit_type IN ({types_sql})"

        records_processed = 0
        records_failed = 0
        new_watermark: datetime | None = None
        batch: list[dict] = []

        for raw in self.paginate(where, order=f"{DATE_FIELD} ASC"):
            row = self._parse(db, raw)
            if row is None:
                records_failed += 1
                continue

            batch.append(row)

            if row.get("filing_date") and (
                new_watermark is None
                or _date_to_dt(row["filing_date"]) > new_watermark
            ):
                new_watermark = _date_to_dt(row["filing_date"])

            if len(batch) >= 1_000:
                records_processed += self._upsert_batch(db, batch)
                batch = []

        if batch:
            records_processed += self._upsert_batch(db, batch)

        return records_processed, records_failed, new_watermark

    def _parse(self, db, raw: dict) -> dict | None:
        # BBL — constructed from borough name + block + lot text fields.
        # Dataset ipu4-2q9a has no bbl field; _build_bbl() handles construction
        # and borough allowlist validation via BOROUGH_NAME_TO_CODE.
        # _build_bbl receives the original raw dict (before Pydantic normalization)
        # to preserve its dict-based test compatibility.
        bbl = _build_bbl(raw)

        try:
            record = PermitRawInput.model_validate(raw)
        except ValidationError as exc:
            self.quarantine(db, raw, f"schema_validation_error:{exc}")
            return None

        bin_val = str(record.bin_).strip() if record.bin_ else None

        if bbl is None and bin_val is None:
            self.quarantine(db, raw, "missing_bbl_and_bin")
            return None

        # Address — combine house number + street name
        house = (record.house_ or "").strip()
        street = (record.street_name or "").strip()
        address = f"{house} {street}".strip() or None

        # Owner name — concatenate first + last
        first = (record.owner_s_first_name or "").strip()
        last = (record.owner_s_last_name or "").strip()
        owner_name = f"{first} {last}".strip() or None

        filing_date = _parse_date(record.filing_date)
        expiration_date = _parse_date(record.expiration_date)

        permit_type = (record.permit_type or "").strip().upper() or None
        # Use "" (not None) so the unique constraint (bbl, filing_date, permit_type,
        # work_type) correctly deduplicates rows where work_type is absent.
        # NULL != NULL in SQL unique indexes, which allows phantom duplicates.
        work_type = (record.work_type or "").strip()

        return {
            "bbl": bbl,
            "bin": bin_val,
            "address": address,
            "zip_code": _clean_zip(record.zip_code),
            "borough": record.borough,
            "permit_type": permit_type,
            "work_type": work_type,
            "owner_name": owner_name,
            "filing_date": filing_date,
            "expiration_date": expiration_date,
            "job_description": record.job_description,
            "raw_data": raw,
        }

    def _upsert_batch(self, db, batch: list[dict]) -> int:
        stmt = (
            insert(PermitRaw)
            .values(batch)
            .on_conflict_do_nothing(
                constraint="uq_permits_raw_bbl_date_type_work"
            )
        )
        result = db.execute(stmt)
        db.commit()
        return result.rowcount


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    value = value.strip()
    # Try MM/DD/YYYY first — confirmed format for recent DOB permit records.
    # Then fall back to ISO variants for historical data or other sources.
    for fmt in ("%m/%d/%Y", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    # Last resort: truncate to YYYY-MM-DD and try once more
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _date_to_dt(d: date | None) -> datetime | None:
    if d is None:
        return None
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _clean_zip(value: str | None) -> str | None:
    if not value:
        return None
    z = str(value).strip().split("-")[0]
    return z if len(z) == 5 and z.isdigit() else None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with get_scraper_db() as db:
        scraper = PermitsScraper()
        run = scraper.run(db)
        print(f"Status: {run.status} | Processed: {run.records_processed}")
