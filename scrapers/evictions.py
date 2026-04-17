"""
NYC Marshal Eviction Records scraper.
Dataset: 6z8x-wfk4 (NYC Open Data)
Update frequency: weekly (OCA reports lag 2–4 weeks — by design)
Watermark field: executed_date

IMPORTANT: This dataset has a known 2–4 week reporting lag.
The OCA (Office of Court Administration) does not report evictions
in real time. An eviction that happened today will not appear here
for 2–4 weeks. This is documented behavior, not a data quality failure.
The scoring engine weights evictions as a LAGGING indicator.

Also: some records lack BBL. These are still persisted (bbl=None)
and contribute to zip-code-level eviction rate scoring.

Field mapping (Socrata → model):
  court_index_number        → court_index_number
  docket_number             → docket_number
  eviction_address          → address
  apartment_no              → (appended to address if present)
  executed_date             → executed_date  (watermark)
  eviction_borough          → borough
  zip_code                  → zip_code
  bbl                       → bbl
  residential_commercial_ind → eviction_type (R=residential, C=commercial)
"""

import logging
from datetime import date, datetime, timezone

from pydantic import BaseModel, ConfigDict, ValidationError, model_validator
from sqlalchemy.dialects.postgresql import insert

from models.bbl import normalize_bbl
from models.database import get_scraper_db
from models.evictions import EvictionRaw
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

DATASET_ID = "6z8x-wfk4"
DATE_FIELD = "executed_date"

# Evictions look back further on first run since dataset lags 2-4 weeks
INITIAL_LOOKBACK_DAYS = 730  # 2 years of history on first run


class EvictionRawInput(BaseModel):
    """Pydantic model for raw eviction records from Socrata 6z8x-wfk4.

    Handles the highest-risk fragility in the codebase: 5 alternate field names
    across 3 semantic fields. The @model_validator normalizes all variants to
    canonical names before use.

    Canonical field mapping:
        zip_code        <- zip_code | eviction_zip | eviction_zip_code
        eviction_borough <- eviction_borough | borough
        eviction_type   <- residential_commercial_ind | eviction_type

    extra="allow" is REQUIRED — OCA dataset changes without notice.
    """
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    executed_date: str | None = None
    court_index_number: str | None = None
    docket_number: str | None = None
    eviction_address: str | None = None
    apartment_no: str | None = None
    eviction_borough: str | None = None  # canonical — set by model_validator
    zip_code: str | None = None          # canonical — set by model_validator
    bbl: str | None = None
    eviction_type: str | None = None     # canonical — set by model_validator

    @model_validator(mode="before")
    @classmethod
    def normalize_alternate_field_names(cls, data: dict) -> dict:
        """Normalize alternate field name variants to canonical names.

        This is the primary defense against the OCA dataset field renames
        that have occurred historically (eviction_zip -> zip_code, etc.).
        """
        if isinstance(data, dict):
            data = dict(data)
            # zip: zip_code is canonical; eviction_zip and eviction_zip_code are alternates
            if "zip_code" not in data or data["zip_code"] is None:
                data["zip_code"] = data.get("eviction_zip") or data.get("eviction_zip_code")
            # borough: eviction_borough is canonical; borough is alternate
            if "eviction_borough" not in data or data["eviction_borough"] is None:
                data["eviction_borough"] = data.get("borough")
            # eviction_type: residential_commercial_ind is the OCA field name
            if "eviction_type" not in data or data["eviction_type"] is None:
                data["eviction_type"] = data.get("residential_commercial_ind")
        return data


class EvictionsScraper(BaseScraper):
    SCRAPER_NAME = "evictions"
    DATASET_ID = DATASET_ID
    INITIAL_LOOKBACK_DAYS = INITIAL_LOOKBACK_DAYS
    # OCA reports lag 2–4 weeks; re-scan the last 45 days on every incremental
    # run so late-arriving eviction records aren't silently skipped.
    WATERMARK_EXTRA_LOOKBACK_DAYS = 45

    def _run(self, db) -> tuple[int, int, datetime | None]:
        where = self.build_where_since(DATE_FIELD, db)

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

            if row.get("executed_date") and (
                new_watermark is None
                or _date_to_dt(row["executed_date"]) > new_watermark
            ):
                new_watermark = _date_to_dt(row["executed_date"])

            if len(batch) >= 1_000:
                records_processed += self._upsert_batch(db, batch)
                batch = []

        if batch:
            records_processed += self._upsert_batch(db, batch)

        return records_processed, records_failed, new_watermark

    def _parse(self, db, raw: dict) -> dict | None:
        try:
            record = EvictionRawInput.model_validate(raw)
        except ValidationError as exc:
            self.quarantine(db, raw, f"schema_validation_error:{exc}")
            return None

        executed_date = _parse_date(record.executed_date)
        if executed_date is None:
            self.quarantine(db, raw, "missing_required_field:executed_date")
            return None

        bbl_raw = record.bbl
        bbl = normalize_bbl(bbl_raw) if bbl_raw else None

        # Address: combine eviction_address + apartment_no if present
        address = (record.eviction_address or "").strip()
        apt = (record.apartment_no or "").strip()
        if apt:
            address = f"{address} Apt {apt}".strip()
        address = address or None

        # Borough and zip — normalized to canonical names by model_validator
        borough = record.eviction_borough
        zip_raw = record.zip_code

        # Eviction type — R=residential, C=commercial; normalized by model_validator
        eviction_type = record.eviction_type

        docket = (record.docket_number or "").strip() or None
        court_index = (record.court_index_number or "").strip() or None

        # Quarantine if no docket AND no court index (can't form upsert key)
        if docket is None and court_index is None and bbl is None:
            self.quarantine(db, raw, "missing_identifiers:no_bbl_docket_or_court_index")
            return None

        return {
            "bbl": bbl,
            "docket_number": docket,
            "address": address,
            "zip_code": _clean_zip(zip_raw),
            "borough": borough,
            "eviction_type": eviction_type,
            "executed_date": executed_date,
            "court_index_number": court_index,
            "raw_data": raw,
        }

    def _upsert_batch(self, db, batch: list[dict]) -> int:
        stmt = (
            insert(EvictionRaw)
            .values(batch)
            .on_conflict_do_nothing(
                constraint="uq_evictions_raw_bbl_date_docket"
            )
        )
        result = db.execute(stmt)
        db.commit()
        return result.rowcount


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    # Try full ISO timestamp formats first, then truncate to date
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value[:26], fmt).date()
        except ValueError:
            continue
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
        scraper = EvictionsScraper()
        run = scraper.run(db)
        print(f"Status: {run.status} | Processed: {run.records_processed}")
