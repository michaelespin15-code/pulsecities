"""
NYC 311 Service Requests scraper.
Dataset: erm2-nwe9 (NYC Open Data)
Update frequency: daily
Watermark field: created_date

BBL is present in the dataset but is frequently null; this is expected.
Null-BBL records are still persisted (bbl=None). They participate in
zip-code-level scoring but are excluded from BBL-level scoring.

Field mapping (Socrata → model):
  unique_key     → unique_key  (natural upsert key)
  created_date   → created_date
  closed_date    → closed_date
  agency         → agency
  complaint_type → complaint_type
  descriptor     → descriptor
  incident_zip   → zip_code      (Socrata uses incident_zip, not zip_code)
  incident_address → address
  borough        → borough
  bbl            → bbl           (often null, acceptable)
  latitude       → point geometry
  longitude      → point geometry
  status         → status
"""

import logging
from datetime import datetime, timezone

from geoalchemy2.shape import from_shape
from pydantic import BaseModel, ConfigDict, ValidationError
from shapely.geometry import Point
from sqlalchemy.dialects.postgresql import insert

from models.bbl import normalize_bbl
from models.complaints import ComplaintRaw
from models.database import get_scraper_db
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class ComplaintRawInput(BaseModel):
    """Pydantic validator for raw 311 complaint records from Socrata erm2-nwe9; extra fields allowed."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    unique_key: str = ""
    complaint_type: str | None = None
    descriptor: str | None = None
    incident_zip: str | None = None
    incident_address: str | None = None
    borough: str | None = None
    agency: str | None = None
    status: str | None = None
    created_date: str | None = None
    closed_date: str | None = None
    latitude: str | None = None
    longitude: str | None = None
    bbl: str | None = None
    x_coord_cd: str | None = None  # fallback for BBL lookup

DATASET_ID = "erm2-nwe9"
DATE_FIELD = "created_date"

# Only ingest complaint types relevant to displacement pressure
# None = ingest all types (recommended for v1, filter at scoring time)
COMPLAINT_TYPE_FILTER: list[str] | None = None


class ComplaintsScraper(BaseScraper):
    SCRAPER_NAME = "311_complaints"
    DATASET_ID = DATASET_ID
    PAGE_TIMEOUT = 120  # 311 Socrata endpoint is slow, override base 60s

    def _run(self, db) -> tuple[int, int, datetime | None]:
        where = self.build_where_since(DATE_FIELD, db)
        if COMPLAINT_TYPE_FILTER:
            types_sql = ", ".join(f"'{t}'" for t in COMPLAINT_TYPE_FILTER)
            where += f" AND complaint_type IN ({types_sql})"

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

            # Track watermark from parsed created_date
            if row.get("created_date") and (
                new_watermark is None or row["created_date"] > new_watermark
            ):
                new_watermark = row["created_date"]

            if len(batch) >= 1_000:
                records_processed += self._upsert_batch(db, batch)
                batch = []

        if batch:
            records_processed += self._upsert_batch(db, batch)

        return records_processed, records_failed, new_watermark

    def _parse(self, db, raw: dict) -> dict | None:
        try:
            record = ComplaintRawInput.model_validate(raw)
        except ValidationError as exc:
            self.quarantine(db, raw, f"schema_validation_error:{exc}")
            return None

        if not record.unique_key:
            self.quarantine(db, raw, "missing_required_field:unique_key")
            return None

        # BBL is nullable; normalize if present, leave None if absent
        bbl_raw = record.bbl or record.x_coord_cd  # some exports use different keys
        bbl = normalize_bbl(bbl_raw) if bbl_raw else None

        # Parse geometry from lat/lng
        location = None
        try:
            lat = record.latitude
            lng = record.longitude
            if lat and lng:
                location = from_shape(Point(float(lng), float(lat)), srid=4326)
        except (TypeError, ValueError):
            pass  # Missing coordinates are common, not a quarantine reason

        # Parse timestamps
        created_date = _parse_dt(record.created_date)
        closed_date = _parse_dt(record.closed_date)

        return {
            "unique_key": record.unique_key,
            "bbl": bbl,
            "complaint_type": record.complaint_type,
            "descriptor": record.descriptor,
            "zip_code": _clean_zip(record.incident_zip),
            "borough": record.borough,
            "address": record.incident_address,
            "agency": record.agency,
            "status": record.status,
            "created_date": created_date,
            "closed_date": closed_date,
            "location": location,
            "raw_data": raw,
        }

    def _upsert_batch(self, db, batch: list[dict]) -> int:
        stmt = (
            insert(ComplaintRaw)
            .values(batch)
            .on_conflict_do_nothing(constraint="uq_complaints_raw_unique_key")
        )
        result = db.execute(stmt)
        db.commit()
        return result.rowcount


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    # Socrata returns ISO 8601: "2024-04-10T14:30:00.000"
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(value[:26], fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _clean_zip(value: str | None) -> str | None:
    if not value:
        return None
    z = value.strip().split("-")[0]  # strip ZIP+4 suffix if present
    return z if len(z) == 5 and z.isdigit() else None


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    try:
        with get_scraper_db() as db:
            scraper = ComplaintsScraper()
            run = scraper.run(db)
            print(f"Status: {run.status} | Processed: {run.records_processed}")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
