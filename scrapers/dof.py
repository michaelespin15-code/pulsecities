"""
NYC Department of Finance (DOF) property assessment data loader.
Dataset: w7rz-68fs (NYC Open Data — Socrata API)

WHAT THIS FEED ACTUALLY IS. Nine fiscal years, 2010/11 through 2018/19, for
72,898 lots, 608,240 rows. Roughly 8.3 rows per lot, one per year. It is a
frozen historical archive: DOF stopped publishing to it after 2018/19.

That shape was misread for a long time and it cost the site a public wrong
number. The loader fetched all nine years, threw the year away, and upserted
every row into parcels keyed on bbl alone, so `assessed_total` ended up holding
whichever fiscal year the paginator happened to emit last. Measured on a random
sample of 35 overlapping lots, 30 held a value that is exactly a DOF avtot from
some year between 2010/11 and 2018/19, and the year varied at random per lot:
2011/12, 2013/14, 2014/15, 2016/17, 2018/19. PLUTO could not correct any of it,
because these are condo unit lots and MapPLUTO carries the billing lot instead.

/property then printed "the city assessed this lot at $287K for the 2026 tax
year" on 10,197 sitemapped pages. The figure was the 2014/15 assessment. The
year came from assessment_history, where scrapers/pluto.py had stamped the
calendar year onto every parcel carrying an assessed value, including 59,423 it
had never seen.

So this loader now writes the year it read. assessment_history is keyed
(bbl, tax_year) and takes one row per real fiscal year, which is order
independent by construction. parcels.assessed_total is derived from that table
after the walk, per lot, newest year first, and only for lots MapPLUTO does not
cover: a 2018/19 assessment must never overwrite a current one.

There is no datetime watermark. DOF exposes no reliable updated_at, and this
archive has not moved in seven years.

Field ownership (DOF vs MapPLUTO):
  DOF owns:    assessed_total
  MapPLUTO owns: units_res, units_total, geometry, owner_name, owner_type,
                 address, zip_code, year_built, lot_area, bldg_area,
                 zoning_dist, land_use

CRITICAL: The on_conflict_do_update set_ dict MUST include ONLY assessed_total.
If DOF were to overwrite geometry, units_res, or other PLUTO-sourced fields,
a DOF run after PLUTO would destroy parcel geometry and unit counts — breaking
the scoring engine's per-unit normalization.

Residential unit counts (units_res) come from MapPLUTO, not DOF. DOF only provides
assessed values.

Field mapping (Socrata → model):
  bble         → bbl          (10-digit BBL, already canonical in DOF dataset)
  boro         → borough      (int)
  block        → block        (str, zero-padded to 5 digits)
  lot          → lot          (str, zero-padded to 4 digits)
  avtot        → assessed_total (assessed value; primary field)
  fullval      → assessed_total (fallback if avtot is absent or zero)
  year         → assessment_history.tax_year (int of the first year, "2018/19" → 2018)
  staddr       → address      (only if PLUTO address is None — DOF does not own address)
  zip          → zip_code     (only if PLUTO zip_code is None — DOF does not own zip)
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from models.bbl import normalize_bbl
from models.database import get_scraper_db
from models.properties import Parcel
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

DATASET_ID = "w7rz-68fs"


class DOFScraper(BaseScraper):
    """DOF property assessment loader, full refresh each run; updates assessed_total only on conflict."""

    SCRAPER_NAME = "dof_assessments"
    DATASET_ID = DATASET_ID
    INITIAL_LOOKBACK_DAYS = 0  # unused, no date watermark for DOF assessments

    # DOF is a full-refresh dataset with 900K+ records. The default 50K-row
    # pages spike memory enough to trigger OOM on constrained hosts. 5K rows
    # per page keeps peak in-flight memory to ~5MB per fetch instead of ~50MB.
    PAGE_SIZE = 5_000

    def _run(self, db) -> tuple[int, int, datetime | None]:
        # DOF has no date filter, always fetch all records with a valid BBL
        where = "bble IS NOT NULL"

        records_processed = 0
        records_failed = 0
        no_year = 0
        newest_year: int | None = None
        batch: list[dict] = []

        for raw in self.paginate(where):
            row = self._parse(db, raw)
            if row is None:
                records_failed += 1
                continue
            # A row whose fiscal year will not parse cannot be filed under a
            # year, and an assessment filed under the wrong year is the bug
            # this loader exists to have stopped making. Drop it and say so.
            if row["assessment_year"] is None:
                no_year += 1
                records_failed += 1
                continue

            if newest_year is None or row["assessment_year"] > newest_year:
                newest_year = row["assessment_year"]
            batch.append(row)

            if len(batch) >= 2_000:
                records_processed += self._upsert_batch(db, batch)
                batch = []

        if batch:
            records_processed += self._upsert_batch(db, batch)

        if no_year:
            logger.warning(
                "dof_assessments: %d rows carried no parseable fiscal year and "
                "were not loaded", no_year,
            )

        synced = self._sync_parcel_assessments(db, newest_year)

        logger.info(
            "DOF assessment load complete: %d rows upserted, %d failed, "
            "%d parcels took a DOF assessment",
            records_processed,
            records_failed,
            synced,
        )
        # No watermark for DOF, full refresh dataset
        return records_processed, records_failed, None

    def _sync_parcel_assessments(self, db, newest_year: int | None) -> int:
        """Set parcels.assessed_total from the newest DOF year this lot has.

        Deliberately a single statement after the walk rather than a value
        carried on each upserted row. Pagination order is unspecified where the
        sort key ties, and this feed has nine rows per lot, so "the last row
        wins" resolved to a different fiscal year per lot. DISTINCT ON with an
        explicit ORDER BY resolves to the newest year for every lot, whatever
        order the rows arrived in.

        The NOT EXISTS is the ownership rule from the header made executable. A
        lot MapPLUTO covers already carries a current assessment, and this
        archive stopped in 2018/19; overwriting the first with the second is how
        /property came to publish an eleven-year-old number.

        The ceiling is the newest year this run actually read, not a pinned
        2018. Pinning it would be the stale-constraint shape the ops notes
        already catalogue: if DOF ever resumes publishing, a constant keeps the
        loader filing new assessments as though they were archive.
        """
        if newest_year is None:
            return 0

        result = db.execute(
            text("""
                UPDATE parcels p
                   SET assessed_total = d.assessed_total,
                       updated_at = now()
                  FROM (
                        SELECT DISTINCT ON (bbl) bbl, assessed_total
                          FROM assessment_history
                         WHERE assessed_total IS NOT NULL
                           AND tax_year <= :dof_ceiling
                         ORDER BY bbl, tax_year DESC
                       ) d
                 WHERE p.bbl = d.bbl
                   AND p.assessed_total IS DISTINCT FROM d.assessed_total
                   AND NOT EXISTS (
                        SELECT 1 FROM assessment_history newer
                         WHERE newer.bbl = p.bbl
                           AND newer.tax_year > :dof_ceiling
                           AND newer.assessed_total IS NOT NULL
                       )
            """),
            {"dof_ceiling": newest_year},
        )
        db.commit()
        return result.rowcount

    def _parse(self, db, raw: dict) -> dict | None:
        # BBL: try bble field first (10-digit canonical), fall back to parts
        bbl_raw = raw.get("bble")
        bbl = None

        if bbl_raw:
            try:
                bbl = normalize_bbl(str(int(float(bbl_raw))))
            except (TypeError, ValueError):
                bbl = None

        if bbl is None:
            # Fallback: construct from boro + block + lot
            try:
                boro = str(int(float(raw["boro"]))).strip()
                block = str(int(float(raw["block"]))).zfill(5)
                lot = str(int(float(raw["lot"]))).zfill(4)
                bbl = normalize_bbl(f"{boro}{block}{lot}")
            except (KeyError, TypeError, ValueError):
                pass

        if bbl is None:
            self.quarantine(db, raw, "invalid_or_missing_bbl")
            return None

        # Assessed value: prefer avtot, fall back to fullval if avtot is absent/zero
        assessed_total = _safe_float(raw.get("avtot"))
        if assessed_total is None or assessed_total == 0.0:
            fallback = _safe_float(raw.get("fullval"))
            if fallback is not None and fallback > 0.0:
                assessed_total = fallback
            elif assessed_total == 0.0:
                # Both present but zero, treat as None (no valid assessment)
                assessed_total = None

        # The fiscal year this assessment belongs to, "2018/19" -> 2018. This
        # is the whole point of the row: nine of them share a BBL and differ
        # only here, so a loader that drops it cannot tell them apart.
        year_raw = raw.get("year")
        assessment_year = None
        if year_raw:
            try:
                assessment_year = int(str(year_raw).strip()[:4])
            except (TypeError, ValueError):
                pass

        return {
            "bbl": bbl,
            "borough": _safe_int(raw.get("boro")),
            "block": str(int(float(raw["block"]))).zfill(5) if raw.get("block") else None,
            "lot": str(int(float(raw["lot"]))).zfill(4) if raw.get("lot") else None,
            # DOF may provide address and zip, but only as supplementary data
            # The on_conflict_do_update does NOT include these fields; PLUTO owns them
            "address": (raw.get("staddr") or "").strip() or None,
            "zip_code": _clean_zip(raw.get("zip")),
            "assessed_total": assessed_total,
            "assessment_year": assessment_year,
            "raw_data": raw,
        }

    def _upsert_batch(self, db, batch: list[dict]) -> int:
        """File every row under its own fiscal year, then make sure the lot exists.

        Two writes, and the order matters. assessment_history is the record: it
        is keyed (bbl, tax_year), so nine years of the same lot land in nine
        rows and no year can overwrite another. parcels only needs the lot to
        exist, because _sync_parcel_assessments picks the value afterwards from
        the years actually on file.
        """
        now = datetime.now(timezone.utc)

        # One row per (lot, fiscal year). DO UPDATE rather than DO NOTHING: a
        # re-run after DOF restates a value has to be able to correct it, and
        # the same DO NOTHING in score_history is what let a half-loaded
        # permits table stay in the permanent record on 2026-08-28.
        history_rows = _dedupe_by_bbl_year(batch)
        if history_rows:
            db.execute(
                text("""
                    INSERT INTO assessment_history (bbl, assessed_total, tax_year, created_at)
                    VALUES (:bbl, :assessed_total, :tax_year, :created_at)
                    ON CONFLICT (bbl, tax_year) DO UPDATE
                       SET assessed_total = EXCLUDED.assessed_total
                """),
                [{"bbl": r["bbl"], "assessed_total": r["assessed_total"],
                  "tax_year": r["assessment_year"], "created_at": now}
                 for r in history_rows],
            )

        # The lot itself. DO NOTHING on the assessment: which year wins is
        # decided once, after the walk, by _sync_parcel_assessments. Deciding
        # it here means deciding it by pagination order, which is what put a
        # 2014/15 figure under a 2026 heading on ten thousand pages.
        parcel_rows = []
        for row in _dedupe_by_bbl(batch):
            parcel_rows.append({
                "bbl": row["bbl"],
                "borough": row.get("borough"),
                "block": row.get("block"),
                "lot": row.get("lot"),
                "address": row.get("address"),
                "zip_code": row.get("zip_code"),
                "assessed_total": row["assessed_total"],
                "on_speculation_watch_list": False,
                "created_at": now,
                "updated_at": now,
            })

        # Every row must carry the same columns. A multi-row insert with
        # heterogeneous keys, plus the Python-side defaults on
        # on_speculation_watch_list/created_at/updated_at, makes SQLAlchemy bind
        # the first row's defaults un-suffixed while later rows get _m1.., which
        # fails to compile (the f405 that aborted the nightly pipeline). Supply
        # all columns on every row, and the NOT NULL watch-list flag explicitly.
        stmt = (
            insert(Parcel)
            .values(parcel_rows)
            .on_conflict_do_nothing(constraint="uq_parcels_bbl")
        )
        db.execute(stmt)
        db.commit()
        return len(history_rows)


def _dedupe_by_bbl(batch: list[dict]) -> list[dict]:
    """Last occurrence per BBL wins.

    A BBL repeats within a page because this feed carries one row per fiscal
    year and there are nine of them. The note that used to sit here said the
    repeats were easement and condo rows sharing a parcel key, which is why
    nobody looked at the year: the duplicate had already been explained. Two
    rows with the same conflict key in a single INSERT ... ON CONFLICT is a
    CardinalityViolation, so the collapse still has to happen. It is only safe
    now because the caller no longer takes the assessment from the survivor.
    """
    deduped: dict[str, dict] = {}
    for row in batch:
        deduped[row["bbl"]] = row
    return list(deduped.values())


def _dedupe_by_bbl_year(batch: list[dict]) -> list[dict]:
    """Last occurrence per (BBL, fiscal year) wins.

    The conflict key of assessment_history, so the same CardinalityViolation
    applies. A lot genuinely restated within one page is rare; when it happens
    the later row is the one DOF served last.
    """
    deduped: dict[tuple[str, int], dict] = {}
    for row in batch:
        deduped[(row["bbl"], row["assessment_year"])] = row
    return list(deduped.values())


def _safe_int(value) -> int | None:
    try:
        return int(float(value)) if value not in (None, "", "0") else None
    except (TypeError, ValueError):
        return None


def _safe_float(value) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _clean_zip(value: str | None) -> str | None:
    if not value:
        return None
    z = str(value).strip().split("-")[0]
    return z if len(z) == 5 and z.isdigit() else None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with get_scraper_db() as db:
        scraper = DOFScraper()
        run = scraper.run(db)
        print(f"Status: {run.status} | Processed: {run.records_processed}")
