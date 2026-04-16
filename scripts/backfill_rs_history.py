"""
Backfill historical rent-stabilized unit counts from the nycdb rentstab_v2 dataset
into assessment_history (stabilized_units column) and rs_buildings (rs_unit_count).

Data source — nycdb rentstab_v2:
  The nycdb project scrapes NYC DOF tax bills to extract stabilized unit counts
  per BBL per year.  Download the CSV before running:

      pip install nycdb
      nycdb --download rentstab_v2 --dbname ignore --user ignore \
            --host ignore --download-only --output-dir /tmp/nycdb

  Or fetch directly (nycdb S3):
      curl -L -o /tmp/rentstab_v2.csv.zip \
        "https://s3.amazonaws.com/justfix-data/nycdb-rentstab_v2.zip"
      unzip /tmp/rentstab_v2.csv.zip -d /tmp/nycdb

  The resulting CSV has this wide format (one row per BBL):
      ucbbl,uc2007,uc2008,uc2009,...,uc2022
      1000010001,12,12,11,10,...,8
      ...

Usage:
    python scripts/backfill_rs_history.py /path/to/rentstab_v2.csv
    python scripts/backfill_rs_history.py /path/to/rentstab_v2.csv --dry-run
"""

import csv
import logging
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from models.bbl import normalize_bbl
from models.database import SessionLocal

logger = logging.getLogger(__name__)

BATCH_SIZE = 2_000

# Column prefix for per-year unit count columns in the nycdb rentstab CSV.
_YEAR_PREFIX = "uc"

# Year range known to be present in rentstab_v2.  Columns outside this range
# that match the prefix are also accepted — the range is used only for logging.
_EXPECTED_YEAR_MIN = 2007
_EXPECTED_YEAR_MAX = 2024


def _parse_year_columns(headers: list[str]) -> list[tuple[int, str]]:
    """
    Return [(year, col_name), ...] for all uc{YYYY} columns in the header row,
    sorted ascending by year.
    """
    years = []
    for col in headers:
        if col.startswith(_YEAR_PREFIX):
            suffix = col[len(_YEAR_PREFIX):]
            try:
                yr = int(suffix)
            except ValueError:
                continue
            if 2000 <= yr <= 2030:
                years.append((yr, col))
    return sorted(years)


def _safe_int(value: str | None) -> int | None:
    if not value or not value.strip():
        return None
    try:
        v = int(float(value.strip()))
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None


def _flush_assessment(db, batch: list[dict]) -> int:
    if not batch:
        return 0
    db.execute(
        text("""
            INSERT INTO assessment_history (bbl, tax_year, stabilized_units, created_at)
            SELECT
                (v->>'bbl')::text,
                (v->>'tax_year')::int,
                (v->>'stabilized_units')::int,
                NOW()
            FROM jsonb_array_elements(CAST(:rows AS jsonb)) v
            ON CONFLICT ON CONSTRAINT pk_assessment_history
            DO UPDATE SET
                stabilized_units = EXCLUDED.stabilized_units
            WHERE assessment_history.stabilized_units IS DISTINCT FROM EXCLUDED.stabilized_units
        """),
        {"rows": __import__("json").dumps(batch)},
    )
    return len(batch)


def _flush_rs_buildings(db, batch: list[dict]) -> int:
    if not batch:
        return 0
    db.execute(
        text("""
            INSERT INTO rs_buildings (bbl, year, rs_unit_count, raw_data, created_at, updated_at)
            SELECT
                (v->>'bbl')::text,
                (v->>'year')::int,
                (v->>'rs_unit_count')::int,
                '{}'::jsonb,
                NOW(),
                NOW()
            FROM jsonb_array_elements(CAST(:rows AS jsonb)) v
            ON CONFLICT ON CONSTRAINT uq_rs_buildings_bbl_year
            DO UPDATE SET
                rs_unit_count = EXCLUDED.rs_unit_count,
                updated_at    = EXCLUDED.updated_at
            WHERE rs_buildings.rs_unit_count IS DISTINCT FROM EXCLUDED.rs_unit_count
        """),
        {"rows": __import__("json").dumps(batch)},
    )
    return len(batch)


def run(csv_path: Path, dry_run: bool = False) -> None:
    total_rows = 0
    total_points = 0
    skipped_bbl = 0

    ah_batch: list[dict] = []
    rs_batch: list[dict] = []
    ah_written = 0
    rs_written = 0

    db = SessionLocal()
    try:
        with csv_path.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            headers = reader.fieldnames or []
            year_cols = _parse_year_columns(headers)

            if not year_cols:
                logger.error(
                    "No uc{YYYY} columns found in %s — is this a rentstab CSV?",
                    csv_path,
                )
                sys.exit(1)

            logger.info(
                "Found %d year columns: %d–%d",
                len(year_cols),
                year_cols[0][0],
                year_cols[-1][0],
            )

            for raw in reader:
                total_rows += 1

                bbl_raw = raw.get("ucbbl") or raw.get("bbl") or ""
                bbl = normalize_bbl(bbl_raw.strip())
                if not bbl:
                    skipped_bbl += 1
                    continue

                for yr, col in year_cols:
                    units = _safe_int(raw.get(col))
                    if units is None:
                        continue

                    total_points += 1
                    ah_batch.append({"bbl": bbl, "tax_year": yr, "stabilized_units": units})
                    rs_batch.append({"bbl": bbl, "year": yr, "rs_unit_count": units})

                if len(ah_batch) >= BATCH_SIZE:
                    if not dry_run:
                        ah_written += _flush_assessment(db, ah_batch)
                        rs_written += _flush_rs_buildings(db, rs_batch)
                        db.commit()
                    ah_batch.clear()
                    rs_batch.clear()
                    logger.info(
                        "Progress: %d BBLs | %d data points | "
                        "%d assessment_history rows | %d rs_buildings rows",
                        total_rows, total_points, ah_written, rs_written,
                    )

        # Final batch
        if ah_batch and not dry_run:
            ah_written += _flush_assessment(db, ah_batch)
            rs_written += _flush_rs_buildings(db, rs_batch)
            db.commit()

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    prefix = "[DRY RUN] " if dry_run else ""
    logger.info(
        "%sComplete: %d BBLs read, %d skipped (invalid BBL), "
        "%d (bbl, year) points | %d → assessment_history | %d → rs_buildings",
        prefix, total_rows, skipped_bbl, total_points, ah_written, rs_written,
    )


def main() -> None:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Backfill nycdb rentstab history")
    parser.add_argument("csv_file", help="Path to rentstab_v2 CSV file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and count rows without writing to the database",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv_file)
    if not csv_path.exists():
        logger.error("File not found: %s", csv_path)
        sys.exit(1)

    run(csv_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
