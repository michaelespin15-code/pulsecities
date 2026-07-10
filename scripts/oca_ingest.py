"""
OCA housing-court petitions — ZIP-level monthly aggregates.

Source: the OCA Data Collective public extracts (Housing Data Coalition /
Right to Counsel), which publish every NY housing-court petition with the
index number hashed and the address reduced to a ZIP. That makes petition
arcs per building impossible by design; what the data is good for is an
early-warning volume signal, since filings lead executed evictions by
months. This ingest keeps only residential NYC petitions and stores
(zip, month, classification) counts.

License: CC BY-NC-SA 4.0. Display with attribution only; this table must
never feed a commercial API surface.

    PYTHONPATH=. venv/bin/python -m scripts.oca_ingest [--keep-downloads]

Runs weekly from cron. Both CSVs stream through a temporary SQLite spill
so the 700MB source never has to fit in RAM on this box.
"""

import argparse
import csv
import logging
import sqlite3
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import date
from pathlib import Path

from sqlalchemy import text

from config.logging_config import configure_logging
from models.database import get_scraper_db

configure_logging()
logger = logging.getLogger(__name__)

_BASE = "https://oca-2-dev.s3.amazonaws.com/public"
_FILES = ("oca_addresses.csv", "oca_index.csv")

# The extracts reach back to 2016; everything matters for trend baselines,
# but rows before this floor are noise for an early-warning signal.
MIN_FILED = "2019-01-01"

csv.field_size_limit(10_000_000)


def _download(workdir: Path) -> None:
    for name in _FILES:
        dest = workdir / name
        logger.info("Downloading %s ...", name)
        subprocess.run(
            ["curl", "-sS", "--fail", "--retry", "3", "-o", str(dest), f"{_BASE}/{name}"],
            check=True,
        )
        logger.info("%s: %.0f MB", name, dest.stat().st_size / 1048576)


def _nyc_zips(db) -> set[str]:
    return {r[0] for r in db.execute(text("SELECT zip_code FROM neighborhoods"))}


def build_spill(addresses_csv: Path, zips: set[str], spill_path: Path) -> int:
    """indexnumberid -> zip for NYC petitions, spilled to SQLite."""
    con = sqlite3.connect(spill_path)
    con.execute("CREATE TABLE ids (id TEXT PRIMARY KEY, zip TEXT) WITHOUT ROWID")
    n = 0
    with open(addresses_csv, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            z = (row.get("postalcode") or "").strip()[:5]
            if z in zips:
                batch.append((row["indexnumberid"], z))
                n += 1
                if len(batch) >= 50_000:
                    con.executemany("INSERT OR IGNORE INTO ids VALUES (?, ?)", batch)
                    batch = []
        if batch:
            con.executemany("INSERT OR IGNORE INTO ids VALUES (?, ?)", batch)
    con.commit()
    con.close()
    return n


def aggregate(index_csv: Path, spill_path: Path) -> Counter:
    """(zip, month, classification) -> filings for residential NYC petitions."""
    con = sqlite3.connect(spill_path)
    cur = con.cursor()
    counts: Counter = Counter()
    with open(index_csv, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("propertytype") or "") != "Residential":
                continue
            filed = (row.get("fileddate") or "")[:10]
            if len(filed) != 10 or filed < MIN_FILED:
                continue
            hit = cur.execute(
                "SELECT zip FROM ids WHERE id = ?", (row["indexnumberid"],)
            ).fetchone()
            if not hit:
                continue
            # Clamp to the column width so a new upstream label can't abort the rebuild.
            classification = ((row.get("classification") or "Other").strip() or "Other")[:40]
            counts[(hit[0], filed[:7] + "-01", classification)] += 1
    con.close()
    return counts


def store(db, counts: Counter) -> None:
    """Full rebuild in one transaction; the table is a pure aggregate."""
    db.execute(text("TRUNCATE oca_petitions_monthly"))
    rows = [
        {"z": z, "m": m, "c": c, "n": n}
        for (z, m, c), n in counts.items()
    ]
    for i in range(0, len(rows), 5000):
        db.execute(text("""
            INSERT INTO oca_petitions_monthly (zip_code, month, classification, filings)
            VALUES (:z, :m, :c, :n)
        """), rows[i:i + 5000])
    db.commit()


def run(keep_downloads: bool = False) -> None:
    with tempfile.TemporaryDirectory(prefix="oca_") as tmp:
        workdir = Path(tmp)
        _download(workdir)

        with get_scraper_db() as db:
            zips = _nyc_zips(db)
        logger.info("Matching against %d NYC ZIPs", len(zips))

        spill = workdir / "ids.sqlite"
        n_ids = build_spill(workdir / "oca_addresses.csv", zips, spill)
        logger.info("NYC petition ids spilled: %s", f"{n_ids:,}")

        counts = aggregate(workdir / "oca_index.csv", spill)
        total = sum(counts.values())
        logger.info("Aggregated %s residential filings into %s (zip, month, class) rows",
                    f"{total:,}", f"{len(counts):,}")

        if not counts:
            logger.error("Zero rows aggregated; refusing to truncate the existing table.")
            sys.exit(1)

        with get_scraper_db() as db:
            store(db, counts)
        logger.info("oca_petitions_monthly rebuilt: %s rows through %s",
                    f"{len(counts):,}", max(m for _, m, _ in counts))

        if keep_downloads:
            keep_dir = Path(__file__).parent / ".oca_cache"
            keep_dir.mkdir(exist_ok=True)
            for name in _FILES:
                (workdir / name).rename(keep_dir / name)
            logger.info("Downloads kept in %s", keep_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest OCA petition CSVs into ZIP-month aggregates")
    parser.add_argument("--keep-downloads", action="store_true",
                        help="keep the raw CSVs in scripts/.oca_cache instead of deleting")
    args = parser.parse_args()
    run(keep_downloads=args.keep_downloads)
