"""
One-time backfill: copy parcels.assessed_total into assessment_history for tax_year=2026.

Run once after the add_assessment_history migration to capture the April 2026
MapPLUTO snapshot as the first year of YoY history. The assessment_spike signal
in scoring/compute.py becomes active after the April 2027 MapPLUTO run adds
tax_year=2027 rows.

    python scripts/backfill_assessment_2026.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from models.database import SessionLocal


def backfill():
    db = SessionLocal()
    try:
        result = db.execute(
            text("""
                INSERT INTO assessment_history (bbl, assessed_total, tax_year, created_at)
                SELECT bbl, assessed_total, 2026, NOW()
                FROM parcels
                WHERE assessed_total IS NOT NULL
                  AND bbl IS NOT NULL
                ON CONFLICT DO NOTHING
            """)
        )
        db.commit()
        print(f"Backfilled {result.rowcount} rows into assessment_history for tax_year=2026.")
    except Exception as exc:
        db.rollback()
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    backfill()
