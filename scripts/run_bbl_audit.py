"""
BBL normalization audit — one-shot, read-only.
Detects format variants across all tables that store a bbl column.
Writes results to scripts/bbl_normalization_audit.json.
"""

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://pulsecities_user:PCities2026NY@localhost/pulsecities",
)

TABLES = [
    "parcels",
    "ownership_raw",
    "complaints_raw",
    "violations_raw",
    "permits_raw",
    "evictions_raw",
    "sales_raw",
    "dcwp_licenses",
    "rs_buildings",
    "property_scores",
    "mtek_alerts",
]

# Canonical: exactly 10 ASCII digits, first char is 1-5
CANONICAL_RE = r"^\d{10}$"


def audit_table(conn, table: str) -> dict:
    # Confirm table and column exist
    exists = conn.execute(
        text(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_name = :t AND column_name = 'bbl'
            """
        ),
        {"t": table},
    ).scalar()
    if not exists:
        return {"skipped": True, "reason": "table or column does not exist"}

    total_rows = conn.execute(
        text(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
    ).scalar()

    non_null = conn.execute(
        text(f"SELECT COUNT(*) FROM {table} WHERE bbl IS NOT NULL")  # noqa: S608
    ).scalar()

    unique_bbls = conn.execute(
        text(f"SELECT COUNT(DISTINCT bbl) FROM {table} WHERE bbl IS NOT NULL")  # noqa: S608
    ).scalar()

    # Rows where bbl exists but doesn't match canonical 10-digit format
    non_canonical = conn.execute(
        text(
            f"""
            SELECT COUNT(*) FROM {table}
            WHERE bbl IS NOT NULL
              AND bbl !~ '{CANONICAL_RE}'
            """
        )
    ).scalar()

    # Canonical-looking but invalid borough code (digit 0 or 6-9)
    invalid_borough = conn.execute(
        text(
            f"""
            SELECT COUNT(*) FROM {table}
            WHERE bbl IS NOT NULL
              AND bbl ~ '{CANONICAL_RE}'
              AND LEFT(bbl, 1) NOT IN ('1','2','3','4','5')
            """
        )
    ).scalar()

    total_bad = non_canonical + invalid_borough

    # Inflation: bad rows inflate row count because they can't join to parcels
    inflation_pct = round(total_bad / non_null * 100, 4) if non_null else 0.0

    # Sample top-10 most common non-canonical values with counts
    variants_rows = conn.execute(
        text(
            f"""
            SELECT bbl, COUNT(*) AS cnt
            FROM {table}
            WHERE bbl IS NOT NULL
              AND (
                bbl !~ '{CANONICAL_RE}'
                OR LEFT(bbl, 1) NOT IN ('1','2','3','4','5')
              )
            GROUP BY bbl
            ORDER BY cnt DESC
            LIMIT 10
            """
        )
    ).fetchall()

    variants = [{"bbl": r[0], "count": r[1]} for r in variants_rows]

    return {
        "total_rows": total_rows,
        "non_null_bbl_rows": non_null,
        "unique_bbls": unique_bbls,
        "non_canonical_rows": non_canonical,
        "invalid_borough_rows": invalid_borough,
        "total_bad_rows": total_bad,
        "inflation_pct": inflation_pct,
        "top_variants": variants,
    }


def main():
    engine = create_engine(DATABASE_URL)
    results = {}

    with engine.connect() as conn:
        for table in TABLES:
            print(f"  auditing {table}...", flush=True)
            results[table] = audit_table(conn, table)

    # Summary
    summary = {
        "tables_audited": len(TABLES),
        "tables_with_issues": sum(
            1
            for r in results.values()
            if not r.get("skipped") and r.get("total_bad_rows", 0) > 0
        ),
        "max_inflation_pct": max(
            (r.get("inflation_pct", 0) for r in results.values() if not r.get("skipped")),
            default=0.0,
        ),
        "normalization_needed": any(
            r.get("inflation_pct", 0) > 1.0
            for r in results.values()
            if not r.get("skipped")
        ),
    }

    import datetime
    output = {
        "meta": {
            "run_date": datetime.date.today().isoformat(),
            "canonical_format": "10-digit zero-padded string (BBBBBBBBBLL), first digit 1-5",
            "tables_checked": TABLES,
        },
        "summary": summary,
        "tables": results,
    }

    out_path = Path(__file__).parent / "bbl_normalization_audit.json"
    out_path.write_text(json.dumps(output, indent=2))
    print(f"\nWrote {out_path}")
    print(f"Summary: {json.dumps(summary, indent=2)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
