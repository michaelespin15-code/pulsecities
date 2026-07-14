"""
Guard: the neighborhood page shows a "recent renovation flips" section only when
the ZIP actually has flips, so quiet ZIPs stay lean (no thin/doorway content).
Data-driven against the live dataset, like test_operator_class_gate.
"""

import warnings

import pytest
from sqlalchemy import text
from fastapi.testclient import TestClient

from api.main import app
from models.database import SessionLocal

warnings.filterwarnings("ignore")
client = TestClient(app)

_FLIP_ZIPS_SQL = text("""
    WITH llc AS (
        SELECT o.bbl, o.doc_date td, p.zip_code
        FROM ownership_raw o JOIN parcels p ON p.bbl = o.bbl
        WHERE o.party_name_normalized LIKE '%LLC%'
          AND o.doc_type IN ('DEED','DEEDP','ASST') AND o.party_type='2'
          AND o.doc_date >= CURRENT_DATE - INTERVAL '365 days' AND p.zip_code IS NOT NULL
    ),
    rp AS (
        SELECT bbl, MIN(filing_date) fp FROM permits_raw
        WHERE raw_data->>'job_type' IN ('A1','A2')
          AND filing_date >= CURRENT_DATE - INTERVAL '365 days' GROUP BY bbl
    )
    SELECT DISTINCT l.zip_code FROM llc l JOIN rp r ON r.bbl = l.bbl
    WHERE r.fp > l.td AND (r.fp - l.td) <= 60
""")

_LABEL = "Recent renovation flips"


def _flip_zips():
    db = SessionLocal()
    try:
        return {r.zip_code for r in db.execute(_FLIP_ZIPS_SQL).fetchall()}
    finally:
        db.close()


def test_section_present_for_a_zip_with_flips():
    zips = _flip_zips()
    if not zips:
        pytest.skip("no renovation flips in current data")
    z = sorted(zips)[0]
    body = client.get(f"/neighborhood/{z}").text
    assert _LABEL in body, f"{z} has flips but the section is missing"
    assert "/property/" in body.split(_LABEL, 1)[1], "flip rows must link to /property"


def test_section_absent_for_a_zip_without_flips():
    flip = _flip_zips()
    db = SessionLocal()
    try:
        scored = [
            r.zip_code for r in db.execute(text(
                "SELECT zip_code FROM displacement_scores WHERE score IS NOT NULL ORDER BY zip_code"
            )).fetchall()
        ]
    finally:
        db.close()
    no_flip = next((z for z in scored if z not in flip), None)
    if no_flip is None:
        pytest.skip("every scored ZIP has flips (unexpected)")
    body = client.get(f"/neighborhood/{no_flip}").text
    assert body.count(_LABEL) == 0, f"{no_flip} has no flips but rendered the section"
