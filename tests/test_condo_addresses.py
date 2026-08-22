"""
Condo unit-lot address recovery — refresh script and route integration tests.

17,065 deed BBLs are condo unit lots (lot 1001–7500) with no parcels row, so
/property/{bbl} 404s on a quarter of the deed record and every deed table that
links one of those BBLs points at a dead page. scripts/refresh_condo_addresses.py
recovers addresses for the unambiguous subset: where a tax block holds exactly
one condo billing lot (7501+) and that lot carries an address, every unit lot
on the block inherits it. Blocks holding several condos are never guessed; they
wait for DOF's PAD mapping (source='pad'), which wins over the inference.

The refresh tests run the script's core against an uncommitted session and roll
back, so the suite never commits to the live table (the score_history lesson).
The route test finds its subject through its own recovery query, not through
the table, so it fails honestly while the table is still empty rather than
skipping.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from models.database import SessionLocal

pytestmark = pytest.mark.integration

# The rule under test, stated independently of the implementation: deed unit
# lots with no parcels row, on blocks where exactly one billing-lot parcel
# exists and it carries an address.
_ORACLE_SQL = """
    WITH unit_lots AS (
        SELECT DISTINCT o.bbl
        FROM ownership_raw o
        WHERE o.doc_type = 'DEED'
          AND substring(o.bbl, 7, 4) >= '1001' AND substring(o.bbl, 7, 4) < '7501'
          AND NOT EXISTS (SELECT 1 FROM parcels p WHERE p.bbl = o.bbl)
    ),
    blocks AS (
        SELECT substring(p.bbl, 1, 6) AS blk,
               max(p.bbl) AS billing_bbl, max(p.address) AS address
        FROM parcels p
        WHERE substring(p.bbl, 7, 4) >= '7501'
        GROUP BY substring(p.bbl, 1, 6)
        HAVING count(*) = 1 AND max(p.address) IS NOT NULL
    )
    SELECT u.bbl, b.billing_bbl, b.address
    FROM unit_lots u JOIN blocks b ON b.blk = substring(u.bbl, 1, 6)
"""


@pytest.fixture()
def db():
    session = SessionLocal()
    yield session
    session.rollback()
    session.close()


@pytest.fixture(scope="module")
def client():
    from api.main import app
    return TestClient(app)


def test_refresh_recovers_unit_lots_with_valid_provenance(db):
    from scripts.refresh_condo_addresses import run

    result = run(db, commit=False)
    assert result["recovered"] > 0

    bad = db.execute(text("""
        SELECT count(*) FROM condo_unit_addresses c
        WHERE c.source = 'block_billing'
          AND (substring(c.bbl, 7, 4) < '1001'
               OR substring(c.bbl, 7, 4) >= '7501'
               OR substring(c.billing_bbl, 7, 4) < '7501'
               OR substring(c.billing_bbl, 1, 6) <> substring(c.bbl, 1, 6)
               OR c.address = ''
               OR EXISTS (SELECT 1 FROM parcels p WHERE p.bbl = c.bbl))
    """)).scalar()
    assert bad == 0


def test_refresh_never_guesses_on_multi_condo_blocks(db):
    from scripts.refresh_condo_addresses import run

    run(db, commit=False)
    # Range form, not substring equality, so the parcels bbl index carries the
    # per-block probe; the equality form seq-scanned parcels once per row.
    guessed = db.execute(text("""
        SELECT count(*) FROM condo_unit_addresses c
        WHERE c.source = 'block_billing'
          AND (SELECT count(*) FROM parcels p
               WHERE p.bbl >= substring(c.bbl, 1, 6) || '7501'
                 AND p.bbl <= substring(c.bbl, 1, 6) || '9999') <> 1
    """)).scalar()
    assert guessed == 0


def test_refresh_matches_the_stated_rule(db):
    from scripts.refresh_condo_addresses import run

    result = run(db, commit=False)
    oracle = db.execute(text(f"SELECT count(*) FROM ({_ORACLE_SQL}) q")).scalar()
    stored = db.execute(text(
        "SELECT count(*) FROM condo_unit_addresses WHERE source = 'block_billing'"
    )).scalar()
    assert result["recovered"] == oracle
    assert stored == oracle


def test_pad_rows_survive_refresh(db):
    from scripts.refresh_condo_addresses import run

    subject = db.execute(text(f"{_ORACLE_SQL} LIMIT 1")).fetchone()
    assert subject is not None, "no recoverable unit lot in the live data"

    db.execute(text("DELETE FROM condo_unit_addresses WHERE bbl = :bbl"),
               {"bbl": subject.bbl})
    db.execute(text("""
        INSERT INTO condo_unit_addresses
            (bbl, billing_bbl, address, zip_code, source, created_at, updated_at)
        VALUES (:bbl, :billing, 'PAD AUTHORITATIVE ADDRESS', NULL, 'pad', now(), now())
    """), {"bbl": subject.bbl, "billing": subject.billing_bbl})

    run(db, commit=False)
    kept = db.execute(text(
        "SELECT address, source FROM condo_unit_addresses WHERE bbl = :bbl"
    ), {"bbl": subject.bbl}).fetchone()
    assert kept.source == "pad"
    assert kept.address == "PAD AUTHORITATIVE ADDRESS"


def test_refresh_is_idempotent(db):
    from scripts.refresh_condo_addresses import run

    first = run(db, commit=False)
    second = run(db, commit=False)
    assert first["recovered"] == second["recovered"]

    dupes = db.execute(text(
        "SELECT count(*) - count(DISTINCT bbl) FROM condo_unit_addresses"
    )).scalar()
    assert dupes == 0


def test_property_page_renders_recovered_unit_lot(client, db):
    # The subject comes from the recovery rule, not from the table, so this
    # fails (404) until the route falls back AND the nightly refresh has
    # actually committed rows — which is the deployed state under test.
    subject = db.execute(text(f"{_ORACLE_SQL} LIMIT 1")).fetchone()
    assert subject is not None, "no recoverable unit lot in the live data"

    resp = client.get(f"/property/{subject.bbl}")
    assert resp.status_code == 200

    html = resp.text
    # Building address present, unit-lot marker in the title so sibling lots
    # and the billing lot's own page don't share one title.
    assert subject.address.title().split()[-1] in html
    assert "unit lot" in html.lower()
    assert 'content="index, follow"' in html  # it has a deed by construction


def test_property_page_still_404s_unknown_bbl(client):
    assert client.get("/property/9999999999").status_code == 404
