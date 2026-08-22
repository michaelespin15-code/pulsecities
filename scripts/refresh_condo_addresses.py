"""
Nightly condo unit-lot address recovery.

17,065 deed BBLs are condo unit lots (lot 1001-7500) with no parcels row, so a
quarter of the deed record joins to no address and /property 404s on lots that
carry real ACRIS history. The unit lot shares its tax block with the condo's
billing lot (7501+), which PLUTO does carry. Where a block holds exactly one
billing-lot parcel and it has an address, every unit lot on the block inherits
it: 4,399 lots at first run. Blocks holding several condos are never guessed;
they wait for DOF's PAD mapping (source='pad'), which this refresh must not
overwrite — the delete touches only its own 'block_billing' rows and the
insert skips any BBL that already has a row.

Rebuilt from scratch nightly, after the 02:00 scrape and before the 03:15
sitemap, so a unit lot that PLUTO starts carrying drops out on its own and the
sitemap only ever lists what the route can render. Zero recovered rows is an
error, not a quiet success: the rule matched thousands on day one, so an empty
result means the deed feed or parcels went sideways upstream.

Usage:
    python -m scripts.refresh_condo_addresses
"""

import logging
import sys

from sqlalchemy import text

from models.database import get_scraper_db
from scheduler.alerts import send_ops_email

logger = logging.getLogger(__name__)

# Two statements, one transaction. A wCTE DELETE is invisible to the INSERT's
# ON CONFLICT check in the same statement, so folding them together makes the
# second run skip rows it just deleted. Readers see the swap atomically at
# commit either way.
_CLEAR_SQL = "DELETE FROM condo_unit_addresses WHERE source = 'block_billing'"

_REFRESH_SQL = """
    WITH unit_lots AS (
        SELECT DISTINCT o.bbl
        FROM ownership_raw o
        WHERE o.doc_type = 'DEED'
          AND substring(o.bbl, 7, 4) >= '1001' AND substring(o.bbl, 7, 4) < '7501'
          AND NOT EXISTS (SELECT 1 FROM parcels p WHERE p.bbl = o.bbl)
    ),
    blocks AS (
        SELECT substring(p.bbl, 1, 6) AS blk,
               max(p.bbl) AS billing_bbl,
               max(p.address) AS address,
               max(p.zip_code) AS zip_code
        FROM parcels p
        WHERE substring(p.bbl, 7, 4) >= '7501'
        GROUP BY substring(p.bbl, 1, 6)
        HAVING count(*) = 1 AND max(p.address) IS NOT NULL
    )
    INSERT INTO condo_unit_addresses
        (bbl, billing_bbl, address, zip_code, source, created_at, updated_at)
    SELECT u.bbl, b.billing_bbl, b.address, b.zip_code,
           'block_billing', now(), now()
    FROM unit_lots u
    JOIN blocks b ON b.blk = substring(u.bbl, 1, 6)
    ON CONFLICT (bbl) DO NOTHING
"""


def run(db, commit: bool = True) -> dict:
    """Rebuild the block-billing rows inside one transaction."""
    db.execute(text(_CLEAR_SQL))
    recovered = db.execute(text(_REFRESH_SQL)).rowcount
    if commit:
        db.commit()
    logger.info("condo address refresh: %d unit lots recovered", recovered)
    return {"recovered": recovered}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        with get_scraper_db() as db:
            result = run(db, commit=False)  # get_scraper_db commits on exit
        if result["recovered"] == 0:
            send_ops_email(
                "Condo address refresh recovered ZERO rows",
                "The block-billing rule recovered nothing. It matched 4,399 unit "
                "lots on day one, so an empty result means ownership_raw or "
                "parcels changed shape upstream. Unit-lot property pages will "
                "404 again once caches expire.",
            )
            return 1
        return 0
    except Exception as exc:
        logger.exception("condo address refresh failed")
        send_ops_email("Condo address refresh FAILED", f"{type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
