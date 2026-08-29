"""
HPD registered responsible parties.

Datasets: tesw-yqqr (Registrations) and feu5-w2e2 (Registration Contacts).

Why this feed and not the deed parties. HPD requires an owner to register a
responsible party so tenants and the courts can reach them, and requires it only
of multiple dwellings and of one- and two-family homes the owner does not live
in. That requirement is the filter: 181,484 of 182,366 registered lots match a
parcel and 77.1% carry three or more residential units. ACRIS records that
someone bought a home and says nothing about whether they run one, which is why
the portfolio test on deed parties failed and /llc/{person-slug} came down.

Two datasets, one pass. Registrations carry the BBL and the building address;
contacts carry the people and their service address, keyed by registrationid.
The join happens here rather than in SQL because the whole reason to touch the
service address is to compare it and throw it away.

What is deliberately NOT stored: the service address. 19.7% of IndividualOwner
contacts give the registered building as their business address, so for roughly
23,000 people that field is a home address. It is compared during ingest and
discarded; only the boolean survives. api/owner_disclosure.py owns the rule that
turns that boolean into a verdict.

No watermark. HPD restates registrations rather than appending, and the contact
rows carry no date of their own, so this is a full refresh keyed on
registrationcontactid.
"""
import logging
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from api.owner_disclosure import is_publishable
from models.bbl import normalize_bbl
from models.database import get_scraper_db
from models.hpd_registration import HpdOwnerContact
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

DATASET_ID = "tesw-yqqr"
CONTACTS_DATASET_ID = "feu5-w2e2"

# Roles worth publishing at all. SiteManager is a building employee rather than
# an owner or an agent of one, so it is left out: naming the super does not tell
# a tenant who is responsible, and it names someone with no ownership interest.
ROLES = ("HeadOfficer", "IndividualOwner", "JointOwner", "CorporateOwner",
         "Agent", "Officer", "Shareholder")


def _bbl_from_parts(row: dict) -> str | None:
    """boroid + block + lot -> canonical 10-digit BBL."""
    try:
        return normalize_bbl(
            f"{int(row['boroid'])}{int(row['block']):05d}{int(row['lot']):04d}")
    except (KeyError, TypeError, ValueError):
        return None


def _norm_addr(house: str | None, street: str | None) -> str:
    return " ".join(f"{(house or '').strip()} {(street or '').strip()}".upper().split())


def _person_name(row: dict) -> str:
    parts = [(row.get("firstname") or "").strip(),
             (row.get("middleinitial") or "").strip(),
             (row.get("lastname") or "").strip()]
    return " ".join(p for p in parts if p)


class HpdRegistrationScraper(BaseScraper):
    """Full refresh of registered responsible parties, gated at load time."""

    SCRAPER_NAME = "hpd_registrations"
    DATASET_ID = DATASET_ID
    INITIAL_LOOKBACK_DAYS = 0  # unused, no date watermark on this feed

    # 194k registrations and 759k contacts on a 4GB box. 5k pages keep peak
    # in-flight memory near 5MB rather than 50MB, same reason as scrapers/dof.py.
    PAGE_SIZE = 5_000

    def _run(self, db) -> tuple[int, int, datetime | None]:
        # --- pass 1: registrations, for the BBL and the building's own address
        # The registrations feed carries boroid/block/lot, not a made-up bbl
        # column. models/bbl.py owns the padding rule; the copy of this data in
        # the violation_leads database has a derived bbl column and this feed
        # does not, which is worth knowing before trusting one for the other.
        buildings: dict[str, tuple[str, str, str | None]] = {}
        for raw in self.paginate("boroid IS NOT NULL"):
            reg = (raw.get("registrationid") or "").strip()
            bbl = _bbl_from_parts(raw)
            if not reg or not bbl:
                continue
            buildings[reg] = (bbl,
                              _norm_addr(raw.get("housenumber"), raw.get("streetname")),
                              (raw.get("registrationenddate") or "")[:10] or None)
        logger.info("%s: %d registrations", self.SCRAPER_NAME, len(buildings))
        if not buildings:
            return 0, 0, None

        # Residential unit counts decide the owner-occupant case. Only the
        # registered lots are needed, so this is not a full parcels scan.
        units = dict(db.execute(text(
            "SELECT bbl, COALESCE(units_res, 0) FROM parcels WHERE bbl = ANY(:b)"),
            {"b": [v[0] for v in buildings.values()]}).fetchall())

        # --- pass 2: contacts. Same paginator, other dataset.
        self.base_url = self.base_url.replace(DATASET_ID, CONTACTS_DATASET_ID)
        roles = ",".join(f"'{r}'" for r in ROLES)

        processed = failed = withheld = 0
        batch: list[dict] = []
        for raw in self.paginate(f"type in ({roles})"):
            row = self._parse(raw, buildings, units)
            if row is None:
                failed += 1
                continue
            if not row["publishable"]:
                withheld += 1
            batch.append(row)
            if len(batch) >= 2_000:
                processed += self._upsert(db, batch)
                batch = []
        if batch:
            processed += self._upsert(db, batch)

        logger.info(
            "%s: %d contacts loaded, %d withheld by the disclosure gate, %d unusable",
            self.SCRAPER_NAME, processed, withheld, failed,
        )
        return processed, failed, None

    def _parse(self, raw: dict, buildings: dict, units: dict) -> dict | None:
        cid = (raw.get("registrationcontactid") or "").strip()
        reg = (raw.get("registrationid") or "").strip()
        if not cid or reg not in buildings:
            return None

        bbl, bldg_addr, end_date = buildings[reg]
        corp = (raw.get("corporationname") or "").strip()
        name = corp or _person_name(raw)
        if not name:
            return None

        at_building = bool(bldg_addr) and _norm_addr(
            raw.get("businesshousenumber"), raw.get("businessstreetname")) == bldg_addr

        return {
            "registration_contact_id": cid,
            "registration_id": reg,
            "bbl": bbl,
            "role": (raw.get("type") or "").strip()[:32],
            "is_organization": bool(corp),
            "name": name,
            "at_building": at_building,
            "publishable": is_publishable(
                is_organization=bool(corp),
                at_building=at_building,
                units_res=units.get(bbl),
            ),
            "registration_end_date": end_date,
            # The service address ends here. It is never put in the row.
        }

    def _upsert(self, db, batch: list[dict]) -> int:
        deduped = {r["registration_contact_id"]: r for r in batch}
        stmt = insert(HpdOwnerContact).values(list(deduped.values()))
        stmt = stmt.on_conflict_do_update(
            constraint="uq_hpd_owner_contact",
            set_={
                "bbl": stmt.excluded.bbl,
                "role": stmt.excluded.role,
                "is_organization": stmt.excluded.is_organization,
                "name": stmt.excluded.name,
                "at_building": stmt.excluded.at_building,
                "publishable": stmt.excluded.publishable,
                "registration_end_date": stmt.excluded.registration_end_date,
                # The column that says when the row was last correct has to move
                # with it; tests/test_upsert_timestamps.py greps for the next
                # writer that forgets.
                "updated_at": text("now()"),
            },
        )
        result = db.execute(stmt)
        db.commit()
        return result.rowcount


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with get_scraper_db() as db:
        run = HpdRegistrationScraper().run(db)
        print(f"Status: {run.status} | Processed: {run.records_processed}")
