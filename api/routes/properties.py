"""
Property (BBL) level API endpoints.

GET /api/properties/{bbl}           — individual parcel with all raw signal data
GET /api/properties/search?address= — address → BBL resolution via NYC GeoSearch

These are the drill-down endpoints used when a user clicks a specific building
on the map. Returns raw signal counts (not scores) so users can see the data.
"""

import logging
import os

import requests
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from config.nyc import NOMINATIM_URL, NOMINATIM_USER_AGENT, SOCRATA_BASE_URL
from models.bbl import normalize_bbl
from models.complaints import ComplaintRaw
from models.database import get_db
from models.evictions import EvictionRaw
from models.ownership import OwnershipRaw
from models.permits import PermitRaw
from models.properties import Parcel
from models.scores import PropertyScore

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/properties", tags=["properties"])
limiter = Limiter(key_func=get_remote_address)

# How many months of raw signal data to return in the API response
SIGNAL_WINDOW_MONTHS = 12


@router.get("/search")
@limiter.limit("30/minute")
def search_by_address(
    request: Request,
    address: str = Query(..., min_length=5, description="Street address in NYC"),
    db: Session = Depends(get_db),
):
    """
    Resolves a street address to a BBL via NYC GeoSearch, then returns the parcel.
    """
    bbl = _geosearch_to_bbl(address)
    if not bbl:
        raise HTTPException(
            status_code=404,
            detail=f"Could not resolve address '{address}' to a NYC property. "
            "Try including borough (e.g. '123 Main St, Brooklyn').",
        )
    return _get_property_data(bbl, db)


@router.get("/{bbl}")
@limiter.limit("60/minute")
def get_property(request: Request, bbl: str, db: Session = Depends(get_db)):
    """
    Returns all available signal data for a specific BBL.
    """
    canonical_bbl = normalize_bbl(bbl)
    if not canonical_bbl:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid BBL format: '{bbl}'. Expected 10-digit string (e.g. 1000010001).",
        )
    return _get_property_data(canonical_bbl, db)


def _get_property_data(bbl: str, db: Session) -> dict:
    parcel = db.query(Parcel).filter(Parcel.bbl == bbl).first()
    score = db.query(PropertyScore).filter(PropertyScore.bbl == bbl).first()

    from datetime import datetime, timedelta, timezone
    since = datetime.now(timezone.utc) - timedelta(days=SIGNAL_WINDOW_MONTHS * 30)

    complaints = (
        db.query(ComplaintRaw)
        .filter(ComplaintRaw.bbl == bbl, ComplaintRaw.created_date >= since)
        .order_by(ComplaintRaw.created_date.desc())
        .limit(50)
        .all()
    )
    permits = (
        db.query(PermitRaw)
        .filter(PermitRaw.bbl == bbl, PermitRaw.filing_date >= since.date())
        .order_by(PermitRaw.filing_date.desc())
        .limit(20)
        .all()
    )
    evictions = (
        db.query(EvictionRaw)
        .filter(EvictionRaw.bbl == bbl, EvictionRaw.executed_date >= since.date())
        .order_by(EvictionRaw.executed_date.desc())
        .limit(20)
        .all()
    )
    ownership = (
        db.query(OwnershipRaw)
        .filter(OwnershipRaw.bbl == bbl)
        .order_by(OwnershipRaw.doc_date.desc())
        .limit(10)
        .all()
    )

    return {
        "bbl": bbl,
        "parcel": _serialize_parcel(parcel) if parcel else None,
        "displacement_score": (
            {"score": round(score.score, 1), "breakdown": score.signal_breakdown}
            if score and score.score is not None
            else None
        ),
        "signals": {
            "complaints_last_12mo": [
                {
                    "type": c.complaint_type,
                    "descriptor": c.descriptor,
                    "date": c.created_date.date().isoformat() if c.created_date else None,
                    "status": c.status,
                }
                for c in complaints
            ],
            "permits_last_12mo": [
                {
                    "type": c.permit_type,
                    "work_type": c.work_type,
                    "filed": c.filing_date.isoformat() if c.filing_date else None,
                    "description": c.job_description,
                }
                for c in permits
            ],
            "evictions_last_12mo": [
                {
                    "date": e.executed_date.isoformat() if e.executed_date else None,
                    "type": e.eviction_type,
                    "docket": e.docket_number,
                }
                for e in evictions
            ],
            "ownership_transfers": [
                {
                    "buyer": o.party_name_normalized or o.party_name,
                    "date": o.doc_date.isoformat() if o.doc_date else None,
                    "doc_type": o.doc_type,
                    "amount": str(o.doc_amount) if o.doc_amount else None,
                }
                for o in ownership
            ],
        },
    }


def _serialize_parcel(p: Parcel) -> dict:
    return {
        "address": p.address,
        "zip_code": p.zip_code,
        "borough": p.borough,
        "year_built": p.year_built,
        "units_residential": p.units_res,
        "units_total": p.units_total,
        "zoning": p.zoning_dist,
        "land_use": p.land_use,
        "owner": p.owner_name,
        "on_speculation_watch_list": p.on_speculation_watch_list,
    }


def _geosearch_to_bbl(address: str) -> str | None:
    """
    Resolve a street address to a BBL via two steps:
    1. Nominatim (OSM) geocodes address → lat/lon
    2. PLUTO SODA API finds nearest parcel → BBL

    NYC Planning GeoSearch (geosearch.planningitc.gov) is dead as of 2026.
    """
    lat, lon = _nominatim_geocode(address)
    if lat is None:
        return None
    return _pluto_bbl_from_coords(lat, lon)


def _nominatim_geocode(address: str) -> tuple[float | None, float | None]:
    """Return (lat, lon) for an address using OSM Nominatim, restricted to NYC."""
    try:
        resp = requests.get(
            NOMINATIM_URL,
            params={
                "q": address,
                "format": "json",
                "limit": 1,
                "countrycodes": "us",
                "viewbox": "-74.26,40.49,-73.70,40.92",  # NYC bounding box
                "bounded": 1,
            },
            headers={"User-Agent": NOMINATIM_USER_AGENT},
            timeout=5,
        )
        resp.raise_for_status()
        results = resp.json()
        if not results:
            return None, None
        return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        logger.warning("Nominatim geocode failed for '%s': %s", address, e)
        return None, None


def _pluto_bbl_from_coords(lat: float, lon: float) -> str | None:
    """Find the BBL of the parcel nearest to (lat, lon) via PLUTO SODA API."""
    try:
        app_token = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")
        delta = 0.0003  # ~30 metre bounding box
        resp = requests.get(
            f"{SOCRATA_BASE_URL}/64uk-42ks.json",
            params={
                "$where": (
                    f"latitude>{lat - delta} AND latitude<{lat + delta} "
                    f"AND longitude>{lon - delta} AND longitude<{lon + delta}"
                ),
                "$select": "bbl",
                "$limit": "1",
                "$$app_token": app_token,
            },
            timeout=8,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            return None
        bbl_raw = rows[0].get("bbl")
        # PLUTO returns BBL as float string e.g. "1001247502.00000000"
        if bbl_raw:
            bbl_raw = str(bbl_raw).split(".")[0]
        return normalize_bbl(bbl_raw) if bbl_raw else None
    except Exception as e:
        logger.warning("PLUTO BBL lookup failed for (%.6f, %.6f): %s", lat, lon, e)
        return None
