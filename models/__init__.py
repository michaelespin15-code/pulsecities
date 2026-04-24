"""
Models package — import all models here so Alembic autogenerate sees them.
"""

from models.base import Base, TimestampMixin, utcnow
from models.bbl import normalize_bbl, bbl_to_parts
from models.neighborhoods import Neighborhood
from models.properties import Parcel
from models.permits import PermitRaw
from models.complaints import ComplaintRaw
from models.violations import ViolationRaw
from models.evictions import EvictionRaw
from models.ownership import OwnershipRaw
from models.sales import SaleRaw
from models.scores import DisplacementScore, PropertyScore
from models.score_history import ScoreHistory
from models.scraper import ScraperRun, ScraperQuarantine
from models.subscribers import Subscriber
from models.dcwp_license import DcwpLicense
from models.dhcr_rs import RsBuilding
from models.mtek_alerts import MtekAlert
from models.operators import Operator, OperatorParcel

__all__ = [
    "Base",
    "TimestampMixin",
    "utcnow",
    "normalize_bbl",
    "bbl_to_parts",
    "Neighborhood",
    "Parcel",
    "PermitRaw",
    "ComplaintRaw",
    "ViolationRaw",
    "EvictionRaw",
    "OwnershipRaw",
    "SaleRaw",
    "DisplacementScore",
    "PropertyScore",
    "ScoreHistory",
    "ScraperRun",
    "ScraperQuarantine",
    "Subscriber",
    "DcwpLicense",
    "RsBuilding",
    "MtekAlert",
    "Operator",
    "OperatorParcel",
]
