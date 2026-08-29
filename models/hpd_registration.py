"""
Registered responsible parties per building, with the disclosure gate already
applied and the service address deliberately absent.

There is no address column here and that is the point. 19.7% of HPD's
IndividualOwner contacts give the registered building as their business
address, so for roughly 23,000 people that field is a home address. A column
holding it is a column something eventually selects, which is exactly how
/property came to print private buyers' names on 43,212 pages. The comparison
happens once, during ingest, and only the boolean survives.

`publishable` is api/owner_disclosure.py's verdict, computed at load time and
stored, so no reader has to remember the rule or re-derive it.
"""
from sqlalchemy import Boolean, Date, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, TimestampMixin


class HpdOwnerContact(TimestampMixin, Base):
    __tablename__ = "hpd_owner_contacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # HPD's own key for the contact row, so a re-run corrects rather than duplicates.
    registration_contact_id: Mapped[str] = mapped_column(String(32), nullable=False)
    registration_id: Mapped[str | None] = mapped_column(String(32), nullable=True)

    bbl: Mapped[str] = mapped_column(String(10), nullable=False)

    # HeadOfficer, IndividualOwner, JointOwner, CorporateOwner, Agent, Officer, Shareholder
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    is_organization: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # A corporation name, or "First Last". Never an address.
    name: Mapped[str] = mapped_column(Text, nullable=False)

    # Did the registered service address match the building itself? The address
    # that answered this is not retained.
    at_building: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # api/owner_disclosure.py's verdict, decided at ingest.
    publishable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    registration_end_date: Mapped[object | None] = mapped_column(Date, nullable=True)

    __table_args__ = (
        UniqueConstraint("registration_contact_id", name="uq_hpd_owner_contact"),
        Index("idx_hpd_owner_contacts_bbl", "bbl"),
        Index("idx_hpd_owner_contacts_name", "name"),
        Index("idx_hpd_owner_contacts_publishable", "publishable"),
    )
