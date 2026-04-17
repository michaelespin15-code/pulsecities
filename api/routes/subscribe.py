"""
Email subscription endpoint.
POST /api/subscribe — save email + zip_code to the subscribers table.
"""

import logging
import re

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, field_validator
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from models.database import get_db
from models.subscribers import Subscriber

logger = logging.getLogger(__name__)
router = APIRouter(tags=["subscribers"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)

_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
_ZIP_RE   = re.compile(r'^\d{5}$')


class SubscribeRequest(BaseModel):
    email: str
    zip_code: str

    @field_validator('email')
    @classmethod
    def validate_email(cls, v: str) -> str:
        v = v.strip().lower()
        if not _EMAIL_RE.match(v) or len(v) > 254:
            raise ValueError('invalid email')
        return v

    @field_validator('zip_code')
    @classmethod
    def validate_zip(cls, v: str) -> str:
        v = v.strip()
        if not _ZIP_RE.match(v):
            raise ValueError('zip_code must be 5 digits')
        return v


@router.post('/subscribe', status_code=status.HTTP_201_CREATED)
@limiter.limit('10/minute')
def subscribe(
    request: Request,
    response: Response,
    body: SubscribeRequest,
    db: Session = Depends(get_db),
):
    """
    Record an email subscription for a zip code's weekly digest.
    Returns 201 on success, 409 if (email, zip_code) pair already exists.
    """
    existing = db.execute(
        select(Subscriber).where(
            Subscriber.email == body.email,
            Subscriber.zip_code == body.zip_code,
        )
    ).scalar_one_or_none()

    if existing:
        raise HTTPException(status_code=409, detail='Already subscribed for this zip code.')

    sub = Subscriber(email=body.email, zip_code=body.zip_code)
    db.add(sub)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail='Already subscribed for this zip code.')

    logger.info('New subscriber zip=%s', body.zip_code)
    return {'status': 'ok'}
