"""
Email subscription endpoint.
POST /api/subscribe — save email + zip_code to the subscribers table,
then send a confirmation email via Resend.
"""

import logging
import os
import re

import resend
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, field_validator
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from config.schedule import DIGEST_SEND_DAY
from models.database import get_db
from models.subscribers import Subscriber

logger = logging.getLogger(__name__)
router = APIRouter(tags=["subscribers"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)

resend.api_key = os.getenv("RESEND_API_KEY", "")

_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
_ZIP_RE   = re.compile(r'^\d{5}$')

_CONFIRMATION_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>You're subscribed — PulseCities</title>
</head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Inter',system-ui,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:48px 24px;">
    <tr>
      <td align="center">
        <table width="100%" cellpadding="0" cellspacing="0" style="max-width:520px;">

          <!-- Header -->
          <tr>
            <td style="padding-bottom:32px;">
              <span style="font-family:'JetBrains Mono',monospace;font-size:18px;font-weight:600;color:#38bdf8;letter-spacing:-0.01em;">PulseCities</span>
            </td>
          </tr>

          <!-- Body -->
          <tr>
            <td style="background:#1e293b;border-radius:12px;padding:32px;border:1px solid rgba(148,163,184,0.1);">
              <p style="margin:0 0 8px;font-size:20px;font-weight:600;color:#f1f5f9;">You're in for {zip_code}.</p>
              <p style="margin:0 0 24px;font-size:14px;color:#94a3b8;line-height:1.6;">
                You'll get a weekly digest of displacement activity in <strong style="color:#cbd5e1;">{zip_code}</strong> — score changes, new LLC acquisitions, permit spikes, and eviction trends. First one goes out {send_day}.
              </p>
              <a href="https://pulsecities.com/neighborhood/{zip_code}"
                 style="display:inline-block;background:#f97316;color:#fff;font-size:13px;font-weight:600;padding:10px 20px;border-radius:6px;text-decoration:none;">
                View {zip_code} now
              </a>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding-top:24px;">
              <p style="margin:0;font-size:11px;color:rgba(148,163,184,0.4);line-height:1.6;">
                PulseCities tracks displacement pressure across all NYC neighborhoods using public records.<br>
                You subscribed at pulsecities.com. To unsubscribe, reply with "unsubscribe".
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>
""".strip()


def _send_confirmation(email: str, zip_code: str) -> None:
    if not resend.api_key:
        logger.warning("RESEND_API_KEY not set — skipping confirmation email")
        return
    try:
        resend.Emails.send({
            "from": "PulseCities <alerts@pulsecities.com>",
            "to": [email],
            "subject": f"You're subscribed to {zip_code} — PulseCities",
            "html": _CONFIRMATION_HTML.replace("{zip_code}", zip_code).replace("{send_day}", DIGEST_SEND_DAY),
        })
        logger.info("Confirmation email sent to %s for zip %s", email, zip_code)
    except Exception:
        logger.exception("Failed to send confirmation email to %s", email)


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
    Sends a confirmation email via Resend on first subscription.
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
    _send_confirmation(body.email, body.zip_code)
    return {'status': 'ok'}
