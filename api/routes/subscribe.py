"""
POST /api/subscribe — ZIP-based or citywide watch subscription.

ZIP-based: { email, zip_code } — watches a specific neighborhood.
Citywide:  { email, is_citywide: true } — watches all of NYC.
"""

import logging
import os
import re

import resend
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, field_validator, model_validator
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
<title>You're subscribed to PulseCities</title>
</head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Inter',system-ui,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:48px 24px;">
    <tr>
      <td align="center">
        <table width="100%" cellpadding="0" cellspacing="0" style="max-width:520px;">
          <tr>
            <td style="padding-bottom:32px;">
              <span style="font-family:'JetBrains Mono',monospace;font-size:18px;font-weight:600;color:#38bdf8;letter-spacing:-0.01em;">PulseCities</span>
            </td>
          </tr>
          <tr>
            <td style="background:#1e293b;border-radius:12px;padding:32px;border:1px solid rgba(148,163,184,0.1);">
              <p style="margin:0 0 8px;font-size:20px;font-weight:600;color:#f1f5f9;">You're watching {zip_code}.</p>
              <p style="margin:0 0 24px;font-size:14px;color:#94a3b8;line-height:1.6;">
                You'll get a weekly digest of displacement activity in <strong style="color:#cbd5e1;">{zip_code}</strong>. First one goes out {send_day}.
              </p>
              <a href="https://pulsecities.com/neighborhood/{zip_code}"
                 style="display:inline-block;background:#f97316;color:#fff;font-size:13px;font-weight:600;padding:10px 20px;border-radius:6px;text-decoration:none;">
                View {zip_code} now
              </a>
            </td>
          </tr>
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

_CITYWIDE_CONFIRMATION_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>You're subscribed to PulseCities</title>
</head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Inter',system-ui,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:48px 24px;">
    <tr>
      <td align="center">
        <table width="100%" cellpadding="0" cellspacing="0" style="max-width:520px;">
          <tr>
            <td style="padding-bottom:32px;">
              <span style="font-family:'JetBrains Mono',monospace;font-size:18px;font-weight:600;color:#38bdf8;letter-spacing:-0.01em;">PulseCities</span>
            </td>
          </tr>
          <tr>
            <td style="background:#1e293b;border-radius:12px;padding:32px;border:1px solid rgba(148,163,184,0.1);">
              <p style="margin:0 0 8px;font-size:20px;font-weight:600;color:#f1f5f9;">You're watching NYC.</p>
              <p style="margin:0 0 24px;font-size:14px;color:#94a3b8;line-height:1.6;">
                You'll get a weekly overview of displacement activity across all NYC neighborhoods. First one goes out {send_day}.
              </p>
              <a href="https://pulsecities.com"
                 style="display:inline-block;background:#f97316;color:#fff;font-size:13px;font-weight:600;padding:10px 20px;border-radius:6px;text-decoration:none;">
                Explore the map
              </a>
            </td>
          </tr>
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

_UNSUBSCRIBE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Unsubscribed from PulseCities</title>
</head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Inter',system-ui,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:48px 24px;">
    <tr>
      <td align="center">
        <table width="100%" cellpadding="0" cellspacing="0" style="max-width:520px;">
          <tr>
            <td style="padding-bottom:32px;">
              <span style="font-family:'JetBrains Mono',monospace;font-size:18px;font-weight:600;color:#38bdf8;letter-spacing:-0.01em;">PulseCities</span>
            </td>
          </tr>
          <tr>
            <td style="background:#1e293b;border-radius:12px;padding:32px;border:1px solid rgba(148,163,184,0.1);">
              <p style="margin:0 0 8px;font-size:20px;font-weight:600;color:#f1f5f9;">You're unsubscribed.</p>
              <p style="margin:0 0 24px;font-size:14px;color:#94a3b8;line-height:1.6;">
                You won't receive any more weekly digests from PulseCities.
              </p>
              <a href="https://pulsecities.com"
                 style="display:inline-block;background:#f97316;color:#fff;font-size:13px;font-weight:600;padding:10px 20px;border-radius:6px;text-decoration:none;">
                Back to PulseCities
              </a>
            </td>
          </tr>
          <tr>
            <td style="padding-top:24px;">
              <p style="margin:0;font-size:11px;color:rgba(148,163,184,0.4);line-height:1.6;">
                PulseCities tracks displacement pressure across all NYC neighborhoods using public records.
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


class SubscribeRequest(BaseModel):
    email: str
    zip_code: str | None = None
    is_citywide: bool = False

    @field_validator('email')
    @classmethod
    def validate_email(cls, v: str) -> str:
        v = v.strip().lower()
        if not _EMAIL_RE.match(v) or len(v) > 254:
            raise ValueError('invalid email')
        return v

    @model_validator(mode='after')
    def validate_zip_or_citywide(self) -> 'SubscribeRequest':
        if self.is_citywide:
            self.zip_code = None
            return self
        if not self.zip_code:
            raise ValueError('zip_code required when not subscribing citywide')
        self.zip_code = self.zip_code.strip()
        if not _ZIP_RE.match(self.zip_code):
            raise ValueError('zip_code must be 5 digits')
        return self


def _send_confirmation(email: str, zip_code: str | None, is_citywide: bool) -> None:
    if not resend.api_key:
        logger.warning("RESEND_API_KEY not set — skipping confirmation email")
        return
    try:
        if is_citywide:
            resend.Emails.send({
                "from": "PulseCities <alerts@pulsecities.com>",
                "to": [email],
                "subject": "You're watching NYC — PulseCities",
                "html": _CITYWIDE_CONFIRMATION_HTML.replace("{send_day}", DIGEST_SEND_DAY),
            })
            logger.info("Citywide confirmation sent to %s", email)
        else:
            resend.Emails.send({
                "from": "PulseCities <alerts@pulsecities.com>",
                "to": [email],
                "subject": f"You're watching {zip_code} — PulseCities",
                "html": _CONFIRMATION_HTML.replace("{zip_code}", zip_code).replace("{send_day}", DIGEST_SEND_DAY),
            })
            logger.info("Confirmation sent to %s for zip %s", email, zip_code)
    except Exception:
        logger.exception("Failed to send confirmation email to %s", email)


@router.post('/subscribe', status_code=status.HTTP_201_CREATED)
@limiter.limit('10/minute')
def subscribe(
    request: Request,
    response: Response,
    body: SubscribeRequest,
    db: Session = Depends(get_db),
):
    if body.is_citywide:
        existing = db.execute(
            select(Subscriber).where(
                Subscriber.email == body.email,
                Subscriber.is_citywide == True,
            )
        ).scalar_one_or_none()
        if existing:
            raise HTTPException(status_code=409, detail='Already watching NYC-wide.')
        sub = Subscriber(email=body.email, zip_code=None, is_citywide=True)
    else:
        existing = db.execute(
            select(Subscriber).where(
                Subscriber.email == body.email,
                Subscriber.zip_code == body.zip_code,
                Subscriber.is_citywide == False,
            )
        ).scalar_one_or_none()
        if existing:
            raise HTTPException(status_code=409, detail='Already watching this area.')
        sub = Subscriber(email=body.email, zip_code=body.zip_code, is_citywide=False)

    db.add(sub)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        if body.is_citywide:
            raise HTTPException(status_code=409, detail='Already watching NYC-wide.')
        raise HTTPException(status_code=409, detail='Already watching this area.')

    logger.info('New subscriber email=%s zip=%s citywide=%s', body.email, body.zip_code, body.is_citywide)
    _send_confirmation(body.email, body.zip_code, body.is_citywide)
    return {'status': 'ok'}


@router.get('/unsubscribe', response_class=HTMLResponse)
def unsubscribe(token: str, db: Session = Depends(get_db)):
    """One-click unsubscribe — linked from every digest email footer."""
    sub = db.execute(
        select(Subscriber).where(Subscriber.unsubscribe_token == token)
    ).scalar_one_or_none()

    if not sub:
        raise HTTPException(status_code=404, detail='Invalid or expired unsubscribe link.')

    email    = sub.email
    zip_code = sub.zip_code
    citywide = sub.is_citywide
    db.delete(sub)
    db.commit()
    logger.info('Unsubscribed: %s (zip=%s citywide=%s)', email, zip_code, citywide)
    return HTMLResponse(content=_UNSUBSCRIBE_HTML, status_code=200)
