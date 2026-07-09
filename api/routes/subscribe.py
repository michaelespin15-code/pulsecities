"""
POST /api/subscribe — ZIP-based or citywide watch subscription.

ZIP-based: { email, zip_code } — watches a specific neighborhood.
Citywide:  { email, is_citywide: true } — watches all of NYC.
"""

import logging
import os
import re
from datetime import datetime, timezone

import resend
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, field_validator, model_validator
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import select, text
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
_SLUG_RE  = re.compile(r'^[a-z0-9-]+$')

# Welcome notes wear the same paper case-file system as the weekly digest, but
# stay transactional where it counts for Gmail's tab classifier: no images, no
# CTA buttons, a plain-text part, low link count, one-click unsubscribe. The
# sheet, masthead, and file line are just inline CSS; classifiers don't weigh
# background colors, they weigh campaign apparatus.
_WELCOME_SHELL = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
</head>
<body style="margin:0;padding:0;background:#EFEBE2;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#EFEBE2;padding:36px 16px;">
    <tr><td align="center">
      <table width="100%" cellpadding="0" cellspacing="0" style="max-width:540px;">

        <tr><td style="background:#FBFAF7;border:1px solid #D9D4C9;border-top:3px solid #E4590F;padding:28px 28px 26px;">
          <table width="100%" cellpadding="0" cellspacing="0">

            <tr><td style="padding-bottom:10px;border-bottom:2px solid #1C2430;">
              <table width="100%" cellpadding="0" cellspacing="0"><tr>
                <td style="font-family:Menlo,Consolas,'Courier New',monospace;font-size:14px;font-weight:700;color:#1C2430;letter-spacing:0.2em;">PULSECITIES</td>
                <td align="right" style="font-family:Menlo,Consolas,'Courier New',monospace;font-size:10px;color:#6D7480;letter-spacing:0.14em;">WEEKLY WATCH</td>
              </tr></table>
            </td></tr>

            <tr><td style="padding:10px 0 22px;">
              <span style="font-family:Menlo,Consolas,'Courier New',monospace;font-size:10px;color:#9A948A;letter-spacing:0.14em;text-transform:uppercase;">{file_line}</span>
            </td></tr>

            {note_body}

            <tr><td style="padding-top:6px;">
              <p style="margin:0;font-family:Georgia,'Times New Roman',serif;font-size:16px;color:#1C2430;line-height:1.7;">Michael<br><span style="font-family:Menlo,Consolas,'Courier New',monospace;font-size:11px;color:#6D7480;letter-spacing:0.08em;">PULSECITIES</span></p>
            </td></tr>

          </table>
        </td></tr>

        <tr><td style="padding:16px 6px 0;">
          <p style="margin:0;font-family:Menlo,Consolas,'Courier New',monospace;font-size:10px;color:#8A8578;line-height:1.7;">You subscribed at pulsecities.com. <a href="https://pulsecities.com/api/unsubscribe?token={token}" style="color:#8A8578;">Unsubscribe</a> anytime, one click.</p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>
""".strip()

_NOTE_P = ('<tr><td style="padding-bottom:18px;"><p style="margin:0;'
           "font-family:Georgia,'Times New Roman',serif;font-size:16px;"
           'color:#1C2430;line-height:1.7;">{}</p></td></tr>')

_CONFIRMATION_HTML = _WELCOME_SHELL.replace("{title}", "You're watching {zip_code}").replace(
    "{file_line}", "Watch opened {opened} &middot; {zip_code} &middot; NYC public records"
).replace("{note_body}", "".join([
    _NOTE_P.format("You're watching {zip_code}."),
    _NOTE_P.format("Every {send_day} you'll get a one-page read of what changed in the public record for your neighborhood: deeds, evictions, permits, violations. Quiet weeks send nothing, so when an email arrives it means something moved."),
    _NOTE_P.format('The current reading is here:<br><a href="https://pulsecities.com/neighborhood/{zip_code}" style="color:#C2410C;">pulsecities.com/neighborhood/{zip_code}</a>'),
]))

_CONFIRMATION_TEXT = """
You're watching {zip_code}.

Every {send_day} you'll get a one-page read of what changed in the public record for your neighborhood: deeds, evictions, permits, violations. Quiet weeks send nothing, so when an email arrives it means something moved.

The current reading: https://pulsecities.com/neighborhood/{zip_code}

Michael
PulseCities

You subscribed at pulsecities.com. Unsubscribe: https://pulsecities.com/api/unsubscribe?token={token}
""".strip()

_CITYWIDE_CONFIRMATION_HTML = _WELCOME_SHELL.replace("{title}", "You're watching NYC").replace(
    "{file_line}", "Watch opened {opened} &middot; citywide &middot; NYC public records"
).replace("{note_body}", "".join([
    _NOTE_P.format("You're watching NYC."),
    _NOTE_P.format("Every {send_day} you'll get a citywide read of where displacement pressure moved in the public record: the neighborhoods at the top of the risk list and the week's notable deeds, evictions, and permits. Quiet weeks send nothing."),
    _NOTE_P.format('The live map is here:<br><a href="https://pulsecities.com/map" style="color:#C2410C;">pulsecities.com/map</a>'),
]))

_CITYWIDE_CONFIRMATION_TEXT = """
You're watching NYC.

Every {send_day} you'll get a citywide read of where displacement pressure moved in the public record: the neighborhoods at the top of the risk list and the week's notable deeds, evictions, and permits. Quiet weeks send nothing.

The live map: https://pulsecities.com/map

Michael
PulseCities

You subscribed at pulsecities.com. Unsubscribe: https://pulsecities.com/api/unsubscribe?token={token}
""".strip()

_OPERATOR_CONFIRMATION_HTML = _WELCOME_SHELL.replace("{title}", "You're following {operator_name}").replace(
    "{file_line}", "Follow opened {opened} &middot; {operator_name} &middot; NYC public records"
).replace("{note_body}", "".join([
    _NOTE_P.format("You're following {operator_name}."),
    _NOTE_P.format("When this operator's cluster records new property acquisitions in NYC public records, it shows up in your {send_day} email. Quiet weeks send nothing."),
    _NOTE_P.format('The operator\'s profile is here:<br><a href="https://pulsecities.com/operator/{operator_slug}" style="color:#C2410C;">pulsecities.com/operator/{operator_slug}</a>'),
]))

_OPERATOR_CONFIRMATION_TEXT = """
You're following {operator_name}.

When this operator's cluster records new property acquisitions in NYC public records, it shows up in your {send_day} email. Quiet weeks send nothing.

The operator's profile: https://pulsecities.com/operator/{operator_slug}

Michael
PulseCities

You subscribed at pulsecities.com. Unsubscribe: https://pulsecities.com/api/unsubscribe?token={token}
""".strip()

_UNSUBSCRIBE_CONFIRM_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="robots" content="noindex">
<title>Unsubscribe from PulseCities</title>
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
              <p style="margin:0 0 8px;font-size:20px;font-weight:600;color:#f1f5f9;">Unsubscribe from the weekly digest?</p>
              <p style="margin:0 0 24px;font-size:14px;color:#94a3b8;line-height:1.6;">
                One click below and you're off the list. No more emails after that.
              </p>
              <form method="post" action="/api/unsubscribe?token={token}" style="margin:0;">
                <button type="submit"
                        style="display:inline-block;background:#f97316;color:#fff;font-size:13px;font-weight:600;padding:10px 20px;border-radius:6px;border:none;cursor:pointer;font-family:inherit;">
                  Unsubscribe
                </button>
              </form>
            </td>
          </tr>
          <tr>
            <td style="padding-top:24px;">
              <p style="margin:0;font-size:11px;color:rgba(148,163,184,0.4);line-height:1.6;">
                Changed your mind? Just close this page. Your subscription stays active.
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
    operator_slug: str | None = None

    @field_validator('email')
    @classmethod
    def validate_email(cls, v: str) -> str:
        v = v.strip().lower()
        if not _EMAIL_RE.match(v) or len(v) > 254:
            raise ValueError('invalid email')
        return v

    @model_validator(mode='after')
    def validate_single_target(self) -> 'SubscribeRequest':
        """A subscription watches exactly one of: a ZIP, the city, an operator."""
        if self.operator_slug is not None:
            if self.is_citywide or self.zip_code:
                raise ValueError('operator_slug cannot combine with zip_code or is_citywide')
            self.operator_slug = self.operator_slug.strip().lower()
            if not _SLUG_RE.match(self.operator_slug) or len(self.operator_slug) > 120:
                raise ValueError('invalid operator_slug')
            return self
        if self.is_citywide:
            self.zip_code = None
            return self
        if not self.zip_code:
            raise ValueError('zip_code required when not subscribing citywide')
        self.zip_code = self.zip_code.strip()
        if not _ZIP_RE.match(self.zip_code):
            raise ValueError('zip_code must be 5 digits')
        return self


def _fill(template: str, values: dict) -> str:
    for key, val in values.items():
        template = template.replace("{" + key + "}", str(val))
    return template


def _send_confirmation(
    email: str,
    zip_code: str | None,
    is_citywide: bool,
    operator_slug: str | None = None,
    operator_name: str | None = None,
    unsubscribe_token: str | None = None,
) -> None:
    if not resend.api_key:
        logger.warning("RESEND_API_KEY not set — skipping confirmation email")
        return

    unsub_url = f"https://pulsecities.com/api/unsubscribe?token={unsubscribe_token or ''}"
    values = {
        "send_day": DIGEST_SEND_DAY,
        "token": unsubscribe_token or "",
        "opened": datetime.now(timezone.utc).strftime("%b %-d, %Y"),
    }

    if operator_slug:
        name = operator_name or operator_slug
        values.update({"operator_name": name, "operator_slug": operator_slug})
        subject = f"You're following {name}"
        html, text_body = _OPERATOR_CONFIRMATION_HTML, _OPERATOR_CONFIRMATION_TEXT
        log_line = ("Operator-follow confirmation sent to %s for %s", email, operator_slug)
    elif is_citywide:
        subject = "You're watching NYC"
        html, text_body = _CITYWIDE_CONFIRMATION_HTML, _CITYWIDE_CONFIRMATION_TEXT
        log_line = ("Citywide confirmation sent to %s", email)
    else:
        values["zip_code"] = zip_code
        subject = f"You're watching {zip_code}"
        html, text_body = _CONFIRMATION_HTML, _CONFIRMATION_TEXT
        log_line = ("Confirmation sent to %s for zip %s", email, zip_code)

    try:
        resend.Emails.send({
            "from": "PulseCities <alerts@pulsecities.com>",
            "to": [email],
            "subject": subject,
            "html": _fill(html, values),
            "text": _fill(text_body, values),
            "headers": {
                "List-Unsubscribe": f"<{unsub_url}>",
                "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
            },
        })
        logger.info(*log_line)
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
    operator_name = None
    if body.operator_slug:
        # Same classification gate as every other operator surface: only
        # rows classed 'operator' are followable; lenders and GSEs 404.
        op_row = db.execute(
            text("SELECT display_name, operator_root, operator_class FROM operators WHERE slug = :slug"),
            {"slug": body.operator_slug},
        ).fetchone()
        if op_row is None or op_row.operator_class != 'operator':
            raise HTTPException(status_code=404, detail='Operator not found')
        operator_name = op_row.display_name or op_row.operator_root

        existing = db.execute(
            select(Subscriber).where(
                Subscriber.email == body.email,
                Subscriber.operator_slug == body.operator_slug,
            )
        ).scalar_one_or_none()
        if existing:
            raise HTTPException(status_code=409, detail='Already following this operator.')
        sub = Subscriber(email=body.email, zip_code=None, is_citywide=False,
                         operator_slug=body.operator_slug)
    elif body.is_citywide:
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

    # Single opt-in: the welcome email promises a digest and there is no
    # confirm-link flow, so rows must be born confirmed or the digest's
    # confirmed=true filter silently drops every new subscriber.
    sub.confirmed = True
    sub.confirmed_at = datetime.now(timezone.utc)

    db.add(sub)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        if body.operator_slug:
            raise HTTPException(status_code=409, detail='Already following this operator.')
        if body.is_citywide:
            raise HTTPException(status_code=409, detail='Already watching NYC-wide.')
        raise HTTPException(status_code=409, detail='Already watching this area.')

    logger.info('New subscriber email=%s zip=%s citywide=%s operator=%s',
                body.email, body.zip_code, body.is_citywide, body.operator_slug)
    _send_confirmation(body.email, body.zip_code, body.is_citywide,
                       operator_slug=body.operator_slug, operator_name=operator_name,
                       unsubscribe_token=sub.unsubscribe_token)
    return {'status': 'ok'}


@router.get('/unsubscribe', response_class=HTMLResponse)
def unsubscribe_confirm(token: str, db: Session = Depends(get_db)):
    """Confirmation page, no state change. Mail scanners (SafeLinks,
    Proofpoint) prefetch GET links from email bodies; if this deleted, a
    subscriber could be silently removed before ever seeing the digest.
    The delete happens on POST, which scanners don't issue."""
    sub = db.execute(
        select(Subscriber).where(Subscriber.unsubscribe_token == token)
    ).scalar_one_or_none()

    if not sub:
        raise HTTPException(status_code=404, detail='Invalid or expired unsubscribe link.')

    from urllib.parse import quote
    page = _UNSUBSCRIBE_CONFIRM_HTML.replace('{token}', quote(token))
    return HTMLResponse(content=page, status_code=200)


@router.post('/unsubscribe', response_class=HTMLResponse)
def unsubscribe(token: str, db: Session = Depends(get_db)):
    """Performs the unsubscribe. Reached two ways: the confirmation page's
    button, and RFC 8058 one-click POSTs that Gmail/Yahoo send to the
    List-Unsubscribe URL (their form body is ignored; token is in the query)."""
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
