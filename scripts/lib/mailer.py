"""
The one place this project sends mail, and the checks every message passes first.

There were four independent `resend.Emails.send` call sites, each deciding for
itself whether a message was worth sending. The daily building alert guarded on
`if events:` at scan time, the weekly digest guarded on per-arm thresholds, the
flips report guarded on `if new_arcs:`, and the subscribe confirmation guarded on
nothing at all. Four rules, one of them absent, and no way to add a fifth path
without inventing a fifth rule.

That is the same shape as the freshness bug this codebase already paid for: a
rule that lives in one reader and is reimplemented everywhere else. The fix is
the same. This module owns the rule, `tests/test_email_guards.py` greps for
anyone calling the SDK directly, and adding a new sender means passing through
here or failing the guard.

What it refuses, and why each one is a real failure rather than a hypothetical:

  no recipient        an address that is empty or malformed
  empty subject       renders as "(no subject)" in every client
  thin body           the class the block digest was going to introduce: an
                      email whose only content is the letterhead and the
                      unsubscribe line. Measured against visible text with the
                      chrome stripped, not against raw HTML length, because the
                      shell alone is 2KB of markup.
  unfilled template   a literal {placeholder} or __TOKEN__ that survived
                      rendering. These reach inboxes looking like a bug because
                      they are one, and neither .format() nor str.replace()
                      raises when a key is missing from the values dict.

Refusals raise. A caller that would rather skip than fail can ask first with
`would_send`, which is what a scan loop wants. Silently dropping the message
would recreate the problem this exists to solve: nobody finds out.
"""

from __future__ import annotations

import logging
import os
import re

logger = logging.getLogger(__name__)

FROM_DEFAULT = "PulseCities <alerts@pulsecities.com>"

# The backstop, not the primary check. Measured rather than guessed, because
# the first draft of this constant was guessed and was wrong: it read 45 on a
# claim that a one-event alert carries "60 to 90 words", and a real one-event
# alert renders 39. That floor would have blocked "your building was sold",
# which is the single most important message this system sends.
#
# Measured on the live templates: the building-alert shell with no events is 28
# visible words, the same shell with one event is 39. 34 sits between them.
#
# A word count cannot tell a terse real email from an empty one reliably, which
# is why `content_items` below is the actual guarantee and this only catches a
# render that claimed content and produced none.
MIN_CONTENT_WORDS = 34

# Placeholder syntaxes actually used in this codebase's templates.
_UNFILLED = re.compile(r"\{[a-z_][a-z0-9_]*\}|__[A-Z][A-Z0-9_]*__")
_TAG = re.compile(r"<[^>]+>")
_STYLE_BLOCK = re.compile(r"<(script|style)\b.*?</\1>", re.S | re.I)
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s.]+\.[^@\s]+$")


class EmailRefused(ValueError):
    """A message that would have been embarrassing to receive."""


def visible_words(html: str) -> list[str]:
    """Words a reader actually sees. Strips tags and style/script blocks, which
    otherwise make any HTML email look content-rich to a length check."""
    if not html:
        return []
    body = _STYLE_BLOCK.sub(" ", html)
    body = _TAG.sub(" ", body)
    body = body.replace("&nbsp;", " ").replace("&middot;", " ").replace("&rarr;", " ")
    return [w for w in re.split(r"\s+", body) if w.strip()]


def check(to: str, subject: str, html: str | None, text: str | None = None,
          content_items: int | None = None,
          min_words: int = MIN_CONTENT_WORDS) -> list[str]:
    """Every reason this message should not be sent. Empty list means send.

    `content_items` is the caller counting what it is actually reporting: events
    on a building, rows in a digest, one for a confirmation. Zero refuses, and
    that is the real guarantee. The word floor below is only a backstop for a
    render that claimed items and produced an empty page.
    """
    problems: list[str] = []

    if content_items is not None and content_items <= 0:
        problems.append(
            f"caller reports {content_items} content items; there is nothing to say"
        )

    if not to or not _EMAIL_RE.match(to.strip()):
        problems.append(f"recipient is not an address: {to!r}")
    if not (subject or "").strip():
        problems.append("subject is empty")

    body = html or text or ""
    if not body.strip():
        problems.append("body is empty")
    else:
        words = visible_words(body) if html else body.split()
        if len(words) < min_words:
            problems.append(
                f"body has {len(words)} visible words, floor is {min_words}; "
                f"this is chrome with nothing in it"
            )

    for field, value in (("subject", subject), ("html", html), ("text", text)):
        if not value:
            continue
        left = _UNFILLED.findall(value)
        if left:
            problems.append(f"{field} has unfilled placeholders: {sorted(set(left))[:5]}")

    return problems


def would_send(to: str, subject: str, html: str | None, text: str | None = None,
               content_items: int | None = None,
               min_words: int = MIN_CONTENT_WORDS) -> bool:
    """Non-raising form, for a scan loop deciding whether to bother building
    the rest of a message."""
    return not check(to, subject, html, text, content_items, min_words)


def send(to: str | list[str], subject: str, html: str | None = None,
         text: str | None = None, headers: dict | None = None,
         sender: str = FROM_DEFAULT, dry_run: bool = False,
         content_items: int | None = None,
         min_words: int = MIN_CONTENT_WORDS,
         retries: tuple[int, ...] = ()) -> bool:
    """Send one message, or raise EmailRefused explaining why it should not go.

    Returns True on send, False when the provider call failed. It never returns
    False for a content problem; that is an exception, because a content problem
    is a bug in the caller and a failed provider call is weather.
    """
    recipients = [to] if isinstance(to, str) else list(to)
    for r in recipients:
        problems = check(r, subject, html, text, content_items, min_words)
        if problems:
            raise EmailRefused(
                f"refusing to send {subject!r} to {r!r}: " + "; ".join(problems)
            )

    if dry_run:
        logger.info("[DRY RUN] would send %r to %s", subject, ", ".join(recipients))
        return True

    import resend
    if not resend.api_key:
        resend.api_key = os.getenv("RESEND_API_KEY", "")
    if not resend.api_key:
        raise EmailRefused("RESEND_API_KEY is not set")

    payload: dict = {"from": sender, "to": recipients, "subject": subject}
    if html:
        payload["html"] = html
    if text:
        payload["text"] = text
    if headers:
        payload["headers"] = headers

    # The ladder exists because the 2026-08-05 outage email died on one TLS
    # error to api.resend.com. Default is no retry: a subscriber digest that
    # fails is retried by the next run, while an ops alert has no next run that
    # matters.
    import time
    for i, delay in enumerate((0,) + tuple(retries)):
        if delay:
            time.sleep(delay)
        try:
            resend.Emails.send(payload)
            return True
        except Exception as exc:
            if i == len(retries):
                logger.exception("Resend failed for %s", ", ".join(recipients))
                return False
            logger.warning("Resend attempt failed (%s), retrying", exc)
    return False
