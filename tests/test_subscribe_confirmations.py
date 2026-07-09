"""
Welcome emails and send hygiene — the Gmail Primary-tab work.

The welcome note is transactional mail, and it has to look like it: plain text
note from a person, no images, no buttons, a real one-click unsubscribe. These
guards pin the classifier-relevant properties (and the same em-dash rule the
digest tests enforce) so a future template edit can't quietly re-marketing-ify
the send:

  1. Subjects carry no em dash and no promotional suffix.
  2. Every send has a plain-text part alongside the HTML.
  3. Every send carries the subscriber's one-click unsubscribe URL in the body
     AND a List-Unsubscribe header (Gmail bulk-sender guidance).
  4. No images in welcome mail; a tracking-pixel-shaped element is exactly what
     pushes a note into Promotions.

No network: resend is stubbed at the module seam.
"""

import pytest

from api.routes import subscribe as sub_mod
from scripts import weekly_digest as digest_mod


class _CaptureEmails:
    def __init__(self):
        self.payloads = []

    def send(self, payload):
        self.payloads.append(payload)
        return {"id": "test"}


@pytest.fixture
def sub_capture(monkeypatch):
    cap = _CaptureEmails()
    monkeypatch.setattr(sub_mod.resend, "api_key", "test-key")
    monkeypatch.setattr(sub_mod.resend, "Emails", cap)
    return cap


@pytest.fixture
def digest_capture(monkeypatch):
    cap = _CaptureEmails()
    monkeypatch.setattr(digest_mod.resend, "api_key", "test-key")
    monkeypatch.setattr(digest_mod.resend, "Emails", cap)
    return cap


def _sent(cap):
    assert len(cap.payloads) == 1, f"expected exactly one send, got {len(cap.payloads)}"
    return cap.payloads[0]


class TestZipWelcome:
    def _send(self, cap):
        sub_mod._send_confirmation(
            "reader@example.com", "11216", False, unsubscribe_token="tok-abc",
        )
        return _sent(cap)

    def test_subject_is_plain_and_em_dash_free(self, sub_capture):
        p = self._send(sub_capture)
        assert p["subject"] == "You're watching 11216"
        assert "—" not in p["subject"]

    def test_has_a_plain_text_part(self, sub_capture):
        p = self._send(sub_capture)
        assert p.get("text"), "welcome mail must carry a text part"
        assert "11216" in p["text"]

    def test_one_click_unsubscribe_in_body_and_header(self, sub_capture):
        p = self._send(sub_capture)
        unsub = "https://pulsecities.com/api/unsubscribe?token=tok-abc"
        assert unsub in p["html"]
        assert unsub in p["text"]
        assert unsub in p["headers"]["List-Unsubscribe"]
        assert p["headers"]["List-Unsubscribe-Post"] == "List-Unsubscribe=One-Click"

    def test_no_images_and_no_em_dash_in_body(self, sub_capture):
        p = self._send(sub_capture)
        assert "<img" not in p["html"].lower()
        assert "—" not in p["html"]


class TestCitywideWelcome:
    def test_subject_and_unsubscribe(self, sub_capture):
        sub_mod._send_confirmation(
            "reader@example.com", None, True, unsubscribe_token="tok-nyc",
        )
        p = _sent(sub_capture)
        assert p["subject"] == "You're watching NYC"
        assert "—" not in p["subject"]
        assert "token=tok-nyc" in p["html"]
        assert "token=tok-nyc" in p["headers"]["List-Unsubscribe"]
        assert p.get("text")


class TestOperatorWelcome:
    def test_subject_and_unsubscribe(self, sub_capture):
        sub_mod._send_confirmation(
            "reader@example.com", None, False,
            operator_slug="acme-realty", operator_name="Acme Realty",
            unsubscribe_token="tok-op",
        )
        p = _sent(sub_capture)
        assert p["subject"] == "You're following Acme Realty"
        assert "—" not in p["subject"]
        assert "Acme Realty" in p["html"]
        assert "token=tok-op" in p["headers"]["List-Unsubscribe"]
        assert p.get("text")


class TestDigestSendHygiene:
    def test_digest_carries_list_unsubscribe_header(self, digest_capture):
        ok = digest_mod.send_digest_email(
            {"email": "reader@example.com", "unsubscribe_token": "tok-d"},
            {"subject": "PulseCities Weekly Watch: Harlem / 10026 update", "html": "<html></html>"},
        )
        assert ok
        p = _sent(digest_capture)
        assert "token=tok-d" in p["headers"]["List-Unsubscribe"]


class TestUnsubscribeIsPostOnly:
    """GET must never change state: corporate mail scanners prefetch GET links
    from email bodies, and a deleting GET silently unsubscribes those readers.
    The delete lives on POST, which is also what RFC 8058 one-click sends."""

    def _client(self):
        from unittest.mock import MagicMock
        from fastapi.testclient import TestClient
        from api.main import app
        from models.database import get_db
        self._get_db = get_db

        fake_sub = MagicMock()
        fake_sub.email       = "reader@example.com"
        fake_sub.zip_code    = "11216"
        fake_sub.is_citywide = False
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = fake_sub
        app.dependency_overrides[self._get_db] = lambda: db
        return TestClient(app), db, app

    def test_get_renders_confirmation_and_does_not_delete(self):
        client, db, app = self._client()
        try:
            resp = client.get("/api/unsubscribe?token=tok-1")
            assert resp.status_code == 200
            assert 'method="post"' in resp.text
            db.delete.assert_not_called()
        finally:
            app.dependency_overrides.pop(self._get_db, None)

    def test_post_performs_the_delete(self):
        client, db, app = self._client()
        try:
            resp = client.post("/api/unsubscribe?token=tok-1")
            assert resp.status_code == 200
            assert "unsubscribed" in resp.text.lower()
            db.delete.assert_called_once()
        finally:
            app.dependency_overrides.pop(self._get_db, None)
