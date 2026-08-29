"""
The ops alert is the one message that has no next run.

On 2026-08-28 the send-through-one-gate refactor moved send_ops_email onto
scripts/lib/mailer.py and did not import it. Every ops email raised NameError
from that commit onward: the nightly flush swallowed it into a WARNING line,
and notify_ops -- the escalation path, the one reserved for failures that must
reach a human tonight -- raised it into its callers instead.

It survived a full green suite because conftest.py blanks RESEND_API_KEY so the
suite can never send real mail, and send_ops_email returns before the mailer
call when there is no key. The guard that stops the tests emailing is exactly
what stopped them reaching the line that was broken.

So these tests supply a key and stub the gate. Anything that asserts on the
source text of alerts.py instead would pass on the word "mailer" appearing in a
comment, which it does, twice.
"""
import pytest

from scheduler import alerts
from scripts.lib import mailer


@pytest.fixture
def gate(monkeypatch):
    """Stub the one send gate and capture what reaches it."""
    calls = []

    def fake_send(**kwargs):
        calls.append(kwargs)
        return True

    monkeypatch.setattr(mailer, "send", fake_send)
    monkeypatch.setenv("RESEND_API_KEY", "re_test_not_a_real_key")
    monkeypatch.setenv("ALERT_EMAIL", "ops@example.com")
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "")
    return calls


class TestItActuallyReachesTheMailer:
    def test_an_ops_email_is_handed_to_the_gate(self, gate):
        alerts.send_ops_email("acris_ownership: source frozen 29d", "body")
        assert gate, (
            "send_ops_email did not reach mailer.send. This is the shape the "
            "NameError took: the call site is there, the name it calls is not."
        )
        assert gate[0]["to"] == ["ops@example.com"]
        assert "acris_ownership" in gate[0]["subject"]

    def test_the_escalation_path_reaches_it_too(self, gate):
        alerts.notify_ops("pipeline_health CRITICAL", "body")
        assert gate, "notify_ops webhooked and then dropped the email"

    def test_a_terse_alert_is_not_refused_for_length(self, gate):
        """"acris_ownership: source frozen 29d" is nine words and is the most
        important message this system sends. It must bypass the content floor."""
        alerts.send_ops_email("subject", "source frozen 29d")
        assert gate[0]["min_words"] == 0

    def test_it_retries(self, gate):
        """The 2026-08-05 outage email died on one TLS error to Resend."""
        alerts.send_ops_email("subject", "body")
        assert gate[0]["retries"], "an ops alert has no next run to retry it"

    def test_multiple_recipients_are_split(self, gate, monkeypatch):
        monkeypatch.setenv("ALERT_EMAIL", "ops@example.com, michael@example.com")
        alerts.send_ops_email("subject", "body")
        assert gate[0]["to"] == ["ops@example.com", "michael@example.com"]


class TestItNeverRaises:
    """The module docstring promises this and two callers depend on it: both
    refresh_condo_addresses and refresh_operator_directory alert from inside an
    `except` block, where a raise here loses the error being reported."""

    def test_a_broken_gate_does_not_escape_send_ops_email(self, gate, monkeypatch):
        def explode(**kwargs):
            raise RuntimeError("resend is having a day")

        monkeypatch.setattr(mailer, "send", explode)
        alerts.send_ops_email("subject", "body")  # must not raise

    def test_a_broken_gate_does_not_escape_notify_ops(self, gate, monkeypatch):
        def explode(**kwargs):
            raise RuntimeError("resend is having a day")

        monkeypatch.setattr(mailer, "send", explode)
        alerts.notify_ops("subject", "body")  # must not raise

    def test_a_refused_message_does_not_escape(self, gate, monkeypatch):
        def refuse(**kwargs):
            raise mailer.EmailRefused("empty body")

        monkeypatch.setattr(mailer, "send", refuse)
        alerts.send_ops_email("subject", "body")  # must not raise

    def test_flush_still_drains_the_buffer_when_the_gate_is_broken(self, gate, monkeypatch):
        def explode(**kwargs):
            raise RuntimeError("resend is having a day")

        monkeypatch.setattr(mailer, "send", explode)
        alerts.send_alert("subject", "body")
        alerts.flush_alerts()
        assert not alerts._pending, "a failed flush must not re-send tomorrow"
