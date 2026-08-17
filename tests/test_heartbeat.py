"""
Guards on the external dead-man's switch.

Every other monitor in this project runs on the box it watches, so the one
outage nobody heard about was the one where the box itself stopped. This ping is
the only check that survives that, which puts two properties above all others:
it must never break the run it reports on, and a failed run must ping /fail
rather than staying silent and waiting out the grace window.

No network. Every test stubs requests.
"""

from unittest.mock import patch

import pytest

import scheduler.heartbeat as hb


class TestDisabledByDefault:
    def test_unset_url_sends_nothing(self, monkeypatch):
        """Inert until configured, so merging this cannot change a run."""
        monkeypatch.delenv("HEARTBEAT_BASE_URL", raising=False)
        with patch.object(hb.requests, "post") as post:
            hb.ping("nightly-pipeline")
        post.assert_not_called()

    def test_blank_url_sends_nothing(self, monkeypatch):
        monkeypatch.setenv("HEARTBEAT_BASE_URL", "")
        with patch.object(hb.requests, "post") as post:
            hb.ping("nightly-pipeline")
        post.assert_not_called()


class TestPingUrls:
    def test_success_pings_the_slug(self, monkeypatch):
        monkeypatch.setenv("HEARTBEAT_BASE_URL", "https://hc-ping.com/key")
        with patch.object(hb.requests, "post") as post:
            hb.ping("nightly-pipeline")
        assert post.call_args[0][0] == "https://hc-ping.com/key/nightly-pipeline"

    def test_failure_pings_the_fail_endpoint(self, monkeypatch):
        """A run that failed should not wait out the grace period to be noticed."""
        monkeypatch.setenv("HEARTBEAT_BASE_URL", "https://hc-ping.com/key")
        with patch.object(hb.requests, "post") as post:
            hb.ping("nightly-pipeline", ok=False, detail="scraper failures")
        assert post.call_args[0][0] == "https://hc-ping.com/key/nightly-pipeline/fail"

    def test_trailing_slash_does_not_double_up(self, monkeypatch):
        monkeypatch.setenv("HEARTBEAT_BASE_URL", "https://hc-ping.com/key/")
        with patch.object(hb.requests, "post") as post:
            hb.ping("nightly-pipeline")
        assert post.call_args[0][0] == "https://hc-ping.com/key/nightly-pipeline"

    def test_ping_is_bounded_by_a_timeout(self, monkeypatch):
        """A hung provider must not hold the pipeline lock open."""
        monkeypatch.setenv("HEARTBEAT_BASE_URL", "https://hc-ping.com/key")
        with patch.object(hb.requests, "post") as post:
            hb.ping("nightly-pipeline")
        assert post.call_args.kwargs["timeout"] == hb._TIMEOUT_SECONDS

    def test_detail_is_capped(self, monkeypatch):
        monkeypatch.setenv("HEARTBEAT_BASE_URL", "https://hc-ping.com/key")
        with patch.object(hb.requests, "post") as post:
            hb.ping("nightly-pipeline", ok=False, detail="x" * 5000)
        assert len(post.call_args.kwargs["data"]) <= 1000


class TestNeverBreaksTheRun:
    def test_network_failure_is_swallowed(self, monkeypatch):
        """Monitoring that can fail the job it monitors is worse than none."""
        monkeypatch.setenv("HEARTBEAT_BASE_URL", "https://hc-ping.com/key")
        with patch.object(hb.requests, "post", side_effect=OSError("no route to host")):
            hb.ping("nightly-pipeline")  # must not raise

    def test_provider_500_is_swallowed(self, monkeypatch):
        monkeypatch.setenv("HEARTBEAT_BASE_URL", "https://hc-ping.com/key")
        with patch.object(hb.requests, "post", side_effect=RuntimeError("boom")):
            hb.ping("nightly-pipeline", ok=False)


class TestPipelineWiring:
    """The ping is only worth anything if it sits on every exit path."""

    def _run_main(self, monkeypatch, pipeline_result, exits=True):
        import scheduler.main as main

        monkeypatch.setattr(main, "_acquire_lock", lambda logger: True)
        monkeypatch.setattr(main, "_release_lock", lambda: None)
        monkeypatch.setattr(main, "flush_alerts", lambda: None)
        monkeypatch.setattr(main, "send_ops_email", lambda *a, **k: None)
        monkeypatch.setattr(main, "configure_logging", lambda: None)

        if isinstance(pipeline_result, Exception):
            monkeypatch.setattr(
                main, "run_nightly_pipeline",
                lambda: (_ for _ in ()).throw(pipeline_result))
        else:
            monkeypatch.setattr(main, "run_nightly_pipeline", lambda: pipeline_result)

        with patch.object(main, "heartbeat") as beat:
            if exits:
                with pytest.raises(SystemExit):
                    main.main()
            else:
                main.main()  # the healthy path returns rather than exiting
        return beat

    def test_success_pings_ok(self, monkeypatch):
        beat = self._run_main(monkeypatch, True, exits=False)
        beat.assert_called_once_with("nightly-pipeline")

    def test_scraper_failures_ping_fail(self, monkeypatch):
        beat = self._run_main(monkeypatch, False)
        assert beat.call_args.kwargs["ok"] is False

    def test_crash_pings_fail(self, monkeypatch):
        """The crash path is the one most likely to skip its own reporting."""
        beat = self._run_main(monkeypatch, RuntimeError("db gone"))
        assert beat.call_args.kwargs["ok"] is False
        assert "db gone" in beat.call_args.kwargs["detail"]

    def test_skipped_run_does_not_ping(self, monkeypatch):
        """A run that skipped because the last one is still going proved nothing.

        Pinging here would hide a pipeline that wedges every single night.
        """
        import scheduler.main as main

        monkeypatch.setattr(main, "_acquire_lock", lambda logger: False)
        monkeypatch.setattr(main, "configure_logging", lambda: None)
        with patch.object(main, "heartbeat") as beat:
            with pytest.raises(SystemExit):
                main.main()
        beat.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
