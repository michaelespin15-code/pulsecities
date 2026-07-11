"""
Infrastructure regression guards.

1. llms.txt consistency: the generator must produce the same top-risk facts as
   /api/stats at the moment of generation, with tier words from the canonical
   band function. The 2026-07-11 audit found the file quoting yesterday's
   scores (it fetched the HTTP endpoint through its one-hour cache) and calling
   Moderate ZIPs "high displacement pressure" (hardcoded label).

2. Deploy drift: the copies under deploy/ must match what is actually
   installed in /etc. They matched by discipline until the logrotate config
   (never mirrored into deploy/) shipped a postrotate signal to a unit that
   does not exist. These tests only run on the production box; anywhere the
   installed file is absent they skip.
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).parent.parent


class TestLlmsTxtConsistency:
    @pytest.mark.integration
    def test_generator_matches_stats_and_tier_bands(self):
        from api.routes.frontend import _tier_info
        from api.routes.stats import compute_top_risk
        from models.database import SessionLocal
        from scripts.gen_llms_txt import build

        text = build()

        db = SessionLocal()
        try:
            _, entries = compute_top_risk(db)
        finally:
            db.close()
        assert entries, "compute_top_risk returned nothing; scores table empty?"

        section = text.split("## Current top-risk neighborhoods", 1)[1]
        section = section.split("##", 1)[0]
        lines = [l for l in section.splitlines() if l.startswith("- ")]
        assert len(lines) == len(entries[:5])

        for line, entry in zip(lines, entries):
            assert entry["name"] in line, f"{entry['name']} missing from: {line}"
            assert str(entry["zip_code"]) in line
            assert f"score {entry['score']}" in line, (
                f"score {entry['score']} not quoted for {entry['name']}: {line}"
            )
            tier_word = _tier_info(float(entry["score"]))[0].lower()
            assert f"{tier_word} displacement pressure" in line, (
                f"tier word must come from _tier_info ({tier_word}): {line}"
            )

    def test_generator_does_not_fetch_the_cached_endpoint(self):
        src = (REPO / "scripts" / "gen_llms_txt.py").read_text()
        assert "urlopen" not in src and "requests.get" not in src, (
            "gen_llms_txt must read the DB via compute_top_risk, not the "
            "HTTP endpoint: the 1-hour stats cache made the file quote "
            "yesterday's scores every morning"
        )


# (repo file, installed file)
_DEPLOY_PAIRS = [
    ("deploy/nginx-pulsecities.conf", "/etc/nginx/sites-enabled/pulsecities"),
    ("deploy/nginx-security-headers.conf",
     "/etc/nginx/snippets/pulsecities-security-headers.conf"),
    ("deploy/pulsecities.cron", "/etc/cron.d/pulsecities"),
    ("deploy/pulsecities.logrotate", "/etc/logrotate.d/pulsecities"),
    ("deploy/pulsecities.service", "/etc/systemd/system/pulsecities.service"),
]


class TestDeployDrift:
    @pytest.mark.parametrize("repo_rel,installed", _DEPLOY_PAIRS,
                             ids=[p[0].split("/")[-1] for p in _DEPLOY_PAIRS])
    def test_deploy_copy_matches_installed(self, repo_rel, installed):
        repo_file = REPO / repo_rel
        installed_file = Path(installed)
        if not installed_file.exists():
            pytest.skip(f"{installed} not present (not the production box)")
        assert repo_file.exists(), f"{repo_rel} missing from the repo"
        assert repo_file.read_text() == installed_file.read_text(), (
            f"{repo_rel} differs from {installed}. Re-deploy or back-port: "
            f"diff {repo_file} {installed}"
        )

    def test_logrotate_covers_every_cron_log(self):
        cron = (REPO / "deploy" / "pulsecities.cron").read_text()
        logrotate = (REPO / "deploy" / "pulsecities.logrotate").read_text()
        logs = set(re.findall(r">>?\s*(/var/log/pulsecities/\S+\.log)", cron))
        missing = sorted(l for l in logs if l not in logrotate)
        assert not missing, (
            f"cron writes to logs that logrotate never rotates: {missing}"
        )
