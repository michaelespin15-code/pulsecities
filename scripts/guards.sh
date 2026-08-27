#!/usr/bin/env bash
#
# The fast lane: repo-consistency guards, no database, no app import.
#
# Install as a pre-commit hook:
#     ln -sf ../../scripts/guards.sh .git/hooks/pre-commit
#
# Why this exists. Three separate guards rotted in the two weeks to 2026-08-27,
# each of which would have caught its own bug the day it shipped:
#
#   - test_logrotate_covers_every_cron_log knew the condo cron's log was not
#     rotating. The commit that added the cron shipped without it being run.
#   - test_source_freshness_disclosure had five assertions red from the commit
#     that wrote them: they matched "Jul 31," against a renderer that has always
#     written "July 31, 2026,". Nobody ran the file.
#   - No guard existed at all for the rule api/freshness.py owns, so seventeen
#     call sites bypassed it and the sitemap published a future <lastmod> for
#     sixteen nights.
#
# The suite that would have caught all three takes 76 seconds on two vCPUs, and
# a check that slow does not get run before a commit. This lane is the subset
# that needs neither Postgres nor api.main, which is also the subset that catches
# structural drift. It runs in under four seconds.
#
# What is deliberately NOT here: deploy-drift checks comparing the repo against
# /etc. Those are true statements about the box, not about the commit, so
# blocking a commit on one punishes the wrong action. They run in the full suite
# and in weekly_ops_health.
#
# Bypass with `git commit --no-verify` when you mean to. It is not a gate on
# judgement, only on forgetting.

set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

LANE=(
  tests/test_date_guards.py
  tests/test_infra_guards.py
  tests/test_fonts.py
  tests/test_vendored_assets.py
  tests/test_asset_stamp.py
  tests/test_tier_bands.py
  tests/test_text_contrast.py
  tests/test_freshness_contract.py
  tests/test_alert_snooze.py
)

PYTHONPATH=. ./venv/bin/python -m pytest "${LANE[@]}" -q \
  --deselect "tests/test_infra_guards.py::TestDeployDrift::test_deploy_copy_matches_installed" \
  || {
    echo
    echo "guards failed. Fix, or commit with --no-verify if you know why."
    exit 1
  }
