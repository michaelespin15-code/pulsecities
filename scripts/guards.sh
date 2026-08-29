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
#
# GitHub Actions runs this same lane, which is the point of it being a script
# rather than a list inside a hook. CI had been red on every push since at
# least 2026-07-15 because the workflow ran `pytest -m "not integration"`
# against a DATABASE_URL pointing at a Postgres that does not exist in the
# runner; a red build nobody can make green is worth less than a narrow green
# one. Two knobs make it portable:
#
#   PYTHON=python        the interpreter, when there is no ./venv
#   GUARDS_NO_DB=1       also skip the handful of guards that do need Postgres

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
  # One-rule-one-reader greps. Each of these was written after the rule it
  # guards had already been bypassed in production, so each belongs in the lane
  # that actually runs rather than the suite that does not.
  tests/test_email_guards.py
  tests/test_permit_kind_guards.py
  tests/test_upsert_timestamps.py
  tests/test_backfill_windows.py
  tests/test_window_anchors.py
  tests/test_deconversion.py
  tests/test_score_history_agreement.py
  tests/test_eviction_label.py
  tests/test_title_budget.py
  tests/test_violation_unit_privacy.py
  tests/test_person_privacy.py
  tests/test_landlord_search_gate.py
  tests/test_citywide_trend.py
  tests/test_scoring_change_notes.py
  tests/test_assessment_dormancy.py
  tests/test_index_gate.py
  tests/test_person_pages_gone.py
)

PYTHON="${PYTHON:-./venv/bin/python}"

DESELECT=(
  # True about the box, not about the commit; see the note above.
  --deselect "tests/test_infra_guards.py::TestDeployDrift::test_deploy_copy_matches_installed"
  # Renders sixty property pages against the live database, twelve seconds. The
  # rule and the one-owner greps in that file are what belong in a pre-commit
  # lane; the rendered sweep runs in the full suite and in CI's database job.
  --deselect "tests/test_person_privacy.py::TestNoPersonSurvivesOnAPage"
  # Renders /displacement against the live database; the window and residential
  # greps in that file are the pre-commit-worthy half.
  --deselect "tests/test_citywide_trend.py::TestItRendersAnAnswer"
  # Issues real requests against the live database; the AST assertion in that
  # file is the pre-commit-worthy half.
  --deselect "tests/test_person_pages_gone.py::TestTheRouteRefuses::test_a_person_slug_404s"
  --deselect "tests/test_person_pages_gone.py::TestTheRouteRefuses::test_a_company_slug_still_renders"
  --deselect "tests/test_person_pages_gone.py::TestTheRouteRefuses::test_the_sitemapped_llc_pages_all_still_resolve"
)
if [ -n "${GUARDS_NO_DB:-}" ]; then
  DESELECT+=(
    --deselect "tests/test_infra_guards.py::TestLlmsTxtConsistency::test_generator_matches_stats_and_tier_bands"
  )
fi

PYTHONPATH=. "$PYTHON" -m pytest "${LANE[@]}" -q "${DESELECT[@]}" \
  || {
    echo
    echo "guards failed. Fix, or commit with --no-verify if you know why."
    exit 1
  }
