## Data Health Issues — 2026-04-18 15:51 UTC

**Requires manual investigation:**

- [ ] Scraper `311_complaints` (311 Complaints) — status: warning
  - record count anomaly: 0 processed, expected ≥5000 (got 0% of minimum)

- [ ] Scraper `dob_permits` (DOB Permits) — status: warning
  - record count anomaly: 25 processed, expected ≥500 (got 5% of minimum)

- [ ] Scraper `acris_ownership` (ACRIS Ownership) — status: warning
  - record count anomaly: 0 processed, expected ≥200 (got 0% of minimum)

- [ ] Scraper `dof_assessments` (DOF Tax Assessments) — status: failed
  - no successful run on record
  - last error: stale run: automatically marked failed by data_health_check.py

---

# TODO

Items flagged for manual follow-up by automated health checks.

---

## Data Health Issues — 2026-04-18 15:50 UTC

**Auto-fixed:**
- Cleared 6 stale `running` locks for `dof_assessments` (runs #122–127, started ~2AM UTC, OOM-killed before completion)
- Reduced `DOFScraper.PAGE_SIZE` from 50,000 to 5,000 to lower peak memory per fetch (~50MB → ~5MB per page)

**Requires manual investigation:**

- [ ] `dof_assessments` — has never completed successfully. Root cause: process OOM-killed on every nightly run (confirmed by "Process killed before completion (OOM or SIGKILL)" in 10 consecutive scraper_run rows across Apr 17–18). The PAGE_SIZE fix is deployed but needs a successful pipeline run to verify. Watch the 2AM UTC run tonight.

- [ ] `311_complaints` — last run processed 0 records against an expected minimum of 5,000. Either the incremental watermark is too recent (tiny fetch window) or the Socrata API returned no results. Check `scraper_runs` watermark_timestamp vs `complaints_raw` MAX(created_date) to rule out watermark drift.

- [ ] `dob_permits` — last run processed 25 records against an expected minimum of 500. Same watermark drift investigation as complaints above.

- [ ] `acris_ownership` — last run processed 0 records against an expected minimum of 200. ACRIS incremental fetches depend on `recorded_datetime`; check if the watermark is ahead of available data.

---
