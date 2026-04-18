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

## Deferred: Assessment Spike Historical Backfill (PLUTO 2022-2025)

**Decision date:** 2026-04-18
**Status:** Deferred — table and scoring logic retained, ingestion script not built.

**Why deferred:**
NYC RPTL §581 caps Class 2 residential building assessments at +6%/year or +20% over 5 years. Fast-moving acquisition operators (MTEK, PHANTOM, BREDIF, etc.) typically hold and sell within 2-4 years — well inside the lag window before DOF assessed values track speculative activity. Backfilling historical PLUTO would produce a signal that reliably fires on slow, long-term neighborhood gentrification but is structurally blind to the aggressive portfolio operators PulseCities specifically investigates.

**What the signal would actually show:**
Buildings where DOF assessment has caught up to sustained market appreciation over 5-10 years. Useful for borough-level gentrification pressure reporting, not useful for identifying individual speculative operators.

**What to do instead:**
Let the live pipeline accumulate data. `PlutoScraper._write_assessment_snapshot()` already writes a row to `assessment_history` on every quarterly PLUTO run. After two distinct `tax_year` values exist, `_aggregate_assessment_spike()` in `scoring/compute.py` activates automatically with no code change. First expected activation: April 2027.

**When to revisit:**
Run `SELECT COUNT(DISTINCT tax_year) FROM assessment_history` in April 2027. If backfill is ever needed for score history reconstruction, the Bytes of the Big Apple archive has PLUTO back to 2002. Column name casing varies by version; `assesstot` is consistent post-2021.

---
