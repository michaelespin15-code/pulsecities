# Postmortem: ACRIS Stale Watermark (2026-03-31 to 2026-05-01)

## Summary

The LLC acquisitions signal, weighted at 26% of the composite displacement score, was running on data frozen at 2026-03-31 for 31 days while the public tool showed no indication of this. The root cause is an upstream NYC Open Data source freeze, not an internal scraper failure. The monitoring gap is a separate finding documented below.

## Timeline

| Date | Event |
|---|---|
| 2026-03-31 | ACRIS master dataset (bnx9-e6tj) last updated on NYC Open Data. All subsequent records with `recorded_datetime > 2026-03-31` return zero rows from Socrata. |
| 2026-04-24 | Scraper begins returning 0 records with `status=success` and `watermark_timestamp=NULL`. Warning messages written to `scraper_runs.warning_message`. |
| 2026-04-25 to 04-26 | Status cycles to `warning` as rolling average (still anchored to healthy-period runs) fires the anomaly check. |
| 2026-04-27 to 04-30 | Rolling average window fills with 0-record runs. Computed `rolling_avg` drops to ~0. Warning condition `records=0 AND rolling_avg>100` stops firing. Status flips to `success` silently. No alert sent. |
| 2026-05-01 | Data integrity audit script (`scripts/data_integrity_audit.py`, built the same session) surfaces the watermark drift finding. Investigation begins. |

## Root Cause: Upstream Source Freeze

NYC Open Data ACRIS master dataset (`bnx9-e6tj`) has not been updated since 2026-03-31. Verified independently:

```
GET /resource/bnx9-e6tj.json?$select=MAX(recorded_datetime) AS max_dt
=> {"max_dt":"2026-03-31T00:00:00.000"}

GET /resource/bnx9-e6tj.json?$select=COUNT(*) AS n&$where=recorded_datetime > '2026-03-31'
=> {"n":"0"}
```

All three ACRIS datasets (master, parties, legals) show the same ceiling date. The freeze is ACRIS-specific. All other signals (DOB permits, OCA evictions, 311 complaints, HPD violations) were current as of 2026-04-28 to 2026-04-29.

The internal scraper behaved correctly. `_check_source_freshness()` detected that the source had not advanced past the watermark and correctly returned 0 records. This is working as intended.

There is no backfill to perform. The data does not exist in the upstream source. When NYC Open Data publishes the next ACRIS batch, the scraper will resume from the last watermark and ingest the intervening records automatically.

## Monitoring Gap (Second-Order Finding)

The fact that this sat for 31 days without an alert is a monitoring failure. Two bugs combined to silence detection:

**Bug 1: Rolling-average self-silencing.**
`_compute_rolling_avg()` in `scrapers/base.py` only counted `status='success'` runs. When 0-record runs with `status='success'` (the scraper returns early from `_check_source_freshness`) filled the 14-day window, the computed average dropped to 0. The condition `records=0 AND rolling_avg>100` stopped firing, and status flipped to `success` silently.

**Bug 2: No alert on `warning` status.**
`scheduler/pipeline.py` only called `send_alert()` on scraper exception (failure). `warning` status was logged but never sent through the webhook.

**Why the rolling-avg check worked at all:** The check did fire correctly for approximately 2 days (April 25-26) before the window filled. A webhook would have delivered those alerts. The absence of a configured `ALERT_WEBHOOK_URL` in the deployment means those warnings went to log only.

## Impact

The public tool was serving displacement scores where the LLC acquisitions signal (26% weight) was frozen at 2026-03-31 data. ZIP codes with high post-March LLC acquisition activity would be under-ranked. The magnitude of the score impact is unknown until NYC Open Data publishes the missing data.

This is a credibility issue. The methodology page claims "updated daily from ACRIS." That claim was false for 31 days and no user-visible indicator existed.

## Fixes Shipped

1. **`scrapers/base.py`**: `_compute_rolling_avg()` now includes both `success` and `warning` status runs, preventing the average from diluting to 0. The `expected_min` config value acts as a permanent floor: zero records from any scraper with `expected_min > 100` always produces `status=warning`, regardless of rolling average.

2. **`scheduler/pipeline.py`**: `_run_scraper_with_retry()` now calls `send_alert()` when `scraper_run.status == 'warning'`, not only on exception.

3. **`scripts/daily_health_check.py`**: New script queries Socrata API directly for each signal's max date and alerts when upstream is stale beyond threshold. ACRIS threshold is 7 days. Runs independently of scraper status.

4. **`/api/stats`**: Returns `data_freshness` object with per-signal staleness. Cached 5 minutes.

5. **Frontend banner**: `index.html` and `app.html` render an amber dismissable banner when any signal in `data_freshness` is stale. Banner text for ACRIS: "ACRIS ownership data current through {date}. NYC Open Data has not published new records since this date." Dismissal stored in `localStorage`, keyed by the stale-through date so it reappears when data changes.

## What Would Have Caught This Earlier

- A configured `ALERT_WEBHOOK_URL` would have delivered the 2-day window of `warning` status alerts on April 25-26, before the rolling average silenced them.
- The `daily_health_check.py` script, now added, queries the upstream Socrata API directly and would have fired on day 8 (April 8, threshold 7 days). It is independent of scraper status and cannot be silenced by rolling-average drift.

## Open Question

NYC Open Data ACRIS refresh cadence is undocumented. The 2026-03-31 freeze may be a one-time publishing gap or a change in how the city maintains this dataset. Monitor `max(recorded_datetime)` via the daily health check and escalate to NYC Open Data support if the freeze persists past 2026-05-15.
