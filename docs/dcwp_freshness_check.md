# DCWP Business-License Freshness Diagnostic

**Date:** 2026-06-11
**Type:** Diagnostic (read-only) — followed by a fix to the status-state logic.
**Trigger:** Status page shows business licenses "data through Apr 16, ok" while the
scraper ran successfully Jun 10/11 processing 231 records. Question: real upstream
freeze or watermark-advance bug?

## Verdict

Two independent things are true:

1. **The Apr-16 staleness is a REAL upstream freeze, not a watermark bug.** NYC has
   published no DCWP license with a creation date after 2026-04-16, and has not
   modified the dataset at all since 2026-04-24. Our table's max matches NYC's max
   exactly. The pipeline is faithfully mirroring a frozen source. Nothing to fix here.

2. **The "ok" badge on a two-month-stale source WAS a status-logic bug — now RESOLVED.**
   `/api/status` previously derived ok/delayed from the time of the last *successful
   run*, so a scraper that ran nightly read "ok" no matter how old its `data_through`
   date was. That is why DCWP (Apr 16) showed **ok** while property transfers (Apr 30,
   a *newer* date) showed **delayed**.

   **Fix (api/routes/status.py):** state is now computed by data age, via the pure
   helper `_source_state(key, watermark, last_success, now)`. A source is `delayed`
   once its `data_through` exceeds a per-source threshold:
   - Daily feeds (evictions, HPD violations, 311, permits, DCWP): **7 days**. (Chosen
     over a tighter ~4 days because healthy daily watermarks trail a few days and DOB
     permits routinely sits 4-5 days back; under a week would flap on a healthy feed.
     A week-old daily source is genuinely stuck.)
   - ACRIS property transfers: **21 days** (deeds carry a ~2-week natural publish lag);
     at 42 days it stays delayed, with the upstream-pause note, exactly as before.
   - DCWP business licenses: now reads **delayed** at Apr 16, and carries the same
     "Source feed paused upstream at NYC Open Data." note as ACRIS.
   - Rent stabilization (dhcr_rs): annual snapshot with a null watermark by design;
     judged by last successful refresh against a 400-day window, so it reads **ok**
     and is never falsely marked delayed for being months old.

   `last_success` is still reported per source for the ops view; it no longer drives
   the public badge. Tests in `tests/test_status_api.py` assert, per source, that a
   stale `data_through` with a recent successful run reads delayed and a fresh one
   reads ok.

---

## 1. Our data vs. the live NYC feed (dataset w7w3-xahh)

| Measure | Our `dcwp_licenses` table | Live NYC Socrata feed |
|---|---|---|
| max `license_creation_date` | **2026-04-16** | **2026-04-16** |
| records with `license_creation_date` > Apr 16 | 0 | **0** |
| max `:updated_at` (NYC system field) | n/a | **2026-04-24T19:30Z** |
| our max `created_at` (last new-row insert) | 2026-04-18 | n/a |
| our max `updated_at` (last row write) | 2026-06-11 02:03Z | n/a |

Queries used:

```
# our side
SELECT COUNT(*), MAX(license_creation_date), MAX(updated_at), MAX(created_at) FROM dcwp_licenses;
# -> 28190 rows, max_create=2026-04-16, max_updated=2026-06-11, max_inserted=2026-04-18

# NYC side
GET /resource/w7w3-xahh.json?$select=max(license_creation_date)   -> 2026-04-16T00:00:00
GET /resource/w7w3-xahh.json?$select=count(license_nbr)&$where=license_creation_date > '2026-04-16T00:00:00'  -> 0
GET /resource/w7w3-xahh.json?$select=max(:updated_at)             -> 2026-04-24T19:30:43Z
```

**Conclusion:** the city's own maximum creation date is Apr 16, with zero records
beyond it, and the dataset has had no row-level update since Apr 24. There is nothing
newer for us to pull. Not a watermark-advance failure on our side.

## 2. Why the scraper "processes 231 records" every night

The nightly run count is constant at 231 and the watermark never moves off Apr 16:

```
scraper_runs (dcwp_licenses), last 6 nights — all identical:
  status=success  records_processed=231  watermark=2026-04-16 00:00:00
```

This is expected behavior, not new data. `DcwpLicensesScraper` uses a 14-day lookback
(`WATERMARK_EXTRA_LOOKBACK_DAYS = 14`) so in-place renewals/suspensions near the
boundary are recaptured. With the watermark frozen at Apr 16, the query each night is:

```
license_creation_date > (2026-04-16 - 10 min - 14 days) ≈ > 2026-04-02
```

That window holds the same ~231 rows every night. They are re-upserted (idempotent on
`license_nbr`, so `updated_at` bumps) but none has a creation date past Apr 16, so the
watermark candidate never exceeds Apr 16. The 231 are boundary-window updates, not
fresh licenses. Confirmed by `max(created_at) = 2026-04-18`: no genuinely new rows have
been inserted since the freeze began.

## 3. The ok/delayed state logic (the second, separate bug)

`api/routes/status.py`:

```python
rows = ... WHERE status = 'success' ORDER BY scraper_name, started_at DESC   # latest SUCCESS per source
...
ok = last_success is not None and (now - last_success) <= _OK_WINDOW          # _OK_WINDOW = 48h
state = "ok" if ok else "delayed"
entry["data_through"] = watermark.date().isoformat()                          # reported, but NOT used for state
```

The state is a function of **last successful run time only**. `data_through` (the
watermark date) is displayed to the user but never feeds the ok/delayed decision. So:

| Source | last *success* run | data_through | state shown | why |
|---|---|---|---|---|
| Business licenses (dcwp_licenses) | 2026-06-11 (recent) | **2026-04-16** | **ok** | ran <48h ago; data age ignored |
| Property transfers (acris_ownership) | 2026-05-12 | 2026-04-30 | delayed | latest run is `warning` (recs=0), so last *success* is ~30 days old → >48h |

Note the inconsistency this produces: ACRIS's `data_through` (Apr 30) is **newer** than
DCWP's (Apr 16), yet ACRIS reads "delayed" and DCWP reads "ok." ACRIS only flips to
delayed because its nightly runs now return `status='warning'` (0 records, feed paused
upstream), which drops it out of the `status='success'` filter and ages out its last
success. The freshness logic never actually inspected the data-through date for either.

**Implication (for a future fix, not done here):** a source whose scraper succeeds
nightly but whose watermark is stuck (frozen upstream, or a genuine advance bug) will
always read "ok." To reflect true data freshness, the state should also consider
`now - data_through` against a per-source threshold, not just last-success recency.

## Evidence summary

- DCWP: source frozen at Apr 16 (NYC max == our max, 0 records beyond, dataset untouched since Apr 24). No data-side bug. Nightly 231 = 14-day lookback re-upserts.
- `/api/status`: ok/delayed keys off last-successful-run time, not data-through age — so a stale-but-running source mislabels as "ok." Confirmed against live `/api/status`.
- Top-level site `data_through` is currently 2026-06-09 (driven by the other, current sources), so the overall freshness chip is unaffected; the issue is the per-source DCWP badge.
