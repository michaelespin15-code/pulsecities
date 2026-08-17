# Five ways this project breaks, and what stops each

Written 2026-08-17 after a sweep that found eight defects across monitoring and
ingestion. None of them were novel. Each was one of five shapes, and each shape
has a structural fix that is cheaper than finding the next instance by hand.

The common thread: every one of these looked deliberate. Each had a comment
explaining reasoning that had been correct at some point. That is what makes this
class expensive. You cannot spot them by reading for obvious mistakes, because
there aren't any.

---

## 1. A rule written down twice drifts apart

**Seen as:** the ACRIS staleness threshold lived in four files as 21, 21, 14 and
7. On a night when deeds were 17 days behind, the public page said ok,
`pipeline_health` exited CRITICAL, and `daily_health_check` emailed a stale
alert. Backup retention said 7 days in `backup_db.sh` while a crontab line pruned
the same directory at 1 day, an hour later, so the repo described a week of dumps
that never existed.

**Why it survives:** every copy carries a plausible comment, so every copy reads
as intentional. Nothing compares them.

**Structural fix:** one definition, imported everywhere. `api/freshness.py` owns
feed thresholds and the data-through query; the schedulers and the API read from
it.

**Automation:** a contract test that enumerates the consumers and asserts they
agree, rather than testing each in isolation. See
`tests/test_freshness_contract.py::TestSingleSourceOfTruth`.

> If a number appears in two files, one of them is already wrong or shortly will
> be. The test is not "is this number right", it is "do all its copies match".

---

## 2. A guard outlives the thing it guarded against

**Seen as:** `PAGE_SIZE = 500  # keeps per-page memory flat on the 1.9GB droplet`,
on a 4 GB box with zero kernel OOM events in three months. The same era left a
90-day DCWP bootstrap that is why we hold 28,190 of 69,885 rows, an HSTS
`max-age` of one day, an expected-minimum of 500 against a feed frozen at 231,
and a rolling-average check needing 3 runs in 14 days for scrapers that now run
monthly.

**Why it survives:** the comment explains the reasoning, which makes the constant
look current. Nothing re-checks whether the premise still holds.

**Structural fix:** a tuning constant needs a *why* and a *when to re-check*.
`ALERT_SNOOZE` entries now take an `@YYYY-MM-DD` suffix recording when they were
set, and `weekly_ops_health` reports any past 30 days. The DCWP entry came back
at 115 days old on the first run.

**Trigger:** when hardware, a data source, or a cadence changes, grep for
constants whose comments cite the old condition before assuming they still bind.
Changing the box is not done until that grep is done.

---

## 3. A check that cannot fail

The most expensive shape, because a broken check and a healthy system look
identical from outside.

**Seen as:**

- `db_stale_days` of **-10**, from two filer-typed future dates. No threshold is
  exceeded by a negative number, so that check was green by construction and
  would have stayed green through a total ingest failure.
- A DOB freshness probe that ordered by a text column to work around `MAX()`
  sorting a text column. It returned a date from 2021 and never errored.
- A scraper writing `warning_message` every night while `status` stayed
  `success`, and the alerting path only firing on `status == 'warning'`.
- `ALERT_SNOOZE` set to the bare substring `dcwp_licenses`, matched against
  subject and body, silencing that scraper's hard failures and quarantine spikes
  alongside the benign anomaly it was written for.
- Every monitor running on the box it monitors.

**Structural fix:** prove the check can go red. For each monitor, a test that
feeds it a synthetic failure and asserts it fires, not just a test that the code
runs. `TestClassification` asserts a 40-day-old feed classifies as stale;
`TestProbeFailures` asserts a dead probe reports unknown rather than zero, since
zero would read as a surplus and hide a real gap.

> A green check is evidence only if you have seen that exact check go red.

---

## 4. A fix applied to one site instead of the class

**Seen as:** `per_worker()` was written for the slowapi limit, with a comment
explaining that module state is per gunicorn worker, and the same fix was never
applied to the AI cache or the daily spend cap in the same file. `through_sql()`
was created to exclude future dates and adopted by two of the four places that
needed it. `WATERMARK_EXTRA_LOOKBACK_DAYS` guarded late-arriving records on
violations and evictions but not complaints, which is why 311 lost 1 to 3.5% of
every day.

**Structural fix:** after fixing an instance, enumerate the class in code and
assert coverage. A registry plus a loop beats remembering.

**Automation:** `TestFutureDatesNeverCountAsFresh` iterates every configured feed
rather than checking ACRIS; `TestFeedCoverage` asserts every daily feed is
reconciled.

> Fix the class, then write the test that enumerates the class. The instance you
> found is rarely the only one.

---

## 5. Silence read as health

**Seen as:** the box was down for sixteen hours on 2026-08-15 after an unattended
kernel upgrade. Zero scraper runs, no database backup, no offsite push, and no
alert, because the five-minute health probe runs on the box that was down. The
missing day left the Saturday R2 slot holding week-old data, and nothing reports
slot age. Separately, 311 had been losing rows for months while every freshness
check stayed green.

**Structural fix:** require positive proof, not absence of complaint.

- **External dead-man's-switch.** A ping at the end of the nightly pipeline to a
  service off this box. It is the only check that survives the box being down.
- **Artifact presence.** Assert one freshness JSON per day for the last seven,
  and seven R2 slots no older than eight days. A monitor that can silently skip a
  day is not a monitor.
- **Reconciliation against the source.** See below.

---

## The one that finds what you did not think of

Everything above validates an assumption you already had. Reconciliation is the
only check here that can find a bug nobody has considered, because it asks the
source instead of asking ourselves.

`scripts/reconcile_upstream.py` compares our row count against Socrata's, per
feed and per day, over a settled window. On its first run it found 311 short by
2,224 rows across ten days, at a drift that grew with age rather than closing.
Freshness, pipeline health and the weekly heartbeat were all green throughout,
because all three asked "how new is the newest record" and none asked "is the day
we already ingested complete".

It would also have caught the 2026-08-15 outage, a short Socrata page truncating
a walk, and an upstream schema change that quietly dropped a column.

**Calibrate before trusting it.** The first version judged days the source had
not finished publishing and reported healthy feeds as drifted. `settle_days` is
now measured per feed against observed parity, not guessed. A monitor that fires
on a healthy system trains you to ignore it, which is the failure it exists to
prevent.

---

## The self-healing ladder

1. **Detect and alert.** The floor. Still requires a human and still degrades
   into noise if the threshold is wrong.
2. **Detect, remediate, re-measure, alert only if remediation failed.**
   `reconcile_upstream --heal --run` rewinds the drifted feed's watermark and
   re-runs the scraper, then measures again. The first version alerted off its
   opening numbers and emailed a warning about rows it had just recovered, which
   is exactly how an alert channel stops being read.
3. **Prevent structurally, so the bug cannot be written.** A single source of
   truth plus a contract test is the only rung that scales, because it removes
   the failure mode instead of watching for it.

**Heal by rewinding state, not by duplicating logic.** The reconciler does not
re-implement ingest. It moves the watermark back and lets the normal nightly path
re-read the range. Every scraper upserts on a natural key, so re-reading a window
writes only what is genuinely missing, and healed rows go through the same parse,
quarantine and upsert as any other night.

---

## Still manual

- Remove the redundant `/var/backups/pulsecities` prune line from the crontab.
  Harmless now that `backup_db.sh` matches it, but it is a second copy of a rule.
- Add the nightly reconciler and an external dead-man's-switch ping to cron.
- Report R2 slot age in `weekly_ops_health`.
- `dob_permits` paginates ordered by `filing_date`, which is text upstream. The
  3-year window is 30,038 rows against a 50,000 page size. Past that it will
  paginate over a lexicographic sort with heavy ties and can skip rows between
  pages. Switch the order to `:id` before the window crosses the threshold.
