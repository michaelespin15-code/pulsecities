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

**2026-08-17, the sharpest instance so far.** The note in this file said to check
`dob_permits`, which paginated on `filing_date` where the base class defaults to
`:id`. The one-line fix took a minute. Writing the test that enumerates the class
took ten, and it failed on **eight** scrapers, not one: every scraper except
`dhcr_rs` had overridden the stable default with its own date column.

Offset pagination only visits each row once when the sort key is unique. Dates
never are. Where rows tie across a page boundary their order between two requests
is unspecified, so the same row lands on both pages and another lands on neither.
Measured live against `wvxf-dwi5`, two consecutive 1,000-row pages ordered by
`inspectiondate` returned 370 duplicates and 1,630 distinct rows where 2,000 were
expected. The duplicates were free, because every scraper upserts on a natural
key. The 370 dropped rows were the loss, and they looked like nothing: the run
succeeded and reported a plausible count.

`hpd_violations` pulls more than one 50,000-row page nightly, so this was live,
not latent: `reconcile_upstream` measured **1,007 rows missing across 19 days**,
all recovered by a rewind once the ordering was stable.

Three things generalize. Ordering by a date reads naturally, which is exactly why
it spread to eight files. The safe default already existed and the overrides were
each written to be helpful. And the estimate of blast radius came from the note
that found the first instance, so it was wrong by a factor of eight: the class is
worth enumerating even when you believe you already know its size.

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

## The reconciler is wired into the nightly run

`run_nightly_pipeline` calls it after the scrapers and before scoring, so a
repair lands in the same night's scores. It rewinds only. Tonight's scrapers have
already run, so the re-read happens on tomorrow's pass through the normal path,
which keeps the nightly wall clock flat: the step costs about 30 seconds.

It is silent the first night a feed drifts, because a rewind that lands is a fix
in progress. It emails only when the watermark is already behind the drifted
range and the gap is still there, which means automatic healing did not work.
A crash in the step can never fail the run; the scrape is already in.

Two things it taught within an hour of being written, both instances of shapes
above:

**A watermark is not private state.** `/api/status` read it as the public
data-through date, so the first rewind advertised a healthy permits feed as three
weeks delayed. That is shape 1: one value serving two purposes drifts apart the
moment one of them moves. Every source with a table now anchors its data-through
to that table via `api/freshness.py`, and the watermark is a resume pointer only.

**Counting rows only works when our table holds one row per upstream row.**
`permits_raw` dedupes on `(bbl, filing_date, permit_type, work_type)`, so 39
upstream rows on 2026-07-15 reduce to exactly the 29 we hold. Reconciling it by
count reports a permanent 25% shortfall that is the dedupe working as designed.
It is excluded, by name, with the reason recorded in `UNRECONCILABLE` and a test
asserting a feed is either reconciled or excluded deliberately. That is shape 3:
a check that reports a healthy system as broken is as useless as one that cannot
fail, and it burns the same credibility.

## Closed 2026-08-17 (later)

- The redundant `/var/backups/pulsecities` prune line is out of the crontab, so
  `backup_db.sh` is the only place retention is written.
- Pagination ordering: fixed across all eight scrapers, guarded by
  `tests/test_pagination_stability.py`, 1,007 lost `hpd_violations` rows
  recovered. Written up under shape 4 above.
- R2 slot age now reports in `weekly_ops_health`, fed by a record
  `backup_offsite.sh` writes after each verified push rather than a second copy
  of the bucket credentials.
- The dead-man's-switch ping is written (`scheduler/heartbeat.py`) and wired to
  every exit path of the nightly run. Inert until `HEARTBEAT_BASE_URL` is set.
- A stale feed now says whether the publisher stopped or our ingest fell behind,
  and only the second one pages nightly. See `stale_cause()`.

## Still manual

- Create the healthchecks.io check and set `HEARTBEAT_BASE_URL` in `.env`. Until
  that exists the ping is a no-op and the box is still its own only witness.
- Add the nightly reconciler to cron. It runs inside the pipeline today, so this
  is only about running it when the pipeline itself does not.

---

## Case, 2026-08-27: a date guard that expired, in seventeen places at once

The clearest instance of patterns 2 and 4 so far, and worth keeping because it
ran for sixteen nights in a crawler-facing file without anyone noticing.

**What happened.** Two ACRIS rows carry a filer-typed `doc_date` of 2026-08-27
on a deed recorded 2026-07-29. They were ingested 2026-08-11. `api/freshness.py`
excluded them correctly the whole time, because its rule was "not dated in the
future" and they sat sixteen days ahead of the calendar. On the morning of
2026-08-27 the calendar reached them, they stopped being future rows, and they
became the newest legal deed date in the table.

**Pattern 2, exactly.** The guard was right when written and expired on a
schedule nobody had written down. `pipeline_health` went CRITICAL to HEALTHY on
a feed frozen since July 31, and `/api/status` published `data_through
2026-08-27` with acris `state: ok`. The fix is the durable form of the same
rule: a record cannot have happened after we wrote it down, so the bound is
`column <= created_at`, which does not expire.

**Pattern 4, also exactly.** Repairing the one reader raised the real question:
who else takes a max over these columns? Seventeen other call sites, none
guarded. The expensive one was `scripts/generate_sitemap.py`, which had been
writing `<lastmod>2026-08-27</lastmod>` onto all 200 hub URLs and onto the typo
row's own property page every night since 2026-08-11. A lastmod a crawler can
prove wrong is worse than no lastmod: the lesson it teaches is to stop believing
the field. `/ops` was the sharpest: it hand-rolled the four freshness subqueries
with a bare `<= CURRENT_DATE` while its own comment claimed it agreed with
`/api/status`.

**Structural fix.** The predicate is `freshness.real_date(column, created_at)`,
so there is one place to change it. `tests/test_date_guards.py` greps for the
pattern rather than exercising today's call sites, because only a grep catches
the eighteenth one. Its `KNOWN_UNGUARDED` list may shrink and must never grow,
and a file cleared to zero has to leave the list, so the backlog fails rather
than rots.

**Trigger.** When a rule earns its own module, grep for everyone who reimplements
it before assuming the module is the only reader. "This function is the canonical
X" is a claim about the codebase, not about the function, and only a grep can
check it.

**The cheap tell.** Ask of any date filter: does this predicate stop being true
as time passes? `< CURRENT_DATE` does. `<= created_at` does not. A guard whose
correctness depends on today's date needs a second bound that does not.
