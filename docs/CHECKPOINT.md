# >>> START HERE after /clear (2026-08-29, the assessment-year session) <<<

**Done and verified live.** Code committed, /property deployed, data repaired.
59,423 fabricated year-stamps deleted, 608,240 DOF rows refiled under their real
fiscal years with 0 failed, 50,806 parcels took a corrected assessment. The nine
year counts match Socrata's own `$group=year` row for row, so nothing was lost.

    420 East 58 St, unit 1075   was $287K "for the 2026 tax year"
                                 now $306K for the 2018 tax year
    944 Park Avenue             was $371,324, a 2015/16 figure, now $424K
    40 East 66 Street           was $134,775, a 2011/12 figure, now $155K

All four render sites on the page carry the year: lede, tax section, the FAQ,
and the FAQPage JSON-LD twin. The scraper ran under the pipeline `flock`.

**The spike signal is still dormant at 0 ZIPs**, which is the check that
mattered: assessment_history now holds ten distinct years carrying assessments
where it held one, and the old global year selection would have paired 2026
against 2018 and activated on eight years of drift.

## What was wrong, and it was public

**DOF dataset w7rz-68fs is nine fiscal years, not one.** 2010/11 through
2018/19, 608,240 rows for 72,898 lots, about 8.3 rows each. It is a frozen
archive; DOF stopped publishing to it after 2018/19.

`scrapers/dof.py` fetched all nine years, parsed the year into a local it never
returned, and upserted every row into `parcels` keyed on bbl alone. Whichever
year the paginator emitted last became the assessment. On a random sample of 35
overlapping lots, **30 held a value that is exactly a DOF avtot, and the year
varied per lot**: 2011/12, 2013/14, 2014/15, 2016/17, 2018/19. MapPLUTO could
not correct any of it, because these are condo unit lots and MapPLUTO carries
the billing lot. The two populations are disjoint: 300 dual-covered unit lots
sampled, none in the DOF feed.

`scrapers/pluto.py` then wrote an `assessment_history` row for every parcel
carrying a value and dated all of them with the calendar year of the run. PLUTO
covers 858,602 lots. 918,338 carry a value. **59,423 rows were dated by a
scraper that had never seen the lot.** /property reads `max(tax_year)` and
prints it as fact:

    The city's Department of Finance assessed this lot at $287K
    for the 2026 tax year.

420 East 58 Street, unit 1075. That is its **2014/15** assessment. **10,197
sitemapped pages** carry a sentence of that shape. Verified live before the fix
and again after, through the socket.

## How it was found, which is the reusable part

`scheduler/alerts.py` called `mailer.send` and never imported mailer, so every
ops email had raised NameError since the one-gate refactor the day before.
Chasing why a green suite shipped that turned up **the repo has no linter**.
pyflakes over 290 modules takes four seconds and names both lines.

Running it repo-wide also printed `local variable 'assessment_year' is assigned
to but never used` in `scrapers/dof.py`. That one line is the whole thing above.
`tests/test_undefined_names.py` now runs the undefined-name check in the guard
lane; the other 105 pyflakes findings are noise and are deliberately not
checked.

## What changed

- **dof.py** files one row per (bbl, fiscal year) into `assessment_history`,
  whose primary key is exactly that pair, so pagination order stops mattering.
  The parcel write is DO NOTHING; the value is chosen once after the walk by
  `_sync_parcel_assessments`, DISTINCT ON newest year, and only for lots with no
  newer assessment on file. The year ceiling is **the newest year the run read**,
  not a pinned 2018.
- **pluto.py** snapshots only the lots the run refreshed, and upserts. A
  snapshot may only date what it looked at.
- **The spike signal pairs per lot and requires consecutive years.** Global year
  selection would have paired 2026 against 2018 the moment DOF files real years,
  and called eight years of drift a spike. Clock-free: it keys off the newest
  year in the table, so it still activates on the second annual MapPLUTO run.
- **alerts.py** imports mailer, and `send_ops_email` now honours the "never
  raises" contract its own docstring promised. Two callers alert from inside an
  `except` block, where a raise loses the error being reported.
- **The /property lede names the year.** "Most recent assessed value" is true of
  a 2018 figure and tells the reader nothing.

## Guards, and two that had to be replaced

The two dormancy guards asserted on the source text of the old CTEs. Both went
red on a rewrite that enforces their rule more strictly than before, which is
the tell: **a guard that fails on a change it approves of is not measuring the
rule.** They run the aggregate now against a rolled-back transaction. A rise
counts; a NULL prior year does not; an eight-year gap does not; a fall does not.

Every new guard here was verified by breaking the thing on purpose:

- delete the mailer import -> 8 of 9 delivery tests red, both lines named by
  pyflakes
- delete the consecutive-year condition -> the gap case comes through at **4.7x**,
  because the newest year then pairs with whatever the lot had before it

`_sync_parcel_assessments` commits, so the test that runs it stubs the commit
out. The suite committed deletes of two days of score_history once.

`tests/test_assessment_year_provenance.py` shipped red on purpose, asserting the
live table while the repair was still unrun. It is green now. Keep it: it is the
only thing that would notice a future writer dating a lot it never read.

## State

- Guard lane **286**, 15s. Full suite 1,718 passed. Four reds, none from this
  work: the `.service` deploy drift (known, needs a restart), two pre-existing
  (`test_llc_page_carries_entity_follow`, `test_no_page_claims_the_wrong_zip_count`,
  both red on a clean tree), and `/flips` render budget, which passes alone in
  3.5s and only fails under concurrent load on two vCPUs.
- **The evictions "anomaly" is a bursty feed, not a fault.** 13 records against
  a static floor of 100, but the ingest runs 0, 0, 0, 0, 109, 343, 13 and the
  weekly executed counts are healthy. The floor fires on small nonzero nights
  and is suppressed on zero ones, which is the least informative pairing. The
  DEGRADED label is display-only and feeds no email.
- ACRIS frozen 29 days, upstream, verified. Everything else current.
- 2 unpushed before this session; more now.

# >>> START HERE after /clear (2026-08-29, the privacy-and-anchor session) <<<

Working tree clean. **0 unpushed.** CI **green on both jobs** for the first time
since at least 2026-07-15. Everything below is deployed and verified against the
running site, not just committed. Tonight's pipeline ran clean in 370s.

## Four things to know before anything else

1. **`/privacy` is now a public promise, not a preference.** It states that four
   things are withheld: individuals' names, apartment numbers, tenant names, and
   private home addresses. Any feature that would publish one of those now
   contradicts a page a reader relies on. Check it before building.
2. **ACRIS `doc_date` reads 2026-08-27 and the feed is still frozen at 07-31.**
   Do not read `max(doc_date)` as freshness: it is filer-entered. The scraper's
   watermark is the truth and it says 29 days stale. This is the false-freshness
   class the repo already documents, wearing a new face.
3. **Anthropic credits work again.** The nightly precompute wrote 6 reads, 0
   failed. A previous checkpoint says exhausted; that is out of date.
4. **The evictions scraper flagged an anomaly tonight**: 13 records against a
   static minimum of 100 and a 14-day average of 69. One night is not a trend.
   Worth a look if it repeats.

## The theme, and it is the useful part of this session

**Five bugs, one shape: a correct rule, written down, applied at one call site
and missed at the others.**

| rule | enforcers before | now |
|---|---|---|
| executed evictions are not "filings" | 8 fixed, 16 missed | grep over whole files |
| an apartment number is a household | email only | `api/unit_privacy.py` |
| a private individual is not named | 1 of 8 sites | `api/person_privacy.py` |
| score_history must match the map | opposite conflict policies | upsert + agreement guard |
| what earns a page an index | sitemap ≠ robots tag | `config.nyc.INDEX_MIN_VIOLATIONS` |

Every one now has a single owner module and a guard that fails when a second
copy appears. **When you find a rule stated in a comment, grep for how many
places implement it before trusting any of them.**

## The other lesson, which cost real time

**Six guards this session passed on something other than what they tested.** An
unused import satisfied one. A helper's own docstring satisfied another. "rank"
occurring elsewhere on the page satisfied a third. A regex bounded on the next
top-level `def` over-captured 981 lines and would have passed with the check
deleted; scoping it with `ast` did not help either, because the function is
genuinely 848 lines and names the helper elsewhere inside itself.

The one that finally held asserts the *statement* structurally (an `ast.If`
whose test calls `is_natural_person` and whose body returns `_not_found`), and
it only became trustworthy when I deleted the check and watched it go red.

**A guard written against source text asserts that a word is present, and the
bug is almost never a missing word.** Break the thing on purpose before
believing the test.

## PII: all eight exposures closed

The 2026-08-28 audit found eight. All are fixed, deployed and verified live.

- **`/property` named private buyers and sellers on 43,212 sitemapped pages.**
  Seven render sites, not the three the audit listed; rendering found the rest,
  including the DOF owner-of-record line and the FAQPage JSON-LD twin. Sweeps
  went 16 leaks of 41 pages, then 1 of 48, then 0 of 71.
- **Apartment numbers** on ~13,000 indexable pages and in `/eviction-case`.
- **`/evictions` named 81 individuals** beside eviction counts across 57 pages.
- **`/llc` printed named agents' street addresses**, often residential.
- **`/api/search/landlord` was a people search** over 36,733 individuals
  returning lat/long. Gated in SQL so the summary aggregate and the rows describe
  the same population.
- **`/llc/{person-slug}` 404s.** 40,532 dossiers, ~1,500 words each, with an FAQ
  headed "Who owns or controls <PERSON>?".
- **`/privacy` exists.**

**Two dead ends, recorded so nobody re-derives them:**

- **The portfolio threshold does not work.** The theory was that a person holding
  many buildings is a landlord and fair to name. Of 102 person-shaped owners on
  repeat-eviction buildings, 6 held 5+ buildings citywide and **all 6 were
  misclassified organisations**, including four affordable-housing nonprofits.
  Every genuine individual held one building.
- **`_is_buyer_entity` is not a privacy gate.** It answers whether a name can be
  linked to `/llc/{slug}`, so it is False for servicers, trustees, and every name
  at or over 48 characters where ACRIS truncates. Ten real organisations read as
  people under it.

Cost of all this: **0.27% of impressions.** Person-name queries were 81
impressions and 10 clicks of 29,700 and 731. `/property` ranks on the address.

## The map answers the question now

Michael asked whether New York is in mass displacement. The honest answer was
that the site could not say, because **every number on it was a rank**. Step 5 of
`scoring/compute.py` normalises each signal to the 5th-95th percentile of that
night's spread, so the top 5% fill the top of the scale whatever the city does.
Over 314 days the mean composite moved 1.1 points and its standard deviation did
not move at all, while real executed evictions swung 48% between trough and peak.

`/displacement` now leads with the level, and every neighbourhood page states it
locally in both languages:

    17,009 executed residential evictions in the twelve months to Jul 2026
    against 17,334 in the twelve before. Down 1.9%.
    4.53 per 1,000 apartments a year across 3,753,413 units.

**Whole months, current one excluded.** A window running to CURRENT_DATE ends on
a half-finished month and renders a collapse that did not happen. Guarded three
ways, and per ZIP the artefact would be larger.

## Other fixes worth knowing

- **The history chart and the map disagreed for the same day.** Opposite conflict
  policies: the map upserted, history did nothing on conflict. A manual DOB NOW
  backfill overlapped the 02:07 nightly on 08-28, so history kept scores computed
  against a half-loaded permits table. 156 of 177 ZIPs differed, 8 across band
  lines. The nightly cron takes a `flock` now.
- **The assessment dormancy log lied every night.** It said "<2 tax years" and
  there are seven. `assessment_history` holds two populations: 197,730
  rent-stabilization rows from `backfill_rs_history.py` with `assessed_total`
  NULL, and 917,978 real 2026 assessments. Prior-year resolved to 2023, so of
  32,565 joined BBLs **exactly zero** satisfied the comparison.
- **`dof_assessments` was never broken and I said it was.** The dataset carries
  608,240 rows for **72,898 distinct BBLs**; 73,168 a run is correct. The 500,000
  floor was set against the parcels table. Now 60,000.
- **`/property` titles**: 100% exceeded 60 characters, so `| PulseCities` was
  never displayed on any of 97,790 pages. Mean 77 -> 60.
- **A guard protecting the map palette had never run**, calling `self._app()`
  from a class that does not define it.

## NEXT, in order

1. **`models/database.py` should build its engine lazily.** It raises at import
   time, so importing any module that touches it requires a DSN. That is what
   killed CI collection, and the workaround is a dummy DSN in the workflow. This
   is the real fix and it touches everything, so it wants daylight.
2. **`retire_raw_data.sh archive`.** Still never run. It **deletes nothing**: it
   writes verified archives and is the gate the `drop` phase refuses to proceed
   without. `drop` is what frees ~9.6GB and needs a code commit removing raw_data
   from the models, a reload, and a window, in that order. Verified: nothing
   reads `raw_data` on complaints_raw or violations_raw; `permit_kinds.py` reads
   it on **permits_raw**, which this script never touches.
3. **Backlinks, still zero.** The one lever neither more pages nor better titles
   substitutes for. You now have a finding worth pitching that did not exist
   before: executed evictions down 1.9% year over year, newsworthy precisely
   because it contradicts the panic framing, on a public page with denominators.
4. **`/neighborhood` depth.** The audit measured it as the worst template: 67.6%
   mean overlap, 41 of 190 pairs over 70%, 47.4% unique content against 66%+ for
   every property tier. **The year-over-year sentence did not fix it** and I said
   it would. Re-measured after: 67.1% mean, max up from 79.4 to 81.1. One
   sentence of 45 words against 550 does not move a 5-gram metric.
5. **`WEIGHT_PERMITS = 0.21`**, still never calibrated against a real permit
   signal. Set when that signal was 414 rows; it is 32,786 now.
6. **Bing.** Everything verified working on our side; bingbot crawls 200x less
   than Google. Only Webmaster Tools can say why.
7. **DOS follow-ons**: registered agent as a second clustering signal, Delaware
   surfaced (663 entities), a monthly `refresh_dos_entities --all`.

## NEEDS MICHAEL

1. **Send the FLGSP pitch.** Unchanged, still the highest-value unsent thing.
2. **The `.service` deploy drift.** The only remaining one, and it needs a
   restart rather than a reload, so do it when you can watch it.
3. **A GSC export** if you want a fresh search read. Nothing local is newer than
   2026-08-27.
4. **HEARTBEAT_BASE_URL, ALERT_WEBHOOK_URL, dedicated R2 creds, OPS_TOKEN
   rotation.** Unchanged.

## Traps

- **Do not quote nginx logs as traffic without filtering user agents.**
  Googlebot sends a Google referrer. Filtering to a search referrer looked like
  14,497 property pages earning visits in a day; filtering bots gave **183**.
  Googlebot was 98.8% of it.
- **Do not use n-gram overlap below ~300 words.** Two ViolationScout pages that
  are 92.6% identical token-for-token score 54.1% on 5-grams, because every
  substituted word breaks up n-grams. Short pages score *better* as they thin.
- **`env -u DATABASE_URL` does not reproduce CI.** The box has `.env` and
  `load_dotenv()` finds it. Move the file to reproduce.
- **The guard lane selects by file, not by marker.** An `integration` mark will
  not keep a test out of it; it has to be named in the DESELECT list.
- **`scripts/eviction_flips_editions.json` is gitignored on purpose.** It is
  human review state. Tests that read it must skip when it is absent.

## State

- Guard lane **277** on the box, under 12s. Full suite green; CI green on both
  jobs (`test` 49s, `schema-tests` 2m34s, 1,204 tests).
- Disk 74%, 21GB free. Database 17GB, 13GB of it complaints_raw + violations_raw.
- **Alembic still has pending revisions with their DDL already applied live. Do
  not stamp.**
- 21 confirmed subscribers.
- ACRIS frozen 29 days, upstream, verified. Every other feed current.
- The repo is public and the DB password is in pushed history. Pushing does not
  worsen it; rotating is the fix.


# >>> START HERE after /clear (2026-08-28 night, the relative-index session) <<<

Michael asked why the map still shows Critical, High and Moderate tiles, and
whether New York is in mass displacement right now. The short answer is no, and
the map was never able to say otherwise. Chasing that turned up a real bug that
had already corrupted one day of permanent history.

**One thing needs a human command, and it is one line.** Today's score_history
row was computed against a half-loaded permits table and is still wrong. The fix
is now in the code; the repair is not, because the scoring engine is a
production write:

    venv/bin/python -m scoring.compute

A dry run was checked first and produces max=63.2 avg=31.1, which is exactly
what displacement_scores already holds. **The map will not move.** Only the
2026-08-28 history row changes, from the half-loaded numbers to the real ones.

## Is New York in panic mode? No, and the index cannot answer that question

**The composite is a ranking, not a measurement.** `scoring/compute.py` Step 5
normalizes each signal to the 5th-95th percentile of that night's spread across
the 177 ZIPs. The top 5% of ZIPs fill the top of every signal's scale by
construction, on the worst night in the city's history and on the calmest.

Measured over the 314 days in score_history:

    month      mean score   sd     mean eviction signal
    2025-10       30.2      15.4        39.0
    2026-02       29.9      15.8        35.5
    2026-08       30.5      15.2        33.0

The mean composite has moved 1.1 points in ten months and the spread has not
moved at all. Over the same window, actual executed evictions citywide ran
between 1,246 and 1,842 a month, a 48% swing between trough and peak. **The
index absorbed the entire swing and reported nothing.** That is not a bug in the
normalization; it is what percentile normalization is for. It is a bug in what
the words on top of it promise.

So "7 Critical, 26 High" is not a measurement of how bad New York is. It is the
top 4% and the next 15%. Those counts have been near-constant since October and
would look the same if every eviction in the city stopped tomorrow.

**The tiles are still telling the truth about rank.** The seven Critical ZIPs run
9 to 13 executed evictions per 1,000 apartments a year against a citywide median
of 3.4 and a citywide rate of 4.3. East Tremont is 12.6, University Heights 12.0,
Belmont 11.8. Three times the median is real and worth a colour. But 12.6 per
1,000 is 1.3% of homes in a year, which is chronic pressure, not an emergency,
and by area the map is 61.8% Low.

**What is actually wrong is that nothing on the site has an absolute anchor.**
NEXT item 5 in the last checkpoint asked whether "Critical" is the right word and
called it editorial. It is not only editorial. Renaming the band leaves the
deeper hole: a reader cannot ask this site "is it getting worse?" and get an
answer, because every number on it is a rank. The honest fixes, in order of
weight: rename the top band (touches emails, alerts, Spanish); or add one
absolute citywide measure, executed evictions per 1,000 apartments per month
against its own twelve-month trend, which is the number the index throws away.
Neither was done unilaterally.

## The bug this uncovered: history and the map disagreed about the same day

`compute_scores()` writes the same numbers to two tables in one pass, and the two
writes had opposite conflict policies:

    displacement_scores   ON CONFLICT DO UPDATE     last run of the day wins
    score_history         ON CONFLICT DO NOTHING    first run of the day wins

Harmless on a day that scores once. Wrong on every day that scores twice, which
is exactly what a data fix looks like.

**What happened on 2026-08-28**, from the logs and table timestamps:

    01:52:55   manual DOB NOW backfill starts, 484,000 permits
    02:07:38   nightly pipeline scores  ->  max 60.2, and this is what history kept
    02:15:44   backfill finishes
    02:21:50   a recompute runs        ->  max 63.2, and this is what the map shows

The nightly run scored against a permits table that was still being loaded. Its
numbers went into the permanent record, and the correction fourteen minutes later
was silently discarded by DO NOTHING. **156 of 177 ZIPs disagreed between the
chart and the map, by a mean of 0.92 points and a maximum of 8.0. Eight sat in
different bands: 10452 and 10457 read Critical in the trend chart and High on the
map on the same afternoon.**

The recovery advice in the pipeline's own alert text said to re-run the scorer,
"idempotent same-day". Under DO NOTHING that advice could not work. It works now.

## Why three separate instruments all said fine

1. **The pipeline's snapshot guard counted rows.** 177 stale rows count exactly
   like 177 fresh ones. It now counts per-ZIP disagreements against
   displacement_scores instead, and fails the pipeline when any row differs by
   more than 0.05.
2. **The live-vs-history drift check compares averages, at a 30% tolerance.**
   The averages were 31.1 and 31.67 -- a 1.8% difference -- while 156 ZIPs
   disagreed. Averaging is the wrong instrument for per-row divergence because
   the errors cancel. `scripts/pipeline_health.py` prints max=63.2 for live and
   max=60.2 for history on adjacent lines and calls the drift OK. The threshold
   was left alone; the per-ZIP count is the right instrument and now exists.
3. **`scraper_health_label()` has a DEGRADED branch that could not fire.** The
   only caller passed `None` for expected_min and the query never selected the
   column. On the night DOB NOW returned 0 records against a floor of 120 -- the
   feed whose own config comment says "watch dob_now_permits: this one going
   quiet is the plan" -- the one report a human reads printed OK. Now wired.

## What changed

1. **score_history Step 7 upserts** instead of dropping later runs, and assigns
   updated_at in the SET list.
2. **`snapshot_scores()` is deleted.** It was a second writer to score_history,
   copying displacement_scores after the fact. displacement_scores carries no
   hpd_violations or rs_unit_loss column, so every row it wrote had two of six
   signals NULL. Nothing in production called it; it survived as a re-export in
   scheduler/pipeline.py "for test imports", kept alive only by its own three
   tests. Those tests are gone with it.
3. **The pipeline guard checks agreement, not row count** (above).
4. **pipeline_health passes the real expected minimum** (above). The report now
   shows dob_now_permits and dob_permits DEGRADED at 0 records, and
   dof_assessments DEGRADED. Labels are display-only and feed no email, so this
   adds no alert traffic.
5. **The nightly cron takes a flock.** It had none, alone among the crons on this
   box. `bash -c` is load-bearing there: `flock LOCK cd /x && cmd` parses as
   `flock LOCK cd /x` followed by an unlocked, un-cd'd command, which is worse
   than no lock. **This is a deploy-drift change** -- `cp deploy/pulsecities.cron
   /etc/cron.d/pulsecities` when convenient. The lock only separates two pipeline
   runs; a manual backfill has to take the same lock to be covered.
6. **The band docstring in api/routes/neighborhoods.py said 85-100 / 67-84 /
   34-66**, the pre-recalibration numbers, in a comment that claims to be the
   mirror of the map legend. Behaviour was never wrong -- it calls
   `scoring.tiers.tier()` -- but the block a reader trusts was three days stale.
7. **tests/test_score_history_agreement.py**, six greps, in the fast lane.

## Later: an SEO sweep, and the word "filings" again (22feb50)

**Sixteen surfaces still called executed evictions "filings".** The 08-28 fix
covered the panel, the summary, the og-image and the digests, and its guard
asserted on the /this-week body alone, so the rule had one enforcer and many
readers. Every one of the sixteen reads `evictions_raw.executed_date`. The
biggest is the **/property meta description on ~97,790 pages**; the loudest is a
**section heading in Sunday's digest**, in a file whose line 448 already said
"executed residential eviction". The Spanish was already right, same split as
before.

**The grep that found them reads whole files, not lines.** The /property
description writes `"... deed transfers, eviction "` and `f"filings, and ..."` on
two source lines, so a per-line grep walks straight past the largest surface. It
also collapses the concatenation without dropping newlines, or every reported
line number shifts.

**Two source attributions were wrong with it.** Dataset 6z8x-wfk4 is the Marshal
eviction records, not housing court data. Only OCA petitions are housing court,
and that copy already reads "petitions filed" correctly.

**app.html's social description was worse than mislabelled.** It claimed "six
public data signals" and then named rent stabilization twice while omitting 311
complaints, which carries 0.17 and is the third-heaviest input.

**The title budget: 100.0% of /property titles were truncated.** Google renders
about 580px, roughly 60 characters. Measured over a random 20,000-parcel draw the
old titles ran mean 77, min 65, and every single one was over 60, because the
fixed tail was 41 characters before the address. **So "| PulseCities" was never
displayed on any of those pages**, which makes dropping it free, and the 14
characters it returns carry "deeds, evictions, permits", the only words that say
why to click this instead of Zillow. Mean 60 now, 46.4% over. This is the lever
the 2026-08-27 read pointed at: ranking without clicking, and /property is 88% of
traffic.

**The map palette guard had never run.** `test_the_two_hot_fills_are_tellable_apart`
calls `self._app()`, defined on `TestCanonicalTierBands`, and was written into
`TestSearchResolvesDeedBbl`, so every run since it shipped ended in
AttributeError rather than measuring a colour. It reads the file directly now and
passes: the two hot fills really are 35.5 dE apart. Same guard-rot shape as the
eviction label, found the same night.

Guard lane is 231 now. Three new files: `test_score_history_agreement.py`,
`test_eviction_label.py`, `test_title_budget.py`.

**Technical SEO is otherwise clean**, measured live over 24 sampled pages across
the three big templates: all 200, canonicals all correct, median warm render
0.08s, no non-200 in the sample. 98,847 URLs in the sitemap. Move 09 (the
violation-gated expansion) is confirmed shipped, contrary to three older
checkpoint sections that still list it as open.

## Found, not fixed, worth a look

- **`dof_assessments` loads 73,168 records every run against a floor of 500,000**,
  the identical count on 2026-07-09 and 2026-08-09, which is the signature of a
  pagination cap rather than a quiet source. assessment_history holds 917,978
  rows for tax year 2026 and 28k-40k for each of 2018-2023, and **2024 and 2025
  are missing entirely**. That is why the assessment signal is dormant and why
  compute.py logs "<2 tax years" every night: only one year has real coverage.
  It carries no weight today, so nothing user-facing is wrong, but the signal
  cannot activate until this is fixed. This is the same unstable-pagination shape
  documented in docs/ops/failure_patterns.md.
- **The history chart has a cliff on 2026-08-28 that the city did not cause.**
  The permit recompute moved the mean ZIP 4.2 points with a maximum of 20.8, and
  87 ZIPs moved 3 or more. A normal night moves the mean ZIP 0.24 points with a
  maximum of 3.1 and one ZIP over 3. Rockaway Park reads 50.0 then 30.6; the
  Upper West Side reads 10.0 then 30.8. The weekly digest was taught to disclose
  this (0140f12) and `scoring_changes` already holds the row. **The chart on ~177
  neighbourhood pages was not**, and a reader looking at a 20-point overnight
  drop has no way to know it was our arithmetic. The mechanism to annotate it
  already exists.


# >>> START HERE after /clear (2026-08-28 later, the read-and-tiers session) <<<

Working tree clean. **141 commits unpushed.** Full suite 1,261 passed, 1 failed,
and the failure is the deploy-drift check that is NEEDS MICHAEL item 3 rather
than a bug. Nothing is running in the background.

## Four things to know before anything else

1. **Tonight's 02:00 pipeline runs three new steps for the first time**: the read
   precompute, the page warmup, and the digest's scoring-change check. All three
   are non-fatal by construction. Grep /var/log/pulsecities/scraper.log for
   "Page warmup" and "Precompute complete".
2. **Two tables were created live and their migrations are NOT applied through
   alembic**: `ai_summaries` and `scoring_changes`. That makes **seven pending
   revisions**, every one with its DDL already on the live database, which is the
   same pattern as the previous five. **Still do not stamp.**
3. **The read costs about $6 a month** in Anthropic spend now, on top of a $2.51
   one-off sweep already paid. It regenerates only when a score moves a full
   point or crosses a tier.
4. **Sunday's digest carries a correction notice** because the permit recompute
   moved 88 ZIPs and that is our change, not the city's. It disappears by itself
   after 2026-09-03, when the scoring_changes row leaves the seven-day window.

## The map was rendering in two colours, and the bands were why (e1622b8, 436d945)

The bands were 34 / 67 / 85 and the highest score New York has ever produced is
63.2, so the top two could not be occupied: 73 of 177 ZIPs sat in one amber
block, the og card printed "0 ZIPs at High risk" every week, and the weekly
digest's crossing-into-High check could never fire. Bands are 34 / 45 / 55 now,
set against the live distribution: 104 Low, 40 Moderate, 26 High, 7 Critical. No
score changed and no history is affected.

**Then Michael said the orange and the red look alike and the map reads like an
emergency. Both halves were right.** The two hot fills measured 23.9 dE apart
where every other step of the ramp is 39 or more, so a four-band legend was
describing a three-colour map. Critical's fill is #b5252b now, 35.5 dE from the
orange and deeper rather than brighter.

**Critical is deliberately two colours from here**: fill #b5252b, text #e4483b.
One value cannot both survive beside the orange as a polygon and hold contrast as
a number on the dark panel. The band stays shared; only the palette forks.

The High band was also carrying 14.4% of the mapped area at 0.82 opacity, which
was most of what read as alarm. By area: Low 61.8%, Moderate 21.2%, High 14.4%,
Critical 2.6%. High is 0.66 now, so seven ZIPs stand out rather than thirty-three
shouting together. Cut points were left alone on purpose: the 45-to-54 band is
ranks 8 to 33 citywide, and 10457 is rank 1 on both executed evictions and HPD
violations at 54.9.

**Open, and editorial rather than technical:** "Critical" is an absolute word on a
relative index. The colours match the data now. If the vocabulary still
overstates, the honest fix is renaming the top band, which touches emails, alerts
and the Spanish copy, so it was not done unilaterally.

## What changed, in one line each

1. **The panel read runs claude-opus-5 and compares instead of restating**, and
   is generated off the request path: 0.2 to 0.9s where it was 5 to 12.
2. **The eviction signal is no longer called "filings" anywhere it is counted.**
3. **/evictions/{borough} exists**, the tier the 127 leaves never had.
4. **/property says taxes, sales history and owner**, and finally shows the
   violations it had only been counting.
5. **CI has a second job with a real Postgres** running 1,158 tests, up from 219
   grep-level assertions.
6. **The score bands sit where New York actually scores**, and the two hot map
   fills can be told apart.
7. **/flips renders in 1.4 seconds instead of 38.**
8. **Sunday's digest no longer reports our own recompute as the city's news**,
   nor prints "0 LLC acquisitions" when ACRIS has simply gone quiet.

## The read (0eff4a1, 1d0cdf1)

Michael asked for the model change and for the read to stop being templated. Both
are done and live.

`claude-opus-5`, adaptive thinking, medium effort, on the panel read and on both
digest narratives. **Thinking tokens are charged against `max_tokens`**, so the
old 400 and 300 would have returned empty paragraphs rather than short ones; both
are 2000 now and the length limit lives in the prompt. Cost measured from the new
`summary usage` line in the journal: ~1,600 in, ~260 out, about $0.015 a
generation and under $3 for a sweep of all 177 ZIPs.

The prompt rewrite matters more than the model. The read used to open with the
score and walk the ranked signal list, which is the panel restated. It is now
handed what a reader cannot work out: citywide rank per signal, the rate per
1,000 apartments, the same 90 days a year earlier, the score a season back, the
most active LLC buyer, and filings proposing to remove homes. Whole context block
measures 0.3s to 0.9s; a cold read is 5s to 10s and cached per ZIP per scoring
run.

Sampled five ZIPs at random rather than by score: 103 to 117 words, five
different openings, every one led with a comparison.

**Two data findings the enrichment forced:**

- **`rs_unit_loss` is 0.0 for all 177 ZIPs**, dormant since the DHCR multi-year
  dataset was decommissioned, and it still carries `WEIGHT_RS_UNIT_LOSS = 0.15`.
  Ranked naively every ZIP came out rank 1 and the model duly told two of them
  "no rent-stabilized units were recorded lost". It renders as "not measured"
  now, and the prompt says not measured is not zero. **The weight is still
  Michael's call.**
- **Signal scores and per-1,000 rates are different scales.** Handed both, the
  model called a rate a multiple of a median signal score.

## The eviction label (0eff4a1)

The signal counts warrants a marshal executed, and eight strings called them
filings. The tooltip two lines under the panel label already read "Executed
residential evictions by NYC marshals. Filed cases that resolved without
execution are not counted." The Spanish was corrected on 2026-08-19; the English
never was. Panel label, count line, lag note, the deterministic summary sentence,
the brief, the og-image and both digests now agree.

## Move 05, the borough tier (cb8ee91)

`/evictions/{borough}` for the five, sharing the leaf route and its cache. No
neighbourhood is named after a borough, so the namespaces cannot collide.

Each page carries what neither other tier can say: every neighbourhood page it
parents with 30-day, 12-month and total counts; the rate per 1,000 apartments;
the buildings with three or more executions and who holds their deeds; the
borough's share of the citywide record. **The Bronx runs 21.5 executions per
1,000 apartments and 32% of every execution the city has published.** On raw
counts Brooklyn looks comparable. Per apartment it is not close.

Linked in three directions, sitemap and llms.txt carry them, eight guards
including one that fails if a borough page omits a neighbourhood it claims to
parent.

Found while building it: the leaf pages had been shipping `sib-list`, a class
_LLC_PAGE_CSS does not define, so both record lists on all 127 pages rendered
with default bullets.

## Move 06 and the violations (eabc818, 9abfdcb)

Headings now use the phrases the search exports show people typing: "Who owns
{address}", "Sales and ownership history", "Open code violations", "311 complaint
history", plus a new "Taxes and assessed value" block, since the assessed figure
was one clause of the lede and "taxes" is a live query.

**The FAQ named a signal the score does not read.** It told every visitor the
composite reads "assessment spikes", which carries no weight and is NULL for all
177 ZIPs, and omitted HPD violations, which carries 0.08. That answer sits in
FAQPage schema on ~97,000 pages. The guard now derives the list from the weights.

**The bigger find: 27,825 buildings are in the sitemap because they carry five or
more violations, and the page printed the count and not one row.** /property
carries a Violation history table now, eight rows, newest first, class and status
beside each, with the statute prefix stripped so a row reads "Repair or replace
the carbon monoxide detecting device" rather than "§ 27-2045(B)(1)(B) HMC, §
12-06...".

## The duplicate guard, which could not see any of that (NEXT item 4)

It drew four pages, six pairs, always the same four, from the richest tier the
sitemap admits. Measured properly, five draws of eight per tier, 140 pairs each:

    tier             mean  median  p95  max  pairs at or above 70%
    deed+eviction     60%    60%   68%  73%   1 of 140
    deed only         66%    66%   70%  78%  11 of 140
    violation only    55%    56%   60%  62%   0 of 140   (was 63%/77%)

The violation tier moved because the page now shows its violations. **Deed-only
pages remain the weak tier and it is not a formatting problem**: single-deed one-
and two-family houses have an address, a year, a unit count, an owner, one deed
and one assessed value, and nothing else on record. Padding would be dishonest.
**The fix is the sitemap gate, which admits one deed where the violation tier
insists on five, and that is a call about what belongs in the index.**

The guard now samples every tier by random draw and asserts on the mean as well
as the max, because a mean that rises is boilerplate added to the template.

## CI now has a database (b92176b, NEXT item 3)

Second job: postgis/postgis:14-3.3, `alembic upgrade head`, then
`pytest -m "not integration"`. Two things it catches that the guard lane cannot.
**The migration chain has to apply from nothing**, which is proven nowhere else:
the production database was migrated one revision at a time over months and would
not notice a chain that no longer replays. It applied clean, all five pending
revisions included. And **1,158 tests run for real** against 219 grep assertions.

A runner cannot hold 918,000 parcels, so the 36 tests that assert on rendered
records carry a new `needs_data` marker and skip there. `tests/conftest.py`
decides by asking the database whether `parcels` has a row, not by reading an
environment variable, so they all still run on the box.

## State

- Full suite on the box: **1,261 passed, 1 failed**, the failure being the
  deploy-drift check, which is NEEDS MICHAEL item 3.
- **Alembic: seven pending revisions**, every one already applied to the live
  database by hand. The two newest are `ai_summaries` (d9c3a71e40b8) and
  `scoring_changes` (e2b71c4d9a35). **Do not stamp.** The chain does replay from
  an empty database, which the CI job now proves on every push.
- Disk 73%, 22GB free. The database is 17GB, of which **9.6GB is `raw_data` in
  two tables nothing reads**: complaints_raw 7.1GB and violations_raw 2.5GB.
- ACRIS still frozen upstream at 2026-07-31, now 28 days. The other feeds are
  current. The nightly pipeline ran clean at 02:00 in 450s.
- All 177 AI reads are warm in `ai_summaries`; the sweep cost $2.51, none failed.
- The block digest still sends 2026-09-01 10:00 ET to ten subscribers. Dry-run
  re-checked after today's label and band changes and it reads correctly.
- 21 subscribers, all confirmed, 13 of them from the last seven days.

## Things worth knowing that nobody asked about

- **tests/test_render_budgets.py may be the flaky one.** It budgets a cold render
  at 8 seconds; /flips measured 7.0s cold on a reloaded process with cold
  Postgres buffers. If it goes red intermittently, raise the budget rather than
  delete the test, and check whether the cold number itself has moved.
- **ClaudeBot's 113k-request day may or may not repeat.** If it becomes daily it
  is sustained load on a two-vCPU box, and the property page's 512-entry cache
  cannot hold a 97,000-URL walk. It is also the AI grounding channel the search
  read calls valuable, so throttling it would cost something real. Watch it
  before acting.
- **Credential scanners are spoofing Claude-User and ChatGPT-User** user agents.
  They are noise in the logs, but they are also probing for /.aws/credentials and
  /config/.env on a box where those files exist elsewhere. Nothing was exposed:
  every one of those requests 404s or 429s.
- **Three tables inside the violation_leads database are owned by
  `pulsecities_user`.** Rotating that password is safe. Dropping or renaming the
  role is not.
- **The precompute writes reads for ZIPs nobody visits.** That is the trade for
  never making a reader wait, and it is bounded at about $6 a month plus the
  daily cap of 500 generations.

## Later the same day: latency, cost, and an email that blamed the city

**/flips took 38 seconds to render cold (6ef309e).** `reno_permits` scanned a
year of permits_raw and grouped by bbl, then inner-joined that against the LLC
purchases and threw the rest away. That scan was small while the renovation rule
only matched legacy BIS rows; the DOB NOW backfill made it match 96% of the
record, and the BIS half is a `raw_data->>'job_type'` extraction no index can
serve. **The page got slower by being fixed.** Scoping the scan to the lots that
can qualify: query 29.6s -> 1.0s, page 38.2s -> 1.4s, same 60 rows.
tests/test_render_budgets.py is the guard, loose at 8s on purpose.

**The read is off the request path (7f9b262).** `ai_summaries` is a shared cache
both workers see and a reload does not clear, and scripts/precompute_reads.py
fills it in the nightly run. After a reload, which used to be the worst case:
**0.22s to 0.9s against 5 to 12 seconds.** All 177 are warm; the sweep cost $2.51
and none failed.

**The regeneration trigger is a full point or a tier crossing, not any change.**
Measured over 21 nights: any-change is 105 ZIPs a night, $45/month; a full point
is 14 a night, $6/month. A ZIP moving 41.3 to 41.5 produces the same paragraph.
`api.routes.ai_summary.is_fresh` owns it and both callers import it.

**Sunday's digest was going to report our own correction as the city's news
(0140f12).** "87 ZIP codes moved by 3+ points this week" was the permit recompute
landing on every ZIP at once. `scoring_changes` records that, and the line now
says so. Two more in the same email: the movers query compared against yesterday
while the copy said "this week", and "0 LLC acquisitions" was ACRIS having
published nothing since Jul 31 rather than a quiet week.

**The nightly run warms the five expensive pages (c233236)**, because what
remains after the query fix is buffer-cache cost: 7.0s cold after a reload, 0.02s
to 1.3s once warmed.

## PulseCities and violation-leads share a Postgres cluster, nothing else

Asked and verified, because both have a table called `violations_raw`:

    pulsecities      17 GB    owner pulsecities_user
    violation_leads  5.9 GB   owner violation_app

`violation_app` holds **no grants** on any pulsecities table, there is no dblink
or postgres_fdw and **zero foreign tables**, and this repo never names the other
database. violation-leads exits at startup if its DATABASE_URL contains
"pulsecities". The raw_data drop cannot reach it.

**One legacy thread runs the other way:** 3 tables inside `violation_leads`
(328 MB) are owned by `pulsecities_user`. Rotating that password is safe, since
ownership is not password-bound. **Dropping or renaming the role is not.**

## Who is actually crawling, and what for

Measured 2026-08-28 from the nginx logs.

**ClaudeBot made 113,127 requests, 72% of all traffic, and it is brand new**: it
made zero on Aug 21, 25 and 27, then walked 97,414 property pages and 14,544 LLC
pages in a day. That is a bulk index crawl, not people asking questions.
112,709 of them were 200s.

**People actually asking an assistant is a much smaller and real number.**
Filtered to content pages returning 200:

    ChatGPT-User      93   (92 of them /property)
    OAI-SearchBot     63
    Claude-User        0
    Perplexity-User    0

**Do not read the raw user-agent counts.** Claude-User showed 396 hits and
Perplexity-User 274, and none of them were content: they are credential scanners
spoofing those agents to probe /.aws/credentials, /config/.env and /db.sql. Any
future log read has to filter to real paths with 200s before quoting a number.

Also crawling: AhrefsBot 7,241 and SemrushBot 1,736, which are SEO tools building
link indexes, and Googlebot 7,587 with 7,512 200s and a single 404.

**The load context for the perf work:** a crawler walking 97,000 distinct
property URLs goes straight through the 512-entry in-process page cache, so
almost every one of those is a cold render.

**No human number can be taken from these logs.** Assets are cached hard so real
browsers make almost no asset requests, and several heavy IPs are Google ranges
with no bot user agent. Plausible and Search Console are the sources.

**Subscribers: 21, all confirmed, 4 in the last two days and 13 in the last
seven.** Mostly building watches, one entity follow.

## Bing: everything on our side works, Bing is simply not showing up

Asked because it felt under-indexed. It is, and it is not for a reason we
control.

Verified working: IndexNow runs nightly at 03:28, submits 5,000 URLs per run in
batches of 1,000, **every batch returns HTTP 200**, and 49,261 URLs have been
submitted to date. The key file at /4494ce2738a74028c1babaef305aec53.txt serves
200. The site is verified in Bing Webmaster Tools (`msvalidate.01` is in the
markup). Fetching as bingbot returns a full 200 with the current content.

The crawl itself:

    date      bingbot   Googlebot
    Aug 18      690        --      (52 of them 429, the old 5r/s limit)
    Aug 21      889      17,731
    Aug 23      530       5,706
    Aug 25       81         509
    Aug 28       34       7,587

So Bing crawls roughly 200x less than Google and is trending down. **The 429
episode on Aug 18 is documented but does not explain it**: bingbot kept crawling
through Aug 23 afterwards, and there have been no 429s since. Both crawlers are
bursty; Googlebot itself fell to 108 on Aug 27.

**What would actually answer it**: Bing Webmaster Tools' Crawl Information and
URL Inspection, which the site is verified for and which says directly whether
pages are indexed, discovered-not-crawled, or excluded. Logs cannot. The durable
lever underneath is backlinks, still zero, which Bing weights harder than Google
for a young domain.

The 5,000-per-run IndexNow cap was left alone. The protocol allows 10,000 per
request, and the comment in scripts/indexnow_submit.py explains the cap exists to
avoid the endpoint's own rate limiting.

## Two calls that were open, both now answered by measurement

**The deed-only sitemap gate: leave it alone.** The hypothesis was that
single-deed one- and two-family houses are the thin tier dragging the template,
and it does not survive being measured. Of 44,418 deed-only pages, the 18,200
that carry a deed and nothing else on a small building overlap at mean 67%, and
the rest of the tier overlaps at mean 66%. There is no thin subset to cut. More
to the point, `/neighborhood`, the template the SEO plan holds up as the good
one, re-measured the same day at **mean 67%, max 77%**, which is where the
deed-only tier already sits. Cutting 18,200 real pages would buy nothing.

**`WEIGHT_RS_UNIT_LOSS = 0.15` is not being wasted, and the previous checkpoint
was wrong to say so.** scoring/compute.py Step 5.5 redistributes dormant-signal
weight across the active signals and has done since before this session; a
signal that is 0.0 for every ZIP contributes no weight at all. The read still
renders it as "not measured" rather than zero, which remains correct. What was
real is the band problem it pointed at, fixed above.

`WEIGHT_PERMITS = 0.21` is still uncalibrated against a live permit signal, and
that one stands.

## NEXT, in order

1. **Run `retire_raw_data.sh archive`.** Safe any time, read-only against the
   database, and it has never been run. It is the prerequisite the `drop` phase
   refuses to proceed without, and running it turns the maintenance window from a
   long job into a short one. 9.6GB is waiting behind it.
2. **Look at Bing Webmaster Tools.** Everything on our side is verified working
   and bingbot still crawls 200x less than Google. BWT's Crawl Information is the
   only thing that can say whether pages are indexed, discovered-not-crawled, or
   excluded, and the site is already verified there.
3. **DOS follow-ons.** Registered agent as a second family-clustering signal,
   Delaware jurisdiction surfaced (663 entities), a monthly
   `refresh_dos_entities --all`.
4. **Calibrate `WEIGHT_PERMITS`.** Still 0.21, set when the permit signal was 414
   rows. It has never been calibrated against a real one.
5. **Decide whether "Critical" is the right word** for the top band of a relative
   index, given the colours now match the data.

## NEEDS MICHAEL, priority order

1. **Send the FLGSP pitch.** Unchanged and still the highest-value unsent thing.
2. **Decide on the push.** 141 commits, and CI can go green on both jobs.
3. **Restart with the committed service unit**, then rotate the DB password, then
   take the maintenance window (`alembic upgrade head` + `retire_raw_data.sh
   drop`). Still the one red test. Note that three tables in the violation_leads
   database are owned by `pulsecities_user`: rotating the password is safe,
   dropping or renaming the role is not.
4. **HEARTBEAT_BASE_URL, ALERT_WEBHOOK_URL, dedicated R2 creds, OPS_TOKEN
   rotation.** Unchanged.


# >>> START HERE after /clear (2026-08-28, the permit-record session) <<<

Working tree clean. 20 commits, none pushed (125 unpushed in total). Everything
below is done, deployed and verified live. Full suite 1,543 passed, 1 failed,
and that one failure is NEEDS MICHAEL item 4 rather than a bug.

## Things with a clock on them, read these first

1. **The block digest sends for the first time on 2026-09-01 at 10:00 ET** to
   ten real subscribers, Michael among them. It has never sent. The cron is
   installed in /etc/cron.d/pulsecities. To hold it, comment that line out. A
   preview of all ten was rendered and reviewed on 2026-08-28 and read
   correctly.
2. **A real subscriber has a wrong email from this morning.**
   `ladycarmenn@aol.com` was sent "New at 1062 Elton Street: 5 new records" at
   03:25, listing permits filed in 2023 and 2024 that a backfill had just
   loaded. The dates on them were right; the word "new" was not. The cause is
   fixed. **Whether that deserves a correction is Michael's call and nothing
   has been sent.**
3. **The GitHub repo is public** (`isPrivate: false`, unauthenticated fetch
   returns 200) and the live DB password is in its pushed history. The previous
   checkpoint called this "defence in depth, not an open door" on the grounds
   that Postgres is localhost-only behind ufw. That is still true and it is not
   the same as private. Pushing does not worsen it: the only unpushed commit
   touching the password is a4ded27, which *removes* it. Rotating is the fix.
4. **Tonight's 02:00 pipeline is the first to run the seventh scraper**
   (dob_now_permits) on its own. It has run twice by hand and once
   accidentally under the nightly, all clean. If it misbehaves, the log is
   /var/log/pulsecities/scraper.log.
5. **IndexNow has ~28,000 new URLs queued** from the sitemap expansion and caps
   at 5,000 a run, so it drains over about six nights. That is by design, not a
   backlog to clear.

## The through-line of the whole session

Every significant find was the same shape: **something that looked healthy and
was returning almost nothing.** Not one of them raised an error, and several had
a comment or a disclosure line nearby explaining the symptom away.

If you only remember one heuristic from this session: **when a feature looks
fine, check what volume it returns against what it should return.** That is what
found all of the following.

## Shipped

**The permit signal was 24.7% of the composite score and was computed from 414
records (bc39644).** `scrapers/permits.py` reads `ipu4-2q9a`, the legacy DOB
Permit Issuance dataset, and DOB NOW superseded it. New scraper on `w9ak-ipjd`,
485,443 rows backfilled from 2021, and the signal is now 32,786 records.

Every displacement score was recomputed. ZIPs with a nonzero permit signal went
105 -> 168 of 177; median rank shift 7 places, max 79; **half the top ten
changed.** 11694 Rockaway Park fell from rank 17 to 96 as its permit signal
collapsed from 85.7 to 6.1. The new top ten is the South Bronx plus Harlem and
Bed-Stuy.

**Why it survived five audits**, which is the part worth keeping: the scraper
ran green nightly reporting ~34 records, which reads as a quiet feed. The
anomaly check *did* fire three times in fourteen nights and reached nobody,
because BaseScraper only escalates a low count to an email when the feed's
expected minimum clears 100 and this one sat at 50. Then pipeline_health, the
one report a human reads, carried a hand-written note for exactly that pattern:
"post-bulk-recovery; rolling-avg warning expected". Correct when written, stale
by the time it mattered. All three fixed (f6a95b8).

The other four feeds were checked the same way and are fine: HPD violations
98.9%, evictions 98.1%, DCWP 100%, 311 at 99.9% net of ingestion lag.

**Flip Watch was finding 15 flips a year and should have found 639 (d265361).**
Four queries hand-rolled `raw_data->>'job_type' IN ('A1','A2')`, a column only
legacy BIS rows have. 90d went 1 to 115, 180d 6 to 279, 365d 15 to 639. This is
the feature the MTEK and PHANTOM pitches are built on. `api/permit_kinds.py`
owns the rule now.

**Every deed window ended at the calendar rather than at the data (cd1a076,
05ffec9).** ACRIS was 28 days behind, and the shortfall scales with the lag over
the window: a 30-day deed count read **1** where it should read **558**; radar's
90-day window found 4 clusters instead of 11. The 30-day case set
`signal_counts.llc_acquisitions` to zero for every ZIP in /api/stats, so the
dominant-signal label on the site's central ranking could never say an LLC was
buying. Thirteen sites fixed. A disclosure line for this class had shipped on
2026-08-18 and was only half the fix.

**The block digest (93859bd), and the hole the backfill opened in it
(5ecade9).** Monthly tax-block report for building watchers, because across the
twelve watched buildings there were zero events in thirty days. A report of
*what happened* is empty for seven of twelve blocks, so it carries a
standing-state section that cannot come up empty. Then the DOB NOW backfill put
485k historical permits inside its ingest window and it was going to open
Michael's own report with nineteen of them. `scripts/lib/backfill_windows.py`
skips rows written during a run stamped 'backfill'. **Any future backfill is now
safe by construction.**

**Homes proposed for removal (1a4d203).** 853 buildings have filed to reduce the
number of homes they hold since DOB NOW began, 1,771 homes. On /property with
the filed description verbatim, and counted on the block digest.

**The violation tier is in the sitemap (7aefdc3).** 27,825 buildings with 5+ HPD
violations, no deed, no eviction. 69,845 -> 97,790 property URLs.

**CI can go green for the first time since July (0aac330).** It ran
`pytest -m "not integration"` against a Postgres the runner does not have, so no
commit could ever pass. It now runs `scripts/guards.sh`, the same lane as the
pre-commit hook: 219 assertions, ~5s.

Smaller: an upsert that refreshed rows without moving `updated_at` in PLUTO and
DOF (63b505f); a covering index taking the scoring aggregate 8.8s to 1.4s
(be15b22); five rotted guards, one of which was hiding a Spanish label that had
been calling executed warrants "filed" for nine days (97e1297, 6dc4d01).

## Two methods this session earned, which matter more than any one fix

**Sample randomly, never by the metric.** The deconversion rule was validated
twice by reading the biggest unit losses and both times it looked convincing
while being wrong: a hotel converting 606 rooms to 312 apartments and a
dormitory reconfiguring 267 suites. Both reduce a count and neither removes a
home. A random sample of ten showed the real signal immediately. The same trap
appeared twice more: a violation-page overlap that looked disqualifying at
74.9% until five draws showed it breached 0 of 5 while the pages already
sitemapped breached 3 of 5, and a permit cost floor that "obviously" improved
quality until it was seen dropping genuine $100 filings.

**Ask whether a lagging feed was fixed or only labelled.** The window-anchoring
disclosure shipped ten days before the numbers underneath it were corrected.

## Rules that now live in one place, with a grep guarding each

Each was written after the rule had already been bypassed in production. All are
in the pre-commit lane and in CI.

    api/permit_kinds.py            what a permit is; two source vocabularies
    api/freshness.window_sql       windows end at the feed, not the calendar
    scripts/lib/backfill_windows   imported history is not new activity
    scripts/lib/mailer.py          one gate for outgoing mail
    api/freshness.real_date        a record cannot postdate its own ingest

## NEXT, in order

1. **Move 05, citywide marshal index.** 60+ place variants of "eviction marshal
   {place}" rank 2 to 43 with zero clicks, and there is no citywide parent for
   the 127 leaves.
2. **Move 06, plain-phrase headings on /property.** "sales history", "taxes",
   "owner" are live queries ranking 12 to 24.
3. **A CI job with a database**, so the non-integration suite runs there too.
   The guard lane is deliberately narrower and is what unblocks the push.
4. **Strengthen the near-duplicate guard.** It samples four pages, six pairs, so
   it rarely draws a bad one; deed-only pages breach the 70% limit in 3 of 5
   independent draws of ten.
5. **DOS follow-ons.** Registered agent as a second clustering signal, Delaware
   jurisdiction surfaced (663 entities), monthly `refresh_dos_entities --all`.

**Open and genuinely a judgement call, not a measurement: `WEIGHT_PERMITS` is
0.21, set when the permit signal was 414 rows. It has never been calibrated
against a real one.** The signal's *definition* was deliberately left alone so
the coverage fix could be measured on its own; `initial_cost` and the dwelling
counts are in `permits_raw` and would sharpen it.

## State

- **Alembic: current f3a91b6c8d27, head c8f4b16d29ea, FIVE pending.** All are
  guarded and their DDL is already applied to the live database, so the gap is
  expected. b8e30d5c1746 (the raw_data drop) still wants the maintenance
  window. **Do not stamp.**
- Disk 73%, 22GB free. permits_raw is 2.9GB after the backfill.
- ACRIS 28 days stale against a 21-day threshold, upstream and unchanged. The
  other four feeds are within two days.
- permits_raw: 745,407 BIS rows, 485,443 DOB NOW.
- 21 confirmed subscribers, 10 of them block-digest recipients.
- Sitemap: 4 files, 97,790 property URLs, served.
- Guard lane: 219 assertions, ~5s, installed as .git/hooks/pre-commit and run
  by CI. Full suite runs in halves on this box; see the older sections.
- `sales_raw` and `property_scores` are empty tables nobody writes.
  `idx_parcels_geometry` is 74MB with zero scans since the Aug 15 reboot. None
  was touched; all three are worth a look someday.

## NEEDS MICHAEL, priority order

1. **Buy Anthropic credits.** The AI read has been dead since 2026-08-23 and is
   the one feature failing in front of users. Weekly digest narratives are down
   with it.
2. **Send the FLGSP pitch.** Re-verified send-ready 2026-08-27 (da26805) and
   stale again after a week. The DOS finding makes it stronger: all 82 shells
   are Delaware companies filed 2026-01-23, two months before they took title,
   every one designating SUMMIT MALLS MANAGEMENT LLC.
3. **Decide on the push.** 125 commits, and CI can now go green. See item 3 of
   the clock list for why the push does not worsen the credential exposure.
4. **Restart with the committed service unit**, then rotate the DB password,
   then take the maintenance window (`alembic upgrade head` +
   `retire_raw_data.sh drop`, ~11GB of a 16GB database):

       cp deploy/pulsecities.service /etc/systemd/system/pulsecities.service
       systemctl daemon-reload && systemctl restart pulsecities

   This is the one red test in the suite.
5. **HEARTBEAT_BASE_URL, ALERT_WEBHOOK_URL, dedicated R2 creds, OPS_TOKEN
   rotation.** Unchanged.

# >>> Earlier the same day: the permit signal <<<

Working tree clean. Two features shipped and deployed, plus four rotted guards
fixed. The permit fix is live: **every displacement score on the site was
recomputed and half the top ten changed.**

## The one thing to read first

**The permit signal carried 24.7% of the composite score and was computed from
414 records. It now reads 32,786.** scrapers/permits.py reads ipu4-2q9a, the
legacy DOB Permit Issuance dataset, and DOB NOW superseded it.

    before        414 permits across 106 of 177 scored ZIPs
    after      32,786 permits across 180 ZIPs
    backfill  485,443 rows from 2021-01-01, matching upstream year by year

Result of the live recompute, measured against a snapshot taken first:

    ZIPs with a nonzero permit signal    105 -> 168 of 177
    median rank shift                    7 places, mean 12.9, max 79
    top-10 overlap                       5 of 10

The falls are the ones to look at. **11694 Rockaway Park went from rank 17 to
96**, its permit signal collapsing from 85.7 to 6.1: a handful of surviving BIS
permits had it reading as the 17th most pressured ZIP in the city. 11693 Broad
Channel 45 to 104. 10310 Staten Island 129 to 168. 11211 Williamsburg 26 to 64.
The new top ten is the South Bronx (10466, 10458, 10468, 10459, 10457, 10452,
10453) plus 10030 Harlem, 11216 and 11233 Bed-Stuy.

**Why it survived five audits.** The scraper runs green every night and reports
thirty-odd records, which reads as a quiet incremental feed rather than a dead
one. The anomaly check did write "count 2 < 50% of static minimum 50" three
times in fourteen nights, and it reached nobody: BaseScraper only escalates a
low count to an email when the feed's expected minimum clears 100, and this one
sat at 50. Then pipeline_health, the one report a human reads, carried a
hand-written note for exactly that pattern reading "post-bulk-recovery;
rolling-avg warning expected" -- the right symptom with an explanation that had
gone stale. All three are fixed (f6a95b8).

**The other four feeds were checked the same way and are fine**: HPD violations
98.9%, evictions 98.1%, DCWP 100%, 311 at 99.9% once its two-day ingestion lag
is excluded. Permits was the only one.

## Shipped

**The block digest (93859bd), cron installed, first send 2026-09-01 10:00 ET**
to ten subscribers including Michael. Monthly report on the tax block around
each watched building. The measurement killed the obvious design: a report of
*what happened this month* is empty for seven of twelve blocks, so it carries a
second section reading standing state which cannot come up empty. Detail in the
section below this one.

**DOB NOW permits (bc39644, b2e5c93a17df).** scrapers/dob_now_permits.py reads
w9ak-ipjd, DOB NOW Build job filings. Notes for whoever touches it next:

- **Not rbx6-tga4.** That one is the closer name match (approved permits) and is
  wrong twice: no job_type, so "alteration to an occupied building" is
  inexpressible, and one row per work permit, so a job with a
  general-construction and a structural permit counts twice.
- **scoring/compute.py is deliberately unchanged.** It still filters
  `permit_type = 'AL'`; the scraper maps DOB NOW's spelled-out job_type onto the
  BIS short codes. Coverage was fixed, definition was not. DOB NOW also carries
  `initial_cost` and existing-vs-proposed dwelling units, either of which would
  sharpen the signal, and folding them in now would confound the two changes.
  **That is the obvious next improvement and it should be its own change.**
- **permits_raw has two identities now** (a7d3f1e08b64), split on
  `source_id IS NULL`. The integrity guard measured what reusing one key would
  have cost: 20,742 DOB NOW rows share the BIS key with another row and would
  have been dropped silently.
- **BaseScraper has a 'backfill' status.** A 485k-row walk recorded as an
  ordinary success puts the 14-day rolling average at ~160k and emails ops for
  eleven nights. Use `--era` or `--since`; both set it.
- **dob_now_permits' floor is 120, above the 100 escalation gate, on purpose.**
  Weekends do not false-alarm: a zero-record run only escalates when the
  source's own max date has moved past our watermark.

**An upsert that refreshes a row must say so (63b505f).** mappluto completed on
2026-08-12 having processed 858,602 records and the newest `updated_at` in the
918k-row parcels table read 2026-07-09. `onupdate=utcnow` does not reach an
`ON CONFLICT DO UPDATE` SET list. Two of four call sites had it right, so the
fix is a grep, and it found a second instance (scrapers/dof.py) that a hand
search had called clean.

**Four rotted guards (97e1297).** The full suite costs five minutes, so nothing
runs it. /this-week asserted "eviction filings", the copy its own 2026-08-19 fix
had removed; fixing it surfaced that **the Spanish string was never corrected**
and had been calling executed warrants "presentados" for nine days. Two email
tests could not reach the code they tested, because conftest blanks
RESEND_API_KEY and mailer refuses before Resend is touched; one of them was
*passing* for that reason and would have gone on passing with the retry ladder
deleted.

**Covering index (be15b22).** _aggregate_permits went 8.8s to 1.4s. The other
five queries reading permits_raw were measured and none moved.

**Flip Watch was finding 15 flips a year and should have found 639 (d265361).**
Four queries hand-rolled `raw_data->>'job_type' IN ('A1','A2')`, a column that
only exists on legacy BIS rows, so they were reading 4,474 renovation permits in
a year against 81,486. /flips, the homepage docket, the weekly editions, the
neighborhood flip rows and both pulse endpoints were all affected; it is the
feature the MTEK and PHANTOM pitches are built on. 90d went 1 to 115, 180d 6 to
279, 365d 15 to 639.

**api/permit_kinds.py owns the rule now.** permits_raw holds two vocabularies:
BIS puts a job code in `raw_data->>'job_type'` and the permit's *trade* in
`permit_type` (EW, PL, AL), while DOB NOW has neither and the scraper maps its
job type into `permit_type`. So `permit_type` means a trade on one source and a
job type on the other. Both resolve to "AL is work on an existing building",
which is why scoring reads it unchanged, and nobody had written that down.
tests/test_permit_kind_guards.py greps for the next hand-rolled copy, tests/
included: test_neighborhood_flips.py had its own copy, kept the old rule when
production was fixed, and then failed insisting a ZIP had no flips while the
page it was checking had found some.

## Two changes measured and deliberately NOT made

Both were the checkpoint's own item 1 ("sharpen the permit signal now that it
has data"). Both were built far enough to measure and neither earned a change
to a public score. Written down so they are not re-proposed from first
principles.

**A cost floor on the permit signal: rejected.** `job_cost` is 100% populated on
DOB NOW and the median alteration is $30,960, so half the signal really is
repair work rather than renovation pressure. But filtering recreates in
miniature the sparsity that just cost 24.7% of the score:

    floor      permits   ZIPs   ZIPs under 10 permits
    none        32,786    180        22
    $25,000     17,153    178        30
    $50,000     12,366    176        43
    $100,000     7,709    168        48
    $250,000     3,360    156        70

And the payoff is small: median rank shift 2 at $25k, 3 at $50k, 4 at $100k,
against the coverage fix's 7. Discarding half the data and pushing twice as many
ZIPs into the thin regime, to move ranks two places, is not a trade worth making
on a published number. Cost-weighting instead of filtering was also measured and
is worse: in 20 of 180 ZIPs a single job is more than half the ZIP's total cost,
and a $2M per-job cap only brings that to 18.

**Unit loss as a score signal: rejected, and it would have published something
false.** 2,164 jobs a year propose fewer dwelling units than the building has,
which sounds like the most direct displacement signal in the dataset. Reading
the actual rows says otherwise. Filers use `proposed_dwelling_units`
inconsistently: some enter the building's total, some the units in scope, many
leave it blank and it arrives as 0.

    11 5 AVENUE     2678 -> 267 units   $65,000    "Renovation to an existing one-bedroom apartment"
    1724 MADISON    792 -> 0 units      $1,500     "Fire Suppression Gas Shut-off Valve"
    685 FIRST AVE   556 -> 0 units      $419,100   "interior built out of a children daycare"

Shipped as a count, the site would have said "2,999 apartments proposed for
removal in 10003" on the strength of a $1,500 gas valve. The only independent
check available is whether the job description corroborates: across
`units_proposed > 0` rows it does about 30% of the time, and for the 596 rows
where proposed is 0 it does 5%, which is what confirms those are blanks.

**What is real in there**, and worth surfacing later as a page-level fact rather
than a score input, is the corroborated subset. Two fields agreeing, roughly 500
jobs a year, and they are exactly the deconversion story:

    33 PERRY STREET      3 -> 1 units   $2,280,000  "convert multi-family townhouse into single family"
    2144 EAST 12 STREET  3 -> 1 units   $1,693,424  "Convert 3 dwellings into 1"
    108 TROUTMAN STREET  4 -> 2 units   $1,884,445

The columns are in place (`job_cost`, `units_existing`, `units_proposed`,
migration c8f4b16d29ea, backfilled and written by the scraper going forward), so
that work starts from data rather than from a backfill.

## NEXT, in order

1. **Corroborated deconversion as a page-level fact.** Not a score input; see
   the section above for why. ~500 jobs a year where the unit counts and the job
   description agree that apartments are being merged away. Natural homes are
   /property and the block digest. Columns are already there.
   **Still open and unanswered: WEIGHT_PERMITS is 0.21, set when the signal was
   414 rows. It has never been calibrated against a real one, and that is a
   judgement call for Michael rather than a measurement.**
2. **Move 09, violation-gated sitemap expansion.** 27,862 buildings carry 5+ HPD
   violations with no deed and no eviction. Gate at 5, not 1.
3. **Move 05, citywide marshal index.** 60+ place variants rank 2 to 43 with
   zero clicks and there is no citywide parent for the 127 leaves.
4. **Move 06, plain-phrase headings on /property.**
5. **DOS follow-ons.** Registered agent as a second clustering signal; Delaware
   jurisdiction surfaced; a monthly `refresh_dos_entities --all`.

## State

- Disk 72%, up from 70%: the backfill cost ~1.2GB. 23GB free.
- **Alembic: current f3a91b6c8d27, head is now b2e5c93a17df, FOUR pending.**
  b8e30d5c1746 (raw_data drop) still wants the window; c4e17b2a9d38,
  a7d3f1e08b64 and b2e5c93a17df are all guarded and their DDL is already applied
  live. Do not stamp.
- ACRIS 28 days stale against a 21-day threshold, unchanged and upstream.
- `test_deploy_copy_matches_installed[pulsecities.service]` is the one guard
  still red, and it is NEEDS MICHAEL item 1, not a bug.
- `sales_raw` and `property_scores` are both empty tables nobody writes.
  `idx_parcels_geometry` is 74MB with zero scans since the Aug 15 reboot. None
  of the three was touched; all three are worth a look.

## NEEDS MICHAEL, unchanged

Buy Anthropic credits (AI read dead since 2026-08-23). Send the FLGSP pitch.
Decide on the push (100+ commits unpushed, CI dead since 08-08). Restart with the
committed service unit, then rotate the DB password, then the maintenance
window. HEARTBEAT_BASE_URL, ALERT_WEBHOOK_URL, dedicated R2 creds, OPS_TOKEN.

# >>> Earlier that day: the block digest <<<

Working tree clean. The block digest shipped (93859bd) and is installed in
/etc/cron.d/pulsecities, so it sends for the first time on **2026-09-01 at
10:00 ET** to ten subscribers, Michael among them. Everything else below is a
finding, not a change.

## The one thing to read first

**The permit signal is 24.7% of the composite score and is computed from 414
records.** `scrapers/permits.py` reads `ipu4-2q9a`, the legacy DOB Permit
Issuance dataset. DOB NOW superseded it, and the remnant that still lands in
the legacy feed is what the score is built on.

    our permits, last 365 days       6,501
    DOB NOW rbx6-tga4, same window   ~170,000       coverage 3.8%
    after the AL / 3+ unit filter      414 across 106 of 177 scored ZIPs

24.7% and not 21%: the dormant-weight redistribution in `compute.py` is working
correctly, rs_unit_loss is dormant, and permits therefore carries 0.21/0.85 of
the live mass. It is the second-heaviest effective weight in the product's
central number.

At roughly four permits per ZIP the percentile normalisation is amplifying
noise. Counterfactual, recomputing every score with the permit term removed and
the rest renormalised: **median rank shift 13 places, max 76, and half the top
ten changes.** 11211 moves 26 to 102, 10002 moves 77 to 144, 10013 moves 76 to
141. Without the term the top ten is almost entirely the South Bronx.

That counterfactual does not prove the permit-free ranking is the right one.
It proves the term is decisive, so its 3.8% coverage is not cosmetic.

The fix is a DOB NOW scraper (`rbx6-tga4`, 990,856 rows, live, carries borough
/ block / lot / bin / work_type / issued_date / job_description, so BBL builds
the same way `ipu4-2q9a`'s does). **The one real design question is the filter.**
Scoring counts `permit_type = 'AL'` today; DOB NOW has no `permit_type` in that
vocabulary, it has `work_type` ("General Construction", "Full Demolition"), so
the "alteration to an occupied building" test has to be re-expressed before the
two feeds can be unioned. Note also that DOB NOW rows are approved work permits
and several can belong to one job, so the 3.8% is a floor on the gap rather
than an exact ratio.

## Shipped

**The block digest (93859bd).** Monthly report on the tax block around each
watched building, which was checkpoint item 1. The radius was measured before
it was built: building 3 of 12 blocks have monthly activity, block 9 of 12 by
ingest date and 5 of 12 by event date, ZIP 12 of 12 but at 1,000 to 4,200
records a month.

**The measurement killed the obvious design and that is the part worth keeping.**
A report of what happened this month is empty for seven of twelve blocks, which
is the retention hole moved one radius out. So the digest carries a second
section reading standing state (open violations, twelve months of deeds and
evictions, the most cited address) which cannot come up empty: every watched
block carries between 8 and 400 open violations today. All ten subscribers
render 185 words or more and pass the mailer gate, three of them with zero new
records.

Four bugs found by reading its own output, each pinned to a test:

- `bbl LIKE '304447%'` cannot use the btree on bbl outside the C collation.
  The twelve-block scan runs in 1.9s as a range and had not finished in two
  minutes as a prefix. **If another block-scoped query gets written, use
  `block_span()`.**
- HPD writes one violation per apartment, so an inspector's afternoon is four
  rows sharing a date. Merged by building, kind and day.
- "And 6 more records" counted lines against a total of records.
- HPD descriptions open with the statute and close with the apartment number,
  in capitals. `_plain()` strips both.

One address is capped at four lines, because on a three-parcel condo block a
single neighbour filled seven of nine. Grouped by subscriber and not by watch,
because one reader follows three buildings across two blocks. The
building-watch confirmation now says what else arrives and when: it promised
"quiet stretches send nothing", and it was also still listing four feeds after
311 was added.

## NEXT, in order

1. **The DOB NOW permit scraper.** Above. Highest-value correctness work
   available: it repairs a quarter of the composite score.
2. **Move 09, violation-gated sitemap expansion.** 27,862 buildings carry 5+
   HPD violations with no deed and no eviction, so they are absent from the
   sitemap. Gate at 5, not 1 (117,309 would qualify at 1).
3. **Move 05, citywide marshal index.** 60+ place variants of "eviction marshal
   {place}" rank 2 to 43 with zero clicks; the head term sits at position 43
   with no citywide parent for the 127 leaves.
4. **Move 06, plain-phrase headings on /property.** "sales history", "taxes",
   "owner" are live queries ranking 12 to 24.
5. **DOS follow-ons.** Registered agent as a second family-clustering signal;
   Delaware jurisdiction surfaced (663 entities); a monthly
   `refresh_dos_entities --all`.

## State

- **ACRIS is 28 days stale against a 21-day threshold**, unchanged and upstream.
  Deeds through 2026-07-31. The other four feeds are inside threshold.
- Disk 70%, Postgres 23GB. `retire_raw_data.sh` still never run.
- `test_deploy_copy_matches_installed[pulsecities.service]` is the one guard
  still red, and it is NEEDS MICHAEL item 1 below, not a bug. The cron and
  logrotate copies were installed this session, so those two are green again.
- Alembic unchanged: current f3a91b6c8d27, head c4e17b2a9d38, two pending and
  that is correct. Do not stamp.

## NEEDS MICHAEL, unchanged from the previous checkpoint

Buy Anthropic credits (AI read dead since 2026-08-23). Send the FLGSP pitch.
Decide on the push (97 commits unpushed, CI dead since 08-08). Restart with the
committed service unit, then rotate the DB password, then the maintenance
window. HEARTBEAT_BASE_URL, ALERT_WEBHOOK_URL, dedicated R2 creds, OPS_TOKEN.

Full detail on every one of these is in the 2026-08-27/28 section below.

# >>> START HERE after /clear (2026-08-27/28, the search-data session) <<<

Working tree clean, 14 commits since 31ef117. Everything below is done and
verified except what is under NEEDS MICHAEL and NEXT. A pre-commit hook now runs
128 guards in 3.4s on every commit, so if a commit is refused, read it.

## The one thing to read first

**Michael pulled live Search Console for the first time since traction: 731
clicks, 29,700 impressions, 2.5% CTR, average position 8 to 11.** The Aug 18
baseline was 5 clicks. That export is the source of most of this session's work
and the numbers are in `memory/project_search_read_2026_08_27.md`. The diagnosis
moved: indexing and thin content are fixed, and the site now ranks and does not
get clicked.

Two artifacts hold the analysis. They are private to Michael's claude.ai account
and he could not open them in his browser (wrong account signed in); standalone
copies are at `reports/` in the repo, gitignored.

    Health audit    https://claude.ai/code/artifact/9df855cd-3675-48bf-9c95-210edeb83168
    Growth plan     https://claude.ai/code/artifact/243aaca3-1245-4932-a634-9d3e1352ac5a

## Shipped

**The freshness rule had one reader and seventeen bypasses (72f9dfa, 2e81055).**
Two ownership_raw rows carry a filer-typed doc_date of 2026-08-27 on a deed
recorded 2026-07-29, ingested 2026-08-11. The rule excluded them only while they
sat ahead of the calendar, and on 2026-08-27 the calendar reached them: ACRIS
went "frozen 27d" to "current today" and /api/status published data_through
2026-08-27 on a feed frozen since July. `generate_sitemap.py` had been writing a
FUTURE `<lastmod>` onto all 200 hub URLs for sixteen nights. /ops hand-rolled the
freshness query with a bare `<= CURRENT_DATE` while its comment claimed it agreed
with /api/status. Fixed with `freshness.real_date(column, created_at)`, which does
not expire, applied to both sitemap queries, /ops, /api/stats, /llc, radar and
family clustering. `tests/test_date_guards.py` greps for the eighteenth one;
19 known bypasses remain in a KNOWN_UNGUARDED list that may shrink, never grow.
Full case study in `docs/ops/failure_patterns.md`.

**Guards were rotting because the suite costs 76s (4d05132).** Three separate
guards existed and had not been run: the logrotate check already knew the condo
log was unrotated, and five assertions in test_source_freshness_disclosure had
been red since the commit that wrote them. `scripts/guards.sh` is the subset
needing neither Postgres nor api.main: 128 assertions, 3.4s, installed as
`.git/hooks/pre-commit` by symlink. Deploy-drift checks are deliberately excluded
(true about the box, not the commit).

**Move 01, deed vocabulary on /llc (38bf651).** The template ranked 5 to 9 on
~1,700 impressions of grantor/grantee/chain-of-title/ACRIS queries and contained
none of those words. Retitled, deed rows now print their ACRIS document ID and
BBL, chain-of-title section added. First draft of the prose was generic and
pushed the thinnest LLC pages to 72% five-gram overlap against the 70% guard;
rewritten per-entity, worst pair 67.8%.

**Citations on every fact block (b75d7a3).** All five blocks on /property and the
chain of title on /llc end with "Source: NYC HPD violations, current through
August 25, 2026", dates from api/freshness so the page and /api/status cannot
disagree. Reason: AI crawlers fetched 82,631 pages in 15 days against Googlebot's
68,881, and a machine cannot come back to check a number later.

**Move 03 part one, sideways links (38a89ba).** `_sibling_buildings` was gated on
curated operator classifications covering 566 parcels; falling through to the
deed buyer covers 6,669. Gated on `_is_buyer_entity` because the first working
version listed buildings for MARIE FRANCK AS TRUSTEE and LAMBERT JUNIOR D, which
is a people-search directory. Also fixed the one JS-only nav row on the site
(4459ea0), on all 177 neighborhood hubs.

**Move 02, the DOS registry (46ad460).** data.ny.gov n9v6-gdp6 joined to deed
buyers: DOS ID, filing date, jurisdiction, registered agent. 12,743 rows in 26s,
81% match, selective ingest (dataset is 4.2M rows, disk is at 69%). Nightly at
03:08. **All 82 FLGSP shells are Delaware companies filed 2026-01-23, two months
before they took title, every one designating SUMMIT MALLS MANAGEMENT LLC at
1350 6th Avenue.** Reading the first run's output caught two classifier bugs: 447
rows spelled "The Limited Liability Company" and 106 spelled "C T CORPORATION
SYSTEM" were being called named third parties.

**One email gate (6b65a95, 84b0a26).** Five `resend.Emails.send` call sites had
five different answers to "is this worth sending", one of them none at all.
`scripts/lib/mailer.py` owns it now; the grep guard found `scheduler/alerts.py`
on its first run, which a hand search had missed. Primary guarantee is
`content_items`; a measured word floor backs it up. **Measuring caught the
guard's own bug**: the floor was 45 on a guess, a real one-event alert renders 39,
so it would have blocked "your building was sold". Ops alerts pass with
min_words=0 because "acris_ownership: source frozen 27d" is nine words.
311 housing complaints added to the alert scan: the four scanned feeds produced
24 events in 90 days across the watched buildings, 311 alone produced 109.

## The correction that matters, so it is not re-derived

**Internal linking does not drive this site's traffic, and I claimed it did.**
Measured: 1,856 property pages are reachable from the eviction tier, 51 of them
earned a visitor (2.7%), against 1,280 of 67,989 unlinked (1.88%). That is 1.4x,
close enough to chance. Googlebot already crawled 57,131 distinct URLs, 81% of
the sitemap, so discovery was never the bottleneck. **Move 03's case is
engagement, not SEO, and Move 09 has no link-graph dependency.**

## NEXT, in order

1. **The block digest.** The measured fix for the retention hole and the only
   piece of that work not shipped. 9 of 13 real subscribers are building
   watchers whose only channel is an event-driven alert; across the 12 watched
   buildings there were **0 events in 30 days**, and 3 buildings have had none in
   a year. Radius test: building 3/12 have monthly activity, **block 12/12**,
   ZIP 12/12 but at 1,000-4,200 events a month. A tax block is a median of 59
   parcels, so it really is "your street". Needs a cap: 1062 Elton Street's block
   runs 108 events a month. `pbarcia@aol.com` subscribed to three adjacent
   buildings on 42nd Street, which is a user hand-building this feature.
   A monthly *building* report was tried on paper and rejected: it would be
   empty for 8 of 12 buildings even including 311 and open-violation state.
2. **Move 09, violation-gated sitemap expansion.** 27,862 buildings carry 5+ HPD
   violations with no deed and no eviction, so they are absent from the sitemap.
   Now unblocked. Gate at 5, not 1 (117,309 would qualify at 1).
3. **Move 05, citywide marshal index.** 60+ place variants of "eviction marshal
   {place}" rank 2 to 43 with zero clicks; the head term `nyc marshal eviction
   list` sits at position 43 and there is no citywide parent for the 127 leaves.
4. **Move 06, plain-phrase headings on /property.** "sales history", "taxes",
   "owner" are live queries ranking 12 to 24.
5. **DOS follow-ons.** Registered agent as a second family-clustering signal
   (independent of the filing address, and Summit Malls proves it works);
   Delaware jurisdiction as a surfaced signal (663 entities); a monthly
   `refresh_dos_entities --all` since the nightly only resolves new names.

## Traps and state, so the next session does not walk into them

1. **Alembic: current is f3a91b6c8d27, head is c4e17b2a9d38, TWO migrations are
   pending and that is correct.** b8e30d5c1746 drops raw_data and wants the
   maintenance window. c4e17b2a9d38 adds dos_entities and **guards its own
   create**, because the table was made directly when the feature shipped. I
   stamped to head by mistake during this session and reverted it; do not stamp.
   `alembic upgrade head` during the window applies the drop then skips the
   create.
2. **The pre-commit hook runs on every commit.** `--no-verify` exists. To extend
   the lane, edit `LANE` in `scripts/guards.sh`.
3. **The one guard that stays red is `test_deploy_copy_matches_installed
   [pulsecities.service]`.** That is NEEDS MICHAEL item 1 below, not a bug.
4. **Do not build person pages.** Person-name queries convert at 12.3% against a
   2.5% site average and are the best-converting queries the site has. 71,197
   person-shaped deed parties, and the ones being searched hold 1-2 buildings.
   Only 163 hold 5+, and the top of that list is banks.
5. Everything in the previous checkpoint's trap list still stands: measure SSR
   through the gunicorn socket, nothing during the 02:00 run, the nginx SSR cache
   keys on path plus ?lang only, most SSR heads are f-strings, `esc()` copy
   cannot carry markup, `_count()` pluralises the head noun, a new SSR route
   needs an nginx location block.

## NEEDS MICHAEL, priority order

1. **Buy Anthropic credits.** The AI read has been dead since 2026-08-23 and 27
   real visitors hit it in five days. It is the one feature failing in front of
   users, and the weekly digest narratives are down with it.
2. **Send the FLGSP pitch.** Re-verified and send-ready as of 2026-08-27
   (da26805): 82 buildings, 82 entities, all deeds 2026-03-31, prices summing to
   exactly $451,300,000, 99 evictions of which 98 predate the sale. Three counts
   were refreshed (RS units 4,793 to 4,823, open violations 10,350 to 9,838,
   worst building 778 to 841). **Stale again after a week.** The DOS finding
   above makes it stronger. Recipients named in the file.
3. **Decide on the push.** 97 commits unpushed, CI dead since 2026-08-08, and
   `.github/workflows/ci.yml` runs on push to main. Verified it does not worsen
   credential exposure: the DB password is already in 403 pushed commits back to
   2026-04-15. Postgres is localhost-only behind ufw, so it is defence-in-depth,
   not an open door. **The purge plan must cover the remote**, not just local
   history.
4. **Restart with the committed service unit**, then rotate the DB password, then
   the maintenance window (`alembic upgrade head` + `retire_raw_data.sh drop`,
   ~11GB of a 16GB DB). Unchanged from the previous checkpoint.
5. **HEARTBEAT_BASE_URL, ALERT_WEBHOOK_URL, dedicated R2 creds, OPS_TOKEN
   rotation.** Unchanged.

## Numbers worth not re-deriving

    Search Console   731 clicks / 29.7k impressions / 2.5% CTR / position 8-11
    logs vs console  nginx unique search-referred IPs run ~1.8x GSC clicks
    subscribers      21 rows, 17 addresses, 13 real people (4 are Michael's, 1 test)
                     9 of 13 are building watchers, 3 ZIP, 2 company
    funnel           /property 977 visitors -> 11 signups (1.13%)
                     /llc 126 -> 2 (1.59%); both live capture points convert alike
    engagement       73% of organic visitors read one page and leave
    crawl            GPTBot 76,289 vs Googlebot 68,881 over 15 days
    sitemap          1,052 core + 69,845 property
    backlinks        0. Still the ceiling, and no code change touches it.

# >>> START HERE after /clear (2026-08-19, family follow + five-audit sweep) <<<

Everything below is done, committed and verified except the items under NEEDS
MICHAEL. Working tree clean. Suite state: every touched suite re-run green this
session (575 test passes across the runs); the halves command in the previous
handoff still stands for a full run.

## Shipped this session

**Portfolio follow (f28ccf8).** "Email me when this portfolio buys again", the
checkpoint item 3 build. subscribers.family_slug (migration c7f2b4a91e83,
applied; the raw_data drop b8e30d5c1746 now revises it and stays pending),
validated against the live clustering at write time since families are
computed, not stored. Confirmation email, weekly-digest delivery (internal
restructurings excluded from "new purchases"), follow card on every
/network/{slug} hub, follow link on /llc member pages. The clustering memo
moved to api.entity_families.families_cached (single-flight lock) so pages,
subscribe and digest share one cache. NOTE: the operator-follow gate only
covers 3 rows classed 'operator'; family follow is what covers the 26
published portfolios.

**Five-audit sweep, fixes applied same day (a4ded27, e245cba, 85950c8).**
Parallel auditors: correctness, rendered-output editorial, security, perf/DB,
ops/infra. Every CRITICAL and HIGH is fixed except the classifier-blocked
items below. Highlights, because the next session should not re-derive them:

- /network ranked co-buying families on a 4x-inflated number (REDROCK "39
  buildings" = 9 real; per-name counts summed across co-buys). Fixed:
  distinct building keys per family-date.
- /evictions/{name} printed the LIMIT 10 as the population ("Ten buildings
  have more than one eviction" where 136 do). Fixed: real count, list of ten.
- /eviction-case rejected ~26% of real index numbers (B309066/25, 326184/24A,
  306624/25-). Fixed: both sides reduce to [A-Z0-9/].
- nginx cache keyed on raw $arg_lang: any ?lang=junk was a fresh entry, one
  loop evicted the warm set (verified live). Fixed with a map; also 19 SSR
  locations had no limit_req (server-scope ceiling now), /ops was cacheable
  with ?t= outside the key (proxy_cache off), SSR TTL 1m->10m, gzip_static
  serves .gz twins (sitemap XML was recompressed per crawler fetch; the
  sitemap script and build:css now write twins — a stale .gz would serve
  silently, so twins are made by the same step that makes the source).
- /neighborhood permits subquery filtered on the wrong table's zip: cost
  8206 -> 77 (one token). The sitemap LATERAL pair is two grouped CTEs:
  cost 3.3M -> 116k; the whole script now runs in 14s.
- The weekly digest averaged 9 week-buckets, two of them stubs (~10%
  understated baselines; complete ISO weeks only now), carried a second
  tier vocabulary (deleted; scoring.tiers everywhere), and labelled executed
  warrants "eviction filings" (fixed here, /week, /this-week, /neighborhood;
  /this-week also counted ALL 311 complaints under a housing label).
- The live DB password was hardcoded in two tracked scripts and matches
  .env's DATABASE_URL. Files fixed; ROTATION STILL PENDING (blocked, below).
- pipeline_health's CRITICAL went nowhere (no MAILTO, no MTA); it now emails
  via notify_ops. The 117-day dcwp snooze silencing a resumed feed: removed.
- logrotate rotated on bytes only ('size' overrides 'weekly'); access log
  (holds the ops token 197x) now 0640. backup_db moved off the 03:30
  cross-tenant collision to 03:50; MANIFEST ships offsite in the state tar.

**Full audit reports** live in this session only; the durable list of what
was NOT fixed is below. crawl_audit: PASS 22 / WARN 0 / FAIL 0 after all of it.

## NEEDS MICHAEL (classifier-blocked or external accounts), priority order

1. **Restart gunicorn with the new unit** (deploy copy committed, /etc not
   yet updated so runtime matches disk):
       cp deploy/pulsecities.service /etc/systemd/system/pulsecities.service
       systemctl daemon-reload && systemctl restart pulsecities
   Changes: WEB_CONCURRENCY=2 as the one source of truth (api/ratelimit.py
   divides budgets by it), --timeout 120 -> 60.
2. **Rotate the DB password** (it sat in git history + two tracked files;
   files fixed, history purge still pre-public):
       sudo -u postgres psql -c "ALTER ROLE pulsecities_user PASSWORD '<new>'"
       then update DATABASE_URL in .env, then restart pulsecities (item 1).
   Every consumer reads .env at runtime; nothing else holds the password.
3. **HEARTBEAT_BASE_URL** — one line in .env, healthchecks.io check
   (period 24h, grace 2h). The Aug 15 reboot proved the box cannot detect
   its own absence: the whole 02:00-04:00 batch skipped, nothing noticed.
4. **ALERT_WEBHOOK_URL** — second alert channel; Resend already dropped
   mail twice on 2026-08-05 (SSL EOF).
5. **Dedicated R2 bucket + token**, set PULSECITIES_R2_* in .env (r2_creds.sh
   already prefers them). Today violation-leads' token both gates our only
   offsite backup AND decrypts our .env archive (passphrase derives from it).
6. **The maintenance window** (Saturday after 04:15 UTC is the clean slot):
       ./venv/bin/python -m alembic upgrade head   # drops raw_data columns
       scripts/retire_raw_data.sh drop             # VACUUM FULL, 10-20 min
   ~11GB reclaimed (DB 16 -> ~6.5GB), Sunday restore-test disk peak 83->69%.
   Same restart: install pg_stat_statements (deploy/postgresql-pg-stat-
   statements.conf -> /etc/postgresql/14/main/conf.d/). AFTER the drop and a
   re-measure of free -m: shared_buffers 512MB -> 768MB (NOT before: 1.9GB
   was already swapped at audit time) and work_mem 16 -> 8MB.
7. **OPS_TOKEN**: rotate it and prefer the X-Ops-Token header; the ?t= form
   is in the access log 197 times. (Keeping ?t= keeps your bookmark working;
   the log is 0640 now, so this is lower heat than it was.)
8. **Send a pitch.** Unchanged. Three verified drafts in docs/outreach/.

## Audit backlog — real findings, deliberately not fixed tonight

Editorial (rendered-output auditor; nothing here is false data, mostly polish):
- /radar: co-lot rows read as duplicate buildings ("4 buildings", 3 distinct
  addresses); use the /llc "N lots in M buildings" vocabulary. Same page: one
  card carries no amounts at all where others do.
- /llc/{slug}: a deed with the same entity on both sides counts as bought AND
  sold (PHANTOM Z CAPITAL, 4 self-transfers); label or exclude.
- /network PHANTOM card (46 companies / 104 buildings) 301s to
  /operator/phantom-capital (43 LLCs / 83 properties) — two definitions, no
  explanation, and the JSON-LD ItemList points at the redirect. Either build
  the family hub or annotate the card.
- Address repair pass ("STR EET", "AVE NUE", ",Apartment Basement", "Mcbride"):
  fixes display AND shrinks the 1,443-vs-1,177 dedupe gap.
- /property: owner-of-record (tax file, lags) vs newest deed buyer shown
  without the lag explanation; RS units > total units needs its one-line
  DHCR-vs-PLUTO clause.
- Homepage title is brand-first ("PulseCities | NYC Displacement Signals");
  the query data says "who owns my building" + evictions vocabulary. This is
  Michael's call, it is the sitewide anchor.
- Smaller: /this-week "Newest flips" carries April deeds undated; monthly
  bars unlabeled year + rolling-vs-calendar mismatch; trailing periods on
  /flips//radar captions; ES parity gaps (chart axis, badge alt, one literal
  translation); /network header sums max(held,sold) while cards show both;
  operator pages still Title Case; titles run past 65 chars sitewide.

Perf/DB backlog:
- Materialise families into a table in the nightly pipeline: removes the 12s
  cold render class entirely, gives subscribe a real existence check, halves
  the /network cold render (_family_shapes re-scans what compute_families
  just scanned).
- permits_raw.raw_data (1.6GB) is the next retirement: promote job_type to a
  column first (frontend.py:1344 reads it per request).
- Drop idx_complaints_raw_bbl (64MB) + idx_violations_raw_bbl (28MB): strict
  prefix duplicates of the (bbl, date DESC) pair. Needs model edit +
  migration; fold into the window.
- DB pool (5+10/worker) vs threadpool (40/worker) mismatch: raise pool_size
  ~10 or cap the threadpool; surfaces as checkout stalls under crawl bursts.
- journald: a neighbor's node processes flood it to 12h retention
  (SystemMaxUse 150M); raise it or quiet the neighbor (cross-tenant).

## Condo address gap — scoped at last (the 2.5h query finished)

Of 17,114 deed BBLs with no parcel row, 17,086 are condo unit lots. Only 406
sit on single-address blocks, so the "unambiguous block" shortcut recovers
2.4% — not worth building. parcels HAS 11,150 condo billing lots (7501+):
the unit-lot -> billing-lot join (DOF PAD file is the authoritative mapping)
is the route to the addresses. Method note: the scoping query burned 2.5
hours because a LATERAL matched on substring(bbl,1,6) with no index — the
same answer is a GROUP BY over parcels plus a hash join, seconds.

## Watch

- **2026-08-22 03:40 UTC** (not 08-21; threshold is strict >): the ACRIS
  upstream-freeze alert fires. It is CORRECT — upstream frozen at 07-31,
  ingest current — informational on a 7-day cadence. Do not snooze it.
- First family-follow digest send: Sunday 18:00 ET (0 followers yet; the
  card shipped tonight).
- Bing/Google reaction to the 65,444-URL sitemap + new .gz delivery.

# >>> START HERE after /clear (2026-08-19, end of the Lighthouse + content session) <<<

Everything below is done, deployed, committed and verified. Nothing is
half-finished, no process is running, and the working tree is clean. 18 commits
since ae5bfae.

## What the site is right now

    Lighthouse   desktop 100 / mobile 96-97 performance
                 100 accessibility, 100 best practices, 100 SEO, 3-of-3 agentic
                 FCP 1.8s  LCP 2.4s  TBT 0  CLS 0  (Moto G Power, Slow 4G)
    crawl_audit  PASS 22, WARN 0, FAIL 0
    sitemap      1,051 core + 65,409 property URLs
    families     26, indexed at /network
    subscribers  8 confirmed (5 ZIP, 2 citywide, 1 building, 0 operator)
    backlinks    0. Still the ceiling. Nothing technical changes that.

**Test state, precisely.** The last complete run was in halves and green: 595
passed + 2 skipped, then 645 passed. Since then every touched suite has been
re-run individually and passes: entity_families, network_index, eviction_case,
fonts, frontend_routes, content_depth, sitemap, text_contrast, ui_copy_guards,
crawler_access, stats_api, eviction_areas, footer_consistency, infra_guards,
indexnow. Run it in halves, not in one process; a single `pytest tests/` on two
vCPU is still at 16% after several minutes.

    python -m pytest $(ls tests/test_*.py | head -44) -q
    python -m pytest $(ls tests/test_*.py | tail -n +45) -q

## New surfaces this session, and how they are wired

**/network** — index of the 26 entity families, ranked by the largest number of
buildings taken on one day. Nine of them moved as a block. It did not exist
until tonight; the hubs had no parent and nothing linked them but the sitemap.
Linked from the homepage operators module (EN and ES), /llc, and every hub.
The full clustering method lives here now; the hubs keep two sentences and a
link up, which is also what cut their shared-text overlap.

**/eviction-case** — look up an executed eviction by marshal docket or Housing
Court index number. Built for "nyc marshal docket number search" in the query
export. Linked from /evictions (EN and ES), the 127 /evictions/{name} pages,
every /property page that has an eviction, and both sibling tenant tools.
Only the empty form indexes; results are noindex.

**499 more /llc pages are indexable.** The floor went from three buildings to
two after measuring duplication rather than guessing (2-building pages: 55%
mean 5-gram overlap, 677 words; the 3-building pages already indexed: 55%, 702).
They are not sitemap-only orphans: each is linked from the property page of a
building it bought.

## Traps this session hit, so the next one does not

1. **Measure SSR pages through the gunicorn socket**, not nginx:
   `curl --unix-socket /tmp/gunicorn.sock http://localhost/PATH`. Two layers of
   cache hid a 12-second cold render on /network/flgsp.
2. **Nothing during the 02:00 UTC nightly run.** Two vCPU: load goes above 4 and
   every timing triples. A query that takes seconds at 21:00 hangs past two
   minutes at 02:00.
3. **The nginx SSR cache keys on path plus `?lang` only.** Any new route that
   reads another query parameter must either be added to the key or opt out
   with `proxy_cache off`, as /eviction-case does. Its own config comment says
   so; I still walked into it.
4. **Most SSR heads in frontend.py are f-strings.** Injected CSS needs `{{ }}`.
   17 of 17 head blocks were f-strings when the fonts went in.
5. **Copy that goes through `esc()` cannot carry markup**, and some strings are
   also rewritten by the language toggle (`recent_sub` on /evictions is both).
   Put links in a sibling element with their own copy key in both languages.
6. **`_count()` pluralises the head noun**, so "of them moved as a block" came
   out "blocks". Use `_count_open` with an explicit plural for phrases.
7. **A new SSR route needs an nginx location block** and an entry in the
   trailing-slash 301 regex. /eviction-case 404'd on its slash form until it
   was added.

## Next, in the order I would do it

1. **Send a pitch.** Three are written and verified and none are sent:
   `docs/outreach/pitch-flgsp-portfolio.md` ($451.3M, 82 buildings, 4,793
   rent-stabilized units), `pitch-carlyle-townhouses.md` (42 Brooklyn buildings,
   $197.5M, The Carlyle Group's address on five deed party rows),
   `pitch-snf-nursing-homes.md` ($549.6M of nursing-home real estate in five
   weeks). Backlinks are zero and press is the only lever on them. Michael's
   action, no code.

2. **The condo address gap.** 17,114 deed BBLs are unit lots absent from PLUTO,
   so a quarter of the deed record resolves to no address and /property and
   /llc say so out loud. Test mapping a unit lot to its block and taking the
   address where the block is unambiguous, DOF's PAD file for the rest. The
   scoping query never got a clean run because the nightly pipeline started.
   Needs an idle box.

3. **Surface the watch on /network and /llc.** The subscriber model already
   supports following an operator and there are zero operator watchers, because
   nothing offers it there. "Email me when this portfolio buys again" is what a
   reporter wants after reading /network/flgsp, and the plumbing exists
   (subscribers.operator_slug, the 03:25 alert cron, weekly_digest.py).

4. **`retire_raw_data.sh drop`** — ~11GB of a 16GB database. Fully prepared:
   archives written and sha256-verified 2026-08-17, both models unmapped,
   migration b8e30d5c1746 pending, script gated on both. Needs a window because
   VACUUM FULL takes ACCESS EXCLUSIVE on the two biggest tables for 10-20
   minutes; /property, /network and /neighborhood block while it runs.

       ./venv/bin/python -m alembic upgrade head && scripts/retire_raw_data.sh drop

5. **Re-export Search Console in two weeks.** There is a fixed baseline at
   `docs/seo/baseline_2026-08-18.md` and this session changed what is indexable
   and what two titles say. That is a measurable before/after rather than a
   guess about whether it worked.

## Decisions left open for Michael

- **/network in the top nav?** It is a hub now, arguably more than /radar. The
  nav is ten links and documented as fragile around nine at 960px, so it was
  left out rather than appended as an eleventh. If it goes in, swap rather than
  append: /network in, /radar folded under it.
- **Plausible.** The only remaining Lighthouse items are its script: a 52-178ms
  forced reflow inside their code and a one-minute cache TTL on their CDN. The
  only fix is proxying it through this origin, which buys ~50ms of connection
  setup and also slips past blockers. Left alone deliberately.
- **`font-display: optional` on Bricolage** would land mobile LCP at FCP, about
  1.8s, by never repainting. The cost is that a first-time visitor on a slow
  connection never sees the brand face that session. A design call, not a
  performance one.

# PulseCities checkpoint, 2026-08-19 — the scores passed, so we went at the content

Lighthouse came back 92 / 100 / 100 / 100 / 3-of-3 after the previous pass.
Michael's read on it is the useful one and is worth keeping: **SEO 100 means the
mechanics pass. It does not ask whether the content answers what searchers ask.**
Everything below came from checking the pages against
`docs/seo/baseline_2026-08-18.md`, the real query export, rather than against
another audit.

## The best-converting page type was telling Google not to index it

/llc/{slug} was noindex below three buildings. TERRA DEVELOPERS LLC has two, took
17 impressions in 28 days and earned **one of the site's five total clicks**,
while carrying `noindex`.

A floor like that should be set by measuring duplication, not by picking a round
number, so it was measured with the method the rest of the repo uses, 5-gram
containment over digit-bearing tokens, 14 pages sampled per group:

    1 building     15,020 entities   584 words   67% mean overlap, 78% max
    2 buildings       499 entities   677 words   55% mean overlap, 72% max
    3+ buildings      156 entities   702 words   55% mean overlap, 72% max

Two-building pages are indistinguishable from the three-building pages already
indexed, and both sit under /neighborhood's 68-69%. One-building pages are
visibly worse and stay out; there are 15,020 of them. Floor is now two buildings
and two lots, in the page gate, the directory query and the sitemap. Core
sitemap 553 -> 1,050 URLs, LLC entries 155 -> 651.

## Two titles used our vocabulary instead of the searcher's

"nyc marshal eviction list" is the largest single query the site takes
impressions on, 52 in 28 days, and /evictions has answered it since the eviction
build: 150 executions with address, neighborhood and date. The title said
"tracker". And BBLs are a real query class (3009970039: 37 impressions, 1
click), so the property description now carries the BBL.

Checked and found already answered, so left alone: every top address query
resolves to a live indexable 1,100-1,250 word page, and the eviction-by-place
pages carry "marshal" plus the place name in title, H1 and description.

## /eviction-case, the query class nothing served

"nyc marshal docket number search" is in the export, and so is a run of address
queries ending in "eviction cases", one at position 6.88 on Bing. Both are
someone holding a piece of paper with a number on it, and the site had all
42,567 of those numbers with nowhere to type one.

The page takes either number, because a tenant cannot be expected to know which
they hold. Marshal dockets are stored with inconsistent leading zeros (065592
and 64865 in the same export) so both sides compare with zeros stripped; index
numbers accept a slash, a hyphen or a stray space. A miss says what a miss
usually means: most Housing Court cases never reach an execution.

Only the empty form indexes. Two traps worth remembering: the shared SSR cache
keys on path plus `?lang` alone, exactly as its own comment warns, so the
location opts out with `proxy_cache off` rather than widening the key and
handing anyone a way to fill the zone with docket numbers; and `recent_sub` on
/evictions is both escaped and rewritten by the language toggle, so the CTA is a
sibling element with its own copy key in both languages.

## Fonts are self-hosted, which is what the LCP was waiting on

The homepage LCP element is the H1, at 3.2s against an FCP of 1.7s. The gap was
the font: paint in the fallback, then repaint when Bricolage arrives after a
handshake to fonts.googleapis.com, a stylesheet, and a second handshake to
fonts.gstatic.com. All three faces now come from /fonts, latin subset,
display=swap, with Bricolage halved to 41KB by dropping an optical-size axis
nothing here varies. Ten different Google Fonts URLs across the site are now one
set. CSP dropped both Google origins; csp_check reports 0 violations across 18
pages. plausible.io is the only third party left.

## Notes for next time

- **Do not measure or deploy during the 02:00 UTC nightly run.** Two vCPU: the
  scraper puts load above 4 and every page timing triples. A scoping query that
  runs in seconds at 21:00 hangs past two minutes at 02:00.
- IndexNow is validated (HTTP 200 now, not 202) and 1,000 URLs went in core-first
  after the sitemap regenerated, so the newly indexable LLC pages and
  /eviction-case were in the first batch.

## Next build, scoped but not started

**The condo address gap.** 17,114 deed BBLs are unit lots absent from PLUTO, so
a quarter of the deed record resolves to no address and /property and /llc say
so out loud. The approach to test is the one the LLC page already uses for ZIP:
map a unit lot to its block and take the address where the block is
unambiguous, falling back to DOF's PAD file for the rest. The measurement query
was written and never got a clean run because the nightly pipeline started;
re-run it against an idle box.

# PulseCities checkpoint, 2026-08-18 (session 3, later) — a Lighthouse run, and what it led to

Michael ran Lighthouse on the homepage: Performance 88, Accessibility 96, SEO
100, Agentic Browsing 2/3. Everything below came out of chasing those four
numbers, and the last item was found by measuring something the report never
looked at.

**SEO 100 -> 92 was my own regression, fixed within the hour.** The `IndexNow:`
line added to robots.txt earlier the same evening is not a robots directive, and
Lighthouse's validator scores an unknown directive as an error. The key file at
the site root is the actual proof of ownership and still serves 200, so IndexNow
is unaffected. `tests/test_indexnow.py` now asserts robots.txt carries no such
line, and `crawl_audit.py` probes the key file instead of grepping robots.

**Agentic Browsing 2/3 -> 3/3.** llms.txt listed bare URLs, which the audit reads
as prose: "File does not appear to contain any links". The Key pages section is
now 20 markdown links and picks up the surfaces added since it was written
(/displacement, /network/{slug}, the eviction pages).

**Accessibility: the grey ramp failed on the cards, not on the page.** Four
failing elements, all in the homepage docket: the +232% stamp, the BBL/ZIP line,
the ACRIS IDs. `tests/test_text_contrast.py` passed them because it measured ink
against the page background (#111823) while they sit on a card (#16202d) and on
a header tinted 5% orange (#21232c). The rule now measures against the LIGHTEST
surface the dark theme paints on, since clearing that clears every darker one;
listing several backgrounds would have proved nothing, because a colour passes
if it clears any one of them.

    --faint  #78838d -> #818c97    3.99 -> 4.51 on the worst surface
    --dim    #85929d -> #8a97a2    keeps the four stops half a point apart
    .gain    var(--stamp) -> var(--red)    3.94 -> 5.06, border keeps the stamp

**Performance.** Three separate things:

1. The LCP image srcset jumped 560w to 1120w, so a 412px phone at DPR 1.75 asks
   for 651px and gets the 1120w file: 74KB into a 719x385 box. A 760w variant
   closes the gap at 40KB. app.html's WebGL fallback was loading the same 1120w
   file into a 560px box.
2. Six API calls fired at once off the initial navigation, /api/flips 2,260ms
   deep in the critical chain. Only /api/stats feeds anything visible before a
   scroll. The docket edition swap now runs on requestIdleCallback after load
   (the card already renders a fallback arc from the HTML), and the operators
   and signals modules load on an IntersectionObserver, so a visitor who never
   scrolls never pays for them.
3. The hero's second hop was asking for data the first hop already knew.
   /api/stats names the top-risk ZIP; the pulse trace then fetched that ZIP's
   90-day history, 788ms of critical path for 1.3KB. /api/stats now carries
   `top_risk_history` in the same hourly cache entry, and a test asserts it is
   identical to /api/score-history so the hero cannot draw a different line
   from the ZIP page.

## The thing the report could not see: cold /network/flgsp took 12 seconds

nginx caches SSR for a minute and the family page caches for six hours, so this
never showed up in a browser. Measured against gunicorn directly it is real, and
crawlers hit cold workers because every reload empties the cache.

- The adoption pass called `_zip5` **1,225,926 times**, 5.5s of regex over data
  that does not change inside the loop. ZIP and token sets are computed once.
- The clustering query grouped all 116,261 deed party names when the LLC-form
  gate two lines later throws away four fifths of them. `LIKE '%LLC%'` in SQL
  leaves 23,444 and produces the same 26 families.
- The hot regexes were module-level `re.sub` calls, recompiled a few hundred
  thousand times a pass.

compute_families 6.0s -> 2.3s, and the startup cache warmer that already
pre-fills top-risk, GeoJSON and stats now fills this too, so nobody waits:
**/network/flgsp 12.0s -> 0.38s**, every other hub 30-40ms.

Lesson worth keeping: measure through the socket, not through nginx. Two layers
of cache had been hiding a 12-second page.

## Verified

Suite green, 1,240 passed. crawl_audit: PASS 22, WARN 0, FAIL 0. Browser-checked
that the deferred fetches fire on scroll, that drawPulseTrace makes no network
call when the bundled history is present, and that the 760w image is the
candidate the browser picks.

## Still open, unchanged

`retire_raw_data.sh drop` is the biggest infra win left and **everything is
ready**: archives written and sha256-verified on 2026-08-17, both models already
unmapped, migration b8e30d5c1746 written and pending, and the script refuses to
run without both preconditions. What remains is only the window, because
VACUUM FULL takes ACCESS EXCLUSIVE on the two biggest tables:

    ./venv/bin/python -m alembic upgrade head && scripts/retire_raw_data.sh drop

complaints_raw 9.5GB -> ~1.1GB, violations_raw 4.0GB -> ~1.1GB, out of a 16GB
database. Everything reading /property, /network or /neighborhood blocks while
it runs.

# PulseCities checkpoint, 2026-08-18 (session 3) — reading the families for the next FLGSP

The handoff asked for one thing: read the nine new entity families and find
another story the size of FLGSP. Two came out of it, and the reading turned up
four clustering bugs that were quietly wrong on the live site.

## The two stories

**A Washington private-equity firm is buying Brooklyn's small buildings.**
`docs/outreach/pitch-carlyle-townhouses.md`. Ten LLCs have taken title to 42
small Brooklyn buildings since April 2025, 38 of them bought from outside
sellers for **$197.5M**. Seven are called TOWNHOUSE RENTAL plus a roman numeral;
five deed party rows give the filing address as **THE CARLYLE GROUP, Washington
DC 20004**, and two adjacent names (BROOKLYN TOWNHOUSE PROPERTY OWNER III and
IV, 202 8TH STREET OWNER) file from the same place. 202 units, 35 of 42 built
before 1974, **zero evictions**, 47 open violations. The story is what kind of
building institutional money is buying, not harm: there is none on the record.
The pitch says plainly that an ACRIS party address is what the filer typed.

**$549.6M of nursing home real estate moved in five weeks.**
`docs/outreach/pitch-snf-nursing-homes.md`. Nine properties changed hands on one
day, 2026-03-13, for **$474.6M**, each bought by a company named
`<address> SNF REALTY LLC`, all nine filing from one Lakewood NJ address. A
tenth on the same pattern paid $75M for a Flushing property two weeks earlier.
Corroboration: PLUTO classes all ten as institutional, and on the same day as
each deed the mortgage was assigned to **Huntington National Bank**, off
Greystone Funding, M&T and Bank Hapoalim. This is a health-care desk story, not
a housing one, and the pitch says so.

**The FLGSP pitch was wrong and is now corrected.** It said $436M and 94
evictions. The real figures are **$451,300,000** (the 82 per-building prices sum
to that exactly) and **99 evictions, 98 of them predating the sale**. The old
numbers were low because two FLGSP companies were missing from the family, for
the reason below.

## `scripts/family_stories.py`, so the next one is found by running something

Profiles every family for story shape: bulk trade, unwind, assembly or hold;
units, DHCR registrations, open violations, evictions split by whether they
predate the family's own deed, counterparties, per-building deed IDs. Writes
`family_stories.json` and a ranked `family_stories.txt`. Rerun it after every
clustering change.

## Four clustering bugs the reading exposed

1. **Filing-address variants split a group.** `C/O: SUMMIT MALLS MANAGEMENT,
   LLC` and `C/O: SUMMIT MALL MANAGEMENT, LLC` keyed as different addresses, so
   two of the 82 FLGSP companies never joined the family and /network/flgsp was
   two buildings and $15.4M short. `_addr_key()` now normalises care-of,
   punctuation and entity form; `_zip5()` normalises the 10,638 ZIP+4 rows.
2. **No third way in for an orphan.** Adoption now pulls in an entity carrying a
   family's coined label when it files from a ZIP that family already uses, on
   the same exclusivity test the label rule applies.
3. **Cross-address merging on a common token invented a landlord.** MARINE PARK
   in Rockaway, DSA on West 72nd and 1 PARK ROW in Grand Rapids were one family
   on the shared token "PARK". /network/1-park-row-commercial is gone; the merge
   now requires the token to be coined.
4. **Transfers to yourself counted as sales.** TOWNHOUSE RENTAL "sold 9" when
   all nine moved between its own companies. Intra-family transfers are now
   excluded and the five-building floor is re-applied afterwards, which also
   retired /network/ddg.

Families went 19 to 26. New hubs: JOBER (Bronstein Properties), THOR (Thor
Equities), RSS (Rialto), KEYSTONE, LBUZ, MEEKER (Cedar Park Capital, and it
bought the BSP Greenpoint portfolio), LORDAE, HOLDINGZ, RAVE, FEROZE. Every
existing slug is preserved except the two that were wrong.

## Family pages carry their own record now

Building rows show units, rent-stabilized count, year built, open violations,
price and date instead of an address and a date. Added a price range, a
portfolio totals sentence, a "Who was on the other side" section naming
counterparties, and an explicit line when the deeds are internal transfers
(REDROCK's nine same-day deeds are exactly that). This also cleared the
near-duplicate guard, which the thinner new families were failing at 70-71%.

Copy fixes on the way through: `_entity_title` printed "Flgsp" (the vowel-less
rule only matched 4 letters) and "Carroll ST"; stat chips read "1 ZIP codes" and
"1 buildings sold"; a seller-only family had a heading reading "Where X buys";
four user-visible British spellings of "neighbourhood" against 131 American ones.

## IndexNow shipped too (handoff item 3)

The one Bing-specific lever, and the last WARN in `scripts/crawl_audit.py`.
Key `4494ce2738a74028c1babaef305aec53`, served at
`frontend/<key>.txt` (the catch-all nginx location picks it up, no config
change) and named in robots.txt. `scripts/indexnow_submit.py` reads the
generated sitemaps for {url: lastmod}, submits only what moved against
`indexnow_state.json`, core URLs first, capped at 5,000 a run so the first pass
does not dump 65,962 URLs at an endpoint that rate-limits for exactly that. Cron
at 03:25, after the 03:15 sitemap. **First real submission returned HTTP 202**,
which is accept-with-key-validation-pending; the backlog drains over the next
nights. `tests/test_indexnow.py` guards that the key file, robots.txt and the
script agree, that the file stays world-readable, and that unchanged URLs are
never resubmitted.

## Data notes worth keeping

- `rs_buildings` holds two sources. `dhcr` is registrations; `hpd_jurisdiction`
  is HPD multiple-dwelling apartment counts and **is not rent stabilization**.
  Filter `source='dhcr'` for any claim with the word stabilized in it. Mixing
  them inflated a portfolio by a third in the first draft of this work.
- PLUTO `units_res` can be badly stale: 438 4th Avenue reads 17 units against 51
  DHCR-registered units in 2022 and 2023.
- Deed amounts are per document. Where one price appears on two documents from
  the same seller on the same day, decide which reading you mean before
  printing a total (the Carlyle pitch gives both).

## New guards

`tests/test_entity_families.py` gained: filing-address variants collapse to one
key; no entity carrying a family's coined label from a family ZIP is left
outside it; every member shares a token or stem with the label. 20 tests there,
suite green.

# PulseCities checkpoint, 2026-07-15 (later) — #8 shared SSR nav DONE

The one real refactor the prior handoff queued is done and live (faedc35).

**What shipped:** a single `_ssr_nav(active, lang, toggle_html, track)` helper
(next to `_FOOTER_HTML`) now renders the top nav on EVERY SSR page. Before this
each page hand-rolled its own `<nav>` with a different link subset and none but
the homepage surfaced `/displacement` or `/this-week`. All 11 navs now carry the
full hub set: Map, Displacement, Neighborhoods, Operators, Flips, Radar, This
week, Methodology. The current page's link is brightened + `aria-current`.
- English pages pass `_LANG_TOGGLE_BTN` (JS EN/ES button) or nothing (this-week,
  week, property).
- Bilingual pages (neighborhoods dir, borough, **and the individual
  `/neighborhood/{zip}` page** — folded in beyond the recipe since it is the
  highest-value organic surface, 177 pages) pass a server-toggle anchor built
  from `alt_url` + the page's `LL`/`L` labels; ES labels come from
  `_SSR_NAV_LABELS["es"]`. localStorage `pc-lang` still auto-redirects ES users,
  so dropping the per-link `?lang=es` suffix does not regress ES persistence.
- Displacement keeps its `Showcase Nav` plausible funnel via `track=True` (7
  outbound links tracked; the self-link is active/untracked).
- Property gained the full hub nav (was brand-only); its `.nav-inner` CSS got
  `justify-content:space-between` + the mobile-wrap media query.

**Guard:** `tests/test_ssr_nav.py` (16 tests) — helper unit tests (hub set,
active marker, ES labels, toggle/track) + integration tests asserting every SSR
route's top nav carries all 8 hub links, incl. ES variants and week editions.
Adjacent guards re-run green: footer/analytics/breadcrumbs/displacement (19),
frontend_routes incl. palette (57), watch-cta/nbhd-flips/operator-seo (7),
ui-copy incl. em-dash (10). Live-verified via nginx: all pages 8/8 hub links.

**#9 progress (this session):**
- **Footer parity DONE** (ef987a0): all static footers (about/methodology/press/
  status/operator/developers) + the brief footer (`briefs.py`) now carry
  `/neighborhoods`, `/displacement`, and LinkedIn to match the SSR
  `_FOOTER_HTML`. `test_footer_consistency` CANON was tightened to require
  `/neighborhoods` + `/displacement`, so future drift fails.
- **Neighborhood lateral-link sections DONE** (d32a068): each `/neighborhood/{zip}`
  now renders "Operators active in {name}" (non-noise operators holding parcels
  in the ZIP -> `/operator/{slug}`), "More {borough} neighborhoods" (borough ZIPs
  by score -> `/neighborhood/{zip}`, self excluded), and an always-on
  `/displacement` CTA. List sections render only when non-empty; bilingual via
  `_NB_L`. Guarded by `test_neighborhood_lateral.py`.

- **nginx trailing-slash 301s DONE** (d75a989): a regex `location` 301s the
  slash form of every content route (`/flips/`, `/brooklyn/`, `/this-week/archive/`,
  etc.) to the canonical slash-less URL, query preserved. Copied to /etc, `nginx -t`
  clean, reloaded; 15 routes verified 301 -> 200. `test_infra_guards` still green.
- **/property in the sitemap DONE** (1e31c7c): only the ~1.5k buildings with BOTH
  a deed transfer AND an eviction (the arc the site documents) are listed, at
  priority 0.5 — NOT the ~912k parcels that merely inherit a ZIP score (doorway
  flood). `test_sitemap.py` guards the flood cap + that every listed property is
  200 + index,follow. sitemap regenerated live (1800 urls); the file is a cron
  artifact so only the generator + test were committed.

**#8, #9, AND P3 polish are now COMPLETE.** P3 this session:
- **/displacement sec-h -> h2** (e8f81df): the four section headers are now
  semantic `<h2 class="sec-h">` (identical render; reset zeroes margins).
- **"biggest NYC landlords" keyword** (e8f81df): /operators title is now
  "Biggest NYC Landlords by Acquisition Volume", H1 "The biggest NYC landlords"
  (server + JS EN/ES), meta rewritten; the sub-desc keeps the accurate
  "ownership clusters from ACRIS deeds" framing. Individual operator pages left
  alone (claiming "biggest" on a small operator would be dishonest).
- **Dynamic OG cards** (c56ef67): `_render_headline()` + `/og/borough/{slug}.png`
  and `/og/this-week/card.png` (slashed path so /og/{zip}.png can't swallow it).
  Borough card = tracked/avg/top; this-week card = score-history deltas
  (risers 7d, high-pressure count, citywide avg) NOT raw filings, which lag ~2wk
  and read 0 mid-lag. Pages point og/twitter:image at the cards.
  `test_og_cards.py` guards dynamic-not-default + references.

The full 2026-07-15 growth+SEO backlog (#8, #9, P3) is now shipped and
live-verified. Nothing left queued from it. Not pushed (Michael runs `git push`).
New guard tests this run: test_ssr_nav, test_neighborhood_lateral, test_sitemap,
test_og_cards. All adjacent suites green.

---

# PulseCities checkpoint, 2026-07-15 — growth levers + full SEO build

## Session outcome (consolidated)

Marathon session: growth levers, then a full 3-agent SEO audit and its fixes.
~21 commits, all live (working tree IS prod), all tested, NOT pushed.

**Live-verified this pass** (curl via nginx, 200 + content): `/`, `/displacement`,
`/property/{bbl}`, `/operator/{slug}`, `/this-week`, `/neighborhood/{zip}`,
`/flips`, `/radar`, `/operators`, `/neighborhoods` all 200. `/displacement`
serves real content + CollectionPage/BreadcrumbList; `/property` renders a real
address H1 (not the old map shell); all 12 SSR pages load Plausible once.

**Shipped (high-impact, done + live):**
- `/displacement` flagship; Plausible on every SSR page; watch-block conversion
  CTA (EN/ES); per-ZIP recent-flips section; weekly post-pack automation.
- Real `/property/{bbl}` SSR bodies (Place + BreadcrumbList, noindex when thin).
- Operator pages: Dataset + BreadcrumbList schema, addresses/ZIPs link to
  `/property` + `/neighborhood`.
- `/this-week` schema (NewsArticle+ItemList+BreadcrumbList); BreadcrumbList on
  `/displacement /operators /neighborhoods /flips /flips-editions /radar` via
  `_crumbs()`.
- On-page: neighborhood titles un-truncated (177 pages), `/map` title, trimmed
  descriptions, `/status` noindex, `/map` sitemap priority 0.9->0.6.
- Homepage Organization schema; `/displacement` in homepage nav + footer + all 13
  SSR footers.
- Guard tests added throughout (displacement, ssr-analytics, watch-cta,
  neighborhood-flips, property, operator-seo, breadcrumbs).

**Internal linking to new pages (verified):**
- `/displacement`: homepage nav + homepage footer + EVERY SSR page footer.
  NOT yet in the individual SSR page TOP navs (that is the pending nav refactor).
- `/property`: linked from `/flips`, `/radar`, `/this-week`, `/flips/editions`,
  `/displacement`, the neighborhood recent-flips section, and operator pages.

**NOT fully optimized — remaining (queued, tasks #8/#9):**
- #8: shared SSR nav constant. `/displacement` is in footers but not the SSR page
  top navs; each nav is hand-rolled and inconsistent. Real refactor across ~10
  page heads; deliberately left for a fresh session.
- #9: neighborhood lateral-link sections (operators-buying-here,
  nearby-neighborhoods); `/displacement` in the about/methodology/press footers;
  nginx trailing-slash 301s; `/property` in the sitemap (index file for volume).
- P3: dynamic OG images (borough/this-week); `/displacement` sec-h `<div>`->`<h2>`;
  "biggest NYC landlords" keyword weave.

**Push:** pushed to GitHub (`main`, private repo `michaelespin15-code/pulsecities`)
through fe460e1 on 2026-07-15. Agents cannot push; Michael runs `! git push`.

---

## For a NEW chat session: environment, patterns, gotchas

**Deploy model (unchanged, critical):** the working tree IS production.
- Static files (`frontend/*.html`, sitemap.xml, robots.txt) are served by nginx
  straight from disk — edits are instantly live, no reload.
- Python (FastAPI/gunicorn) runs as systemd unit `pulsecities`. After editing
  any `.py`, `systemctl reload pulsecities` (import-check FIRST:
  `venv/bin/python -c "from api.main import app"` — a syntax error left unreloaded
  is invisible; a bad reload crash-loops).
- nginx config lives in `deploy/nginx-pulsecities.conf`. To change routing: edit
  it, `cp deploy/nginx-pulsecities.conf /etc/nginx/sites-enabled/pulsecities`,
  `nginx -t`, `systemctl reload nginx`.
- **New SSR routes need an nginx `location = /route` proxy block** — SSR pages are
  individually allow-listed; an unlisted path falls through to static and 404s.
  This bit /displacement; don't forget it for new routes.
- The nightly-generated files (`frontend/sitemap.xml`, `frontend/llms.txt`,
  `scripts/*_state.json`, `eviction_flips_editions.json`) show as `M` in git
  every session — they are cron artifacts, NOT your edits. Do not commit them
  with feature work.
- Box is 4GB/2CPU; run pytest in targeted subsets, not the whole suite at once.

**Verify a page:** `curl -s -k -H "Host: pulsecities.com" https://127.0.0.1/PATH`
or TestClient (`from fastapi.testclient import TestClient`). Both hit the prod DB
(single-DB box) — reads are safe; NEVER let a test commit to prod tables (use
dry_run + rollback; see the audit-2026-07-11 note below).

**SSR page pattern (all in `api/routes/frontend.py`, ~4000 lines):** each page
is a full-HTML f-string with inline `<style>` (dark theme `#0f172a`), built with
these module helpers: `_jsonld(obj)`, `_set_meta()`, `_tier_info(score)`,
`_FOOTER_HTML` / `_FOOTERS[lang]`, `_crumbs(*(name,path))` (BreadcrumbList for a
JSON-LD @graph), and `{_PLAUSIBLE}` injected right after the
`<script type="application/ld+json">{jsonld}</script>` line (this is how every
SSR page loads analytics — keep the pattern on new pages).
- Property page: `_build_property_page()`. Neighborhood: `_build_neighborhood_page()`
  (bilingual via `_NB_L[lang]`, `?lang=es`). Displacement: `displacement_page()`
  with `_approved_flip_arcs()` (named eviction-flip arcs come ONLY from approved
  editions — the human review gate; do not bypass it).
- **Palette guard:** never use the retired bright greens (`#4ade80`, `#22c55e`,
  `#16a34a`, `#eab308`) anywhere in `frontend.py`/`briefs.py` — `test_frontend_routes
  ::TestCanonicalTierBands` fails the build. Use canonical `#3E6B54` (positive),
  `#C08B2D`, `#F97316`, `#EF4444`.
- **Footer guard:** `test_footer_consistency` requires the CANON link subset on
  every footer; the SSR footer is `_FOOTER_HTML`. Extra links are allowed.
- **No em dashes in UI copy** (Michael's standing rule).

**Tests added this session:** test_displacement_page, test_ssr_analytics,
test_watch_cta, test_neighborhood_flips, test_property_page, test_operator_seo,
test_breadcrumbs, test_flips_postpack, plus the base-scraper source-unchanged
guards.

## Remaining work — executable recipes

**#8 shared SSR nav constant (the one real refactor left).** Every SSR page
hand-rolls its own `<nav>` and they disagree (different link subsets; none but
the homepage include /displacement). Build one `_SSR_NAV` constant (mirror
`_FOOTER_HTML`) with the full hub set — Map, Displacement, Neighborhoods,
Operators, Flips, Radar, This-week, Methodology — and interpolate it into each
page's nav: operators, neighborhoods, borough, flips, flips/editions, radar,
this-week, week (`_week_nav_html`), displacement, property. Verify each renders
and run the frontend + footer + analytics tests. ~10 nav blocks; do it carefully.

**#9 remainder:**
- Neighborhood lateral sections in `_build_neighborhood_page()` (render only when
  non-empty, like the flips section): "Operators buying in {name}" (query
  `operators` via `operator_parcels`→`parcels` filtered to the ZIP, link
  `/operator/{slug}`), "Nearby neighborhoods" (same-borough ZIPs by score, link
  `/neighborhood/{zip}`), and a `/displacement` CTA link. Bilingual copy keys in
  `_NB_L`.
- Add `/displacement` (+ LinkedIn for parity) to the about/methodology/press
  static footers.
- nginx trailing-slash 301s: a regex `location` returning 301 for the exact-match
  content routes (currently `/flips/` etc. 404). Place before the catch-all.
- `/property` in the sitemap: `generate_sitemap.py`, query BBLs that have signals
  (index the substantive ones only), split into a sitemap index if volume is large.

**P3 polish:** dynamic OG images for `/borough` + `/this-week` (infra exists in
`api/routes/og_images.py`); `/displacement` section headers `<div class="sec-h">`
-> `<h2 class="sec-h">`; weave "biggest NYC landlords" into operator titles/H1.

## Action items on Michael (only he can do)
- **Anthropic credits**: top up, or the AI read (`/api/summary`) keeps returning
  503 to visitors (it degrades gracefully with a cooldown, so no crash).
- **Before the repo goes PUBLIC** (part of the job-search "ship the proof" plan):
  scrub `DATABASE_URL` from git history (`git filter-repo`) and rotate the
  Postgres password — it was committed in the April initial commit. Anthropic key
  + NYC token in that old `.env` were placeholders; Resend/R2 keys were never
  committed. Fine while the repo stays private.
- Distribution: post this week's flip post-pack; send the 153% reporter tip.

## Bigger-picture backlog (pre-existing, not this session)
Growth bottleneck is distribution, not features (~4 real external subscribers,
near-zero human traffic, Googlebot crawling fine). Parked directions: "ship the
proof" (repo public + builder write-up), automate-the-drop (DONE this session),
conversion instrumentation (Plausible now on all pages — build the funnel views).

---

# PulseCities checkpoint, 2026-07-14 — digest retime, evictions guard, drop automation, /displacement showcase

## Growth build (later session): /displacement + SEO push

Michael picked "build, don't post" and asked to build the three growth levers
plus optimize SEO all around. Standing prefs honored (autonomous, no prompts).
Progress this session, tracked in the task list (#1 done, #2-#5 pending):

- **/displacement flagship SHIPPED** (e39f58b). One SSR destination pulling the
  strongest signals into a narrative: eviction-to-resale arcs, highest-pressure
  neighborhoods, largest landlords, buying clusters. Each section deep-links out
  (/flips/editions, /neighborhoods, /operators, /radar). Live at
  https://pulsecities.com/displacement. Full meta/OG/JSON-LD (CollectionPage),
  dark editorial theme matching the other SSR pages, cached _PAGE_TTL.
  - **Approval gate held**: named eviction-flip arcs come only from APPROVED
    editions via `_approved_flip_arcs()` (17 approved arcs today); the 3 pending
    W28 arcs stay off. test_displacement_page.py guards this + rendering.
  - **Plausible wired on this page** (nav/section/CTA events) as the first SSR
    page with funnel tracking. Data queries reuse existing shapes: displacement_
    scores for hot ZIPs, operators table for landlords, query_flips/query_radar.
  - New route needed a **nginx `location = /displacement` proxy block** (SSR
    pages are individually allow-listed; unknown paths 404 as static). Edited
    deploy/nginx-pulsecities.conf, cp'd to /etc, nginx -t, reloaded. Registered
    in generate_sitemap.py (priority 0.9).
- **Homepage links to it** (04cd936): nav_displacement in desktop nav + More
  menu, EN + ES. Later also added to the shared SSR footer, EN + ES (b9f6649),
  so all 13 SSR pages link it. Was otherwise an orphan (sitemap-only).

- **#2 Plausible on every SSR page DONE** (e06b55a). All 12 SSR page heads build
  their own <!DOCTYPE> and were untracked; now a shared `_PLAUSIBLE` head const
  is injected after the JSON-LD line across all of them (+ /this-week via its
  canonical anchor, no JSON-LD block). test_ssr_analytics.py fails if any SSR
  page ships without analytics or double-injects. Funnel events live on the
  showcase; the subscribe conversion event ships with #3.
- **#3 watch-this-block CTA DONE** (8f05615). Bilingual (EN/ES) subscribe card
  on the neighborhood SSR pages, the organic landing surface. POSTs the page ZIP
  to /api/subscribe, fires plausible('Subscribe') + 'Neighborhood Watch Submit',
  closing search -> view -> subscribe. JS built with json.dumps for safe ZIP/copy
  interpolation; success uses canonical #3E6B54 (the stale-green palette guard
  test_ssr_tier_colors caught #4ade80 on the first pass). test_watch_cta.py
  guards presence/wiring/localization/palette. OG cards + copy-link already
  existed on these pages, so per-page share/OG was already covered.

- **#4 recent-renovation-flips section DONE** (484b4d5). Folded per-ZIP flips
  (LLC deed + A1/A2 permit within 60d, past 365d) into neighborhood SSR pages as
  unique indexable content + /property internal links, bilingual, renders only
  when non-empty. Chose this over new per-ZIP pages precisely for the doorway
  risk: flips are sparse (top ZIP ~2). test_neighborhood_flips.py guards it.
- **#5 sitewide SEO pass — partial** (b9f6649, 466e85d). See audit below.

## Full SEO audit (3 parallel agents, 2026-07-14) + fixes applied

Michael: "do as much SEO as needed, use agents, full audit." Dispatched 3
general-purpose agents (technical / on-page+schema / content+internal-linking),
all verified against live pages + source. Headline: **technical SEO is healthy**
(no criticals — robots/canonicals/hreflang/redirects/404s/sitemap all correct).
The real wins are on-page schema and internal linking.

Applied this session (466e85d):
- Neighborhood <title> dropped borough: 73 -> ~68 chars, stops SERP truncation
  across 177 pages (EN+ES). /displacement desc 204 -> 167. /map title
  'Explore' -> 'NYC Displacement Risk Map | PulseCities'. /status noindex.
  /map sitemap priority 0.9 -> 0.6.

Audit fixes shipped this session:
- **#6 /property/{bbl} real SSR bodies DONE** (a0d5dc6). Was the app.html map
  shell (H1 "PulseCities", identical across parcels). Now renders per-building
  public-record body (address H1, area score, ownership transfers, evictions,
  permits, 311 volume) + Place + BreadcrumbList JSON-LD + Plausible + up-links to
  /neighborhood, owning /operator, borough. Buildings with no records/score are
  noindex,follow so thin pages don't dilute the index. _build_property_page().
  test_property_page.py guards real-content + noindex gate.
- **#7 operator pages DONE** (44a10c5). Added Dataset + BreadcrumbList JSON-LD
  (was zero schema); acquisition addresses now link to /property and ZIPs to
  /neighborhood in the server HTML; client portfolio table addresses link to
  /property too. test_operator_seo.py guards it.
- **#9 partial** (f1272f2): homepage Organization schema (logo/founder/sameAs)
  added; /displacement added to the homepage footer.

- **#8 mostly done** (b0e9204, 6bf7348): /this-week now emits NewsArticle +
  ItemList + BreadcrumbList (was zero); BreadcrumbList folded into /displacement,
  /operators, /neighborhoods, /flips, /flips/editions, /radar via a shared
  _crumbs() helper (JSON-LD @graph). test_breadcrumbs.py guards coverage.

Still queued:
- **#8 remainder** (riskier, left for a fresh session): ONLY the shared SSR nav
  constant is left — every SSR page has a different hand-rolled nav that omits
  /displacement. It is a real refactor across ~10 page heads; do it deliberately,
  not at the tail of a marathon session.
- **#9 remainder**: neighborhood lateral-link sections (operators-buying-here,
  nearby-neighborhoods, /displacement CTA); /displacement + LinkedIn in the
  about/methodology/press footers; homepage operator/signal module rows as links;
  nginx trailing-slash 301s; /property in the sitemap (index file for volume).
- Minor/P3: dynamic OG images (borough/this-week); og:image dims on ~11 pages;
  /displacement sec-h divs -> h2; "biggest NYC landlords" keyword weave.

Session commits (SEO push): e39f58b, 04cd936, e06b55a, 8f05615, b9f6649, 484b4d5,
466e85d, a0d5dc6, 44a10c5, f1272f2 (+ checkpoints). All live (working tree is
prod); nginx cp'd to /etc and reloaded; NOT pushed (Michael runs git push).

## Earlier session (three questions -> drop automation)

Michael opened with three questions (where to post the flip email, why the weekly
digest sends 5am Sunday, why a pipeline-anomaly email fired), then "what else can
we optimize"; he chose "automate the drop." Three shipped, two commits.

## Shipped

1. **Digest retimed to Sunday 6:00 PM ET** (f48d7e6). Was `0 9 * * 0` UTC = 5am
   EDT, the worst open window. Now DST-pinned like the donna cron: fires at the
   22:00/23:00 UTC slots that straddle 18:00 Eastern and guards on the Eastern
   clock, so it stays 6pm ET across DST. config/schedule.py (DIGEST_CRON
   `0 18 * * 0`, tz America/New_York) and deploy/pulsecities.cron updated in
   step; deployed to /etc/cron.d, gunicorn reloaded, /api/schedule verified live.
   Only the digest moved; flips scan (09:30) and ops-health (09:45) are internal
   emails and stayed put.
2. **Evictions "0 records" anomaly was a false positive, now suppressed**
   (f48d7e6). Not a break: source has 961 records/30d, max executed_date 07-07;
   we pulled ~189/night 07-06..10 draining a catch-up backfill and are now caught
   up, so on_conflict_do_nothing returns 0 new. base.run() now compares
   new_watermark to the last successful watermark: a 0-record run whose watermark
   did not advance is steady state (INFO, status success, no page); a 0-record
   run with no evidence the source advanced still warns. Generic in base.py, so
   any lookback scraper benefits; no evictions.py change. **ACRIS still alerts by
   design** (genuine 14-day upstream freeze, watermark stuck at 06-30; that is a
   real outage worth surfacing, not weekly-cadence quiet).
3. **Drop automation** (05b59d6). The weekly flip email now appends a
   ready-to-post pack: X thread (numbered, budgeted under 280 incl. the k/N
   suffix, ACRIS docs dropped first on overflow), Bluesky post on the biggest
   gain (under 300), reporter tip with the deed numbers + buyer portfolio scale.
   Ships in the existing Sunday 09:30 flips cron, only when there are new arcs.
   Nothing auto-posts; it lands in the review email. test_flips_postpack.py
   guards char limits, thread shape, receipts, and the no-em-dash rule.

## Traffic reality check (the real bottleneck)

- **~4 real external subscribers.** 8 distinct rows in `subscribers`; the rest
  are Michael (michaelespin15 x2, michael.e@caprium, mespin@caprium) and one
  mailinator audit account. Real external: jhonsassler, hbpmes0730, jvxnyc,
  pulgarinkevin73.
- **Near-zero human traffic.** Today's requests are dominated by /api/health and
  bots; a handful of real content page views. Googlebot IS crawling (38 hits),
  so SEO plumbing works. The product out-features every competitor; the missing
  piece is attention, not another signal.
- Directions Michael did NOT pick this session (parked): **ship the proof**
  (make repo public after a git-history secrets scrub, builder write-up, reporter
  pitch) and **fix conversion** (funnel events via the wired Plausible, a
  watch-your-block hook, per-page share/OG).
- **On Michael:** confirm the Anthropic credit balance. The AI read degrades to
  503 on a failed model call; if credits are exhausted the headline feature is
  dark to any visitor who clicks it. Not re-verified this session (billing).

## Verification

test_base_scraper.py 7 pass, test_flips_postpack.py 7 pass, evictions+ownership
88 pass, pipeline_health+status 75 pass. Post-pack rendered end-to-end against
this week's three real arcs (all tweets 168-205 chars, Bluesky 232). Nothing
else touched.

---

# PulseCities checkpoint, 2026-07-11 (early morning) — full audit #2 closed

## Build sessions (~05:00–06:00 UTC, Michael approved "lets do the next build sessions")

1. **LLC-to-LLC filter: already shipped 2026-04-20** (77d9419). v2_roadmap.md was
   stale and is now marked; the 38% churn figure describes what the live filter
   excludes. No re-score happened or was needed.
2. **Offsite backups LIVE** (225d4a4): backup_offsite.sh pushes the newest dump
   to R2 nightly 04:10 (vs-archive bucket, pulsecities-backups/ prefix, borrowed
   violation-leads token; PULSECITIES_R2_* env vars switch to a dedicated bucket).
   Weekday slots + monthly pin = zero-maintenance retention. Uploads via rclone
   (apt-installed): curl 7.81 cannot sign streamed bodies and cannot slurp 1.6GB.
   First push byte-verified. False-alarm ops email fired during testing from the
   sandboxed shell; the real push succeeded.
3. **Vacate orders surfaced** (in f26e489): "Vacated by city order" section on
   neighborhood pages (distinct buildings + orders + latest month, 365d window,
   display-only). Bed-Stuy shows 8 buildings / 10 orders at launch.
4. **Spanish SSR shipped for the whole ranking funnel** (f26e489, f94faa0):
   /neighborhood/{zip}, /neighborhoods, and all five borough pages render fully
   in Spanish at ?lang=es (titles, metas, generated summary via bilingual
   _build_summary, FAQ + FAQPage JSON-LD, dates, footer). English is the
   parameterless canonical; hreflang en/es/x-default on both; EN/ES toggle
   stores pc-lang (site-wide key) and English pages honor a stored 'es'.
   /this-week already had a client-side ES layer on the same key; nothing needed.
   nginx borough proxy now forwards query strings ($is_args$args) — it was
   silently dropping them.

## Post-audit batch (~04:30–04:50 UTC, Michael approved)

- **HPD class-I violations now ingest** (ed66cb1): scraper accepts A/B/C/I;
  365-day backfill landed 68,898 rows incl. 1,742 vacate orders. Scoring
  stays B/C only — TestClassIGate pins the filter at every B/C-labeled
  surface. Watch alerts and operator monitors now include class I, so a
  vacate order on a watched building emails the watcher.
- **AI-read failure cooldown** (30db10f): 10 min per worker after a failed
  model call; panels get an instant 503 instead of a 3.5s doomed round-trip.
  /week/{current} 302s to /this-week.
- **Incident, 90s**: the cooldown's first deploy had `global` after use;
  gunicorn crash-looped 04:39:50–04:41. The NEW health probe caught the 502
  at 04:40:03 and escalated (first real proof the alert path delivers; a
  recovery all-clear follows). Rule reaffirmed: import-check every touched
  module BEFORE `systemctl reload pulsecities`.

Suite green (884 passed / 2 skipped before the fixes; re-run green after, plus 18
new guard tests). Site verified live end to end after every change. Box was
resized: 4GB RAM / 2 vCPU now (hostname still says 1gb). Tests still run in two
halves by convention.

## The headline finding (root-caused and fixed)

**The test suite was mutating production score data on every run.**
- `test_scoring_guard.py` ran `DELETE FROM score_history WHERE scored_at = today`
  and committed it — every post-scrape test run silently destroyed that day's
  snapshot. This is why score_history was missing 2026-07-10 and 2026-07-11.
- `TestOrphanCleanup` ran a real `compute_scores(force=True)` against prod
  mid-suite; `test_dhcr_scraper` transiently rewrote ZIP 10026 with synthetic
  data (visitors could see score 50 for Harlem for a few seconds).
- All three now run inside uncommitted transactions (`dry_run=True` +
  rollback); verified byte-identical DB state across a full run.
- Both missing snapshot days were recomputed via the history-only backfill path
  (177 rows each, averages continuous: 30.27 / 30.59 / 30.57).
- The nightly pipeline now has a snapshot invariant gate: scored count must be
  in today's score_history or the run fails loudly.

## Also fixed this session (2026-07-11, ~03:10–04:20 UTC)

1. **Monitoring last mile** (the June-outage class): `send_alert` buffered
   anomalies now flush into ONE ops email per pipeline run; `notify_ops()` is
   the severe path (webhook + immediate email); scoring crash / zero-scored /
   missing snapshot all fail loudly; health probe dedupes (one alert per
   outage + 6h re-alert + recovery all-clear). `ALERT_SNOOZE=dcwp_licenses` in
   .env silences the known upstream stall (remove when it recovers).
2. **Perf**: cold `/api/stats` was 10–37s, now **0.87s**. Causes: stale
   visibility map on violations_raw (VACUUM ANALYZE + per-table autovacuum
   tuning on the 4 big tables) and a missing complaints (created_date, zip)
   partial index (migration `b9e4f2a7c1d8`, which also drops two redundant
   indexes). Do not remove the autovacuum reloptions.
3. **llms.txt honesty**: generator fetched the 1h-cached HTTP endpoint, so the
   file quoted yesterday's scores every morning and hardcoded "high
   displacement pressure" for all five entries. Now reads the DB through
   `compute_top_risk()` (extracted from stats.py, shared) and takes tier words
   from `_tier_info`. Atomic writes with 0644 (mkstemp is 0600 → nginx 403,
   found live) for llms.txt + sitemap.
4. **nginx**: security headers were missing on every static page (add_header
   inheritance) — now a snippet included per location + server, with HSTS
   max-age=86400. tailwind.min.css no longer 30d-immutable (1h
   must-revalidate). Doubled Cache-Control (expires+add_header) cleaned up.
5. **logrotate** signalled nonexistent gunicorn.service; would have silently
   ended gunicorn logging at the first 50M rotation. Fixed, mirrored to
   deploy/pulsecities.logrotate, and every cron log is now rotated.
6. **Script robustness**: backup dumps to .tmp + gzip -t + mv (no more
   truncated "newest backup"); flips scan state corrupt-guard + atomic write,
   and it refuses to clobber the editions archive on a bad read; OCA ingest
   refuses a >20% shrunken upstream extract and filters the 99999 sentinel
   (19 phantom rows purged); ops-health can't crash silently; building alerts
   wait for the pipeline lock (up to 45 min) before advancing the watermark;
   missing RESEND_API_KEY exits 1 so cron sees the failure.
7. **API**: key middleware survives DB outage (401, not raw 500) and the cache
   prune race; ops token constant-time compare; ops log tail bounded (64KB
   seek); /api/health accepts HEAD (uptime monitors); search escapes LIKE
   wildcards; CORS comment resolved (deliberately open, documented).
8. **UI copy**: six `—` em-dash connectors in app.html JS strings became
   middle dots / rephrases; operator page shows a friendly EN/ES notice on
   hydrate failure (was silently blank), sets title/canonical AFTER slug
   resolution (was clobbering SSR and self-canonicalizing raw params);
   methodology signals table scrolls in a wrapper; homepage chip now says
   "LLC transfers in the last 90 days" (was "on record").
9. **Data hygiene**: scraper_quarantine 284MB → 4MB (208k known-benign HPD
   class-I rejects >30d pruned, VACUUM FULL) + 90-day retention in the nightly
   pipeline; digest citywide trigger now uses the canonical High=67 (was 75);
   dead jobs.sqlite removed.
10. **New regression guards**: sitewide em-dash test (catches `—` and
    `&mdash;`, comments and placeholder glyphs excluded, ops.html exempt);
    llms.txt-vs-stats consistency (structural + live); deploy/ vs /etc drift
    tests; logrotate-covers-every-cron-log test.

## NEEDS MICHAEL (priority order)

1. **Anthropic credits still EXHAUSTED** (verified live 03:15, fresh 400). Map
   AI read fails politely; Sunday's digest goes out WITHOUT AI narratives
   unless topped up before 09:00 UTC. console.anthropic.com.
2. **`git push`** — 40+ commits ahead of origin (env blocks the agent's push).
3. **Search Console submission**, then the press pitch (ACRIS thaw makes the
   data fresher than any time in 6 weeks — good week to send).
4. ~~Class-I decision~~ — RESOLVED: ingested + displayed, never scored (see
   post-audit batch above). Press angle now available: vacate-order counts
   by ZIP are quotable numbers nothing else on the site captured before.

## News

- **ACRIS thawed 2026-07-11**: 15,806 ownership rows overnight, watermark
  2026-06-30 (a day earlier than DOF's estimate). Expect big ingests for a few
  nights while the 43-day backlog clears; scores will move.
- DOF CardinalityViolation (4 failed nights 07-06..09) was already fixed in
  c015b50 (2026-07-09, batch de-dupe) — verified, no action.

## Watch

- **Sunday 2026-07-12**: digest 09:00 (dry-run validated this session: 6 ZIP +
  2 citywide render clean), flips scan 09:30 (quiet = expected), restore-test
  05:00, ops-health 09:45 (now also the end-to-end proof that ops email
  delivers — if no email arrives Sunday morning, that's itself the finding).
  Mon 04:15 first OCA cron (now with shrink guard).
- Tonight's building-alerts first cron ran clean (0 watches with new records).
- Swap is ~75% used steady-state; box shares with other services. Fine today.

## Open decisions (parked, Michael's call)

- OCA petitions as 7th score signal (breaks 187d comparability — deliberate or
  not at all). LLC-to-LLC filter (v2 roadmap): measured this session — **38% of
  the 180d LLC-acquisition signal is corp-to-corp churn** (3,235 of 8,517).
  Strongest signal-quality improvement available; also breaks comparability,
  so consider bundling both re-scores into one announced methodology change.
- Spanish SSR pages (/neighborhood, /this-week, borough pages are EN-only, the
  most shareable pages drop ES readers). Offsite backups (everything dies with
  the disk). Plausible upgrade. Per-key API tiers. Gunicorn access-log
  timestamps (needs unit edit + restart, not just reload).

## Facts the next session should not re-derive

- Deploy model unchanged: working tree IS production; `systemctl reload
  pulsecities` for Python; nginx: edit deploy/, cp to /etc, nginx -t, reload;
  push blocked for agents, Michael runs `! git push`.
- Integration tests hit the PRODUCTION DB by design (single-DB box). The rule
  that keeps this safe: any test that writes score tables stays inside an
  uncommitted transaction (dry_run + rollback). Never add a test that commits
  to prod tables.
- llms.txt + sitemap generators must chmod 0644 after mkstemp or nginx 403s.
- Canonical palette + thresholds unchanged (Low<34, Moderate<67, High<85,
  Critical 85+); digest, llms, ai_summary, frontend all pin to _tier_info.
- The concurrent `claude --resume` sessions on this box belong to other
  projects (/root/michaelespin, /root/violation-leads); check
  `readlink /proc/PID/cwd` before assuming they touch pulsecities.

## 2026-08-07 — Full audit sweep + de-AI design pass (session end)

Five-agent audit (backend, frontend, copy, visual, infra) and same-day fix
pass; 15 commits, 25ac829..8c8d185. What changed that later sessions build on:

- **Palette is now house-owned.** Every framework hex is retuned (accent
  #ed6317, bg #111823, muted #93a1ad, link #6fb1d8, stamp #e4483b); Tailwind
  stops overridden in tailwind.config.js so utilities follow. Ramp Low
  #3E6B54 / Moderate #C08B2D unchanged. A stock #f97316 anywhere is a
  regression now.
- **Design language tightened:** stat-card grids on /displacement and
  /this-week are ledger strips; landing modules are ruled columns; hero
  pulse-trace draws the top-risk ZIP's real 90-day line; map preview re-shot
  from the live app (was the old bright-green ramp); neighborhood h1 is
  "Name ZIP" under a kicker, no title-tag pipe; trend chart ticks on human
  steps.
- **Live bugs fixed:** renovation-flip endpoint 500 (psycopg int, _days
  guard), map app back-button trap, /displacement literal &middot;, press.html
  false "feed paused" claim, ops.html XSS, ~500 lines dead hero code purged.
- **Ops hardened:** nginx server_tokens off, Referrer-Policy, /ops.html 404,
  week+detail trailing-slash 301s, limit_req on /property /brief /og (all
  mirrored to deploy/); OOMScoreAdjust -500 on the app unit, -800 drop-in for
  postgres (applies at its next restart); daily_health_check + pipeline_health
  finally in cron (03:40/03:45) and their alerts actually flush; Resend ops
  email retries; ownership scraper retries party/legal fetches and fails loud
  so the watermark can't skip batches.
- **Repo hygiene:** sitemap.xml, llms.txt, state JSONs untracked (nightly
  churn); tree stays clean.
- **Deferred (needs Michael):** purge .env from git history or rotate the DB
  password BEFORE the repo goes public; CSP report-only; shared_buffers raise;
  dedicated PULSECITIES_R2_* creds (offsite currently borrows
  violation-leads'); og-image.png still says 178 ZIPs.

## 2026-08-07 (later) — Traction pages

/evictions (citywide marshal-eviction tracker, hub nav, FAQ schema, ES
strings) and /who-owns-my-building (tenant-intent landing, top buyers,
official registry links) shipped in 3abd296, driven by Search Console
demand. Notes for later sessions: new SSR routes 404 until nginx gets a
`location =` block (deploy copy synced); hub nav is 9 links, .nav-inner
960px; _addr_title() fixes str.title() ordinal mangling on all-caps
addresses; property titles now lead with the records promise, not score
jargon (874dd52). Remaining traction backlog lives in the traction-pages
memory note.

---

# PulseCities checkpoint, 2026-08-18 — typography, CSP, tier bands, raw_data staged, and the first real Search Console read

## Shipped (7 commits, 96c87ad..ceb20ab, all live, suite green 1,282 passed)

- **96c87ad** tabular figures + `text-wrap: balance`. The premise needed narrowing:
  Google serves DM Sans with **no `tnum` table**, so `font-variant-numeric` is dead
  CSS on it, and JetBrains Mono is already fixed-pitch. Only `.arc-gain`/`.row-val`
  on /displacement qualified (Bricolage is proportional *and* ships tnum). Measured
  live: gain chips spanned 53.6–74.7px before, 72.38px each after.
- **0a582a1** CSP report-only + Tailwind content-hash stamping. `npm run build:css`
  now also runs `scripts/stamp_asset_hash.py`; **commit app.html with
  tailwind.min.css** and never run `tailwindcss` directly.
- **1ca54ed** the OG card said "178 NYC ZIP codes". Not staleness — the wrong
  query: `neighborhoods` carries a **99999 sentinel** row. Canonical is **177**.
  `scripts/generate_og_image.py` regenerates it from the DB.
- **0cf5e08** tier bands (85/67/34) were hand-written in **ten** places; now
  `scoring/tiers.py`. Colour deliberately NOT centralised (dark page / risk ramp /
  paper ink differ on purpose). Verified behaviour-identical: 7,007 comparisons.
- **1ea21e4** the raw_data archive's R2 upload had never worked. **R2 layout:
  bucket `vs-archive`, prefix `pulsecities-backups/`**; derivation now lives once
  in `scripts/lib/r2_creds.sh`, shared with backup_offsite.sh.
- **870e2a4** /flips and /radar claimed windows their deeds no longer covered.
  Both now carry the /evictions-style through-line, EN + ES.
- **ceb20ab** raw_data drop staged (see below) plus small backlog.

## The one thing waiting on a maintenance window

    ./venv/bin/python -m alembic upgrade head   # applies b8e30d5c1746
    scripts/retire_raw_data.sh drop             # runs the migration + VACUUM FULL

Archives are done and verified offsite (5,148,918 + 2,070,570 rows). Expect
16GB -> ~7GB, and the 1.7GB nightly dump falls with it. **Do not restore the old
ordering**: migration a1f4c07b9e52 made the columns nullable first because both
were NOT NULL with no server default, and the scrapers build plain dicts for a
Core insert — deploying the code before the drop would have failed the 02:00
scrape. Also do `pg_stat_statements` in the same window (config is installed at
`/etc/postgresql/14/main/conf.d/`, needs a **restart**, then `CREATE EXTENSION`).

## Corrections — believed true, actually false. Do not act on these again.

- **LLC-to-LLC filter is already live in scoring** (`NOT EXISTS` on an LLC grantor
  in `_aggregate_llc_acquisitions`). The parked "38% corp-to-corp churn" decision
  is stale.
- Of five "unused" deps, only **APScheduler** was dead. numpy <- shapely; xlrd,
  openpyxl, tqdm <- nycdb, used as a CLI by backfill_rs_history.py.
- **idx_parcels_geometry stays.** 74MB / zero scans is true, but it is the GiST
  index on a mapping product's geometry column and worth <1% post-drop.
- A health_check.log CRITICAL dated **before 2026-08-17 16:38 UTC** is a false
  alarm from the old local 14-day ACRIS threshold (fixed in 278a8ee).
- `scripts/data_health_check.json` is tracked and reports "critical" from
  **2026-04-18**; dof_assessments has succeeded twice since. The script is not in
  cron. Revive or delete, don't half-keep.

## ACRIS is frozen upstream at 2026-07-31

Verified against the city's API — newest `recorded_datetime` genuinely is 07-31.
Real threshold is 21 days, so a **genuine** CRITICAL fires ~2026-08-21. That means
"NYC stopped publishing", not "we broke something".

## Search Console — first real read (315 queries, ~5 clicks). READ THIS BEFORE SEO WORK.

Michael pasted the query export on 2026-08-18. Headline: **impressions are real and
CTR is ~0.** This is a coverage-and-CTR problem, not a demand problem. Four findings,
in priority order. All numbers below are impressions unless noted.

### 1. "eviction marshal {neighborhood}" is the biggest cluster and nothing targets it

~35 distinct neighborhood variants, **~200 impressions, 0 clicks**:
`nyc marshal eviction list` **52**, `eviction marshal wakefield` 15,
`... mott haven` 11, `nyc eviction williamsbridge` 10, `marshal eviction list` 8,
`... bushwick` 8, `... washington heights` 7, `... midtown` 7, `... ozone park` 7,
`... upper east side` 6, `... east village` 6, then a long tail of st. george,
highbridge, east new york, queens, lower east side, bronx, upper west side,
brownsville, east harlem, borough park, bay ridge, flatbush, co-op city, crown
heights, richmond hill, manhattan, brooklyn, bed-stuy, jackson heights, east
flatbush. Plus `nyc marshal docket number search`, `nyc marshal list`.

We **have** this data (marshal evictions, 2024-04-12 onward, per BBL/ZIP) and
`/evictions` is citywide only. **The build: neighborhood-level eviction pages** on
the query's own shape. This is the clearest opportunity in the export. Guard against
the doorway-page trap by making each page carry real counts, dates and marshal
detail, not a ZIP score restated.

### 2. The property sitemap gate excludes the addresses people actually search

Address queries are a large slice: `134 macon st brooklyn ny` **35**,
`1339 lincoln pl brooklyn ny 11213` 23, `882 morris ave bronx ny` 7,
`2258 morris ave bronx ny` 7, `286 audubon avenue` 7, `265 west 34th street` 7,
`161 veronica place` 6, `1905 atlantic ave` 6, `303 troutman street` 5, and ~60 more
at 1–4. Also `3009970039` — **37 impressions, 1 click — that is a raw BBL.**

**Every address I sampled already has a working /property page. None are in the
sitemap.** `generate_sitemap.py` requires a deed **AND** an eviction (~1,792 of
918,338 parcels); these have deeds and no eviction. The rule was written to avoid
912k thin doorway pages, which is right in spirit and too narrow in practice — a
parcel with 6 recorded deeds is not thin. Revisit the threshold (e.g. N records of
any kind) rather than the intersection. Scale to weigh: 82,756 parcels have a deed,
19,448 have an eviction.

### 3. LLC pages convert best, and the gate excludes the best one

`norworth holdings llc` — 5 impressions, **3 clicks. That is 3 of the site's 5 total
clicks**, and `/llc/norworth-holdings-llc` renders 200 but is **NOT sitemapped**: it
has 3 BBLs on 1 block and the gate is `count(DISTINCT bbl) >= 3 AND
count(DISTINCT substring(bbl,1,6)) >= 2`. The `blocks >= 2` half is excluding the
highest-CTR page on the site. Also `mf blue valley apartments llc` 12/1 click,
`terra developers` 12/1 click.

Sitemapped LLC pages: **122**. LLC grantees with 2+ deeds: **1,557**. Total distinct
LLC grantees: 17,161. Loosening the gate is the highest-confidence lever here.

Caveat, do not over-promise: some demand is unanswerable. `15 west 26th street llc
deed acris` (32) and `wooster street llc ...` (16+9+1) are **not in ownership_raw at
all** — our slice is 198,446 rows, not a full ACRIS mirror.

### 4. Pages that already rank and get no clicks — a title/snippet problem

- **Rent-stabilized**: ~24 distinct phrasings, ~30 impressions, **0 clicks**, and
  `/is-my-building-rent-stabilized` exists. `is my building rent stabilized` 6,
  `how to check if apartment is rent stabilized nyc` 2, plus twenty near-identical
  long-tail phrasings. Ranking or snippet, not coverage.
- **"who owns"**: ~20 impressions across `who owns this` 4, `find building owner` 2,
  `who owns my building` 2, `nyc landlord search`, `who owns what in nyc`, etc. Page
  exists. 0 clicks.
- **Landlords**: `biggest landlords in nyc` 3, `largest landlords in nyc` 1.
  /operators is already titled for this. 0 clicks.
- **ZIP lookups** are a real intent we half-serve: the `10032` family alone is ~16
  impressions across ten phrasings; also 11219, 11413, 11355, 11427, 11104, 10044.
- **Spanish is surfacing**: `queens codigo postal` 3, `codigo postal far rockaway` 2,
  `codigo postal staten island` 1+1, `codigo de queens` 1. The ES pages earn
  impressions; worth serving ZIP-lookup intent in Spanish deliberately.

### Also worth noting

- `/flips` and `/radar` are titled **"Flip Watch"** and **"Speculation Radar"** —
  brand names nobody searches. Compare `/who-owns-my-building`, which is literally a
  query and was built from Search Console demand. Retitling those two costs nothing.
- `nyc displacement risk map` 6 and `vulnerability assessment nyc` 6 are the only
  product-category queries in the whole export.
- Ignore the `1_1751...` / `2_1729...` numeric queries; they are tracking IDs, not
  human intent.

### Suggested order when Michael returns

P1 neighborhood eviction pages (new demand, we have the data).
P2 sitemap gates: property (intersection -> substantive-record threshold) and LLC
(drop `blocks >= 2`). Cheapest wins, pages already exist and render.
P3 CTR pass on rent-stabilized / who-owns / operators — they rank and nobody clicks.
P4 retitle /flips and /radar to intent-bearing titles.

## Bing Webmaster read (2026-08-18) — small sample, but it supplies the missing variable

34 impressions and 1 click across three months (May 18 – Aug 16), against Google's
hundreds in 28 days. **Do not optimise for Bing.** Its value here is two things
Google's export did not give us: an independent sample, and **average position**.

### Position is the missing diagnosis: we rank 5–10, not 1–3

Every meaningful query sits on the bottom of page 1 or on page 2:

    nyc displacement map                      6 imp   pos 8.50
    53 west 174th st ... eviction cases       8 imp   pos 6.88
    nyc displacement risk map                 2 imp   pos 9.00
    marshal evictions nyc                     1 imp   pos 10.0
    ny marshall eviction list by zipcode      1 imp   pos 5.00
    displacement risk by neighbohood nyc      1 imp   pos 8.00
    bronx gs properties llc                   1 imp   pos 2.00
    michael espin                             1 imp   pos 6.00  -> 1 click (100% CTR)

This revises what I told Michael from the Google export. I said the zero-click
clusters were "ranking or snippet"; the evidence now leans **ranking**. The sharpest
case: `/map` is titled exactly **"NYC Displacement Risk Map"**, which is verbatim the
query, and it still averages position 8.5–9. On-page targeting is already correct, so
rewriting titles will not move that term. That is a domain-authority problem, and it
is the same "distribution, not features" conclusion the earlier checkpoint reached,
now visible in the data.

The exception is LLC-name queries: `bronx gs properties llc` ranks **position 2**.
Entity-name pages rank well because nobody else competes for them. That is consistent
with LLC pages being the best converter on Google (3 of 5 clicks) and is the strongest
argument for loosening the LLC sitemap gate.

### Independent confirmation of all four Google clusters

Address (`971 dean street nyc`), address+eviction, LLC (`bredif ms seller llc`,
`bredif wb high point llc`, `water view castle llc`), and **raw BBL**
(`3068410001`, `6469640028`) all reappear in a separate engine's sample. Two
independent samples agreeing means these patterns are real, not a Google artifact.
Note `/property/3009970039` — Google's 37-impression BBL query — renders 200 and is
**still not sitemapped**, same gate as finding #2.

### New query shapes worth building for

- **`ny marshall eviction list by zipcode`** — the eviction cluster asked for
  explicitly *by ZIP*. That is the pivot P1 should use.
- **`53 west 174th st bronx ny 10453 eviction cases`** (top Bing query, 8) — address
  **+** eviction in one query. Supports P1 and P2 simultaneously: per-address eviction
  history is a page shape we already have data for.
- **`how much did water view castle llc purchase 1341 ocan parkway brooklyn ny for`**
  — natural-language transaction question. **Already answerable**: property pages
  carry an "Ownership transfers" table with an Amount column rendering `$26.7M` style
  figures (82,460 of 137,570 deeds have a `doc_amount`). I initially recorded this as
  a content gap and was wrong — the first two BBLs I sampled simply had no transfers
  in window. It is a coverage/ranking problem, not a missing-content one.
- **`laggy accris`** — someone searching for ACRIS being slow. A positioning angle
  rather than a build: the site is, functionally, a faster ACRIS for the questions
  people actually ask.
- **`michael espin`** produced the only Bing click, at 100% CTR. Personal-brand search
  converts; relevant given what the project is for.

### What this does not change

P1–P4 in the Google section still stand, in the same order. Bing sharpens *why*: P2
and P3 (sitemap gates, entity pages) target query shapes where we can realistically
rank, because entity and address names have little competition. The category term
("nyc displacement map") is the one we are targeting correctly and still losing, and
no amount of on-page work will win it.

## Bing URL/crawl data (2026-08-18) — this REVERSES the "expand the sitemap" advice

Two facts from the crawl export change the plan.

### 1. Zero backlinks. Every URL, every folder.

`Backlinks 0` on all 76 rows exported, and `-` at folder level. That is the ceiling,
and it explains position 5–10 on terms we target correctly. **No amount of page work
overcomes it.** The MTEK/PHANTOM press pitch (verified, unsent) is the only lever on
this list that touches authority.

### 2. The site's indexable mass is its thinnest content

Visible word counts, measured 2026-08-18:

    evictions hub    1,897     x1
    neighborhood       643     x177
    who-owns           562     x1
    displacement       476     x1
    rent-stabilized    469     x1
    radar              362     x1
    flips              348     x1
    borough            223     x5
    LLC entity         178     x122      <- 
    property           165     x1,792    <- 
    operators          130     x1        <- thinnest page on the site

**property + LLC are 1,914 of the 2,159 sitemap URLs and the two thinnest types.**
Worse, they are near-duplicates: two different LLC pages share **94 of 115 unique
words (82%)**, so unique content is roughly 20–40 words per page. Bing's own counters
agree — of ~314 URLs known: **133 indexed, 107 warning, 74 excluded**.

**So do not loosen the sitemap gates yet.** The previous section recommended adding
~1,400 LLC pages and thousands of property pages. Doing that now would multiply
near-duplicate 170-word pages on a zero-authority domain that is already excluding 74
URLs. **Deepen the templates first, then expand.** The gates were over-tight for the
wrong reason, but loosening them before the pages carry substance makes it worse.

### What to add — we already hold the data

**Property pages (165 -> target ~600, the /neighborhood depth):** ownership chain as
prose ("bought 2019 for $X, resold 2024 for $Y, N% in M months"), rent-stabilized
status from `rs_buildings`, this building vs its ZIP, eviction history in sentences,
other buildings by the same owner. The tables are already there; the page has almost
no prose.

**LLC pages (178 -> target ~500):** portfolio summary (buildings, ZIPs, first/last
acquisition, total consideration), acquisition timeline, neighbourhoods with links,
and whether any building carried an eviction *before* purchase — the site's own thesis,
currently invisible on the entity page.

### New page type worth creating: entity families

The crawl list shows `phantom-capital-14/16/25/30/33` as five isolated thin pages.
They are one operation. Measured:

    PHANTOM CAPITAL       28 sibling entities,  59 buildings
    BREDIF                 4 sibling entities, 134 buildings
    numbered-sibling families with 3+ entities sitewide:  49

**49 family hubs**, each genuinely substantial (28 entities and 59 buildings is an
investigation, not a doorway page), each giving the 122 orphaned LLC pages real
internal links, and each matching live demand — Bing recorded `bredif ms seller llc`
and `bredif wb high point llc` as separate queries. This is the one "create new pages"
idea in the export that adds substance rather than surface area.

### Revised order

1. Deepen the property and LLC templates (they are the mass, and they are thin).
2. Build the 49 entity-family hubs; link the thin LLC pages into them.
3. Eviction-by-ZIP pages (still the best new demand) — built at ~600 words from the
   start, not at property-page depth.
4. Only then loosen the sitemap gates.
5. `/operators` is 130 words and ranks for "biggest landlords in nyc". Cheapest fix.
6. Backlinks are the actual ceiling. That is the press pitch, not a code task.

Ignore two things in the export: `/status` shows `Document size 0` but serves 13,560
bytes (stale Bing record), and the single "Title too long >70 chars" warning.

## Structured data + titles audit (2026-08-18)

Bing's validator said "1 markup type found" on an LLC page; that is just the first
block it rendered. Actual coverage, measured across every template:

    /                       Dataset, Organization, WebSite, SearchAction, Place, SoftwareApplication...
    /neighborhood/{zip}     Dataset, FAQPage, Place, PropertyValue, Breadcrumb
    /evictions              Dataset, FAQPage, Organization, Breadcrumb
    /who-owns-my-building   FAQPage, Breadcrumb
    /is-my-building-...     FAQPage, Breadcrumb
    /llc/{slug}             Organization, Breadcrumb
    /property/{bbl}         Place, PostalAddress, Breadcrumb
    /operators /flips /radar /brooklyn    ItemList, Breadcrumb
    /llc                    Breadcrumb ONLY          <- gap

Coverage is good. Two real gaps and two non-issues.

**Gap 1: `/llc` is the only directory page without `ItemList`.** /operators, /flips,
/radar and /brooklyn all declare one; the LLC directory lists 122 entities and
declares nothing. Straight inconsistency, cheap fix.

**Gap 2 — and this is the one that matters: no `FAQPage` on the two mass templates.**
`/property` (x1,792) and `/llc` (x122) are the thinnest pages on the site AND the ones
whose queries are phrased as questions. From the exports: "who owns this building",
"how much did water view castle llc purchase 1341 ocan parkway brooklyn ny for",
"find the owner of a building", "53 west 174th st ... eviction cases". The site
already uses FAQPage well on four other templates, so the pattern exists.

Adding a FAQ block to the property and LLC templates, answered from that building's or
entity's own records, does three jobs at once: it supplies the prose those pages lack
(165 and 178 words today), it makes them eligible for FAQ rich results, and it matches
the observed query phrasing verbatim. **This is the same work as "deepen the
templates" in the section above, not a separate task** — do it as one change.

**Non-issue 1: titles over 70 chars.** Three templates exceed it, and because two are
the mass types it is really ~1,914 pages, not the "1 instance" Bing reported. But all
three lead with the query-matching text — `65 Broadway, Manhattan NY 10006: ...`,
`BRONX GS PROPERTIES LLC: ...`, `Is my building rent stabilized? ...` — so what
truncates is the tail and the ` | PulseCities` suffix. Google truncates on pixel width
anyway. Low priority; do not spend the deepening effort here.

**Non-issue 2: og:title mismatches** on /map (`PulseCities | NYC Displacement Risk Map`
vs the title's reverse order) and /neighborhood (a pipe separator). Cosmetic. og:title
drives social cards, not ranking, and brand-first is defensible there.

## >>> SEO work: read docs/seo/PLAN.md, not the four sections above <<<

The Google / Bing / crawl / schema analysis appended above was written in four passes
as the data arrived, and the later passes **reverse** the earlier ones. Do not act on
them in sequence. `docs/seo/PLAN.md` is the consolidated, ordered version and
`docs/seo/baseline_2026-08-18.md` is the fixed baseline to measure against
(commit 3040269). The sections above are kept only as the reasoning trail.

Headline: shown for the right queries, ranks 5–10, **zero backlinks on every URL**, and
the two highest-volume templates (`/property` 165 words x1,792, `/llc` 178 words x122)
are the thinnest content on the site. **Depth before expansion.**

## 2026-08-18 (later) — SEO plan step 1 shipped: both mass templates deepened

`docs/seo/PLAN.md` is updated in place and carries the detail. Headlines only here.

**Shipped, live, suite green (1,174 passed).** `/property` (1,792 sitemapped) and
`/llc` (122) went from 100 and 84-210 visible words to 479-640 and 450-777,
measured across 180 live pages. FAQPage on both, answered from each building's or
entity's own records rather than boilerplate. `ItemList` on `/llc`. Guarded by
`tests/test_content_depth.py`.

**The plan's duplication metric was wrong. Do not use it again.** "Unique-word
overlap <50%" is unachievable and does not measure duplication: `/neighborhood`,
the template the plan calls good, scores 92-97% on it. Replaced with 5-gram
containment over digit-bearing tokens. Measured: hand-written hubs 0-1%,
property/LLC mean 49-50% max 63-66%, `/neighborhood` 68-69%.

**Two correctness bugs found by reading the rendered page, not by testing.**
1. The Ownership transfers table rendered one row per ACRIS *party*, so every
   deed showed twice and the seller sat under a column headed "Buyer". All
   82,756 parcels with a deed. Now grouped by document, seller in the sub-line.
2. Entity names went through `str.title()`, printing "Llc", "Bronx Gs". Fixed
   with `_entity_title()` (acronym set plus a vowel-less-token rule); it also
   strips the stray trailing punctuation on 27,900 PLUTO owner names.

**Two data findings that change later work.**
1. **ACRIS party addresses ARE populated** (19,511 buyer-side deed rows), which
   contradicts `project_entity_resolution_status`. They cluster: 42 entities at
   one Midtown suite, 35 at another, 27 at 520 Fifth Ave. This is a better spine
   for the step 2 family hubs than numbered name stems, and it unblocks entity
   resolution step 3.
2. **17,114 of 64,849 deed BBLs (26%) are condo unit lots absent from PLUTO**, so
   a quarter of the deed record joins to no address, ZIP or neighbourhood. LLC
   pages now recover the ZIP from the tax block where that block sits in one ZIP
   (92% of blocks) and say plainly when a lot has no building file. **The
   addresses are still missing. That is an ingestion gap worth its own pass.**

**Perf note.** The ZIP peer-comparison query is per-ZIP data that was being run
per-building (402ms on 11207, and there are ZIPs half again that size). Memoised
in `_zip_context()`: 434ms for the first building in a ZIP, 66ms for every one
after. ~180 ZIPs, so it is bounded.

**Gotcha for the next session.** `_long_date(d, lang)` already existed; defining a
one-arg `_long_date` shadowed it and 500'd every neighbourhood page. Only the test
suite caught it. `frontend.py` is ~6,000 lines in one namespace, so grep before
naming a helper.

## 2026-08-18 (session 2) — SEO plan steps 1-6 shipped

`docs/seo/PLAN.md` carries the table. Only the outreach item is left, and no
code change touches it.

**The finding that mattered most was not in the plan.** `_build_property_page`
decided robots with `bool(owners or evicts or permits) or score is not None`.
The score is ZIP-level, so every parcel in a scored ZIP rendered `index,
follow`: **596,432 parcels with no building record at all**, ~429 words each,
81% identical by 5-gram containment. The plan looked at the sitemap (1,792
URLs) and concluded the gate was tight. The sitemap was never the gate.

**Shipped, live, suite green (1,216 passed).**
- /property and /llc deepened, FAQPage on both, ItemList on /llc.
- Robots gate now requires a building-level record.
- Sitemap 2,159 -> 65,810 URLs, split into a sitemap index (spec caps a file at
  50k), per-URL lastmod from each page's newest record (751 distinct values in
  the first property file, against 2,111 URLs sharing one date before).
- /evictions/{neighbourhood}: 127 pages, 515-716 words, keyed on NAME not ZIP
  because three ZIPs are called Bushwick.
- /network/{slug}: 8 entity families. FLGSP = 80 companies / 80 buildings.
- /operators 72 -> 482 words; /flips and /radar retitled off their brand names.
- `complaints_raw(bbl, created_date DESC)` + `violations_raw(bbl,
  inspection_date DESC)`: the 85.8s property page is now 48ms.

**Correctness bugs found by reading rendered output, not by testing.**
1. Ownership transfers rendered one row per ACRIS *party*: every deed twice,
   seller under a column headed "Buyer", on 82,756 parcels.
2. `str.title()` printed "Llc" and "Bronx Gs" on every entity name.
3. `_plural` produced "addresss", "entitys", "buildingss".
4. "BANK" as a family token merged Flagstar with US Bank; summing per-entity
   building counts read five condo units as five buildings.
5. An LLC-page sentence claimed a shared filing address is "how the same
   operation appears as many names". At 525 6th Avenue that is an attorney's
   office. Corrected to state the fact and name both readings.

**Method note worth keeping.** Unique-word overlap is not a duplication
measure: /neighborhood, the template the plan calls good, scores 92-97% on it.
5-gram containment over digit-bearing tokens separates correctly (hand-written
hubs 0-1%, /neighborhood 68-69%) and is what every content test here uses.
Calibrate a metric against a page you already believe is fine before optimising
toward it.

**Crawl reality, measured from nginx logs.** Googlebot fetched 988 distinct
/property and 999 distinct /llc URLs in 14 days, so crawl budget is not the
bottleneck and this work will be seen in days to weeks. But ~877 of the 999 LLC
URLs it crawls were noindex, which the widened gate now fixes.

**Still open:** `retire_raw_data.sh` has never been run (9.3GB of a 16GB DB),
17,114 condo-unit deed BBLs have no address in PLUTO, and the MTEK/PHANTOM
press pitch is unsent. Backlinks remain the ranking ceiling.

## 2026-08-18 (session 2, later) — the ACRIS address block was a typo

`scripts/backfill_party_addresses.py` asked Socrata 636b-3b5g for `addr_1`. The
column is `address_1`. Every fetch returned no address, every update wrote NULL,
and the project recorded "ACRIS has no party address data" and parked entity
resolution on it for months. The scraper read `address_1` correctly the whole
time, which is why rows from May 2026 on have addresses and the April backfill
left 141,354 empty.

A second bug: it matched Socrata rows to DB rows by document_id alone, so a
buyer's address could land on the seller's row. Now matched on
(document_id, party_type, name).

**The backfill COMPLETED.** 141,154 rows, then a second pass for 750 rows that
five Socrata 503s had skipped. It is idempotent (only touches
`party_addr_1 IS NULL`), so re-running is always safe.

    buyer deed rows with an address   19,511 -> 68,223   (28% -> 99%)
    LLC buyers with NO address        11,160 -> 0        (71% -> 0%)

**The clustering was re-run and the site redeployed on the new data:**

    entity families   10 -> 19        entities covered   179 -> 256

New families that did not exist before, all of them invisible while the
addresses were missing: BRICKS (22 entities, 8 held + 17 sold), TSADIK (16),
REDROCK (14), 1 PARK ROW COMMERCIAL (10), BSP (9), SNF (9), SCHUMAN, BAHM,
MBSD, DDG. PHANTOM also grew from 27 entities / 57 buildings to 35 / 70.
Sitemap carries 18 `/network/` URLs; suite green at 1,227.


## >>> START HERE after /clear (2026-08-18, session 2 end) <<<

Everything below is done, deployed, committed and green (1,227 passed). Nothing
is half-finished and no process is running.

**Read first:** `docs/seo/PLAN.md` (all 6 steps DONE),
`docs/outreach/pitch-flgsp-portfolio.md` (unsent),
`docs/seo/crawler_readiness.md`, `docs/seo/resubmit_sitemap.md`.

**Michael resubmitted the sitemap to Search Console and Bing on 2026-08-18, and
it worked.** Within the hour, from real remote crawler IPs: Googlebot fetched
the sitemap 4x, bingbot 10x, and both began crawling the page types that did
not exist that morning (`/evictions/wakefield`, `/network/flgsp`,
`/network/bsp-smk`). 365 crawler responses, 364 of them 200, **zero rate-limit
rejections**. Every 429 in that day's log came from 127.0.0.1 and was this
session's own load testing.

**The single highest-value action remains unsent and needs no code:** the FLGSP
pitch. 82 buildings, 4,941 units of which 4,793 are DHCR-registered
rent-stabilized, traded in one day for ~$435.9M, 10,350 open violations. Live at
`/network/flgsp`. Three caveats are in the doc and must survive into any
version: 93 of the 94 evictions PREDATE the sale, the deeds do not say who is
behind FLGSP, and check whether the trade was already covered when it closed.

**Next build, in order:**
1. **Read the 9 new families for another FLGSP.** BRICKS sold 17 buildings and
   holds 8; BSP holds nothing and sold 11. A portfolio being unwound is the
   shape that produced the last story. This is the pipeline from data to press,
   and press is the only lever on the backlink ceiling.
2. **`retire_raw_data.sh`** — 9.3GB of a 16GB database, never run. Two phases;
   needs a code commit removing `raw_data` from the models and scrapers FIRST,
   then gunicorn reload, then `drop`. Wants a maintenance window.
3. **IndexNow** — the only Bing-specific lever, and the single WARN left in
   `scripts/crawl_audit.py`.
4. **17,114 condo-unit deed BBLs are absent from PLUTO**, so a quarter of the
   deed record resolves to no address. LLC pages recover the ZIP from the tax
   block where that block is unambiguous; the addresses are still missing.
5. **A third clustering signal**: the FLGSP *sellers* (1023/1038/1042 REALTY
   LLC) still do not form a family, because they share only "REALTY" and a
   street number and REALTY is a stop word. Linking them needs
   "counterparties in one bulk transfer", which is a different claim from
   sharing an identity. Deliberately not done.

**Method notes worth keeping.** Unique-word overlap is not a duplication
measure (`/neighborhood` scores 92-97% on it); use 5-gram containment over
digit-bearing tokens, calibrated against a page you already trust. And every
content bug this session was found by reading rendered output, not by reasoning
about code: doubled deed rows, "Llc", "addresss", a false claim about shared
filing addresses, and 127 eviction pages shipped without their borough.
