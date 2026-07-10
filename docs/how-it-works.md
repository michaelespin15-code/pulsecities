# How PulseCities works

PulseCities turns scattered NYC public records into a single map of where
displacement pressure is building, who is buying, and how the two connect. This
is a technical walkthrough of what runs under the site: the data pipeline, the
displacement score, the entity resolution that reconstructs landlord networks
from shell LLCs, and the correctness work that keeps the numbers trustworthy
when a journalist clicks through.

Live at [pulsecities.com](https://pulsecities.com). Built solo.

## The problem

The records that describe displacement already exist, but they live in separate
systems that never talk to each other. A deed transfer is in ACRIS. The eviction
that preceded it is in the marshals' dataset. The renovation permit that follows
is in DOB. The 311 complaints and HPD violations are in two more places. Each one
is a public dataset; none of them is joined to the others, and none is joined to
the parcel. So the arc that tells the actual story, an eviction followed months
later by an LLC purchase followed by a permit and a flip, is invisible unless
someone stitches the records together by hand, one address at a time.

PulseCities does that stitching continuously, at the parcel level, for the whole
city.

## Pipeline

A cron job runs the full pipeline nightly at 02:00 UTC. Source-specific scrapers
pull from NYC Open Data (Socrata) and ACRIS, in dependency order so that parcel
geometry lands before the records that join against it:

```
PLUTO parcels → 311 complaints → DOB permits → evictions → ACRIS deeds → DHCR → HPD → scoring → snapshot
```

Each scraper normalizes its source into a raw table (`evictions_raw`,
`ownership_raw`, and so on), keyed to a BBL (borough-block-lot) where the source
provides one. Ingestion is idempotent: every batch is an upsert that does nothing
on conflict, so a scraper can re-run its lookback window every night without
duplicating rows. Bad or unparseable records are quarantined rather than dropped,
and each run records its own success or failure so the pipeline can report which
source broke without stopping the others. The site serves the last good data even
when a single scraper fails upstream, and it emails me when one does.

Stack: Python scrapers, PostgreSQL with PostGIS, FastAPI, a MapLibre and Tailwind
frontend served as static files. The working tree is production; deploys are a
git pull.

## The displacement score

Each ZIP code gets a composite 0 to 100 score, recomputed nightly from six
signals drawn from the raw tables:

- LLC acquisition rate (deed transfers to LLC grantees)
- eviction rate (marshal-executed residential evictions)
- permit intensity (alteration permits on multi-unit residential parcels)
- HPD violation rate (class B and C, the serious ones)
- 311 complaint trend
- rent-stabilized unit loss (from DHCR registrations)

The score is deliberately a **pressure indicator, not a prediction**. It counts
how many displacement signals are lit in a ZIP right now; it does not claim to
know what happens to any specific building or tenant. That distinction is stated
on the methodology page and in the product copy, because the honest version is
the only version worth publishing.

Two of the six signals need a second year of annual data before they mean
anything (assessment spikes and rent-stabilized loss). While they are dormant,
their weight is redistributed proportionally across the active signals rather
than counted as zero, so a ZIP is never penalized for a signal the data cannot
yet support. Every nightly score is also snapshotted into a history table, which
is what powers the week-over-week movement on the site and the "top movers" list.

## Entity resolution: reconstructing operators from shell LLCs

This is the part that makes PulseCities more than a dashboard. A single landlord
routinely buys through a dozen differently-named LLCs, one per building or block,
precisely so that no public search shows the whole portfolio. `MTEK NYC LLC`,
`MTEK GOLD LLC`, and `MTEK FRANKLIN LLC` are the same operator; ACRIS shows them
as unrelated buyers.

The resolver extracts a brand token from each normalized grantee name (the
identifying word that survives across an operator's LLCs), then clusters every
deed grantee under that token. The tricky cases are what the logic is actually
about:

- **Compound brands.** Some operators spell a brand both fused and spaced across
  filings (`ICECAP` and `ICE CAP`). A naive fuse of short adjacent tokens
  resolves those together, but it also wrongly fuses `MTEK GOLD` into a spurious
  `MTEKGOLD` root, splitting an operator's own entities apart. The fix is a
  two-pass approach: collect the standalone brand tokens across the whole corpus
  first, then only fuse a compound when the fused spelling actually exists as a
  brand somewhere. `ICE CAP` fuses because `ICECAP` is real; `MTEK GOLD` stays
  under `MTEK` because `MTEKGOLD` never appears.
- **Surname clusters.** A common last name generates hundreds of unrelated
  single-LLC investors who happen to share a token. Those get filtered by
  acquisitions-per-LLC: a coordinated operator runs several properties per shell,
  a coincidental name cluster runs one each.
- **Internal transfers.** Some portfolios move the same building between their own
  entities (an REIT shuffling notes through an issuer). Counting each hop as an
  acquisition inflated one operator's total to 132 before I excluded transfers
  whose grantor and grantee resolve to the same operator.

The output is an operator profile: the full portfolio, the shell entities behind
it, the ZIP spread, and the eviction-then-buy matches, all reconstructed from
public deeds.

## The classification gate

Clustering by brand token also sweeps up things that are not operators: banks
that appear in ACRIS through foreclosures, mortgage servicers, GSEs, government
bodies, nonprofit HDFCs. Showing "Wells Fargo, 400 properties" as a displacement
operator would be both wrong and a credibility hole.

Every cluster passes through a classification gate before it can appear on any
public surface. The gate combines a curated allowlist of confirmed operators, a
blocklist of financial and government name roots, and behavioral signals (for
example, a cluster whose deeds are almost all $0 or nominal amounts is moving
paper, not buying buildings). Only clusters classified as `operator` get a public
profile, a network graph, a CSV export, or an OG card. Everything else is
invisible on those surfaces even on a direct API hit.

The gate is enforced in one place, the `operator_class` column, and every public
endpoint reads from it. A regression test drives the gate off the live database
rather than a fixture, so a newly misclassified cluster is caught the moment it
lands in the table, with no hardcoded list to drift.

## The eviction-flip scan

The clearest displacement story is an arc: a residential eviction, then an LLC
buying that building within a year, then reselling it at a markup. It is one
query over `evictions_raw` joined to `ownership_raw` at the BBL, filtered to
purchases within twelve months of an eviction and resales at 25 percent or more.
It runs weekly and diffs against the prior run, so it surfaces only what is new.
It found a Brooklyn building bought for $712,775 five months after an eviction
and resold for $1.15M, and a Bronx house bought for $500,000 after two evictions
and resold for $999,000 eighty-four days later. Both verify in ACRIS in minutes,
which is the point: every finding carries its document IDs.

## Correctness, because a wrong number ends the story

A tool that a journalist cites has to be right, and "right" here is mostly about
the seams between datasets. Some of the work that matters more than it looks:

- **NULL keys silently defeating de-duplication.** The eviction and permit unique
  keys include the BBL, and Postgres treats NULLs as distinct in a unique
  constraint, so every NULL-BBL row bypassed the upsert and re-inserted nightly.
  It had accumulated 18,000 duplicate eviction rows, some events counted 55
  times, quietly inflating every per-ZIP eviction number including the scores. The
  fix was a COALESCE-based unique index that closes the NULL hole, a one-time
  dedupe, and a recompute of the affected scores and history.
- **Directory drift.** The operator directory was rebuilt on a schedule that
  nobody was running, so its published counts fell behind the deeds. It now
  refreshes monthly, then checks invariants (every operator's parcel rows match
  its headline count, no operator below the promotion floor) before it will
  declare success, and emails a before-and-after diff so a number change on a
  public page is never a surprise.
- **Honest windows.** A stat chip never labels a time window the query does not
  actually cover, and when the upstream ACRIS feed pauses (as it is now, frozen
  in late May), the site says so plainly rather than presenting stale deeds as
  current. Evictions, permits, complaints, and violations stay daily-fresh, so
  the scores and the weekly review stay live.
- **Regression guards driven off the database.** The recurring mistakes,
  duplicate rows, classification leaks, false freshness, get DB-driven tests that
  fail the moment the condition reappears, regardless of how it got there.

None of this is visible on the site. All of it is why the numbers on the site
hold up when someone checks them against the source.

## Limitations

The score is a signal count, not a forecast, and it is only as current as its
slowest feed. Eviction *filing* data (the petition, which leads an executed
eviction by months) is public only de-identified to ZIP code, so the
building-level arcs are built on executed evictions and deeds, not filings.
Operator resolution is name-based and errs toward not showing a cluster rather
than showing a wrong one. These are documented on the site, not hidden.

## What's next

Petition-volume as a ZIP-level early-warning signal, a documented read-only API
for newsroom data teams, and building-level watch alerts. The through-line is the
same one the whole project is built on: connect the public records that describe
displacement, at the parcel level, and be honest about what the connection does
and does not prove.
