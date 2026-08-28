# SEO + PII session, paused 2026-08-28 ~21:50 UTC (usage limit)

Resume here. Three commits already landed this session and are safe:
`36a84c4` (score_history upsert), `22feb50` (eviction label + title budget),
`efc16be` (checkpoint). Working tree clean apart from this file.

## UPDATE: the page-quality audit came back. The PII audit did not.

Its headline finding is verified structurally by me, and it is the most
significant thing in this session. **Do not act on it without Michael**: the fix
is one predicate but it drops the indexable corpus by 57%, and the crawl data
says the exposure is latent rather than active, so there is no emergency.

### The sitemap is healthy. The robots tag is not.

Every sitemapped tier measured fine, and **the project's earlier decision not to
cut deed-only is verified**: /neighborhood, the template treated as known-good,
measures *worse* on every duplication statistic (67.6% mean vs 66.7%, 41 vs 8
pairs over 70%, 47.4% vs 66.3% unique content).

    tier              pages    words  overlap mean  pairs >=70%  unique
    deed+eviction     1,098      832       61.9%       4/190      71.2%
    deed-only        46,193      689       66.7%       8/190      66.3%
    eviction-only    18,357      794       57.5%       2/190      69.2%
    violation-only   27,825      843       55.7%       1/190      71.0%
    /neighborhood       177      550       67.6%      41/190      47.4%

The 2026-08-28 violation tier is the *most* distinctive thing in the sitemap, so
admitting it was right.

### The finding: 131,496 pages are indexable but unsitemapped

    routable (HTTP 200)        922,737
    rendering "index, follow"  229,286
    in sitemap                  97,790
    the gap                    131,496

`api/routes/frontend.py:2604` sets robots from `has_signals`, defined at :1961 as
a broad OR over any document, any eviction, any violation, any rent-stab
registration, or owners/evicts/permits. **The sitemap gate is stricter**: a deed,
an eviction, or 5+ violations. The two predicates have never agreed.

Largest shadow block: **90,001 pages whose only record is 1 to 4 violations**,
all rendering `index, follow`, measuring 66.3% mean overlap with 30 of 190 pairs
over the 70% line and a max of 80.7%. That is worse than every sitemapped tier
and worse than the deed-only tier the project debated cutting. Also in the gap:
20,494 permit-only, 10,995 non-deed-ACRIS-only, 3,531 rent-stab-registration-only
(which carry **zero data rows**).

**This is a partial fix, not an oversight, and the comment at :1948 says so.**
The predicate used to also pass on `score is not None`, which is ZIP-level, so
596,432 parcels were indexable; that was found and narrowed. The narrowing was
directionally right and stopped short of the sitemap's own standard. Same shape
as the eviction label and the score_history policy: one rule, two enforcers,
fixed on one surface.

**Why it is not urgent:** Googlebot crawled 63,487 distinct /property BBLs over
~14 days of logs and only **348 (0.5%)** were outside the sitemap. Property pages
emit no outbound /property links. The shadow corpus is real but essentially
uncrawled, so the window to fix it quietly is open.

**Proposed fix, for Michael to accept or reject:** make `has_signals` require the
same record the sitemap demands (a deed, an eviction, or 5+ violations). Drops
indexable from 229,286 to ~97,790.

### Two cheap items found alongside, both safe to just do

1. `tests/test_content_depth.py` `_TIERS` (line 114) omits eviction-only
   (`d IS NULL AND e IS NOT NULL`), leaving 18,357 sitemapped pages, 18.8% of the
   corpus, with no depth guard. Verified: the dict has three keys.
2. `scripts/generate_sitemap.py`'s docstring records eviction-only at "68% mean",
   the worst tier. Two independent draws measure **57.5% and 58.4%**, the
   second-*best*. That stale number is the one that would justify cutting 18,357
   pages, so it is wrong in the dangerous direction.

**Not measured, still needs Michael:** actual Google index coverage. Sitemapped
is not indexed, and that number decides whether the shadow corpus matters at all.

## The PII audit was still in flight and its result was NOT received

Re-run it if it never reported. It was READ ONLY.

**PulseCities PII / legal exposure audit.** Brief: does the site publish
   personal data about identifiable private individuals? Six areas: natural
   persons vs entities in rendered party names (there is a comma-name heuristic
   in the repo); tenant identifiability from address + eviction date; 311
   free-text reaching a page; which pages are actually indexable vs merely
   reachable (`robots` meta in api/routes/frontend.py); stated policy in
   methodology.html/about.html vs actual behaviour; and defamation framing,
   specifically the project's own rule "never imply the buyer evicted".
## What I measured before pausing, all of it real

### PulseCities traffic shape (current)

- Sitemap: **98,847 URLs**. 90,000+ /property across three property sitemaps;
  core has 652 /llc, 177 /neighborhood, 133 /evictions, 44 /week, 26 /network.
- nginx, 2026-08-28 to 21:42: **211 unique organic search-referred IPs**, and
  **/property is 12,491 of ~12,900 organic-referred requests, about 97%**. It was
  88% on 08-23, so the concentration is increasing.
- **The ~5,000 impressions/day figure in the other session's analysis is
  plausible and I did NOT refute it.** GSC on 08-21 read 1,791 impressions and
  45 clicks in a day. The documented log-to-GSC ratio is ~1.8x unique search IPs
  per GSC click, so 211 IPs implies roughly 117 clicks/day, and at the site's
  steady 2.5% CTR that is ~4,700 impressions/day. Growth from 08-21 to 08-28
  accounts for the gap. **Do not "correct" that number, it is about right.**
- Per-page rate is therefore ~0.05 impressions/page/day, close to their 0.064.

### Where the other session IS wrong, and it matters

It models PulseCities as 78k **thin** pages, "spray extremely wide, catch a
trickle each... driven almost entirely by page count, not page quality." That is
not what this site is. Measured live over 24 random pages across the three big
templates: **/property renders 1,224 to 1,749 words**, /llc 1,394 to 1,777,
/neighborhood 1,269 to 1,406, all 200, canonicals correct, median warm render
0.08s. Property pages carry per-building deeds, taxes, owner, violations, 311
history and score.

So the transferable lesson is **not** "make 2,500 pages". It is:

> **Every page must carry data that exists only on that page.**

/property passes that test on 97,790 pages. That is why it works, and it is
much harder to copy than page count.

### ViolationScout, measured live from the public site only (repo untouched)

54 URLs: 31 /nyc, 13 /codes, 9 root, 1 /docket. All 200.

    words: mean 258, median 204, min 88, max 1122
    50 of 54 pages under 400 words
    34 of 54 pages have zero H2

**The finding that matters, and the other session did not have it.** The 30
borough x category pages are not merely thin, they are token-substituted:

    /nyc/bronx-lead-paint  vs  /nyc/brooklyn-lead-paint
    135 body words each (nav/footer stripped)
    92.6% token similarity
    ONLY 9 differing tokens: the borough name x5, and four numbers
      30,661 vs 23,150 | 1,021 vs 821 | 879 vs 453 | 32 vs 20

That is the textbook doorway-page shape: identical prose differentiated by a
place name. Note the numbers ARE real per-borough data, so the problem is not
fabrication, it is **a template too thin to carry the data it already has**.

Caveat worth keeping: 5-gram overlap between those two pages is only **54.1%**,
below the 70% PulseCities treats as a near-duplicate failure. Short pages score
*better* on n-gram overlap while reading as more duplicative, so **n-gram overlap
is the wrong instrument below ~300 words.** Token similarity caught it.

### The ZIP axis is viable, measured against real data

From violations_raw (2,093,270 rows):

    188 distinct ZIPs
    649,296 Class C rows
    10458: 25,897 Class C across   907 buildings, 19,607 distinct defect texts
    11226: 25,210 Class C across   958 buildings, 20,038 distinct defect texts
    10457: 23,076 Class C across   767 buildings, 17,581 distinct defect texts

So ZIP x category pages have genuinely differentiated substance available, unlike
borough x category which has five values. The axis works; the template is the
blocker.

### The biggest unexploited asset on ViolationScout

`violationscout.com/llms.txt` contains **original research with denominators**
that is not published as a page anywhere:

- Of 173,994 OATH/ECB hearings in three years, 20.5% (35,619) ended in DEFAULT.
  Average penalty on default **$7,267** vs **$1,628** where the respondent
  admitted or stipulated. Roughly 4.5x. Stated as observational, not causal.
- 9.4% (16,402) dismissed outright.
- Median days to certification: Class C 16, Class B 38, Class A 51, but **54% of
  Class C are never certified at all** (n=867,182).
- Of 282 scored buildings whose dominant hazard is lead paint, 50% have owners
  who certified corrections on other classes and zero lead violations.

That is a news hook sitting in a text file only machines read. Both sites have
**zero backlinks**, and this is the cheapest route to the first ones.

## Where I had got to on the recommendation

Ranked, but not yet written up or agreed:

**ViolationScout**
1. Publish the llms.txt research as real pages with charts and method notes. It
   is the only genuinely original thing either site holds, and backlinks are the
   documented blocker on both.
2. Fix the template depth BEFORE scaling the axis. Scaling a 135-word
   token-substituted template to 1,080 ZIP pages manufactures 1,080 doorway
   pages, which is an active penalty rather than weak ranking.
3. Then go ZIP x category, not borough x category. Data proven available above.
4. Do NOT build building-level pages. Same PII reasoning as PulseCities below,
   and it is the fastest way to lose the claim-honesty positioning.

**PulseCities**
1. Pending both audit results.
2. Backlinks are still zero and the FLGSP finding (82 buildings, 4,793
   rent-stabilized units, ~$436M in one day) is still unsent. Same unlock as
   ViolationScout item 1.
3. Already verified fixed, do not redo: /flips, /radar and /operators now carry
   query-shaped titles, and the LLC sitemap gate was loosened (122 -> 652).

## Still owed to Michael from earlier in the session

    sudo systemctl reload pulsecities
    sudo cp deploy/pulsecities.cron /etc/cron.d/pulsecities

The Python half of the eviction-label and title fixes does not serve until that
reload. The three HTML files are already live. A GSC export is needed for any
fresh search read; there is nothing local newer than 2026-08-27.
