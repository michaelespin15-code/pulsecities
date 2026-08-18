# Crawler readiness: what to check, what it cost us, how to run it elsewhere

Written 2026-08-18, after taking the sitemap from 2,159 URLs to 65,944 and then
auditing whether the engines could actually consume it.

Run the check with:

    venv/bin/python scripts/crawl_audit.py                  # this box
    venv/bin/python scripts/crawl_audit.py https://other-site.com

---

## Does 65,944 URLs in a sitemap hurt?

No, and it also will not help as much as the number suggests. Three separate
things get confused here, so they are worth separating.

**Sitemap size is not a ranking input.** A sitemap is a discovery hint. Google
and Bing crawl what they judge worth crawling; listing a URL neither forces a
crawl nor promises an index slot. There is no penalty for a large sitemap, and
no bonus either.

**The spec caps are hard, and they are the only real failure mode.** One
sitemap file may hold at most 50,000 URLs and 50MB uncompressed. Exceed either
and the file is rejected outright, usually silently. That is why this site now
serves a sitemap *index* at `/sitemap.xml` naming three children, with property
URLs chunked at 45,000. An index may not point at another index; one level of
nesting is the limit for both engines.

**What actually matters is the ratio of listed to worth-listing.** A sitemap
full of thin, noindex or redirecting URLs teaches a crawler that the file is
not a reliable signal, and it fetches it less eagerly. So the check that earns
its keep is not "how many URLs" but "does every sampled URL return 200, render
indexable, and canonicalise to itself". All 120 sampled here do.

**The honest expectation.** Googlebot fetched 988 distinct `/property` and 999
distinct `/llc` URLs in 14 days. At that rate, 65,000 URLs is years of crawling.
Most of the sitemap will sit undiscovered for a long time, and that is fine:
the gain is that the pages people already search for are now listed and
indexable, not that all 65,000 get indexed. If Search Console fills up with
"Discovered, currently not indexed", that is the expected shape and not a
penalty.

---

## What the audit found here

Everything passed except one thing, and it was the thing that mattered.

**A rate limit tuned for scrapers was rejecting bingbot.** The SSR routes
carried 5r/s per IP, with a comment asserting it "absorbs any human or crawler".
The access logs disagreed: 18 bingbot rejections and 1 Googlebot rejection in 14
days. Bing crawls in bursts from a narrow IP range and trips a per-IP limit that
Google's spread-out fleet never notices. From inside the application a 429 to a
crawler is invisible; it shows up only if you go and count it.

This is the general lesson: **the interesting failures live between the layers.**
The application rendered perfect HTML. The sitemap was valid. The proxy in front
threw both away for one of the two engines.

Two smaller findings fell out of the same pass:

- `/neighborhood/` and `/operator/` carried no rate limit at all, while the
  cheaper `/property/` did. `/neighborhood` is the most expensive SSR page here
  at 1.15s cold, so it was the one that most needed a bound.
- No IndexNow key. Bing and Yandex accept instant URL submission, and it is
  about the only Bing-specific lever that exists. Still open.

An earlier attempt exempted crawlers by user-agent map. It was removed: it did
not work on this nginx, the exemption is trivially spoofable, and config whose
behaviour cannot be quickly verified is worse than a plainer one. The ceiling
moved instead.

---

## Checks worth running on any site

In the order a crawler meets them.

| # | Check | Why it bites |
|---|---|---|
| 1 | `robots.txt` resolves; declares `Sitemap:` on the canonical https host | Both engines read the sitemap location here first |
| 2 | Sitemap index is well-formed, correct 0.9 namespace, one level deep | Nested indexes are rejected |
| 3 | Each child under 50,000 URLs and 50MB | Over either cap the file is dropped, usually silently |
| 4 | `lastmod` is a W3C datetime, and is per-URL | A blanket date on every URL is worth nothing; ours has 751 distinct values |
| 5 | No duplicate, off-host, http-not-https, or trailing-slash-mismatched URLs | Each one is a contradiction between sitemap and canonical |
| 6 | Sitemaps gzip and support conditional GET | A 7.9MB file re-transferred in full on every crawl wastes budget |
| 7 | **Sample real sitemap URLs: 200, not noindex, self-canonical** | The single highest-value check; a sitemap listing noindex URLs is self-discrediting |
| 8 | Fetch a page under each engine's own user agent | Catches WAF rules, bot filters and rate limits sitting in front of the app |
| 9 | Grep the access log for 429/503 by crawler user agent | The only way to see rejections the app never hears about |
| 10 | Confirm the robots meta gate matches the sitemap gate | Ours disagreed by 596,432 pages |

Check 10 is the one that cost the most here. The sitemap listed 1,792 property
URLs while the robots tag said `index, follow` on 596,432 record-less parcels,
because indexability keyed off a ZIP-level score rather than the page's own
record. A sitemap-only reading of the problem reported it as tight.

---

## Asking Claude Code to run this on another site

Paste this. It does not assume the site is like this one.

> Audit this site's crawler readiness end to end. Do not tell me it looks fine
> without measuring.
>
> 1. Fetch robots.txt and every sitemap it declares. Verify the index is
>    well-formed and correctly namespaced, that no child exceeds 50,000 URLs or
>    50MB, that nesting is at most one level, and that lastmod is a valid W3C
>    datetime and actually varies per URL rather than being one blanket stamp.
> 2. Check for duplicate URLs across sitemaps, off-host URLs, http URLs, and
>    trailing-slash mismatches against the canonical form.
> 3. Sample at least 100 random URLs from the sitemap and confirm each returns
>    200, is not noindex, does not redirect, and has a self-referencing
>    canonical. Pace the requests so you do not trip the site's own rate limit
>    and then report the site as broken.
> 4. Request a representative page under the Googlebot, bingbot and
>    Google-InspectionTool user agents and confirm each gets 200, not a
>    redirect, challenge, 429 or 503.
> 5. If you can reach the web server access logs, count 429 and 503 responses
>    grouped by crawler user agent over the last two weeks. Report any crawler
>    being rejected.
> 6. Find where indexability is actually decided in the code, and compare that
>    rule against the sitemap's inclusion rule. Report any page that is
>    indexable but not sitemapped, or sitemapped but not indexable, and tell me
>    how many pages fall in each gap.
> 7. Check whether the rate limiting, WAF or CDN in front of the app can reject
>    a crawler, and what rate a normal sitemap sweep would hit.
>
> Report findings with counts and the evidence you measured them from. Where
> something is wrong, fix it and add a regression test. Where you are guessing,
> say so.

Two habits that made the difference here, worth repeating in any prompt:

- **Calibrate a metric against a page you already believe is fine before you
  optimise toward it.** Our plan's duplication metric rated `/neighborhood`, the
  template it called good, at 92-97%. The metric was wrong, not the page.
- **Read the rendered output.** Every content bug this session, the doubled deed
  rows, "Llc", "addresss", the false claim about shared filing addresses, was
  found by looking at a page, not by reasoning about the code or running tests.
