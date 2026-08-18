# Resubmitting the sitemap, 2026-08-18

Only you can do this; it needs console logins. Ten minutes, and it is what makes
this week's work visible to the engines.

## What changed and why it needs a resubmit

`https://pulsecities.com/sitemap.xml` is the same URL, but it is no longer a
list of URLs. It is now a **sitemap index** naming three children:

    sitemap.xml                 index
      sitemap-core.xml          535 URLs   hubs, 177 neighborhoods, 156 LLCs,
                                           127 eviction pages, 7 family hubs
      sitemap-property-1.xml    45,000 URLs
      sitemap-property-2.xml    20,409 URLs

Both engines follow an index from the same URL without being told, so this is
not strictly required. Resubmitting forces a re-read now instead of waiting for
the next scheduled fetch, which on a zero-authority domain can be weeks.

## Google Search Console

1. Open Search Console, pick the `pulsecities.com` property.
2. Left sidebar, **Indexing → Sitemaps**.
3. Under "Add a new sitemap", enter `sitemap.xml` and Submit. Submitting an
   already-listed sitemap is what forces the refetch; you do not need to remove
   it first.
4. Status should reach **Success** within a few minutes to a day, and
   "Discovered URLs" should climb toward 65,944. If it says *Couldn't fetch*,
   wait an hour and resubmit before assuming anything is wrong.
5. Optional but useful: **URL Inspection** on
   `https://pulsecities.com/evictions/wakefield` and
   `https://pulsecities.com/network/flgsp`, then **Request Indexing** on each.
   These are brand new page types with no history, and this is the fastest way
   to get the first one of each looked at.

**What you will see, and what is not a problem.** "Discovered - currently not
indexed" will grow into the tens of thousands. That is the expected shape when a
sitemap is far larger than the crawl rate, not a penalty. Googlebot fetched
about 1,000 distinct property URLs in the last 14 days; 65,944 is years of
crawling at that pace. The gain is that the pages people already search for are
now listed and indexable.

## Bing Webmaster Tools

1. Open Bing Webmaster Tools, pick the site.
2. **Sitemaps** in the left nav, then **Submit sitemap**.
3. Enter the full URL `https://pulsecities.com/sitemap.xml`. Bing wants the
   absolute URL where Google takes a relative path.
4. While you are there, check **Site Explorer → Crawl information** for 429s.
   The nginx rate limit was rejecting bingbot 18 times in 14 days and was raised
   on 2026-08-18; the count should go to zero from here.

Bing matters out of proportion to its traffic here. Its 34 impressions were the
only source of *position* data you have, which is what showed the site ranks
5–10 rather than having a snippet problem.

## Verify it yourself first

    venv/bin/python scripts/crawl_audit.py

21 checks, 0 failures as of 2026-08-18. Run it before submitting so you are not
asking an engine to fetch something broken.

## The one thing still not done

**IndexNow.** Bing and Yandex accept instant URL submission: drop a key file at
the site root and POST changed URLs on publish. It is the only Bing-specific
lever that exists, it suits a site whose sitemap changes nightly, and it is
currently the single WARN in the audit.
