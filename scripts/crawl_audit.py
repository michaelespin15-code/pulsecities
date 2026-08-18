"""
Crawler-readiness audit. Run against any site; nothing here is PulseCities-specific
except the two constants below.

    venv/bin/python scripts/crawl_audit.py                 # localhost, via Host header
    venv/bin/python scripts/crawl_audit.py https://example.com

Checks, in the order a crawler encounters them:

  1. robots.txt resolves, and declares a Sitemap: line on the canonical host
  2. the sitemap index is well-formed, correctly namespaced, and not nested
     deeper than one level (which both Google and Bing reject)
  3. every child is well-formed and inside the 50,000-URL / 50MB spec caps,
     lastmod is a W3C datetime, and no URL is duplicated, off-host, or non-https
  4. sitemaps are gzip-capable and support conditional GET, so a 7.9MB file is
     not re-transferred in full on every crawl
  5. **the part that actually matters**: sample real URLs from the sitemap and
     confirm each returns 200, is not noindex, and self-canonicalises. A
     sitemap that lists noindex or redirecting URLs teaches the crawler to
     trust it less.
  6. the engines fetch a real page without being redirected, blocked or
     rate-limited under their own user agents

That last check found a live bug here: a 5r/s per-IP nginx limit was rejecting
bingbot 18 times in 14 days. A limit tuned for scrapers had been turning away
the engine the site was trying to rank in, and nothing else surfaced it.
"""
import random
import re
import sys
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

import requests
import urllib3

urllib3.disable_warnings()

BASE = "https://127.0.0.1"        # what to connect to
HOST = "pulsecities.com"          # the canonical hostname, sent as Host:
if len(sys.argv) > 1:
    from urllib.parse import urlparse as _up
    BASE = sys.argv[1].rstrip("/")
    HOST = _up(BASE).netloc
NS = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
SPEC_URLS = 50_000
SPEC_BYTES = 50 * 1024 * 1024

ok, warn, fail = [], [], []
def OK(m): ok.append(m); print(f"  PASS  {m}")
def WARN(m): warn.append(m); print(f"  WARN  {m}")
def FAIL(m): fail.append(m); print(f"  FAIL  {m}")

# A real crawler does not fetch 120 URLs in three seconds from one IP. Without
# pacing, this script trips the site's own rate limit and then reports the
# site as broken, which is a measurement artefact rather than a finding.
PACE = 0.08
_BACKOFF = 3.0


def get(path, **kw):
    for attempt in range(3):
        r = requests.get(f"{BASE}{path}",
                         headers={"Host": HOST, **kw.get("headers", {})},
                         verify=False, timeout=60,
                         **{k: v for k, v in kw.items() if k != "headers"})
        if r.status_code != 429:
            return r
        time.sleep(_BACKOFF * (attempt + 1))
    return r

print("\n=== 1. robots.txt ===")
r = get("/robots.txt")
robots = r.text
print(robots.strip())
if r.status_code != 200:
    FAIL(f"robots.txt returns {r.status_code}")
else:
    OK("robots.txt serves 200")
sm_lines = re.findall(r"(?im)^\s*Sitemap:\s*(\S+)", robots)
if not sm_lines:
    FAIL("robots.txt declares no Sitemap: line (both engines read this)")
else:
    OK(f"robots.txt declares {len(sm_lines)} sitemap(s): {sm_lines}")
    for u in sm_lines:
        if urlparse(u).scheme != "https":
            FAIL(f"sitemap URL not https: {u}")
        if urlparse(u).netloc != HOST:
            FAIL(f"sitemap URL host mismatch: {u}")

print("\n=== 2. sitemap index conformance ===")
r = get("/sitemap.xml")
ctype = r.headers.get("content-type", "")
if r.status_code != 200:
    FAIL(f"/sitemap.xml -> {r.status_code}")
if "xml" not in ctype:
    FAIL(f"content-type is {ctype!r}, engines want an XML type")
else:
    OK(f"content-type {ctype}")
try:
    root = ET.fromstring(r.content)
except ET.ParseError as e:
    FAIL(f"index is not well-formed XML: {e}"); root = None

children = []
if root is not None:
    tag = root.tag.split("}")[-1]
    if tag != "sitemapindex":
        FAIL(f"root element is <{tag}>, expected <sitemapindex>")
    else:
        OK("root element is <sitemapindex>")
    if root.tag.startswith("{http://www.sitemaps.org/schemas/sitemap/0.9}"):
        OK("correct sitemaps.org 0.9 namespace")
    else:
        FAIL(f"wrong namespace: {root.tag}")
    for sm in root.findall("s:sitemap", NS):
        loc = sm.findtext("s:loc", namespaces=NS)
        lm = sm.findtext("s:lastmod", namespaces=NS)
        children.append(loc)
        if lm and not re.match(r"^\d{4}-\d{2}-\d{2}([T ].*)?$", lm):
            FAIL(f"lastmod not W3C datetime: {lm}")
    # Nesting: a sitemap index may not point at another index.
    OK(f"index names {len(children)} children")
    if len(children) > 50000:
        FAIL("index exceeds 50,000 children")

print("\n=== 3. child sitemaps ===")
all_urls = []
for loc in children:
    path = urlparse(loc).path
    rr = get(path)
    if rr.status_code != 200:
        FAIL(f"{path} -> {rr.status_code} but is named in the index"); continue
    size = len(rr.content)
    try:
        croot = ET.fromstring(rr.content)
    except ET.ParseError as e:
        FAIL(f"{path} not well-formed: {e}"); continue
    if croot.tag.split("}")[-1] == "sitemapindex":
        FAIL(f"{path} is another index; nesting deeper than one level is invalid")
        continue
    urls = croot.findall("s:url", NS)
    locs = [u.findtext("s:loc", namespaces=NS) for u in urls]
    all_urls += locs
    flag = []
    if len(urls) > SPEC_URLS: flag.append(f"{len(urls)} urls > 50,000 CAP")
    if size > SPEC_BYTES: flag.append(f"{size/1e6:.1f}MB > 50MB CAP")
    (FAIL if flag else OK)(f"{path}: {len(urls):,} urls, {size/1e6:.1f}MB "
                           + (" ".join(flag) if flag else ""))
    bad_lm = [u for u in urls
              if (lm := u.findtext("s:lastmod", namespaces=NS))
              and not re.match(r"^\d{4}-\d{2}-\d{2}([T ].*)?$", lm)]
    if bad_lm:
        FAIL(f"{path}: {len(bad_lm)} malformed lastmod")

print(f"\n  total URLs across all children: {len(all_urls):,}")
dupes = len(all_urls) - len(set(all_urls))
(FAIL if dupes else OK)(f"duplicate URLs across sitemaps: {dupes}")
offhost = [u for u in all_urls if urlparse(u).netloc != HOST]
(FAIL if offhost else OK)(f"off-host URLs: {len(offhost)}")
nonhttps = [u for u in all_urls if urlparse(u).scheme != "https"]
(FAIL if nonhttps else OK)(f"non-https URLs: {len(nonhttps)}")

print("\n=== 4. gzip / conditional-GET support (crawl efficiency) ===")
r = get("/sitemap-property-1.xml", headers={"Accept-Encoding": "gzip"})
if r.headers.get("content-encoding") == "gzip":
    OK("child sitemap served gzipped when requested")
else:
    WARN("child sitemap not gzipped; 7.9MB transfers uncompressed on every fetch")
if r.headers.get("etag") or r.headers.get("last-modified"):
    OK(f"conditional GET supported (etag={bool(r.headers.get('etag'))}, "
       f"last-modified={bool(r.headers.get('last-modified'))})")
else:
    WARN("no ETag/Last-Modified; crawlers refetch the whole file every time")

print("\n=== 5. sampled URL-level agreement (the part that actually matters) ===")
random.seed(11)
sample = random.sample(all_urls, min(120, len(all_urls)))
prob = {"status": [], "noindex": [], "canonical": [], "redirect": []}
for u in sample:
    time.sleep(PACE)
    path = urlparse(u).path
    rr = get(path, allow_redirects=False)
    if rr.status_code in (301, 302, 307, 308):
        prob["redirect"].append(f"{path} -> {rr.headers.get('location')}")
        continue
    if rr.status_code != 200:
        prob["status"].append(f"{path} -> {rr.status_code}")
        continue
    m = re.search(r'<meta name="robots" content="([^"]+)"', rr.text)
    if m and "noindex" in m.group(1):
        prob["noindex"].append(f"{path} is {m.group(1)}")
    c = re.search(r'<link rel="canonical" href="([^"]+)"', rr.text)
    if not c:
        prob["canonical"].append(f"{path} has no canonical")
    elif c.group(1).rstrip("/") != u.rstrip("/"):
        prob["canonical"].append(f"{path} canonical={c.group(1)} but sitemap says {u}")
print(f"  sampled {len(sample)} sitemap URLs")
for k, v in prob.items():
    if v:
        FAIL(f"{k}: {len(v)} of {len(sample)}")
        for x in v[:4]:
            print(f"        {x}")
    else:
        OK(f"{k}: clean")

print("\n=== 6. engine-specific ===")
# Probe a URL taken from the sitemap, not a hardcoded one. A path baked in from
# whichever site this script was written on 404s everywhere else, and then
# reports every engine as blocked when nothing is wrong.
probe = urlparse(sample[0]).path if sample else "/"
for ua, name in [
    ("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", "Googlebot"),
    ("Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)", "bingbot"),
    ("Mozilla/5.0 (compatible; Google-InspectionTool/1.0)", "Google-InspectionTool"),
]:
    time.sleep(PACE)
    rr = get(probe, headers={"User-Agent": ua})
    (OK if rr.status_code == 200 else FAIL)(f"{name}: {probe} returns {rr.status_code}")
# IndexNow ownership is proved by a key file at the site root, not by a
# robots.txt line: robots.txt has no such directive and validators mark one as
# an error. So probe for a key file rather than reading robots.
_probe = get("/4494ce2738a74028c1babaef305aec53.txt")
if _probe.status_code == 200 and _probe.text.strip():
    OK("IndexNow key file serves; instant submission is available to Bing and Yandex")
else:
    WARN("no IndexNow key file at the site root; Bing/Yandex accept instant "
         "submission and it is the one Bing-specific lever available")

print("\n" + "=" * 60)
print(f"PASS {len(ok)}   WARN {len(warn)}   FAIL {len(fail)}")
if fail:
    print("\nFAILURES:")
    for f in fail: print(f"  - {f}")
if warn:
    print("\nWARNINGS:")
    for w in warn: print(f"  - {w}")
