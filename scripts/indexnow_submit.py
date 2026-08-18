"""
IndexNow: tell Bing what changed on the night it changes.

Google ignores IndexNow. Bing, Yandex, Seznam and Naver accept it, and bingbot
is already the heavier crawler here: it fetched the sitemap ten times within an
hour of the 2026-08-18 resubmission. What is left to shorten is the lag between
a page changing and bingbot coming back for it, and this is the only lever for
that which does not involve waiting.

Ownership is proved by a key file served from the site root, and a POST to
api.indexnow.org carries the URLs. The protocol is blunt and the failure mode is
being ignored for crying wolf, so this submits only what moved: state lives in
`indexnow_state.json` as {url: lastmod}, taken from the sitemaps themselves
rather than recomputed, so what gets submitted is exactly what a crawler would
see if it re-read the file.

Run it after the nightly sitemap regeneration, which is where the lastmod values
it reads come from.

Usage:
    python -m scripts.indexnow_submit             # whatever moved since last run
    python -m scripts.indexnow_submit --all       # every URL, ignoring state
    python -m scripts.indexnow_submit --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
FRONTEND = ROOT / "frontend"
STATE = Path(__file__).resolve().parent / "indexnow_state.json"

HOST = "pulsecities.com"
BASE = f"https://{HOST}"
KEY = "4494ce2738a74028c1babaef305aec53"
KEY_FILE = FRONTEND / f"{KEY}.txt"
ENDPOINT = "https://api.indexnow.org/indexnow"

_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# The protocol allows 10,000 per request. The cap here is per run, not per
# request: on a site with 65,810 sitemapped URLs the first run would otherwise
# dump the lot, which is exactly the behaviour the endpoint rate-limits. The
# backlog drains over a few nights and the run says how much is left.
MAX_PER_RUN = 5_000
BATCH = 1_000


def sitemap_urls() -> dict[str, str]:
    """{url: lastmod} across every child sitemap, core first.

    Core is first because dict order decides what gets sent when a run hits the
    cap, and a changed hub matters more than one more property page.
    """
    index = FRONTEND / "sitemap.xml"
    if not index.exists():
        sys.exit("no frontend/sitemap.xml; run scripts.generate_sitemap first")

    children = [
        loc.text.rsplit("/", 1)[-1]
        for loc in ET.parse(index).getroot().findall("sm:sitemap/sm:loc", _NS)
        if loc.text
    ]
    children.sort(key=lambda n: (0 if "core" in n else 1, n))

    urls: dict[str, str] = {}
    for name in children:
        path = FRONTEND / name
        if not path.exists():
            continue
        for url in ET.parse(path).getroot().findall("sm:url", _NS):
            loc = url.findtext("sm:loc", namespaces=_NS)
            mod = url.findtext("sm:lastmod", default="", namespaces=_NS)
            if loc:
                urls[loc] = mod
    return urls


def _load_state() -> dict[str, str]:
    if not STATE.exists():
        return {}
    try:
        return json.loads(STATE.read_text())
    except json.JSONDecodeError:
        # A truncated state file must not stop the submission; the worst case
        # of treating it as empty is one oversized run.
        return {}


def changed(urls: dict[str, str], state: dict[str, str]) -> list[str]:
    return [u for u, mod in urls.items() if state.get(u) != mod]


def submit(batch: list[str], dry_run: bool) -> bool:
    payload = {
        "host": HOST,
        "key": KEY,
        "keyLocation": f"{BASE}/{KEY}.txt",
        "urlList": batch,
    }
    if dry_run:
        print(f"  would submit {len(batch)} URLs, first is {batch[0]}")
        return True
    r = requests.post(ENDPOINT, json=payload, timeout=30,
                      headers={"Content-Type": "application/json; charset=utf-8"})
    # 200 accepted, 202 accepted with the key still being verified. Everything
    # else is worth printing in full: 403 means the key file is not reachable,
    # 422 means a URL does not belong to this host, 429 means back off.
    if r.status_code in (200, 202):
        print(f"  {len(batch)} URLs, HTTP {r.status_code}")
        return True
    print(f"  REJECTED {len(batch)} URLs, HTTP {r.status_code}: {r.text[:200]}")
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="submit every URL")
    ap.add_argument("--limit", type=int, default=MAX_PER_RUN,
                    help="cap this run below the usual %(default)s, for a first "
                         "submission that proves the key before sending volume")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not KEY_FILE.exists() or KEY_FILE.read_text().strip() != KEY:
        sys.exit(f"key file missing or wrong: {KEY_FILE}")

    urls = sitemap_urls()
    state = {} if args.all else _load_state()
    queue = changed(urls, state)
    print(f"{len(urls):,} URLs in the sitemaps, {len(queue):,} new or changed")
    if not queue:
        return 0

    cap = max(1, min(args.limit, MAX_PER_RUN))
    if len(queue) > cap:
        print(f"capping this run at {cap:,}; "
              f"{len(queue) - cap:,} carry over to the next run")
        queue = queue[:cap]

    sent: list[str] = []
    for i in range(0, len(queue), BATCH):
        chunk = queue[i:i + BATCH]
        if not submit(chunk, args.dry_run):
            break
        sent.extend(chunk)

    if sent and not args.dry_run:
        # Only what was accepted goes into state, so a failed batch is retried
        # tomorrow rather than being marked done.
        state.update({u: urls[u] for u in sent})
        STATE.write_text(json.dumps(state, indent=0, sort_keys=True))
    print(f"submitted {len(sent):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
