"""
Guard: the rate limiter must not turn away the search engines.

Found in production, not in review. The SSR routes carried a 5r/s per-IP nginx
limit whose comment claimed it "absorbs any human or crawler". Fourteen days of
access logs said otherwise: bingbot took 18 rate-limit rejections and Googlebot
one. Bing crawls in bursts from a narrow IP range, so it trips a per-IP limit
that Google's spread-out fleet never notices, and nothing on the site surfaced
it because a 429 to a crawler looks like nothing at all from the inside.

This reads the deployed nginx config rather than making live requests, so it
runs in CI without a server and without becoming the noisy neighbour it is
meant to detect. The live equivalent is scripts/crawl_audit.py.
"""

import re
from pathlib import Path

import pytest

CONF = Path(__file__).parent.parent / "deploy" / "nginx-pulsecities.conf"

# Bing's observed peak against this site was roughly 5r/s in bursts. Anything at
# or under that is a limit that will reject it again.
MIN_SSR_RATE = 15
MIN_SSR_BURST = 40


@pytest.fixture(scope="module")
def conf() -> str:
    assert CONF.exists(), f"missing {CONF}"
    return CONF.read_text()


def _zones(conf: str) -> dict[str, int]:
    """zone name -> requests per second."""
    out = {}
    for m in re.finditer(r"limit_req_zone\s+\S+\s+zone=(\w+):\S+\s+rate=(\d+)r/([sm]);", conf):
        name, n, unit = m.group(1), int(m.group(2)), m.group(3)
        out[name] = n if unit == "s" else n / 60.0
    return out


def test_ssr_zone_exists(conf):
    assert "ssr_heavy" in _zones(conf), "the SSR rate-limit zone is gone entirely"


def test_ssr_rate_clears_observed_crawler_bursts(conf):
    rate = _zones(conf)["ssr_heavy"]
    assert rate >= MIN_SSR_RATE, (
        f"ssr_heavy is {rate}r/s. bingbot was rejected 18 times in 14 days at "
        f"5r/s; anything under {MIN_SSR_RATE}r/s will do it again on the "
        f"templates that make up almost every sitemap URL."
    )


def test_ssr_burst_clears_observed_crawler_bursts(conf):
    bursts = [int(m) for m in re.findall(r"limit_req\s+zone=ssr_heavy\s+burst=(\d+)", conf)]
    assert bursts, "ssr_heavy zone is defined but applied nowhere"
    assert min(bursts) >= MIN_SSR_BURST, (
        f"smallest ssr_heavy burst is {min(bursts)}; a crawler sweeping a "
        f"sitemap arrives in bursts, so the burst is what it actually meets"
    )


def test_the_crawlable_templates_are_all_covered_by_one_policy(conf):
    """Every location that proxies a mass template should carry the same limit,
    or one of them becomes the weak point a crawl stalls on."""
    missing = []
    for route in ("/property/", "/llc/", "/neighborhood/"):
        block = re.search(
            r"location\s+" + re.escape(route) + r"\s*\{(.*?)\n    \}", conf, re.S
        )
        if not block:
            missing.append(f"{route}: no location block")
        elif "limit_req" not in block.group(1):
            missing.append(f"{route}: no limit_req, so it is unbounded")
    assert not missing, "\n  ".join(missing)


def test_robots_txt_still_allows_the_crawlable_routes(conf):
    robots = (Path(__file__).parent.parent / "frontend" / "robots.txt").read_text()
    disallowed = re.findall(r"(?im)^\s*Disallow:\s*(\S+)", robots)
    for route in ("/property/", "/llc/", "/neighborhood/", "/evictions/", "/network/"):
        for d in disallowed:
            assert not route.startswith(d.rstrip("*")) or d in ("/", ""), \
                f"robots.txt disallows {d}, which blocks {route}"
    assert re.search(r"(?im)^\s*Sitemap:\s*https://", robots), \
        "robots.txt declares no https Sitemap: line; both engines read it there"
