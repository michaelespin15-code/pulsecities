#!/usr/bin/env python3
"""Report what the CSP would block, by loading every page in a real browser.

The policy in deploy/nginx-security-headers.conf ships report-only. This is the
tool that decides when it can stop being report-only, and the tool to run after
adding any third-party script, font, tile source or embed.

Reading the source is not enough to write this policy, which is the whole
reason this exists: the map style is fetched from basemaps.cartocdn.com, but
the style document then names tiles.basemaps.cartocdn.com for the vector tiles,
the sprite sheet and the glyph ranges. A policy built by grepping the HTML
passes all 17 other pages and leaves the map blank. Only a browser finds that.

    ./venv/bin/python scripts/csp_check.py [--base https://pulsecities.com]

Exits non-zero if anything was reported, so it can gate a deploy.
"""

import argparse
import asyncio
import sys

PAGES = ["/", "/map", "/displacement", "/evictions", "/neighborhoods",
         "/neighborhood/11216", "/press", "/methodology", "/flips", "/radar",
         "/this-week", "/llc", "/about", "/developers", "/status",
         "/property/3013370001", "/brooklyn", "/who-owns-my-building"]

# Attached before any page script runs, so nothing is missed during load.
LISTENER = """
window.__cspViolations = [];
document.addEventListener('securitypolicyviolation', e => {
  window.__cspViolations.push({
    directive: e.effectiveDirective || e.violatedDirective,
    blocked: (e.blockedURI || '').slice(0, 140),
    disposition: e.disposition,
  });
});
"""

# The map needs longer: tiles, sprite and glyph ranges all load after first paint.
SETTLE_MS = {"/map": 8000}


async def run(base: str) -> int:
    from playwright.async_api import async_playwright

    findings = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context(viewport={"width": 1280, "height": 900})
        await ctx.add_init_script(LISTENER)
        page = await ctx.new_page()
        for path in PAGES:
            try:
                await page.goto(base + path, wait_until="load", timeout=45000)
            except Exception as exc:
                print(f"  ! {path} did not load: {type(exc).__name__}", file=sys.stderr)
                continue
            await page.wait_for_timeout(SETTLE_MS.get(path, 800))
            counts = {}
            for v in await page.evaluate("window.__cspViolations || []"):
                counts[(v["directive"], v["blocked"], v["disposition"])] = (
                    counts.get((v["directive"], v["blocked"], v["disposition"]), 0) + 1)
            if counts:
                findings[path] = counts
        await browser.close()

    for path, counts in findings.items():
        print(f"\n{path}")
        for (directive, blocked, disposition), n in counts.items():
            print(f"   x{n:<3} [{disposition}] {directive:<14} {blocked}")

    total = sum(len(c) for c in findings.values())
    print(f"\n{total} distinct violation(s) across {len(PAGES)} pages")
    if total == 0:
        print("Policy covers every origin these pages use; safe to consider enforcing.")
    return 1 if total else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="https://pulsecities.com")
    args = ap.parse_args()
    return asyncio.run(run(args.base.rstrip("/")))


if __name__ == "__main__":
    raise SystemExit(main())
