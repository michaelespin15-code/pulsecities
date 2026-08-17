#!/usr/bin/env python3
"""Stamp the Tailwind build's content hash into the pages that link it.

The build is written in place, to a fixed filename, so the URL never changed
when the CSS did. nginx worked around that by dropping /tailwind.min.css to a
one-hour must-revalidate while every other static asset gets thirty days
immutable, which is a real cost paid on every visit to the map app, and it
still leaves an hour in which a browser can hold the old CSS against new HTML.
The map app is the only page that links a stylesheet at all -- the rest inline
theirs -- so the mismatch shows up there as unstyled or half-styled chrome.

Appending the hash as ?v= gives the file a new URL whenever its bytes change,
which is what makes an immutable policy safe. Runs as the postbuild step of
`npm run build:css`; tests/test_asset_stamp.py fails the suite if a build is
committed without it.
"""

import hashlib
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ASSET = ROOT / "frontend" / "tailwind.min.css"
PAGES = [ROOT / "frontend" / "app.html"]

# Matches the link whether or not it already carries a stamp.
LINK = re.compile(r'(href="/tailwind\.min\.css)(\?v=[0-9a-f]+)?(")')


def asset_hash() -> str:
    return hashlib.sha256(ASSET.read_bytes()).hexdigest()[:12]


def main() -> int:
    if not ASSET.exists():
        print(f"stamp: {ASSET} not found; run the CSS build first", file=sys.stderr)
        return 1

    digest = asset_hash()
    changed = []
    for page in PAGES:
        html = page.read_text(encoding="utf-8")
        stamped, n = LINK.subn(rf'\1?v={digest}\3', html)
        if not n:
            print(f"stamp: no /tailwind.min.css link in {page.name}", file=sys.stderr)
            return 1
        if stamped != html:
            page.write_text(stamped, encoding="utf-8")
            changed.append(page.name)

    print(f"stamp: tailwind.min.css -> ?v={digest}"
          + (f" (updated {', '.join(changed)})" if changed else " (already current)"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
