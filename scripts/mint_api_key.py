"""
Mint a partner API key. Prints the plaintext once; only the SHA-256 is stored.

    PYTHONPATH=. venv/bin/python -m scripts.mint_api_key --label "Newsroom X" --email data@newsroom.example
    PYTHONPATH=. venv/bin/python -m scripts.mint_api_key --list
    PYTHONPATH=. venv/bin/python -m scripts.mint_api_key --revoke "Newsroom X"
"""

import argparse
import hashlib
import secrets

from sqlalchemy import text

from models.database import get_scraper_db


def mint(label: str, email: str, tier: str) -> None:
    key = "pc_live_" + secrets.token_urlsafe(32)
    key_hash = hashlib.sha256(key.encode()).hexdigest()
    with get_scraper_db() as db:
        db.execute(text("""
            INSERT INTO api_keys (key_hash, label, owner_email, tier, active, created_at, updated_at)
            VALUES (:h, :l, :e, :t, true, now(), now())
        """), {"h": key_hash, "l": label, "e": email, "t": tier})
        db.commit()
    print(f"Key for {label} <{email}> (tier: {tier}):")
    print(f"  {key}")
    print("This is the only time the plaintext is shown. Store it now.")


def list_keys() -> None:
    with get_scraper_db() as db:
        rows = db.execute(text("""
            SELECT label, owner_email, tier, active, created_at, last_used_at
            FROM api_keys ORDER BY created_at
        """)).fetchall()
    if not rows:
        print("No keys minted yet.")
        return
    for r in rows:
        state = "active" if r.active else "revoked"
        used = r.last_used_at.strftime("%Y-%m-%d %H:%M") if r.last_used_at else "never"
        print(f"{r.label:30} {r.owner_email:34} {r.tier:10} {state:8} last used {used}")


def revoke(label: str) -> None:
    with get_scraper_db() as db:
        n = db.execute(text(
            "UPDATE api_keys SET active = false, updated_at = now() WHERE label = :l AND active"
        ), {"l": label}).rowcount
        db.commit()
    print(f"Revoked {n} key(s) labeled {label!r}." if n else f"No active key labeled {label!r}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mint, list, or revoke partner API keys")
    parser.add_argument("--label", help="who the key is for, e.g. a newsroom name")
    parser.add_argument("--email", help="contact email for the key owner")
    parser.add_argument("--tier", default="partner", help="key tier (default: partner)")
    parser.add_argument("--list", action="store_true", help="list all keys")
    parser.add_argument("--revoke", metavar="LABEL", help="revoke the active key with this label")
    args = parser.parse_args()

    if args.list:
        list_keys()
    elif args.revoke:
        revoke(args.revoke)
    elif args.label and args.email:
        mint(args.label, args.email, args.tier)
    else:
        parser.error("provide --label and --email to mint, or --list / --revoke LABEL")
