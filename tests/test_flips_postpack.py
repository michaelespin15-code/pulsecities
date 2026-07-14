"""
Guards for the ready-to-post pack in scripts/weekly_eviction_flips.py.

The pack ships copy that a human pastes straight into X, Bluesky, and a reporter
email, so the invariants that matter are: it fits the platform char limits, the
thread is numbered correctly, the receipts and site are present, and no em dash
sneaks into published copy.
"""

from scripts.weekly_eviction_flips import (
    build_x_thread,
    build_bluesky_post,
    build_reporter_pitch,
    build_post_pack,
    MAX_TWEET,
    MAX_BLUESKY,
)


def _arc(bbl, addr, zip_, ev, buy_date, buy_amt, sell_date, sell_amt, gain, ev_count=1):
    return {
        "key": f"{bbl}:BUY:SELL",
        "bbl": bbl,
        "address": addr,
        "zip_code": zip_,
        "eviction_date": ev,
        "eviction_count": ev_count,
        "buy_doc": "2025052100578001",
        "buy_date": buy_date,
        "buy_amt": buy_amt,
        "buyer": "QUEENS CORE SOLUTIONS LLC",
        "sell_doc": "2026060200564001",
        "sell_date": sell_date,
        "sell_amt": sell_amt,
        "gain_pct": gain,
    }


ARCS = [
    _arc("4116370041", "111-47 133 STREET", "11420", "2025-02-26",
         "2025-05-20", 380000.0, "2026-05-11", 960000.0, 153, ev_count=2),
    _arc("4156640038", "13-56 EGGERT PLACE", "11691", "2025-02-28",
         "2025-09-02", 350000.0, "2026-05-29", 700000.0, 100),
]


def test_thread_within_tweet_limit_and_numbered():
    thread = build_x_thread(ARCS)
    # lead + one per arc + closing
    assert len(thread) == len(ARCS) + 2
    for i, tweet in enumerate(thread, 1):
        assert len(tweet) <= MAX_TWEET, f"tweet {i} is {len(tweet)} chars: {tweet}"
        assert tweet.endswith(f"({i}/{len(thread)})")


def test_single_arc_thread_skips_the_summary_lead():
    thread = build_x_thread(ARCS[:1])
    # one arc + closing, no multi-building lead
    assert len(thread) == 2
    assert "buildings this week" not in thread[0]


def test_bluesky_within_limit_and_carries_site_and_top_arc():
    post = build_bluesky_post(ARCS)
    assert len(post) <= MAX_BLUESKY
    assert "pulsecities.com" in post
    # highest gain wins the single Bluesky slot
    assert "153% gain" in post


def test_arc_tweet_drops_docs_when_address_is_long():
    # An address long enough that core + ACRIS docs would blow past 280, but
    # core alone still fits. The tweet must keep the core and shed the docs.
    long_arc = _arc(
        "1234567890",
        "1234 SOME EXTREMELY LONG AND ENDLESSLY DESCRIPTIVE BOULEVARD OF THE "
        "AMERICAS GRAND APARTMENT COMPLEX BUILDING NORTH TOWER ANNEX",
        "10001", "2025-02-26", "2025-05-20", 380000.0, "2026-05-11", 960000.0, 153,
    )
    thread = build_x_thread([long_arc])
    arc_tweet = thread[0]  # single-arc thread: arc tweet is first
    assert len(arc_tweet) <= MAX_TWEET
    assert "ACRIS:" not in arc_tweet


def test_reporter_pitch_has_receipts_and_signoff():
    pitch = build_reporter_pitch(ARCS, scales={}, total=20)
    assert "Subject:" in pitch
    assert "2025052100578001" in pitch and "2026060200564001" in pitch
    assert "153% gain" in pitch  # top arc
    assert "pulsecities.com" in pitch
    assert "20 such arcs" in pitch


def test_reporter_pitch_notes_portfolio_scale_when_known():
    pitch = build_reporter_pitch(ARCS, scales={ARCS[0]["key"]: 14}, total=20)
    assert "holds 14 NYC buildings" in pitch


def test_pack_has_no_em_dash():
    pack = build_post_pack(ARCS, scales={ARCS[0]["key"]: 14}, total=20)
    assert "—" not in pack, "published copy must never contain an em dash"
