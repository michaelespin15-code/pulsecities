"""
Score history API endpoint.

GET /api/score-history/{zip_code}?days=90
  Returns a chronological list of daily displacement score snapshots for a zip code.
  Returns 200 with [] when the zip exists but has no history yet.
  Returns 400 for invalid zip format.

Security:
  - zip_code validated as 5-digit numeric before any DB query (T-06-01-01)
  - days clamped to [1, 365] to prevent full-table scans (T-06-01-02)
  - rate-limited to 60/minute following existing endpoint pattern (T-06-01-03)
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/score-history", tags=["score-history"])
limiter = Limiter(key_func=get_remote_address, headers_enabled=True)


@router.get("/{zip_code}")
@limiter.limit("60/minute")
def get_score_history(
    request: Request,
    response: Response,
    zip_code: str,
    days: int = 90,
    db: Session = Depends(get_db),
):
    """
    Returns an ordered list of daily displacement score snapshots for a zip code.

    Query params:
      days (int, default 90): look-back window in calendar days, clamped to [1, 365].

    Response shape:
      [{"date": "YYYY-MM-DD", "score": float}, ...]

    Empty list (200) when the zip code has no history yet.
    400 when zip_code is not a 5-digit numeric string.
    """
    response.headers["Cache-Control"] = "public, max-age=3600"
    # --- Input validation (T-06-01-01) ---
    if not (len(zip_code) == 5 and zip_code.isdigit()):
        raise HTTPException(
            status_code=400,
            detail="zip_code must be a 5-digit numeric string",
        )

    # --- Clamp days to safe range (T-06-01-02) ---
    days = max(1, min(days, 365))

    rows = db.execute(
        text("""
            SELECT scored_at, composite_score
            FROM score_history
            WHERE zip_code = :zip
              AND scored_at >= CURRENT_DATE - :days * INTERVAL '1 day'
            ORDER BY scored_at ASC
        """),
        {"zip": zip_code, "days": days},
    ).fetchall()

    return [
        {"date": row.scored_at.isoformat(), "score": round(row.composite_score, 1)}
        for row in rows
    ]
