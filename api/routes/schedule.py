"""GET /api/schedule — exposes digest send schedule so frontend and emails stay in sync."""
from fastapi import APIRouter
from config.schedule import DIGEST_CRON, DIGEST_SEND_DAY, DIGEST_SEND_TIMEZONE

router = APIRouter(tags=["system"])


@router.get("/schedule")
def get_schedule():
    return {
        "send_day":  DIGEST_SEND_DAY,
        "cron":      DIGEST_CRON,
        "timezone":  DIGEST_SEND_TIMEZONE,
    }
