"""
Nightly pipeline cron entry point.

Invoked by system cron at 2:00 AM UTC:
  0 2 * * * root /root/pulsecities/venv/bin/python -m scheduler.main >> /var/log/pulsecities/scraper.log 2>&1

Exits 0 on success, 1 if any scraper failed or if an uncaught exception occurs.
This satisfies SCHED-02: non-zero exit code on pipeline failure.
"""

import logging
import sys

from config.logging_config import configure_logging
from scheduler.pipeline import run_nightly_pipeline


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("scheduler/main.py started")
    try:
        ok = run_nightly_pipeline()
        if not ok:
            logger.error("Pipeline completed with one or more scraper failures — exiting 1")
            sys.exit(1)
        logger.info("Pipeline completed successfully — exiting 0")
    except Exception as exc:
        logger.error("Pipeline raised uncaught exception: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
