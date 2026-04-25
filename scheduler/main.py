"""
Nightly pipeline cron entry point.

Invoked by system cron at 2:00 AM UTC:
  0 2 * * * root /root/pulsecities/venv/bin/python -m scheduler.main >> /var/log/pulsecities/scraper.log 2>&1

Exits 0 on success, 1 if any scraper failed or if an uncaught exception occurs.
This satisfies SCHED-02: non-zero exit code on pipeline failure.
"""

import logging
import os
import sys

from config.logging_config import configure_logging
from scheduler.pipeline import run_nightly_pipeline

_LOCK_FILE = "/tmp/pulsecities_pipeline.lock"


def _acquire_lock(logger: logging.Logger) -> bool:
    """
    Write PID to lock file and return True.
    If a lock file exists with a live PID, log and return False.
    Stale locks (dead PID) are removed and overwritten.
    """
    if os.path.exists(_LOCK_FILE):
        try:
            existing_pid = int(open(_LOCK_FILE).read().strip())
        except (ValueError, OSError):
            existing_pid = None

        if existing_pid is not None:
            try:
                os.kill(existing_pid, 0)  # signal 0: check existence only
                logger.warning(
                    "pipeline already running, skipping (lock=%s pid=%d)",
                    _LOCK_FILE, existing_pid,
                )
                return False
            except (ProcessLookupError, PermissionError):
                logger.warning("removing stale lock file (pid=%d no longer alive)", existing_pid)

        os.remove(_LOCK_FILE)

    with open(_LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    return True


def _release_lock() -> None:
    try:
        os.remove(_LOCK_FILE)
    except FileNotFoundError:
        pass


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("scheduler/main.py started")

    if not _acquire_lock(logger):
        sys.exit(0)

    try:
        ok = run_nightly_pipeline()
        if not ok:
            logger.error("Pipeline completed with one or more scraper failures — exiting 1")
            sys.exit(1)
        logger.info("Pipeline completed successfully — exiting 0")
    except Exception as exc:
        logger.error("Pipeline raised uncaught exception: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        _release_lock()


if __name__ == "__main__":
    main()
