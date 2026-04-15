"""
Shared database engine and session factory.
Create the engine once at import time — never per-request.
All scrapers and the API import get_db() or SessionLocal from here.
"""

import os
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")

# pool_size=5, max_overflow=10 — shared across scrapers and API workers
# pool_pre_ping=True — avoids stale connection errors after idle periods
# On 2GB VPS: PostgreSQL max_connections=100, so 2 workers × 15 = 30 max connections
engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """FastAPI dependency — yields a database session and closes it after the request."""
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_scraper_db():
    """Context manager for scraper use — commits or rolls back cleanly."""
    db: Session = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
