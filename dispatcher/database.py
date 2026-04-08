import logging
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from config import settings

logger = logging.getLogger(__name__)


# pool_pre_ping=True: validates connections before use, handles DB restarts
# pool_recycle=1800: recycle connections after 30 min to avoid stale TCP sockets
#
# Pool sizing: each worker needs up to 2 concurrent connections (TX1 and TX2 can
# briefly overlap).  The watchdog also needs one connection periodically.
# Formula: (workers × 2) + 2 headroom, minimum 10.
_pool_size = max(settings.worker_concurrency * 2 + 2, 10)
_max_overflow = _pool_size  # allow doubling under burst load

engine = create_engine(
    settings.database_url,
    pool_size=_pool_size,
    max_overflow=_max_overflow,
    pool_pre_ping=True,
    pool_timeout=10,   # fail fast if pool is exhausted rather than waiting 30s
    pool_recycle=1800,
    echo=False,
)

# expire_on_commit=False: prevents DetachedInstanceError when accessing
# attributes after session.commit() outside the context manager
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


@contextmanager
def get_db_session():
    """Context manager that commits on success, rolls back on any exception."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db():
    """Create all tables and reset stuck jobs. Called once at startup."""
    import models  # local import to avoid circular dependency
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created/verified.")
    _reset_processing_jobs()


def _reset_processing_jobs():
    """On startup, return any 'processing' jobs to 'pending'.

    Implements the crash-recovery guarantee: if the worker process died
    while delivering a job (status='processing'), that job would be stuck
    forever without this reset. We return it to 'pending' so it is retried.
    This may cause at-most one duplicate delivery per crashed job — acceptable
    for at-least-once semantics.
    """
    with get_db_session() as session:
        result = session.execute(
            text("""
                UPDATE webhook_jobs
                SET status = 'pending',
                    updated_at = NOW()
                WHERE status = 'processing'
            """)
        )
        count = result.rowcount
        if count > 0:
            logger.warning(
                f"Startup recovery: reset {count} stuck 'processing' "
                f"job(s) back to 'pending'."
            )
        else:
            logger.info("Startup recovery: no stuck jobs found.")
