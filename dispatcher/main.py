"""FastAPI ingestion API + worker lifecycle manager.

Startup
-------
  1. Configure structured (JSON) logging.
  2. Create/verify DB tables (idempotent).
  3. Reset stuck 'processing' jobs to 'pending' — crash-recovery guarantee.
  4. Spawn WORKER_CONCURRENCY delivery threads + 1 watchdog thread.

Shutdown (SIGTERM / SIGINT)
----------------------------
  1. Signal workers via stop_event.set().
  2. Wait up to 30 s for in-flight deliveries to complete.

Rate limiting
-------------
  POST /events is rate-limited to 200 req/min per client IP via slowapi.
  All other endpoints are unlimited (internal monitoring / status queries).

Observability
-------------
  GET /metrics — Prometheus scrape endpoint (prometheus_client).
  GET /health  — 200 OK + DB probe; 503 if database is unreachable.
  GET /stats   — current job counts by status.
"""

import asyncio
import logging
import sys
import threading
import uuid as uuid_lib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field, field_validator
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import metrics as metrics_module
from config import settings
from constants import STATUS_FAILED, STATUS_PENDING, VALID_STATUSES
from database import get_db_session, init_db
from models import WebhookJob
from ssrf import ssrf_violation
from worker import notify_job_enqueued, start_workers


# ─── Structured JSON logging ──────────────────────────────────────────────────
# python-json-logger emits one JSON object per log record, making logs easy to
# ingest into Datadog, ELK, or any log aggregation tool that parses JSON.
# Configured before any logger is acquired so all modules emit JSON.

def _configure_logging() -> None:
    from pythonjsonlogger import jsonlogger

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)


_configure_logging()
logger = logging.getLogger(__name__)


# ─── Rate limiter ─────────────────────────────────────────────────────────────

limiter = Limiter(key_func=get_remote_address)


# ─── Lifecycle ────────────────────────────────────────────────────────────────

_worker_threads: list[threading.Thread] = []
_stop_event: threading.Event | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker_threads, _stop_event

    logger.info("=== Webhook Dispatcher starting up ===")

    # Run blocking DB init in a thread so we don't stall the async event loop.
    await asyncio.to_thread(init_db)

    _worker_threads, _stop_event = start_workers(settings.worker_concurrency)
    logger.info(f"=== Ready — {settings.worker_concurrency} worker(s) running ===")

    yield  # ← application serves requests here

    logger.info("=== Shutting down — signalling workers ===")
    if _stop_event:
        _stop_event.set()
    for t in _worker_threads:
        t.join(timeout=30)
    logger.info("=== Shutdown complete ===")


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Webhook Dispatcher",
    description=(
        "At-least-once webhook delivery engine.\n\n"
        "- **POST /events** — enqueue a delivery job\n"
        "- **GET /jobs/{id}** — poll delivery status\n"
        "- **POST /jobs/{id}/retry** — re-enqueue a failed job\n"
        "- **GET /jobs** — list jobs with optional status filter\n"
        "- **GET /stats** — queue statistics by status\n"
        "- **GET /health** — health check (probes database)\n"
        "- **GET /metrics** — Prometheus metrics"
    ),
    version="2.0.0",
    lifespan=lifespan,
)

# Rate limiter middleware and exception handler.
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

# CORS: configurable via CORS_ORIGINS env var (default: disabled).
_cors_origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
if _cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )


# ─── Middleware ───────────────────────────────────────────────────────────────


@app.middleware("http")
async def limit_request_size(request: Request, call_next):
    """Reject payloads exceeding max_payload_bytes before JSON parsing.

    Checked against Content-Length so an oversized body never touches the
    JSON parser or the JSONB column.
    """
    if request.method == "POST":
        content_length = request.headers.get("content-length")
        if content_length:
            size = int(content_length)
            if size > settings.max_payload_bytes:
                return JSONResponse(
                    status_code=413,
                    content={
                        "detail": (
                            f"Request body too large: {size} bytes. "
                            f"Maximum allowed: {settings.max_payload_bytes} bytes."
                        )
                    },
                )
    return await call_next(request)


# ─── SSRF protection ──────────────────────────────────────────────────────────


def _check_ssrf(url: str) -> None:
    """Raise HTTP 400 if the URL resolves to a private/loopback IP.

    Delegates to ssrf.ssrf_violation() — the same function the delivery worker
    calls at dispatch time — so both sites stay in sync.
    """
    violation = ssrf_violation(url)
    if violation:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=violation)


# ─── Request / Response schemas ───────────────────────────────────────────────


class EventRequest(BaseModel):
    event_type: str = Field(
        ...,
        min_length=1,
        max_length=255,
        examples=["payment.captured"],
        description="Dot-namespaced event type.",
    )
    payload: dict[str, Any] = Field(
        ...,
        examples=[{"transaction_id": "TXN-001", "amount": 149.99}],
        description="Arbitrary JSON payload delivered to the target.",
    )
    target_url: str | None = Field(
        default=None,
        description="Override delivery URL. Defaults to TARGET_URL env var.",
        examples=["https://merchant.example.com/webhooks"],
    )
    idempotency_key: str | None = Field(
        default=None,
        max_length=255,
        description=(
            "Optional deduplication key. Re-submitting with the same key "
            "returns the existing job instead of creating a duplicate. "
            f"Keys are honoured for {settings.idempotency_ttl_hours} hours."
        ),
        examples=["payment-captured-TXN-001"],
    )

    @field_validator("target_url")
    @classmethod
    def validate_url_scheme(cls, v: str | None) -> str | None:
        if v is None:
            return v
        parsed = urlparse(v)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(
                f"target_url must use http:// or https:// (got '{parsed.scheme}://'). "
                "Schemes like ftp://, file://, and data:// are not permitted."
            )
        if not parsed.netloc:
            raise ValueError("target_url must include a valid hostname.")
        return v


class EventResponse(BaseModel):
    job_id: str
    status: str
    message: str


class JobStatus(BaseModel):
    """All timestamps are UTC (ISO 8601 with +00:00 suffix)."""

    id: str
    event_type: str
    status: str
    attempt_count: int
    max_attempts: int
    next_attempt_at: datetime | None
    last_attempt_at: datetime | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime


class JobListResponse(BaseModel):
    items: list[JobStatus]
    total: int
    limit: int
    offset: int


# ─── Helpers ──────────────────────────────────────────────────────────────────


def get_session():
    """FastAPI dependency: yields a DB session, commits on clean exit."""
    with get_db_session() as session:
        yield session


def _job_to_status(job: WebhookJob) -> JobStatus:
    return JobStatus(
        id=str(job.id),
        event_type=job.event_type,
        status=job.status,
        attempt_count=job.attempt_count,
        max_attempts=job.max_attempts,
        next_attempt_at=job.next_attempt_at,
        last_attempt_at=job.last_attempt_at,
        last_error=job.last_error,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def _validate_job_id(job_id: str) -> None:
    """Raise HTTP 422 for malformed UUIDs before they reach the database.

    Without this guard, SQLAlchemy passes the raw string to PostgreSQL which
    raises 'invalid input syntax for type uuid' — an opaque 500.
    """
    try:
        uuid_lib.UUID(job_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"'{job_id}' is not a valid UUID.",
        )


# ─── Routes ───────────────────────────────────────────────────────────────────


@app.post(
    "/events",
    response_model=EventResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Enqueue a webhook delivery job",
    responses={
        202: {"description": "Job enqueued (or existing job returned for duplicate key)"},
        400: {"description": "SSRF protection triggered"},
        413: {"description": "Request body exceeds size limit"},
        422: {"description": "Validation error"},
        429: {"description": "Rate limit exceeded"},
    },
)
@limiter.limit("200/minute")
def ingest_event(
    request: Request,
    req: EventRequest,
    session: Session = Depends(get_session),
):
    """Accept an event and enqueue it for async delivery.

    Returns **202** immediately. Poll **GET /jobs/{job_id}** for status.

    **Idempotency**: supply ``idempotency_key`` to safely retry on network
    errors. Keys are de-duplicated within a ``IDEMPOTENCY_TTL_HOURS`` window.
    """
    target = req.target_url or settings.target_url
    if not target:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "target_url must be provided in the request body "
                "or set via the TARGET_URL environment variable."
            ),
        )

    _check_ssrf(target)

    # ── Idempotency check ─────────────────────────────────────────────────────
    if req.idempotency_key:
        ttl_cutoff = datetime.now(timezone.utc) - timedelta(
            hours=settings.idempotency_ttl_hours
        )
        existing = (
            session.query(WebhookJob)
            .filter(WebhookJob.idempotency_key == req.idempotency_key)
            .filter(WebhookJob.created_at > ttl_cutoff)
            .first()
        )
        if existing:
            logger.info(
                f"Idempotent return key={req.idempotency_key} "
                f"job={existing.id} status={existing.status}"
            )
            return EventResponse(
                job_id=str(existing.id),
                status=existing.status,
                message=f"Duplicate key — returning existing job (status={existing.status})",
            )

    # ── Insert new job ────────────────────────────────────────────────────────
    job = WebhookJob(
        event_type=req.event_type,
        payload=req.payload,
        target_url=target,
        idempotency_key=req.idempotency_key,
        status=STATUS_PENDING,
        max_attempts=settings.max_attempts,
        next_attempt_at=datetime.now(timezone.utc),
    )
    session.add(job)

    try:
        session.flush()
    except IntegrityError:
        # Race condition: concurrent request with same key won the INSERT.
        session.rollback()
        if req.idempotency_key:
            winner = (
                session.query(WebhookJob)
                .filter(WebhookJob.idempotency_key == req.idempotency_key)
                .first()
            )
            if winner:
                logger.info(
                    f"Idempotency race resolved key={req.idempotency_key} "
                    f"winner={winner.id}"
                )
                return EventResponse(
                    job_id=str(winner.id),
                    status=winner.status,
                    message=f"Concurrent duplicate — returning winner job (status={winner.status})",
                )
        logger.error("Unexpected IntegrityError during job insert", exc_info=True)
        raise HTTPException(status_code=409, detail="Duplicate constraint violation.")

    metrics_module.jobs_enqueued_total.labels(event_type=req.event_type).inc()

    logger.info(
        f"Enqueued job={job.id} event_type={req.event_type} "
        f"target={target} idempotency_key={req.idempotency_key}"
    )

    # Wake idle workers immediately — delivery starts in milliseconds.
    notify_job_enqueued()

    return EventResponse(
        job_id=str(job.id),
        status=STATUS_PENDING,
        message=f"Enqueued for delivery to {target}",
    )


@app.get(
    "/jobs/{job_id}",
    response_model=JobStatus,
    summary="Get delivery status of a specific job",
    responses={
        404: {"description": "Job not found"},
        422: {"description": "job_id is not a valid UUID"},
    },
)
def get_job(job_id: str, session: Session = Depends(get_session)):
    _validate_job_id(job_id)
    job = session.get(WebhookJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return _job_to_status(job)


@app.post(
    "/jobs/{job_id}/retry",
    response_model=EventResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Re-enqueue a failed job for delivery",
    responses={
        202: {"description": "Job reset to pending"},
        404: {"description": "Job not found"},
        409: {"description": "Job is not in 'failed' status"},
        422: {"description": "job_id is not a valid UUID"},
    },
)
def retry_job(job_id: str, session: Session = Depends(get_session)):
    """Reset a permanently failed job to pending for a fresh delivery attempt.

    This gives the job a full new retry budget (attempt_count reset to 0,
    max_attempts reset to the current MAX_ATTEMPTS setting).

    Only jobs with ``status=failed`` can be retried.  Pending, processing, and
    delivered jobs are rejected with HTTP 409.
    """
    _validate_job_id(job_id)
    job = session.get(WebhookJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    if job.status != STATUS_FAILED:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Job '{job_id}' is in status='{job.status}'. "
                "Only 'failed' jobs can be retried."
            ),
        )

    now = datetime.now(timezone.utc)
    job.status = STATUS_PENDING
    job.attempt_count = 0
    job.max_attempts = settings.max_attempts
    job.next_attempt_at = now
    job.last_error = None
    job.updated_at = now
    session.add(job)

    metrics_module.jobs_replayed_total.inc()
    logger.info(f"Job {job_id} manually reset to pending for retry.")

    notify_job_enqueued()

    return EventResponse(
        job_id=job_id,
        status=STATUS_PENDING,
        message="Job reset to pending. Delivery will be attempted shortly.",
    )


@app.get(
    "/jobs",
    response_model=JobListResponse,
    summary="List jobs with optional status filter and pagination",
)
def list_jobs(
    status_filter: str | None = Query(
        default=None,
        alias="status",
        description=f"Filter by status: {', '.join(sorted(VALID_STATUSES))}",
    ),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
):
    query = session.query(WebhookJob)
    if status_filter:
        if status_filter not in VALID_STATUSES:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid status '{status_filter}'. "
                    f"Must be one of: {sorted(VALID_STATUSES)}"
                ),
            )
        query = query.filter(WebhookJob.status == status_filter)

    total = query.count()
    jobs = (
        query.order_by(WebhookJob.created_at.desc()).limit(limit).offset(offset).all()
    )
    return JobListResponse(
        items=[_job_to_status(j) for j in jobs],
        total=total,
        limit=limit,
        offset=offset,
    )


@app.get("/stats", summary="Queue statistics grouped by delivery status")
def queue_stats(session: Session = Depends(get_session)):
    rows = session.execute(
        text("SELECT status, COUNT(*) FROM webhook_jobs GROUP BY status ORDER BY status")
    ).fetchall()
    return {
        "counts": {row[0]: row[1] for row in rows},
        "total": sum(row[1] for row in rows),
    }


@app.get("/health", summary="Health check — probes database connectivity")
def health():
    """Returns 200 OK when the API and database are both reachable.

    Returns 503 if the database is unreachable.  Container orchestrators and
    load balancers use this endpoint; a 200 with a broken DB would cause the
    container to appear healthy while no jobs can be processed.
    """
    try:
        with get_db_session() as session:
            session.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as exc:
        logger.error(f"Health check DB probe failed: {exc}")
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "detail": "database unreachable"},
        )


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics():
    """Prometheus scrape endpoint.

    Exposes all metrics registered in metrics.py.
    Typical scrape interval: 15 s.
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ─── Exception handlers ───────────────────────────────────────────────────────


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded. Please slow down."},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.error(
        f"Unhandled exception [{type(exc).__name__}] "
        f"on {request.method} {request.url.path}: {exc}",
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "An internal error occurred. Please try again later."},
    )


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, workers=1)
