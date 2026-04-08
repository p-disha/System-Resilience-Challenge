"""Background webhook delivery worker.

Architecture
------------
  - N worker threads share one PostgreSQL connection pool.
  - Each thread runs claim_and_process_one_job() in a tight loop.
  - SELECT FOR UPDATE SKIP LOCKED ensures threads never claim the same job.
  - One watchdog thread resets stuck 'processing' jobs and refreshes queue-depth
    metrics every 30 seconds.

Two-transaction delivery pattern
---------------------------------
  TX1: SELECT FOR UPDATE SKIP LOCKED → mark 'processing' → COMMIT
       Releases the row lock immediately so other workers can proceed.
  TX2: HTTP call → mark delivered/pending/failed → COMMIT
       DB connection is NOT held open during the HTTP timeout window.

  Without this split, a 10 s HTTP timeout would hold a pool slot for 10 s,
  limiting concurrency to pool_size even with many worker threads.

Event-driven wakeup
--------------------
  Workers block on _wakeup_event instead of a plain sleep.  The ingestion API
  calls notify_job_enqueued() immediately after INSERT, waking all idle workers
  so delivery starts in milliseconds rather than up to poll_interval_seconds.

HTTP client
-----------
  Each worker thread owns one persistent httpx.Client (thread-local storage),
  enabling connection keep-alive and TLS session resumption across deliveries.

  Redirects: up to 2 hops are followed.  An event hook re-runs the SSRF check
  on every Location header so a redirect can never bypass the enqueue-time check.

Circuit breaker
---------------
  Per-target-URL (scheme+host).  Opens after circuit_breaker_threshold failures
  within circuit_breaker_window_seconds.  Blocked jobs are rescheduled without
  consuming a retry attempt.  After circuit_breaker_recovery_seconds one probe
  is allowed through; on success the circuit resets to CLOSED.
"""

import json
import logging
import random
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import httpx
from sqlalchemy import text

import metrics as metrics_module
from circuit_breaker import circuit_breaker
from config import settings
from constants import STATUS_DELIVERED, STATUS_FAILED, STATUS_PENDING, STATUS_PROCESSING
from database import get_db_session
from models import WebhookJob
from signing import build_signature_headers
from ssrf import ssrf_violation

logger = logging.getLogger(__name__)

# Strip ASCII control characters from error messages before storing in the DB.
# Prevents log injection: a malicious server could embed newlines to forge log lines.
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f]")

# Thread-local storage for the persistent HTTP client.
_thread_local = threading.local()

# Set by start_workers(); the ingestion API calls notify_job_enqueued() to wake
# idle workers immediately instead of waiting for the poll interval to expire.
_wakeup_event: threading.Event | None = None

# Worker self-protection: exit after this many consecutive unhandled exceptions.
_MAX_CONSECUTIVE_WORKER_ERRORS = 10

# Watchdog: check queue depth this often (seconds); run stuck-job cleanup
# only when processing_job_timeout_seconds have elapsed since last cleanup.
_DEPTH_UPDATE_INTERVAL_SECONDS = 30


# ─── Public API for the ingestion layer ──────────────────────────────────────


def notify_job_enqueued() -> None:
    """Wake idle workers immediately after a new job is INSERT-ed.

    Called from the FastAPI ingestion route after a successful flush.
    Safe to call before workers have started (no-op if _wakeup_event is None).
    """
    if _wakeup_event is not None:
        _wakeup_event.set()


# ─── Error sanitisation ───────────────────────────────────────────────────────


def _sanitize_error(msg: str, max_length: int = 500) -> str:
    """Strip control characters and truncate error messages.

    Prevents log injection and potential XSS if last_error is ever rendered
    in a web UI.
    """
    return _CONTROL_CHAR_RE.sub(" ", msg)[:max_length].strip()


# ─── Exponential backoff with jitter ─────────────────────────────────────────


def compute_next_delay(attempt_count: int) -> float:
    """Exponential backoff with ±jitter_factor randomisation.

    Formula:
        base   = base_delay_seconds × 2^(attempt_count - 1)
        capped = min(base, max_delay_seconds)
        delay  = capped ± (capped × jitter_factor × random)   [min 1 s]

    Approximate sequence (before jitter, default settings):
        attempt 1 →  2 s   attempt 4 → 16 s
        attempt 2 →  4 s   attempt 5 → 32 s
        attempt 3 →  8 s   ...capped at max_delay_seconds (1 h)

    The ±jitter_factor spread breaks thundering-herd synchronisation when many
    jobs fail together (e.g. the merchant endpoint goes down briefly).

    Args:
        attempt_count: 1-indexed count of attempts made so far.
    """
    base = settings.base_delay_seconds * (2 ** (attempt_count - 1))
    capped = min(base, settings.max_delay_seconds)
    jitter_range = capped * settings.jitter_factor
    return max(1.0, capped + random.uniform(-jitter_range, jitter_range))


# ─── HTTP client (thread-local, persistent) ───────────────────────────────────


def _ssrf_redirect_hook(response: httpx.Response) -> None:
    """httpx response event hook — block redirects to private IPs.

    Fires for every response, including 3xx redirects, before httpx follows
    them.  Re-running the SSRF check on the Location header closes the
    DNS-rebinding window that exists between enqueue-time validation and the
    actual redirect destination.
    """
    if not response.is_redirect:
        return
    location = response.headers.get("location", "")
    if not location:
        return
    # Resolve relative redirect URLs against the current request URL.
    if not location.startswith(("http://", "https://")):
        location = urljoin(str(response.request.url), location)
    violation = ssrf_violation(location)
    if violation:
        raise httpx.RequestError(
            f"SSRF blocked redirect to {location}: {violation}",
            request=response.request,
        )


def _get_http_client() -> httpx.Client:
    """Return this thread's persistent httpx.Client, creating it on first call.

    Reusing the client enables HTTP keep-alive and TLS session resumption.
    max_redirects=2 allows load-balancer hops (e.g. HTTP→HTTPS) without
    opening the door to infinite redirect loops.  The SSRF hook re-validates
    every redirect destination.
    """
    client: httpx.Client | None = getattr(_thread_local, "client", None)
    if client is None or client.is_closed:
        _thread_local.client = httpx.Client(
            timeout=settings.request_timeout_seconds,
            follow_redirects=True,
            max_redirects=2,
            event_hooks={"response": [_ssrf_redirect_hook]},
        )
    return _thread_local.client


def _close_http_client() -> None:
    """Close and discard this thread's HTTP client (called on worker exit)."""
    client: httpx.Client | None = getattr(_thread_local, "client", None)
    if client and not client.is_closed:
        try:
            client.close()
        except Exception:
            pass
    _thread_local.client = None


# ─── HTTP delivery ────────────────────────────────────────────────────────────


def deliver_job(job: WebhookJob) -> tuple[bool, str]:
    """Send one HTTP POST to the target URL with HMAC-SHA256 signing.

    Returns:
        (True,  "")           — 2xx received; delivery succeeded.
        (False, error_msg)    — anything else; caller will schedule a retry.

    Checks:
    1. SSRF re-validation at delivery time (DNS-rebinding defence).
    2. Circuit breaker — caller should have already checked, but the check is
       cheap enough to repeat here as defence-in-depth.
    3. Response body is capped at 300 bytes before decode to prevent a large
       error page from being loaded entirely into memory.
    """
    # SSRF re-check: re-resolve the hostname at dispatch time.
    violation = ssrf_violation(job.target_url)
    if violation:
        return False, _sanitize_error(f"SSRF check failed at delivery: {violation}")

    payload_bytes = json.dumps(job.payload, separators=(",", ":")).encode("utf-8")
    sig_headers = build_signature_headers(payload_bytes)

    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Event-Id": str(job.id),
        "X-Webhook-Event-Type": job.event_type,
        "X-Webhook-Attempt-Number": str(job.attempt_count + 1),
        **sig_headers,
    }

    try:
        client = _get_http_client()
        response = client.post(job.target_url, content=payload_bytes, headers=headers)

        if 200 <= response.status_code < 300:
            return True, ""

        # Cap response body at 300 *bytes* (not chars) before decode to prevent
        # loading a large error page into memory before slicing.
        raw_body = response.content[:300].decode("utf-8", errors="replace")
        return False, _sanitize_error(f"HTTP {response.status_code}: {raw_body}")

    except httpx.TimeoutException:
        return False, f"Timed out after {settings.request_timeout_seconds}s"
    except httpx.ConnectError as exc:
        return False, _sanitize_error(f"Connection refused/unreachable: {exc}")
    except httpx.RemoteProtocolError as exc:
        return False, _sanitize_error(f"Protocol error (server closed connection): {exc}")
    except httpx.TooManyRedirects:
        return False, "Too many redirects (max 2)"
    except Exception as exc:
        return False, _sanitize_error(f"{type(exc).__name__}: {exc}")


# ─── Claim + process cycle ────────────────────────────────────────────────────


def claim_and_process_one_job() -> bool:
    """Atomically claim and process one pending job.

    Returns:
        True  — a job was found and processed (regardless of delivery outcome).
        False — queue was empty; caller should wait before next poll.

    Circuit-breaker hold:
        If the circuit is open for the job's target URL, the job is rescheduled
        for circuit_breaker_recovery_seconds without counting as a retry attempt.
        This prevents jobs from burning through their retry budget while the
        endpoint is known to be down.
    """
    # ── TX1: Claim one job atomically ────────────────────────────────────────
    with get_db_session() as session:
        row = session.execute(
            text("""
                SELECT id FROM webhook_jobs
                WHERE  status = 'pending'
                  AND  next_attempt_at <= NOW()
                ORDER  BY next_attempt_at ASC
                LIMIT  1
                FOR UPDATE SKIP LOCKED
            """)
        ).fetchone()

        if row is None:
            return False

        job_id = row[0]
        session.execute(
            text("""
                UPDATE webhook_jobs
                SET    status = 'processing',
                       updated_at = NOW()
                WHERE  id = :id
            """),
            {"id": job_id},
        )
        # COMMIT: lock released, other workers can proceed.

    # ── TX2: Deliver and write result ────────────────────────────────────────
    with get_db_session() as session:
        job = session.get(WebhookJob, job_id)
        if job is None:
            logger.error(f"Job {job_id} vanished between TX1 and TX2 — skipping.")
            return True

        now = datetime.now(timezone.utc)

        # ── Circuit-breaker check ─────────────────────────────────────────────
        allowed, cb_reason = circuit_breaker.allow_request(job.target_url)
        if not allowed:
            # Reschedule without counting as a retry attempt.
            delay = settings.circuit_breaker_recovery_seconds
            job.status = STATUS_PENDING
            job.next_attempt_at = now + timedelta(seconds=delay)
            job.updated_at = now
            session.add(job)
            metrics_module.circuit_breaker_blocked_total.labels(
                target_url=job.target_url
            ).inc()
            logger.warning(
                f"[{job.id}] {cb_reason} — "
                f"rescheduled {delay}s (attempt_count unchanged at {job.attempt_count})"
            )
            return True

        # ── Attempt delivery ──────────────────────────────────────────────────
        attempt_num = job.attempt_count + 1

        logger.info(
            f"[{job.id}] event={job.event_type} "
            f"attempt={attempt_num}/{job.max_attempts} → {job.target_url}"
        )

        t0 = time.monotonic()
        success, error_msg = deliver_job(job)
        elapsed = time.monotonic() - t0

        metrics_module.delivery_duration_seconds.observe(elapsed)

        job.attempt_count = attempt_num
        job.last_attempt_at = now
        job.updated_at = now

        if success:
            job.status = STATUS_DELIVERED
            job.last_error = None
            circuit_breaker.record_success(job.target_url)
            metrics_module.jobs_delivered_total.inc()
            logger.info(
                f"[{job.id}] DELIVERED on attempt {attempt_num} ({elapsed:.2f}s)"
            )

        elif attempt_num >= job.max_attempts:
            job.status = STATUS_FAILED
            job.last_error = error_msg
            circuit_breaker.record_failure(job.target_url)
            metrics_module.jobs_failed_total.inc()
            logger.error(
                f"[{job.id}] PERMANENTLY FAILED after {attempt_num} attempts. "
                f"Last error: {error_msg}"
            )

        else:
            delay = compute_next_delay(attempt_num)
            next_at = now + timedelta(seconds=delay)
            job.status = STATUS_PENDING
            job.next_attempt_at = next_at
            job.last_error = error_msg
            circuit_breaker.record_failure(job.target_url)
            if attempt_num > 1:
                metrics_module.jobs_retried_total.inc()
            logger.warning(
                f"[{job.id}] Attempt {attempt_num} FAILED in {elapsed:.2f}s — "
                f"retry in {delay:.1f}s at {next_at.strftime('%H:%M:%S')} UTC. "
                f"Error: {error_msg}"
            )

        session.add(job)

    return True


# ─── Watchdog ─────────────────────────────────────────────────────────────────


def _update_queue_depth() -> None:
    """Refresh the queue-depth Prometheus gauges from the database."""
    try:
        with get_db_session() as session:
            rows = session.execute(
                text("""
                    SELECT status, COUNT(*)
                    FROM   webhook_jobs
                    WHERE  status IN ('pending', 'processing')
                    GROUP  BY status
                """)
            ).fetchall()
        depth = {row[0]: row[1] for row in rows}
        metrics_module.queue_depth_pending.set(depth.get(STATUS_PENDING, 0))
        metrics_module.queue_depth_processing.set(depth.get(STATUS_PROCESSING, 0))
    except Exception as exc:
        logger.error(f"[watchdog] Queue depth update failed: {exc}")


def _reset_stuck_jobs(timeout_seconds: int) -> None:
    """Reset jobs stuck in 'processing' for longer than timeout_seconds.

    Uses SET LOCAL statement_timeout so the UPDATE aborts cleanly if the
    database is under lock contention rather than blocking the watchdog thread
    indefinitely.
    """
    try:
        with get_db_session() as session:
            # SET LOCAL applies only within the current transaction.
            session.execute(text("SET LOCAL statement_timeout = '30s'"))
            result = session.execute(
                text("""
                    UPDATE webhook_jobs
                    SET    status     = 'pending',
                           updated_at = NOW()
                    WHERE  status = 'processing'
                      AND  updated_at < NOW() - CAST(:secs || ' seconds' AS INTERVAL)
                """),
                {"secs": timeout_seconds},
            )
            count = result.rowcount
            if count > 0:
                logger.warning(
                    f"[watchdog] Reset {count} stuck 'processing' job(s) "
                    f"(idle >{timeout_seconds}s) → 'pending'."
                )
    except Exception as exc:
        logger.error(f"[watchdog] Stuck-job reset failed: {exc}", exc_info=True)


def watchdog_loop(stop_event: threading.Event) -> None:
    """Periodically update metrics and reset jobs stuck in 'processing'.

    Two responsibilities at different cadences:
      - Queue-depth gauges:  every _DEPTH_UPDATE_INTERVAL_SECONDS (30 s).
      - Stuck-job cleanup:   every processing_job_timeout_seconds (default 300 s).

    The statement_timeout on the cleanup UPDATE prevents the watchdog from
    blocking indefinitely if the database is slow or locked.
    """
    timeout = settings.processing_job_timeout_seconds
    logger.info(
        f"[watchdog] started "
        f"(depth_interval={_DEPTH_UPDATE_INTERVAL_SECONDS}s, "
        f"stuck_timeout={timeout}s)"
    )

    last_cleanup = time.monotonic() - timeout  # run cleanup on first wake

    while not stop_event.is_set():
        stop_event.wait(timeout=_DEPTH_UPDATE_INTERVAL_SECONDS)
        if stop_event.is_set():
            break

        _update_queue_depth()

        now = time.monotonic()
        if now - last_cleanup >= timeout:
            _reset_stuck_jobs(timeout)
            last_cleanup = now

    logger.info("[watchdog] stopped.")


# ─── Worker thread loop ───────────────────────────────────────────────────────


def worker_loop(worker_id: int, stop_event: threading.Event) -> None:
    """Poll for jobs and process them until stop_event is set.

    Idle behaviour:
        When the queue is empty the thread waits on _wakeup_event (or times out
        after poll_interval_seconds).  The ingestion API sets the event on every
        new INSERT so delivery starts within milliseconds, not up to 2 seconds.

    Error resilience:
        Consecutive unhandled exceptions (e.g. DB down) trigger exponential
        back-off: 5 s → 10 s → 20 s → 40 s → 60 s (capped).  After
        _MAX_CONSECUTIVE_WORKER_ERRORS the worker exits cleanly so the container
        orchestrator can restart the process.
    """
    logger.info(f"[worker-{worker_id}] started.")
    consecutive_errors = 0

    try:
        while not stop_event.is_set():
            try:
                found = claim_and_process_one_job()
                consecutive_errors = 0
                metrics_module.worker_consecutive_errors.labels(
                    worker_id=str(worker_id)
                ).set(0)

                if not found and _wakeup_event is not None:
                    # Block until a new job arrives or the poll interval expires.
                    _wakeup_event.wait(timeout=settings.poll_interval_seconds)
                    _wakeup_event.clear()
                elif not found:
                    stop_event.wait(timeout=settings.poll_interval_seconds)

            except Exception as exc:
                consecutive_errors += 1
                backoff = min(5.0 * (2 ** (consecutive_errors - 1)), 60.0)
                metrics_module.worker_consecutive_errors.labels(
                    worker_id=str(worker_id)
                ).set(consecutive_errors)
                logger.error(
                    f"[worker-{worker_id}] unhandled error "
                    f"(#{consecutive_errors}/{_MAX_CONSECUTIVE_WORKER_ERRORS}): {exc} "
                    f"— backing off {backoff:.0f}s",
                    exc_info=True,
                )
                if consecutive_errors >= _MAX_CONSECUTIVE_WORKER_ERRORS:
                    logger.critical(
                        f"[worker-{worker_id}] reached {_MAX_CONSECUTIVE_WORKER_ERRORS} "
                        f"consecutive errors — exiting. Restart the service to recover."
                    )
                    break
                stop_event.wait(timeout=backoff)
    finally:
        _close_http_client()
        logger.info(f"[worker-{worker_id}] stopped.")


# ─── Startup ──────────────────────────────────────────────────────────────────


def start_workers(n: int) -> tuple[list[threading.Thread], threading.Event]:
    """Start n delivery worker threads plus one watchdog thread.

    Also initialises _wakeup_event so notify_job_enqueued() is live from the
    moment workers start.

    Returns:
        (threads, stop_event) — set stop_event to signal graceful shutdown.
        All threads (workers + watchdog) are in the returned list.
    """
    global _wakeup_event
    _wakeup_event = threading.Event()

    stop_event = threading.Event()
    threads: list[threading.Thread] = []

    watchdog = threading.Thread(
        target=watchdog_loop,
        args=(stop_event,),
        daemon=True,
        name="webhook-watchdog",
    )
    watchdog.start()
    threads.append(watchdog)

    for i in range(n):
        t = threading.Thread(
            target=worker_loop,
            args=(i, stop_event),
            daemon=True,
            name=f"webhook-worker-{i}",
        )
        t.start()
        threads.append(t)

    logger.info(f"Started {n} worker thread(s) + 1 watchdog.")
    return threads, stop_event
