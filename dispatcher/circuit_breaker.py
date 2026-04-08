"""Per-target-URL circuit breaker.

Prevents the dispatcher from hammering a clearly-down merchant endpoint by
temporarily blocking delivery attempts after repeated failures.

States
------
  CLOSED   — deliveries proceed normally.
  OPEN     — deliveries are rejected immediately without an HTTP call.
             The circuit opens when ``failure_threshold`` failures accumulate
             within ``window_seconds``.
  HALF-OPEN — after ``recovery_seconds`` the circuit allows one probe attempt
             through.  On success it resets to CLOSED.  On failure the
             recovery timer restarts (back to OPEN).

All state is in-memory and thread-safe.  State is lost on process restart —
this is intentional: a fresh start should give the endpoint another chance
before the circuit re-opens based on new failures.

Usage
-----
    allowed, reason = circuit_breaker.allow_request(url)
    if not allowed:
        # reschedule without counting as an attempt
        return False, reason

    success, err = do_http_call(url)

    if success:
        circuit_breaker.record_success(url)
    else:
        circuit_breaker.record_failure(url)
"""

import logging
import threading
import time
from collections import deque
from urllib.parse import urlparse

import metrics as metrics_module

logger = logging.getLogger(__name__)


def _normalise_url(url: str) -> str:
    """Reduce a URL to scheme+host for circuit-breaker keying.

    Two jobs with the same host but different paths share a circuit.
    A single bad merchant endpoint should open the circuit for all jobs
    targeting that host, not just the specific path that first failed.
    """
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


class CircuitBreaker:
    """Thread-safe per-URL circuit breaker."""

    def __init__(
        self,
        failure_threshold: int = 5,
        window_seconds: int = 60,
        recovery_seconds: int = 300,
    ) -> None:
        self._lock = threading.Lock()
        # host → deque of failure timestamps (monotonic clock)
        self._failures: dict[str, deque[float]] = {}
        # host → monotonic timestamp when circuit re-enters HALF-OPEN
        self._open_until: dict[str, float] = {}
        # host → True while a half-open probe is in flight
        self._probing: dict[str, bool] = {}

        self.failure_threshold = failure_threshold
        self.window_seconds = window_seconds
        self.recovery_seconds = recovery_seconds

    # ── Public API ────────────────────────────────────────────────────────────

    def allow_request(self, url: str) -> tuple[bool, str]:
        """Return ``(True, "")`` if the delivery should proceed.

        Returns ``(False, reason)`` when the circuit is OPEN.
        A HALF-OPEN probe is allowed through for the first caller after the
        recovery timeout; subsequent callers are blocked until the probe lands.
        """
        host = _normalise_url(url)
        with self._lock:
            until = self._open_until.get(host)
            if until is None:
                return True, ""  # CLOSED

            now = time.monotonic()
            if now >= until:
                # Recovery timeout elapsed → allow one HALF-OPEN probe
                if not self._probing.get(host):
                    self._probing[host] = True
                    logger.info(f"[circuit] HALF-OPEN probe allowed for {host}")
                    return True, ""
                return False, f"Circuit OPEN for {host} (half-open probe in progress)"

            remaining = int(until - now)
            return False, f"Circuit OPEN for {host} — {remaining}s until probe"

    def record_success(self, url: str) -> None:
        """Reset the circuit to CLOSED on a successful delivery."""
        host = _normalise_url(url)
        with self._lock:
            was_open = host in self._open_until
            self._failures.pop(host, None)
            self._open_until.pop(host, None)
            self._probing.pop(host, None)
        if was_open:
            logger.info(f"[circuit] CLOSED (recovered) for {host}")

    def record_failure(self, url: str) -> None:
        """Record a delivery failure; open the circuit if threshold exceeded."""
        host = _normalise_url(url)
        with self._lock:
            now = time.monotonic()

            # Half-open probe failed → re-open immediately
            if self._probing.pop(host, False):
                self._open_until[host] = now + self.recovery_seconds
                logger.warning(
                    f"[circuit] Half-open probe FAILED for {host} — "
                    f"re-opened for {self.recovery_seconds}s"
                )
                return

            # Circuit already open → extend the recovery timer
            if host in self._open_until:
                self._open_until[host] = now + self.recovery_seconds
                return

            # CLOSED — accumulate failure, evict stale timestamps
            if host not in self._failures:
                self._failures[host] = deque()
            bucket = self._failures[host]
            while bucket and now - bucket[0] > self.window_seconds:
                bucket.popleft()
            bucket.append(now)

            if len(bucket) >= self.failure_threshold:
                self._open_until[host] = now + self.recovery_seconds
                metrics_module.circuit_breaker_open_total.labels(
                    target_url=host
                ).inc()
                logger.warning(
                    f"[circuit] OPENED for {host} — "
                    f"{len(bucket)} failures in {self.window_seconds}s. "
                    f"Blocking for {self.recovery_seconds}s."
                )


# ── Module-level singleton ────────────────────────────────────────────────────
# Shared across all worker threads.  Initialised from settings at import time.

from config import settings  # noqa: E402 (import after class definition)

circuit_breaker = CircuitBreaker(
    failure_threshold=settings.circuit_breaker_threshold,
    window_seconds=settings.circuit_breaker_window_seconds,
    recovery_seconds=settings.circuit_breaker_recovery_seconds,
)
