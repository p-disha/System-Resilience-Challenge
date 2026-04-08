"""Prometheus metrics for the webhook dispatcher.

All metrics are module-level singletons registered with the default registry.
Import and mutate them from worker.py and main.py.

Scrape endpoint: GET /metrics  (registered in main.py)
Typical Prometheus scrape interval: 15s.

Metric naming follows the Prometheus convention:
  <namespace>_<subsystem>_<name>_<unit>
  namespace = "webhook"
"""

from prometheus_client import Counter, Gauge, Histogram

# ── Ingestion ─────────────────────────────────────────────────────────────────

jobs_enqueued_total = Counter(
    "webhook_jobs_enqueued_total",
    "Total webhook jobs accepted via POST /events.",
    ["event_type"],
)

# ── Delivery outcomes ─────────────────────────────────────────────────────────

jobs_delivered_total = Counter(
    "webhook_jobs_delivered_total",
    "Total jobs successfully delivered to the target URL.",
)

jobs_failed_total = Counter(
    "webhook_jobs_failed_total",
    "Total jobs permanently failed after exhausting all retry attempts.",
)

jobs_retried_total = Counter(
    "webhook_jobs_retried_total",
    "Total retry attempts (delivery attempts beyond the first for a job).",
)

jobs_replayed_total = Counter(
    "webhook_jobs_replayed_total",
    "Total failed jobs manually reset to pending via POST /jobs/{id}/retry.",
)

delivery_duration_seconds = Histogram(
    "webhook_delivery_duration_seconds",
    "HTTP round-trip latency for actual delivery attempts (excludes circuit-blocked jobs).",
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

# ── Circuit breaker ───────────────────────────────────────────────────────────

circuit_breaker_open_total = Counter(
    "webhook_circuit_breaker_open_total",
    "Times a circuit breaker tripped open for a target URL.",
    ["target_url"],
)

circuit_breaker_blocked_total = Counter(
    "webhook_circuit_breaker_blocked_total",
    "Delivery attempts skipped because the circuit breaker is open.",
    ["target_url"],
)

# ── Queue depth (updated by watchdog every 30s) ───────────────────────────────

queue_depth_pending = Gauge(
    "webhook_queue_depth_pending",
    "Current number of jobs with status=pending waiting for delivery.",
)

queue_depth_processing = Gauge(
    "webhook_queue_depth_processing",
    "Current number of jobs with status=processing (actively being delivered).",
)

# ── Worker health ─────────────────────────────────────────────────────────────

worker_consecutive_errors = Gauge(
    "webhook_worker_consecutive_errors",
    "Current count of consecutive unhandled errors per worker thread.",
    ["worker_id"],
)
