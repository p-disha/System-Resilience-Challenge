# Webhook Dispatcher

At-least-once webhook delivery engine for a Fintech platform.

**Guarantees:** Messages survive service crashes · Exponential backoff with jitter · HMAC-SHA256 signing · Idempotent ingestion

---

## Quick Start

```bash
cp .env.example .env
docker-compose up --build
```

| Service | Port | Description |
|---|---|---|
| dispatcher | 8000 | FastAPI ingestion API + background delivery workers |
| mock-receiver | 5001 | Chaotic endpoint — 70% failure rate |
| postgres | 5432 | Persistent job queue (survives restarts) |

---

## API Reference

### POST /events — Enqueue a webhook

```bash
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "payment.captured",
    "payload": {"transaction_id": "TXN-001", "amount": 149.99},
    "idempotency_key": "unique-key-per-event"
  }'
```

Response (202 Accepted):
```json
{"job_id": "3f7c1a2b-...", "status": "pending", "message": "Enqueued for delivery to ..."}
```

### GET /jobs/{job_id} — Poll delivery status

```bash
curl http://localhost:8000/jobs/3f7c1a2b-...
```

### GET /jobs?status=pending — List jobs with filter

```bash
curl "http://localhost:8000/jobs?status=failed&limit=20&offset=0"
```

### POST /jobs/{job_id}/retry — Re-enqueue a failed job

Reset a permanently failed job to pending with a fresh retry budget:

```bash
curl -X POST http://localhost:8000/jobs/3f7c1a2b-.../retry
```

Response (202):
```json
{"job_id": "3f7c1a2b-...", "status": "pending", "message": "Job reset to pending. Delivery will be attempted shortly."}
```

Returns HTTP 409 if the job is not in `failed` status.

### GET /stats — Queue statistics

```bash
curl http://localhost:8000/stats
```

### GET /health — Health check (probes database)

Returns 200 when both API and database are reachable. Returns 503 if the database is unreachable.

```bash
curl http://localhost:8000/health
```

### GET /metrics — Prometheus metrics

```bash
curl http://localhost:8000/metrics
```

Exposes counters (`jobs_enqueued_total`, `jobs_delivered_total`, `jobs_failed_total`, `jobs_retried_total`, `jobs_replayed_total`), histograms (`delivery_duration_seconds`), gauges (`queue_depth_pending`, `queue_depth_processing`), and circuit-breaker counters.

---

## Running the Backoff Demo

The demo script sends one event and polls until it's delivered, printing each failed attempt and its retry delay:

```bash
# Terminal 1 — start services
docker-compose up --build

# Terminal 2 — run demo (FORCE_FAIL_COUNT=5 means exactly 5 failures then success)
bash demo.sh
```

Expected output:
```
12:00:00 [demo]  Sending POST /events ...
12:00:00 [✓]    Event enqueued. Job ID: abc-123
12:00:02 [!]    Attempt 1/10 FAILED | next_retry=12:00:04 | error: HTTP 500: Forced failure 1/5
12:00:04 [!]    Attempt 2/10 FAILED | next_retry=12:00:08 | error: HTTP 500: Forced failure 2/5
12:00:08 [!]    Attempt 3/10 FAILED | next_retry=12:00:16 | error: HTTP 500: Forced failure 3/5
12:00:16 [!]    Attempt 4/10 FAILED | next_retry=12:00:32 | error: HTTP 500: Forced failure 4/5
12:00:32 [!]    Attempt 5/10 FAILED | next_retry=12:01:04 | error: HTTP 500: Forced failure 5/5
12:01:04 [✓]    DELIVERED on attempt 6/10
```

**Watch the full backoff sequence in the dispatcher logs:**
```bash
docker-compose logs -f dispatcher
docker-compose logs -f mock-receiver
```

### Mock receiver behaviour

| Mode | Configuration | Behaviour |
|---|---|---|
| Demo (default) | `FORCE_FAIL_COUNT=5` | First 5 attempts per event always fail with 500, then always succeed |
| Chaos | `FORCE_FAIL_COUNT=0` | 40% HTTP 500, 20% connection drop, 10% HTTP 503, 30% success |

---

## Crash Recovery Demo

Proves that no message is lost when the process is killed mid-retry:

```bash
# 1. Enqueue a job
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"event_type": "payment.captured", "payload": {"txn": "TXN-999"}}'

# 2. Kill the dispatcher while it's waiting to retry
docker-compose kill dispatcher

# 3. The job is still in PostgreSQL (volume persists)
docker exec $(docker-compose ps -q postgres) \
  psql -U dispatcher -d webhooks \
  -c "SELECT id, status, attempt_count, next_attempt_at FROM webhook_jobs;"

# 4. Restart — dispatcher resets 'processing' → 'pending' on startup
docker-compose start dispatcher

# 5. Watch it resume
docker-compose logs -f dispatcher
```

---

## Architecture

```
POST /events  ──[rate limit: 200/min]──►  FastAPI (main.py)
                                               │  INSERT + notify_job_enqueued()
                                               ▼
                            webhook_jobs table (PostgreSQL) ◄── persistent queue
                                               │
                        ┌──────────────────────┴───────────────────┐
                        ▼                                           ▼
               Worker threads (×3)                        Watchdog thread (×1)
               SELECT FOR UPDATE                          • resets stuck jobs
               SKIP LOCKED                               • refreshes queue-depth
                        │                                  gauges every 30s
                        ▼
               Circuit breaker check
               (per scheme+host)
                        │ allowed
                        ▼
               TX 1: claim → 'processing' → COMMIT  (lock released)
                        │
               HTTP POST to target (10s timeout)  ← no DB connection held
                        │
               TX 2: write result → COMMIT
                 2xx → 'delivered'
                 5xx → 'pending' + next_attempt_at = NOW() + backoff(attempt)
                 exhausted → 'failed'
```

---

## Engineering Decisions

### Why PostgreSQL as the Queue?

No Redis, no RabbitMQ. The `webhook_jobs` table **is** the queue.
- **Durability**: ACID guarantees — a job inserted is never silently lost.
- **Visibility**: Standard SQL tools can inspect, query, and manually requeue jobs.
- **Simplicity**: One fewer service to operate and monitor.
- **`SELECT FOR UPDATE SKIP LOCKED`**: Built-in PostgreSQL primitive for concurrent, lock-free job claiming. No advisory locks, no external coordination.

### Crash Recovery (At-Least-Once)

```python
# On every startup:
UPDATE webhook_jobs SET status = 'pending' WHERE status = 'processing'
```

If the worker process is killed while a job is `processing`, the job is returned to `pending` on the next startup. This guarantees delivery at least once. The receiver's idempotency key (`X-Webhook-Event-Id` header) lets merchants deduplicate.

### Exponential Backoff — No Thundering Herd

```
delay = base × 2^(attempt-1), capped at max_delay
final = delay ± (delay × jitter_factor × random)
```

The `±20%` jitter is the key. Without it, a burst of 100 failed jobs would all retry at exactly the same second — overwhelming the target the moment it comes back. With jitter, retries spread across a window proportional to the delay.

### Two-Transaction Delivery Pattern

The worker uses **two separate database transactions** per delivery:

- **TX 1**: Claim job (SELECT FOR UPDATE SKIP LOCKED → mark 'processing' → COMMIT)
  - Lock released immediately. Other workers can process other jobs concurrently.
- **TX 2**: Do the HTTP call → update result → COMMIT
  - No DB connection is held during the HTTP call (up to 10s per timeout).

Without this split, every worker thread would hold a pool connection open during the HTTP call. With `WORKER_CONCURRENCY=10` and `REQUEST_TIMEOUT_SECONDS=10`, the pool would be exhausted and new jobs couldn't be claimed.

### HMAC-SHA256 — Stripe-Style Signing

Signing scheme:
```
signed_content = f"{unix_timestamp}.{raw_json_body}"
signature = HMAC-SHA256(key=WEBHOOK_SECRET, msg=signed_content)
header: X-Webhook-Signature: sha256=<hex>
header: X-Webhook-Timestamp: <unix_seconds>
```

Including the timestamp in the signed content prevents replay attacks: a captured HTTP request cannot be replayed more than 5 minutes later because the timestamp will be stale.

`hmac.compare_digest` is used for all signature comparisons to prevent timing side-channel attacks.

### Circuit Breaker

A per-target circuit breaker (keyed by `scheme://host`) prevents hammering a known-failed endpoint:

| State | Behaviour |
|-------|-----------|
| **CLOSED** | All requests pass through normally |
| **OPEN** | Jobs are rescheduled by `circuit_breaker_recovery_seconds` without consuming a retry attempt |
| **HALF-OPEN** | One probe is allowed after the recovery window; success → CLOSED, failure → OPEN again |

The threshold defaults to `10` failures within `60` seconds — deliberately above `FORCE_FAIL_COUNT=5` so the demo doesn't accidentally trip the breaker.

### Rate Limiting

`POST /events` is limited to **200 requests/minute per client IP** via `slowapi`. Excess requests receive `HTTP 429`. All other endpoints (status, health, metrics) are unlimited.

### Observability

- **Structured JSON logging** — every log line is a JSON object (`python-json-logger`), ready for Datadog, ELK, or any JSON log aggregator.
- **Prometheus metrics** — scraped at `GET /metrics`:
  - `webhook_jobs_enqueued_total[event_type]` — ingestion throughput per event type
  - `webhook_jobs_delivered_total` / `webhook_jobs_failed_total` / `webhook_jobs_retried_total`
  - `webhook_delivery_duration_seconds` — histogram of HTTP round-trip times
  - `webhook_queue_depth_pending` / `webhook_queue_depth_processing` — live queue depth gauges
  - `webhook_circuit_breaker_open_total[target_url]` / `webhook_circuit_breaker_blocked_total[target_url]`

### Manual Replay

`POST /jobs/{id}/retry` resets a permanently failed job to `pending` with a fresh retry budget (`attempt_count=0`, `max_attempts` reset to current config). Workers pick it up within milliseconds via event-driven wakeup.

### Idempotent Ingestion

Supply `idempotency_key` to prevent duplicate jobs on client retry:

```json
{"event_type": "payment.captured", "payload": {...}, "idempotency_key": "txn-001-captured"}
```

If a job with that key already exists, the existing job's ID and status are returned — no duplicate delivery. Implemented as a partial unique index in PostgreSQL (`WHERE idempotency_key IS NOT NULL`) so jobs without a key are unaffected.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | required | PostgreSQL connection string |
| `WEBHOOK_SECRET` | required | HMAC signing key (≥32 chars; shared with receiver) |
| `TARGET_URL` | required | Default webhook delivery endpoint |
| `WORKER_CONCURRENCY` | `3` | Parallel delivery threads |
| `POLL_INTERVAL_SECONDS` | `2` | Max idle wait between polls (woken instantly on new job) |
| `MAX_ATTEMPTS` | `10` | Max delivery attempts before marking `failed` |
| `REQUEST_TIMEOUT_SECONDS` | `10` | Per-request HTTP delivery timeout |
| `BASE_DELAY_SECONDS` | `2` | First retry delay (doubles each attempt) |
| `MAX_DELAY_SECONDS` | `3600` | Backoff delay cap (1 hour) |
| `JITTER_FACTOR` | `0.20` | ±20% delay randomisation |
| `MAX_PAYLOAD_BYTES` | `1048576` | Reject request bodies larger than this (1 MB) |
| `ALLOW_PRIVATE_TARGETS` | `false` | Set `true` only in test environments (SSRF protection) |
| `CORS_ORIGINS` | `""` | Comma-separated origins, or `*`; empty = CORS disabled |
| `CIRCUIT_BREAKER_THRESHOLD` | `10` | Failures within window before circuit opens |
| `CIRCUIT_BREAKER_WINDOW_SECONDS` | `60` | Failure counting window |
| `CIRCUIT_BREAKER_RECOVERY_SECONDS` | `300` | How long circuit stays open before half-open probe |
| `PROCESSING_JOB_TIMEOUT_SECONDS` | `300` | Watchdog: reset stuck `processing` jobs after this long |
| `IDEMPOTENCY_TTL_HOURS` | `24` | Hours an idempotency key is honoured |
| `FORCE_FAIL_COUNT` | `5` | Mock receiver: force N failures then succeed (0 = pure chaos) |

---

## Deliverables

### Live Test Run — 2026-04-08

Full log: [`test-run.log`](test-run.log)

All tests ran on Docker Compose (Windows 11 / Docker Desktop 29.2.1) with the stack:
- `dispatcher` — FastAPI v2.0.0 + 3 worker threads + 1 watchdog + circuit breaker + Prometheus metrics
- `postgres:15` — persistent job queue
- `mock-receiver` — Flask, `FORCE_FAIL_COUNT=5` (first 5 attempts always return HTTP 500)

---

### Backoff Sequence (Job `48ffb20e`)

Event `payment.captured` with `idempotency_key=demo-run-001`:

```
[ATTEMPT 1]  10:37:55  → HTTP 500  retry in  2.0s
[ATTEMPT 2]  10:37:57  → HTTP 500  retry in  4.1s
[ATTEMPT 3]  10:38:03  → HTTP 500  retry in  8.5s
[ATTEMPT 4]  10:38:13  → HTTP 500  retry in 12.8s
[ATTEMPT 5]  10:38:27  → HTTP 500  retry in 28.3s
[ATTEMPT 6]  10:38:56  → HTTP 200  DELIVERED ✓
```

| Attempt | Base delay | Actual delay (±20% jitter) | Result |
|---------|-----------|---------------------------|--------|
| 1 | 2.0s | 2.0s | FAIL HTTP 500 |
| 2 | 4.0s | 4.1s | FAIL HTTP 500 |
| 3 | 8.0s | 8.5s | FAIL HTTP 500 |
| 4 | 16.0s | 12.8s | FAIL HTTP 500 |
| 5 | 32.0s | 28.3s | FAIL HTTP 500 |
| 6 | — | — | SUCCESS HTTP 200 |

Total time from enqueue to delivery: **~61 seconds**

---

### Crash Recovery (Job `18ba35fd`)

Proves no message is lost when the process is killed mid-retry:

```
10:39:51  Attempt 1 FAILED  retry in  1.8s
10:39:53  Attempt 2 FAILED  retry in  3.6s
10:39:57  Attempt 3 FAILED  retry in  8.0s  ← docker compose kill dispatcher

[PostgreSQL direct query while dispatcher is dead]
id=18ba35fd  status=pending  attempt_count=3  next_attempt_at=10:40:05

[Dispatcher restarted at 10:40:12]
10:40:13  Attempt 4 FAILED  retry in 15.6s
10:40:28  Attempt 5 FAILED  retry in 29.9s
10:41:00  Attempt 6 SUCCESS → DELIVERED ✓
```

Job survived process kill with zero data loss. Dispatcher resumed from exactly where it left off.

---

### Test Results

| Test | Result |
|------|--------|
| Backoff sequence (2s→4.1s→8.5s→12.8s→28.3s, ±20% jitter) | PASS |
| Delivered on attempt 6 after 5 forced failures | PASS |
| Idempotency — duplicate key returns same job_id | PASS |
| Invalid UUID path param → HTTP 422 | PASS |
| Bad URL scheme (`ftp://`) → HTTP 422 | PASS |
| Payload > 1 MB → HTTP 413 | PASS |
| Queue stats correct after delivery | PASS |
| List jobs with status filter | PASS |
| Crash recovery — job survives process kill, resumes after restart | PASS |
| Manual retry — failed job reset, workers re-deliver | PASS |
| Retry guard — non-failed job → HTTP 409 | PASS |
| Prometheus /metrics — all counters, histograms, gauges exported | PASS |
| HMAC signature verified on all 18 attempts (0 rejections) | PASS |

**13/13 tests passed.**

Jobs delivered: 3 · Total attempts: 18 · Signature rejections: 0 · Jobs lost to process kill: 0 · Avg delivery latency: 7.4ms

---

## Database Schema

```sql
CREATE TABLE webhook_jobs (
    id               UUID PRIMARY KEY,
    event_type       VARCHAR(255)  NOT NULL,
    payload          JSONB         NOT NULL,
    target_url       TEXT          NOT NULL,
    idempotency_key  VARCHAR(255),          -- optional, unique when set
    status           VARCHAR(20)   NOT NULL DEFAULT 'pending',
    attempt_count    INTEGER       NOT NULL DEFAULT 0,
    max_attempts     INTEGER       NOT NULL DEFAULT 10,
    next_attempt_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    last_attempt_at  TIMESTAMPTZ,
    last_error       TEXT,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Makes worker polling O(log n) instead of full table scan
CREATE INDEX ix_webhook_jobs_status_next_attempt
    ON webhook_jobs (status, next_attempt_at);

-- Deduplication index (partial — only enforced for non-null keys)
CREATE UNIQUE INDEX ix_webhook_jobs_idempotency_key
    ON webhook_jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
```
