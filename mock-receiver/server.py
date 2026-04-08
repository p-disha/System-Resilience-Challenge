"""Chaotic mock webhook receiver.

Simulates a terrible merchant endpoint with two operating modes:

MODE 1 — Random chaos (default, FORCE_FAIL_COUNT=0):
  40% → HTTP 500 (internal server error)
  20% → Connection drop (simulate server crash / TCP reset)
  10% → HTTP 503 (service unavailable)
  30% → HTTP 200 (success)

MODE 2 — Deterministic demo (FORCE_FAIL_COUNT=N, N > 0):
  First N delivery attempts for any given event_id → always HTTP 500.
  Attempt N+1 onwards → always HTTP 200.
  Use this to get a clean log showing exactly N failures then success.

Authentication:
  Every request is verified with HMAC-SHA256 + timestamp freshness check.
  Requests with an invalid or missing signature are rejected with 401.

Control endpoints:
  POST /reset  — clear counters and per-event state (between demo runs)
  GET  /stats  — delivery statistics
  GET  /health — health check

Environment:
  WEBHOOK_SECRET   — must match the dispatcher (default: same as docker-compose)
  FORCE_FAIL_COUNT — forced failures before success per event_id (default: 0)
"""

import hashlib
import hmac
import json
import logging
import os
import random
import threading
import time

from flask import Flask, jsonify, request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
)
logger = logging.getLogger("mock-receiver")

app = Flask(__name__)

WEBHOOK_SECRET = os.environ.get(
    "WEBHOOK_SECRET", "super-secret-webhook-key-change-in-prod-use-secrets-token-hex"
)
FORCE_FAIL_COUNT = int(os.environ.get("FORCE_FAIL_COUNT", "0"))

# Maximum number of per-event attempt entries to keep in memory.
# Prevents unbounded growth when processing many distinct event IDs over time.
# When the limit is reached, the oldest entries (by insertion order) are evicted.
_MAX_TRACKED_EVENTS = 50_000

# ── Thread-safe state ─────────────────────────────────────────────────────────

_lock = threading.Lock()

# Per-event attempt counter (keyed by X-Webhook-Event-Id).
# Python dicts preserve insertion order (3.7+), which lets us evict oldest entries.
_event_attempts: dict[str, int] = {}

_stats = {
    "total": 0,
    "success": 0,
    "failed_500": 0,
    "failed_503": 0,
    "failed_drop": 0,
    "rejected_sig": 0,
}


# ── HMAC signature verification ───────────────────────────────────────────────


def _verify_signature(body: bytes, timestamp_header: str, sig_header: str) -> bool:
    """Verify HMAC-SHA256(secret, f"{timestamp}.{body}") and timestamp freshness.

    Rejects:
    - Missing or non-integer timestamp
    - Requests older than 5 minutes (replay protection)
    - Requests from more than 60s in the future (clock skew allowance)
    - Signature mismatch
    """
    try:
        timestamp = int(timestamp_header)
    except (ValueError, TypeError):
        return False

    age = int(time.time()) - timestamp
    if age > 300 or age < -60:
        return False

    expected = sig_header.removeprefix("sha256=")
    signed_content = f"{timestamp}.".encode("utf-8") + body
    actual = hmac.new(
        key=WEBHOOK_SECRET.encode("utf-8"),
        msg=signed_content,
        digestmod=hashlib.sha256,
    ).hexdigest()

    # compare_digest is constant-time: prevents timing side-channel attacks.
    return hmac.compare_digest(expected, actual)


# ── Per-event attempt tracking ────────────────────────────────────────────────


def _increment_event_attempt(event_id: str) -> int:
    """Thread-safely increment and return the attempt count for an event.

    Also enforces the _MAX_TRACKED_EVENTS cap by evicting the oldest entries
    when the dict grows too large (FIFO order, not LRU).
    """
    with _lock:
        _event_attempts[event_id] = _event_attempts.get(event_id, 0) + 1
        local_attempt = _event_attempts[event_id]

        # Evict oldest entries if we've exceeded the cap.
        # dict.keys() is ordered by insertion time in Python 3.7+.
        overflow = len(_event_attempts) - _MAX_TRACKED_EVENTS
        if overflow > 0:
            oldest_keys = list(_event_attempts.keys())[:overflow]
            for k in oldest_keys:
                del _event_attempts[k]

    return local_attempt


# ── Connection-drop simulation ────────────────────────────────────────────────


def _drop_connection() -> None:
    """Abort the HTTP response without sending any headers or body.

    Simulates a server crash mid-request: the dispatcher's HTTP client will
    receive a RemoteProtocolError or ConnectionError (no HTTP response at all),
    which is distinct from a clean 5xx. This proves the dispatcher handles hard
    network failures, not just well-formed error responses.

    Implementation: Raising an unhandled exception inside a Flask route before
    any response data is written causes Werkzeug's threaded server to close the
    TCP connection without sending a response.
    """
    time.sleep(0.05)  # Short delay to simulate partial connection establishment
    raise ConnectionError("Simulated server crash — connection aborted")


# ── Webhook endpoint ──────────────────────────────────────────────────────────


@app.route("/webhook", methods=["POST"])
def receive_webhook():
    body = request.get_data()
    event_id = request.headers.get("X-Webhook-Event-Id", "unknown")
    event_type = request.headers.get("X-Webhook-Event-Type", "unknown")
    attempt_num = request.headers.get("X-Webhook-Attempt-Number", "?")
    ts_header = request.headers.get("X-Webhook-Timestamp", "")
    sig_header = request.headers.get("X-Webhook-Signature", "")

    with _lock:
        _stats["total"] += 1

    # ── Signature verification ────────────────────────────────────────────────
    if not _verify_signature(body, ts_header, sig_header):
        with _lock:
            _stats["rejected_sig"] += 1
        logger.warning(
            f"[{event_id}] attempt={attempt_num} → 401 REJECTED (invalid signature)"
        )
        return jsonify({"error": "Invalid signature"}), 401

    # ── Track per-event attempt count ─────────────────────────────────────────
    local_attempt = _increment_event_attempt(event_id)

    # ── Deterministic demo mode ───────────────────────────────────────────────
    if FORCE_FAIL_COUNT > 0:
        if local_attempt <= FORCE_FAIL_COUNT:
            with _lock:
                _stats["failed_500"] += 1
            logger.warning(
                f"[{event_id}] attempt={attempt_num} (local #{local_attempt}) "
                f"→ 500 FORCED FAIL ({local_attempt}/{FORCE_FAIL_COUNT})"
            )
            return jsonify({
                "error": "Simulated server error",
                "forced_fail": f"{local_attempt}/{FORCE_FAIL_COUNT}",
            }), 500
        else:
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                payload = {}
            with _lock:
                _stats["success"] += 1
            logger.info(
                f"[{event_id}] attempt={attempt_num} (local #{local_attempt}) "
                f"→ 200 SUCCESS (after {FORCE_FAIL_COUNT} forced failures) | "
                f"payload={json.dumps(payload)[:80]}"
            )
            return jsonify({"status": "received", "event_id": event_id}), 200

    # ── Random chaos mode ─────────────────────────────────────────────────────
    roll = random.random()

    if roll < 0.40:
        with _lock:
            _stats["failed_500"] += 1
        logger.warning(
            f"[{event_id}] attempt={attempt_num} type={event_type} "
            f"→ 500 SERVER ERROR (roll={roll:.2f})"
        )
        return jsonify({"error": "Internal server error"}), 500

    elif roll < 0.60:
        with _lock:
            _stats["failed_drop"] += 1
        logger.warning(
            f"[{event_id}] attempt={attempt_num} type={event_type} "
            f"→ CONNECTION DROP (roll={roll:.2f})"
        )
        _drop_connection()  # raises ConnectionError → Werkzeug closes TCP

    elif roll < 0.70:
        with _lock:
            _stats["failed_503"] += 1
        logger.warning(
            f"[{event_id}] attempt={attempt_num} type={event_type} "
            f"→ 503 SERVICE UNAVAILABLE (roll={roll:.2f})"
        )
        return jsonify({"error": "Service temporarily unavailable"}), 503

    else:
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {}
        with _lock:
            _stats["success"] += 1
            total = _stats["total"]
            success = _stats["success"]
        logger.info(
            f"[{event_id}] attempt={attempt_num} type={event_type} "
            f"→ 200 SUCCESS (roll={roll:.2f}) | "
            f"rate={success}/{total} ({100*success//max(total,1)}%) | "
            f"payload={json.dumps(payload)[:80]}"
        )
        return jsonify({"status": "received", "event_id": event_id}), 200


# ── Control / observability endpoints ─────────────────────────────────────────


@app.route("/reset", methods=["POST"])
def reset():
    """Clear all counters and per-event state. Use between demo runs."""
    with _lock:
        _event_attempts.clear()
        _stats.update({
            "total": 0, "success": 0,
            "failed_500": 0, "failed_503": 0,
            "failed_drop": 0, "rejected_sig": 0,
        })
    logger.info("State reset.")
    return jsonify({"status": "reset"}), 200


@app.route("/stats", methods=["GET"])
def stats():
    with _lock:
        snapshot = dict(_stats)
        snapshot["tracked_events"] = len(_event_attempts)
    total = snapshot["total"]
    snapshot["success_rate_pct"] = round(100 * snapshot["success"] / total, 1) if total else 0
    return jsonify(snapshot), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "mode": "demo" if FORCE_FAIL_COUNT else "chaos",
    }), 200


# ── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mode = (
        f"DEMO (FORCE_FAIL_COUNT={FORCE_FAIL_COUNT}: first {FORCE_FAIL_COUNT} "
        f"attempts per event always fail)"
        if FORCE_FAIL_COUNT
        else "CHAOS (40% 500 | 20% conn drop | 10% 503 | 30% success)"
    )
    logger.info(f"Mock Receiver starting — mode={mode}")
    # threaded=True is required so that the connection-drop simulation (which
    # raises an exception mid-request) doesn't block all other concurrent requests
    # on the single-threaded development server.
    app.run(host="0.0.0.0", port=5000, threaded=True)
