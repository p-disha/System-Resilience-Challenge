#!/usr/bin/env bash
# demo.sh — Proves at-least-once delivery with exponential backoff.
#
# What this script does:
#   1. Waits for the dispatcher API to be healthy.
#   2. Sends a POST /events with a unique idempotency_key.
#   3. Polls GET /jobs/{id} every 2 seconds and prints status updates.
#   4. Exits when the job is delivered or permanently failed.
#
# Expected behaviour (with FORCE_FAIL_COUNT=5 in docker-compose.yml):
#   Attempt 1 → FAIL  (mock-receiver returns 500, forced)
#   Attempt 2 → FAIL  retry after ~2s
#   Attempt 3 → FAIL  retry after ~4s
#   Attempt 4 → FAIL  retry after ~8s
#   Attempt 5 → FAIL  retry after ~16s
#   Attempt 6 → SUCCESS (mock-receiver force-succeeds after 5 forced fails)
#
# Usage:
#   docker-compose up -d          # start all services
#   bash demo.sh                  # run the demo in another terminal
#
# OR watch live logs alongside:
#   docker-compose logs -f dispatcher mock-receiver

set -euo pipefail

API="http://localhost:8000"
COLOR_GREEN="\033[0;32m"
COLOR_YELLOW="\033[0;33m"
COLOR_RED="\033[0;31m"
COLOR_CYAN="\033[0;36m"
COLOR_RESET="\033[0m"

log()    { echo -e "$(date '+%H:%M:%S') ${COLOR_CYAN}[demo]${COLOR_RESET} $*"; }
ok()     { echo -e "$(date '+%H:%M:%S') ${COLOR_GREEN}[✓]${COLOR_RESET}   $*"; }
warn()   { echo -e "$(date '+%H:%M:%S') ${COLOR_YELLOW}[!]${COLOR_RESET}   $*"; }
fail()   { echo -e "$(date '+%H:%M:%S') ${COLOR_RED}[✗]${COLOR_RESET}   $*"; }

# ── 1. Wait for API ───────────────────────────────────────────────────────────
log "Waiting for dispatcher API to be healthy..."
for i in $(seq 1 30); do
    if curl -sf "${API}/health" > /dev/null 2>&1; then
        ok "API is up."
        break
    fi
    if [ "$i" -eq 30 ]; then
        fail "API did not become healthy after 30 attempts. Is docker-compose up running?"
        exit 1
    fi
    echo -n "."
    sleep 2
done
echo

# ── 2. Send a test event ──────────────────────────────────────────────────────
IDEMPOTENCY_KEY="demo-$(date +%s)"
log "Sending POST /events with idempotency_key=${IDEMPOTENCY_KEY}..."

RESPONSE=$(curl -sf -X POST "${API}/events" \
    -H "Content-Type: application/json" \
    -d "{
        \"event_type\": \"payment.captured\",
        \"payload\": {
            \"transaction_id\": \"TXN-$(date +%s)\",
            \"amount\": 149.99,
            \"currency\": \"USD\",
            \"merchant_id\": \"merchant-001\"
        },
        \"idempotency_key\": \"${IDEMPOTENCY_KEY}\"
    }")

JOB_ID=$(echo "$RESPONSE" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)

if [ -z "$JOB_ID" ]; then
    fail "Failed to enqueue event. Response: $RESPONSE"
    exit 1
fi

ok "Event enqueued. Job ID: ${COLOR_CYAN}${JOB_ID}${COLOR_RESET}"
log "Polling GET /jobs/${JOB_ID} every 2s until delivered or failed..."
echo

# ── 3. Poll for status ────────────────────────────────────────────────────────
PREV_ATTEMPTS=0

while true; do
    STATUS_JSON=$(curl -sf "${API}/jobs/${JOB_ID}" 2>/dev/null || echo '{}')

    JOB_STATUS=$(echo "$STATUS_JSON"    | grep -o '"status":"[^"]*"'       | cut -d'"' -f4)
    ATTEMPTS=$(echo "$STATUS_JSON"      | grep -o '"attempt_count":[0-9]*' | grep -o '[0-9]*')
    MAX_ATT=$(echo "$STATUS_JSON"       | grep -o '"max_attempts":[0-9]*'  | grep -o '[0-9]*')
    LAST_ERROR=$(echo "$STATUS_JSON"    | grep -o '"last_error":"[^"]*"'   | cut -d'"' -f4)
    NEXT_RETRY=$(echo "$STATUS_JSON"    | grep -o '"next_attempt_at":"[^"]*"' | cut -d'"' -f4)

    ATTEMPTS=${ATTEMPTS:-0}
    JOB_STATUS=${JOB_STATUS:-unknown}

    # Print a line whenever attempt_count increments
    if [ "$ATTEMPTS" -gt "$PREV_ATTEMPTS" ]; then
        PREV_ATTEMPTS=$ATTEMPTS
        if [ "$JOB_STATUS" = "pending" ] || [ "$JOB_STATUS" = "processing" ]; then
            warn "Attempt ${ATTEMPTS}/${MAX_ATT} FAILED | next_retry=${NEXT_RETRY} | error=${LAST_ERROR}"
        fi
    fi

    if [ "$JOB_STATUS" = "delivered" ]; then
        echo
        ok "══════════════════════════════════════════════"
        ok "  DELIVERED on attempt ${ATTEMPTS}/${MAX_ATT}"
        ok "══════════════════════════════════════════════"
        echo
        log "Final job state:"
        echo "$STATUS_JSON" | python3 -m json.tool 2>/dev/null || echo "$STATUS_JSON"
        break
    fi

    if [ "$JOB_STATUS" = "failed" ]; then
        echo
        fail "══════════════════════════════════════════════"
        fail "  PERMANENTLY FAILED after ${ATTEMPTS} attempts"
        fail "  Last error: ${LAST_ERROR}"
        fail "══════════════════════════════════════════════"
        echo
        log "Final job state:"
        echo "$STATUS_JSON" | python3 -m json.tool 2>/dev/null || echo "$STATUS_JSON"
        exit 1
    fi

    sleep 2
done

# ── 4. Show receiver stats ────────────────────────────────────────────────────
echo
log "Mock receiver stats:"
curl -sf "http://localhost:5001/stats" | python3 -m json.tool 2>/dev/null \
    || curl -sf "http://localhost:5001/stats"
echo

# ── 5. Verify idempotency — send the same key again ──────────────────────────
log "Verifying idempotency — re-submitting the same idempotency_key..."
IDEMPOTENT_RESPONSE=$(curl -sf -X POST "${API}/events" \
    -H "Content-Type: application/json" \
    -d "{
        \"event_type\": \"payment.captured\",
        \"payload\": {\"duplicate\": true},
        \"idempotency_key\": \"${IDEMPOTENCY_KEY}\"
    }")

RETURNED_JOB_ID=$(echo "$IDEMPOTENT_RESPONSE" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)
RETURNED_STATUS=$(echo "$IDEMPOTENT_RESPONSE" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)

if [ "$RETURNED_JOB_ID" = "$JOB_ID" ]; then
    ok "Idempotency works: same job_id returned (${RETURNED_JOB_ID}), status=${RETURNED_STATUS}"
else
    fail "Idempotency broken! Original=${JOB_ID} but got ${RETURNED_JOB_ID}"
fi

echo
ok "Demo complete. Watch the full backoff sequence in the logs:"
echo "    docker-compose logs --no-log-prefix dispatcher | grep '${JOB_ID:0:8}'"
