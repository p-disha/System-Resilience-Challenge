"""Shared constants — single source of truth for job statuses.

Define status strings here so that main.py, worker.py, and any future modules
all reference the same values.  Adding a new status (e.g. 'suppressed' for a
circuit-breaker hold) only requires a change here.
"""

STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_DELIVERED = "delivered"
STATUS_FAILED = "failed"

# Exhaustive set used for API validation (list endpoint status filter).
VALID_STATUSES: frozenset[str] = frozenset(
    {STATUS_PENDING, STATUS_PROCESSING, STATUS_DELIVERED, STATUS_FAILED}
)
