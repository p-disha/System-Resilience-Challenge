import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Index, Integer, String, Text, text as sa_text
from sqlalchemy.dialects.postgresql import JSONB, UUID

from database import Base


class WebhookJob(Base):
    __tablename__ = "webhook_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    event_type = Column(String(255), nullable=False)
    payload = Column(JSONB, nullable=False)
    target_url = Column(Text, nullable=False)

    # Optional client-supplied key for at-most-once ingestion.
    # Partial unique index below enforces uniqueness only for non-null values.
    idempotency_key = Column(String(255), nullable=True)

    # Delivery state machine: pending → processing → delivered | failed
    status = Column(String(20), nullable=False, default="pending")
    attempt_count = Column(Integer, nullable=False, default=0)
    max_attempts = Column(Integer, nullable=False, default=10)

    # Scheduling column: worker only picks rows where next_attempt_at <= NOW().
    # Encodes the retry delay directly — no separate schedule table needed.
    next_attempt_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    # Separate from updated_at so it only tracks actual delivery attempts.
    last_attempt_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        # Compound index: makes the worker's SELECT FOR UPDATE SKIP LOCKED query O(log n)
        # instead of a full table scan as the jobs table grows.
        Index("ix_webhook_jobs_status_next_attempt", "status", "next_attempt_at"),

        # Partial unique index: idempotency_key uniqueness is only enforced
        # when the column is non-null.  This allows many jobs without a key
        # while still preventing double-submission for keyed events.
        Index(
            "ix_webhook_jobs_idempotency_key",
            "idempotency_key",
            unique=True,
            postgresql_where=sa_text("idempotency_key IS NOT NULL"),
        ),
    )

    def __repr__(self):
        return (
            f"<WebhookJob id={self.id} status={self.status} "
            f"attempt={self.attempt_count}/{self.max_attempts}>"
        )
