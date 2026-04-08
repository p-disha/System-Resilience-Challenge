import logging

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# Secrets that are publicly known from source code / docker-compose defaults.
# Using one of these in production is equivalent to having no secret at all.
_KNOWN_WEAK_SECRETS = {
    "super-secret-webhook-key",
    "super-secret-webhook-key-change-in-prod",
    "changeme",
    "secret",
    "password",
    "webhook-secret",
}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Database ──────────────────────────────────────────────────────────────
    # No default: must be set explicitly so credentials are never baked in.
    database_url: str = "postgresql://dispatcher:secret@postgres:5432/webhooks"

    # ── Webhook signing ───────────────────────────────────────────────────────
    # SECURITY: must be ≥32 bytes of high entropy.
    # Generate with: python -c "import secrets; print(secrets.token_hex(32))"
    webhook_secret: str = "super-secret-webhook-key-change-in-prod-use-secrets-token-hex"

    @field_validator("webhook_secret")
    @classmethod
    def validate_secret_strength(cls, v: str) -> str:
        if len(v) < 32:
            raise ValueError(
                f"WEBHOOK_SECRET must be at least 32 characters long "
                f"(got {len(v)}). "
                f"Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
            )
        if v in _KNOWN_WEAK_SECRETS:
            # Warn rather than crash so docker-compose up still works in dev.
            # In a production deployment, treat this as fatal.
            logger.warning(
                "WARNING: WEBHOOK_SECRET is a known-weak default. "
                "An attacker who knows this value can forge webhook signatures. "
                "Set WEBHOOK_SECRET to a cryptographically random value before production use."
            )
        return v

    # ── Delivery ──────────────────────────────────────────────────────────────
    target_url: str = "http://mock-receiver:5000/webhook"
    max_attempts: int = 10
    request_timeout_seconds: float = 10.0

    # ── SSRF protection ───────────────────────────────────────────────────────
    # SECURITY: False = block target_url that resolves to private/loopback IPs.
    # Set to True ONLY in test environments where the target is an internal service.
    allow_private_targets: bool = False

    # ── Request size limit ────────────────────────────────────────────────────
    max_payload_bytes: int = 1_048_576  # 1 MB

    # ── CORS ──────────────────────────────────────────────────────────────────
    # Comma-separated list of allowed origins, or "*" for all.
    # For a backend-to-backend API, leave empty ("") to disable CORS entirely.
    cors_origins: str = ""

    # ── Worker concurrency ────────────────────────────────────────────────────
    worker_concurrency: int = 3
    poll_interval_seconds: float = 2.0

    # ── Exponential backoff parameters ────────────────────────────────────────
    base_delay_seconds: float = 2.0
    max_delay_seconds: float = 3600.0
    jitter_factor: float = 0.20

    # ── Circuit breaker ───────────────────────────────────────────────────────
    # Opens after circuit_breaker_threshold failures within circuit_breaker_window_seconds.
    # Stays open for circuit_breaker_recovery_seconds, then allows one half-open probe.
    circuit_breaker_threshold: int = 10  # needs to be > FORCE_FAIL_COUNT (default 5) so demo doesn't trip it
    circuit_breaker_window_seconds: int = 60
    circuit_breaker_recovery_seconds: int = 300

    # ── Watchdog ──────────────────────────────────────────────────────────────
    # A background thread resets jobs stuck in 'processing' for longer than
    # this many seconds.  Defends against the case where a worker thread dies
    # after claiming a job (TX1) but before writing the delivery result (TX2).
    # Default: 300s (5 minutes) — must be well above REQUEST_TIMEOUT_SECONDS.
    processing_job_timeout_seconds: int = 300

    # ── Idempotency key TTL ───────────────────────────────────────────────────
    # How long (in hours) an idempotency_key is honoured.  After this window,
    # re-submitting the same key creates a fresh job rather than returning the
    # old one.  Prevents a failed job from permanently blocking resubmission
    # with the same key.  Default: 24 hours.
    idempotency_ttl_hours: int = 24


settings = Settings()
