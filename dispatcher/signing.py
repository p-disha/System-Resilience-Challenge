"""HMAC-SHA256 webhook signing and verification.

Signing scheme (inspired by Stripe webhooks):
  signed_payload = f"{unix_timestamp}.{raw_json_body}"
  signature = HMAC-SHA256(key=WEBHOOK_SECRET, msg=signed_payload)
  header: X-Webhook-Signature: sha256=<hex_digest>
  header: X-Webhook-Timestamp: <unix_timestamp>

Including the timestamp in the signed payload prevents replay attacks:
a captured request cannot be replayed after max_age_seconds because the
timestamp will be outside the acceptable window.
"""

import hashlib
import hmac
import time

from config import settings


def compute_signature(payload_bytes: bytes, timestamp: int) -> str:
    """Compute HMAC-SHA256 over timestamp-prefixed payload.

    Args:
        payload_bytes: Raw JSON bytes of the request body.
        timestamp: Unix timestamp (seconds) to include in signed content.

    Returns:
        Lowercase hex string of the HMAC-SHA256 digest.
    """
    signed_content = f"{timestamp}.".encode("utf-8") + payload_bytes
    return hmac.new(
        key=settings.webhook_secret.encode("utf-8"),
        msg=signed_content,
        digestmod=hashlib.sha256,
    ).hexdigest()


def build_signature_headers(payload_bytes: bytes) -> dict[str, str]:
    """Build X-Webhook-Timestamp and X-Webhook-Signature headers.

    Args:
        payload_bytes: Raw JSON bytes that will be sent as the request body.

    Returns:
        Dict with the two signature-related headers to attach to the request.
    """
    timestamp = int(time.time())
    signature = compute_signature(payload_bytes, timestamp)
    return {
        "X-Webhook-Timestamp": str(timestamp),
        "X-Webhook-Signature": f"sha256={signature}",
    }


def verify_signature(
    payload_bytes: bytes,
    timestamp_header: str,
    signature_header: str,
    max_age_seconds: int = 300,
) -> bool:
    """Verify a received webhook's HMAC signature and timestamp freshness.

    Args:
        payload_bytes: Raw request body bytes.
        timestamp_header: Value of X-Webhook-Timestamp header.
        signature_header: Value of X-Webhook-Signature header.
        max_age_seconds: Reject requests older than this (replay protection).

    Returns:
        True if the signature is valid and the timestamp is fresh.
    """
    try:
        timestamp = int(timestamp_header)
    except (ValueError, TypeError):
        return False

    age = int(time.time()) - timestamp
    if age > max_age_seconds or age < -60:  # -60s allowance for clock skew
        return False

    expected_sig = signature_header.removeprefix("sha256=")
    actual_sig = compute_signature(payload_bytes, timestamp)

    # hmac.compare_digest prevents timing-side-channel attacks
    return hmac.compare_digest(expected_sig, actual_sig)
