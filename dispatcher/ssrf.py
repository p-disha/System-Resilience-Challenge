"""SSRF protection — shared between the ingestion API and the delivery worker.

The check runs in two places:
  1. POST /events (main.py)  — fast-fail at enqueue time for obvious bad URLs.
  2. deliver_job  (worker.py) — re-checked at delivery time to defend against
     DNS-rebinding attacks where a public hostname is later pointed at a private IP.

Design:
  ssrf_violation(url) → str | None
    Returns an error message if the URL is disallowed, None if it is safe.
    Callers decide what to do with the violation (raise HTTP 400 or fail the job).
"""

import ipaddress
import socket as stdlib_socket
from urllib.parse import urlparse

from config import settings


def ssrf_violation(url: str) -> str | None:
    """Return a violation message if *url* resolves to a private/reserved IP.

    Returns ``None`` when:
    - ``ALLOW_PRIVATE_TARGETS=true`` (Docker / test environments), or
    - the hostname resolves to a public IP address, or
    - DNS resolution fails (let the HTTP call fail naturally).

    Returns an error string when the hostname is a known-dangerous literal
    (``localhost``, ``0.0.0.0``, ``::1``) or resolves to any private,
    loopback, link-local, or reserved address.
    """
    if settings.allow_private_targets:
        return None

    parsed = urlparse(url)
    hostname = parsed.hostname
    if not hostname:
        return None

    # Reject hardcoded dangerous literals immediately (no DNS lookup needed).
    if hostname.lower() in ("localhost", "::1", "0.0.0.0"):
        return f"SSRF protection: hostname '{hostname}' is not allowed."

    try:
        _, _, ip_strings = stdlib_socket.gethostbyname_ex(hostname)
        for ip_str in ip_strings:
            ip = ipaddress.ip_address(ip_str)
            if (
                ip.is_private
                or ip.is_loopback
                or ip.is_link_local
                or ip.is_reserved
                or ip.is_unspecified
            ):
                return (
                    f"SSRF protection: '{hostname}' resolves to "
                    f"private/reserved IP {ip_str}."
                )
    except stdlib_socket.gaierror:
        # DNS failure — let the delivery attempt handle it.
        pass

    return None
