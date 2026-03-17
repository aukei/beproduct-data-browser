"""
BeProduct SDK client singleton with HTTP response interception for rate-limit header capture.

The BeProduct SDK uses the `requests` library internally. We monkey-patch the session's
`send()` method so every HTTP response is inspected for X-RateLimit-* headers without
modifying the SDK itself.

Rate limit status is stored in a module-level dict so it survives Streamlit reruns within
the same Python process.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional

import requests

# Lazy import of SDK - only fails at runtime if not installed
try:
    from beproduct.sdk import BeProduct  # type: ignore
except ImportError:
    BeProduct = None  # type: ignore

from app.config import settings


# ---------------------------------------------------------------------------
# Rate-limit state (module-level so it persists across Streamlit reruns)
# ---------------------------------------------------------------------------
_rate_lock = threading.Lock()

_rate_state: dict[str, Any] = {
    "requests_used": None,      # int - calls consumed in current window
    "requests_limit": None,     # int - total allowed per window
    "requests_remaining": None, # int - remaining in window
    "reset_at": None,           # str - ISO timestamp or epoch seconds
    "last_checked": None,       # float - time.time() of last response received
    "window_seconds": 3600,     # int - assumed window size if not in headers
}


def _patch_session(session: requests.Session) -> None:
    """Wrap session.send() to capture rate-limit response headers."""
    original_send = session.send

    def patched_send(request, **kwargs):  # type: ignore
        response = original_send(request, **kwargs)
        _capture_rate_limit_headers(response.headers)
        return response

    session.send = patched_send  # type: ignore


def _capture_rate_limit_headers(headers: Any) -> None:
    """
    Parse rate-limit headers from an HTTP response.

    BeProduct may use any of these common header patterns:
      X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset
      RateLimit-Limit, RateLimit-Remaining, RateLimit-Reset  (RFC 6585 draft)
    """
    def _int(key: str) -> Optional[int]:
        v = headers.get(key)
        return int(v) if v is not None else None

    limit = _int("X-RateLimit-Limit") or _int("RateLimit-Limit")
    remaining = _int("X-RateLimit-Remaining") or _int("RateLimit-Remaining")
    reset_raw = headers.get("X-RateLimit-Reset") or headers.get("RateLimit-Reset")

    with _rate_lock:
        _rate_state["last_checked"] = time.time()

        if limit is not None:
            _rate_state["requests_limit"] = limit
        if remaining is not None:
            _rate_state["requests_remaining"] = remaining
            if limit is not None:
                _rate_state["requests_used"] = limit - remaining

        if reset_raw is not None:
            # Could be epoch seconds (int) or ISO datetime string
            try:
                reset_epoch = int(reset_raw)
                _rate_state["reset_at"] = datetime.fromtimestamp(
                    reset_epoch, tz=timezone.utc
                ).isoformat()
            except ValueError:
                _rate_state["reset_at"] = reset_raw  # already a string


def get_rate_limit_status() -> dict[str, Any]:
    """Return a copy of the current rate-limit state."""
    with _rate_lock:
        return dict(_rate_state)


# ---------------------------------------------------------------------------
# Singleton client
# ---------------------------------------------------------------------------
_client_instance: Optional[Any] = None
_client_lock = threading.Lock()


def get_client() -> Any:
    """
    Return the shared BeProduct SDK client, creating it on first call.

    The SDK auto-refreshes access tokens using the stored refresh_token.
    We patch the underlying requests session immediately after construction
    to capture rate-limit headers from every API call.
    """
    global _client_instance

    if _client_instance is not None:
        return _client_instance

    if BeProduct is None:
        raise ImportError(
            "The 'beproduct' package is not installed. Run: pip install beproduct"
        )

    with _client_lock:
        if _client_instance is not None:
            return _client_instance

        client = BeProduct(
            client_id=settings.CLIENT_ID,
            client_secret=settings.CLIENT_SECRET,
            refresh_token=settings.REFRESH_TOKEN,
            company_domain=settings.COMPANY_DOMAIN,
        )

        # Patch the session if accessible (SDK internals may vary by version)
        session = getattr(client, "_session", None) or getattr(client, "session", None)
        if session is None:
            # Try to find session in sub-clients
            for attr in ("style", "material", "color", "directory"):
                sub = getattr(client, attr, None)
                if sub:
                    session = getattr(sub, "_session", None) or getattr(sub, "session", None)
                    if session:
                        break

        if session and isinstance(session, requests.Session):
            _patch_session(session)
        # If session not found, rate-limit tracking is disabled (non-fatal)

        _client_instance = client

    return _client_instance


def reset_client() -> None:
    """Force recreation of the SDK client (e.g., after credential change)."""
    global _client_instance
    with _client_lock:
        _client_instance = None
