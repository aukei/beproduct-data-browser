"""
BeProduct SDK client singleton with HTTP response interception for rate-limit header capture.

The BeProduct SDK (v0.6.x) uses bare `requests.get()` / `requests.post()` calls in its
`_raw_api.RawApi` class — NOT a `requests.Session`. We therefore monkey-patch the
`requests` module functions themselves so every API response is inspected for
X-RateLimit-* headers.

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
    "reset_at": None,           # str - ISO timestamp or epoch int
    "last_checked": None,       # float - time.time() of last response received
}

# Track whether we have already patched requests
_requests_patched = False
_patch_lock = threading.Lock()


def _capture_rate_limit_headers(headers: Any) -> None:
    """
    Parse rate-limit headers from an HTTP response.

    BeProduct may use any of these common header patterns:
      X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset
      RateLimit-Limit, RateLimit-Remaining, RateLimit-Reset  (RFC draft)
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
            try:
                reset_epoch = int(reset_raw)
                _rate_state["reset_at"] = datetime.fromtimestamp(
                    reset_epoch, tz=timezone.utc
                ).isoformat()
            except ValueError:
                _rate_state["reset_at"] = str(reset_raw)


def _patch_requests_module() -> None:
    """
    Monkey-patch `requests.get` and `requests.post` to capture rate-limit headers.
    Called once at client initialisation.
    """
    global _requests_patched
    with _patch_lock:
        if _requests_patched:
            return

        _original_get = requests.get
        _original_post = requests.post

        def _patched_get(url, **kwargs):
            response = _original_get(url, **kwargs)
            _capture_rate_limit_headers(response.headers)
            return response

        def _patched_post(url, **kwargs):
            response = _original_post(url, **kwargs)
            _capture_rate_limit_headers(response.headers)
            return response

        requests.get = _patched_get    # type: ignore[method-assign]
        requests.post = _patched_post  # type: ignore[method-assign]

        # Also patch inside the beproduct._raw_api module where the SDK imports it
        try:
            import beproduct._raw_api as _raw_api_mod  # type: ignore
            _raw_api_mod.requests.get = _patched_get    # type: ignore
            _raw_api_mod.requests.post = _patched_post  # type: ignore
        except Exception:
            pass  # Non-fatal if module structure has changed

        _requests_patched = True


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
    We patch the requests module on first call to capture rate-limit headers.
    """
    global _client_instance

    if _client_instance is not None:
        return _client_instance

    if BeProduct is None:
        raise ImportError(
            "The 'beproduct' package is not installed. Run: pip install -r requirements.txt"
        )

    with _client_lock:
        if _client_instance is not None:
            return _client_instance

        # Patch requests before constructing client so all subsequent calls are intercepted
        _patch_requests_module()

        client = BeProduct(
            client_id=settings.CLIENT_ID,
            client_secret=settings.CLIENT_SECRET,
            refresh_token=settings.REFRESH_TOKEN,
            company_domain=settings.COMPANY_DOMAIN,
        )

        _client_instance = client

    return _client_instance


def reset_client() -> None:
    """Force recreation of the SDK client (e.g., after credential change)."""
    global _client_instance
    with _client_lock:
        _client_instance = None
