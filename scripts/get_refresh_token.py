"""
One-time helper script to obtain a BeProduct refresh token via OAuth2 Authorization Code flow.

Usage:
    python scripts/get_refresh_token.py

Prerequisites:
    1. Copy .env.example to .env and fill in:
       BEPRODUCT_CLIENT_ID
       BEPRODUCT_CLIENT_SECRET
       BEPRODUCT_CALLBACK_URL  (must be registered with BeProduct support)
    2. Run this script — it will:
       a. Open a browser tab to the BeProduct login page
       b. Start a temporary local HTTP server to capture the callback
       c. Exchange the authorization code for tokens
       d. Print the refresh_token to paste into your .env

This script is ONLY needed once per user per application. The refresh_token does
not expire unless explicitly revoked by BeProduct support.
"""

from __future__ import annotations

import http.server
import os
import threading
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root
_root = Path(__file__).parent.parent
load_dotenv(_root / ".env")

CLIENT_ID = os.environ.get("BEPRODUCT_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("BEPRODUCT_CLIENT_SECRET", "")
CALLBACK_URL = os.environ.get("BEPRODUCT_CALLBACK_URL", "http://localhost:8765/callback")

AUTH_ENDPOINT = "https://id.winks.io/ids/connect/authorize"
TOKEN_ENDPOINT = "https://id.winks.io/ids/connect/token"
SCOPE = "openid profile email roles offline_access BeProductPublicApi"

# Parse callback host and port from CALLBACK_URL
_parsed = urllib.parse.urlparse(CALLBACK_URL)
_CALLBACK_HOST = _parsed.hostname or "localhost"
_CALLBACK_PORT = _parsed.port or 8765
_CALLBACK_PATH = _parsed.path or "/callback"

# Shared state to pass the code from the HTTP handler back to the main thread
_auth_code_holder: list[str] = []
_server_ready = threading.Event()
_code_received = threading.Event()


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    """Minimal HTTP handler that captures the ?code= parameter."""

    def do_GET(self):  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == _CALLBACK_PATH:
            params = urllib.parse.parse_qs(parsed.query)
            code = params.get("code", [None])[0]
            if code:
                _auth_code_holder.append(code)
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h2>Authorization successful!</h2>"
                    b"<p>You can close this tab and return to the terminal.</p></body></html>"
                )
                _code_received.set()
                return

        self.send_response(400)
        self.end_headers()
        self.wfile.write(b"Bad request")

    def log_message(self, format, *args):  # noqa: A002
        pass  # suppress request log noise


def _run_server():
    server = http.server.HTTPServer((_CALLBACK_HOST, _CALLBACK_PORT), _CallbackHandler)
    _server_ready.set()
    server.handle_request()  # serve exactly ONE request
    server.server_close()


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print(
            "ERROR: BEPRODUCT_CLIENT_ID and BEPRODUCT_CLIENT_SECRET must be set in .env\n"
            "Copy .env.example to .env and fill in the values."
        )
        return

    print("=" * 60)
    print("BeProduct Refresh Token Bootstrap")
    print("=" * 60)
    print(f"Client ID     : {CLIENT_ID}")
    print(f"Callback URL  : {CALLBACK_URL}")
    print()

    # 1. Start local callback server
    server_thread = threading.Thread(target=_run_server, daemon=True)
    server_thread.start()
    _server_ready.wait(timeout=3)

    # 2. Build authorization URL
    params = urllib.parse.urlencode({
        "client_id": CLIENT_ID,
        "response_type": "code",
        "scope": SCOPE,
        "redirect_uri": CALLBACK_URL,
    })
    auth_url = f"{AUTH_ENDPOINT}?{params}"
    print("Opening browser to BeProduct login…")
    print(f"If browser doesn't open, navigate to:\n  {auth_url}")
    print()
    webbrowser.open(auth_url)

    # 3. Wait for callback
    print(f"Waiting for BeProduct to redirect to {CALLBACK_URL} …")
    if not _code_received.wait(timeout=120):
        print("ERROR: Timed out waiting for authorization code. Please try again.")
        return

    auth_code = _auth_code_holder[0]
    print(f"Authorization code received: {auth_code[:12]}…")
    print()

    # 4. Exchange code for tokens
    print("Exchanging authorization code for tokens…")
    token_data = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": auth_code,
        "redirect_uri": CALLBACK_URL,
    }).encode()

    req = urllib.request.Request(TOKEN_ENDPOINT, data=token_data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    import json
    try:
        with urllib.request.urlopen(req) as resp:
            token_response = json.loads(resp.read().decode())
    except Exception as e:
        print(f"ERROR: Token exchange failed: {e}")
        return

    refresh_token = token_response.get("refresh_token")
    access_token = token_response.get("access_token")

    if not refresh_token:
        print("ERROR: No refresh_token in response:")
        print(json.dumps(token_response, indent=2))
        return

    print("=" * 60)
    print("SUCCESS! Your tokens:")
    print("=" * 60)
    print(f"\nREFRESH TOKEN:\n  {refresh_token}")
    print(f"\nACCESS TOKEN (expires in {token_response.get('expires_in', '?')}s):\n  {access_token[:40]}…")
    print()
    print("Add this line to your .env file:")
    print(f"  BEPRODUCT_REFRESH_TOKEN={refresh_token}")
    print()
    print("The refresh token does NOT expire — store it safely!")
    print("=" * 60)


if __name__ == "__main__":
    main()
