"""
Configuration loader - reads settings from .env file.
All app code should import from here rather than reading os.environ directly.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root (two levels up from this file)
_root = Path(__file__).parent.parent
load_dotenv(_root / ".env")


def _require(key: str) -> str:
    """Get a required env var, raising a clear error if missing."""
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"Copy .env.example to .env and fill in your credentials."
        )
    return val


def _optional(key: str, default: str = "") -> str:
    return os.getenv(key, default)


class Settings:
    """Central configuration object."""

    # BeProduct API credentials
    CLIENT_ID: str = _require("BEPRODUCT_CLIENT_ID")
    CLIENT_SECRET: str = _require("BEPRODUCT_CLIENT_SECRET")
    REFRESH_TOKEN: str = _require("BEPRODUCT_REFRESH_TOKEN")
    COMPANY_DOMAIN: str = _require("BEPRODUCT_COMPANY_DOMAIN")
    CALLBACK_URL: str = _optional("BEPRODUCT_CALLBACK_URL", "http://localhost:8765/callback")

    # Sync settings
    SYNC_INTERVAL_MINUTES: int = int(_optional("SYNC_INTERVAL_MINUTES", "15"))

    # Database
    DB_PATH: Path = _root / _optional("DB_PATH", "data/beproduct.db")


settings = Settings()
