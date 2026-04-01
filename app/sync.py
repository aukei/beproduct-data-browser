"""
Sync engine: pulls data from BeProduct SaaS into local SQLite.

Two modes:
  - full_sync(entity)       : fetches ALL records (used on first run or manual trigger)
  - incremental_sync(entity): fetches only records modified since last sync (via ModifiedAt filter)
  - sync_all()              : runs incremental (or full if never synced) for every entity

Thread-safety: each sync call holds an entity-level lock so concurrent Streamlit reruns
don't trigger duplicate syncs.
"""

from __future__ import annotations

import logging
import threading
import traceback
from datetime import datetime, timezone
from typing import Callable, Optional

from app import db
from app.beproduct_client import get_client
from app.config import settings

logger = logging.getLogger(__name__)

# Per-entity locks to prevent concurrent syncs
_locks: dict[str, threading.Lock] = {
    "styles": threading.Lock(),
    "materials": threading.Lock(),
    "colors": threading.Lock(),
    "directory": threading.Lock(),
}


# ---------------------------------------------------------------------------
# Sync progress callback type
# ---------------------------------------------------------------------------
ProgressCallback = Callable[[str, int], None]


def _noop_progress(msg: str, count: int) -> None:
    pass


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _modified_after_filter(since_iso: str) -> dict:
    return {
        "field": "ModifiedAt",
        "operator": "Gt",
        "value": since_iso,
    }


def _any_modified_after_filter(since_iso: str) -> dict:
    """Broader filter that includes app modifications, not just attributes."""
    return {
        "field": "FolderModifiedAt",
        "operator": "Gt",
        "value": since_iso,
    }


# ---------------------------------------------------------------------------
# Styles sync
# ---------------------------------------------------------------------------

def sync_styles(
    incremental: bool = False,
    progress: ProgressCallback = _noop_progress,
) -> tuple[bool, str]:
    """
    Sync styles from BeProduct to local DB.
    Returns (success, message).
    """
    with _locks["styles"]:
        try:
            client = get_client()
            filters = []

            if incremental:
                meta = db.get_sync_meta("styles")
                if meta and meta.get("last_sync_at"):
                    last = meta["last_sync_at"]
                    filters.append(_any_modified_after_filter(last))
                    logger.info(f"Styles incremental sync since {last}")
                else:
                    incremental = False  # fall back to full

            count = 0
            for style in client.style.attributes_list(filters=filters if filters else None):
                db.upsert_style(style)
                count += 1
                if count % 50 == 0:
                    progress("styles", count)

            db.set_sync_meta("styles", sync_type="incremental" if incremental else "full")
            msg = f"Synced {count} style(s)"
            logger.info(msg)
            progress("styles", count)
            return True, msg

        except Exception as e:
            msg = f"Style sync failed: {e}"
            logger.error(f"{msg}\n{traceback.format_exc()}")
            return False, msg


# ---------------------------------------------------------------------------
# Materials sync
# ---------------------------------------------------------------------------

def sync_materials(
    incremental: bool = False,
    progress: ProgressCallback = _noop_progress,
) -> tuple[bool, str]:
    with _locks["materials"]:
        try:
            client = get_client()
            filters = []

            if incremental:
                meta = db.get_sync_meta("materials")
                if meta and meta.get("last_sync_at"):
                    last = meta["last_sync_at"]
                    filters.append(_any_modified_after_filter(last))
                    logger.info(f"Materials incremental sync since {last}")
                else:
                    incremental = False

            count = 0
            for material in client.material.attributes_list(filters=filters if filters else None):
                db.upsert_material(material)
                count += 1
                if count % 50 == 0:
                    progress("materials", count)

            db.set_sync_meta("materials", sync_type="incremental" if incremental else "full")
            msg = f"Synced {count} material(s)"
            logger.info(msg)
            progress("materials", count)
            return True, msg

        except Exception as e:
            msg = f"Material sync failed: {e}"
            logger.error(f"{msg}\n{traceback.format_exc()}")
            return False, msg


# ---------------------------------------------------------------------------
# Colors sync
# ---------------------------------------------------------------------------

def sync_colors(
    incremental: bool = False,
    progress: ProgressCallback = _noop_progress,
) -> tuple[bool, str]:
    with _locks["colors"]:
        try:
            client = get_client()
            filters = []

            if incremental:
                meta = db.get_sync_meta("colors")
                if meta and meta.get("last_sync_at"):
                    last = meta["last_sync_at"]
                    filters.append(_any_modified_after_filter(last))
                    logger.info(f"Colors incremental sync since {last}")
                else:
                    incremental = False

            count = 0
            for color in client.color.attributes_list(filters=filters if filters else None):
                db.upsert_color(color)
                count += 1
                if count % 50 == 0:
                    progress("colors", count)

            db.set_sync_meta("colors", sync_type="incremental" if incremental else "full")
            msg = f"Synced {count} color palette(s)"
            logger.info(msg)
            progress("colors", count)
            return True, msg

        except Exception as e:
            msg = f"Color sync failed: {e}"
            logger.error(f"{msg}\n{traceback.format_exc()}")
            return False, msg


# ---------------------------------------------------------------------------
# Directory sync
# ---------------------------------------------------------------------------

def sync_directory(
    progress: ProgressCallback = _noop_progress,
) -> tuple[bool, str]:
    """Directory does not support ModifiedAt filter — always does a full sweep."""
    with _locks["directory"]:
        try:
            client = get_client()
            total_count = 0
            changed_count = 0
            for record in client.directory.directory_list():
                total_count += 1
                if db.upsert_directory_record(record):
                    changed_count += 1
                if total_count % 50 == 0:
                    progress("directory", total_count)

            db.set_sync_meta("directory", sync_type="full")
            if changed_count == 0:
                msg = f"Directory up to date ({total_count} records checked)"
            elif changed_count == total_count:
                msg = f"Synced {changed_count} directory record(s)"
            else:
                msg = f"Synced {changed_count} directory record(s) ({total_count} checked)"
            logger.info(msg)
            progress("directory", total_count)
            return True, msg

        except Exception as e:
            msg = f"Directory sync failed: {e}"
            logger.error(f"{msg}\n{traceback.format_exc()}")
            return False, msg


# ---------------------------------------------------------------------------
# Sync-all convenience function
# ---------------------------------------------------------------------------

def sync_all(
    force_full: bool = False,
    progress: ProgressCallback = _noop_progress,
) -> dict[str, tuple[bool, str]]:
    """
    Run sync for all entities.
    Uses incremental mode if a prior sync exists, unless force_full=True.
    Returns a dict of {entity: (success, message)}.
    """
    results: dict[str, tuple[bool, str]] = {}

    # Determine if each entity has ever been synced
    incremental = not force_full

    results["styles"] = sync_styles(incremental=incremental, progress=progress)
    results["materials"] = sync_materials(incremental=incremental, progress=progress)
    results["colors"] = sync_colors(incremental=incremental, progress=progress)
    results["directory"] = sync_directory(progress=progress)

    return results


# ---------------------------------------------------------------------------
# APScheduler job wrapper
# ---------------------------------------------------------------------------

def scheduled_incremental_sync() -> None:
    """Called by APScheduler background job. Runs incremental sync silently."""
    logger.info("Scheduled incremental sync started")
    results = sync_all(force_full=False)
    for entity, (ok, msg) in results.items():
        level = logging.INFO if ok else logging.WARNING
        logger.log(level, f"[{entity}] {msg}")
    logger.info("Scheduled incremental sync complete")
