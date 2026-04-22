"""
Sidebar component: navigation, sync controls, API rate-limit display, status summary.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import streamlit as st

from app import db
from app.beproduct_client import get_rate_limit_status
from app.config import settings


# ---------------------------------------------------------------------------
# Sync job tracker (module-level so it doesn't reset on Streamlit reruns)
# ---------------------------------------------------------------------------
_sync_running = threading.Event()

# File-based sync status for cross-thread communication
_SYNC_STATUS_FILE = Path("/tmp/beproduct_sync_status.json")

# Track last known sync status to avoid unnecessary reruns
_last_sync_status_key = "_last_sync_status"
_sync_completed_ts_key = "_sync_completed_ts"


def _format_ts(iso_str: Optional[str]) -> str:
    """Format an ISO timestamp to a human-friendly local string."""
    if not iso_str:
        return "Never"
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        local_dt = dt.astimezone()
        return local_dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return iso_str


def _read_sync_status_file() -> Optional[dict]:
    """Read sync status from file."""
    try:
        if _SYNC_STATUS_FILE.exists():
            with open(_SYNC_STATUS_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return None


def _write_sync_status_file(status: dict) -> None:
    """Write sync status to file."""
    try:
        with open(_SYNC_STATUS_FILE, "w") as f:
            json.dump(status, f)
    except Exception:
        pass


def _clear_sync_status_file() -> None:
    """Remove sync status file."""
    try:
        if _SYNC_STATUS_FILE.exists():
            _SYNC_STATUS_FILE.unlink()
    except Exception:
        pass


def render_sidebar() -> str:
    """
    Render the full sidebar. Returns the selected page name.
    """
    with st.sidebar:
        st.title("🧶 BeProduct Data Browser")
        st.caption(f"Company: **{settings.COMPANY_DOMAIN}**")
        st.divider()

        # ── Navigation ────────────────────────────────────────────────────
        st.subheader("Navigate")
        page = st.radio(
            "Section",
            options=[
                "🏠 Overview",
                "👗 Styles",
                "🧵 Materials",
                "🎨 Colors",
                "🖼️ Images",
                "🧱 Blocks",
                "📒 Directory",
                "👤 Users",
                "📊 Data Tables",
            ],
            label_visibility="collapsed",
        )

        st.divider()

        # ── Sync controls ─────────────────────────────────────────────────
        st.subheader("Sync")
        _render_sync_controls()

        # ── Sync metadata ─────────────────────────────────────────────────
        _render_sync_meta()

        st.divider()

        # ── API rate limit ────────────────────────────────────────────────
        st.subheader("API Rate Limit")
        _render_rate_limit()

        st.divider()

        # ── Record counts ─────────────────────────────────────────────────
        st.subheader("Local DB")
        _render_db_counts()

    return page


def _render_sync_controls() -> None:
    import logging
    logger = logging.getLogger(__name__)
    
    col1, col2 = st.columns(2)

    with col1:
        if st.button("⬇ Full Sync", use_container_width=True, help="Download all records from BeProduct"):
            _trigger_sync(force_full=True)

    with col2:
        if st.button("🔄 Incremental", use_container_width=True, help="Sync only records modified since last sync"):
            _trigger_sync(force_full=False)

    # Check sync status from file (written by background thread)
    file_status = _read_sync_status_file()
    
    if file_status is None:
        # No sync in progress or completed
        return
    
    if file_status.get("running"):
        # Check if sync has been running for too long (more than 5 minutes) - might be stale
        started_at = file_status.get("started_at", 0)
        elapsed = time.time() - started_at
        if elapsed > 300:  # 5 minutes timeout
            st.warning("⚠️ Sync appears to have stalled. Please refresh the page.")
            _clear_sync_status_file()
            return
        
        st.info("⏳ Sync in progress…")
        # Auto-poll: rerun after a short delay to check if sync finished
        time.sleep(2)
        st.rerun()
    elif file_status.get("results"):
        results = file_status["results"]
        all_ok = all(ok for ok, _ in results.values()) if results else False
        if all_ok:
            st.success("✅ Sync complete")
        elif results:
            st.warning("⚠️ Sync completed with errors")
        else:
            st.success("✅ Sync complete")
        if results:
            with st.expander("Details"):
                for entity, (ok, msg) in results.items():
                    icon = "✅" if ok else "❌"
                    st.write(f"{icon} **{entity}**: {msg}")
        # Clear the status file after displaying
        _clear_sync_status_file()
        logger.info("Sync completed and status cleared")


def _trigger_sync(force_full: bool) -> None:
    """Launch sync in a background thread and write status to file."""
    import logging
    from app.sync import sync_all
    
    logger = logging.getLogger(__name__)

    # Check if sync is already running via file
    file_status = _read_sync_status_file()
    if file_status and file_status.get("running"):
        # Check if it's stale (running for more than 5 minutes)
        started_at = file_status.get("started_at", 0)
        if time.time() - started_at < 300:
            st.warning("Sync already running — please wait.")
            return
        # Stale status - clear it and start new sync
        _clear_sync_status_file()

    # Write initial running status to file
    _write_sync_status_file({"running": True, "results": {}, "started_at": time.time()})
    logger.info(f"Sync started (force_full={force_full})")

    def _run():
        try:
            results = sync_all(force_full=force_full)
            _write_sync_status_file({
                "running": False,
                "results": results,
                "completed_at": time.time()
            })
            logger.info(f"Sync completed: {results}")
        except Exception as e:
            _write_sync_status_file({
                "running": False,
                "results": {"error": (False, str(e))},
                "completed_at": time.time()
            })
            logger.error(f"Sync failed: {e}")

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    # Trigger one rerun to show "Sync in progress" message
    st.rerun()


def _render_sync_meta() -> None:
    """Show last sync timestamp per entity."""
    entities = ["styles", "materials", "colors", "directory", "images", "blocks", "users"]
    rows = []
    for e in entities:
        meta = db.get_sync_meta(e)
        if meta:
            rows.append(f"**{e}**: {_format_ts(meta.get('last_sync_at'))} ({meta.get('sync_type', 'full')})")
        else:
            rows.append(f"**{e}**: Never synced")

    with st.expander("Last sync times", expanded=False):
        for r in rows:
            st.caption(r)


def _render_rate_limit() -> None:
    """Display API rate-limit status from response headers."""
    rl = get_rate_limit_status()
    used = rl.get("requests_used")
    limit = rl.get("requests_limit")
    remaining = rl.get("requests_remaining")
    reset_at = rl.get("reset_at")
    last_checked = rl.get("last_checked")

    # Show last checked time if available
    if last_checked:
        ago = int(time.time() - last_checked)
        st.caption(f"Last API call: {ago}s ago")
    else:
        st.caption("ℹ️ No API calls made yet")
        return

    # If we have rate limit data, display it
    if limit is not None and used is not None:
        fraction = min(used / limit, 1.0)
        color_warning = fraction > 0.8
        label = f"{used} / {limit} requests used"
        st.progress(fraction, text=label)

        if color_warning:
            st.warning("⚠️ Approaching rate limit")
    elif remaining is not None:
        st.metric("Remaining requests", remaining)
    else:
        # API was called but no rate limit headers were returned
        st.caption("ℹ️ Rate limit headers not provided by API")

    if reset_at:
        st.caption(f"Resets at: {_format_ts(reset_at)}")


def _render_db_counts() -> None:
    """Show record counts and dirty count per entity."""
    try:
        counts = db.get_row_counts()
        st.metric("Styles", counts.get("styles", 0), delta=f"{counts.get('styles_dirty', 0)} pending" if counts.get('styles_dirty') else None)
        st.metric("Materials", counts.get("materials", 0), delta=f"{counts.get('materials_dirty', 0)} pending" if counts.get('materials_dirty') else None)
        st.metric("Colors", counts.get("colors", 0), delta=f"{counts.get('colors_dirty', 0)} pending" if counts.get('colors_dirty') else None)
        st.metric("Images", counts.get("images", 0), delta=f"{counts.get('images_dirty', 0)} pending" if counts.get('images_dirty') else None)
        st.metric("Blocks", counts.get("blocks", 0), delta=f"{counts.get('blocks_dirty', 0)} pending" if counts.get('blocks_dirty') else None)
        st.metric("Directory", counts.get("directory", 0))
        st.metric("Users", counts.get("users", 0))
        st.metric("Data Tables", counts.get("data_tables", 0))
    except Exception as e:
        st.caption(f"DB not initialised: {e}")
