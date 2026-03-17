"""
Sidebar component: navigation, sync controls, API rate-limit display, status summary.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Optional

import streamlit as st

from app import db
from app.beproduct_client import get_rate_limit_status
from app.config import settings


# ---------------------------------------------------------------------------
# Sync job tracker (module-level so it doesn't reset on Streamlit reruns)
# ---------------------------------------------------------------------------
_sync_running = threading.Event()


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
            options=["🏠 Overview", "👗 Styles", "🧵 Materials", "🎨 Colors", "📒 Directory"],
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
    col1, col2 = st.columns(2)

    with col1:
        if st.button("⬇ Full Sync", use_container_width=True, help="Download all records from BeProduct"):
            _trigger_sync(force_full=True)

    with col2:
        if st.button("🔄 Incremental", use_container_width=True, help="Sync only records modified since last sync"):
            _trigger_sync(force_full=False)

    if st.session_state.get("_sync_status"):
        status = st.session_state["_sync_status"]
        if status.get("running"):
            st.info("⏳ Sync in progress…")
        elif status.get("results"):
            results = status["results"]
            all_ok = all(ok for ok, _ in results.values())
            if all_ok:
                st.success("✅ Sync complete")
            else:
                st.warning("⚠️ Sync completed with errors")
            with st.expander("Details"):
                for entity, (ok, msg) in results.items():
                    icon = "✅" if ok else "❌"
                    st.write(f"{icon} **{entity}**: {msg}")


def _trigger_sync(force_full: bool) -> None:
    """Launch sync in a background thread and store status in session_state."""
    from app.sync import sync_all

    if st.session_state.get("_sync_status", {}).get("running"):
        st.warning("Sync already running — please wait.")
        return

    st.session_state["_sync_status"] = {"running": True, "results": {}}

    def _run():
        results = sync_all(force_full=force_full)
        st.session_state["_sync_status"] = {"running": False, "results": results}

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    st.rerun()


def _render_sync_meta() -> None:
    """Show last sync timestamp per entity."""
    entities = ["styles", "materials", "colors", "directory"]
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

    if limit is None and used is None:
        st.caption("ℹ️ Rate limit data not yet available — make an API call first")
        return

    if limit and used is not None:
        fraction = min(used / limit, 1.0)
        color_warning = fraction > 0.8
        label = f"{used} / {limit} requests used"
        st.progress(fraction, text=label)

        if color_warning:
            st.warning("⚠️ Approaching rate limit")

    if remaining is not None and limit is None:
        st.metric("Remaining requests", remaining)

    if reset_at:
        st.caption(f"Resets at: {_format_ts(reset_at)}")

    if last_checked:
        ago = int(time.time() - last_checked)
        st.caption(f"Last API call: {ago}s ago")


def _render_db_counts() -> None:
    """Show record counts and dirty count per entity."""
    try:
        counts = db.get_row_counts()
        st.metric("Styles", counts.get("styles", 0), delta=f"{counts.get('styles_dirty', 0)} pending" if counts.get('styles_dirty') else None)
        st.metric("Materials", counts.get("materials", 0), delta=f"{counts.get('materials_dirty', 0)} pending" if counts.get('materials_dirty') else None)
        st.metric("Colors", counts.get("colors", 0), delta=f"{counts.get('colors_dirty', 0)} pending" if counts.get('colors_dirty') else None)
        st.metric("Directory", counts.get("directory", 0))
    except Exception as e:
        st.caption(f"DB not initialised: {e}")
