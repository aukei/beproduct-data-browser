"""
Overview / home page: summary statistics and quick-action cards.
"""

from __future__ import annotations

from datetime import datetime, timezone

import streamlit as st

from app import db
from app.beproduct_client import get_rate_limit_status
from app.config import settings


def render_overview_page() -> None:
    st.header("🏠 Overview")
    st.caption(
        f"Local sync copy of **{settings.COMPANY_DOMAIN}** BeProduct data. "
        f"Auto-syncs every **{settings.SYNC_INTERVAL_MINUTES} minutes**."
    )

    st.divider()

    # ── Record counts ─────────────────────────────────────────────────────
    st.subheader("📊 Local Database Summary")
    try:
        counts = db.get_row_counts()
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Styles", counts.get("styles", 0),
                    delta=f"{counts.get('styles_dirty', 0)} pending" if counts.get("styles_dirty") else None)
        col2.metric("Materials", counts.get("materials", 0),
                    delta=f"{counts.get('materials_dirty', 0)} pending" if counts.get("materials_dirty") else None)
        col3.metric("Colors", counts.get("colors", 0),
                    delta=f"{counts.get('colors_dirty', 0)} pending" if counts.get("colors_dirty") else None)
        col4.metric("Directory", counts.get("directory", 0))
    except Exception as e:
        st.warning(f"Database not yet initialised: {e}")
        st.info("Use **⬇ Full Sync** in the sidebar to download your BeProduct data.")
        return

    # ── Sync status ───────────────────────────────────────────────────────
    st.divider()
    st.subheader("🔄 Sync Status")
    entities = ["styles", "materials", "colors", "directory"]
    any_synced = False

    cols = st.columns(len(entities))
    for i, entity in enumerate(entities):
        meta = db.get_sync_meta(entity)
        with cols[i]:
            if meta and meta.get("last_sync_at"):
                last = meta["last_sync_at"]
                try:
                    dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
                    local_dt = dt.astimezone()
                    age_seconds = (datetime.now(timezone.utc) - dt).total_seconds()
                    if age_seconds < 3600:
                        age_str = f"{int(age_seconds // 60)}m ago"
                    else:
                        age_str = f"{int(age_seconds // 3600)}h ago"
                    st.metric(entity.capitalize(), age_str, delta=meta.get("sync_type", "full"))
                    any_synced = True
                except Exception:
                    st.metric(entity.capitalize(), "Unknown")
            else:
                st.metric(entity.capitalize(), "Not synced", delta=None)

    if not any_synced:
        st.info("💡 **First time?** Click **⬇ Full Sync** in the sidebar to download all your BeProduct data.")

    # ── Pending push-backs ────────────────────────────────────────────────
    total_dirty = (
        counts.get("styles_dirty", 0)
        + counts.get("materials_dirty", 0)
        + counts.get("colors_dirty", 0)
    )
    if total_dirty > 0:
        st.divider()
        st.subheader("🔴 Pending Push-backs")
        st.warning(
            f"You have **{total_dirty}** locally-edited record(s) not yet pushed to BeProduct SaaS. "
            f"Open each record and click **🚀 Push to BeProduct**, or use the button below to push all."
        )
        if st.button("🚀 Push All Dirty Records", type="primary"):
            from app.push import push_all_dirty
            with st.spinner("Pushing all locally-modified records…"):
                results = push_all_dirty()
            _show_push_results(results)

    # ── Rate limit ────────────────────────────────────────────────────────
    rl = get_rate_limit_status()
    if rl.get("requests_limit"):
        st.divider()
        st.subheader("⚡ API Rate Limit")
        used = rl.get("requests_used", 0) or 0
        limit = rl["requests_limit"]
        fraction = min(used / limit, 1.0)
        st.progress(fraction, text=f"{used} / {limit} API requests used in current window")
        if rl.get("reset_at"):
            st.caption(f"Resets at: {rl['reset_at']}")


def _show_push_results(results: dict) -> None:
    for entity, records in results.items():
        if records:
            st.subheader(entity.capitalize())
            for rec_id, ok, msg in records:
                if ok:
                    st.success(f"✅ {rec_id[:8]}… — {msg}")
                else:
                    st.error(f"❌ {rec_id[:8]}… — {msg}")
