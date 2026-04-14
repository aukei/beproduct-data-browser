"""
Users page: read-only metadata display.
Users are synced from the API but cannot be edited locally.
"""

from __future__ import annotations

import json
from typing import Optional

import pandas as pd
import streamlit as st

from app import db


# ── Raw JSON Dialog ──────────────────────────────────────────────────────
@st.dialog("📄 Raw JSON")
def raw_json_dialog(data: dict) -> None:
    """Show raw JSON in a modal dialog."""
    st.json(data)


def render_users_page() -> None:
    st.header("👥 Users")
    st.caption("User data is synced from BeProduct and is read-only.")

    _render_users_list()


def _render_users_list() -> None:
    st.subheader("All Users")

    col1, col2 = st.columns([3, 2])
    with col1:
        search = st.text_input("🔍 Search by username, email, name", key="user_search")
    with col2:
        active_only = st.checkbox("Active only", key="user_active_only")

    users = db.get_users(
        search=search or None,
        active_only=active_only,
        limit=500,
    )

    if not users:
        st.info("No users found. Run a sync to populate data from BeProduct.")
        return

    rows = []
    for u in users:
        rows.append({
            "ID": u["id"],
            "Username": u.get("username", ""),
            "Email": u.get("email", ""),
            "First Name": u.get("first_name", ""),
            "Last Name": u.get("last_name", ""),
            "Title": u.get("title", ""),
            "Role": u.get("role", ""),
            "Account Type": u.get("account_type", ""),
            "Active": "✅" if u.get("active") else "❌",
        })

    df = pd.DataFrame(rows)
    st.caption(f"Showing {len(df)} user(s)")

    # Split layout: left = table, right = JSON detail panel
    col_left, col_right = st.columns([2, 1], gap="large")

    # LEFT: Data grid with selection
    with col_left:
        event = st.dataframe(
            df.drop(columns=["ID"]),
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
        )

        # Handle row selection
        selected_row_idx: Optional[int] = None
        if event and event.selection and event.selection.rows:
            selected_row_idx = event.selection.rows[0]
            st.session_state["users_selected_row_idx"] = selected_row_idx

    # RIGHT: JSON detail panel
    with col_right:
        selected_row_idx = st.session_state.get("users_selected_row_idx")
        if selected_row_idx is not None and selected_row_idx < len(users):
            selected_user = users[selected_row_idx]
            user_label = f"{selected_user.get('username', '')} — {selected_user.get('email', '')}"
            with st.expander(f"📄 {user_label}", expanded=True):
                raw_data = selected_user.get("data_json")
                if isinstance(raw_data, str):
                    raw_data = json.loads(raw_data)
                st.json(raw_data or selected_user)
        else:
            st.info("👈 Click a row to view raw JSON")
