"""
Users page: list + detail view with create capability.
No delete — SDK does not support user deletion.
"""

from __future__ import annotations

import json
from typing import Optional

import pandas as pd
import streamlit as st

from app import db
from app.push import create_user
from app.ui._create_dialog import show_create_entity_dialog


# ── Raw JSON Dialog ──────────────────────────────────────────────────────
@st.dialog("Raw JSON")
def raw_json_dialog(data: dict) -> None:
    st.json(data)


def render_users_page() -> None:
    st.header("Users")

    # Handle post-create navigation
    created_id = st.session_state.pop("created_user_id", None)
    if created_id:
        st.session_state["user_selected_id"] = created_id

    selected_id = st.session_state.get("user_selected_id")
    if selected_id:
        _render_user_detail(selected_id)
        return

    _render_users_list()


def _render_users_list() -> None:
    st.subheader("All Users")

    # Create button
    col_create, _ = st.columns(2)
    with col_create:
        if st.button("+ Create New User", use_container_width=True):
            st.session_state["show_create_user"] = True

    if st.session_state.get("show_create_user"):
        show_create_entity_dialog(
            "User",
            on_create_callback=lambda fields: create_user(fields),
        )
        st.session_state["show_create_user"] = False

    col1, col2 = st.columns([3, 2])
    with col1:
        search = st.text_input("Search by username, email, name", key="user_search")
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
            "Active": "Yes" if u.get("active") else "No",
        })

    df = pd.DataFrame(rows)
    st.caption(f"Showing {len(df)} user(s)")

    col_left, col_right = st.columns([2, 1], gap="large")

    with col_left:
        event = st.dataframe(
            df.drop(columns=["ID"]),
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
        )

        selected_row_idx: Optional[int] = None
        if event and event.selection and event.selection.rows:
            selected_row_idx = event.selection.rows[0]
            st.session_state["users_selected_row_idx"] = selected_row_idx

    with col_right:
        selected_row_idx = st.session_state.get("users_selected_row_idx")
        if selected_row_idx is not None and selected_row_idx < len(users):
            selected_user = users[selected_row_idx]
            user_label = f"{selected_user.get('username', '')} -- {selected_user.get('email', '')}"
            if st.button("View Details", key="user_view_btn", use_container_width=True, type="primary"):
                st.session_state["user_selected_id"] = selected_user["id"]
                st.rerun()
            with st.expander(f"{user_label}", expanded=True):
                raw_data = selected_user.get("data_json")
                if isinstance(raw_data, str):
                    raw_data = json.loads(raw_data)
                st.json(raw_data or selected_user)
        else:
            st.info("Click a row to view details")


def _render_user_detail(user_id: str) -> None:
    row = db.get_user(user_id)
    if not row:
        st.error(f"User {user_id} not found in local DB")
        st.session_state.pop("user_selected_id", None)
        return

    data = json.loads(row["data_json"]) if isinstance(row.get("data_json"), str) else row.get("data_json", {})

    if st.button("< Back to list"):
        st.session_state.pop("user_selected_id", None)
        st.rerun()

    name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
    st.subheader(f"User: {name or row.get('username', 'Unknown')}")
    st.caption(f"**ID:** `{user_id}`")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Username", row.get("username", "--"))
    col2.metric("Role", row.get("role", "--"))
    col3.metric("Account Type", row.get("account_type", "--"))
    col4.metric("Active", "Yes" if row.get("active") else "No")

    st.divider()

    # User info display
    st.subheader("User Information")
    col_a, col_b = st.columns(2)
    with col_a:
        st.text_input("Email", value=row.get("email", ""), disabled=True)
        st.text_input("First Name", value=row.get("first_name", ""), disabled=True)
        st.text_input("Last Name", value=row.get("last_name", ""), disabled=True)
    with col_b:
        st.text_input("Title", value=row.get("title", ""), disabled=True)
        st.text_input("Registered On", value=row.get("registered_on", ""), disabled=True)

    st.divider()

    # Cross-references: used as createdBy/modifiedBy across entities
    st.subheader("Referenced By")
    st.caption("Entities where this user appears as createdBy, modifiedBy, or in Users-type fields.")
    st.caption("Note: Cross-reference scanning is performed via JSON text search and may be slow for large datasets.")

    # Show refs for styles and materials that reference this user
    with st.expander("Show referenced entities", expanded=False):
        _show_user_references(user_id, name or row.get("username", ""))

    st.caption("Note: Users cannot be deleted via the API.")

    with st.expander("Raw JSON", expanded=False):
        st.json(data)


def _show_user_references(user_id: str, user_name: str) -> None:
    """Search for entities that reference this user by ID."""
    import json as json_mod

    results = []

    # Search styles
    with db.get_conn() as conn:
        for table, entity_type in [("styles", "Style"), ("materials", "Material")]:
            rows = conn.execute(
                f"SELECT id, header_number FROM {table} WHERE data_json LIKE ?",
                (f'%{user_id}%',),
            ).fetchall()
            for r in rows:
                results.append({
                    "Entity Type": entity_type,
                    "Header Number": r["header_number"] or r["id"][:8],
                })

    if results:
        st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)
    else:
        st.caption("No entities reference this user (or data not yet synced).")
