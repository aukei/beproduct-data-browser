"""
Directory page: list of vendors/factories/partners + detail view.
Includes create functionality and cross-reference display.
No delete — SDK does not support directory deletion.
"""

from __future__ import annotations

import json
from typing import Optional

import pandas as pd
import streamlit as st

from app import db
from app.push import push_directory, create_directory_entry
from app.ui._create_dialog import show_create_entity_dialog


# ── Raw JSON Dialog ──────────────────────────────────────────────────────
@st.dialog("Raw JSON")
def raw_json_dialog(data: dict) -> None:
    st.json(data)


def render_directory_page() -> None:
    st.header("Directory")

    # Handle post-create navigation
    created_id = st.session_state.pop("created_directory_id", None)
    if created_id:
        st.session_state["directory_selected_id"] = created_id

    selected_id = st.session_state.get("directory_selected_id")
    if selected_id:
        _render_directory_detail(selected_id)
        return

    _render_directory_list()


def _render_directory_list() -> None:
    st.subheader("Vendors, Factories & Partners")

    # Create button
    col_create, _ = st.columns(2)
    with col_create:
        if st.button("+ Create New Directory Entry", use_container_width=True):
            st.session_state["show_create_directory"] = True

    if st.session_state.get("show_create_directory"):
        show_create_entity_dialog(
            "Directory",
            on_create_callback=lambda fields: create_directory_entry(fields),
        )
        st.session_state["show_create_directory"] = False

    col1, col2 = st.columns([3, 2])
    with col1:
        search = st.text_input("Search by name or ID", key="dir_search")
    with col2:
        partner_types = ["All Types", "VENDOR", "FACTORY", "AGENT", "RETAILER", "SUPPLIER", "OTHER"]
        type_sel = st.selectbox("Partner Type", options=partner_types, key="dir_type_sel")

    partner_type = None if type_sel == "All Types" else type_sel

    records = db.get_directory_records(
        partner_type=partner_type,
        search=search or None,
        limit=500,
    )

    if not records:
        st.info("No directory records found. Run a sync to populate data from BeProduct.")
        return

    rows = []
    for r in records:
        rows.append({
            "ID": r["id"],
            "Directory ID": r.get("directory_id", ""),
            "Name": r.get("name", ""),
            "Type": r.get("partner_type", ""),
            "Country": r.get("country", ""),
            "Last Synced": (r.get("synced_at") or "")[:10],
        })

    df = pd.DataFrame(rows)
    st.caption(f"Showing {len(df)} record(s)")

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
            st.session_state["directory_selected_row_idx"] = selected_row_idx

    with col_right:
        selected_row_idx = st.session_state.get("directory_selected_row_idx")
        if selected_row_idx is not None and selected_row_idx < len(records):
            selected_record = records[selected_row_idx]
            rec_label = f"{selected_record.get('directory_id', '')} -- {selected_record.get('name', '')}"
            if st.button("View Details", key="dir_view_btn", use_container_width=True, type="primary"):
                st.session_state["directory_selected_id"] = selected_record["id"]
                st.rerun()
            with st.expander(f"{rec_label}", expanded=True):
                raw_data = selected_record.get("data_json")
                if isinstance(raw_data, str):
                    raw_data = json.loads(raw_data)
                st.json(raw_data or selected_record)
        else:
            st.info("Click a row to view details")


def _render_directory_detail(record_id: str) -> None:
    row = db.get_directory_record(record_id)
    if not row:
        st.error(f"Directory record {record_id} not found in local DB")
        st.session_state.pop("directory_selected_id", None)
        return

    data = json.loads(row["data_json"])

    if st.button("< Back to list"):
        st.session_state.pop("directory_selected_id", None)
        st.rerun()

    st.subheader(f"{row.get('name', 'Unknown')}  ({row.get('partner_type', '')})")
    st.caption(f"**ID:** `{record_id}`")

    col1, col2, col3 = st.columns(3)
    col1.metric("Country", row.get("country", "--"))
    col2.metric("Active", "Yes" if row.get("active") else "No")
    col3.metric("Directory ID", row.get("directory_id", "--"))

    st.divider()

    # Address / contact info
    st.subheader("Address")
    address_fields = ["address", "city", "state", "zip", "country", "phone", "website"]
    addr_data = {k: data.get(k, "") for k in address_fields}

    col_a, col_b = st.columns(2)
    with col_a:
        st.text_input("Address", value=addr_data.get("address", ""), disabled=True)
        st.text_input("City", value=addr_data.get("city", ""), disabled=True)
        st.text_input("State", value=addr_data.get("state", ""), disabled=True)
        st.text_input("Zip", value=addr_data.get("zip", ""), disabled=True)
    with col_b:
        st.text_input("Country", value=addr_data.get("country", ""), disabled=True)
        st.text_input("Phone", value=addr_data.get("phone", ""), disabled=True)
        st.text_input("Website", value=addr_data.get("website", ""), disabled=True)

    # Contacts
    contacts = data.get("contacts", [])
    if contacts:
        st.divider()
        st.subheader("Contacts")
        contact_rows = []
        for c in contacts:
            contact_rows.append({
                "First Name": c.get("firstName", ""),
                "Last Name": c.get("lastName", ""),
                "Email": c.get("email", ""),
                "Title": c.get("title", ""),
                "Mobile": c.get("mobilePhone", ""),
                "Work Phone": c.get("workPhone", ""),
                "Role": c.get("role", ""),
            })
        st.dataframe(pd.DataFrame(contact_rows), use_container_width=True, hide_index=True)

    st.divider()

    # Cross-references: which styles/materials reference this partner
    st.subheader("Referenced By")
    refs = db.get_entities_by_partner(record_id)
    if refs:
        ref_rows = []
        for ref in refs:
            ref_rows.append({
                "Entity Type": ref.get("entity_type", ""),
                "Header Number": ref.get("header_number", ""),
                "Field": ref.get("field_name", ""),
            })
        st.dataframe(pd.DataFrame(ref_rows), use_container_width=True, hide_index=True)
    else:
        st.caption("No styles or materials reference this partner.")

    st.divider()

    # Push-back (create/update in BeProduct)
    col_push, _ = st.columns([1, 3])
    if col_push.button("Push to BeProduct", type="primary"):
        with st.spinner("Pushing directory record to BeProduct..."):
            ok, msg = push_directory(record_id)
        if ok:
            st.success(msg)
        else:
            st.error(msg)

    st.caption("Note: Directory entries cannot be deleted via the API.")

    with st.expander("Raw JSON", expanded=False):
        st.json(data)
