"""
Directory page: list of vendors/factories/partners + detail view.
"""

from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from app import db
from app.push import push_directory


def render_directory_page() -> None:
    st.header("📒 Directory")

    selected_id = st.session_state.get("directory_selected_id")
    if selected_id:
        _render_directory_detail(selected_id)
        return

    _render_directory_list()


def _render_directory_list() -> None:
    st.subheader("Vendors, Factories & Partners")

    col1, col2 = st.columns([3, 2])
    with col1:
        search = st.text_input("🔍 Search by name or ID", key="dir_search")
    with col2:
        partner_types = ["All Types", "VENDOR", "FACTORY", "AGENT", "RETAILER", "OTHER"]
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
            "Active": "✅" if r.get("active") else "❌",
            "Last Synced": (r.get("synced_at") or "")[:10],
        })

    df = pd.DataFrame(rows)
    st.caption(f"Showing {len(df)} record(s)")

    event = st.dataframe(
        df.drop(columns=["ID"]),
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    if event and event.selection and event.selection.rows:
        row_idx = event.selection.rows[0]
        rec_id = df.iloc[row_idx]["ID"]
        st.session_state["directory_selected_id"] = rec_id
        st.rerun()


def _render_directory_detail(record_id: str) -> None:
    row = db.get_directory_record(record_id)
    if not row:
        st.error(f"Directory record {record_id} not found in local DB")
        st.session_state.pop("directory_selected_id", None)
        return

    data = json.loads(row["data_json"])

    if st.button("← Back to list"):
        st.session_state.pop("directory_selected_id", None)
        st.rerun()

    st.subheader(f"📒 {row.get('name', 'Unknown')}  ({row.get('partner_type', '')})")

    col1, col2, col3 = st.columns(3)
    col1.metric("Country", row.get("country", "—"))
    col2.metric("Active", "Yes" if row.get("active") else "No")
    col3.metric("Directory ID", row.get("directory_id", "—"))

    st.divider()

    # Address / contact info
    st.subheader("📍 Address")
    address_fields = ["address", "city", "state", "zip", "country", "phone", "fax", "website"]
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
        st.text_input("Fax", value=addr_data.get("fax", ""), disabled=True)
        st.text_input("Website", value=addr_data.get("website", ""), disabled=True)

    # Contacts
    contacts = data.get("contacts", [])
    if contacts:
        st.divider()
        st.subheader("👤 Contacts")
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

    # Push-back (create/update in BeProduct)
    col_push, _ = st.columns([1, 3])
    if col_push.button("🚀 Push to BeProduct", type="primary"):
        with st.spinner("Pushing directory record to BeProduct…"):
            ok, msg = push_directory(record_id)
        if ok:
            st.success(msg)
        else:
            st.error(msg)

    with st.expander("🔍 Raw JSON", expanded=False):
        st.json(data)
