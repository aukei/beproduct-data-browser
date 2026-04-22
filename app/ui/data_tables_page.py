"""
Data Tables page: list data tables, view/edit rows, add/delete rows.

Data Tables are custom lookup/mapping tables in BeProduct (e.g., "E1 Color Codes").
The SDK has no wrapper — all operations use client.raw_api.
"""

from __future__ import annotations

import json
from typing import Any, Optional

import pandas as pd
import streamlit as st

from app import db
from app.push import (
    push_data_table_row,
    add_data_table_row,
    delete_data_table_row as push_delete_dt_row,
)


def render_data_tables_page() -> None:
    st.header("Data Tables")
    st.caption("Custom lookup/mapping tables from BeProduct. Accessed via raw API.")

    selected_table_id = st.session_state.get("dt_selected_table_id")
    if selected_table_id:
        _render_data_table_detail(selected_table_id)
        return

    _render_data_tables_list()


def _render_data_tables_list() -> None:
    st.subheader("All Data Tables")

    search = st.text_input("Search by name", key="dt_search")

    tables = db.get_data_tables(search=search or None, limit=500)

    if not tables:
        st.info("No data tables found. Run a sync to populate data from BeProduct.")
        return

    rows = []
    for t in tables:
        rows.append({
            "ID": t["id"],
            "Name": t.get("name", ""),
            "Description": t.get("description", ""),
            "Active": "Yes" if t.get("active") else "No",
            "Modified": (t.get("modified_at") or "")[:10],
        })

    df = pd.DataFrame(rows)
    st.caption(f"Showing {len(df)} data table(s)")

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
            st.session_state["dt_selected_row_idx"] = selected_row_idx

    with col_right:
        selected_row_idx = st.session_state.get("dt_selected_row_idx")
        if selected_row_idx is not None and selected_row_idx < len(tables):
            selected_table = tables[selected_row_idx]
            if st.button("View / Edit Rows", key="dt_view_btn", use_container_width=True, type="primary"):
                st.session_state["dt_selected_table_id"] = selected_table["id"]
                st.rerun()
            st.caption(f"**{selected_table.get('name', '')}**")
            st.caption(selected_table.get("description", ""))

            # Show row count
            rows_in_table = db.get_data_table_rows(selected_table["id"], limit=5000)
            st.metric("Rows", len(rows_in_table))
        else:
            st.info("Click a table to view its rows")


def _render_data_table_detail(table_id: str) -> None:
    table = db.get_data_table(table_id)
    if not table:
        st.error(f"Data table {table_id} not found in local DB")
        st.session_state.pop("dt_selected_table_id", None)
        return

    if st.button("< Back to table list"):
        st.session_state.pop("dt_selected_table_id", None)
        st.rerun()

    st.subheader(f"Data Table: {table.get('name', 'Unknown')}")
    st.caption(table.get("description", ""))
    st.caption(f"**ID:** `{table_id}`")

    st.divider()

    # Fetch rows
    dt_rows = db.get_data_table_rows(table_id, limit=5000)

    if not dt_rows:
        st.info("No rows in this data table.")
        _render_add_row_form(table_id)
        return

    # Parse rows into a display table
    display_rows = []
    field_names: list[str] = []

    for dt_row in dt_rows:
        data = json.loads(dt_row["data_json"]) if isinstance(dt_row.get("data_json"), str) else dt_row.get("data_json", {})
        fields = data.get("fields", [])

        row_dict: dict[str, Any] = {"_row_id": dt_row["id"]}
        for field in fields:
            fname = field.get("name", field.get("id", ""))
            fval = field.get("value", "")
            row_dict[fname] = fval
            if fname and fname not in field_names:
                field_names.append(fname)

        dirty_flag = " [dirty]" if dt_row.get("is_dirty") else ""
        row_dict["Status"] = f"Pending{dirty_flag}" if dt_row.get("is_dirty") else "Synced"
        display_rows.append(row_dict)

    if display_rows:
        df = pd.DataFrame(display_rows)
        st.caption(f"Showing {len(df)} row(s)")

        # Show the table (hide internal _row_id)
        display_cols = [c for c in df.columns if c != "_row_id"]
        event = st.dataframe(
            df[display_cols],
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
        )

        # Handle row selection for editing/deleting
        selected_idx: Optional[int] = None
        if event and event.selection and event.selection.rows:
            selected_idx = event.selection.rows[0]

        if selected_idx is not None and selected_idx < len(display_rows):
            selected_row = display_rows[selected_idx]
            row_id = selected_row["_row_id"]

            st.divider()
            st.subheader("Selected Row")

            col_edit, col_delete = st.columns(2)
            with col_delete:
                if st.button("Delete Row", type="secondary"):
                    with st.spinner("Deleting row..."):
                        ok, msg = push_delete_dt_row(table_id, row_id)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

            # Show row fields for editing
            dt_row_data = next((r for r in dt_rows if r["id"] == row_id), None)
            if dt_row_data:
                data = json.loads(dt_row_data["data_json"]) if isinstance(dt_row_data.get("data_json"), str) else dt_row_data.get("data_json", {})
                fields = data.get("fields", [])

                with st.form(key=f"dt_row_edit_{row_id}"):
                    edited_fields = []
                    for field in fields:
                        fid = field.get("id", "")
                        fname = field.get("name", fid)
                        fval = field.get("value", "")
                        new_val = st.text_input(fname, value=str(fval) if fval else "", key=f"dtr_{fid}_{row_id}")
                        edited_fields.append({"id": fid, "value": new_val})

                    if st.form_submit_button("Push Row Update", use_container_width=True, type="primary"):
                        with st.spinner("Pushing row update..."):
                            ok, msg = push_data_table_row(table_id, row_id, edited_fields)
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)

    st.divider()
    _render_add_row_form(table_id)


def _render_add_row_form(table_id: str) -> None:
    """Render form to add a new row to the data table."""
    st.subheader("Add New Row")

    # Try to get schema for column names
    schema_fields: list[dict] = []
    try:
        from app.beproduct_client import get_client
        client = get_client()
        schema = client.raw_api.get(f"DataTable/{table_id}/Schema")
        if isinstance(schema, list):
            schema_fields = schema
        elif isinstance(schema, dict):
            schema_fields = schema.get("fields", schema.get("columns", []))
    except Exception:
        pass

    if not schema_fields:
        # Fallback: infer columns from existing rows
        existing_rows = db.get_data_table_rows(table_id, limit=1)
        if existing_rows:
            data = json.loads(existing_rows[0]["data_json"]) if isinstance(existing_rows[0].get("data_json"), str) else existing_rows[0].get("data_json", {})
            for f in data.get("fields", []):
                schema_fields.append({"id": f.get("id", ""), "name": f.get("name", f.get("id", ""))})

    if not schema_fields:
        st.caption("No schema available. Add at least one row manually to infer columns.")
        with st.form(key="dt_add_row_manual"):
            field_id = st.text_input("Field ID")
            field_val = st.text_input("Field Value")
            if st.form_submit_button("Add Row"):
                if field_id:
                    with st.spinner("Adding row..."):
                        ok, msg, new_id = add_data_table_row(table_id, [{"id": field_id, "value": field_val}])
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
        return

    with st.form(key="dt_add_row"):
        new_fields = []
        for sf in schema_fields:
            fid = sf.get("id", sf.get("fieldId", ""))
            fname = sf.get("name", sf.get("fieldName", fid))
            val = st.text_input(fname, key=f"dt_new_{fid}")
            new_fields.append({"id": fid, "value": val})

        if st.form_submit_button("Add Row", type="primary", use_container_width=True):
            with st.spinner("Adding row..."):
                ok, msg, new_id = add_data_table_row(table_id, new_fields)
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)
