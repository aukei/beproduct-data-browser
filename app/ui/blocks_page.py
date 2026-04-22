"""
Blocks page: list view + detail with size classes.
Pattern mirrors styles_page.py.
"""

from __future__ import annotations

import json
from typing import Optional

import pandas as pd
import streamlit as st

from app import db, push
from app.ui._create_dialog import show_create_entity_dialog
from app.ui._delete_dialog import show_delete_confirmation_dialog
from app.ui._field_editor import render_field_form


# ── Raw JSON Dialog ──────────────────────────────────────────────────────
@st.dialog("📄 Raw JSON")
def raw_json_dialog(data: dict) -> None:
    """Show raw JSON in a modal dialog."""
    st.json(data)


def render_blocks_page() -> None:
    st.header("🧩 Blocks")

    selected_id = st.session_state.get("block_selected_id")
    if selected_id:
        _render_block_detail(selected_id)
        return

    _render_blocks_list()


def _render_blocks_list() -> None:
    st.subheader("All Blocks")

    # Create button
    col_create, col_refresh = st.columns(2)
    with col_create:
        if st.button("➕ Create New Block", use_container_width=True):
            st.session_state["show_create_block"] = True
    
    # Show create dialog if needed
    if st.session_state.get("show_create_block"):
        try:
            users = db.get_users(limit=500)
            directory = db.get_directory_records(limit=500)
        except Exception:
            users, directory = [], []
        
        show_create_entity_dialog(
            "Block",
            on_create_callback=push.create_block,
            users_list=users,
            directory_list=directory,
        )
        st.session_state["show_create_block"] = False

    col1, col2, col3 = st.columns([3, 2, 1])
    with col1:
        search = st.text_input("🔍 Search by number or name", key="blk_search")
    with col2:
        all_blks = db.get_blocks(limit=5000)
        folders: dict[str, str] = {}
        for b in all_blks:
            fid = b.get("folder_id") or ""
            fname = b.get("folder_name") or "(no folder)"
            if fid:
                folders[fid] = fname
        folder_options = ["All Folders"] + [f"{fname} ({fid[:8]}…)" for fid, fname in folders.items()]
        folder_sel = st.selectbox("📁 Folder", options=folder_options, key="blk_folder_sel")
    with col3:
        dirty_only = st.checkbox("Pending only", key="blk_dirty_only")

    selected_folder_id: Optional[str] = None
    if folder_sel != "All Folders":
        idx = folder_options.index(folder_sel) - 1
        selected_folder_id = list(folders.keys())[idx]

    blocks = db.get_blocks(
        folder_id=selected_folder_id,
        search=search or None,
        limit=500,
        dirty_only=dirty_only,
    )

    if not blocks:
        st.info("No blocks found. Run a sync to populate data from BeProduct.")
        return

    rows = []
    for b in blocks:
        rows.append({
            "ID": b["id"],
            "Number": b.get("header_number", ""),
            "Name": b.get("header_name", ""),
            "Folder": b.get("folder_name", ""),
            "Active": "✅" if b.get("active") else "❌",
            "Modified": (b.get("modified_at") or "")[:10],
            "Status": "🔴 Pending push" if b.get("is_dirty") else "✅ Synced",
        })

    df = pd.DataFrame(rows)
    st.caption(f"Showing {len(df)} record(s)")

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
            st.session_state["blocks_selected_row_idx"] = selected_row_idx

    # RIGHT: JSON detail panel
    with col_right:
        selected_row_idx = st.session_state.get("blocks_selected_row_idx")
        if selected_row_idx is not None and selected_row_idx < len(blocks):
            selected_block = blocks[selected_row_idx]
            blk_label = f"{selected_block.get('header_number', '')} — {selected_block.get('header_name', '')}"
            if st.button("✏️ Edit / View Details", key="blk_edit_btn", use_container_width=True, type="primary"):
                st.session_state["block_selected_id"] = selected_block["id"]
                st.rerun()
            with st.expander(f"📄 {blk_label}", expanded=True):
                raw_data = selected_block.get("data_json")
                if isinstance(raw_data, str):
                    raw_data = json.loads(raw_data)
                st.json(raw_data or selected_block)
        else:
            st.info("👈 Click a row to view raw JSON")


def _render_block_detail(record_id: str) -> None:
    row = db.get_block(record_id)
    if not row:
        st.error(f"Block {record_id} not found in local DB")
        st.session_state.pop("block_selected_id", None)
        return

    data = json.loads(row["data_json"])

    if st.button("← Back to list"):
        st.session_state.pop("block_selected_id", None)
        st.rerun()

    st.subheader(f"Block: {row.get('header_number', '')} — {row.get('header_name', '')}")

    if row.get("is_dirty"):
        st.warning("⚠️ This record has unpushed local changes.")

    # ── Action buttons ───────────────────────────────────────────────────
    col_actions1, col_actions2, col_actions3 = st.columns(3)
    with col_actions1:
        if st.button("🗑️ Delete Block", use_container_width=True, type="secondary"):
            st.session_state["show_delete_block"] = True
    with col_actions2:
        pass
    with col_actions3:
        pass

    # Show delete confirmation dialog
    if st.session_state.get("show_delete_block"):
        show_delete_confirmation_dialog(
            "Block",
            record_id,
            f"{row.get('header_number', '')} — {row.get('header_name', '')}",
            on_delete_callback=push.delete_block,
            referential_impacts=None,
        )
        st.session_state["show_delete_block"] = False

    # ── Status bar ───────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    col1.metric("Folder", row.get("folder_name", "—"))
    col2.metric("Active", "Yes" if row.get("active") else "No")
    col3.metric("Modified", (row.get("modified_at") or "")[:10])

    st.divider()

    st.subheader("📝 Attributes")
    header_data = data.get("headerData", {})
    fields_list = header_data.get("fields", [])

    # Try to get folder schema for better field rendering
    try:
        schema = None
        if row.get("folder_id"):
            from app.beproduct_client import get_client
            client = get_client()
            schema_list = client.schema.get_folder_schema("Block", row.get("folder_id"))
            schema = {s["field_id"]: s for s in schema_list}
    except Exception:
        schema = None

    edited_fields, save_clicked = render_field_form(
        fields_list,
        form_key=f"block_form_{record_id}",
        schema_dict=schema or {},
        users=db.get_users(limit=500),
        directory=db.get_directory_records(limit=500),
        show_submit=True,
        submit_label="💾 Save Locally",
        submit_type="secondary",
    )

    col_save, col_push = st.columns(2)
    with col_push:
        push_clicked = st.button("🚀 Push to BeProduct", use_container_width=True, type="primary")

    if save_clicked:
        updated_data = dict(data)
        updated_data["headerData"] = {**header_data, "fields": edited_fields}
        updated_data["headerName"] = next(
            (f["value"] for f in edited_fields if f["id"] == "header_name"), data.get("headerName")
        )
        db.update_block_local(record_id, updated_data)
        st.success("Saved locally. Click **Push to BeProduct** to sync the change.")
        st.rerun()

    if push_clicked:
        with st.spinner("Pushing to BeProduct…"):
            ok, msg = push.push_block(record_id)
        if ok:
            st.success(msg)
        else:
            st.error(msg)
        st.rerun()

    # ── Size Classes ─────────────────────────────────────────────────
    size_classes = header_data.get("sizeClasses", [])
    if size_classes:
        st.divider()
        st.subheader("📐 Size Classes")
        for sc in size_classes:
            with st.expander(f"**{sc.get('name', 'Unknown')}** (Active: {sc.get('active', True)})"):
                st.caption(f"Sizes: {sc.get('sizeRange', '')}")
                if sc.get("sizes"):
                    size_rows = []
                    for sz in sc["sizes"]:
                        size_rows.append({
                            "Name": sz.get("name", ""),
                            "Price": sz.get("price"),
                            "Currency": sz.get("currency", ""),
                            "Sample": "✅" if sz.get("isSampleSize") else "",
                        })
                    st.dataframe(pd.DataFrame(size_rows), use_container_width=True, hide_index=True)

    # ── Front Image preview ──────────────────────────────────────────
    front_image = header_data.get("frontImage") or {}
    if front_image:
        st.divider()
        st.subheader("🖼️ Front Image")
        preview_url = front_image.get("preview")
        if preview_url:
            st.image(preview_url, use_container_width=True)

    with st.expander("🔍 Raw JSON", expanded=False):
        st.json(data)
