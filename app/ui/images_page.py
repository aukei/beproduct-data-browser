"""
Images page: list view with search/filter + detail/edit view with push-back.
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


def _show_raw_button(img_id: str, raw_data: dict, label: str) -> None:
    """Render a raw JSON button for an image row."""
    if st.button("📄", key=f"raw_{img_id}", help=f"View raw JSON for {label}"):
        raw_json_dialog(raw_data)


def render_images_page() -> None:
    st.header("🖼️ Images")

    selected_id = st.session_state.get("image_selected_id")
    if selected_id:
        _render_image_detail(selected_id)
        return

    _render_images_list()


def _render_images_list() -> None:
    st.subheader("All Images")

    # Create button
    col_create, col_refresh = st.columns(2)
    with col_create:
        if st.button("➕ Create New Image", use_container_width=True):
            st.session_state["show_create_image"] = True
    
    # Show create dialog if needed
    if st.session_state.get("show_create_image"):
        try:
            users = db.get_users(limit=500)
            directory = db.get_directory_records(limit=500)
        except Exception:
            users, directory = [], []
        
        show_create_entity_dialog(
            "Image",
            on_create_callback=push.create_image,
            users_list=users,
            directory_list=directory,
        )
        st.session_state["show_create_image"] = False

    col1, col2, col3 = st.columns([3, 2, 1])
    with col1:
        search = st.text_input("🔍 Search by number or name", key="img_search")
    with col2:
        all_imgs = db.get_images(limit=5000)
        folders: dict[str, str] = {}
        for i in all_imgs:
            fid = i.get("folder_id") or ""
            fname = i.get("folder_name") or "(no folder)"
            if fid:
                folders[fid] = fname
        folder_options = ["All Folders"] + [f"{fname} ({fid[:8]}…)" for fid, fname in folders.items()]
        folder_sel = st.selectbox("📁 Folder", options=folder_options, key="img_folder_sel")
    with col3:
        dirty_only = st.checkbox("Pending only", key="img_dirty_only")

    selected_folder_id: Optional[str] = None
    if folder_sel != "All Folders":
        idx = folder_options.index(folder_sel) - 1
        selected_folder_id = list(folders.keys())[idx]

    images = db.get_images(
        folder_id=selected_folder_id,
        search=search or None,
        limit=500,
        dirty_only=dirty_only,
    )

    if not images:
        st.info("No images found. Run a sync to populate data from BeProduct.")
        return

    rows = []
    for i in images:
        rows.append({
            "ID": i["id"],
            "Number": i.get("header_number", ""),
            "Name": i.get("header_name", ""),
            "Folder": i.get("folder_name", ""),
            "Active": "✅" if i.get("active") else "❌",
            "Modified": (i.get("modified_at") or "")[:10],
            "Status": "🔴 Pending push" if i.get("is_dirty") else "✅ Synced",
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
            st.session_state["images_selected_row_idx"] = selected_row_idx

    # RIGHT: JSON detail panel
    with col_right:
        selected_row_idx = st.session_state.get("images_selected_row_idx")
        if selected_row_idx is not None and selected_row_idx < len(images):
            selected_image = images[selected_row_idx]
            img_label = f"{selected_image.get('header_number', '')} — {selected_image.get('header_name', '')}"
            if st.button("✏️ Edit / View Details", key="img_edit_btn", use_container_width=True, type="primary"):
                st.session_state["image_selected_id"] = selected_image["id"]
                st.rerun()
            with st.expander(f"📄 {img_label}", expanded=True):
                raw_data = selected_image.get("data_json")
                if isinstance(raw_data, str):
                    raw_data = json.loads(raw_data)
                st.json(raw_data or selected_image)
        else:
            st.info("👈 Click a row to view raw JSON")


def _render_image_detail(record_id: str) -> None:
    row = db.get_image(record_id)
    if not row:
        st.error(f"Image {record_id} not found in local DB")
        st.session_state.pop("image_selected_id", None)
        return

    data = json.loads(row["data_json"])

    if st.button("← Back to list"):
        st.session_state.pop("image_selected_id", None)
        st.rerun()

    st.subheader(f"Image: {row.get('header_number', '')} — {row.get('header_name', '')}")

    if row.get("is_dirty"):
        st.warning("⚠️ This record has unpushed local changes.")

    # ── Action buttons ───────────────────────────────────────────────────
    col_actions1, col_actions2, col_actions3 = st.columns(3)
    with col_actions1:
        if st.button("🗑️ Delete Image", use_container_width=True, type="secondary"):
            st.session_state["show_delete_image"] = True
    with col_actions2:
        pass
    with col_actions3:
        pass

    # Show delete confirmation dialog
    if st.session_state.get("show_delete_image"):
        show_delete_confirmation_dialog(
            "Image",
            record_id,
            f"{row.get('header_number', '')} — {row.get('header_name', '')}",
            on_delete_callback=push.delete_image,
            referential_impacts=None,
        )
        st.session_state["show_delete_image"] = False

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
            schema_list = client.schema.get_folder_schema("Image", row.get("folder_id"))
            schema = {s["field_id"]: s for s in schema_list}
    except Exception:
        schema = None

    edited_fields, save_clicked = render_field_form(
        fields_list,
        form_key=f"image_form_{record_id}",
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
        db.update_image_local(record_id, updated_data)
        st.success("Saved locally. Click **Push to BeProduct** to sync the change.")
        st.rerun()

    if push_clicked:
        with st.spinner("Pushing to BeProduct…"):
            ok, msg = push.push_image(record_id)
        if ok:
            st.success(msg)
        else:
            st.error(msg)
        st.rerun()

    # ── Image preview ────────────────────────────────────────────────
    preview_data = header_data.get("preview") or {}
    if preview_data:
        st.divider()
        st.subheader("🖼️ Preview")
        preview_url = preview_data.get("preview")
        if preview_url:
            st.image(preview_url, use_container_width=True)

    # ── Raw JSON expander ────────────────────────────────────────────
    with st.expander("🔍 Raw JSON", expanded=False):
        st.json(data)
