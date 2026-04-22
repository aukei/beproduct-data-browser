"""
Styles page: list view with search/filter + detail/edit view with push-back.
Includes create/delete functionality with cross-reference warnings.
"""

from __future__ import annotations

import json
from typing import Any, Optional

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


def _show_raw_button(style_id: str, raw_data: dict, label: str) -> None:
    """Render a raw JSON button for a style row."""
    if st.button("📄", key=f"raw_{style_id}", help=f"View raw JSON for {label}"):
        raw_json_dialog(raw_data)


def render_styles_page() -> None:
    st.header("👗 Styles")

    # ── Check if viewing a detail ──────────────────────────────────────────
    selected_id = st.session_state.get("style_selected_id")

    if selected_id:
        _render_style_detail(selected_id)
        return

    # ── List / search view ─────────────────────────────────────────────────
    _render_styles_list()


def _render_styles_list() -> None:
    st.subheader("All Styles")

    # Create button
    col_create, col_refresh = st.columns(2)
    with col_create:
        if st.button("➕ Create New Style", use_container_width=True):
            st.session_state["show_create_style"] = True
    
    # Show create dialog if needed
    if st.session_state.get("show_create_style"):
        try:
            users = db.get_users(limit=500)
            directory = db.get_directory_records(limit=500)
        except Exception:
            users, directory = [], []
        
        show_create_entity_dialog(
            "Style",
            on_create_callback=push.create_style,
            users_list=users,
            directory_list=directory,
        )
        st.session_state["show_create_style"] = False

    # Filters row
    col1, col2, col3 = st.columns([3, 2, 1])
    with col1:
        search = st.text_input("🔍 Search by style number or name", key="style_search")
    with col2:
        # Build folder options from DB
        all_styles = db.get_styles(limit=5000)
        folders: dict[str, str] = {}
        for s in all_styles:
            fid = s.get("folder_id") or ""
            fname = s.get("folder_name") or "(no folder)"
            if fid:
                folders[fid] = fname
        folder_options = ["All Folders"] + [f"{fname} ({fid[:8]}…)" for fid, fname in folders.items()]
        folder_sel = st.selectbox("📁 Folder", options=folder_options, key="style_folder_sel")
    with col3:
        dirty_only = st.checkbox("Pending only", key="style_dirty_only")

    # Resolve folder_id from selection
    selected_folder_id: Optional[str] = None
    if folder_sel != "All Folders":
        idx = folder_options.index(folder_sel) - 1
        selected_folder_id = list(folders.keys())[idx]

    # Fetch
    styles = db.get_styles(
        folder_id=selected_folder_id,
        search=search or None,
        limit=500,
        dirty_only=dirty_only,
    )

    if not styles:
        st.info("No styles found. Run a sync to populate data from BeProduct.")
        return

    # Store styles in session_state for button callbacks
    if "styles_cache" not in st.session_state:
        st.session_state["styles_cache"] = styles

    # Build display dataframe
    rows = []
    for s in styles:
        rows.append({
            "ID": s["id"],
            "Number": s.get("header_number", ""),
            "Name": s.get("header_name", ""),
            "Folder": s.get("folder_name", ""),
            "Active": "✅" if s.get("active") else "❌",
            "Modified": (s.get("modified_at") or "")[:10],
            "Status": "🔴 Pending push" if s.get("is_dirty") else "✅ Synced",
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
            st.session_state["styles_selected_row_idx"] = selected_row_idx

    # RIGHT: JSON detail panel
    with col_right:
        selected_row_idx = st.session_state.get("styles_selected_row_idx")
        if selected_row_idx is not None and selected_row_idx < len(styles):
            selected_style = styles[selected_row_idx]
            style_label = f"{selected_style.get('header_number', '')} — {selected_style.get('header_name', '')}"
            if st.button("✏️ Edit / View Details", key="style_edit_btn", use_container_width=True, type="primary"):
                st.session_state["style_selected_id"] = selected_style["id"]
                st.rerun()
            with st.expander(f"📄 {style_label}", expanded=True):
                raw_data = selected_style.get("data_json")
                if isinstance(raw_data, str):
                    raw_data = json.loads(raw_data)
                st.json(raw_data or selected_style)
        else:
            st.info("👈 Click a row to view raw JSON")


def _render_style_detail(record_id: str) -> None:
    row = db.get_style(record_id)
    if not row:
        st.error(f"Style {record_id} not found in local DB")
        st.session_state.pop("style_selected_id", None)
        return

    data = json.loads(row["data_json"])

    # Back button
    if st.button("← Back to list"):
        st.session_state.pop("style_selected_id", None)
        st.rerun()

    st.subheader(f"Style: {row.get('header_number', '')} — {row.get('header_name', '')}")

    if row.get("is_dirty"):
        st.warning("⚠️ This record has unpushed local changes.")

    # ── Action buttons ───────────────────────────────────────────────────
    col_actions1, col_actions2, col_actions3 = st.columns(3)
    with col_actions1:
        if st.button("🗑️ Delete Style", use_container_width=True, type="secondary"):
            st.session_state["show_delete_style"] = True
    with col_actions2:
        pass
    with col_actions3:
        pass

    # Show delete confirmation dialog
    if st.session_state.get("show_delete_style"):
        # Get referential impacts
        impacts_color = db.get_colorways_referencing_color(record_id) if record_id else []
        impacts_image = db.get_colorways_referencing_image(record_id) if record_id else []
        all_impacts = impacts_color + impacts_image
        
        show_delete_confirmation_dialog(
            "Style",
            record_id,
            f"{row.get('header_number', '')} — {row.get('header_name', '')}",
            on_delete_callback=push.delete_style,
            referential_impacts=all_impacts if all_impacts else None,
        )
        st.session_state["show_delete_style"] = False

    # ── Status bar ───────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    col1.metric("Folder", row.get("folder_name", "—"))
    col2.metric("Active", "Yes" if row.get("active") else "No")
    col3.metric("Modified", (row.get("modified_at") or "")[:10])

    st.divider()

    # ── Editable attribute fields ────────────────────────────────────────
    st.subheader("📝 Attributes")
    header_data = data.get("headerData", {})
    fields_list = header_data.get("fields", [])

    # Try to get folder schema for better field rendering
    try:
        schema = None
        if row.get("folder_id"):
            from app.beproduct_client import get_client
            client = get_client()
            schema_list = client.schema.get_folder_schema("Style", row.get("folder_id"))
            schema = {s["field_id"]: s for s in schema_list}
    except Exception:
        schema = None

    edited_fields, save_clicked = render_field_form(
        fields_list,
        form_key=f"style_form_{record_id}",
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
        db.update_style_local(record_id, updated_data)
        st.success("Saved locally. Click **Push to BeProduct** to sync the change.")
        st.rerun()

    if push_clicked:
        with st.spinner("Pushing to BeProduct…"):
            ok, msg = push.push_style(record_id)
        if ok:
            st.success(msg)
        else:
            st.error(msg)
        st.rerun()

    # ── Colorways with cross-references ──────────────────────────────────
    colorways = data.get("colorways", [])
    if colorways:
        st.divider()
        st.subheader("🎨 Colorways")
        cw_rows = []
        for cw in colorways:
            cw_id = cw.get("id", "")
            color_source_id = cw.get("colorSourceId", "")
            image_header_id = cw.get("imageHeaderId", "")
            
            cw_rows.append({
                "Number": cw.get("colorNumber", ""),
                "Name": cw.get("colorName", ""),
                "Primary": cw.get("primaryColor", ""),
                "Secondary": cw.get("secondaryColor", ""),
                "Color Ref": color_source_id[:8] + "…" if color_source_id else "—",
                "Image Ref": image_header_id[:8] + "…" if image_header_id else "—",
                "Hidden": "Yes" if cw.get("hideColorway") else "No",
            })
        st.dataframe(pd.DataFrame(cw_rows), use_container_width=True, hide_index=True)

    # ── Size range ───────────────────────────────────────────────────────
    sizes = data.get("sizeRange", [])
    if sizes:
        st.divider()
        st.subheader("📐 Sizes")
        size_rows = []
        for sz in sizes:
            size_rows.append({
                "Name": sz.get("name", ""),
                "Price": sz.get("price"),
                "Currency": sz.get("currency", ""),
                "Unit": sz.get("unitOfMeasure", ""),
                "Sample": "✅" if sz.get("isSampleSize") else "",
                "Comments": sz.get("comments", ""),
            })
        st.dataframe(pd.DataFrame(size_rows), use_container_width=True, hide_index=True)

    # ── Raw JSON expander ────────────────────────────────────────────────
    with st.expander("🔍 Raw JSON", expanded=False):
        st.json(data)
