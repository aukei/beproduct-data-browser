"""
Color Palettes page: list + detail/edit with push-back.
"""

from __future__ import annotations

import json
from typing import Optional

import pandas as pd
import streamlit as st

from app import db
from app.push import push_color


# ── Raw JSON Dialog ──────────────────────────────────────────────────────
@st.dialog("📄 Raw JSON")
def raw_json_dialog(data: dict) -> None:
    """Show raw JSON in a modal dialog."""
    st.json(data)


def _show_raw_button(color_id: str, raw_data: dict, label: str) -> None:
    """Render a raw JSON button for a color row."""
    if st.button("📄", key=f"raw_{color_id}", help=f"View raw JSON for {label}"):
        raw_json_dialog(raw_data)


def render_colors_page() -> None:
    st.header("🎨 Color Palettes")

    selected_id = st.session_state.get("color_selected_id")
    if selected_id:
        _render_color_detail(selected_id)
        return

    _render_colors_list()


def _render_colors_list() -> None:
    st.subheader("All Color Palettes")

    col1, col2, col3 = st.columns([3, 2, 1])
    with col1:
        search = st.text_input("🔍 Search by number or name", key="col_search")
    with col2:
        all_cols = db.get_colors(limit=5000)
        folders: dict[str, str] = {}
        for c in all_cols:
            fid = c.get("folder_id") or ""
            fname = c.get("folder_name") or "(no folder)"
            if fid:
                folders[fid] = fname
        folder_options = ["All Folders"] + [f"{fname} ({fid[:8]}…)" for fid, fname in folders.items()]
        folder_sel = st.selectbox("📁 Folder", options=folder_options, key="col_folder_sel")
    with col3:
        dirty_only = st.checkbox("Pending only", key="col_dirty_only")

    selected_folder_id: Optional[str] = None
    if folder_sel != "All Folders":
        idx = folder_options.index(folder_sel) - 1
        selected_folder_id = list(folders.keys())[idx]

    colors = db.get_colors(
        folder_id=selected_folder_id,
        search=search or None,
        limit=500,
        dirty_only=dirty_only,
    )

    if not colors:
        st.info("No color palettes found. Run a sync to populate data from BeProduct.")
        return

    rows = []
    for c in colors:
        rows.append({
            "ID": c["id"],
            "Number": c.get("header_number", ""),
            "Name": c.get("header_name", ""),
            "Folder": c.get("folder_name", ""),
            "Active": "✅" if c.get("active") else "❌",
            "Modified": (c.get("modified_at") or "")[:10],
            "Status": "🔴 Pending push" if c.get("is_dirty") else "✅ Synced",
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
            st.session_state["colors_selected_row_idx"] = selected_row_idx

    # RIGHT: JSON detail panel
    with col_right:
        selected_row_idx = st.session_state.get("colors_selected_row_idx")
        if selected_row_idx is not None and selected_row_idx < len(colors):
            selected_color = colors[selected_row_idx]
            color_label = f"{selected_color.get('header_number', '')} — {selected_color.get('header_name', '')}"
            with st.expander(f"📄 {color_label}", expanded=True):
                raw_data = selected_color.get("data_json")
                if isinstance(raw_data, str):
                    raw_data = json.loads(raw_data)
                st.json(raw_data or selected_color)
        else:
            st.info("👈 Click a row to view raw JSON")


def _render_color_detail(record_id: str) -> None:
    row = db.get_color(record_id)
    if not row:
        st.error(f"Color palette {record_id} not found in local DB")
        st.session_state.pop("color_selected_id", None)
        return

    data = json.loads(row["data_json"])

    if st.button("← Back to list"):
        st.session_state.pop("color_selected_id", None)
        st.rerun()

    st.subheader(f"Palette: {row.get('header_number', '')} — {row.get('header_name', '')}")

    if row.get("is_dirty"):
        st.warning("⚠️ This record has unpushed local changes.")

    col1, col2, col3 = st.columns(3)
    col1.metric("Folder", row.get("folder_name", "—"))
    col2.metric("Active", "Yes" if row.get("active") else "No")
    col3.metric("Modified", (row.get("modified_at") or "")[:10])

    st.divider()

    # Editable attribute fields
    st.subheader("📝 Attributes")
    header_data = data.get("headerData", {})
    fields_list = header_data.get("fields", [])
    READONLY_TYPES = {"UserLabel", "Auto"}
    READONLY_IDS = {"created_by", "modified_by", "version"}

    edited_fields = []
    with st.form(key=f"col_form_{record_id}"):
        for field in fields_list:
            fid = field.get("id", "")
            fname = field.get("name", fid)
            ftype = field.get("type", "Text")
            fval = field.get("value") or ""
            readonly = ftype in READONLY_TYPES or fid in READONLY_IDS

            if readonly:
                st.text_input(fname, value=str(fval), disabled=True, key=f"cf_{fid}")
                edited_fields.append(field)
            elif ftype == "TrueFalse":
                new_val = st.checkbox(fname, value=str(fval).lower() in ("yes", "true", "1"), key=f"cf_{fid}")
                edited_fields.append({**field, "value": "Yes" if new_val else "No"})
            else:
                new_val = st.text_input(fname, value=str(fval), key=f"cf_{fid}")
                edited_fields.append({**field, "value": new_val})

        col_save, col_push = st.columns(2)
        save_clicked = col_save.form_submit_button("💾 Save Locally", use_container_width=True)
        push_clicked = col_push.form_submit_button("🚀 Push to BeProduct", use_container_width=True, type="primary")

    if save_clicked:
        updated_data = dict(data)
        updated_data["headerData"] = {**header_data, "fields": edited_fields}
        # Color palettes use colorPaletteName, not headerName
        updated_data["colorPaletteName"] = next(
            (f["value"] for f in edited_fields if f["id"] == "header_name"), data.get("colorPaletteName")
        )
        db.update_color_local(record_id, updated_data)
        st.success("Saved locally. Click **Push to BeProduct** to sync.")
        st.rerun()

    if push_clicked:
        with st.spinner("Pushing to BeProduct…"):
            ok, msg = push_color(record_id)
        if ok:
            st.success(msg)
        else:
            st.error(msg)
        st.rerun()

    # Color swatches table
    # Colors are now nested at headerData.colors.colors (not at top level)
    colors_wrapper = data.get("headerData", {}).get("colors") or {}
    colors_list = colors_wrapper.get("colors", [])
    if colors_list:
        st.divider()
        st.subheader("🖌️ Colors in Palette")
        color_rows = []
        for c in colors_list:
            hex_val = c.get("hex", "")
            color_rows.append({
                "Number": c.get("color_number", ""),
                "Name": c.get("color_name", ""),
                "Hex": f"#{hex_val}" if hex_val and not hex_val.startswith("#") else hex_val,
            })
        st.dataframe(
            pd.DataFrame(color_rows),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Hex": st.column_config.TextColumn("Hex Color"),
            },
        )

    with st.expander("🔍 Raw JSON", expanded=False):
        st.json(data)
