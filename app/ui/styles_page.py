"""
Styles page: list view with search/filter + detail/edit view with push-back.
"""

from __future__ import annotations

import json
from typing import Any, Optional

import pandas as pd
import streamlit as st

from app import db
from app.push import push_style


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

    READONLY_TYPES = {"UserLabel", "Auto"}
    READONLY_IDS = {"created_by", "modified_by", "version"}

    edited_fields: list[dict] = []
    with st.form(key=f"style_form_{record_id}"):
        for field in fields_list:
            fid = field.get("id", "")
            fname = field.get("name", fid)
            ftype = field.get("type", "Text")
            fval = field.get("value") or ""
            readonly = ftype in READONLY_TYPES or fid in READONLY_IDS

            if readonly:
                st.text_input(fname, value=str(fval), disabled=True, key=f"sf_{fid}")
                edited_fields.append(field)
            elif ftype == "TrueFalse":
                new_val = st.checkbox(fname, value=str(fval).lower() in ("yes", "true", "1"), key=f"sf_{fid}")
                edited_fields.append({**field, "value": "Yes" if new_val else "No"})
            else:
                new_val = st.text_input(fname, value=str(fval), key=f"sf_{fid}")
                edited_fields.append({**field, "value": new_val})

        col_save, col_push = st.columns(2)
        save_clicked = col_save.form_submit_button("💾 Save Locally", use_container_width=True)
        push_clicked = col_push.form_submit_button("🚀 Push to BeProduct", use_container_width=True, type="primary")

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
            ok, msg = push_style(record_id)
        if ok:
            st.success(msg)
        else:
            st.error(msg)
        st.rerun()

    # ── Colorways ────────────────────────────────────────────────────────
    colorways = data.get("colorways", [])
    if colorways:
        st.divider()
        st.subheader("🎨 Colorways")
        cw_rows = []
        for cw in colorways:
            cw_rows.append({
                "Number": cw.get("colorNumber", ""),
                "Name": cw.get("colorName", ""),
                "Primary": cw.get("primaryColor", ""),
                "Secondary": cw.get("secondaryColor", ""),
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
