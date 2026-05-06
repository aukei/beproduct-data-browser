"""
Shared schema-based field editor component.

Renders form fields based on their type and schema metadata.
Supports all BeProduct field types with appropriate Streamlit widgets.
Auto-loads directory partners and users from local DB when not provided.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Optional

import streamlit as st

from app import db

logger = logging.getLogger(__name__)


# Field types that are always read-only (system-managed)
READONLY_FIELD_TYPES = frozenset({
    "UserLabel",       # created_by, modified_by
    "LabelText",       # season_year, factory_id_no, etc.
    "LabelMaterial",   # core_main_material
    "LabelSize",       # core_size_range
    "LabelStyleGroup", # core_style_group
    "Label3dStyle",    # core_3d_style
    "Label3dMaterial", # core_3d_material
    "FormulaField",    # calculated fields
    "Auto",            # auto-generated
})

# Field IDs that should always be read-only
READONLY_FIELD_IDS = frozenset({"created_by", "modified_by", "version"})


# ---------------------------------------------------------------------------
# Lazy-loaded lookup caches (populated once per render cycle)
# ---------------------------------------------------------------------------

def _get_directory_partners() -> list[dict[str, str]]:
    """Load directory partners from local DB for PartnerDropDown fields."""
    records = db.get_directory_records(limit=5000)
    return [
        {"id": r["id"], "name": r.get("name", r.get("directory_id", r["id"]))}
        for r in records
    ]


def _extract_user_name(user: dict[str, Any]) -> str:
    """Extract display name from user dict (handles both formatted and raw DB formats)."""
    # If 'name' key exists, use it (pre-formatted)
    if "name" in user:
        return user["name"]
    # Otherwise construct from first_name, last_name, username
    constructed = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
    return constructed or user.get("username", "")


def _get_user_options() -> list[dict[str, str]]:
    """Load users from local DB for Users-type fields."""
    users = db.get_users(limit=5000)
    return [
        {
            "id": u["id"],
            "name": _extract_user_name(u),
            "email": u.get("email", ""),
        }
        for u in users
    ]


def _format_value_for_display(value: Any) -> str:
    """Format a field value for display in a disabled text input."""
    if value is None:
        return ""
    if isinstance(value, dict):
        return value.get("value") or value.get("name") or str(value)
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(item.get("value") or item.get("name") or str(item))
            else:
                parts.append(str(item))
        return ", ".join(parts)
    return str(value)


# ---------------------------------------------------------------------------
# Per-field renderer
# ---------------------------------------------------------------------------

def render_field(
    field: dict[str, Any],
    key_prefix: str,
    schema: Optional[dict[str, Any]] = None,
    users_list: Optional[list[dict]] = None,
    directory_list: Optional[list[dict]] = None,
) -> dict[str, Any]:
    """
    Render a single field based on its type and return the updated field dict.

    Args:
        field: Field dict with keys: id, name, type, value, required, formula (optional)
        key_prefix: Unique prefix for streamlit widget keys (e.g., "style_form_1")
        schema: Optional schema metadata dict with possible_values, data_type, etc.
        users_list: Optional list of user dicts for Users field type (auto-loaded if None)
        directory_list: Optional list of directory dicts for PartnerDropDown (auto-loaded if None)

    Returns:
        Updated field dict with new value
    """
    fid = field.get("id", "")
    fname = field.get("name", fid)
    ftype = field.get("type", "Text")
    fval = field.get("value")
    required = field.get("required", False)
    formula = field.get("formula", "")

    is_readonly = ftype in READONLY_FIELD_TYPES or fid in READONLY_FIELD_IDS
    widget_key = f"{key_prefix}_{fid}"
    label = f"{fname} *" if required else fname

    # ── Read-Only Fields ──────────────────────────────────────────────────
    if is_readonly:
        display_val = _format_value_for_display(fval)
        if ftype == "FormulaField" and formula:
            st.text_input(label, value=display_val or f"[Formula: {formula}]", disabled=True, key=widget_key, help="Read-only calculated field")
        else:
            st.text_input(label, value=display_val, disabled=True, key=widget_key)
        return field

    # ── Text Input ────────────────────────────────────────────────────────
    if ftype == "Text":
        new_val = st.text_input(label, value=str(fval) if fval else "", key=widget_key)
        return {**field, "value": new_val}

    # ── Memo (Multi-line Text) ────────────────────────────────────────────
    if ftype == "Memo":
        new_val = st.text_area(label, value=str(fval) if fval else "", key=widget_key, height=100)
        return {**field, "value": new_val}

    # ── True/False (Checkbox) ─────────────────────────────────────────────
    if ftype == "TrueFalse":
        is_checked = str(fval).lower() in ("yes", "true", "1", "on") if fval else False
        new_val = st.checkbox(label, value=is_checked, key=widget_key)
        return {**field, "value": "Yes" if new_val else "No"}

    # ── DropDown (Single Select) ──────────────────────────────────────────
    if ftype == "DropDown":
        options = []
        if schema and "possible_values" in schema:
            options = [opt.get("value", opt.get("id", "")) for opt in schema["possible_values"] if isinstance(opt, dict)]

        current_val = _format_value_for_display(fval)

        if options:
            # Ensure current value is in options
            if current_val and current_val not in options:
                options = [current_val] + options
            all_options = [""] + options
            idx = all_options.index(current_val) if current_val in all_options else 0
            new_val = st.selectbox(label, options=all_options, index=idx, key=widget_key)
            return {**field, "value": new_val if new_val else ""}
        else:
            # Fallback to text input when no schema options available
            new_val = st.text_input(label, value=current_val, key=widget_key, help=f"DropDown — no options loaded (sync schema for selection)")
            return {**field, "value": new_val}

    # ── MultiSelect ───────────────────────────────────────────────────────
    if ftype == "MultiSelect":
        options = []
        if schema and "possible_values" in schema:
            options = [opt.get("value", opt.get("id", "")) for opt in schema["possible_values"] if isinstance(opt, dict)]

        current_vals = []
        if fval:
            if isinstance(fval, list):
                for item in fval:
                    if isinstance(item, dict):
                        current_vals.append(item.get("value") or item.get("code") or str(item))
                    else:
                        current_vals.append(str(item))
            elif isinstance(fval, str):
                current_vals = [v.strip() for v in fval.split(",") if v.strip()]

        if options:
            # Add current values not in options
            for cv in current_vals:
                if cv not in options:
                    options.append(cv)
            new_vals = st.multiselect(label, options=options, default=current_vals, key=widget_key)
            return {**field, "value": new_vals}
        else:
            # Fallback: comma-separated text
            new_val = st.text_input(label, value=", ".join(current_vals), key=widget_key, help="MultiSelect — no options loaded (comma-separated)")
            return {**field, "value": [v.strip() for v in new_val.split(",") if v.strip()] if new_val else []}

    # ── ComboBox (Dropdown with freetext) ─────────────────────────────────
    if ftype == "ComboBox":
        options = []
        if schema and "possible_values" in schema:
            options = [opt.get("value", opt.get("id", "")) for opt in schema["possible_values"] if isinstance(opt, dict)]

        current_val = _format_value_for_display(fval)

        if options:
            all_options = ["(Custom)"] + options
            if current_val in options:
                idx = all_options.index(current_val)
            else:
                idx = 0
            sel = st.selectbox(f"{label} (or type custom)", options=all_options, index=idx, key=f"{widget_key}_sel")
            if sel == "(Custom)":
                new_val = st.text_input(f"{fname} (custom value)", value=current_val, key=f"{widget_key}_txt")
            else:
                new_val = sel
            return {**field, "value": new_val}
        else:
            new_val = st.text_input(label, value=current_val, key=widget_key)
            return {**field, "value": new_val}

    # ── PartnerDropDown (Directory Partner Selection) ────────────────────
    if ftype == "PartnerDropDown":
        partners = directory_list if directory_list is not None else _get_directory_partners()

        current_code = ""
        current_display = ""
        if isinstance(fval, dict):
            current_code = fval.get("code") or fval.get("id") or ""
            current_display = fval.get("value") or fval.get("name") or current_code
        elif isinstance(fval, str):
            current_code = fval
            for p in partners:
                if p["id"] == fval:
                    current_display = p["name"]
                    break
            if not current_display:
                current_display = fval

        if partners:
            partner_names = ["(None)"] + [p["name"] for p in partners]
            partner_ids = [""] + [p["id"] for p in partners]
            try:
                idx = partner_ids.index(current_code)
            except ValueError:
                idx = 0
            sel_idx = st.selectbox(
                label, options=range(len(partner_names)),
                format_func=lambda i: partner_names[i],
                index=idx, key=widget_key,
            )
            sel_id = partner_ids[sel_idx]
            sel_name = partner_names[sel_idx] if sel_idx > 0 else ""
            if sel_id:
                return {**field, "value": {"code": sel_id, "value": sel_name}}
            else:
                return {**field, "value": ""}
        else:
            new_val = st.text_input(label, value=current_display, key=widget_key, help="No directory records synced")
            return {**field, "value": new_val}

    # ── Users (User Selection) ────────────────────────────────────────────
    if ftype == "Users":
        users = users_list if users_list is not None else _get_user_options()

        current_id = ""
        if isinstance(fval, dict):
            current_id = fval.get("id") or fval.get("code") or ""
        elif isinstance(fval, str):
            current_id = fval

        if users:
            # Extract user name handling both formatted and raw DB formats
            user_labels = ["(None)"] + [f"{_extract_user_name(u)} ({u.get('email', '')})" for u in users]
            user_ids = [""] + [u["id"] for u in users]
            try:
                idx = user_ids.index(current_id)
            except ValueError:
                idx = 0
            sel_idx = st.selectbox(
                label, options=range(len(user_labels)),
                format_func=lambda i: user_labels[i],
                index=idx, key=widget_key,
            )
            sel_id = user_ids[sel_idx]
            sel_name = user_labels[sel_idx] if sel_idx > 0 else ""
            if sel_id:
                return {**field, "value": {"id": sel_id, "name": sel_name}}
            else:
                return {**field, "value": ""}
        else:
            display = _format_value_for_display(fval)
            new_val = st.text_input(label, value=display, key=widget_key, help="No users synced")
            return {**field, "value": new_val}

    # ── Date ──────────────────────────────────────────────────────────────
    if ftype == "Date":
        date_val = None
        if fval and isinstance(fval, str):
            try:
                date_val = datetime.fromisoformat(fval.replace("Z", "+00:00")).date()
            except (ValueError, AttributeError):
                pass
        new_date = st.date_input(label, value=date_val, key=widget_key)
        return {**field, "value": new_date.isoformat() if new_date else ""}

    # ── DateTime ──────────────────────────────────────────────────────────
    if ftype == "DateTime":
        dt_val = None
        if fval and isinstance(fval, str):
            try:
                dt_val = datetime.fromisoformat(fval.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass
        col1, col2 = st.columns(2)
        with col1:
            new_date = st.date_input(label, value=dt_val.date() if dt_val else None, key=f"{widget_key}_date")
        with col2:
            new_time = st.time_input("Time", value=dt_val.time() if dt_val else None, key=f"{widget_key}_time")
        if new_date and new_time:
            return {**field, "value": datetime.combine(new_date, new_time).isoformat()}
        return {**field, "value": ""}

    # ── Numeric types ─────────────────────────────────────────────────────
    if ftype in ("Number", "Decimal", "Percent", "Currency", "Weight", "Measure"):
        step = 1 if ftype == "Number" else 0.01
        try:
            numeric_val = float(fval) if fval else 0.0
        except (ValueError, TypeError):
            numeric_val = 0.0

        suffix = ""
        if ftype == "Percent":
            suffix = " (%)"
        elif ftype == "Currency":
            suffix = " ($)"
        elif ftype == "Weight":
            suffix = " (wt)"

        new_val = st.number_input(f"{label}{suffix}", value=numeric_val, step=float(step), key=widget_key)
        return {**field, "value": int(new_val) if ftype == "Number" else new_val}

    # ── CompositeControl (Structured Data) ────────────────────────────────
    if ftype == "CompositeControl":
        if isinstance(fval, list) and fval:
            st.caption(f"**{fname}**")
            composite_rows = []
            for i, item in enumerate(fval):
                if isinstance(item, dict):
                    col_a, col_b = st.columns(2)
                    code = col_a.text_input("Component", value=item.get("code", ""), key=f"{widget_key}_c_{i}")
                    value = col_b.text_input("Value", value=str(item.get("value", "")), key=f"{widget_key}_v_{i}")
                    composite_rows.append({"code": code, "value": value})
                else:
                    composite_rows.append(item)
            return {**field, "value": composite_rows}
        elif isinstance(fval, dict):
            st.caption(f"**{fname}**")
            new_json = st.text_area(label, value=json.dumps(fval, indent=2), key=widget_key, height=100)
            try:
                return {**field, "value": json.loads(new_json)}
            except json.JSONDecodeError:
                return {**field, "value": fval}
        else:
            new_val = st.text_input(label, value=str(fval) if fval else "", key=widget_key)
            return {**field, "value": new_val}

    # ── Fallback for unknown types ────────────────────────────────────────
    new_val = st.text_input(label, value=_format_value_for_display(fval), key=widget_key, help=f"Field type: {ftype}")
    return {**field, "value": new_val}


# ---------------------------------------------------------------------------
# Full form renderer (wraps render_field in a st.form)
# ---------------------------------------------------------------------------

def render_field_form(
    fields: list[dict[str, Any]],
    form_key: str,
    schema_dict: Optional[dict[str, dict[str, Any]]] = None,
    users: Optional[list[dict]] = None,
    directory: Optional[list[dict]] = None,
    show_submit: bool = True,
    submit_label: str = "Save",
    submit_type: str = "secondary",
) -> tuple[list[dict[str, Any]], bool]:
    """
    Render a complete field form with submit button.

    Args:
        fields: List of field dicts to render
        form_key: Unique key for the streamlit form
        schema_dict: Optional dict mapping field_id -> schema metadata
        users: Optional list of user dicts (auto-loaded from DB if None)
        directory: Optional list of directory dicts (auto-loaded from DB if None)
        show_submit: Whether to show submit button
        submit_label: Label for submit button
        submit_type: Streamlit button type

    Returns:
        Tuple of (updated_fields_list, submit_clicked: bool)
    """
    schema_dict = schema_dict or {}

    edited_fields: list[dict[str, Any]] = []
    submit_clicked = False

    with st.form(key=form_key):
        for field in fields:
            fid = field.get("id", "")
            schema = schema_dict.get(fid)

            updated_field = render_field(
                field,
                key_prefix=form_key,
                schema=schema,
                users_list=users,
                directory_list=directory,
            )
            edited_fields.append(updated_field)

        if show_submit:
            submit_clicked = st.form_submit_button(submit_label, use_container_width=True, type=submit_type)

    return edited_fields, submit_clicked
