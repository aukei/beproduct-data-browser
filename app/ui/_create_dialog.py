"""
Shared "Create New Record" dialog component.

Fetches the folder schema after folder selection so all required fields are shown
(not just header_number / header_name). This is important because every folder can
have its own set of required fields configured by the BeProduct admin.

Confirmed required fields per entity (from live schema):
  Style:    header_number, header_name, year (DropDown), season (DropDown), team (DropDown)
  Material: header_number, header_name, material_type (DropDown), material_category (DropDown)
  Color:    header_number, header_name, year (DropDown), season (DropDown), palette_type (DropDown), team (DropDown)
  Image:    header_number, header_name  (only — confirmed from live schema)
  Block:    header_number, header_name  (only — confirmed from live schema)
  Directory: directoryId, name, partnerType  (no folder — upsert via directory_add)
  User:     email, username, firstName, lastName  (no folder)
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

import streamlit as st

from app.beproduct_client import get_client

logger = logging.getLogger(__name__)

# Master folder names as used by client.schema.get_folder_schema()
_ENTITY_MASTER_FOLDER = {
    "style":    "Style",
    "material": "Material",
    "color":    "Color",
    "image":    "Image",
    "block":    "Block",
}

# Partner type options for Directory creation
_PARTNER_TYPES = ["VENDOR", "FACTORY", "AGENT", "RETAILER", "SUPPLIER", "OTHER"]


def _get_folders(entity_type: str) -> list[dict]:
    """Fetch available folders for a given entity type."""
    client = get_client()
    if entity_type == "style":
        return client.style.folders()
    if entity_type == "material":
        return client.material.folders()
    if entity_type == "color":
        return client.color.folders()
    if entity_type == "image":
        return client.image.folders()
    if entity_type == "block":
        return client.block.folders()
    return []


def _get_schema_fields(entity_type: str, folder_id: str) -> list[dict]:
    """
    Fetch folder schema and return only field dicts for required fields.
    Returns list of field dicts compatible with render_field() signature.
    """
    master_folder = _ENTITY_MASTER_FOLDER.get(entity_type)
    if not master_folder:
        return []

    client = get_client()
    schema_list = client.schema.get_folder_schema(master_folder, folder_id)

    fields = []
    for s in schema_list:
        if not s.get("required"):
            continue

        ftype = s.get("field_type", "Text")
        # Skip system read-only types — they can't be set on create
        if ftype in ("UserLabel", "LabelText", "LabelMaterial", "LabelSize",
                     "LabelStyleGroup", "Label3dStyle", "Label3dMaterial",
                     "FormulaField", "Auto"):
            continue

        field: dict[str, Any] = {
            "id": s["field_id"],
            "name": s["field_name"],
            "type": ftype,
            "value": "",
            "required": True,
        }

        # Attach possible_values so DropDown/MultiSelect renders a selectbox
        if s.get("possible_values"):
            field["possible_values"] = s["possible_values"]

        fields.append(field)

    return fields


def show_create_entity_dialog(
    entity_type: str,
    on_create_callback: Callable,
    users_list: Optional[list[dict]] = None,
    directory_list: Optional[list[dict]] = None,
) -> None:
    """
    Render an inline create form for a given entity type.

    The folder schema is fetched from the API after the user selects a folder
    so that all required fields (including DropDowns like year/season/team) are shown.

    Args:
        entity_type: "Style", "Material", "Color", "Image", "Block", "Directory", "User"
        on_create_callback:
            - For entity types with folders: callable(folder_id: str, fields: dict) -> (bool, str, Optional[str])
            - For Directory/User: callable(fields: dict) -> (bool, str, Optional[str])
        users_list: Optional preloaded users for Users-type fields.
        directory_list: Optional preloaded directory records for PartnerDropDown fields.
    """
    from app.ui._field_editor import render_field

    entity_lower = entity_type.lower()

    with st.container(border=True):
        st.subheader(f"Create New {entity_type}")

        # ── Folder selection (not for Directory or User) ──────────────────────
        selected_folder_id: Optional[str] = None
        schema_fields: list[dict] = []

        if entity_lower not in ("directory", "user"):
            try:
                folders = _get_folders(entity_lower)
            except Exception as e:
                st.error(f"Failed to load folders: {e}")
                return

            if not folders:
                st.error(f"No folders found for {entity_type}.")
                return

            folder_options = [f.get("name", f.get("id", "")) for f in folders]
            chosen_name = st.selectbox(
                "Folder",
                options=folder_options,
                key=f"create_{entity_lower}_folder",
            )
            selected_folder = next(
                (f for f in folders if f.get("name") == chosen_name), folders[0]
            )
            selected_folder_id = selected_folder.get("id")

            # Fetch schema for the selected folder
            try:
                schema_fields = _get_schema_fields(entity_lower, selected_folder_id)
            except Exception as e:
                st.warning(f"Could not load folder schema: {e}. Showing minimum fields only.")
                schema_fields = [
                    {"id": "header_number", "name": "Header Number", "type": "Text", "value": "", "required": True},
                    {"id": "header_name",   "name": "Header Name",   "type": "Text", "value": "", "required": True},
                ]

        # ── Hardcoded fields for Directory and User ───────────────────────────
        elif entity_lower == "directory":
            schema_fields = [
                {"id": "directoryId", "name": "Directory ID",  "type": "Text",     "value": "",       "required": True},
                {"id": "name",        "name": "Name",           "type": "Text",     "value": "",       "required": True},
                {
                    "id": "partnerType", "name": "Partner Type", "type": "DropDown", "value": "VENDOR", "required": True,
                    "possible_values": [{"value": t} for t in _PARTNER_TYPES],
                },
            ]
        elif entity_lower == "user":
            schema_fields = [
                {"id": "email",     "name": "Email",      "type": "Text", "value": "", "required": True},
                {"id": "username",  "name": "Username",   "type": "Text", "value": "", "required": True},
                {"id": "firstName", "name": "First Name", "type": "Text", "value": "", "required": True},
                {"id": "lastName",  "name": "Last Name",  "type": "Text", "value": "", "required": True},
            ]

        if not schema_fields:
            st.info("No required fields found for this folder.")
            return

        # ── Render form ───────────────────────────────────────────────────────
        st.caption(f"* Required fields ({len(schema_fields)} total)")

        with st.form(key=f"create_{entity_lower}_form"):
            edited_fields: list[dict] = []
            for field in schema_fields:
                # Build schema dict for this field (for DropDown options)
                field_schema = None
                if field.get("possible_values"):
                    field_schema = {"possible_values": field["possible_values"]}

                updated = render_field(
                    field,
                    key_prefix=f"create_{entity_lower}",
                    schema=field_schema,
                    users_list=users_list,
                    directory_list=directory_list,
                )
                edited_fields.append(updated)

            submit = st.form_submit_button(
                f"Create {entity_type}",
                type="primary",
                use_container_width=True,
            )

        if st.button("Cancel", key=f"create_{entity_lower}_cancel"):
            st.rerun()

        if submit:
            # Validate
            fields_dict: dict[str, Any] = {}
            for field in edited_fields:
                fid = field.get("id", "")
                fval = field.get("value")
                if field.get("required") and (fval is None or fval == ""):
                    st.error(f"'{field.get('name')}' is required.")
                    return
                fields_dict[fid] = fval

            with st.spinner(f"Creating {entity_type}..."):
                try:
                    if entity_lower in ("directory", "user"):
                        success, message, new_id = on_create_callback(fields_dict)
                    else:
                        success, message, new_id = on_create_callback(selected_folder_id, fields_dict)
                except Exception as e:
                    st.error(f"Creation failed: {e}")
                    return

            if success:
                st.success(message)
                if new_id:
                    st.session_state[f"created_{entity_lower}_id"] = new_id
                st.rerun()
            else:
                st.error(message)
