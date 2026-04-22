"""
Shared "Create New Record" dialog component.

Handles folder selection, required fields form, and creation submission.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import streamlit as st

from app.beproduct_client import get_client
from app.ui._field_editor import render_field_form

logger = logging.getLogger(__name__)


def show_create_entity_dialog(
    entity_type: str,  # "Style", "Material", "Color", "Image", "Block", "Directory", "User"
    on_create_callback,  # Function(folder_id, fields, colorways, sizes) -> tuple[bool, str, Optional[str]]
    optional_fields: Optional[list[dict]] = None,  # Additional editable fields beyond header_number/header_name
    users_list: Optional[list[dict]] = None,
    directory_list: Optional[list[dict]] = None,
) -> None:
    """
    Show a create dialog for a given entity type.
    
    Args:
        entity_type: One of "Style", "Material", "Color", "Image", "Block", "Directory", "User"
        on_create_callback: Async function called with (folder_id, fields) or just (fields) for Directory/User
        optional_fields: Additional fields to include in the form
        users_list: List of user dicts for Users field types
        directory_list: List of directory dicts for PartnerDropDown field types
    """
    entity_type = entity_type.lower()
    
    with st.dialog(f"➕ Create New {entity_type.title()}"):
        st.write(f"**Create a new {entity_type}**")
        
        # Folder selection (not for Directory or User)
        selected_folder_id: Optional[str] = None
        if entity_type not in ("directory", "user"):
            st.subheader("📁 Folder")
            
            try:
                client = get_client()
                # Get available folders based on entity type
                if entity_type == "style":
                    folders = client.style.folders()
                elif entity_type == "material":
                    folders = client.material.folders()
                elif entity_type == "color":
                    folders = client.color.folders()
                elif entity_type == "image":
                    folders = client.image.folders()
                elif entity_type == "block":
                    folders = client.block.folders()
                else:
                    folders = []
                
                if not folders:
                    st.error(f"No folders available for {entity_type}s")
                    return
                
                folder_names = [f.get("name", f.get("id", "")) for f in folders]
                selected_folder_name = st.selectbox(
                    f"Select a folder for the new {entity_type}",
                    options=folder_names,
                    key="create_folder_sel",
                )
                
                selected_folder_id = next(
                    (f.get("id") for f in folders if f.get("name") == selected_folder_name),
                    None,
                )
                
            except Exception as e:
                st.error(f"Failed to load folders: {e}")
                return
        
        # Required fields form
        st.subheader("📝 Required Fields")
        
        required_fields = []
        
        # Build required fields based on entity type
        if entity_type in ("style", "material", "color", "image", "block"):
            required_fields = [
                {
                    "id": "header_number",
                    "name": "Header Number",
                    "type": "Text",
                    "value": "",
                    "required": True,
                },
                {
                    "id": "header_name",
                    "name": "Header Name",
                    "type": "Text",
                    "value": "",
                    "required": True,
                },
            ]
        
        elif entity_type == "directory":
            required_fields = [
                {
                    "id": "directoryId",
                    "name": "Directory ID",
                    "type": "Text",
                    "value": "",
                    "required": True,
                },
                {
                    "id": "name",
                    "name": "Name",
                    "type": "Text",
                    "value": "",
                    "required": True,
                },
                {
                    "id": "partnerType",
                    "name": "Partner Type",
                    "type": "DropDown",
                    "value": "VENDOR",
                    "required": True,
                },
            ]
        
        elif entity_type == "user":
            required_fields = [
                {
                    "id": "email",
                    "name": "Email",
                    "type": "Text",
                    "value": "",
                    "required": True,
                },
                {
                    "id": "username",
                    "name": "Username",
                    "type": "Text",
                    "value": "",
                    "required": True,
                },
                {
                    "id": "firstName",
                    "name": "First Name",
                    "type": "Text",
                    "value": "",
                    "required": True,
                },
                {
                    "id": "lastName",
                    "name": "Last Name",
                    "type": "Text",
                    "value": "",
                    "required": True,
                },
            ]
        
        # Add optional fields if provided
        if optional_fields:
            required_fields.extend(optional_fields)
        
        # Render form
        edited_fields, submit_clicked = render_field_form(
            required_fields,
            form_key=f"create_{entity_type}_form",
            users=users_list,
            directory=directory_list,
            show_submit=True,
            submit_label=f"➕ Create {entity_type.title()}",
            submit_type="primary",
        )
        
        if submit_clicked:
            # Validate required fields
            fields_dict = {}
            for field in edited_fields:
                fid = field.get("id")
                fval = field.get("value")
                
                if field.get("required") and not fval:
                    st.error(f"Required field '{field.get('name')}' is empty")
                    return
                
                fields_dict[fid] = fval
            
            # Call creation callback
            with st.spinner(f"Creating {entity_type}…"):
                if entity_type in ("directory", "user"):
                    success, message, new_id = on_create_callback(fields_dict)
                else:
                    success, message, new_id = on_create_callback(selected_folder_id, fields_dict)
                
                if success:
                    st.success(message)
                    st.session_state[f"created_{entity_type}_id"] = new_id
                    st.rerun()
                else:
                    st.error(message)
