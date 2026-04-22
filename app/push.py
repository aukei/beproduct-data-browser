"""
Push-back handler: takes locally-edited records (is_dirty=1) and pushes them
back to BeProduct SaaS via the Python SDK.

Each function returns (success: bool, message: str).
"""

from __future__ import annotations

import json
import logging
import traceback
from typing import Any, Optional

from app import db
from app.beproduct_client import get_client

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Style push-back
# ---------------------------------------------------------------------------

def push_style(record_id: str) -> tuple[bool, str]:
    """
    Push a locally-modified style to BeProduct.
    Extracts field values from the stored data_json and calls attributes_update().
    """
    row = db.get_style(record_id)
    if not row:
        return False, f"Style {record_id} not found in local DB"
    if not row.get("is_dirty"):
        return True, "No local changes to push"

    try:
        data = json.loads(row["data_json"])
        client = get_client()

        # Build fields dict from headerData.fields array
        fields = _extract_fields(data.get("headerData", {}).get("fields", []))

        # Build colorways update list
        colorways = _extract_colorways(data.get("colorways", []))

        # Build sizes list
        sizes = data.get("sizeRange") or []

        client.style.attributes_update(
            header_id=record_id,
            fields=fields if fields else None,
            colorways=colorways if colorways else None,
            sizes=sizes if sizes else None,
        )

        db.mark_style_clean(record_id)
        msg = f"Style {row.get('header_number', record_id)} pushed to BeProduct successfully"
        logger.info(msg)
        return True, msg

    except Exception as e:
        msg = f"Failed to push style {record_id}: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg


# ---------------------------------------------------------------------------
# Material push-back
# ---------------------------------------------------------------------------

def push_material(record_id: str) -> tuple[bool, str]:
    row = db.get_material(record_id)
    if not row:
        return False, f"Material {record_id} not found in local DB"
    if not row.get("is_dirty"):
        return True, "No local changes to push"

    try:
        data = json.loads(row["data_json"])
        client = get_client()

        fields = _extract_fields(data.get("headerData", {}).get("fields", []))
        colorways = _extract_colorways(data.get("colorways", []))
        sizes = data.get("sizeRange") or []
        suppliers = data.get("suppliers") or []

        client.material.attributes_update(
            header_id=record_id,
            fields=fields if fields else None,
            colorways=colorways if colorways else None,
            sizes=sizes if sizes else None,
            suppliers=suppliers if suppliers else None,
        )

        db.mark_material_clean(record_id)
        msg = f"Material {row.get('header_number', record_id)} pushed to BeProduct successfully"
        logger.info(msg)
        return True, msg

    except Exception as e:
        msg = f"Failed to push material {record_id}: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg


# ---------------------------------------------------------------------------
# Color palette push-back
# ---------------------------------------------------------------------------

def push_color(record_id: str) -> tuple[bool, str]:
    row = db.get_color(record_id)
    if not row:
        return False, f"Color palette {record_id} not found in local DB"
    if not row.get("is_dirty"):
        return True, "No local changes to push"

    try:
        data = json.loads(row["data_json"])
        client = get_client()

        fields = _extract_fields(data.get("headerData", {}).get("fields", []))
        # Colors are now nested at headerData.colors.colors (not top-level data.colors)
        colors_wrapper = data.get("headerData", {}).get("colors") or {}
        colors = colors_wrapper.get("colors") or []

        client.color.attributes_update(
            header_id=record_id,
            fields=fields if fields else None,
            colors=colors if colors else None,
        )

        db.mark_color_clean(record_id)
        msg = f"Color palette {row.get('header_number', record_id)} pushed to BeProduct successfully"
        logger.info(msg)
        return True, msg

    except Exception as e:
        msg = f"Failed to push color palette {record_id}: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg


# ---------------------------------------------------------------------------
# Image push-back
# ---------------------------------------------------------------------------

def push_image(record_id: str) -> tuple[bool, str]:
    """Push a locally-modified image to BeProduct."""
    row = db.get_image(record_id)
    if not row:
        return False, f"Image {record_id} not found in local DB"
    if not row.get("is_dirty"):
        return True, "No local changes to push"

    try:
        data = json.loads(row["data_json"])
        client = get_client()

        fields = _extract_fields(data.get("headerData", {}).get("fields", []))

        client.image.attributes_update(
            header_id=record_id,
            fields=fields if fields else None,
        )

        db.mark_image_clean(record_id)
        msg = f"Image {row.get('header_number', record_id)} pushed to BeProduct successfully"
        logger.info(msg)
        return True, msg

    except Exception as e:
        msg = f"Failed to push image {record_id}: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg


# ---------------------------------------------------------------------------
# Block push-back
# ---------------------------------------------------------------------------

def push_block(record_id: str) -> tuple[bool, str]:
    """Push a locally-modified block to BeProduct."""
    row = db.get_block(record_id)
    if not row:
        return False, f"Block {record_id} not found in local DB"
    if not row.get("is_dirty"):
        return True, "No local changes to push"

    try:
        data = json.loads(row["data_json"])
        client = get_client()

        fields = _extract_fields(data.get("headerData", {}).get("fields", []))

        client.block.attributes_update(
            header_id=record_id,
            fields=fields if fields else None,
        )

        db.mark_block_clean(record_id)
        msg = f"Block {row.get('header_number', record_id)} pushed to BeProduct successfully"
        logger.info(msg)
        return True, msg

    except Exception as e:
        msg = f"Failed to push block {record_id}: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg


# ---------------------------------------------------------------------------
# Directory push-back
# ---------------------------------------------------------------------------

def push_directory(record_id: str) -> tuple[bool, str]:
    """
    Directory records can be created/updated.
    Note: fax, active, and contacts are no longer part of the API schema and are omitted.
    """
    row = db.get_directory_record(record_id)
    if not row:
        return False, f"Directory record {record_id} not found in local DB"

    try:
        data = json.loads(row["data_json"])
        client = get_client()

        # Build the fields dict for directory_add (upsert-style)
        # Remove: fax, active, contacts (not supported by current API)
        fields = {
            "directoryId": data.get("directoryId", ""),
            "name": data.get("name", ""),
            "address": data.get("address", ""),
            "country": data.get("country", ""),
            "zip": data.get("zip", ""),
            "state": data.get("state", ""),
            "city": data.get("city", ""),
            "phone": data.get("phone", ""),
            "partnerType": data.get("partnerType", "VENDOR"),
            "website": data.get("website", ""),
        }

        client.directory.directory_add(fields=fields)
        msg = f"Directory record {row.get('name', record_id)} pushed to BeProduct successfully"
        logger.info(msg)
        return True, msg

    except Exception as e:
        msg = f"Failed to push directory record {record_id}: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg


# ---------------------------------------------------------------------------
# Push all dirty records
# ---------------------------------------------------------------------------

def push_all_dirty() -> dict[str, list[tuple[str, bool, str]]]:
    """
    Push every dirty record across all entities.
    Returns dict of {entity: [(record_id, success, message), ...]}.
    """
    results: dict[str, list[tuple[str, bool, str]]] = {
        "styles": [],
        "materials": [],
        "colors": [],
        "images": [],
        "blocks": [],
    }

    for row in db.get_styles(dirty_only=True):
        ok, msg = push_style(row["id"])
        results["styles"].append((row["id"], ok, msg))

    for row in db.get_materials(dirty_only=True):
        ok, msg = push_material(row["id"])
        results["materials"].append((row["id"], ok, msg))

    for row in db.get_colors(dirty_only=True):
        ok, msg = push_color(row["id"])
        results["colors"].append((row["id"], ok, msg))

    for row in db.get_images(dirty_only=True):
        ok, msg = push_image(row["id"])
        results["images"].append((row["id"], ok, msg))

    for row in db.get_blocks(dirty_only=True):
        ok, msg = push_block(row["id"])
        results["blocks"].append((row["id"], ok, msg))

    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_fields(fields_list: list[dict]) -> dict[str, Any]:
    """
    Convert the BeProduct headerData.fields array into the flat dict
    format expected by attributes_update().
    e.g. [{'id': 'header_name', 'value': 'My Style'}] → {'header_name': 'My Style'}
    Skips read-only system fields.
    """
    READONLY_FIELDS = {"created_by", "modified_by", "active", "version"}
    result = {}
    for f in fields_list:
        fid = f.get("id", "")
        if fid and fid not in READONLY_FIELDS:
            result[fid] = f.get("value")
    return result


def _extract_colorways(colorways_list: list[dict]) -> list[dict]:
    """
    Convert colorways from the full API response format to the SDK update format.
    """
    result = []
    for cw in colorways_list:
        entry: dict[str, Any] = {
            "id": cw.get("id"),
            "fields": {
                "color_number": cw.get("colorNumber", ""),
                "color_name": cw.get("colorName", ""),
                "primary": cw.get("primaryColor", ""),
                "secondary": cw.get("secondaryColor", ""),
                **cw.get("fields", {}),
            },
        }
        result.append(entry)
    return result


# ---------------------------------------------------------------------------
# Create operations (new records)
# ---------------------------------------------------------------------------

def create_style(
    folder_id: str,
    fields: dict[str, Any],
    colorways: Optional[list[dict]] = None,
    sizes: Optional[list[dict]] = None,
) -> tuple[bool, str, Optional[str]]:
    """
    Create a new style in BeProduct.
    
    Returns:
        Tuple of (success: bool, message: str, new_record_id: Optional[str])
    """
    try:
        client = get_client()
        result = client.style.attributes_create(
            folder_id=folder_id,
            fields=fields,
            colorways=colorways,
            sizes=sizes,
        )
        
        # result is the new style record
        new_id = result.get("id")
        if new_id:
            # Upsert into local DB
            db.upsert_style(result)
            msg = f"Style '{fields.get('header_number', 'New')}' created successfully"
            logger.info(msg)
            return True, msg, new_id
        else:
            return False, "Create succeeded but no ID returned", None
    
    except Exception as e:
        msg = f"Failed to create style: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg, None


def create_material(
    folder_id: str,
    fields: dict[str, Any],
    colorways: Optional[list[dict]] = None,
    sizes: Optional[list[dict]] = None,
    suppliers: Optional[list[dict]] = None,
) -> tuple[bool, str, Optional[str]]:
    """Create a new material in BeProduct."""
    try:
        client = get_client()
        result = client.material.attributes_create(
            folder_id=folder_id,
            fields=fields,
            colorways=colorways,
            sizes=sizes,
            suppliers=suppliers,
        )
        
        new_id = result.get("id")
        if new_id:
            db.upsert_material(result)
            msg = f"Material '{fields.get('header_number', 'New')}' created successfully"
            logger.info(msg)
            return True, msg, new_id
        else:
            return False, "Create succeeded but no ID returned", None
    
    except Exception as e:
        msg = f"Failed to create material: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg, None


def create_color(
    folder_id: str,
    fields: dict[str, Any],
    colors: Optional[list[dict]] = None,
) -> tuple[bool, str, Optional[str]]:
    """Create a new color palette in BeProduct."""
    try:
        client = get_client()
        result = client.color.attributes_create(
            folder_id=folder_id,
            fields=fields,
            colors=colors,
        )
        
        new_id = result.get("id")
        if new_id:
            db.upsert_color(result)
            msg = f"Color palette '{fields.get('header_number', 'New')}' created successfully"
            logger.info(msg)
            return True, msg, new_id
        else:
            return False, "Create succeeded but no ID returned", None
    
    except Exception as e:
        msg = f"Failed to create color palette: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg, None


def create_image(
    folder_id: str,
    fields: dict[str, Any],
) -> tuple[bool, str, Optional[str]]:
    """Create a new image in BeProduct."""
    try:
        client = get_client()
        result = client.image.attributes_create(
            folder_id=folder_id,
            fields=fields,
        )
        
        new_id = result.get("id")
        if new_id:
            db.upsert_image(result)
            msg = f"Image '{fields.get('header_number', 'New')}' created successfully"
            logger.info(msg)
            return True, msg, new_id
        else:
            return False, "Create succeeded but no ID returned", None
    
    except Exception as e:
        msg = f"Failed to create image: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg, None


def create_block(
    folder_id: str,
    fields: dict[str, Any],
    size_classes: Optional[list[dict]] = None,
) -> tuple[bool, str, Optional[str]]:
    """Create a new block in BeProduct."""
    try:
        client = get_client()
        result = client.block.attributes_create(
            folder_id=folder_id,
            fields=fields,
            size_classes=size_classes,
        )
        
        new_id = result.get("id")
        if new_id:
            db.upsert_block(result)
            msg = f"Block '{fields.get('header_number', 'New')}' created successfully"
            logger.info(msg)
            return True, msg, new_id
        else:
            return False, "Create succeeded but no ID returned", None
    
    except Exception as e:
        msg = f"Failed to create block: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg, None


def create_directory_entry(
    fields: dict[str, Any],
) -> tuple[bool, str, Optional[str]]:
    """Create a new directory entry in BeProduct."""
    try:
        client = get_client()
        result = client.directory.directory_add(fields=fields)
        
        new_id = result.get("id")
        if new_id:
            db.upsert_directory_record(result)
            msg = f"Directory entry '{fields.get('name', 'New')}' created successfully"
            logger.info(msg)
            return True, msg, new_id
        else:
            return False, "Create succeeded but no ID returned", None
    
    except Exception as e:
        msg = f"Failed to create directory entry: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg, None


def create_user(
    fields: dict[str, Any],
) -> tuple[bool, str, Optional[str]]:
    """Create a new user in BeProduct."""
    try:
        client = get_client()
        result = client.user.user_create(fields=fields)
        
        new_id = result.get("id")
        if new_id:
            db.upsert_user(result)
            msg = f"User '{fields.get('email', 'New')}' created successfully"
            logger.info(msg)
            return True, msg, new_id
        else:
            return False, "Create succeeded but no ID returned", None
    
    except Exception as e:
        msg = f"Failed to create user: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg, None


# ---------------------------------------------------------------------------
# Delete operations
# ---------------------------------------------------------------------------

def delete_style(record_id: str) -> tuple[bool, str]:
    """Delete a style from BeProduct and remove from local DB."""
    try:
        client = get_client()
        client.style.attributes_delete(header_id=record_id)
        db.delete_style(record_id)
        msg = f"Style {record_id} deleted successfully"
        logger.info(msg)
        return True, msg
    
    except Exception as e:
        msg = f"Failed to delete style {record_id}: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg


def delete_material(record_id: str) -> tuple[bool, str]:
    """Delete a material from BeProduct and remove from local DB."""
    try:
        client = get_client()
        client.material.attributes_delete(header_id=record_id)
        db.delete_material(record_id)
        msg = f"Material {record_id} deleted successfully"
        logger.info(msg)
        return True, msg
    
    except Exception as e:
        msg = f"Failed to delete material {record_id}: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg


def delete_color(record_id: str) -> tuple[bool, str]:
    """Delete a color palette from BeProduct and remove from local DB."""
    try:
        client = get_client()
        client.color.attributes_delete(header_id=record_id)
        db.delete_color(record_id)
        msg = f"Color palette {record_id} deleted successfully"
        logger.info(msg)
        return True, msg
    
    except Exception as e:
        msg = f"Failed to delete color palette {record_id}: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg


def delete_image(record_id: str) -> tuple[bool, str]:
    """Delete an image from BeProduct and remove from local DB."""
    try:
        client = get_client()
        client.image.attributes_delete(header_id=record_id)
        db.delete_image(record_id)
        msg = f"Image {record_id} deleted successfully"
        logger.info(msg)
        return True, msg
    
    except Exception as e:
        msg = f"Failed to delete image {record_id}: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg


def delete_block(record_id: str) -> tuple[bool, str]:
    """Delete a block from BeProduct and remove from local DB."""
    try:
        client = get_client()
        client.block.attributes_delete(header_id=record_id)
        db.delete_block(record_id)
        msg = f"Block {record_id} deleted successfully"
        logger.info(msg)
        return True, msg
    
    except Exception as e:
        msg = f"Failed to delete block {record_id}: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg


# ---------------------------------------------------------------------------
# Data Table row operations (uses raw_api — no SDK wrapper)
# ---------------------------------------------------------------------------

def push_data_table_row(
    table_id: str,
    row_id: str,
    row_fields: list[dict[str, Any]],
) -> tuple[bool, str]:
    """
    Push a data table row update to BeProduct.
    Uses raw_api since the SDK has no DataTable wrapper.
    """
    try:
        client = get_client()
        body = [
            {
                "rowId": row_id,
                "rowFields": row_fields,
                "deleteRow": False,
            }
        ]
        result = client.raw_api.post(f"DataTable/{table_id}/Update", body=body)
        db.mark_data_table_row_clean(row_id)
        msg = f"Data table row {row_id} pushed successfully"
        logger.info(msg)
        return True, msg

    except Exception as e:
        msg = f"Failed to push data table row {row_id}: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg


def add_data_table_row(
    table_id: str,
    row_fields: list[dict[str, Any]],
) -> tuple[bool, str, Optional[str]]:
    """
    Add a new row to a data table in BeProduct.
    Returns (success, message, new_row_id).
    """
    try:
        client = get_client()
        body = [
            {
                "rowId": None,  # null = insert new row
                "rowFields": row_fields,
                "deleteRow": False,
            }
        ]
        result = client.raw_api.post(f"DataTable/{table_id}/Update", body=body)

        added_ids = []
        if isinstance(result, dict):
            added_ids = result.get("added", [])
        
        new_id = added_ids[0] if added_ids else None
        msg = f"Data table row added successfully"
        logger.info(msg)
        return True, msg, new_id

    except Exception as e:
        msg = f"Failed to add data table row: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg, None


def delete_data_table_row(
    table_id: str,
    row_id: str,
) -> tuple[bool, str]:
    """Delete a row from a data table in BeProduct."""
    try:
        client = get_client()
        body = [
            {
                "rowId": row_id,
                "rowFields": [],
                "deleteRow": True,
            }
        ]
        client.raw_api.post(f"DataTable/{table_id}/Update", body=body)
        db.delete_data_table_row(row_id)
        msg = f"Data table row {row_id} deleted successfully"
        logger.info(msg)
        return True, msg

    except Exception as e:
        msg = f"Failed to delete data table row {row_id}: {e}"
        logger.error(f"{msg}\n{traceback.format_exc()}")
        return False, msg
