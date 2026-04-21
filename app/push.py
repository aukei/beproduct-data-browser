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
