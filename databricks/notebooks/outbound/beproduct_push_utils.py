# Databricks notebook source
"""
BeProduct Push-Back Utilities

This module provides:
1. Merge logic for comparing Databricks local vs BeProduct remote state
2. Conflict detection based on modified_at timestamps
3. Push operations to BeProduct API with conflict resolution
4. Audit logging for compliance

Used by all outbound push notebooks.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Iterator, Optional, Dict, Any, Tuple, List

import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ============================================================================
# MERGE & CONFLICT DETECTION
# ============================================================================

class ConflictDetector:
    """Detect conflicts between local (Databricks) and remote (BeProduct) versions."""
    
    @staticmethod
    def detect_conflict(
        local_record: Dict,
        remote_record: Dict,
        strategy: str = "local_wins",
    ) -> Tuple[bool, str]:
        """
        Compare local vs remote and determine if conflict exists.
        
        Args:
            local_record: Databricks record (must have modified_at)
            remote_record: Latest from BeProduct API (must have modifiedAt)
            strategy: "local_wins", "remote_wins", or "manual_review"
        
        Returns:
            (is_conflict: bool, reason: str)
        """
        local_modified = local_record.get("_databricks_modified_at")
        remote_modified = remote_record.get("modifiedAt")
        
        if not local_modified or not remote_modified:
            return False, "Missing timestamp"
        
        # Parse timestamps
        try:
            local_dt = datetime.fromisoformat(str(local_modified).replace("Z", "+00:00"))
            remote_dt = datetime.fromisoformat(str(remote_modified).replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return False, "Invalid timestamp format"
        
        # Determine conflict
        if local_dt > remote_dt:
            # Local is newer
            return False, f"Local is newer ({local_dt} > {remote_dt}), safe to push"
        elif remote_dt > local_dt:
            # Remote is newer
            if strategy == "remote_wins":
                return True, f"Remote is newer ({remote_dt} > {local_dt}), skipping per remote_wins strategy"
            elif strategy == "manual_review":
                return True, f"Remote is newer ({remote_dt} > {local_dt}), requires manual review"
            else:  # local_wins
                return False, f"Remote is newer but local_wins strategy applied, will overwrite"
        else:
            # Same timestamp — check content
            local_json = local_record.get("data_json", "{}")
            remote_json = json.dumps(remote_record)
            
            if local_json == remote_json:
                return False, "Content identical, no conflict"
            else:
                if strategy == "manual_review":
                    return True, "Content differs but timestamps match, requires manual review"
                else:
                    return False, f"Content differs, using {strategy} strategy"


class RecordMerger:
    """Merge local changes with remote state."""
    
    @staticmethod
    def merge(
        local_record: Dict,
        remote_record: Dict,
        strategy: str = "local_wins",
    ) -> Dict:
        """
        Merge local and remote records based on strategy.
        
        Args:
            local_record: Databricks record with edits
            remote_record: Latest from BeProduct API
            strategy: "local_wins" or "remote_wins"
        
        Returns:
            Merged record ready for push
        """
        if strategy == "local_wins":
            # Use local version as source of truth
            merged = dict(local_record)
            # Keep remote's unedited metadata
            merged["id"] = remote_record.get("id", local_record.get("id"))
            merged["createdAt"] = remote_record.get("createdAt")
            merged["createdBy"] = remote_record.get("createdBy")
            return merged
        
        else:  # remote_wins (should not reach here if conflict detected properly)
            # Use remote as base, don't apply local changes
            return remote_record


# ============================================================================
# PUSH OPERATIONS
# ============================================================================

class BeProductPusher:
    """Push modified records back to BeProduct API."""
    
    API_BASE = "https://developers.beproduct.com/api"
    
    def __init__(self, oauth_handler):
        """
        Args:
            oauth_handler: BeProductOAuth instance with get_access_token() method
        """
        self.oauth = oauth_handler
        self.company_domain = oauth_handler.company_domain if hasattr(oauth_handler, 'company_domain') else None
        self.base_url = f"{self.API_BASE}/{self.company_domain}" if self.company_domain else None
    
    def _get_headers(self) -> Dict[str, str]:
        """Get authorization headers."""
        token = self.oauth.get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
    
    # ========================================================================
    # PUSH: STYLES
    # ========================================================================
    
    def push_style(
        self,
        record_dict: Dict,
        overwrite_conflicts: bool = False,
    ) -> Tuple[bool, str]:
        """
        Push style update to BeProduct.
        
        Args:
            record_dict: Style record with fields to update
            overwrite_conflicts: if True, push even if conflict detected
        
        Returns:
            (success: bool, message: str)
        """
        try:
            style_id = record_dict.get("id")
            if not style_id:
                return False, "No style ID provided"
            
            # Extract headerData fields from data_json
            data_json_str = record_dict.get("data_json")
            if data_json_str:
                data = json.loads(data_json_str)
                fields = {}
                for field in data.get("headerData", {}).get("fields", []):
                    field_id = field.get("id")
                    if field_id:
                        fields[field_id] = field.get("value")
            else:
                fields = {}
            
            # Push to API
            response = requests.post(
                f"{self.base_url}/Style/Update",
                headers=self._get_headers(),
                json={
                    "id": style_id,
                    "fields": fields,
                },
                timeout=30,
            )
            
            if response.status_code == 200:
                return True, f"Style {style_id} pushed successfully"
            else:
                return False, f"API returned {response.status_code}: {response.text[:200]}"
        
        except Exception as e:
            logger.error(f"Error pushing style: {e}")
            return False, str(e)
    
    # ========================================================================
    # PUSH: MATERIALS
    # ========================================================================
    
    def push_material(
        self,
        record_dict: Dict,
        overwrite_conflicts: bool = False,
    ) -> Tuple[bool, str]:
        """Push material update to BeProduct."""
        try:
            material_id = record_dict.get("id")
            if not material_id:
                return False, "No material ID provided"
            
            data_json_str = record_dict.get("data_json")
            if data_json_str:
                data = json.loads(data_json_str)
                fields = {}
                for field in data.get("headerData", {}).get("fields", []):
                    field_id = field.get("id")
                    if field_id:
                        fields[field_id] = field.get("value")
            else:
                fields = {}
            
            response = requests.post(
                f"{self.base_url}/Material/Update",
                headers=self._get_headers(),
                json={
                    "id": material_id,
                    "fields": fields,
                },
                timeout=30,
            )
            
            if response.status_code == 200:
                return True, f"Material {material_id} pushed successfully"
            else:
                return False, f"API returned {response.status_code}: {response.text[:200]}"
        
        except Exception as e:
            logger.error(f"Error pushing material: {e}")
            return False, str(e)
    
    # ========================================================================
    # PUSH: COLORS
    # ========================================================================
    
    def push_color(
        self,
        record_dict: Dict,
        overwrite_conflicts: bool = False,
    ) -> Tuple[bool, str]:
        """Push color palette update to BeProduct."""
        try:
            color_id = record_dict.get("id")
            if not color_id:
                return False, "No color ID provided"
            
            data_json_str = record_dict.get("data_json")
            if data_json_str:
                data = json.loads(data_json_str)
                fields = {}
                for field in data.get("headerData", {}).get("fields", []):
                    field_id = field.get("id")
                    if field_id:
                        fields[field_id] = field.get("value")
            else:
                fields = {}
            
            response = requests.post(
                f"{self.base_url}/Color/Update",
                headers=self._get_headers(),
                json={
                    "id": color_id,
                    "fields": fields,
                },
                timeout=30,
            )
            
            if response.status_code == 200:
                return True, f"Color {color_id} pushed successfully"
            else:
                return False, f"API returned {response.status_code}: {response.text[:200]}"
        
        except Exception as e:
            logger.error(f"Error pushing color: {e}")
            return False, str(e)
    
    # ========================================================================
    # PUSH: IMAGES
    # ========================================================================
    
    def push_image(
        self,
        record_dict: Dict,
        overwrite_conflicts: bool = False,
    ) -> Tuple[bool, str]:
        """Push image update to BeProduct."""
        try:
            image_id = record_dict.get("id")
            if not image_id:
                return False, "No image ID provided"
            
            data_json_str = record_dict.get("data_json")
            if data_json_str:
                data = json.loads(data_json_str)
                fields = {}
                for field in data.get("headerData", {}).get("fields", []):
                    field_id = field.get("id")
                    if field_id:
                        fields[field_id] = field.get("value")
            else:
                fields = {}
            
            response = requests.post(
                f"{self.base_url}/Image/Update",
                headers=self._get_headers(),
                json={
                    "id": image_id,
                    "fields": fields,
                },
                timeout=30,
            )
            
            if response.status_code == 200:
                return True, f"Image {image_id} pushed successfully"
            else:
                return False, f"API returned {response.status_code}: {response.text[:200]}"
        
        except Exception as e:
            logger.error(f"Error pushing image: {e}")
            return False, str(e)
    
    # ========================================================================
    # PUSH: BLOCKS
    # ========================================================================
    
    def push_block(
        self,
        record_dict: Dict,
        overwrite_conflicts: bool = False,
    ) -> Tuple[bool, str]:
        """Push block update to BeProduct."""
        try:
            block_id = record_dict.get("id")
            if not block_id:
                return False, "No block ID provided"
            
            data_json_str = record_dict.get("data_json")
            if data_json_str:
                data = json.loads(data_json_str)
                fields = {}
                for field in data.get("headerData", {}).get("fields", []):
                    field_id = field.get("id")
                    if field_id:
                        fields[field_id] = field.get("value")
            else:
                fields = {}
            
            response = requests.post(
                f"{self.base_url}/Block/Update",
                headers=self._get_headers(),
                json={
                    "id": block_id,
                    "fields": fields,
                },
                timeout=30,
            )
            
            if response.status_code == 200:
                return True, f"Block {block_id} pushed successfully"
            else:
                return False, f"API returned {response.status_code}: {response.text[:200]}"
        
        except Exception as e:
            logger.error(f"Error pushing block: {e}")
            return False, str(e)
    
    # ========================================================================
    # PUSH: DIRECTORY (upsert only)
    # ========================================================================
    
    def push_directory(
        self,
        record_dict: Dict,
        overwrite_conflicts: bool = False,
    ) -> Tuple[bool, str]:
        """Push directory update to BeProduct (uses directory_add which is upsert)."""
        try:
            directory_id = record_dict.get("directory_id")
            if not directory_id:
                return False, "No directory_id provided"
            
            # Extract fields for directory_add
            push_body = {
                "directoryId": directory_id,
                "name": record_dict.get("name"),
                "partnerType": record_dict.get("partner_type"),
                "country": record_dict.get("country"),
                "address": record_dict.get("address"),
                "city": record_dict.get("city"),
                "state": record_dict.get("state"),
                "zip": record_dict.get("zip_code"),
                "phone": record_dict.get("phone"),
                "website": record_dict.get("website"),
                "active": bool(record_dict.get("active")),
            }
            
            response = requests.post(
                f"{self.base_url}/Directory/Add",
                headers=self._get_headers(),
                json=push_body,
                timeout=30,
            )
            
            if response.status_code == 200:
                return True, f"Directory {directory_id} pushed successfully"
            else:
                return False, f"API returned {response.status_code}: {response.text[:200]}"
        
        except Exception as e:
            logger.error(f"Error pushing directory: {e}")
            return False, str(e)
    
    # ========================================================================
    # PUSH: DATA TABLE ROWS
    # ========================================================================
    
    def push_data_table_row(
        self,
        table_id: str,
        row_id: str,
        row_fields: Dict[str, Any],
    ) -> Tuple[bool, str]:
        """
        Push data table row update to BeProduct.
        
        Args:
            table_id: DataTable ID
            row_id: Row ID (for update) or None (for insert)
            row_fields: Dict of {field_id: value}
        
        Returns:
            (success: bool, message: str)
        """
        try:
            # Build update request
            update_request = [
                {
                    "rowId": row_id,
                    "rowFields": [
                        {"id": field_id, "value": value}
                        for field_id, value in row_fields.items()
                    ],
                    "deleteRow": False,
                }
            ]
            
            response = requests.post(
                f"{self.base_url}/DataTable/{table_id}/Update",
                headers=self._get_headers(),
                json=update_request,
                timeout=30,
            )
            
            if response.status_code == 200:
                return True, f"Data table row {'inserted' if not row_id else 'updated'} in {table_id}"
            else:
                return False, f"API returned {response.status_code}: {response.text[:200]}"
        
        except Exception as e:
            logger.error(f"Error pushing data table row: {e}")
            return False, str(e)


# ============================================================================
# AUDIT LOGGING
# ============================================================================

def create_audit_log_entry(
    record_id: str,
    master_type: str,
    action: str,
    databricks_modified_at: Optional[str],
    beproduct_modified_at: Optional[str],
    error_message: Optional[str] = None,
    job_id: Optional[str] = None,
    run_id: Optional[str] = None,
    user: str = "databricks_job",
) -> Dict[str, Any]:
    """
    Create an audit log entry for push operations.
    
    Args:
        record_id: BeProduct record ID
        master_type: Entity type (styles, materials, colors, etc.)
        action: INSERT, UPDATE, CONFLICT_SKIPPED, ERROR
        databricks_modified_at: Local version timestamp
        beproduct_modified_at: Remote version timestamp
        error_message: Error details if action=ERROR
        job_id: Databricks job ID
        run_id: Databricks job run ID
        user: User who triggered the job
    
    Returns:
        Dict suitable for inserting into audit table
    """
    return {
        "audit_id": f"{record_id}_{int(datetime.now(timezone.utc).timestamp() * 1000)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "job_id": job_id,
        "run_id": run_id,
        "master_type": master_type,
        "record_id": record_id,
        "action": action,
        "databricks_modified_at": databricks_modified_at,
        "beproduct_modified_at": beproduct_modified_at,
        "error_message": error_message,
        "databricks_user": user,
        "_databricks_modified_at": datetime.now(timezone.utc).isoformat(),
        "_databricks_modified_by": "databricks_job",
    }
