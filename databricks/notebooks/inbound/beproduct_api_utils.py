# Databricks notebook source
"""
BeProduct API Client & Data Normalization Utilities

This module provides:
1. OAuth token management with auto-refresh
2. Paginated API calls for all BeProduct endpoints
3. Data normalization functions (API response → flat Delta table rows)
4. Error handling and retry logic

Used by all inbound sync notebooks.
"""

import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Iterator, Optional, Dict, Any, List

import requests
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# OAUTH TOKEN MANAGEMENT
# ============================================================================

class TokenCache:
    """In-memory token cache with TTL (survives within a single job run)."""
    
    def __init__(self, ttl_seconds: int = 28800):  # 8 hours
        self.ttl_seconds = ttl_seconds
        self.token = None
        self.expires_at = None
    
    def is_valid(self) -> bool:
        """Check if cached token is still valid."""
        if self.token is None:
            return False
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) < self.expires_at
    
    def set(self, token: str, ttl_seconds: Optional[int] = None) -> None:
        """Set token with optional custom TTL."""
        self.token = token
        ttl = ttl_seconds or self.ttl_seconds
        self.expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl)
        logger.info(f"Token cached, expires at {self.expires_at.isoformat()}")
    
    def get(self) -> Optional[str]:
        """Get token if valid, else None."""
        if self.is_valid():
            return self.token
        return None


class BeProductOAuth:
    """Handle OAuth 2.0 token refresh using client credentials + refresh token."""
    
    OAUTH_URL = "https://id.winks.io/oauth/token"
    
    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.token_cache = TokenCache()
    
    def get_access_token(self, force_refresh: bool = False) -> str:
        """
        Get a valid access token, using cache if possible.
        
        Args:
            force_refresh: if True, bypass cache and fetch new token
        
        Returns:
            access_token string
        
        Raises:
            requests.RequestException if OAuth call fails
        """
        if not force_refresh:
            cached = self.token_cache.get()
            if cached:
                logger.debug("Using cached access token")
                return cached
        
        logger.info("Refreshing access token...")
        
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }
        
        try:
            response = requests.post(
                self.OAUTH_URL,
                json=payload,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            
            access_token = data.get("access_token")
            expires_in = data.get("expires_in", 28800)  # default 8h
            
            self.token_cache.set(access_token, expires_in)
            logger.info(f"✅ Access token refreshed (expires in {expires_in}s)")
            
            return access_token
        
        except requests.RequestException as e:
            logger.error(f"❌ OAuth token refresh failed: {e}")
            raise


# ============================================================================
# API CLIENT
# ============================================================================

class BeProductClient:
    """
    HTTP client for BeProduct API with pagination, error handling, and retry logic.
    
    All API methods yield records from paginated endpoints.
    """
    
    API_BASE = "https://developers.beproduct.com/api"
    
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        company_domain: str,
    ):
        self.oauth = BeProductOAuth(client_id, client_secret, refresh_token)
        self.company_domain = company_domain
        self.base_url = f"{self.API_BASE}/{company_domain}"
    
    def _get_headers(self) -> Dict[str, str]:
        """Get authorization headers with fresh access token."""
        token = self.oauth.get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
    
    def _paginate(
        self,
        method: str,
        endpoint: str,
        body: Optional[Dict] = None,
        page_size: int = 1000,
    ) -> Iterator[Dict]:
        """
        Paginate through an API endpoint.
        
        Yields individual records from paginated response.
        Handles retries and rate limiting with exponential backoff.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        page_number = 0
        total_yielded = 0
        max_retries = 3
        
        while True:
            for attempt in range(max_retries):
                try:
                    headers = self._get_headers()
                    
                    if method.upper() == "GET":
                        response = requests.get(
                            url,
                            headers=headers,
                            params={
                                "pageSize": page_size,
                                "pageNumber": page_number,
                            },
                            timeout=60,
                        )
                    else:  # POST
                        response = requests.post(
                            url,
                            headers=headers,
                            json=body or {},
                            params={
                                "pageSize": page_size,
                                "pageNumber": page_number,
                            },
                            timeout=60,
                        )
                    
                    if response.status_code == 429:
                        # Rate limited — exponential backoff
                        wait_time = 2 ** attempt
                        logger.warning(f"Rate limited. Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    # Extract result array and total count
                    # Note: BeProduct API uses "result" key, not "items"
                    records = data.get("result", [])
                    total = data.get("total", 0)
                    
                    # Yield each record
                    for record in records:
                        yield record
                        total_yielded += 1
                    
                    # Check if we've fetched all records
                    if len(records) < page_size or total_yielded >= total:
                        logger.info(f"✅ Paginated {total_yielded} total records from {endpoint}")
                        return
                    
                    page_number += 1
                    break  # Success, move to next page
                
                except requests.RequestException as e:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.warning(f"Request failed: {e}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"❌ Request failed after {max_retries} attempts: {e}")
                        raise
    
    # ========================================================================
    # STYLE ENDPOINTS
    # ========================================================================
    
    def fetch_styles(
        self,
        folder_id: Optional[str] = None,
        incremental_filter: Optional[str] = None,
    ) -> Iterator[Dict]:
        """Fetch all styles (optionally filtered by folder or modified_at)."""
        body = {
            "filters": []
        }
        
        if incremental_filter:
            body["filters"].append({
                "field": "FolderModifiedAt",
                "operator": "Gte",
                "value": incremental_filter,
            })
        
        if folder_id:
            body["filters"].append({
                "field": "FolderId",
                "operator": "Eq",
                "value": folder_id,
            })
        
        yield from self._paginate("POST", "Style/List", body=body)
    
    def fetch_style_by_id(self, style_id: str) -> Dict:
        """Fetch single style by ID."""
        response = requests.post(
            f"{self.base_url}/Style/Get",
            headers=self._get_headers(),
            json={"id": style_id},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    
    # ========================================================================
    # MATERIAL ENDPOINTS
    # ========================================================================
    
    def fetch_materials(
        self,
        folder_id: Optional[str] = None,
        incremental_filter: Optional[str] = None,
    ) -> Iterator[Dict]:
        """Fetch all materials."""
        body = {"filters": []}
        
        if incremental_filter:
            body["filters"].append({
                "field": "FolderModifiedAt",
                "operator": "Gte",
                "value": incremental_filter,
            })
        
        if folder_id:
            body["filters"].append({
                "field": "FolderId",
                "operator": "Eq",
                "value": folder_id,
            })
        
        yield from self._paginate("POST", "Material/List", body=body)
    
    def fetch_material_by_id(self, material_id: str) -> Dict:
        """Fetch single material by ID."""
        response = requests.post(
            f"{self.base_url}/Material/Get",
            headers=self._get_headers(),
            json={"id": material_id},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    
    # ========================================================================
    # COLOR ENDPOINTS
    # ========================================================================
    
    def fetch_colors(
        self,
        folder_id: Optional[str] = None,
        incremental_filter: Optional[str] = None,
    ) -> Iterator[Dict]:
        """Fetch all color palettes."""
        body = {"filters": []}
        
        if incremental_filter:
            body["filters"].append({
                "field": "FolderModifiedAt",
                "operator": "Gte",
                "value": incremental_filter,
            })
        
        if folder_id:
            body["filters"].append({
                "field": "FolderId",
                "operator": "Eq",
                "value": folder_id,
            })
        
        yield from self._paginate("POST", "Color/List", body=body)
    
    def fetch_color_by_id(self, color_id: str) -> Dict:
        """Fetch single color palette by ID."""
        response = requests.post(
            f"{self.base_url}/Color/Get",
            headers=self._get_headers(),
            json={"id": color_id},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    
    # ========================================================================
    # IMAGE ENDPOINTS
    # ========================================================================
    
    def fetch_images(
        self,
        folder_id: Optional[str] = None,
        incremental_filter: Optional[str] = None,
    ) -> Iterator[Dict]:
        """Fetch all images."""
        body = {"filters": []}
        
        if incremental_filter:
            body["filters"].append({
                "field": "FolderModifiedAt",
                "operator": "Gte",
                "value": incremental_filter,
            })
        
        if folder_id:
            body["filters"].append({
                "field": "FolderId",
                "operator": "Eq",
                "value": folder_id,
            })
        
        yield from self._paginate("POST", "Image/List", body=body)
    
    def fetch_image_by_id(self, image_id: str) -> Dict:
        """Fetch single image by ID."""
        response = requests.post(
            f"{self.base_url}/Image/Get",
            headers=self._get_headers(),
            json={"id": image_id},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    
    # ========================================================================
    # BLOCK ENDPOINTS
    # ========================================================================
    
    def fetch_blocks(
        self,
        folder_id: Optional[str] = None,
        incremental_filter: Optional[str] = None,
    ) -> Iterator[Dict]:
        """Fetch all blocks."""
        body = {"filters": []}
        
        if incremental_filter:
            body["filters"].append({
                "field": "FolderModifiedAt",
                "operator": "Gte",
                "value": incremental_filter,
            })
        
        if folder_id:
            body["filters"].append({
                "field": "FolderId",
                "operator": "Eq",
                "value": folder_id,
            })
        
        yield from self._paginate("POST", "Block/List", body=body)
    
    def fetch_block_by_id(self, block_id: str) -> Dict:
        """Fetch single block by ID."""
        response = requests.post(
            f"{self.base_url}/Block/Get",
            headers=self._get_headers(),
            json={"id": block_id},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    
    # ========================================================================
    # DIRECTORY ENDPOINTS
    # ========================================================================
    
    def fetch_directory(self) -> Iterator[Dict]:
        """Fetch all directory records (no pagination, typically <1000 records)."""
        yield from self._paginate("GET", "Directory/List")
    
    def fetch_directory_by_id(self, directory_id: str) -> Dict:
        """Fetch single directory record by ID."""
        response = requests.get(
            f"{self.base_url}/Directory/{directory_id}",
            headers=self._get_headers(),
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    
    # ========================================================================
    # USER ENDPOINTS
    # ========================================================================
    
    def fetch_users(self) -> Iterator[Dict]:
        """Fetch all users (no pagination, typically <1000 records)."""
        yield from self._paginate("GET", "User/List")
    
    # ========================================================================
    # DATA TABLE ENDPOINTS
    # ========================================================================
    
    def fetch_data_tables(self) -> Iterator[Dict]:
        """Fetch all data table definitions."""
        yield from self._paginate("POST", "DataTable/List", body={"filters": []})
    
    def fetch_data_table_rows(self, table_id: str) -> Iterator[Dict]:
        """Fetch all rows from a specific data table."""
        yield from self._paginate(
            "POST",
            f"DataTable/{table_id}/Data",
            body={"filters": []},
            page_size=5000,  # Use larger page size for data rows
        )


# ============================================================================
# DATA NORMALIZATION
# ============================================================================

def _extract_active_from_fields(record: Dict) -> int:
    """Extract 'active' field from headerData.fields[]."""
    for field in record.get("headerData", {}).get("fields", []):
        if field.get("id") == "active":
            value = str(field.get("value", "")).lower()
            return 1 if value in ("yes", "true", "1") else 0
    return 0


def _extract_header_data_fields(record: Dict) -> Dict[str, Any]:
    """Parse headerData.fields[] into a flat dict {field_id: value}."""
    fields_dict = {}
    for field in record.get("headerData", {}).get("fields", []):
        field_id = field.get("id")
        if field_id:
            fields_dict[field_id] = field.get("value")
    return fields_dict


def normalize_style_row(api_response: Dict, sync_time: str, batch_id: str) -> Dict:
    """Normalize Style API response to flat Delta row."""
    return {
        "id": api_response.get("id"),
        "folder_id": api_response.get("folder", {}).get("id"),
        "folder_name": api_response.get("folder", {}).get("name"),
        "header_number": api_response.get("headerNumber"),
        "header_name": api_response.get("headerName"),
        "active": _extract_active_from_fields(api_response),
        "created_at": api_response.get("createdAt"),
        "modified_at": api_response.get("modifiedAt"),
        "synced_at": sync_time,
        "last_beproduct_id": api_response.get("id"),
        "data_json": json.dumps(api_response),
        "_databricks_modified_at": sync_time,
        "_databricks_modified_by": "databricks_job",
        "_sync_batch_id": batch_id,
        "colorway_count": len(api_response.get("colorways", [])),
        "size_range_count": len(api_response.get("sizeRange", [])),
    }


def normalize_material_row(api_response: Dict, sync_time: str, batch_id: str) -> Dict:
    """Normalize Material API response to flat Delta row."""
    return {
        "id": api_response.get("id"),
        "folder_id": api_response.get("folder", {}).get("id"),
        "folder_name": api_response.get("folder", {}).get("name"),
        "header_number": api_response.get("headerNumber"),
        "header_name": api_response.get("headerName"),
        "active": _extract_active_from_fields(api_response),
        "created_at": api_response.get("createdAt"),
        "modified_at": api_response.get("modifiedAt"),
        "synced_at": sync_time,
        "last_beproduct_id": api_response.get("id"),
        "data_json": json.dumps(api_response),
        "_databricks_modified_at": sync_time,
        "_databricks_modified_by": "databricks_job",
        "_sync_batch_id": batch_id,
        "colorway_count": len(api_response.get("colorways", [])),
        "size_range_count": len(api_response.get("sizeRange", [])),
    }


def normalize_color_row(api_response: Dict, sync_time: str, batch_id: str) -> Dict:
    """Normalize Color Palette API response to flat Delta row. Handles colorPaletteNumber quirk."""
    # Color palettes use colorPaletteNumber/colorPaletteName instead of headerNumber/headerName
    header_number = api_response.get("colorPaletteNumber") or api_response.get("headerNumber")
    header_name = api_response.get("colorPaletteName") or api_response.get("headerName")
    
    color_chips = api_response.get("headerData", {}).get("colors", {}).get("colors", [])
    
    return {
        "id": api_response.get("id"),
        "folder_id": api_response.get("folder", {}).get("id"),
        "folder_name": api_response.get("folder", {}).get("name"),
        "header_number": header_number,
        "header_name": header_name,
        "active": _extract_active_from_fields(api_response),
        "created_at": api_response.get("createdAt"),
        "modified_at": api_response.get("modifiedAt"),
        "synced_at": sync_time,
        "last_beproduct_id": api_response.get("id"),
        "data_json": json.dumps(api_response),
        "_databricks_modified_at": sync_time,
        "_databricks_modified_by": "databricks_job",
        "_sync_batch_id": batch_id,
        "color_chip_count": len(color_chips),
    }


def normalize_image_row(api_response: Dict, sync_time: str, batch_id: str) -> Dict:
    """Normalize Image API response to flat Delta row."""
    return {
        "id": api_response.get("id"),
        "folder_id": api_response.get("folder", {}).get("id"),
        "folder_name": api_response.get("folder", {}).get("name"),
        "header_number": api_response.get("headerNumber"),
        "header_name": api_response.get("headerName"),
        "active": _extract_active_from_fields(api_response),
        "created_at": api_response.get("createdAt"),
        "modified_at": api_response.get("modifiedAt"),
        "synced_at": sync_time,
        "last_beproduct_id": api_response.get("id"),
        "data_json": json.dumps(api_response),
        "_databricks_modified_at": sync_time,
        "_databricks_modified_by": "databricks_job",
        "_sync_batch_id": batch_id,
    }


def normalize_block_row(api_response: Dict, sync_time: str, batch_id: str) -> Dict:
    """Normalize Block API response to flat Delta row."""
    size_classes = api_response.get("headerData", {}).get("sizeClasses", [])
    
    return {
        "id": api_response.get("id"),
        "folder_id": api_response.get("folder", {}).get("id"),
        "folder_name": api_response.get("folder", {}).get("name"),
        "header_number": api_response.get("headerNumber"),
        "header_name": api_response.get("headerName"),
        "active": _extract_active_from_fields(api_response),
        "created_at": api_response.get("createdAt"),
        "modified_at": api_response.get("modifiedAt"),
        "synced_at": sync_time,
        "last_beproduct_id": api_response.get("id"),
        "data_json": json.dumps(api_response),
        "_databricks_modified_at": sync_time,
        "_databricks_modified_by": "databricks_job",
        "_sync_batch_id": batch_id,
        "size_class_count": len(size_classes),
    }


def normalize_directory_row(api_response: Dict, sync_time: str, batch_id: str) -> Dict:
    """Normalize Directory API response to flat Delta row."""
    return {
        "id": api_response.get("id"),
        "directory_id": api_response.get("directoryId"),
        "name": api_response.get("name"),
        "partner_type": api_response.get("partnerType"),
        "country": api_response.get("country"),
        "active": 1 if api_response.get("active") else 0,
        "address": api_response.get("address"),
        "city": api_response.get("city"),
        "state": api_response.get("state"),
        "zip_code": api_response.get("zip"),
        "phone": api_response.get("phone"),
        "website": api_response.get("website"),
        "modified_at": api_response.get("modifiedAt"),
        "synced_at": sync_time,
        "last_beproduct_id": api_response.get("id"),
        "data_json": json.dumps(api_response),
        "_databricks_modified_at": sync_time,
        "_databricks_modified_by": "databricks_job",
        "_sync_batch_id": batch_id,
        "contact_count": len(api_response.get("contacts", [])),
    }


def normalize_user_row(api_response: Dict, sync_time: str, batch_id: str) -> Dict:
    """Normalize User API response to flat Delta row."""
    return {
        "id": api_response.get("id"),
        "email": api_response.get("email"),
        "username": api_response.get("username"),
        "first_name": api_response.get("firstName"),
        "last_name": api_response.get("lastName"),
        "title": api_response.get("title"),
        "account_type": api_response.get("accountType"),
        "role": api_response.get("role"),
        "registered_on": api_response.get("registerdOn"),  # NOTE: API typo "registerd"
        "active": 1 if api_response.get("active") else 0,
        "synced_at": sync_time,
        "last_beproduct_id": api_response.get("id"),
        "data_json": json.dumps(api_response),
        "_databricks_modified_at": sync_time,
        "_databricks_modified_by": "databricks_job",
        "_sync_batch_id": batch_id,
    }


def normalize_data_table_definition_row(api_response: Dict, sync_time: str, batch_id: str) -> Dict:
    """Normalize DataTable definition API response to flat Delta row."""
    return {
        "id": api_response.get("id"),
        "name": api_response.get("name"),
        "description": api_response.get("description"),
        "active": 1 if api_response.get("active") else 0,
        "created_at": api_response.get("createdAt"),
        "modified_at": api_response.get("modifiedAt"),
        "synced_at": sync_time,
        "last_beproduct_id": api_response.get("id"),
        "data_json": json.dumps(api_response),
        "_databricks_modified_at": sync_time,
        "_databricks_modified_by": "databricks_job",
        "_sync_batch_id": batch_id,
    }


def normalize_data_table_row(
    api_response: Dict,
    table_id: str,
    table_name: str,
    sync_time: str,
    batch_id: str,
) -> Dict:
    """Normalize DataTable row API response to flat Delta row."""
    # Flatten fields from the row
    fields_dict = {}
    for field in api_response.get("fields", []):
        field_id = field.get("id")
        if field_id:
            fields_dict[field_id] = field.get("value")
    
    # Add prefixed fields to output
    result = {
        "id": api_response.get("id"),
        "data_table_id": table_id,
        "data_table_name": table_name,
        "created_at": api_response.get("createdAt"),
        "modified_at": api_response.get("modifiedAt"),
        "synced_at": sync_time,
        "last_beproduct_id": api_response.get("id"),
        "data_json": json.dumps(api_response),
        "_databricks_modified_at": sync_time,
        "_databricks_modified_by": "databricks_job",
        "_sync_batch_id": batch_id,
        "field_count": len(api_response.get("fields", [])),
    }
    
    # Add individual field values with field_<id> prefix
    for field_id, value in fields_dict.items():
        safe_col = f"field_{field_id.lower().replace(' ', '_').replace('-', '_')}"
        result[safe_col] = value
    
    return result
