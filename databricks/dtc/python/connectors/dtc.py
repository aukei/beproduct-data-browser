"""
DTC API Connector for pulling requests and sheet data.

Provides methods to:
- Fetch a specific request by ID
- Get available views for a request
- Fetch sheet data from a specific view
- Convert to Pandas DataFrame for Databricks
"""

import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import pandas as pd

from client.rest_client import RestClient

logger = logging.getLogger(__name__)


class DTCConnector:
    """Connector for DTC (Data Collaboration Application) API."""

    def __init__(
        self,
        api_key: str,
        environment: str = "uat",
        workspace_name: str = "Kontoor",
    ):
        """
        Initialize DTC connector.

        Args:
            api_key: DTC API key
            environment: "uat" or "prod"
            workspace_name: Default workspace name
        """
        env_map = {
            "uat": "https://dtc-api.lfuat.net",
            "prod": "https://dtc-api.lfapps.net",
        }
        base_url = env_map.get(environment.lower(), "https://dtc-api.lfuat.net")

        self.client = RestClient(
            base_url=f"{base_url}/api",
            api_key=api_key,
            timeout=30,
        )
        self.workspace_name = workspace_name
        logger.info(
            f"DTCConnector initialized: workspace={workspace_name}, env={environment}"
        )

    def get_request(self, request_id: str) -> Dict[str, Any]:
        """
        Get a single request by ID.

        Args:
            request_id: DTC request ID

        Returns:
            Request details dict
        """
        logger.info(f"Fetching request: {request_id}")
        return self.client.get(f"/v1/requests/{request_id}")

    def get_views(self, request_id: str) -> List[Dict[str, str]]:
        """
        Get all available views for a request.

        Args:
            request_id: DTC request ID

        Returns:
            List of views with viewId and viewName
        """
        logger.info(f"Fetching views for request: {request_id}")
        response = self.client.get(f"/v1/requests/{request_id}/views")
        return response.get("data", [])

    def get_sheet(
        self, sheet_id: str, view_id: str, filters: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Get sheet data for a specific view.

        Args:
            sheet_id: DTC sheet ID
            view_id: DTC view ID
            filters: Optional filters dict

        Returns:
            Sheet data dict with sheetData array
        """
        logger.info(f"Fetching sheet: {sheet_id}, view: {view_id}")
        return self.client.get(f"/v1/sheets/{sheet_id}/views/{view_id}")

    def get_document_metadata(self, request_id: str) -> Dict[str, Any]:
        """
        Get Document metadata for a request.
        
        Document is the schema definition. A Request is an instance of a Document.
        Views are column projections defined on a Document and auto-apply to all Requests.
        
        Args:
            request_id: DTC request ID
            
        Returns:
            Document metadata dict with schema info
        """
        req = self.get_request(request_id)
        
        return {
            "document_name": req.get("documentName"),
            "request_id": req.get("requestId"),
            "request_reference": req.get("requestReference"),
            "request_description": req.get("requestDescription"),
            "workspace_name": req.get("workspaceName"),
            "sheet_id": req.get("sheetId"),
            "request_status": req.get("requestStatusName"),
            "request_is_active": req.get("requestIsActive"),
            "owner_name": req.get("ownerName"),
            "owner_email": req.get("ownerUserEmail", req.get("ownerEmail")),
            "created_at": req.get("createdDat"),
            "updated_at": req.get("updatedDat"),
        }

    def pull_request_to_dataframe(
        self, request_id: str, view_id: str
    ) -> tuple:
        """
        Pull a specific request's sheet data and convert to DataFrame.
        
        Returns both the data and document metadata for Delta table properties.

        Args:
            request_id: DTC request ID
            view_id: DTC view ID

        Returns:
            Tuple of (DataFrame, document_metadata_dict)
            - DataFrame: Row data with metadata columns
            - dict: Document metadata for Delta table properties
        """
        # Get request metadata
        req = self.get_request(request_id)
        sheet_id = req.get("sheetId")

        if not sheet_id:
            raise ValueError(f"Request {request_id} has no sheetId")

        # Get sheet data
        sheet = self.get_sheet(sheet_id, view_id)

        # Extract metadata
        metadata = {
            "request_id": req.get("requestId"),
            "request_reference": req.get("requestReference"),
            "request_description": req.get("requestDescription"),
            "document_name": req.get("documentName"),
            "workspace_name": req.get("workspaceName"),
            "request_status": req.get("requestStatusName"),
            "request_is_active": req.get("requestIsActive"),
            "owner_name": req.get("ownerName"),
            "owner_email": req.get("ownerUserEmail", req.get("ownerEmail")),
            "updated_at": req.get("updatedDat"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        # Convert sheet data to DataFrame
        rows = sheet.get("sheetData", [])
        logger.info(f"Converting {len(rows)} rows to DataFrame")

        # Flatten: each row becomes one DataFrame row with metadata
        data = []
        for row in rows:
            flat_row = {**metadata, **row}
            # Ensure rowIndex and rowId are captured
            flat_row["row_index"] = row.get("rowIndex")
            flat_row["row_id"] = row.get("rowId")
            data.append(flat_row)

        df = pd.DataFrame(data)

        # Normalize column names (remove HTML tags, clean up)
        df.columns = [self._normalize_column_name(col) for col in df.columns]

        logger.info(f"Created DataFrame with {len(df)} rows, {len(df.columns)} columns")
        
        # Get document metadata separately for table properties
        doc_metadata = self.get_document_metadata(request_id)
        
        return df, doc_metadata

    @staticmethod
    def _normalize_column_name(name: str) -> str:
        """
        Normalize column names: remove HTML tags, spaces.

        Args:
            name: Original column name

        Returns:
            Normalized column name
        """
        # Replace HTML <BR/> tags with underscore
        normalized = name.replace("<BR/>", "_").replace("<br/>", "_")
        # Replace multiple spaces with single space
        normalized = " ".join(normalized.split())
        return normalized

    def close(self):
        """Close the connector."""
        self.client.close()
        logger.info("DTCConnector closed")
