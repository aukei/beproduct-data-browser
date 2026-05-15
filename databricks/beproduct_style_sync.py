# Databricks notebook source
"""
BeProduct STYLE Master Data Sync Job
=====================================

Retrieves STYLE master data from BeProduct for folder 'KTB' and stores in Delta Lake.
Supports both FULL and INCREMENTAL refresh modes.

Schedule: Daily at 7pm HKT (11am UTC)

Parameters:
  - refresh_mode: "FULL" (default) or "INCREMENTAL"
  - catalog: Target Databricks catalog (default: "main")
  - schema: Target Databricks schema (default: "beproduct")
  - table_name: Table name (default: "ktb_styles")

Field Mapping:
  Compulsory fields (extracted as columns):
    - LF Sytle Number → lf_style_number
    - Description → description
    - Team → team
    - Season → season
    - Year → year

  Interested fields (extracted as columns):
    - Product Status → product_status
    - Customer Style Number → customer_style_number
    - Product Category → product_category
    - Product Sub Category → product_sub_category
    - Divison → division
    - Brands → brands
    - Garment Finish → garment_finish
    - Techpack Stage → techpack_stage
    - Lot code → lot_code
    - Parent Vendor → parent_vendor
    - Factory → factory

  All other fields: stored in data_json column as JSON string
"""

# COMMAND ----------

# Widgets for job parameters
dbutils.widgets.text(
    "refresh_mode",
    "INCREMENTAL",
    "Refresh Mode (FULL or INCREMENTAL)"
)
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "beproduct", "Schema Name")
dbutils.widgets.text("table_name", "ktb_styles", "Table Name")

refresh_mode = dbutils.widgets.get("refresh_mode").upper()
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")

print(f"🚀 Starting BeProduct STYLE sync job")
print(f"   Mode: {refresh_mode} | Catalog: {catalog}.{schema}.{table_name}")

# COMMAND ----------

import json
import logging
import requests
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# ============================================================================
# BeProduct OAuth & API Client
# ============================================================================

class BeProductClient:
    """
    Standalone BeProduct API client.
    Handles OAuth token refresh and API requests.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        company_domain: str,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.company_domain = company_domain

        self.auth_url = "https://id.winks.io/connect/token"
        self.api_base = f"https://us.beproduct.com/api/{company_domain}"

        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0

        # Refresh token immediately to start fresh
        self._refresh_access_token()

    def _refresh_access_token(self) -> None:
        """Obtain a new access token using the refresh token."""
        logger.info("Refreshing BeProduct access token...")

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        try:
            response = requests.post(self.auth_url, data=payload, timeout=10)
            response.raise_for_status()

            data = response.json()
            self.access_token = data["access_token"]
            expires_in = data.get("expires_in", 28800)  # 8 hours default
            self.token_expires_at = time.time() + expires_in

            logger.info(
                f"✅ Access token obtained (expires in {expires_in}s)"
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to refresh BeProduct token: {str(e)}"
            ) from e

    def _ensure_token_valid(self) -> None:
        """Refresh token if it's expired or about to expire (within 60 seconds)."""
        if time.time() >= self.token_expires_at - 60:
            self._refresh_access_token()

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """GET request to BeProduct API."""
        self._ensure_token_valid()

        url = f"{self.api_base}/{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        logger.debug(f"GET {url} | params: {params}")

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()

        return response.json()

    def post(
        self,
        endpoint: str,
        body: Optional[Dict] = None,
    ) -> Dict:
        """POST request to BeProduct API."""
        self._ensure_token_valid()

        url = f"{self.api_base}/{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        logger.debug(f"POST {url} | body: {body}")

        response = requests.post(
            url,
            headers=headers,
            json=body or {},
            timeout=30,
        )
        response.raise_for_status()

        return response.json()

# COMMAND ----------

# ============================================================================
# Configuration & Secrets
# ============================================================================

# Retrieve BeProduct credentials from Databricks secrets
# Expected secret scope: "beproduct" with keys:
#   - client_id
#   - client_secret
#   - refresh_token
#   - company_domain
#
# To set up secrets:
# dbutils.secrets.put("beproduct", "client_id", "your_client_id")
# dbutils.secrets.put("beproduct", "client_secret", "your_client_secret")
# dbutils.secrets.put("beproduct", "refresh_token", "your_refresh_token")
# dbutils.secrets.put("beproduct", "company_domain", "your_domain")

try:
    client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
    client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
    refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
    company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")
except Exception as e:
    raise RuntimeError(
        f"Failed to retrieve BeProduct credentials from Databricks secrets.\n"
        f"Ensure you have created a secret scope 'beproduct' with keys:\n"
        f"  - client_id\n"
        f"  - client_secret\n"
        f"  - refresh_token\n"
        f"  - company_domain\n"
        f"Error: {str(e)}"
    ) from e

# Initialize API client
api = BeProductClient(
    client_id=client_id,
    client_secret=client_secret,
    refresh_token=refresh_token,
    company_domain=company_domain,
)

print("✅ BeProduct API client initialized")

# COMMAND ----------

# ============================================================================
# Field Mapping & Schema
# ============================================================================

# Map BeProduct field names to table column names
COMPULSORY_FIELDS = {
    "LF Sytle Number": "lf_style_number",
    "Description": "description",
    "Team": "team",
    "Season": "season",
    "Year": "year",
}

INTERESTED_FIELDS = {
    "Product Status": "product_status",
    "Customer Style Number": "customer_style_number",
    "Product Category": "product_category",
    "Product Sub Category": "product_sub_category",
    "Divison": "division",
    "Brands": "brands",
    "Garment Finish": "garment_finish",
    "Techpack Stage": "techpack_stage",
    "Lot code": "lot_code",
    "Parent Vendor": "parent_vendor",
    "Factory": "factory",
}

# All fields to extract as columns
EXTRACTED_FIELDS = {**COMPULSORY_FIELDS, **INTERESTED_FIELDS}

FOLDER_NAME = "KTB"

print(f"📋 Field mapping:")
print(f"   Compulsory: {len(COMPULSORY_FIELDS)} fields")
print(f"   Interested: {len(INTERESTED_FIELDS)} fields")
print(f"   Total extracted: {len(EXTRACTED_FIELDS)} fields")

# COMMAND ----------

# ============================================================================
# Sync Metadata Management
# ============================================================================

def get_last_sync_timestamp() -> Optional[str]:
    """
    Get last successful sync timestamp for incremental refresh.
    Returns ISO 8601 timestamp or None if not found.
    """
    try:
        spark.sql(f"USE CATALOG {catalog}")
        spark.sql(f"USE SCHEMA {schema}")

        # Check if metadata table exists
        try:
            result = spark.sql(
                f"SELECT last_sync_at FROM {catalog}.{schema}.ktb_styles_sync_meta LIMIT 1"
            ).collect()
            if result:
                return result[0]["last_sync_at"]
        except Exception:
            pass  # Table may not exist yet

        return None

    except Exception as e:
        logger.warning(f"Failed to retrieve sync metadata: {str(e)}")
        return None


def save_sync_metadata(last_sync_at: str) -> None:
    """Save sync metadata for next incremental refresh."""
    try:
        spark.sql(f"USE CATALOG {catalog}")
        spark.sql(f"USE SCHEMA {schema}")

        # Create or replace metadata table
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {catalog}.{schema}.ktb_styles_sync_meta
            USING DELTA
            AS SELECT '{last_sync_at}' AS last_sync_at
            """
        )
        logger.info(f"✅ Sync metadata saved: {last_sync_at}")

    except Exception as e:
        logger.error(f"Failed to save sync metadata: {str(e)}")


# COMMAND ----------

# ============================================================================
# Fetch Data from BeProduct API
# ============================================================================

def fetch_styles(folder_name: str, since_iso: Optional[str] = None) -> List[Dict]:
    """
    Fetch Style records from BeProduct API for a specific folder.

    If since_iso is provided, uses FolderModifiedAt > since_iso filter
    for incremental fetch.

    Returns list of style records.
    """
    logger.info(f"Fetching styles from folder '{folder_name}'...")

    # Build filter if incremental
    filters = None
    if since_iso:
        filters = [
            {
                "field": "FolderModifiedAt",
                "operator": "Gt",
                "value": since_iso,
            }
        ]
        logger.info(f"  Incremental filter: FolderModifiedAt > {since_iso}")

    # API endpoint: /Style/attributes_list
    # This endpoint supports folder filtering and modified_at filtering
    endpoint = "Style/attributes_list"

    styles = []
    page_index = 0
    page_size = 100

    while True:
        try:
            logger.info(f"  Fetching page {page_index} (size: {page_size})...")

            body = {
                "pageIndex": page_index,
                "pageSize": page_size,
                "folderName": folder_name,
            }

            if filters:
                body["filters"] = filters

            response = api.post(endpoint, body=body)

            items = response.get("items", [])
            if not items:
                logger.info(f"  No more records to fetch")
                break

            styles.extend(items)
            logger.info(f"    Fetched {len(items)} records")

            # Check if more pages exist
            total = response.get("total", 0)
            if len(styles) >= total:
                break

            page_index += 1

        except Exception as e:
            logger.error(f"Error fetching page {page_index}: {str(e)}")
            raise

    logger.info(f"✅ Fetched {len(styles)} total styles from '{folder_name}'")
    return styles


# COMMAND ----------

# ============================================================================
# Transform Records to Row Format
# ============================================================================

def extract_field_value(record: Dict, field_path: str) -> Any:
    """
    Extract value from record by field path.
    Supports dot notation for nested fields.
    
    Examples:
      - "description" → record["description"]
      - "attributes.Season" → record["attributes"]["Season"]
    """
    parts = field_path.split(".")
    value = record

    for part in parts:
        if isinstance(value, dict):
            value = value.get(part)
        else:
            return None

        if value is None:
            return None

    return value


def transform_style_record(record: Dict) -> Dict:
    """
    Transform a BeProduct Style record into a Delta table row.

    Extracts compulsory & interested fields as columns.
    Stores full record as JSON.
    Adds system fields (id, folder_name, created_at, modified_at, synced_at).

    Returns dict ready for Delta write.
    """
    # System fields
    row = {
        "id": record.get("id"),
        "folder_name": FOLDER_NAME,
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }

    # Timestamps
    if "createdOn" in record:
        row["created_at"] = record["createdOn"]

    if "modifiedOn" in record:
        row["modified_at"] = record["modifiedOn"]

    # Extract named fields from attributes
    attributes = record.get("attributes", {})

    for beproduct_name, column_name in EXTRACTED_FIELDS.items():
        value = attributes.get(beproduct_name)
        row[column_name] = value

    # Store full record as JSON
    row["data_json"] = json.dumps(record)

    return row


# COMMAND ----------

# ============================================================================
# Main Sync Logic
# ============================================================================

# Determine refresh mode
if refresh_mode == "FULL":
    logger.info("🔄 FULL REFRESH mode")
    since_iso = None
else:  # INCREMENTAL
    logger.info("🔄 INCREMENTAL REFRESH mode")
    since_iso = get_last_sync_timestamp()

    if since_iso:
        logger.info(f"   Last sync: {since_iso}")
    else:
        logger.info(
            "   No previous sync found, falling back to FULL refresh"
        )
        refresh_mode = "FULL"

# Fetch data from BeProduct
print(f"\n📥 Fetching data from BeProduct...")
styles = fetch_styles(folder_name=FOLDER_NAME, since_iso=since_iso)

if not styles:
    print(f"⚠️  No styles to sync")
    dbutils.notebook.exit(0)

# Transform records
print(f"\n🔄 Transforming {len(styles)} records...")
rows = [transform_style_record(s) for s in styles]

# Convert to Spark DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row

# Infer schema from first row
if rows:
    # Get all column names from transformed rows
    all_cols = set()
    for row in rows:
        all_cols.update(row.keys())

    # Create schema with StringType for all columns
    # (allowing flexibility for different data types)
    fields = [
        StructField(col, StringType(), True) for col in sorted(all_cols)
    ]
    schema = StructType(fields)

    # Create DataFrame
    df = spark.createDataFrame(
        [Row(**{col: str(row.get(col)) for col in all_cols} if row.get(col) is not None else {col: None} for col in all_cols)
         for row in rows],
        schema=schema,
    )

    print(f"✅ Transformed {len(rows)} rows")
    print(f"\nDataFrame schema:")
    df.printSchema()

    # COMMAND ----------

    # ============================================================================
    # Write to Delta Table
    # ============================================================================

    print(f"\n💾 Writing to Delta table...")
    print(
        f"   Catalog: {catalog}"
    )
    print(f"   Schema: {schema}")
    print(f"   Table: {table_name}")

    full_table_path = f"{catalog}.{schema}.{table_name}"

    try:
        spark.sql(f"USE CATALOG {catalog}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        # Determine write mode
        write_mode = "overwrite" if refresh_mode == "FULL" else "append"

        # Write data
        (
            df.write.format("delta")
            .mode(write_mode)
            .option("mergeSchema", "true")
            .saveAsTable(full_table_path)
        )

        # Get row count
        row_count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {full_table_path}"
        ).collect()[0]["cnt"]

        print(f"✅ Data written successfully")
        print(f"   Total rows in table: {row_count}")

        # Save metadata for next incremental sync
        sync_timestamp = datetime.now(timezone.utc).isoformat()
        save_sync_metadata(sync_timestamp)

        # Log summary
        print(f"\n📊 Sync Summary")
        print(f"   Mode: {refresh_mode}")
        print(f"   Rows synced: {len(rows)}")
        print(f"   Write mode: {write_mode}")
        print(f"   Table: {full_table_path}")
        print(f"   Total rows: {row_count}")
        print(f"   Timestamp: {sync_timestamp}")

    except Exception as e:
        logger.error(f"Failed to write to Delta table: {str(e)}")
        raise

print("\n✅ BeProduct STYLE sync job completed successfully!")
