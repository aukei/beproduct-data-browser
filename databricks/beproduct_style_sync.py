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
"""

# COMMAND ----------

# ============================================================================
# CELL 1: Setup - Install SDK, Import, Configure Parameters
# ============================================================================

import sys
import subprocess

print("=" * 80)
print("SETUP CELL: Install SDK, Import Libraries, Configure Parameters")
print("=" * 80)

# Install BeProduct SDK
print("\n📦 Installing BeProduct SDK...")
try:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "beproduct"])
    print("✅ BeProduct SDK installed")
except Exception as e:
    print(f"❌ Failed: {str(e)}")
    raise

# Import libraries
print("\n📚 Importing libraries...")
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from beproduct.sdk import BeProduct
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
print("✅ All libraries imported")

# Configure job parameters with widgets
print("\n⚙️  Configuring job parameters...")
dbutils.widgets.text("refresh_mode", "INCREMENTAL", "Refresh Mode (FULL or INCREMENTAL)")
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "beproduct", "Schema Name")
dbutils.widgets.text("table_name", "ktb_styles", "Table Name")

refresh_mode = dbutils.widgets.get("refresh_mode").upper()
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")

print("✅ Parameters configured:")
print(f"   refresh_mode: {refresh_mode}")
print(f"   catalog: {catalog}")
print(f"   schema: {schema}")
print(f"   table_name: {table_name}")

print("\n" + "=" * 80)
print("✅ SETUP COMPLETE - Ready to sync")
print("=" * 80)

# COMMAND ----------

# ============================================================================
# CELL 2: Main Sync Logic
# ============================================================================

print("\n" + "=" * 80)
print("SYNC CELL: Fetch, Transform, and Write Data")
print("=" * 80)

# Get parameters from previous cell
refresh_mode_val = dbutils.widgets.get("refresh_mode").upper()
catalog_val = dbutils.widgets.get("catalog")
schema_val = dbutils.widgets.get("schema")
table_name_val = dbutils.widgets.get("table_name")

# Field mapping configuration
# Keys are BeProduct field names (from headerData.fields[].name)
# Values are Delta table column names
COMPULSORY_FIELDS = {
    "LF Style Number": "lf_style_number",
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
    "Division": "division",  # Note: might be "Divison" in some systems (typo)
    "Brands": "brands",
    "Garment Finish": "garment_finish",
    "Techpack Stage": "techpack_stage",
    "Lot code": "lot_code",
    "Parent Vendor": "parent_vendor",
    "Factory": "factory",
}

EXTRACTED_FIELDS = {**COMPULSORY_FIELDS, **INTERESTED_FIELDS}
FOLDER_NAME = "KTB"

print(f"\n📋 Configuration:")
print(f"   Mode: {refresh_mode_val}")
print(f"   Target: {catalog_val}.{schema_val}.{table_name_val}")
print(f"   Extracted fields: {len(EXTRACTED_FIELDS)}")

# ============================================================================
# Step 1: Get Credentials and Initialize Client
# ============================================================================

print(f"\n{'='*80}")
print("Step 1: Initialize BeProduct SDK")
print("=" * 80)

try:
    print("🔐 Retrieving credentials from Databricks secrets...")
    client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
    client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
    refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
    company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")
    print("   ✓ client_id, client_secret, refresh_token, company_domain retrieved")
    
    print("🚀 Creating BeProduct SDK client...")
    api = BeProduct(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        company_domain=company_domain,
    )
    print("✅ BeProduct SDK client initialized")
except Exception as e:
    print(f"❌ Failed to initialize: {str(e)}")
    raise

# ============================================================================
# Step 2: Check Sync Metadata
# ============================================================================

print(f"\n{'='*80}")
print("Step 2: Check Sync Metadata")
print("=" * 80)

def get_last_sync_timestamp() -> Optional[str]:
    """Get last sync timestamp for incremental refresh."""
    try:
        spark.sql(f"USE CATALOG {catalog_val}")
        spark.sql(f"USE SCHEMA {schema_val}")
        
        tables = spark.sql(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_catalog = '{catalog_val}' "
            f"  AND table_schema = '{schema_val}' "
            f"  AND table_name = 'ktb_styles_sync_meta'"
        ).collect()
        
        if not tables:
            return None
        
        result = spark.sql(
            f"SELECT last_sync_at FROM {catalog_val}.{schema_val}.ktb_styles_sync_meta LIMIT 1"
        ).collect()
        
        if result:
            return result[0]["last_sync_at"]
        return None
    except Exception as e:
        logger.warning(f"Could not retrieve metadata: {str(e)}")
        return None

if refresh_mode_val == "FULL":
    print("🔄 FULL REFRESH mode")
    since_iso = None
else:
    print("🔄 INCREMENTAL REFRESH mode")
    since_iso = get_last_sync_timestamp()
    if since_iso:
        print(f"   Last sync: {since_iso}")
    else:
        print("   No previous sync found, switching to FULL refresh")
        refresh_mode_val = "FULL"
        since_iso = None

# ============================================================================
# Step 3: Fetch Styles
# ============================================================================

print(f"\n{'='*80}")
print("Step 3: Fetch Styles from BeProduct")
print("=" * 80)

try:
    print(f"📥 Fetching styles from folder '{FOLDER_NAME}'...")
    print(f"   (This may take a moment...)")
    
    filters = None
    if since_iso:
        filters = [{
            "field": "FolderModifiedAt",
            "operator": "Gt",
            "value": since_iso,
        }]
        print(f"   Filter: FolderModifiedAt > {since_iso}")
    else:
        print(f"   No filter (fetching all styles)")
    
    styles = []
    all_styles = []
    count = 0
    
    print(f"\n   Calling api.style.attributes_list(filters={filters})...")
    
    # Get iterator
    iterator = api.style.attributes_list(filters=filters)
    print(f"   Iterator created: {type(iterator)}")
    
    # Iterate through results
    for style in iterator:
        all_styles.append(style)
        
        # Show first result with FULL structure for debugging
        if len(all_styles) == 1:
            print(f"\n   🔍 FIRST RESULT STRUCTURE (id={style.get('id', '?')[:16]}...):")
            print(f"      Top-level keys: {list(style.keys())}")
            
            # Check top-level for LF_Style_number
            if "LF_Style_number" in style:
                print(f"      LF_Style_number (top-level): {style['LF_Style_number']}")
            
            # Check attributes (might be in headerData instead)
            attrs = style.get("attributes", {})
            if attrs:
                print(f"\n      Attributes ({len(attrs)} fields):")
                # Show all attribute values for first style
                for key, val in sorted(attrs.items()):
                    print(f"        - '{key}': {repr(val)[:80]}")
            
            # Check headerData - this might contain the attributes
            header_data = style.get("headerData", {})
            if header_data:
                print(f"\n      headerData ({len(header_data)} fields):")
                # Show all header data values for first style
                for key, val in sorted(header_data.items()):
                    val_str = repr(val)[:100]
                    print(f"        - '{key}': {val_str}")
            
            # Check folder
            folder = style.get("folder", {})
            if folder:
                print(f"\n      Folder info:")
                print(f"        - {folder}")
            
            # Extract fields from headerData.fields
            fields_list = header_data.get("fields", [])
            if fields_list:
                print(f"\n      Fields from headerData.fields ({len(fields_list)} fields):")
                fields_dict = {}
                for field in fields_list:
                    field_name = field.get("name", "?")
                    field_value = field.get("value", "")
                    fields_dict[field_name] = field_value
                    print(f"        - '{field_name}': {repr(field_value)[:80]}")
                
                # Verify expected fields exist
                print(f"\n      ✅ VERIFICATION - Checking for expected fields:")
                expected = {
                    "LF Style Number": "LFBP-WM1MJ-002",
                    "Lot code": "112394630",
                    "Brands": "Wrangler",
                    "Customer Style Number": "127-WM1MJ-XXXX-009",
                    "Description": "MOD MALE T1 WASHED LEATHER JACKET",
                    "Garment Finish": "LEATHER JACKET + TBC Wash",
                    "Product Category": "Jackets",
                    "Product Sub Category": "Jacket",
                    "Product Status": "Proto",
                    "Season": "Spring",
                    "Techpack Stage": "Draft",
                    "Year": "2027",
                    "Team": "KTB",
                }
                
                for field_name, expected_value in expected.items():
                    actual_value = fields_dict.get(field_name, "NOT_FOUND")
                    status = "✓" if actual_value != "NOT_FOUND" else "✗"
                    print(f"        {status} {field_name}: {actual_value}")
            
            print()
        
        # Show first few results with detailed info
        if len(all_styles) <= 5:
            style_id = style.get("id", "NO_ID")[:16]
            
            # Try multiple ways to get folder name
            folder_obj = style.get("folder", {})
            folder_name = folder_obj.get("name", "?") if folder_obj else "?"
            
            # Try multiple ways to get LF Style number
            lf_style = (
                style.get("LF_Style_number") or 
                style.get("attributes", {}).get("LF Sytle Number") or
                style.get("attributes", {}).get("LF_Style_number") or
                style.get("attributes", {}).get("LF Style Number") or
                "NO_LF"
            )
            
            print(f"     Result {len(all_styles)}: folder='{folder_name}', lf_style={lf_style}, id={style_id}...")
        
        # Filter by KTB folder (case-sensitive match)
        # Folder is nested: style.get("folder", {}).get("name")
        folder_obj = style.get("folder", {})
        actual_folder = folder_obj.get("name", "") if folder_obj else ""
        if actual_folder == FOLDER_NAME:
            styles.append(style)
            count += 1
            if count % 50 == 0:
                print(f"     Matched {count} styles so far...")
    
    print(f"\n✅ Fetch complete:")
    print(f"   Total results from API: {len(all_styles)}")
    print(f"   Styles with folder='{FOLDER_NAME}': {len(styles)}")
    
    if len(all_styles) == 0:
        print(f"\n   ⚠️  API returned 0 results!")
        print(f"   Possible reasons:")
        print(f"     - No styles exist in your BeProduct instance")
        print(f"     - Credentials are invalid")
        print(f"     - Filter is too restrictive")
    
    if len(all_styles) > 0 and len(styles) == 0:
        unique_folders = set(s.get("folder", {}).get("name", "?") for s in all_styles if s.get("folder"))
        print(f"\n   ⚠️  WARNING: API returned {len(all_styles)} styles, but NONE matched folder '{FOLDER_NAME}'")
        print(f"   Unique folders in results: {unique_folders}")
        print(f"   (Check folder name spelling and case sensitivity)")

except Exception as e:
    print(f"❌ Failed to fetch styles: {str(e)}")
    print(f"   Exception type: {type(e).__name__}")
    import traceback
    traceback.print_exc()
    raise

# Check if we got any data
print(f"\n   Checking data...")
print(f"   styles list length: {len(styles)}")

HAS_DATA = len(styles) > 0

if not HAS_DATA:
    print(f"\n❌ No styles to sync")
    print(f"   Total API results: {len(all_styles)}")
    if len(all_styles) > 0:
        print(f"   But none matched folder '{FOLDER_NAME}'")
    print(f"\n⚠️  No data to process - skipping transformation and write steps")
else:
    print(f"\n✅ {len(styles)} styles ready for processing")

# Only proceed if we have data
if HAS_DATA:
    
    # ============================================================================
    # Step 4: Transform Records
    # ============================================================================

    print(f"\n{'='*80}")
    print("Step 4: Transform Records")
    print("=" * 80)

    def transform_style_record(record: Dict) -> Dict:
        """Transform a BeProduct Style record into a Delta table row."""
        # Extract folder info
        folder_obj = record.get("folder", {})
        folder_name = folder_obj.get("name", "") if folder_obj else ""
        
        row = {
            "id": record.get("id"),
            "folder_name": folder_name,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }
        
        if "createdAt" in record:
            row["created_at"] = record["createdAt"]
        if "modifiedAt" in record:
            row["modified_at"] = record["modifiedAt"]
        
        # Extract attributes from headerData.fields (list of field objects)
        # Each field has: {id, name, value, type, required, ...}
        header_data = record.get("headerData", {})
        fields_list = header_data.get("fields", [])
        
        # Convert fields list to dict keyed by field name
        attributes = {}
        for field in fields_list:
            field_name = field.get("name", "")
            field_value = field.get("value")
            if field_name:
                attributes[field_name] = field_value
        
        # Extract compulsory and interested fields
        for beproduct_name, column_name in EXTRACTED_FIELDS.items():
            row[column_name] = attributes.get(beproduct_name)
        
        # Store full record as JSON
        row["data_json"] = json.dumps(record)
        
        return row

    try:
        print(f"🔄 Transforming {len(styles)} records...")
        rows = [transform_style_record(s) for s in styles]
        print(f"✅ Transformed {len(rows)} rows")
    except Exception as e:
        print(f"❌ Failed to transform: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

    # ============================================================================
    # Step 5: Create Spark DataFrame
    # ============================================================================

    print(f"\n{'='*80}")
    print("Step 5: Create Spark DataFrame")
    print("=" * 80)

    try:
        print(f"📊 Creating DataFrame from {len(rows)} rows...")
        
        # Get all column names
        all_cols = set()
        for row in rows:
            all_cols.update(row.keys())
        sorted_cols = sorted(all_cols)
        
        print(f"   Columns: {len(sorted_cols)}")
        
        # Create schema
        fields = [StructField(col, StringType(), True) for col in sorted_cols]
        schema = StructType(fields)
        
        # Convert to Spark rows
        def row_to_spark_row(row_dict, cols):
            return Row(**{col: str(row_dict.get(col)) if row_dict.get(col) is not None else None for col in cols})
        
        spark_rows = [row_to_spark_row(row, sorted_cols) for row in rows]
        df = spark.createDataFrame(spark_rows, schema=schema)
        
        row_count = df.count()
        print(f"✅ DataFrame created: {row_count} rows")
        
    except Exception as e:
        print(f"❌ Failed to create DataFrame: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

    # ============================================================================
    # Step 6: Write to Delta Table
    # ============================================================================

    print(f"\n{'='*80}")
    print("Step 6: Write to Delta Table")
    print("=" * 80)

    full_table_path = f"{catalog_val}.{schema_val}.{table_name_val}"

    try:
        print(f"💾 Writing to {full_table_path}...")
        
        spark.sql(f"USE CATALOG {catalog_val}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_val}.{schema_val}")
        
        write_mode = "overwrite" if refresh_mode_val == "FULL" else "append"
        print(f"   Write mode: {write_mode}")
        
        (
            df.write.format("delta")
            .mode(write_mode)
            .option("mergeSchema", "true")
            .saveAsTable(full_table_path)
        )
        
        final_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_path}").collect()[0]["cnt"]
        print(f"✅ Data written successfully")
        print(f"   Total rows in table: {final_count}")
        
    except Exception as e:
        print(f"❌ Failed to write: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

    # ============================================================================
    # Step 7: Save Sync Metadata
    # ============================================================================

    print(f"\n{'='*80}")
    print("Step 7: Save Sync Metadata")
    print("=" * 80)

    try:
        sync_timestamp = datetime.now(timezone.utc).isoformat()
        spark.sql(f"USE CATALOG {catalog_val}")
        spark.sql(f"USE SCHEMA {schema_val}")
        
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {catalog_val}.{schema_val}.ktb_styles_sync_meta
            USING DELTA
            AS SELECT '{sync_timestamp}' AS last_sync_at
            """
        )
        print(f"✅ Metadata saved: {sync_timestamp}")
    except Exception as e:
        print(f"⚠️  Could not save metadata: {str(e)}")

# ============================================================================
# Summary
# ============================================================================

    print(f"\n{'='*80}")
    print("SYNC SUMMARY")
    print("=" * 80)

    print(f"\n✅ Job completed successfully!")
    print(f"\n   Mode: {refresh_mode_val}")
    print(f"   Rows synced: {len(rows)}")
    print(f"   Write mode: {write_mode}")
    print(f"   Table: {full_table_path}")
    print(f"   Total rows: {final_count}")
    print(f"   Timestamp: {sync_timestamp}")

    print(f"\n{'='*80}")

else:
    # No data to process
    print(f"\n{'='*80}")
    print("NO DATA TO SYNC")
    print("=" * 80)
    print(f"\n⚠️  Job completed with no data")
    print(f"\n   API returned: {len(all_styles)} total styles")
    print(f"   Matched folder '{FOLDER_NAME}': 0 styles")
    
    if len(all_styles) > 0:
        unique_folders = set(s.get("folder", {}).get("name", "?") for s in all_styles if s.get("folder"))
        print(f"\n   Available folders in your account:")
        for folder in sorted(unique_folders):
            print(f"     - {folder}")
        print(f"\n   Please check:")
        print(f"     1. Folder name spelling (is it '{FOLDER_NAME}' or something else?)")
        print(f"     2. Folder name case-sensitivity (should be exactly: {FOLDER_NAME})")
    else:
        print(f"\n   Please check:")
        print(f"     1. BeProduct credentials are valid")
        print(f"     2. Your account has styles defined")
    
    print(f"\n{'='*80}")
