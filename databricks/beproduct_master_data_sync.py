# Databricks notebook source
"""
BeProduct Master Data Sync Job
===============================

Pulls valid dropdown values (Master Data) from BeProduct API using authenticated requests.

This job fetches enumerated values for dropdown/multiselect fields:
  - BRANDS, TEAM, SEASON, YEAR
  - PRODUCT STATUS, PRODUCT CATEGORY, PRODUCT SUB CATEGORY
  - DIVISION, TECHPACK STAGE, GARMENT FINISH
  - PARENT VENDOR, FACTORY

Master Data is used for validation when pushing changes back to BeProduct.

Schedule: Daily (e.g., 8am UTC, before pull job)

Parameters:
  - catalog: Target Databricks catalog (default: "lft")
  - schema: Target Databricks schema (default: "beproduct")
"""

# COMMAND ----------

import sys
import subprocess

print("=" * 80)
print("SETUP CELL: Install packages, Import Libraries, Configure Parameters")
print("=" * 80)

# Install required packages
print("\n📦 Installing dependencies...")
try:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "beproduct", "requests"])
    print("✅ Dependencies installed (beproduct, requests)")
except Exception as e:
    print(f"❌ Failed: {str(e)}")
    raise

# Import libraries
print("\n📚 Importing libraries...")
import json
import logging
import requests
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from beproduct.sdk import BeProduct

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
print("✅ All libraries imported")

# Configure job parameters with widgets
print("\n⚙️  Configuring job parameters...")
dbutils.widgets.text("catalog", "lft", "Catalog Name")
dbutils.widgets.text("schema", "beproduct", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print("✅ Parameters configured:")
print(f"   catalog: {catalog}")
print(f"   schema: {schema}")

print("\n" + "=" * 80)
print("✅ SETUP COMPLETE - Ready to sync master data")
print("=" * 80)

# COMMAND ----------

print("\n" + "=" * 80)
print("MASTER DATA SYNC CELL: Fetch and Store Master Data")
print("=" * 80)

# Get parameters from previous cell
catalog_val = dbutils.widgets.get("catalog")
schema_val = dbutils.widgets.get("schema")

# ============================================================================
# Step 1: Authenticate with BeProduct API
# ============================================================================

print(f"\n{'='*80}")
print("Step 1: Authenticate with BeProduct API")
print("=" * 80)

try:
    print("🔐 Retrieving credentials from Databricks secrets...")
    client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
    client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
    refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
    company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")
    print("   ✓ All credentials retrieved")
    
    print("🚀 Initializing BeProduct SDK...")
    api = BeProduct(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        company_domain=company_domain,
    )
    print("✅ BeProduct SDK initialized (OAuth handled internally)")
    
    # Extract access token from SDK for API calls
    # The SDK stores the token after authentication
    if hasattr(api, 'access_token'):
        access_token = api.access_token
    elif hasattr(api, '_access_token'):
        access_token = api._access_token
    else:
        # Make a dummy request to trigger token generation
        try:
            api.get_styles()  # This should populate the token
            access_token = getattr(api, 'access_token', None) or getattr(api, '_access_token', None)
        except:
            pass
    
    if not access_token:
        print("⚠️  Could not extract token from SDK, will attempt requests without explicit token")
        access_token = None
    else:
        print(f"✅ Access token obtained from SDK")
    
except Exception as e:
    print(f"❌ Authentication failed: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

# ============================================================================
# Step 2: Define Master Data to Fetch
# ============================================================================

print(f"\n{'='*80}")
print("Step 2: Define Master Data to Fetch")
print("=" * 80)

# Map of master data type → field ID (from Swagger API docs)
# API pattern: /api/{company}/MasterData/{fieldId}
# Note: fieldId is the internal field identifier extracted from data_json
MASTER_DATA_FIELD_IDS = {
    "brands": "brands_multi",  # MultiSelect field
    "teams": "team",            # DropDown field
    "seasons": "season",        # DropDown field
    "years": "year",            # DropDown field
    "product_status": "style_status",  # DropDown field
    "product_category": "product_category",  # DropDown field
    "product_sub_category": "product_sub_category",  # DropDown field
    "division": "division",     # DropDown field
    "techpack_stage": "techpack_stage",  # DropDown field
    "garment_finish": "garment_finish",  # Text field (not typical master data)
    "parent_vendor": "parent_vendor",    # PartnerDropDown field
    "factory": "factory",       # PartnerDropDown field
}

print(f"📋 Master data types to sync: {len(MASTER_DATA_FIELD_IDS)}")
for data_type, field_id in MASTER_DATA_FIELD_IDS.items():
    print(f"   - {data_type}: (field_id: {field_id})")

# ============================================================================
# Step 3: Fetch Master Data from API
# ============================================================================

print(f"\n{'='*80}")
print("Step 3: Fetch Master Data from BeProduct API")
print("=" * 80)

master_data_cache = {}

base_url = f"https://developers.beproduct.com/api/{company_domain}/MasterData"

print(f"\n📡 API Base URL: {base_url}")
print(f"✅ SDK initialized and ready for authenticated requests")

# Setup headers with Bearer token (if available)
headers = {
    "Content-Type": "application/json"
}

if access_token:
    headers["Authorization"] = f"Bearer {access_token}"
    print(f"ℹ️  Using Bearer token for authentication")

for data_type, field_id in MASTER_DATA_FIELD_IDS.items():
    try:
        print(f"\n🔍 Fetching {data_type} ({field_id})...")
        
        url = f"{base_url}/{field_id}"
        print(f"   URL: {url}")
        
        # Try with SDK session first (if available), then with requests
        response = None
        
        # Try using SDK's session if available
        if hasattr(api, 'session'):
            try:
                response = api.session.get(url, timeout=30)
            except Exception as sdk_session_error:
                print(f"   ℹ️  SDK session failed: {str(sdk_session_error)[:80]}")
        
        # Fallback to direct requests
        if response is None:
            response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            master_data_cache[data_type] = data
            
            # Count items
            if isinstance(data, list):
                count = len(data)
            elif isinstance(data, dict):
                count = len(data)
            else:
                count = 1
            
            print(f"   ✅ Success: {count} items")
        
        elif response.status_code == 401:
            print(f"   ❌ Unauthorized (401) - Authentication failed")
            print(f"      Response: {response.text[:200]}")
            master_data_cache[data_type] = []
        
        elif response.status_code == 404:
            print(f"   ⚠️  Endpoint not found (404) - field may not support master data")
            master_data_cache[data_type] = []
        
        else:
            print(f"   ⚠️  Failed ({response.status_code}): {response.text[:200]}")
            master_data_cache[data_type] = []
    
    except requests.exceptions.ConnectionError as ce:
        print(f"   ❌ Connection Error: {str(ce)[:100]}")
        print(f"   ⚠️  Verify company_domain is correct in secrets")
        master_data_cache[data_type] = []
    
    except Exception as e:
        print(f"   ❌ Error: {str(e)[:100]}")
        import traceback
        traceback.print_exc()
        master_data_cache[data_type] = []

# ============================================================================
# Step 4: Transform and Store Master Data
# ============================================================================

print(f"\n{'='*80}")
print("Step 4: Transform and Store Master Data in Delta Lake")
print("=" * 80)

total_stored = 0

for data_type, data_list in master_data_cache.items():
    try:
        print(f"\n📝 Processing {data_type}...")
        
        if not data_list:
            print(f"   ⚠️  No data to store (empty result)")
            continue
        
        # Create table name
        table_name = f"beproduct_master_{data_type}"
        full_table_path = f"{catalog_val}.{schema_val}.{table_name}"
        
        # Convert to rows
        rows = []
        if isinstance(data_list, list):
            for item in data_list:
                row = {
                    "value": item.get("id") or item.get("name") or item,
                    "label": item.get("name") or item.get("label") or item,
                    "data_json": json.dumps(item),
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                }
                rows.append(row)
        
        if rows:
            # Create DataFrame
            df = spark.createDataFrame(rows)
            
            # Write to Delta (overwrite to keep only latest master data)
            df.write.format("delta").mode("overwrite").saveAsTable(full_table_path)
            
            print(f"   ✅ Stored {len(rows)} items to {table_name}")
            total_stored += len(rows)
        else:
            print(f"   ⚠️  No rows to store")
    
    except Exception as e:
        print(f"   ❌ Failed: {str(e)}")
        import traceback
        traceback.print_exc()

# ============================================================================
# Step 5: Create Validation Summary
# ============================================================================

print(f"\n{'='*80}")
print("Step 5: Master Data Validation Summary")
print("=" * 80)

try:
    spark.sql(f"USE CATALOG {catalog_val}")
    spark.sql(f"USE SCHEMA {schema_val}")
    
    print(f"\n📊 Master Data Tables in Delta Lake:")
    
    for data_type in MASTER_DATA_FIELD_IDS.keys():
        table_name = f"beproduct_master_{data_type}"
        
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog_val}.{schema_val}.{table_name}").collect()[0]["cnt"]
            status = "✓" if count > 0 else "✗"
            print(f"   {status} {table_name}: {count} items")
        except:
            print(f"   ✗ {table_name}: (table not found or error)")

except Exception as e:
    print(f"❌ Failed to create summary: {str(e)}")

# ============================================================================
# Summary
# ============================================================================

print(f"\n{'='*80}")
print("MASTER DATA SYNC COMPLETE")
print("=" * 80)

print(f"\n✅ Synced {total_stored} total master data items")
print(f"   Catalog: {catalog_val}")
print(f"   Schema: {schema_val}")
print(f"   Timestamp: {datetime.now(timezone.utc).isoformat()}")

if total_stored == 0:
    print(f"\n⚠️  No data was synced - likely API connectivity or endpoint issues")
    print(f"   See troubleshooting guide in MASTER_DATA_SETUP.md")

print(f"\n📋 Available master data tables:")
print(f"   - beproduct_master_brands")
print(f"   - beproduct_master_teams")
print(f"   - beproduct_master_seasons")
print(f"   - beproduct_master_years")
print(f"   - beproduct_master_product_status")
print(f"   - beproduct_master_product_category")
print(f"   - beproduct_master_product_sub_category")
print(f"   - beproduct_master_division")
print(f"   - beproduct_master_techpack_stage")
print(f"   - beproduct_master_garment_finish")
print(f"   - beproduct_master_parent_vendor")
print(f"   - beproduct_master_factory")

print(f"\n{'='*80}")
