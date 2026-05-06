# Databricks notebook source
"""
Sync Styles from BeProduct API to Databricks Delta

This notebook:
1. Fetches all styles from BeProduct API (or incremental if sync completed before)
2. Normalizes to Delta table schema
3. Writes/merges into main.beproduct.bp_styles Delta table
4. Updates sync metadata

Schedule: Every 6 hours
Parameters:
  - incremental_mode (bool): if True, only fetch records modified since last sync
  - target_catalog (str): Unity Catalog name (default: main)
  - target_schema (str): Schema name (default: beproduct)
  - batch_size (int): rows per commit (default: 1000)
"""

import logging
from datetime import datetime, timezone
from typing import Optional

# Databricks imports
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# Import shared utilities (must be in same workspace)
import sys
sys.path.insert(0, "/Workspace/beproduct/notebooks/inbound")

from beproduct_api_utils import (
    BeProductClient, BeProductOAuth,
    normalize_style_row,
)

# ============================================================================
# PARAMETERS
# ============================================================================

# Get parameters from widget (set by job)
try:
    incremental_mode = dbutils.widgets.get("incremental_mode") == "true"
except:
    incremental_mode = False  # Default to full sync

try:
    target_catalog = dbutils.widgets.get("target_catalog")
except:
    target_catalog = "main"

try:
    target_schema = dbutils.widgets.get("target_schema")
except:
    target_schema = "beproduct"

try:
    batch_size = int(dbutils.widgets.get("batch_size"))
except:
    batch_size = 1000

table_name = f"`{target_catalog}`.`{target_schema}`.`bp_styles`"

# ============================================================================
# SETUP
# ============================================================================

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

spark = SparkSession.getActiveSession()

# Retrieve BeProduct credentials from Databricks Secrets
try:
    client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
    client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
    refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
    company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")
except Exception as e:
    print(f"❌ Failed to retrieve secrets: {e}")
    print("   Make sure beproduct secret scope is configured with: client_id, client_secret, refresh_token, company_domain")
    raise

# Create client
try:
    oauth = BeProductOAuth(client_id, client_secret, refresh_token)
    oauth.company_domain = company_domain
    client = BeProductClient(client_id, client_secret, refresh_token, company_domain)
except Exception as e:
    print(f"❌ Failed to create BeProduct client: {e}")
    raise

# ============================================================================
# FETCH DATA
# ============================================================================

sync_start_time = datetime.now(timezone.utc).isoformat()
sync_batch_id = f"beproduct_sync_styles_{int(datetime.now(timezone.utc).timestamp())}"

print(f"⏱  Starting Styles sync")
print(f"   Batch ID: {sync_batch_id}")
print(f"   Mode: {'Incremental' if incremental_mode else 'Full'}")

# Determine incremental filter
incremental_filter = None
if incremental_mode:
    try:
        # Query last sync time from metadata table (if it exists)
        result = spark.sql(f"""
            SELECT MAX(synced_at) as last_sync_at 
            FROM {table_name}
            WHERE synced_at IS NOT NULL
        """).collect()
        
        if result and result[0]["last_sync_at"]:
            incremental_filter = result[0]["last_sync_at"]
            print(f"   Last sync: {incremental_filter}")
    except Exception as e:
        print(f"   ⚠️  Could not read last sync time: {e}")
        print(f"   Falling back to full sync")
        incremental_mode = False

# ============================================================================
# FETCH FROM BEPRODUCT
# ============================================================================

rows = []
total_fetched = 0

try:
    print(f"📥 Fetching styles from BeProduct...")
    
    for api_row in client.fetch_styles(incremental_filter=incremental_filter):
        # Normalize to Delta row
        normalized = normalize_style_row(api_row, sync_start_time, sync_batch_id)
        rows.append(normalized)
        total_fetched += 1
        
        # Batch insert
        if len(rows) >= batch_size:
            df = spark.createDataFrame(rows)
            
            # Try to write as append, fallback to create if table doesn't exist
            try:
                df.write.format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(table_name)
                print(f"   ✅ Wrote batch of {len(rows)} rows")
            except Exception as e:
                if "does not exist" in str(e):
                    print(f"   📝 Creating table {table_name}...")
                    df.write.format("delta") \
                        .mode("overwrite") \
                        .option("mergeSchema", "true") \
                        .saveAsTable(table_name)
                    print(f"   ✅ Created table and wrote {len(rows)} rows")
                else:
                    print(f"   ❌ Write error: {e}")
                    raise
            
            rows = []
    
    # Final batch
    if rows:
        df = spark.createDataFrame(rows)
        try:
            df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(table_name)
            print(f"   ✅ Wrote final batch of {len(rows)} rows")
        except Exception as e:
            if "does not exist" in str(e):
                df.write.format("delta") \
                    .mode("overwrite") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(table_name)
            else:
                raise
    
    print(f"📥 ✅ Fetched {total_fetched} styles from BeProduct API")

except Exception as e:
    print(f"❌ Fetch failed: {e}")
    raise

# ============================================================================
# VERIFY & LOG
# ============================================================================

try:
    result_df = spark.sql(f"SELECT COUNT(*) as total_records FROM {table_name}")
    total = result_df.collect()[0]["total_records"]
    print(f"✅ Final table state: {total:,} total records in {table_name}")
    print(f"   Fetched this run: {total_fetched}")
except Exception as e:
    print(f"⚠️  Could not verify final count: {e}")

print(f"✅ Styles sync complete")
