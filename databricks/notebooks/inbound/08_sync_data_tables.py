# Databricks notebook source
"""Sync Data Tables from BeProduct API to Databricks Delta

Creates one table per data table definition (bp_data_table_<table_id>)
with all rows flattened and available for analytics.
"""

import logging
from datetime import datetime, timezone
import sys
sys.path.insert(0, "/Workspace/beproduct/notebooks/inbound")

from beproduct_api_utils import BeProductClient, normalize_data_table_definition_row, normalize_data_table_row
from pyspark.sql import SparkSession

try:
    target_catalog = dbutils.widgets.get("target_catalog")
    target_schema = dbutils.widgets.get("target_schema")
    batch_size = int(dbutils.widgets.get("batch_size"))
except:
    target_catalog = "main"
    target_schema = "beproduct"
    batch_size = 1000

spark = SparkSession.getActiveSession()
logging.basicConfig(level=logging.INFO)

client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")

client = BeProductClient(client_id, client_secret, refresh_token, company_domain)

sync_start_time = datetime.now(timezone.utc).isoformat()
sync_batch_id = f"beproduct_sync_data_tables_{int(datetime.now(timezone.utc).timestamp())}"

print(f"⏱  Starting DataTables sync")

# First, sync list of tables
tables_def_table = f"`{target_catalog}`.`{target_schema}`.`bp_data_tables`"

table_defs = []
total_tables = 0

try:
    print(f"📥 Fetching data table definitions...")
    
    for api_row in client.fetch_data_tables():
        table_defs.append(api_row)
        total_tables += 1
    
    print(f"✅ Found {total_tables} data table definitions")
    
    if table_defs:
        rows = [normalize_data_table_definition_row(t, sync_start_time, sync_batch_id) for t in table_defs]
        df = spark.createDataFrame(rows)
        try:
            df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(tables_def_table)
        except:
            pass

except Exception as e:
    print(f"❌ Failed to fetch table definitions: {e}")
    raise

# Now sync rows for each table
total_rows = 0

try:
    for table_def in table_defs:
        table_id = table_def.get("id")
        table_name = table_def.get("name", "unknown")
        
        print(f"📥 Syncing rows for: {table_name} ({table_id})...")
        
        # Use safe table name
        safe_table_name = table_name.lower().replace(" ", "_").replace("-", "_")
        target_table = f"`{target_catalog}`.`{target_schema}`.`bp_data_table_{safe_table_name}`"
        
        rows = []
        row_count = 0
        
        try:
            for row_data in client.fetch_data_table_rows(table_id):
                normalized = normalize_data_table_row(
                    row_data, table_id, table_name,
                    sync_start_time, sync_batch_id
                )
                rows.append(normalized)
                row_count += 1
                total_rows += 1
                
                if len(rows) >= batch_size:
                    df = spark.createDataFrame(rows)
                    try:
                        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)
                    except Exception as e:
                        if "does not exist" in str(e):
                            df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
                        else:
                            raise
                    rows = []
            
            if rows:
                df = spark.createDataFrame(rows)
                try:
                    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)
                except Exception as e:
                    if "does not exist" not in str(e):
                        raise
            
            print(f"   ✅ Synced {row_count} rows to {target_table}")
        
        except Exception as e:
            print(f"   ❌ Error syncing {table_name}: {e}")
            # Continue with next table

except Exception as e:
    print(f"❌ Failed during data table sync: {e}")
    raise

print(f"✅ DataTables sync complete ({total_rows} total rows)")
