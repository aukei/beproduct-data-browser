# Databricks notebook source
"""Sync Blocks from BeProduct API to Databricks Delta"""

import logging
from datetime import datetime, timezone
import sys
sys.path.insert(0, "/Workspace/beproduct/notebooks/inbound")

from beproduct_api_utils import BeProductClient, normalize_block_row
from pyspark.sql import SparkSession

try:
    incremental_mode = dbutils.widgets.get("incremental_mode") == "true"
except:
    incremental_mode = False

try:
    target_catalog = dbutils.widgets.get("target_catalog")
    target_schema = dbutils.widgets.get("target_schema")
    batch_size = int(dbutils.widgets.get("batch_size"))
except:
    target_catalog = "main"
    target_schema = "beproduct"
    batch_size = 1000

table_name = f"`{target_catalog}`.`{target_schema}`.`bp_blocks`"

spark = SparkSession.getActiveSession()
logging.basicConfig(level=logging.INFO)

client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")

client = BeProductClient(client_id, client_secret, refresh_token, company_domain)

sync_start_time = datetime.now(timezone.utc).isoformat()
sync_batch_id = f"beproduct_sync_blocks_{int(datetime.now(timezone.utc).timestamp())}"

print(f"⏱  Starting Blocks sync")

incremental_filter = None
if incremental_mode:
    try:
        result = spark.sql(f"SELECT MAX(synced_at) as last_sync_at FROM {table_name}").collect()
        if result and result[0]["last_sync_at"]:
            incremental_filter = result[0]["last_sync_at"]
    except:
        pass

rows = []
total_fetched = 0

try:
    print(f"📥 Fetching blocks from BeProduct...")
    
    for api_row in client.fetch_blocks(incremental_filter=incremental_filter):
        rows.append(normalize_block_row(api_row, sync_start_time, sync_batch_id))
        total_fetched += 1
        
        if len(rows) >= batch_size:
            df = spark.createDataFrame(rows)
            try:
                df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
            except Exception as e:
                if "does not exist" in str(e):
                    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)
                else:
                    raise
            rows = []
    
    if rows:
        df = spark.createDataFrame(rows)
        try:
            df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
        except Exception as e:
            if "does not exist" not in str(e):
                raise
    
    print(f"✅ Fetched {total_fetched} blocks")

except Exception as e:
    print(f"❌ Failed: {e}")
    raise

print(f"✅ Blocks sync complete")
