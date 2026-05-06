# Databricks notebook source
"""Sync Directory from BeProduct API to Databricks Delta"""

import logging
from datetime import datetime, timezone
import sys
sys.path.insert(0, "/Workspace/beproduct/notebooks/inbound")

from beproduct_api_utils import BeProductClient, normalize_directory_row
from pyspark.sql import SparkSession

try:
    target_catalog = dbutils.widgets.get("target_catalog")
    target_schema = dbutils.widgets.get("target_schema")
except:
    target_catalog = "main"
    target_schema = "beproduct"

table_name = f"`{target_catalog}`.`{target_schema}`.`bp_directory`"

spark = SparkSession.getActiveSession()
logging.basicConfig(level=logging.INFO)

client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")

client = BeProductClient(client_id, client_secret, refresh_token, company_domain)

sync_start_time = datetime.now(timezone.utc).isoformat()
sync_batch_id = f"beproduct_sync_directory_{int(datetime.now(timezone.utc).timestamp())}"

print(f"⏱  Starting Directory sync")

rows = []
total_fetched = 0

try:
    print(f"📥 Fetching directory from BeProduct...")
    
    for api_row in client.fetch_directory():
        rows.append(normalize_directory_row(api_row, sync_start_time, sync_batch_id))
        total_fetched += 1
    
    if rows:
        df = spark.createDataFrame(rows)
        try:
            df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)
        except Exception as e:
            if "does not exist" in str(e):
                df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)
            else:
                raise
    
    print(f"✅ Fetched {total_fetched} directory records")

except Exception as e:
    print(f"❌ Failed: {e}")
    raise

print(f"✅ Directory sync complete")
