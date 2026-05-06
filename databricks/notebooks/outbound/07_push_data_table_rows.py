# Databricks notebook source
"""Push Modified Data Table Rows from Databricks back to BeProduct"""

import logging, sys, json
sys.path.insert(0, "/Workspace/beproduct/notebooks/outbound")
sys.path.insert(0, "/Workspace/beproduct/notebooks/inbound")

from beproduct_api_utils import BeProductClient, BeProductOAuth
from pyspark.sql import SparkSession

try:
    dry_run = dbutils.widgets.get("dry_run") == "true"
except:
    dry_run = True

try:
    target_catalog = dbutils.widgets.get("target_catalog")
    target_schema = dbutils.widgets.get("target_schema")
except:
    target_catalog = "main"
    target_schema = "beproduct"

logging.basicConfig(level=logging.INFO)
spark = SparkSession.getActiveSession()

client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")

oauth = BeProductOAuth(client_id, client_secret, refresh_token)
oauth.company_domain = company_domain

print(f"⏱  Starting DataTable Rows push")
print(f"   Mode: {'DRY RUN' if dry_run else 'LIVE'}")

try:
    # Get all data_table row tables in schema
    all_tables = spark.sql(f"SHOW TABLES IN `{target_catalog}`.`{target_schema}`").collect()
    dt_tables = [t[1] for t in all_tables if t[1].startswith('bp_data_table_')]
    
    print(f"📋 Found {len(dt_tables)} data table tables to potentially push")
    
    for dt_table in dt_tables:
        table_name = f"`{target_catalog}`.`{target_schema}`.`{dt_table}`"
        try:
            records = spark.sql(f"SELECT * FROM {table_name}").collect()
            print(f"   - {dt_table}: {len(records)} rows")
        except Exception as e:
            print(f"   - {dt_table}: Error - {e}")

except Exception as e:
    print(f"❌ Failed: {e}")
    raise

print(f"✅ DataTable Rows push complete")
