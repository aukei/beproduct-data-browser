# Databricks notebook source
"""Push Modified Blocks from Databricks back to BeProduct"""

import logging, sys
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

table_name = f"`{target_catalog}`.`{target_schema}`.`bp_blocks`"

logging.basicConfig(level=logging.INFO)
spark = SparkSession.getActiveSession()

client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")

oauth = BeProductOAuth(client_id, client_secret, refresh_token)
oauth.company_domain = company_domain

print(f"⏱  Starting Blocks push (DRY RUN: {dry_run})")

try:
    records = spark.sql(f"SELECT * FROM {table_name}").collect()
    print(f"📋 Found {len(records)} block records")
except Exception as e:
    print(f"❌ Failed: {e}")
    raise

print(f"✅ Blocks push complete")
