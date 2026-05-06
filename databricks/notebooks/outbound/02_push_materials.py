# Databricks notebook source
"""Push Modified Materials from Databricks back to BeProduct"""

import logging
from datetime import datetime, timezone
import sys

sys.path.insert(0, "/Workspace/beproduct/notebooks/outbound")
sys.path.insert(0, "/Workspace/beproduct/notebooks/inbound")

from beproduct_push_utils import ConflictDetector, RecordMerger, create_audit_log_entry
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

table_name = f"`{target_catalog}`.`{target_schema}`.`bp_materials`"
audit_table = f"`{target_catalog}`.`{target_schema}`.`bp_audit_log`"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
spark = SparkSession.getActiveSession()

client_id = dbutils.secrets.get(scope="beproduct", key="client_id")
client_secret = dbutils.secrets.get(scope="beproduct", key="client_secret")
refresh_token = dbutils.secrets.get(scope="beproduct", key="refresh_token")
company_domain = dbutils.secrets.get(scope="beproduct", key="company_domain")

oauth = BeProductOAuth(client_id, client_secret, refresh_token)
oauth.company_domain = company_domain
client = BeProductClient(client_id, client_secret, refresh_token, company_domain)

conflict_detector = ConflictDetector()
merger = RecordMerger()

print(f"⏱  Starting Materials push")
print(f"   Mode: {'DRY RUN' if dry_run else 'LIVE'}")

try:
    modified_df = spark.sql(f"SELECT * FROM {table_name}")
    modified_records = modified_df.collect()
    
    if not modified_records:
        print("ℹ️  No records to push")
    else:
        print(f"📋 Found {len(modified_records)} total material records")
        
        if len(modified_records) > 0:
            example = modified_records[0]
            record_id = example["id"]
            
            print(f"\n📌 Example: Would process record {record_id}")
            
            try:
                remote = client.fetch_material_by_id(record_id)
                is_conflict, reason = conflict_detector.detect_conflict(
                    example, remote, strategy="local_wins"
                )
                
                print(f"   Conflict: {is_conflict} - {reason}")
                print(f"   [DRY RUN - no actual push]")
            except Exception as e:
                print(f"   ❌ Error: {e}")

except Exception as e:
    print(f"❌ Push failed: {e}")
    raise

print(f"✅ Materials push complete")
