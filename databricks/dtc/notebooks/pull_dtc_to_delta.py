# Databricks notebook source
"""
DTC Master Chart Sync Notebook

Syncs a specific DTC request to a Databricks Delta table.
Can be run as a scheduled job.

Target Table: lft.beproduct.dtc_master_chart_uat
Source: DTC API (request ID: 69f076f0b7247a661226be9a)
"""

# COMMAND ----------

# Import libraries
import sys
import logging
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("=" * 80)
print("DTC MASTER CHART SYNC")
print("=" * 80)
print(f"Start time: {datetime.now(timezone.utc).isoformat()}")

# COMMAND ----------

# CELL 1: Configuration & Secrets
print("\n[CELL 1] Configuration & Secrets")
print("-" * 80)

# Define widgets with defaults (will be overridden by job parameters if provided)
try:
    # These lines create the widgets with default values for interactive runs
    dbutils.widgets.text("dtc_workspace_name", "Kontoor", "DTC Workspace Name")
    dbutils.widgets.text("dtc_request_id", "69f076f0b7247a661226be9a", "DTC Request ID")
    dbutils.widgets.text("dtc_environment", "uat", "DTC Environment (uat/prod)")
    dbutils.widgets.text("target_catalog", "lft", "Target Catalog")
    dbutils.widgets.text("target_schema", "beproduct", "Target Schema")
    dbutils.widgets.text("target_table", "dtc_master_chart_uat", "Target Table Name")
    dbutils.widgets.text("write_mode", "overwrite", "Write Mode (overwrite/append)")
except Exception as e:
    # If widgets already exist (running as job), this is expected
    pass

# Parameters (can be overridden by Databricks job)
DTC_WORKSPACE_NAME = dbutils.widgets.get("dtc_workspace_name")
DTC_REQUEST_ID = dbutils.widgets.get("dtc_request_id")
DTC_ENVIRONMENT = dbutils.widgets.get("dtc_environment")
TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA = dbutils.widgets.get("target_schema")
TARGET_TABLE = dbutils.widgets.get("target_table")
WRITE_MODE = dbutils.widgets.get("write_mode")

print(f"Workspace: {DTC_WORKSPACE_NAME}")
print(f"Request ID: {DTC_REQUEST_ID}")
print(f"Environment: {DTC_ENVIRONMENT}")
print(f"Target: {TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")

# Get DTC API key from Databricks secrets
try:
    dtc_api_key = dbutils.secrets.get("beproduct", "dtc_api_key_uat")
    print("✅ DTC API key loaded from secrets")
except Exception as e:
    print(f"❌ Failed to load DTC API key: {e}")
    print("   You need to set up the secret:")
    print("   databricks secrets put-secret beproduct dtc_api_key_uat --string-value YOUR_KEY")
    raise

# COMMAND ----------

# CELL 2: Import DTCConnector
print("\n[CELL 2] Import DTCConnector")
print("-" * 80)

# Add python library path for imports
# Notebook location: /Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta
# Python modules location: /Workspace/Repos/beproduct-sync/DTC/python/
python_path = "/Workspace/Repos/beproduct-sync/DTC/python"
sys.path.insert(0, python_path)
print(f"📁 Python path: {python_path}")

try:
    from connectors.dtc import DTCConnector
    print("✅ DTCConnector imported successfully")
except ImportError as e:
    print(f"❌ Failed to import DTCConnector: {e}")
    print(f"   Python path: {sys.path}")
    print("   Make sure the databricks/dtc/python folder exists in the workspace")
    raise

# COMMAND ----------

# CELL 3: Pull Data from DTC
print("\n[CELL 3] Pull Data from DTC")
print("-" * 80)

try:
    # Initialize connector
    connector = DTCConnector(
        api_key=dtc_api_key,
        environment=DTC_ENVIRONMENT,
        workspace_name=DTC_WORKSPACE_NAME,
    )
    print(f"✅ DTCConnector initialized")
    print(f"   Workspace: {DTC_WORKSPACE_NAME}")
    print(f"   Environment: {DTC_ENVIRONMENT}")

    # Get request details
    request = connector.get_request(DTC_REQUEST_ID)
    request_ref = request.get("requestReference", "UNKNOWN")
    sheet_id = request.get("sheetId")
    print(f"✅ Request loaded: {request_ref} (sheet: {sheet_id})")

    # Get available views
    views = connector.get_views(DTC_REQUEST_ID)
    print(f"✅ Found {len(views)} views")
    
    # Use the first view (or "Full Version" if available)
    view_id = None
    for v in views:
        if v.get("viewName") == "Full Version":
            view_id = v.get("viewId")
            break
    if not view_id and views:
        view_id = views[0].get("viewId")
    
    print(f"✅ Using view: {view_id}")

    # Pull data to DataFrame and Document metadata
    print(f"Pulling sheet data...")
    df, document_metadata = connector.pull_request_to_dataframe(DTC_REQUEST_ID, view_id)
    print(f"✅ Pulled {len(df)} rows, {len(df.columns)} columns")
    
    # Display document metadata
    print(f"\nDocument Metadata:")
    for key, value in document_metadata.items():
        print(f"  {key}: {value}")
    
    # Display sample
    print(f"\nDataFrame shape: {df.shape}")
    print(f"\nColumn names (normalized for Delta Lake):")
    print(f"  Note: HTML tags and spaces removed from DTC field names")
    print(f"  Example: 'Product Status' → 'Product_Status'")
    print(f"  Example: 'Proto Sample<BR/>Date' → 'Proto_SampleDate'")
    print(f"\nSample columns (first 10):")
    for i, col in enumerate(list(df.columns)[:10], 1):
        print(f"  {i}. {col}")
    if len(df.columns) > 10:
        print(f"  ... and {len(df.columns) - 10} more")
    
    connector.close()

except Exception as e:
    print(f"❌ Failed to pull from DTC: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

# CELL 4: Convert to Spark DataFrame
print("\n[CELL 4] Convert to Spark DataFrame")
print("-" * 80)

try:
    # Convert Pandas to Spark
    spark_df = spark.createDataFrame(df)
    print(f"✅ Created Spark DataFrame: {spark_df.count()} rows")
    
    # Show schema
    print("\nSchema:")
    spark_df.printSchema()

except Exception as e:
    print(f"❌ Failed to create Spark DataFrame: {e}")
    raise

# COMMAND ----------

# CELL 5: Add Metadata Columns
print("\n[CELL 5] Add Metadata Columns")
print("-" * 80)

from pyspark.sql.functions import lit, current_timestamp

# Add sync metadata
spark_df = spark_df.withColumn("sync_timestamp", current_timestamp()) \
                   .withColumn("sync_date", lit(datetime.now().date()))

print(f"✅ Added metadata columns")

# COMMAND ----------

# CELL 6: Write to Delta Table
print("\n[CELL 6] Write to Delta Table")
print("-" * 80)

target_table_path = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"
print(f"Writing to: {target_table_path}")

try:
    # Write mode options:
    # - overwrite: replace entire table
    # - append: add to existing
    # - merge: upsert based on row_id
    
    if WRITE_MODE == "overwrite":
        print(f"Write mode: OVERWRITE (replace entire table)")
        spark_df.write.format("delta").mode("overwrite").saveAsTable(target_table_path)
    elif WRITE_MODE == "append":
        print(f"Write mode: APPEND (add rows)")
        spark_df.write.format("delta").mode("append").saveAsTable(target_table_path)
    else:
        print(f"Write mode: {WRITE_MODE}")
        spark_df.write.format("delta").mode(WRITE_MODE).saveAsTable(target_table_path)
    
    print(f"✅ Data written to {target_table_path}")
    
    # Store Document metadata as table properties
    print(f"\nStoring Document metadata as table properties...")
    try:
        # Build ALTER TABLE statement for properties
        properties_statements = []
        for key, value in document_metadata.items():
            # Escape values for SQL
            sql_value = str(value).replace("'", "''") if value else ""
            properties_statements.append(f"'{key}'='{sql_value}'")
        
        if properties_statements:
            props_sql = ", ".join(properties_statements)
            alter_sql = f"ALTER TABLE {target_table_path} SET TBLPROPERTIES ({props_sql})"
            spark.sql(alter_sql)
            print(f"✅ Document metadata stored as table properties")
            print(f"   Document: {document_metadata.get('document_name')}")
            print(f"   Request: {document_metadata.get('request_reference')}")
            print(f"   Owner: {document_metadata.get('owner_name')}")
    except Exception as prop_error:
        print(f"⚠️  Warning: Could not set table properties: {prop_error}")
        # Don't fail the entire sync for this

except Exception as e:
    print(f"❌ Failed to write to Delta table: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

# CELL 7: Verify Write
print("\n[CELL 7] Verify Write")
print("-" * 80)

try:
    # Read back and verify
    verify_df = spark.read.table(target_table_path)
    row_count = verify_df.count()
    col_count = len(verify_df.columns)
    
    print(f"✅ Table verified:")
    print(f"   Rows: {row_count}")
    print(f"   Columns: {col_count}")
    print(f"   Last updated: {datetime.now(timezone.utc).isoformat()}")
    
    # Display sample
    print(f"\nSample data (first 3 rows):")
    verify_df.select("request_reference", "row_index", "lf_style", "sync_timestamp").limit(3).display()

except Exception as e:
    print(f"⚠️  Could not verify table: {e}")

# COMMAND ----------

print("\n" + "=" * 80)
print("✅ SYNC COMPLETE")
print("=" * 80)
print(f"End time: {datetime.now(timezone.utc).isoformat()}")
