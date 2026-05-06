# Databricks ETL Pipeline Setup Guide

This guide walks you through deploying the bidirectional BeProduct ↔ Databricks ETL pipeline.

## Architecture Overview

```
┌─────────────────┐                              ┌──────────────────┐
│   BeProduct API │◄─────────────────────────────│  Databricks      │
│                 │                              │  Delta Tables    │
│  - Styles       │  PUSH (on-demand, local_wins)│                  │
│  - Materials    │                              │  - bp_styles     │
│  - Colors       │  SYNC (every 6 hours)        │  - bp_materials  │
│  - Images       │─────────────────────────────►│  - bp_colors     │
│  - Blocks       │                              │  - bp_images     │
│  - Directory    │                              │  - bp_blocks     │
│  - Users        │                              │  - bp_directory  │
│  - DataTables   │                              │  - bp_users      │
└─────────────────┘                              │  - bp_data_*     │
                                                 │  - bp_audit_log  │
                                                 └──────────────────┘
```

## Prerequisites

### 1. Databricks Workspace
- Azure Databricks workspace with Unity Catalog enabled
- SQL Warehouse or all-purpose cluster with Spark 14.3+
- Permission to create jobs, tables, and secrets

### 2. BeProduct API Credentials
You'll need:
- `BEPRODUCT_CLIENT_ID`
- `BEPRODUCT_CLIENT_SECRET`
- `BEPRODUCT_REFRESH_TOKEN` (from `get_refresh_token.py`)
- `BEPRODUCT_COMPANY_DOMAIN` (e.g., "lifung")

Request credentials from [support@beproduct.com](mailto:support@beproduct.com) if needed.

### 3. Databricks CLI (for deployment)
```bash
pip install databricks-cli
databricks configure --token  # or --username/password
```

## Step 1: Create Secrets in Databricks

Secrets are stored securely and used by all notebooks.

### Create Secret Scope

```bash
databricks secrets create-scope --scope beproduct
```

### Add Secrets

```bash
databricks secrets put --scope beproduct --key client_id --string-value "your_client_id"
databricks secrets put --scope beproduct --key client_secret --string-value "your_client_secret"
databricks secrets put --scope beproduct --key refresh_token --string-value "your_refresh_token"
databricks secrets put --scope beproduct --key company_domain --string-value "your_domain"
```

### Verify Secrets

```bash
databricks secrets list --scope beproduct
# Should show: client_id, client_secret, refresh_token, company_domain
```

## Step 2: Upload Notebooks to Databricks Workspace

### Option A: Using Databricks CLI (Recommended)

```bash
cd databricks/

# Upload shared utilities
databricks workspace import-dir notebooks/inbound /Workspace/beproduct/notebooks/inbound

databricks workspace import-dir notebooks/outbound /Workspace/beproduct/notebooks/outbound
```

### Option B: Using Databricks UI

1. Open Databricks workspace
2. Create folder: `/Workspace/beproduct/notebooks/inbound`
3. Create folder: `/Workspace/beproduct/notebooks/outbound`
4. Upload each `.py` notebook file

## Step 3: Create Unity Catalog & Schema

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS main;

-- Create schema
CREATE SCHEMA IF NOT EXISTS main.beproduct;

-- Grant permissions (adjust for your workspace)
GRANT USE_CATALOG ON CATALOG main TO users;
GRANT USE_SCHEMA ON SCHEMA main.beproduct TO users;
GRANT CREATE_TABLE ON SCHEMA main.beproduct TO users;
```

## Step 4: Create Audit Log Table

```sql
CREATE TABLE IF NOT EXISTS main.beproduct.bp_audit_log (
    audit_id STRING,
    timestamp TIMESTAMP,
    job_id STRING,
    run_id STRING,
    master_type STRING,
    record_id STRING,
    action STRING,
    databricks_modified_at TIMESTAMP,
    beproduct_modified_at TIMESTAMP,
    error_message STRING,
    databricks_user STRING,
    _databricks_modified_at TIMESTAMP,
    _databricks_modified_by STRING
)
USING DELTA;

CREATE INDEX idx_audit_master_type ON main.beproduct.bp_audit_log(master_type);
CREATE INDEX idx_audit_timestamp ON main.beproduct.bp_audit_log(timestamp);
```

## Step 5: Deploy Jobs

### Option A: Using Databricks UI (Recommended for First Time)

1. Open Databricks workspace
2. Go to Workflows → Jobs
3. Create job manually:
   - **Name**: `beproduct-sync-styles`
   - **Cluster**: New job cluster (2 workers, i3.xlarge)
   - **Task**: Notebook
   - **Notebook path**: `/Workspace/beproduct/notebooks/inbound/01_sync_styles`
   - **Timeout**: 3600 seconds
   - **Max retries**: 1
   - **Schedule**: Cron - `0 0 0,6,12,18 * * ?` (every 6 hours)
   - **Parameters**:
     - `incremental_mode=true`
     - `target_catalog=main`
     - `target_schema=beproduct`
     - `batch_size=1000`

4. Repeat for all 8 inbound jobs and 7 outbound jobs

### Option B: Using Terraform (IaC - Recommended for Production)

```bash
cd databricks/jobs/

# Initialize Terraform
terraform init

# Plan deployment
terraform plan

# Apply
terraform apply
```

(See `terraform/main.tf` for configuration)

### Option C: Using Databricks CLI

```bash
# Convert YAML to JSON (requires Python)
python -c "import yaml; import json; print(json.dumps(yaml.safe_load(open('inbound/sync_jobs.yaml'))))" > /tmp/sync_jobs.json

# Create jobs from JSON
databricks jobs configure --json-file /tmp/sync_jobs.json
```

## Step 6: Test Inbound Sync (First Time Only)

### Run a test full sync:

1. Go to Databricks workspace → Workflows → Jobs
2. Select `beproduct-sync-styles`
3. Click **Run Now**
4. Monitor the run in real-time
5. Check the output for:
   - `✅ Fetched N styles from BeProduct API`
   - `✅ Final: X total records in main.beproduct.bp_styles`

### Verify data in Delta table:

```sql
SELECT COUNT(*) as total_records FROM main.beproduct.bp_styles;

SELECT DISTINCT synced_at FROM main.beproduct.bp_styles LIMIT 1;

SELECT * FROM main.beproduct.bp_styles LIMIT 5;
```

## Step 7: Test Outbound Push (Dry Run)

### Run a test push with dry_run=true:

1. Go to Databricks workspace → Workflows → Jobs
2. Select `beproduct-push-styles`
3. Click **Run Now**
4. The notebook will:
   - Read modified styles from Delta
   - Detect conflicts with BeProduct API
   - Log what WOULD be pushed (without actually pushing)
   - Show audit log entries that WOULD be created

### Expected output:
```
⏱  Starting Styles push
   Mode: DRY RUN
📋 Found X total style records
📌 Example: Would push record [UUID]
   Local modified_at: 2026-05-05T...
   Remote modified_at: 2026-05-04T...
   Conflict: False - Local is newer, safe to push
   [DRY RUN - no actual push]
✅ Push operation completed
```

## Step 8: Monitor & Maintain

### Check Sync Status

```sql
-- Check latest synced_at for each master
SELECT 
    'styles' as master,
    MAX(synced_at) as last_sync,
    COUNT(*) as record_count
FROM main.beproduct.bp_styles
GROUP BY 'styles'

UNION ALL

SELECT 'materials', MAX(synced_at), COUNT(*) FROM main.beproduct.bp_materials
UNION ALL
SELECT 'colors', MAX(synced_at), COUNT(*) FROM main.beproduct.bp_colors
-- ... etc for other masters
```

### Check Push Audit Log

```sql
SELECT 
    master_type,
    action,
    COUNT(*) as count,
    MAX(timestamp) as latest
FROM main.beproduct.bp_audit_log
GROUP BY master_type, action
ORDER BY latest DESC;
```

### Monitor Job Runs

```bash
# List recent job runs
databricks jobs list-runs --job-id <job_id> --limit 10

# Get specific run details
databricks jobs get-run --run-id <run_id>

# Check run output/logs
databricks runs get-output --run-id <run_id>
```

## Step 9: Configure Alerts & Notifications (Optional)

### Set up job failure alerts:

1. Go to Databricks workspace → Workflows → Jobs
2. Select a job
3. Click **Edit**
4. Scroll to **Notifications**
5. Add email or Slack notification on **All Completions** or **On Failure**

## Step 10: Enable Push-Back (When Ready)

Once you're confident in the sync process:

1. Update outbound job parameters:
   - Change `dry_run` from `true` to `false`
2. Review the conflict strategy:
   - Current: `local_wins` (Databricks overwrites BeProduct)
   - Alternative: `manual_review` (requires approval before push)
3. Run a test push with one record
4. Verify the change in BeProduct UI
5. Enable full push runs

## Troubleshooting

### Issue: "Secret scope not found"

```bash
# Check if scope exists
databricks secrets list-scopes

# Create if missing
databricks secrets create-scope --scope beproduct
```

### Issue: "Table not found" when pushing

```sql
-- Check if table exists
SELECT * FROM main.beproduct.bp_audit_log LIMIT 1;

-- If missing, create it
CREATE TABLE main.beproduct.bp_audit_log (
    audit_id STRING,
    timestamp TIMESTAMP,
    job_id STRING,
    run_id STRING,
    master_type STRING,
    record_id STRING,
    action STRING,
    databricks_modified_at TIMESTAMP,
    beproduct_modified_at TIMESTAMP,
    error_message STRING,
    databricks_user STRING,
    _databricks_modified_at TIMESTAMP,
    _databricks_modified_by STRING
) USING DELTA;
```

### Issue: "Rate limit exceeded" (429)

The API client implements exponential backoff:
- 1st retry: wait 1 second
- 2nd retry: wait 2 seconds
- 3rd retry: wait 4 seconds

If this persists:
1. Increase job timeout
2. Reduce batch_size parameter (from 1000 to 500)
3. Contact BeProduct support for rate limit increase

### Issue: Incremental sync not working

```sql
-- Check if synced_at timestamp is being set
SELECT MAX(synced_at) as last_sync FROM main.beproduct.bp_styles;

-- If NULL, run a full sync (set incremental_mode=false)
-- Then verify synced_at is populated
```

## Performance Tuning

### For large catalogs (>10,000 records):

1. **Increase batch size**:
   ```
   batch_size: 5000  (default 1000)
   ```

2. **Use larger cluster**:
   ```
   node_type_id: i3.2xlarge  (default i3.xlarge)
   num_workers: 4             (default 1-2)
   ```

3. **Stagger sync runs** (already done, but can adjust):
   ```
   - Styles: 00:00
   - Materials: 00:30
   - Colors: 01:00
   - Images: 01:30
   - etc.
   ```

4. **Optimize incremental filter** (for advanced use):
   - Modify `beproduct_api_utils.py` to use `FolderModifiedAt` filter more aggressively

## Security Best Practices

✅ **DO:**
- Store credentials in Databricks Secrets (not in code)
- Use Unity Catalog for fine-grained access control
- Audit all push operations via `bp_audit_log` table
- Enable job notifications for failures
- Rotate refresh_token every 6 months

❌ **DON'T:**
- Hardcode credentials in notebooks
- Share personal access tokens
- Run push jobs on unsecured clusters
- Set `dry_run=false` without testing first

## Next Steps

1. ✅ Complete setup steps 1-8
2. ✅ Test inbound sync with all 8 masters
3. ✅ Test outbound push with dry_run=true
4. ⚠️  Enable push-back only after confirming data quality
5. 📊 Set up monitoring dashboard (optional)
6. 📝 Document your custom workflow (if any)

## Support & Resources

- **BeProduct API Docs**: https://developers.beproduct.com/swagger/
- **Databricks Docs**: https://docs.databricks.com/
- **GitHub Issues**: Report bugs at [this repo]

---

**Last updated**: 2026-05-05  
**Status**: Ready for deployment
