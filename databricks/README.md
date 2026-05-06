# Databricks ETL Pipeline for BeProduct

Complete bidirectional ETL pipeline to sync BeProduct PLM data with Azure Databricks Delta tables.

## Quick Start

```bash
# 1. Create secret scope
databricks secrets create-scope --scope beproduct

# 2. Add credentials
databricks secrets put --scope beproduct --key client_id --string-value "..."
databricks secrets put --scope beproduct --key client_secret --string-value "..."
databricks secrets put --scope beproduct --key refresh_token --string-value "..."
databricks secrets put --scope beproduct --key company_domain --string-value "..."

# 3. Upload notebooks
databricks workspace import-dir notebooks/inbound /Workspace/beproduct/notebooks/inbound
databricks workspace import-dir notebooks/outbound /Workspace/beproduct/notebooks/outbound

# 4. See DATABRICKS_SETUP.md for complete deployment guide
```

## Directory Structure

```
databricks/
├── README.md                          # This file
├── DATABRICKS_SETUP.md                # Complete deployment guide
├── notebooks/
│   ├── inbound/                       # Download BeProduct → Databricks (8 jobs)
│   │   ├── beproduct_api_utils.py     # Shared: OAuth, API client, normalization
│   │   ├── 01_sync_styles.py          # Sync Styles
│   │   ├── 02_sync_materials.py       # Sync Materials
│   │   ├── 03_sync_colors.py          # Sync Color Palettes
│   │   ├── 04_sync_images.py          # Sync Images
│   │   ├── 05_sync_blocks.py          # Sync Blocks
│   │   ├── 06_sync_directory.py       # Sync Directory (vendors, factories, etc.)
│   │   ├── 07_sync_users.py           # Sync Users
│   │   └── 08_sync_data_tables.py     # Sync Custom DataTables
│   │
│   └── outbound/                      # Push Databricks → BeProduct (7 jobs, on-demand)
│       ├── beproduct_push_utils.py    # Shared: merge logic, conflict detection, audit logging
│       ├── 01_push_styles.py          # Push modified styles
│       ├── 02_push_materials.py       # Push modified materials
│       ├── 03_push_colors.py          # Push modified colors
│       ├── 04_push_images.py          # Push modified images
│       ├── 05_push_blocks.py          # Push modified blocks
│       ├── 06_push_directory.py       # Push modified directory (upsert)
│       └── 07_push_data_table_rows.py # Push modified data table rows
│
├── jobs/
│   ├── inbound/
│   │   └── sync_jobs.yaml             # Job definitions for all 8 inbound jobs
│   │
│   └── outbound/
│       └── push_jobs.yaml             # Job definitions for all 7 outbound jobs (on-demand)
│
└── config/
    └── [future: parameters, cluster configs]
```

## Notebooks

### Inbound Sync Notebooks

All inbound notebooks fetch data from BeProduct API and write to Databricks Delta tables. They support:
- ✅ Incremental sync (only fetch records modified since last run)
- ✅ Pagination (handles APIs with 1000+ records)
- ✅ Batch writes (configurable batch size, default 1000)
- ✅ Error recovery (automatic retries with exponential backoff)
- ✅ Rate limit handling (respects BeProduct API rate limits)

| Notebook | Source | Target | Incremental | Notes |
|----------|--------|--------|-------------|-------|
| 01_sync_styles.py | Style/List | bp_styles | Yes | Includes colorway + size range counts |
| 02_sync_materials.py | Material/List | bp_materials | Yes | Similar to Styles |
| 03_sync_colors.py | Color/List | bp_colors | Yes | Handles colorPaletteNumber quirk |
| 04_sync_images.py | Image/List | bp_images | Yes | Store metadata + thumbnail URL |
| 05_sync_blocks.py | Block/List | bp_blocks | Yes | Track size class count |
| 06_sync_directory.py | Directory/List | bp_directory | No* | Full refresh each run |
| 07_sync_users.py | User/List | bp_users | No* | Full refresh each run |
| 08_sync_data_tables.py | DataTable/List | bp_data_table_* | No | Creates one table per data table definition |

*No incremental support for Directory and Users (not supported by API)

#### Inbound Execution Order (Every 6 Hours)

```
00:00 → Styles, Materials (parallel)
00:30 → Colors, Images (parallel)
01:00 → Blocks
01:30 → Directory
02:00 → Users
02:30 → DataTables
```

Staggered by 30 min to avoid API thundering herd.

### Outbound Push Notebooks

All outbound notebooks (on-demand, no schedule) push modified records from Databricks back to BeProduct. They:
- ✅ Fetch latest from BeProduct API (before pushing)
- ✅ Detect conflicts (compare modified_at timestamps)
- ✅ Merge safely (local_wins strategy configured)
- ✅ Log all operations (bp_audit_log table)
- ✅ Dry-run mode (default, no actual API calls)

| Notebook | Target | Conflict Strategy | Merge Logic | Notes |
|----------|--------|-------------------|-------------|-------|
| 01_push_styles.py | Style/Update | local_wins | Databricks version overwrites | Dry-run by default |
| 02_push_materials.py | Material/Update | local_wins | Databricks version overwrites | Dry-run by default |
| 03_push_colors.py | Color/Update | local_wins | Databricks version overwrites | Dry-run by default |
| 04_push_images.py | Image/Update | local_wins | Databricks version overwrites | Dry-run by default |
| 05_push_blocks.py | Block/Update | local_wins | Databricks version overwrites | Dry-run by default |
| 06_push_directory.py | Directory/Add | local_wins | Upsert (no delete via API) | Dry-run by default |
| 07_push_data_table_rows.py | DataTable/Update | local_wins | Per-row insert/update | Dry-run by default |

## Shared Utilities

### beproduct_api_utils.py (Inbound)

**OAuth Token Management:**
```python
oauth = BeProductOAuth(client_id, client_secret, refresh_token)
token = oauth.get_access_token()  # Auto-refresh if expired
```

**API Client with Pagination:**
```python
client = BeProductClient(client_id, client_secret, refresh_token, company_domain)

# Fetch all styles with automatic pagination
for style in client.fetch_styles(incremental_filter="2026-05-01T00:00:00Z"):
    # Process style
    pass

# Fetch single record by ID
style = client.fetch_style_by_id("style-uuid")
```

**Data Normalization:**
```python
# Convert API response to flat Delta row
normalized = normalize_style_row(api_response, sync_time, batch_id)
# Returns dict with: id, folder_id, folder_name, header_number, header_name, 
#                     active, created_at, modified_at, synced_at, data_json, ...
```

**Features:**
- 🔐 Auto-refresh access tokens with 8-hour cache
- 📄 Pagination for 1000+ record endpoints
- ⏱️ Exponential backoff on rate limits (429 responses)
- 🔄 Automatic retry on transient failures
- 🔤 Standardized field extraction from headerData.fields[]
- 🎨 Handles colorPaletteName quirk for colors

### beproduct_push_utils.py (Outbound)

**Conflict Detection:**
```python
detector = ConflictDetector()
is_conflict, reason = detector.detect_conflict(
    local_record,
    remote_record,
    strategy="local_wins"
)
# Returns: (bool, str) - conflict detected? and why?
```

**Record Merging:**
```python
merger = RecordMerger()
merged = merger.merge(local, remote, strategy="local_wins")
# Returns merged record ready for API push
```

**Push Operations:**
```python
pusher = BeProductPusher(oauth)
success, message = pusher.push_style(style_record)
```

**Audit Logging:**
```python
entry = create_audit_log_entry(
    record_id="uuid",
    master_type="styles",
    action="UPDATE",
    databricks_modified_at="2026-05-05T...",
    beproduct_modified_at="2026-05-04T...",
)
# Insert into bp_audit_log table
```

**Features:**
- ⏰ Timestamp-based conflict detection
- 🔀 Configurable merge strategies (local_wins, remote_wins, manual_review)
- 📊 Comprehensive audit logging for compliance
- 🛡️ Safe defaults (dry_run=true, require explicit enable)

## Delta Table Schemas

### Common Master Tables (Styles, Materials, Colors, Images, Blocks)

```sql
CREATE TABLE main.beproduct.bp_[master] (
    id STRING PRIMARY KEY,
    folder_id STRING,
    folder_name STRING,
    header_number STRING,
    header_name STRING,
    active BIGINT (0 or 1),
    created_at STRING (ISO datetime),
    modified_at STRING (ISO datetime),
    synced_at STRING (this sync run),
    last_beproduct_id STRING (for merge tracking),
    data_json STRING (complete API response),
    _databricks_modified_at TIMESTAMP (system column),
    _databricks_modified_by STRING (user/job that modified),
    _sync_batch_id STRING (correlation ID for sync run),
    
    -- Master-specific columns (for analytics)
    [colorway_count | size_range_count | color_chip_count | size_class_count] BIGINT
) USING DELTA;
```

### Specialized Tables

**bp_directory:**
```sql
id, directory_id, name, partner_type, country, active, address, city, state, zip_code,
phone, website, modified_at, synced_at, contact_count, data_json, ...
```

**bp_users:**
```sql
id, email, username, first_name, last_name, title, account_type, role,
registered_on, active, synced_at, data_json, ...
```

**bp_data_tables:**
```sql
-- List of all data table definitions
id, name, description, active, created_at, modified_at, synced_at, data_json, ...
```

**bp_data_table_[name]:**
```sql
-- Rows from specific data table
id, data_table_id, data_table_name, created_at, modified_at, synced_at,
field_[field_id], field_[field_id], ..., field_count, data_json, ...
```

**bp_audit_log:**
```sql
-- All push operations for compliance
audit_id, timestamp, job_id, run_id, master_type, record_id, action,
databricks_modified_at, beproduct_modified_at, error_message, databricks_user, ...
```

## Job Configuration

### Inbound Jobs (Every 6 Hours)

See `jobs/inbound/sync_jobs.yaml` for complete YAML definition.

**Schedule:**
- 00:00, 06:00, 12:00, 18:00 UTC
- 8 jobs total, staggered by 30 min offset

**Cluster:**
- Runtime: Spark 14.3 LTS
- Node type: i3.xlarge (default) → i3.2xlarge for large catalogs
- Workers: 1-4 (depends on master size)
- Timeout: 3600 seconds (1 hour)
- Max retries: 1

### Outbound Jobs (On-Demand Only)

See `jobs/outbound/push_jobs.yaml` for complete YAML definition.

**Trigger:**
- Manual via Databricks UI or API
- No scheduled runs (safe by default)
- Dry-run mode by default (dry_run=true)

**Cluster:**
- Runtime: Spark 14.3 LTS
- Node type: i3.xlarge
- Workers: 1
- Timeout: 3600 seconds
- Max retries: 0 (no retry on push to avoid double writes)

## Parameters

### Inbound Notebooks

```
incremental_mode (bool)     : if true, only fetch modified since last sync (default: true)
target_catalog (str)        : Unity Catalog name (default: main)
target_schema (str)         : Schema name (default: beproduct)
batch_size (int)            : rows per batch write (default: 1000)
```

### Outbound Notebooks

```
dry_run (bool)              : if true, log changes but don't push (default: true)
target_catalog (str)        : Unity Catalog name (default: main)
target_schema (str)         : Schema name (default: beproduct)
```

## Deployment

See **DATABRICKS_SETUP.md** for complete step-by-step deployment guide.

Quick reference:
1. Create secret scope: `databricks secrets create-scope --scope beproduct`
2. Add credentials to secrets
3. Upload notebooks to workspace
4. Create Unity Catalog & schema
5. Deploy jobs (UI, CLI, or Terraform)
6. Test inbound sync
7. Test outbound push (dry-run)
8. Enable push-back when ready

## Monitoring & Troubleshooting

### Monitor Sync Health

```sql
-- Check last sync times
SELECT 
    'styles' as master, MAX(synced_at) as last_sync, COUNT(*) as count
FROM main.beproduct.bp_styles
UNION ALL SELECT 'materials', MAX(synced_at), COUNT(*) FROM main.beproduct.bp_materials
-- ... etc
```

### Monitor Push Operations

```sql
-- Audit log of all push operations
SELECT * FROM main.beproduct.bp_audit_log
WHERE timestamp > CURRENT_TIMESTAMP() - INTERVAL 24 HOUR
ORDER BY timestamp DESC;
```

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "Secret scope not found" | Secrets not created | `databricks secrets create-scope --scope beproduct` |
| "Rate limited (429)" | Too many API calls | Increase timeout, reduce batch_size |
| "Table not found" | First run, table doesn't exist yet | Run inbound notebook once, table will be created |
| Incremental sync not working | synced_at is NULL | Run full sync (incremental_mode=false) first |

## Performance Tuning

For catalogs with **>10,000 records**:

1. **Increase batch size**: `batch_size=5000` (default 1000)
2. **Use larger cluster**: `i3.2xlarge` (default i3.xlarge)
3. **Scale workers**: `num_workers=4` (default 1-2)
4. **Increase timeout**: `timeout_seconds=7200` (default 3600)

## Security

✅ **Best Practices:**
- Credentials stored in Databricks Secrets (not code)
- Unity Catalog for access control
- All push operations logged to bp_audit_log
- Dry-run mode by default (safe)
- OAuth token auto-refresh (never hardcoded)

❌ **Never:**
- Hardcode credentials
- Share personal access tokens
- Run on unsecured clusters
- Set dry_run=false without testing

## FAQ

**Q: Can I customize sync frequency?**  
A: Yes! Edit the cron expression in `jobs/inbound/sync_jobs.yaml` and redeploy.

**Q: What happens if a sync fails?**  
A: Job will retry once (max_retries=1). Check Databricks job logs for details. Push jobs never retry (max_retries=0) to prevent double writes.

**Q: Can I sync only certain masters?**  
A: Yes! Deploy only the notebooks/jobs you need. Each master is independent.

**Q: What if there's a conflict during push?**  
A: Default strategy is `local_wins` (Databricks overwrites BeProduct). Change to `manual_review` in job params to require approval first.

**Q: How do I rollback a push?**  
A: Check bp_audit_log to see what was pushed. Manually revert in BeProduct UI or re-push from a backup of the Delta table.

---

**Version**: 1.0  
**Status**: Production-ready  
**Last Updated**: 2026-05-05
