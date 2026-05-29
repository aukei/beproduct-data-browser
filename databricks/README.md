# Databricks Sync Pipeline

Multi-source sync platform for syncing master data and transaction records with Delta Lake tables, including support for bidirectional changes with audit trails.

## Sources Supported

### 1. **DTC** (Data Collaboration Tool) - Phase 1: Pull ✅
Bi-directional sync of worksheets/requests from DTC platform.

**Status:** Phase 1 (Pull) Complete & Production-Ready  
**Location:** `/databricks/dtc/`

**Features:**
- Pull DTC requests to Delta tables (daily)
- Automatic column name normalization
- Document metadata stored as table properties
- Environment-aware (UAT/Prod)
- Parameterized request IDs (any worksheet)

**Phase 2 Design Ready:** Change tracking for push (Snapshot + Change Log pattern)

---

### 2. **BeProduct** (Style Master Data) - STYLE & Master Data ✅
Pull and push STYLE records and reference master data.

**Status:** Production-Ready  
**Notebooks:** `beproduct_style_sync.py`, `beproduct_style_push.py`, `beproduct_master_data_sync.py`

**Features:**
- FULL and INCREMENTAL sync modes
- Timestamp-based change detection
- Field ID mapping for API
- Dry-run mode for testing
- Audit trail for all operations

---

## Architecture

```
DTC (Phase 1: Pull ✅)
  ├─ Daily at 2am UTC
  │  └─ Pulls DTC requests → Delta table
  │  └─ Normalizes column names (HTML + spaces → underscores)
  │  └─ Stores Document metadata as table properties
  │  └─ Overwrite mode (snapshot approach)
  │
  └─ Phase 2: Bi-directional (Design Ready ⏳)
     ├─ Snapshot + Change Log pattern
     ├─ Detect INSERT/UPDATE/DELETE
     └─ Push changes back to DTC via PATCH

STYLE SYNC (Pull)
  └─ Daily at 7pm HKT (11am UTC)
     └─ Syncs STYLE records from BeProduct to Delta
     └─ Supports FULL and INCREMENTAL refresh modes
     └─ Extracts 16 key fields; stores full JSON for audit

MASTER DATA SYNC
  └─ Daily (before push operations)
     └─ Pulls dropdown reference values from BeProduct API
     └─ Creates validation tables for field values

STYLE PUSH (Push)
  └─ Manual trigger or hourly
     └─ Detects changes via timestamp comparison
     └─ Pushes modified STYLE records back to BeProduct
     └─ Dry-run mode by default
     └─ Audit trail for all push operations
```

## Notebooks

### 0. DTC Sync: `dtc/notebooks/pull_dtc_to_delta.py`

**Purpose:** Fetch DTC requests and store in Delta Lake (Pull only in Phase 1).

**Schedule:** Daily at 2:00 AM UTC (customizable)

**Parameters:**
- `dtc_request_id` - DTC request to sync (default: `69f076f0b7247a661226be9a` - KON FW26 Wrangler)
- `dtc_environment` - `uat` or `prod` (default: `uat`)
- `target_catalog` - Target Databricks catalog (default: `lft`)
- `target_schema` - Target Databricks schema (default: `beproduct`)
- `target_table` - Delta table name (default: `dtc_master_chart_uat`)
- `write_mode` - `overwrite`, `append`, or `merge` (default: `overwrite`)

**Output:**
- Delta table: `{catalog}.{schema}.{target_table}` (e.g., `lft.beproduct.dtc_master_chart_uat`)
- Table properties: Document metadata (document_name, owner, timestamps)

**Features:**
- ✅ Any DTC request/worksheet (parameterized)
- ✅ Environment-aware API URLs (uat/prod)
- ✅ Column name normalization (removes HTML tags, spaces)
- ✅ Document metadata stored as table properties
- ✅ Automatic snapshot creation (baseline for Phase 2 change tracking)
- ✅ 247 rows × 114 columns in <1 second

**Documentation:**
- Full guide: `dtc/README.md`
- Change tracking design: `dtc/CHANGE_TRACKING_DESIGN.md`
- Quick start: `QUICK_START_DTC.md`

---

### 1. STYLE Pull: `beproduct_style_sync.py`

**Purpose:** Fetch STYLE records from BeProduct and store in Delta Lake.

**Schedule:** Daily at 7pm HKT (11am UTC)

**Parameters:**
- `folder_name` - BeProduct folder name (default: `KTB`, supports: `KTB`, `WMT`, `WALMART`, etc.)
- `refresh_mode` - `FULL` (all records) or `INCREMENTAL` (only modified) (default: `INCREMENTAL`)
- `catalog` - Target Databricks catalog (default: `lft`)
- `schema` - Target Databricks schema (default: `beproduct`)
- `table_name` - Delta table name (default: `ktb_styles`)

**Extracted Fields (16 key fields):**

**Compulsory:**
- LF Style Number
- Description
- Team
- Season
- Year

**Interested:**
- Product Status
- Customer Style Number
- Product Category
- Product Sub Category
- Division
- Brands
- Garment Finish
- Techpack Stage
- Lot Code
- Parent Vendor
- Factory

**Output:**
- Delta table: `{catalog}.{schema}.{table_name}` (e.g., `lft.beproduct.ktb_styles`)
- Audit table: `{catalog}.{schema}.{table_name}_sync_meta` (sync metadata & timestamps)

**Features:**
- ✅ FULL and INCREMENTAL sync modes
- ✅ Timestamp-based incremental detection
- ✅ Field extraction from BeProduct `headerData.fields[]` structure
- ✅ Full JSON stored for audit trail
- ✅ Stream-specific audit metadata tables

### 2. Master Data Sync: `beproduct_master_data_sync.py`

**Purpose:** Fetch valid dropdown values (Master Data) from BeProduct for field validation.

**Schedule:** Daily (e.g., 10am UTC, before STYLE PUSH)

**Parameters:**
- `catalog` - Target Databricks catalog (default: `lft`)
- `schema` - Target Databricks schema (default: `beproduct`)

**Master Data Types Extracted:**
- BRANDS
- TEAMS
- SEASONS
- YEARS
- PRODUCT STATUS
- PRODUCT CATEGORY
- PRODUCT SUB CATEGORY
- DIVISION
- TECHPACK STAGE
- GARMENT FINISH
- PARENT VENDOR
- FACTORY

**Output:**
- Master data tables: `{catalog}.{schema}.beproduct_master_{type}` (e.g., `lft.beproduct.beproduct_master_brands`)
- Each table has columns: `value`, `label`, `data_json`, `synced_at`

**Features:**
- ✅ Authenticates with BeProduct OAuth
- ✅ Fetches via `/api/{company}/MasterData/{fieldId}` endpoints
- ✅ Creates reference tables for validation
- ✅ Uses SDK session for authenticated requests

**Usage Example:**
```sql
-- Validate brands before pushing
SELECT value, label 
FROM lft.beproduct.beproduct_master_brands 
ORDER BY label;
```

### 3. STYLE Push: `beproduct_style_push.py`

**Purpose:** Push modified STYLE records from Delta Lake back to BeProduct.

**Trigger:** Manual (Databricks UI/API) or hourly schedule

**Parameters:**
- `folder_name` - BeProduct folder name (default: `KTB`)
- `source_table_name` - Source Delta table name (default: `ktb_styles`)
- `dry_run` - `true` (log only) or `false` (actually push) (default: `true`)
- `catalog` - Target Databricks catalog (default: `lft`)
- `schema` - Target Databricks schema (default: `beproduct`)

**Output:**
- Audit table: `{catalog}.{schema}.{source_table_name}_push_log` (push operation history)

**Features:**
- ✅ Change detection via `modified_at > synced_at` timestamp comparison
- ✅ Field ID mapping from `data_json` (required by BeProduct API)
- ✅ Dry-run mode by default (safe, no actual updates)
- ✅ Comprehensive logging of all push operations
- ✅ Audit trail for compliance

**Usage Example:**
```sql
-- View last 5 push operations
SELECT * FROM lft.beproduct.ktb_styles_push_log
ORDER BY pushed_at DESC
LIMIT 5;
```

## Setup

### Prerequisites

1. **Databricks workspace** with Spark 14.3+ and Delta Lake
2. **BeProduct credentials** in Databricks Secrets:
   ```
   Scope: beproduct
   Keys:
     - client_id
     - client_secret
     - refresh_token
     - company_domain
   ```
3. **Catalog & Schema** accessible to cluster

### Quick Start

1. **Create secret scope:**
   ```bash
   databricks secrets create-scope --scope beproduct
   ```

2. **Add credentials:**
   ```bash
   databricks secrets put --scope beproduct --key client_id --string-value "YOUR_CLIENT_ID"
   databricks secrets put --scope beproduct --key client_secret --string-value "YOUR_CLIENT_SECRET"
   databricks secrets put --scope beproduct --key refresh_token --string-value "YOUR_REFRESH_TOKEN"
   databricks secrets put --scope beproduct --key company_domain --string-value "YOUR_COMPANY_DOMAIN"
   ```

3. **Upload notebooks to Databricks Repos:**
   - `/Repos/beproduct-sync/STYLE/beproduct_style_sync`
   - `/Repos/beproduct-sync/STYLE/beproduct_style_push`
   - `/Repos/beproduct-sync/MASTERDATA/beproduct_master_data_sync`

4. **Create jobs** in Databricks Workflows:
   - Pull job: Schedule daily at 7pm HKT (11am UTC)
   - Master data job: Schedule daily at 10am UTC
   - Push job: Manual trigger or hourly

5. **Test each notebook** individually before scheduling

## Documentation

- **`QUICK_START.md`** - Step-by-step setup for STYLE sync/push
- **`QUICK_REFERENCE.md`** - Quick reference for all jobs and parameters
- **`PUSH_SETUP.md`** - Detailed push job setup and testing
- **`PUSH_QUICK_START.md`** - Push job quick start
- **`MASTER_DATA_SETUP.md`** - Master data job setup and troubleshooting
- **`MASTER_DATA_QUICK_START.md`** - Master data job quick start

## Audit Trail

All sync and push operations are logged to metadata tables:

**Pull Audit:** `{table_name}_sync_meta`
- Records: last sync timestamp, record count, sync status

**Push Audit:** `{table_name}_push_log`
- Records: what was pushed, when, by which job, success/failure

**View last 5 sync operations:**
```sql
SELECT * FROM lft.beproduct.ktb_styles_sync_meta
ORDER BY synced_at DESC
LIMIT 5;
```

**View last 5 push operations:**
```sql
SELECT * FROM lft.beproduct.ktb_styles_push_log
ORDER BY pushed_at DESC
LIMIT 5;
```

## Multi-Folder Setup

To sync different BeProduct folders (e.g., KTB, WMT, WALMART):

1. Create separate jobs with different `folder_name` and `table_name` parameters:
   ```
   Job 1: folder_name=KTB, table_name=ktb_styles
   Job 2: folder_name=WMT, table_name=wmt_styles
   Job 3: folder_name=WALMART, table_name=walmart_styles
   ```

2. Each folder gets its own:
   - Delta table (`ktb_styles`, `wmt_styles`, etc.)
   - Sync metadata table (`ktb_styles_sync_meta`, `wmt_styles_sync_meta`, etc.)
   - Push log table (`ktb_styles_push_log`, `wmt_styles_push_log`, etc.)

3. Master data is shared across all folders (one set of reference tables)

## Troubleshooting

### Authentication Errors

**Error:** `unauthorized_client` or `401 Unauthorized`

**Solution:**
1. Verify credentials in Databricks secrets:
   ```bash
   databricks secrets get --scope beproduct --key client_id
   ```
2. Verify OAuth token endpoint is accessible
3. Check BeProduct support for any special OAuth configuration needed

### Connection Errors

**Error:** `Name or service not known` or DNS resolution failure

**Solution:**
1. Verify API base URL is correct: `https://developers.beproduct.com`
2. Verify `company_domain` in secrets (should be like `lifung`, `kinto`, etc.)
3. Check Databricks cluster can reach `developers.beproduct.com`

### No Data Synced

**Solution:**
1. Check notebook logs for errors
2. Verify `folder_name` matches a valid BeProduct folder
3. Try FULL refresh mode to fetch all records
4. Check if BeProduct folder actually has records

## Performance

### Typical Sync Times

- **FULL sync of 50 styles:** ~30-60 seconds
- **INCREMENTAL sync (no changes):** ~10-15 seconds
- **INCREMENTAL sync (50 new/modified):** ~30 seconds

### Scaling for Large Datasets

For catalogs with >1000 records:
1. Increase cluster worker count (default: 1)
2. Use larger node type (default: i3.xlarge)
3. Increase job timeout (default: 1800 seconds)

## Next Steps

1. ✅ Create secret scope and add credentials
2. ✅ Upload notebooks to Databricks
3. ✅ Run STYLE SYNC once to create tables
4. ✅ Run MASTER DATA SYNC to create reference tables
5. ✅ Test STYLE PUSH in dry-run mode
6. ✅ Schedule jobs in Databricks Workflows
7. ✅ Monitor audit tables for sync health

---

**Version:** 2.1  
**Status:** Production-ready (DTC Phase 1 + STYLE sync/push & Master Data)  
**Last Updated:** 2026-05-29

## Summary

| Source | Phase | Status | Notes |
|--------|-------|--------|-------|
| **DTC** | 1: Pull | ✅ Complete | Phase 2 design ready (change tracking) |
| **DTC** | 2: Push | ⏳ Design | Snapshot + change log pattern |
| **BeProduct** | STYLE Pull | ✅ Complete | FULL & INCREMENTAL modes |
| **BeProduct** | Master Data | ✅ Complete | Reference tables for validation |
| **BeProduct** | STYLE Push | ✅ Complete | Timestamp-based detection, dry-run mode |
