# DTC Master Chart Sync Hub

**Status**: ✅ MVP Ready (Pull-only)  
**Target Table**: `lft.beproduct.dtc_master_chart_uat`  
**Source**: DTC API (Kontoor workspace)  
**Schedule**: Daily (configurable)

---

## Overview

This module provides a two-way sync solution for DTC (Data Collaboration Application) requests to Databricks Delta Lake.

**Current Scope**:
- ✅ Pull: DTC → Databricks (read-only)
- ⏳ Push: Databricks → DTC (planned)
- ⏳ Change tracking: Row-level delta detection (planned)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DTC MASTER CHART SYNC                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  DTC API (UAT)                                                  │
│  ├─ Request: KON FW26 Wrangler                                 │
│  │  └─ 14 views (Full Version, Vendor 1-3, etc.)             │
│  │     └─ 247 rows × 114 columns                              │
│  │                                                              │
│  ↓ (DTCConnector)                                              │
│                                                                 │
│  Databricks Workspace                                           │
│  └─ Table: lft.beproduct.dtc_master_chart_uat                 │
│     ├─ 247 rows                                                │
│     ├─ 114 columns + metadata (sync_timestamp, sync_date)     │
│     └─ Updated daily (configurable)                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### 1. Local Testing (Already Done ✅)

```bash
python3 test_dtc_connector.py
```

Output:
```
✅ DTCConnector created
✅ Request loaded: KON FW26 Wrangler
✅ Got 14 views
✅ DataFrame created: 247 rows, 114 columns
✅ ALL TESTS PASSED
```

### 2. Deploy to Databricks

**Status**: ✅ Deployed to `/Workspace/Repos/beproduct-sync/DTC/`

#### Step 1: Verify secrets are configured

The DTC API key has been added to the existing `beproduct` secrets scope:

```bash
# Verify
databricks secrets list-secrets beproduct | grep dtc
# Output: dtc_api_key_uat
```

#### Step 2: Run the notebook (test)

The code is deployed at `/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta`

```bash
# Test run via Databricks UI:
# 1. Navigate to /Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta
# 2. Click "Run" and select a cluster

# Or via CLI:
databricks runs submit \
  --notebook-task notebook_path=/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta \
  --existing-cluster-id <CLUSTER_ID>

# Monitor
databricks runs get-output --run-id <RUN_ID>
```

#### Step 3: Create a scheduled job

```bash
# Create job configuration
cat > dtc_job_config.json << 'EOF'
{
  "name": "dtc_master_chart_daily_sync",
  "new_cluster": {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2,
    "aws_attributes": {
      "availability": "SPOT"
    }
  },
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta",
    "base_parameters": {
      "dtc_request_id": "69f076f0b7247a661226be9a",
      "dtc_environment": "uat",
      "target_catalog": "lft",
      "target_schema": "beproduct",
      "target_table": "dtc_master_chart_uat",
      "write_mode": "overwrite"
    }
  },
  "schedule": {
    "quartz_cron_expression": "0 2 * * *",
    "timezone_id": "UTC"
  },
  "timeout_seconds": 3600,
  "max_concurrent_runs": 1
}
EOF

# Create the job
databricks jobs create --json-file dtc_job_config.json

# View jobs
databricks jobs list | grep dtc
```

#### Step 4: Monitor the job

```bash
# List recent runs
databricks jobs list-runs --job-id <JOB_ID> --limit 10

# Get run details
databricks runs get --run-id <RUN_ID>

# View logs
databricks runs get-output --run-id <RUN_ID>
```

---

## File Structure

```
databricks/dtc/
├── python/
│   ├── client/
│   │   ├── __init__.py
│   │   └── rest_client.py           ← Generic HTTP client with auth + retry
│   │
│   ├── connectors/
│   │   ├── __init__.py
│   │   └── dtc.py                   ← DTC-specific pull logic
│
├── notebooks/
│   └── pull_dtc_to_delta.py         ← Main Databricks notebook
│
├── tests/
│   └── test_dtc_connector.py         ← Local test script (already validated ✅)
│
└── README.md (this file)
```

---

## Notebook Parameters

The `pull_dtc_to_delta.py` notebook accepts these parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dtc_request_id` | `69f076f0b7247a661226be9a` | DTC request to sync |
| `dtc_environment` | `uat` | DTC environment (`uat` or `prod`) |
| `target_catalog` | `lft` | Databricks catalog |
| `target_schema` | `beproduct` | Databricks schema |
| `target_table` | `dtc_master_chart_uat` | Target Delta table |
| `write_mode` | `overwrite` | Write mode: `overwrite`, `append`, or `merge` |

**Example**:
```python
# In Databricks notebook cell
dbutils.widgets.text("dtc_request_id", "69f076f0b7247a661226be9a")
dbutils.widgets.text("write_mode", "overwrite")
```

---

## Data Specification

### Input (DTC Request)
- **Request ID**: `69f076f0b7247a661226be9a`
- **Request Reference**: `KON FW26 Wrangler`
- **Views Available**: 14 (Full Version, Vendor 1-3, Factory Allocation, etc.)
- **Rows in Full Version View**: 247
- **Columns**: 114

### Output (Delta Table)
- **Location**: `lft.beproduct.dtc_master_chart_uat`
- **Rows**: 247 (from DTC)
- **Columns**: 114 (DTC fields) + 4 (metadata)
  - `sync_timestamp`: When the sync ran
  - `sync_date`: Date of sync
  - `request_id`, `request_reference`, etc.

### Data Types
- Strings: Product descriptions, styles, names, statuses
- Numbers: Prices (FOB), quantities, lead times, months
- Dates: All stored as ISO 8601 UTC strings from DTC
- Nulls: Many sparse fields (75-90% null for some columns)

---

## Example Queries

Once the data is synced to Databricks:

```sql
-- Count rows by product status
SELECT product_status, COUNT(*) as count
FROM lft.beproduct.dtc_master_chart_uat
GROUP BY product_status
ORDER BY count DESC;

-- Find rows with prices > $5
SELECT lf_style, style_description, 
       CAST(`fob_price_(usd/yd/)_in_cw` AS FLOAT) as fob_price
FROM lft.beproduct.dtc_master_chart_uat
WHERE CAST(`fob_price_(usd/yd/)_in_cw` AS FLOAT) > 5
ORDER BY fob_price DESC;

-- Recently synced data
SELECT COUNT(*) as total_rows, 
       MAX(sync_timestamp) as last_sync,
       MIN(sync_timestamp) as first_sync
FROM lft.beproduct.dtc_master_chart_uat;
```

---

## Troubleshooting

### Issue: `ModuleNotFoundError: No module named 'connectors.dtc'`

**Cause**: Python path not set correctly

**Solution**: Verify the deployment location:
```bash
# Check that files exist at the correct location
databricks workspace list /Workspace/Repos/beproduct-sync/DTC/python/connectors/

# Output should show rest_client.py and dtc.py
```

### Issue: `401 Unauthorized` from DTC API

**Cause**: API key invalid or expired

**Solution**:
```bash
# Verify key is set
databricks secrets list-secrets beproduct | grep dtc

# Update if needed
databricks secrets put-secret beproduct dtc_api_key_uat \
  --string-value "NEW_KEY_HERE"
```

### Issue: `Table not found: lft.beproduct.dtc_master_chart_uat`

**Cause**: Table doesn't exist yet

**Solution**: Run notebook with `write_mode=overwrite` to create:
```bash
databricks runs submit \
  --notebook-task notebook_path=/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta \
  --existing-cluster-id <CLUSTER_ID>
```

### Issue: Timeout after 3600 seconds

**Cause**: 247 rows + 114 columns might take too long

**Solution**: Increase timeout or use a larger cluster:
```json
{
  "timeout_seconds": 7200,
  "new_cluster": {
    "num_workers": 4
  }
}
```

---

## Next Steps (Roadmap)

### Phase 1: Pull (✅ Complete)
- [x] RestClient with auth + retry
- [x] DTCConnector for pulling requests
- [x] Databricks notebook to write to Delta table
- [x] Local testing validated
- [x] Deploy to Databricks workspace (`/Workspace/Repos/beproduct-sync/DTC/`)
- [x] Secrets configured in `beproduct` scope
- [ ] Schedule daily job (next step)

### Phase 2: Push (Coming Soon)
- [ ] Implement `DTCConnector.push()` method
- [ ] Create push notebook
- [ ] Add change tracking table
- [ ] Test PATCH endpoint

### Phase 3: Change Tracking (Coming Soon)
- [ ] Snapshot diffing algorithm
- [ ] Change log table (INSERT/UPDATE/DELETE)
- [ ] Conflict resolution rules
- [ ] Incremental sync

### Phase 4: Multi-App (Future)
- [ ] BeProduct connector
- [ ] Miro connector
- [ ] XTS connector
- [ ] N-to-N conflict resolution

---

## Support & Questions

**API Reference**: See `data_samples/DTC_API_FINDINGS.md`

**Architecture**: See `.kilo/plans/1779966530296-shiny-comet.md`

**Issues**: Check `Troubleshooting` section above

---

## Files Created

| File | Purpose | Status |
|------|---------|--------|
| `databricks/dtc/python/client/rest_client.py` | Generic HTTP client | ✅ Complete |
| `databricks/dtc/python/connectors/dtc.py` | DTC-specific logic | ✅ Complete |
| `databricks/dtc/notebooks/pull_dtc_to_delta.py` | Main Databricks notebook | ✅ Complete |
| `databricks/dtc/tests/test_dtc_connector.py` | Local test script | ✅ Passing |
| `databricks/dtc/README.md` | This file | ✅ Complete |

---

**Last Updated**: 2026-05-28  
**Status**: ✅ MVP Ready for Databricks Deployment

