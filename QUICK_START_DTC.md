# Quick Start: DTC Sync in Databricks

**Deployment Status**: ✅ Complete  
**Location**: `/Workspace/Repos/beproduct-sync/DTC/`

---

## 1. Run Initial Test (2 minutes)

Open Databricks and navigate to:
```
/Workspace/Repos/beproduct-sync/DTC/notebooks/pull_dtc_to_delta
```

Click **Run** and select a cluster.

**Expected output**:
```
✅ DTC API key loaded from secrets
✅ DTCConnector imported successfully
✅ Request loaded: KON FW26 Wrangler
✅ Got 14 views
✅ DataFrame created: 247 rows, 114 columns
✅ Delta table created: lft.beproduct.dtc_master_chart_uat
```

**If you see errors**, check:
- Cluster is running
- Secret exists: `databricks secrets list-secrets beproduct`
- Python path: `/Workspace/Repos/beproduct-sync/DTC/python/`

---

## 2. Create Daily Job (3 minutes)

Once the test succeeds, run this to schedule daily syncs:

```bash
databricks jobs create --json-file - << 'EOF'
{
  "name": "dtc_master_chart_daily_sync",
  "new_cluster": {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2,
    "aws_attributes": {"availability": "SPOT"}
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
```

**Output**: `Job created with ID: XXXXXX`

---

## 3. Monitor Job Runs

```bash
# List all DTC jobs
databricks jobs list | grep dtc

# View recent runs
databricks jobs list-runs --job-id <JOB_ID> --limit 5

# Check latest run status
databricks runs get --run-id <RUN_ID>
```

---

## 4. Query the Synced Data

Once synced, query the table in Databricks SQL:

```sql
SELECT 
  COUNT(*) as total_rows,
  MAX(sync_timestamp) as last_sync,
  COUNT(DISTINCT request_id) as unique_requests
FROM lft.beproduct.dtc_master_chart_uat;
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError` | Verify `/Workspace/Repos/beproduct-sync/DTC/python/` exists |
| `401 Unauthorized` | Check secret: `databricks secrets get-secret beproduct dtc_api_key_uat` |
| `Table not found` | Run notebook once to create table with `write_mode=overwrite` |
| Job timeout | Increase `timeout_seconds` in job config |

---

## Full Documentation

See: `/Workspace/Repos/beproduct-sync/DTC/README.md`

---

**Next Steps for Future Phases**:
- Phase 2: Push updates back to DTC
- Phase 3: Change tracking (row-level deltas)
- Phase 4: Multi-app sync (BeProduct, Miro, XTS)
