# Audit Trail Quick Reference

## View Sync History

```sql
-- Last 10 pulls (BeProduct → Databricks)
SELECT * FROM main.beproduct.ktb_styles_sync_meta ORDER BY last_sync_at DESC LIMIT 10;

-- Last 10 pushes (Databricks → BeProduct)
SELECT * FROM main.beproduct.ktb_styles_push_log ORDER BY pushed_at DESC LIMIT 10;
```

## Daily Summary

```sql
-- Pull activity by day
SELECT
    DATE(last_sync_at) as date,
    COUNT(*) as sync_count,
    SUM(records_synced) as total_records
FROM main.beproduct.ktb_styles_sync_meta
GROUP BY DATE(last_sync_at)
ORDER BY date DESC;

-- Push activity by day
SELECT
    DATE(pushed_at) as date,
    COUNT(*) as push_count,
    SUM(records_pushed) as total_pushed,
    SUM(records_failed) as total_failed
FROM main.beproduct.ktb_styles_push_log
GROUP BY DATE(pushed_at)
ORDER BY date DESC;
```

## Compare Folders

```sql
-- Which folder synced last?
SELECT
    'KTB' as folder,
    MAX(last_sync_at) as last_sync
FROM main.beproduct.ktb_styles_sync_meta

UNION ALL

SELECT
    'WMT' as folder,
    MAX(last_sync_at) as last_sync
FROM main.beproduct.wmt_styles_sync_meta
ORDER BY last_sync DESC;
```

## Find Issues

```sql
-- Failed pushes
SELECT * FROM main.beproduct.ktb_styles_push_log
WHERE records_failed > 0
ORDER BY pushed_at DESC;

-- Slow syncs (lots of records)
SELECT * FROM main.beproduct.ktb_styles_sync_meta
WHERE records_synced > 50
ORDER BY last_sync_at DESC;
```

## Table Locations

| Flow | Table |
|------|-------|
| **Pull** (BeProduct → DB) | `{catalog}.{schema}.{table_name}_sync_meta` |
| **Push** (DB → BeProduct) | `{catalog}.{schema}.{table_name}_push_log` |

## Example

```
Pull job:  lft.beproduct.ktb_styles_sync_meta
Push job:  lft.beproduct.ktb_styles_push_log
```

## Job Output Shows

Every job run prints the last 5 sync/push records:

```
📜 SYNC HISTORY (last 5 syncs):
   1. 2026-05-22 | INCREMENTAL  |   5 records | 5 styles synced from folder 'KTB'
   2. 2026-05-22 | INCREMENTAL  |   2 records | 2 styles synced from folder 'KTB'
```

---

For detailed docs, see `AUDIT_TRAIL.md`
