# Audit Trail & Sync History

## Overview

Both pull and push jobs now maintain detailed audit logs for every sync operation. This allows you to:

- **Track what changed** - When and how many records were synced
- **Audit operations** - See who ran what and with what parameters
- **Troubleshoot issues** - Review history to understand sync patterns
- **Compliance** - Maintain records of data modifications

---

## Sync Metadata Tables

### Pull Job: `{table_name}_sync_meta`

Records each pull from BeProduct to Databricks.

**Schema:**
```sql
CREATE TABLE {table_name}_sync_meta (
    last_sync_at STRING,     -- ISO timestamp of pull
    sync_type STRING,        -- "FULL" or "INCREMENTAL"
    records_synced LONG,     -- Number of records pulled
    summary STRING           -- Human-readable description
)
```

**Example records:**
```
last_sync_at              | sync_type    | records_synced | summary
2026-05-22T11:45:30...   | FULL         | 52             | 52 styles synced from folder 'KTB' (mode: FULL)
2026-05-22T12:00:15...   | INCREMENTAL  | 5              | 5 styles synced from folder 'KTB' (mode: INCREMENTAL)
2026-05-22T13:15:42...   | INCREMENTAL  | 2              | 2 styles synced from folder 'KTB' (mode: INCREMENTAL)
```

### Push Job: `{table_name}_push_log`

Records each push from Databricks back to BeProduct.

**Schema:**
```sql
CREATE TABLE {table_name}_push_log (
    pushed_at STRING,        -- ISO timestamp of push
    records_pushed LONG,     -- Number successfully pushed
    records_failed LONG,     -- Number that failed
    summary STRING           -- Human-readable description
)
```

**Example records:**
```
pushed_at                 | records_pushed | records_failed | summary
2026-05-22T11:50:22...   | 3              | 0              | 3 styles pushed to BeProduct
2026-05-22T12:05:10...   | 5              | 1              | 5 styles pushed to BeProduct (1 failed)
2026-05-22T13:20:30...   | 2              | 0              | 2 styles pushed to BeProduct
```

---

## Querying Sync History

### View Last 10 Syncs (Pull)

```sql
SELECT
    last_sync_at,
    sync_type,
    records_synced,
    summary
FROM main.beproduct.ktb_styles_sync_meta
ORDER BY last_sync_at DESC
LIMIT 10;
```

Output:
```
last_sync_at              | sync_type    | records_synced | summary
2026-05-22T13:15:42.123   | INCREMENTAL  | 2              | 2 styles synced from folder 'KTB' (mode: INCREMENTAL)
2026-05-22T12:00:15.456   | INCREMENTAL  | 5              | 5 styles synced from folder 'KTB' (mode: INCREMENTAL)
2026-05-22T11:45:30.789   | FULL         | 52             | 52 styles synced from folder 'KTB' (mode: FULL)
```

### View Last 10 Pushes (Push)

```sql
SELECT
    pushed_at,
    records_pushed,
    records_failed,
    summary
FROM main.beproduct.ktb_styles_push_log
ORDER BY pushed_at DESC
LIMIT 10;
```

Output:
```
pushed_at                 | records_pushed | records_failed | summary
2026-05-22T13:20:30.123   | 2              | 0              | 2 styles pushed to BeProduct
2026-05-22T12:05:10.456   | 5              | 1              | 5 styles pushed to BeProduct (1 failed)
2026-05-22T11:50:22.789   | 3              | 0              | 3 styles pushed to BeProduct
```

### Compare Multiple Tables

```sql
-- View sync history for KTB and WMT styles
SELECT 'KTB' as folder, last_sync_at, sync_type, records_synced, summary
FROM main.beproduct.ktb_styles_sync_meta
UNION ALL
SELECT 'WMT' as folder, last_sync_at, sync_type, records_synced, summary
FROM main.beproduct.wmt_styles_sync_meta
ORDER BY last_sync_at DESC
LIMIT 20;
```

### Daily Summary (Pull)

```sql
-- How many styles synced each day?
SELECT
    DATE(last_sync_at) as sync_date,
    COUNT(*) as sync_count,
    SUM(records_synced) as total_records,
    COUNT(CASE WHEN sync_type = 'FULL' THEN 1 END) as full_syncs,
    COUNT(CASE WHEN sync_type = 'INCREMENTAL' THEN 1 END) as incremental_syncs
FROM main.beproduct.ktb_styles_sync_meta
GROUP BY DATE(last_sync_at)
ORDER BY sync_date DESC;
```

Output:
```
sync_date  | sync_count | total_records | full_syncs | incremental_syncs
2026-05-22 | 7          | 15            | 1          | 6
2026-05-21 | 6          | 52            | 1          | 5
2026-05-20 | 8          | 28            | 0          | 8
```

### Daily Summary (Push)

```sql
-- How many styles pushed each day?
SELECT
    DATE(pushed_at) as push_date,
    COUNT(*) as push_count,
    SUM(records_pushed) as total_pushed,
    SUM(records_failed) as total_failed,
    ROUND(100.0 * SUM(records_pushed) / (SUM(records_pushed) + SUM(records_failed)), 1) as success_rate
FROM main.beproduct.ktb_styles_push_log
GROUP BY DATE(pushed_at)
ORDER BY push_date DESC;
```

Output:
```
push_date  | push_count | total_pushed | total_failed | success_rate
2026-05-22 | 4          | 10           | 1            | 90.9%
2026-05-21 | 3          | 8            | 0            | 100.0%
2026-05-20 | 2          | 5            | 0            | 100.0%
```

### Find Slow Syncs

```sql
-- Which syncs took longest (by records)? Or most failures?
SELECT
    pushed_at,
    records_pushed,
    records_failed,
    CASE 
        WHEN records_failed > 0 THEN ROUND(100.0 * records_failed / (records_pushed + records_failed), 1)
        ELSE 0
    END as failure_rate,
    summary
FROM main.beproduct.ktb_styles_push_log
WHERE records_failed > 0
   OR records_pushed > 20
ORDER BY pushed_at DESC
LIMIT 10;
```

### Audit Trail for a Specific Record

```sql
-- When was a specific style last synced/pushed?
WITH last_pull AS (
    SELECT MAX(last_sync_at) as last_pull_at
    FROM main.beproduct.ktb_styles_sync_meta
),
last_push AS (
    SELECT MAX(pushed_at) as last_push_at
    FROM main.beproduct.ktb_styles_push_log
)
SELECT
    'LFBP-WM1MJ-002' as style_number,
    (SELECT last_pull_at FROM last_pull) as last_synced_from_beproduct,
    (SELECT last_push_at FROM last_push) as last_pushed_to_beproduct,
    DATEDIFF(HOUR, (SELECT last_pull_at FROM last_pull), (SELECT last_push_at FROM last_push)) as hours_since_last_sync;
```

---

## Integration with Job Output

### Pull Job Output

Every pull job run now shows:

```
================================================================================
Step 7: Save Sync Metadata
================================================================================
✅ Metadata saved to ktb_styles_sync_meta:
   Timestamp: 2026-05-22T13:15:42.123456+00:00
   Type: INCREMENTAL
   Records: 5
   Summary: 5 styles synced from folder 'KTB' (mode: INCREMENTAL)

================================================================================
SYNC SUMMARY
================================================================================

✅ Job completed successfully!

   Mode: INCREMENTAL
   Rows synced: 5
   Write mode: append
   Table: lft.beproduct.ktb_styles
   Total rows: 52
   Timestamp: 2026-05-22T13:15:42.123456+00:00

📜 SYNC HISTORY (last 5 syncs):
   1. 2026-05-22 | INCREMENTAL  |   5 records | 5 styles synced from folder 'KTB' (mode: INCREMENTAL)
   2. 2026-05-22 | INCREMENTAL  |   2 records | 2 styles synced from folder 'KTB' (mode: INCREMENTAL)
   3. 2026-05-22 | FULL         |  52 records | 52 styles synced from folder 'KTB' (mode: FULL)
   4. 2026-05-21 | INCREMENTAL  |   3 records | 3 styles synced from folder 'KTB' (mode: INCREMENTAL)
   5. 2026-05-21 | INCREMENTAL  |   1 records | 1 styles synced from folder 'KTB' (mode: INCREMENTAL)
```

### Push Job Output

Every push job run now shows:

```
================================================================================
Step 6: Log Push Metadata
================================================================================
✅ Push log saved to ktb_styles_push_log:
   Timestamp: 2026-05-22T13:25:15.789123+00:00
   Pushed: 3
   Failed: 0
   Summary: 3 styles pushed to BeProduct

================================================================================
PUSH SUMMARY
================================================================================

✅ Push job complete!

   Records pushed: 3
   Records failed: 0
   Success rate: 100.0%

📜 PUSH HISTORY (last 5 pushes):
   1. 2026-05-22 ✓ | 3 pushed, 0 failed | 3 styles pushed to BeProduct
   2. 2026-05-22 ✓ | 5 pushed, 1 failed | 5 styles pushed to BeProduct (1 failed)
   3. 2026-05-22 ✓ | 2 pushed, 0 failed | 2 styles pushed to BeProduct
   4. 2026-05-21 ✓ | 8 pushed, 0 failed | 8 styles pushed to BeProduct
   5. 2026-05-21 ✗ | 0 pushed, 3 failed | 3 styles pushed to BeProduct (3 failed)
```

---

## Best Practices

### Monitor Trends

```sql
-- Alert if last pull was more than 2 hours ago
SELECT
    DATEDIFF(HOUR, MAX(last_sync_at), CURRENT_TIMESTAMP()) as hours_since_sync
FROM main.beproduct.ktb_styles_sync_meta
HAVING DATEDIFF(HOUR, MAX(last_sync_at), CURRENT_TIMESTAMP()) > 2;
```

### Track Failed Pushes

```sql
-- Monitor push failures
SELECT
    pushed_at,
    records_pushed,
    records_failed,
    ROUND(100.0 * records_failed / (records_pushed + records_failed), 1) as failure_rate
FROM main.beproduct.ktb_styles_push_log
WHERE records_failed > 0
ORDER BY pushed_at DESC;
```

### Validate Consistency

```sql
-- Are KTB and WMT synced at similar times?
SELECT
    COALESCE(k.sync_date, w.sync_date) as date,
    k.total_ktb,
    w.total_wmt,
    ABS(DATEDIFF(HOUR, k.last_ktb, w.last_wmt)) as hours_apart
FROM (
    SELECT
        DATE(last_sync_at) as sync_date,
        SUM(records_synced) as total_ktb,
        MAX(last_sync_at) as last_ktb
    FROM main.beproduct.ktb_styles_sync_meta
    GROUP BY DATE(last_sync_at)
) k
FULL OUTER JOIN (
    SELECT
        DATE(last_sync_at) as sync_date,
        SUM(records_synced) as total_wmt,
        MAX(last_sync_at) as last_wmt
    FROM main.beproduct.wmt_styles_sync_meta
    GROUP BY DATE(last_sync_at)
) w
ON k.sync_date = w.sync_date
ORDER BY date DESC;
```

---

## Cleanup & Retention

### Archive Old Logs (Optional)

```sql
-- Keep only last 90 days of logs
DELETE FROM main.beproduct.ktb_styles_sync_meta
WHERE last_sync_at < DATE_SUB(CURRENT_DATE(), 90);

DELETE FROM main.beproduct.ktb_styles_push_log
WHERE pushed_at < DATE_SUB(CURRENT_DATE(), 90);
```

### View Log Size

```sql
-- How much space are the logs taking?
SELECT
    'ktb_styles_sync_meta' as table_name,
    COUNT(*) as row_count,
    ROUND(SIZE(data_json) / 1024 / 1024) as size_mb
FROM main.beproduct.ktb_styles_sync_meta

UNION ALL

SELECT
    'ktb_styles_push_log' as table_name,
    COUNT(*) as row_count,
    0 as size_mb
FROM main.beproduct.ktb_styles_push_log;
```

---

**Last Updated:** 2026-05-22
