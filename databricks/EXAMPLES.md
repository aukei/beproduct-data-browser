# SQL Examples & Monitoring

## Common Queries

### Basic Statistics

```sql
-- Total styles in KTB folder
SELECT COUNT(*) as total_styles
FROM main.beproduct.ktb_styles;

-- Styles added today
SELECT COUNT(*) as new_today
FROM main.beproduct.ktb_styles
WHERE DATE(created_at) = CURRENT_DATE();

-- Recently modified
SELECT COUNT(*) as modified_today
FROM main.beproduct.ktb_styles
WHERE DATE(modified_at) = CURRENT_DATE();
```

### Status Distribution

```sql
-- Count by product status
SELECT
    product_status,
    COUNT(*) as count
FROM main.beproduct.ktb_styles
GROUP BY product_status
ORDER BY count DESC;

-- Status breakdown with details
SELECT
    product_status,
    COUNT(*) as count,
    COUNT(DISTINCT team) as teams,
    COUNT(DISTINCT season) as seasons
FROM main.beproduct.ktb_styles
GROUP BY product_status;
```

### Team & Season Analysis

```sql
-- Styles by team and season
SELECT
    team,
    season,
    year,
    COUNT(*) as count,
    COUNT(DISTINCT product_status) as statuses
FROM main.beproduct.ktb_styles
GROUP BY team, season, year
ORDER BY year DESC, season, team;

-- Most active teams
SELECT
    team,
    COUNT(*) as total_styles,
    COUNT(DISTINCT season) as seasons,
    COUNT(DISTINCT year) as years
FROM main.beproduct.ktb_styles
GROUP BY team
ORDER BY total_styles DESC
LIMIT 20;
```

### Category Analysis

```sql
-- Styles by product category
SELECT
    product_category,
    product_sub_category,
    COUNT(*) as count
FROM main.beproduct.ktb_styles
WHERE product_category IS NOT NULL
GROUP BY product_category, product_sub_category
ORDER BY count DESC;

-- Sub-category distribution
SELECT
    product_sub_category,
    COUNT(*) as count,
    COUNT(DISTINCT division) as divisions,
    COUNT(DISTINCT brands) as brands
FROM main.beproduct.ktb_styles
WHERE product_sub_category IS NOT NULL
GROUP BY product_sub_category
ORDER BY count DESC;
```

### Vendor & Factory Analysis

```sql
-- Top vendors and factories
SELECT
    parent_vendor,
    factory,
    COUNT(*) as style_count,
    COUNT(DISTINCT product_category) as categories
FROM main.beproduct.ktb_styles
WHERE parent_vendor IS NOT NULL
   OR factory IS NOT NULL
GROUP BY parent_vendor, factory
ORDER BY style_count DESC
LIMIT 30;

-- Styles without vendor info
SELECT
    COUNT(*) as styles_without_vendor
FROM main.beproduct.ktb_styles
WHERE parent_vendor IS NULL
  AND factory IS NULL;
```

### Search & Filter

```sql
-- Find specific style by number
SELECT
    lf_style_number,
    description,
    team,
    season,
    year,
    product_status,
    customer_style_number,
    modified_at
FROM main.beproduct.ktb_styles
WHERE lf_style_number LIKE '%ABC%'
   OR customer_style_number LIKE '%ABC%'
LIMIT 50;

-- Find styles in active development
SELECT
    lf_style_number,
    description,
    team,
    techpack_stage,
    garment_finish,
    modified_at
FROM main.beproduct.ktb_styles
WHERE techpack_stage IS NOT NULL
  AND product_status = 'Active'
ORDER BY modified_at DESC
LIMIT 100;
```

### Time-based Analysis

```sql
-- Styles by creation date
SELECT
    DATE(created_at) as created_date,
    COUNT(*) as count
FROM main.beproduct.ktb_styles
WHERE created_at >= DATE_SUB(CURRENT_DATE(), 90)
GROUP BY DATE(created_at)
ORDER BY created_date DESC;

-- Last modification timeline
SELECT
    DATE_TRUNC('week', modified_at) as week,
    COUNT(*) as modifications,
    COUNT(DISTINCT DATE(modified_at)) as days_modified
FROM main.beproduct.ktb_styles
GROUP BY DATE_TRUNC('week', modified_at)
ORDER BY week DESC
LIMIT 12;
```

### Brand Analysis

```sql
-- Top brands in KTB
SELECT
    brands,
    COUNT(*) as count
FROM main.beproduct.ktb_styles
WHERE brands IS NOT NULL
GROUP BY brands
ORDER BY count DESC;

-- Styles per brand and category
SELECT
    brands,
    product_category,
    COUNT(*) as count
FROM main.beproduct.ktb_styles
WHERE brands IS NOT NULL
  AND product_category IS NOT NULL
GROUP BY brands, product_category
ORDER BY count DESC;
```

### Missing Data Analysis

```sql
-- Data completeness check
SELECT
    COUNT(*) as total,
    COUNT(DISTINCT id) as unique_ids,
    COUNT(DISTINCT lf_style_number) as with_style_number,
    COUNT(DISTINCT description) as with_description,
    COUNT(DISTINCT team) as with_team,
    COUNT(DISTINCT season) as with_season,
    COUNT(DISTINCT year) as with_year,
    COUNT(DISTINCT product_status) as with_status
FROM main.beproduct.ktb_styles;

-- Null value summary
SELECT
    COUNT(*) - COUNT(lf_style_number) as missing_style_number,
    COUNT(*) - COUNT(description) as missing_description,
    COUNT(*) - COUNT(team) as missing_team,
    COUNT(*) - COUNT(season) as missing_season,
    COUNT(*) - COUNT(year) as missing_year,
    COUNT(*) - COUNT(product_status) as missing_status,
    COUNT(*) - COUNT(customer_style_number) as missing_customer_number,
    COUNT(*) - COUNT(product_category) as missing_category
FROM main.beproduct.ktb_styles;
```

---

## JSON Data Access

For fields not extracted as columns, access the full JSON:

```sql
-- Get all attributes from JSON
SELECT
    lf_style_number,
    get_json_object(data_json, '$.attributes') as all_attributes
FROM main.beproduct.ktb_styles
LIMIT 5;

-- Extract specific JSON fields
SELECT
    lf_style_number,
    get_json_object(data_json, '$.attributes."Lot code"') as lot_code,
    get_json_object(data_json, '$.attributes."Garment Finish"') as garment_finish,
    get_json_object(data_json, '$.id') as beproduct_id,
    get_json_object(data_json, '$.createdOn') as created_on
FROM main.beproduct.ktb_styles
LIMIT 10;

-- Using from_json for complex operations
SELECT
    lf_style_number,
    from_json(data_json, 'struct<id:string, attributes:map<string,string>>') as parsed
FROM main.beproduct.ktb_styles
LIMIT 5;
```

---

## Monitoring Queries

### Sync Status

```sql
-- When was the last sync?
SELECT
    last_sync_at,
    DATEDIFF(CURRENT_TIMESTAMP(), last_sync_at) as hours_ago
FROM main.beproduct.ktb_styles_sync_meta;

-- How fresh is our data?
SELECT
    MAX(synced_at) as last_sync,
    MIN(modified_at) as oldest_change,
    MAX(modified_at) as newest_change,
    DATEDIFF(CURRENT_TIMESTAMP(), MAX(synced_at)) as sync_age_hours
FROM main.beproduct.ktb_styles;
```

### Job Performance

To track job execution, create a simple logging table:

```sql
-- Create a job metrics table (run once)
CREATE TABLE main.beproduct.ktb_styles_job_metrics (
    run_date TIMESTAMP,
    run_mode STRING,
    record_count BIGINT,
    duration_minutes FLOAT,
    total_rows_in_table BIGINT,
    status STRING
)
USING DELTA;

-- After each job run, insert metrics:
INSERT INTO main.beproduct.ktb_styles_job_metrics
SELECT
    CURRENT_TIMESTAMP() as run_date,
    'INCREMENTAL' as run_mode,
    COUNT(*) as record_count,
    0.5 as duration_minutes,  -- set manually from job logs
    (SELECT COUNT(*) FROM main.beproduct.ktb_styles) as total_rows_in_table,
    'SUCCESS' as status;

-- View job history
SELECT
    DATE(run_date) as date,
    run_mode,
    SUM(record_count) as records_synced,
    AVG(duration_minutes) as avg_duration,
    COUNT(*) as runs
FROM main.beproduct.ktb_styles_job_metrics
GROUP BY DATE(run_date), run_mode
ORDER BY date DESC;
```

---

## Advanced Queries

### Data Quality Checks

```sql
-- Find styles missing critical fields
SELECT
    lf_style_number,
    CASE WHEN description IS NULL THEN 'Missing' ELSE 'Present' END as has_description,
    CASE WHEN team IS NULL THEN 'Missing' ELSE 'Present' END as has_team,
    CASE WHEN product_status IS NULL THEN 'Missing' ELSE 'Present' END as has_status
FROM main.beproduct.ktb_styles
WHERE description IS NULL
   OR team IS NULL
   OR product_status IS NULL
ORDER BY lf_style_number;

-- Identify potential duplicates
SELECT
    lf_style_number,
    COUNT(*) as count,
    MAX(synced_at) as latest_sync
FROM main.beproduct.ktb_styles
GROUP BY lf_style_number
HAVING COUNT(*) > 1;
```

### Comparison with Previous Sync

```sql
-- If keeping historical snapshots, compare changes
SELECT
    current.lf_style_number,
    current.product_status as current_status,
    current.modified_at as current_modified,
    current.team,
    current.season
FROM main.beproduct.ktb_styles current
WHERE DATE(current.modified_at) = CURRENT_DATE()
ORDER BY current.modified_at DESC
LIMIT 50;
```

### Export to Parquet for Analytics

```sql
-- Create read-only snapshot for BI tools
CREATE TABLE main.beproduct.ktb_styles_snapshot
USING PARQUET
AS
SELECT
    id,
    lf_style_number,
    description,
    team,
    season,
    year,
    product_status,
    product_category,
    product_sub_category,
    division,
    brands,
    parent_vendor,
    factory,
    modified_at,
    synced_at
FROM main.beproduct.ktb_styles
WHERE product_status = 'Active'
ORDER BY modified_at DESC;
```

---

## Dashboards & Alerts

### Create a Summary Dashboard

In Databricks SQL Editor, save these as queries:

1. **Total Styles** (KPI)
   ```sql
   SELECT COUNT(*) as total_styles FROM main.beproduct.ktb_styles;
   ```

2. **Status Distribution** (Bar chart)
   ```sql
   SELECT product_status, COUNT(*) as count
   FROM main.beproduct.ktb_styles
   GROUP BY product_status;
   ```

3. **Last Sync** (Scalar)
   ```sql
   SELECT last_sync_at FROM main.beproduct.ktb_styles_sync_meta;
   ```

4. **Recently Modified** (Table)
   ```sql
   SELECT
       lf_style_number,
       description,
       product_status,
       modified_at
   FROM main.beproduct.ktb_styles
   WHERE DATE(modified_at) >= CURRENT_DATE() - 7
   ORDER BY modified_at DESC;
   ```

Then create a dashboard and pin these queries.

### Set Up Alerts

In Databricks SQL:

```sql
-- Alert: No data synced in last 24 hours
SELECT
    CASE
        WHEN DATEDIFF(CURRENT_TIMESTAMP(), MAX(synced_at)) > 24
        THEN 'ALERT: No sync in 24 hours'
        ELSE 'OK'
    END as status
FROM main.beproduct.ktb_styles;
```

---

## Performance Tips

1. **Use Delta statistics** for query optimization:
   ```sql
   ANALYZE TABLE main.beproduct.ktb_styles COMPUTE STATISTICS;
   ```

2. **Filter early** on indexed columns (`folder_name`, `product_status`)
3. **Cache frequently queried data**:
   ```sql
   CACHE TABLE main.beproduct.ktb_styles;
   ```

4. **Use Z-order for common filters**:
   ```sql
   OPTIMIZE main.beproduct.ktb_styles ZORDER BY (team, season, product_status);
   ```

---

For more information, see `SETUP.md` and `QUICK_START.md`.
