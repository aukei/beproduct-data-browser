# BeProduct STYLE Master Sync Job - Setup & Usage

## Overview

This Databricks job retrieves **STYLE master data** from BeProduct's **KTB folder** and stores it in a Delta Lake table. It runs **daily at 7pm HKT (11am UTC)** and supports both **FULL** and **INCREMENTAL** refresh modes.

### Key Features

- ✅ **Incremental by default** — only fetches records modified since last sync
- ✅ **Full refresh option** — manual trigger with `refresh_mode=FULL` for complete data refresh
- ✅ **Schema-aware** — extracts 16 key fields as columns, stores full JSON for extensibility
- ✅ **Fault tolerant** — automatic retry on transient failures
- ✅ **Isolated credentials** — uses Databricks Secrets for authentication

---

## Prerequisites

1. **Databricks workspace** with SQL Warehouse or all-purpose cluster
2. **BeProduct credentials:**
   - `client_id`
   - `client_secret`
   - `refresh_token`
   - `company_domain`

   > See [Getting BeProduct Credentials](#getting-beproduct-credentials) below

3. **Databricks Personal Access Token (PAT)** for API calls (to set up secrets + create job)

---

## Step 1: Set Up Databricks Secrets

Store BeProduct credentials in Databricks Secrets so the job can access them securely.

### Option A: Using Databricks UI

1. Go to **User Settings** → **Secret Keys** (bottom-left corner)
2. Click **+ Create Secret**
3. Scope: `beproduct` (create if doesn't exist)
4. Add these key-value pairs:
   - Key: `client_id` → Value: `your_client_id`
   - Key: `client_secret` → Value: `your_client_secret`
   - Key: `refresh_token` → Value: `your_refresh_token`
   - Key: `company_domain` → Value: `your_company_domain`

### Option B: Using Databricks CLI

```bash
# Set up CLI: https://docs.databricks.com/dev-tools/cli/

databricks secrets create-scope beproduct
databricks secrets put-secret beproduct client_id --string-value "your_client_id"
databricks secrets put-secret beproduct client_secret --string-value "your_client_secret"
databricks secrets put-secret beproduct refresh_token --string-value "your_refresh_token"
databricks secrets put-secret beproduct company_domain --string-value "your_company_domain"
```

### Option C: Using Databricks Python API

```python
import requests

# Set these
PAT = "dapi..."  # Your Databricks PAT
WORKSPACE_URL = "https://adb-xxxx.azuredatabricks.net"

def put_secret(scope, key, value):
    url = f"{WORKSPACE_URL}/api/2.1/secrets/put"
    headers = {"Authorization": f"Bearer {PAT}"}
    body = {
        "scope": scope,
        "key": key,
        "bytes_value": value.encode(),
    }
    requests.post(url, json=body, headers=headers)

put_secret("beproduct", "client_id", "your_client_id")
put_secret("beproduct", "client_secret", "your_client_secret")
put_secret("beproduct", "refresh_token", "your_refresh_token")
put_secret("beproduct", "company_domain", "your_company_domain")

print("✅ Secrets created")
```

---

## Step 2: Upload the Notebook

### Option A: Using Databricks UI

1. Go to **Workspace** → **Repos** (or create a new repo if you want)
2. Create a folder: `/Repos/beproduct-data-browser/databricks`
3. Upload the notebook file `beproduct_style_sync.py`
4. Note the full path: `/Repos/beproduct-data-browser/databricks/beproduct_style_sync`

### Option B: Using Databricks CLI

```bash
# Navigate to your workspace repo
cd /path/to/beproduct-data-browser

# Upload notebook (Databricks automatically converts .py to .scala or .sql)
databricks workspace import databricks/beproduct_style_sync.py \
  /Repos/beproduct-data-browser/databricks/beproduct_style_sync \
  -l PYTHON -o
```

### Option C: Using Git Sync with Repos

If your workspace is connected to Git, simply push this file to your repo:

```bash
git add databricks/beproduct_style_sync.py
git commit -m "Add BeProduct STYLE sync notebook"
git push
```

Then the path will be: `/Repos/<your-repo-path>/databricks/beproduct_style_sync`

---

## Step 3: Create the Databricks Job

### Option A: Using Databricks UI (Easiest)

1. Go to **Workflows** → **Jobs**
2. Click **+ Create job**
3. Configure:

   **Job name:** `BeProduct STYLE Master - KTB Daily Sync`

   **Task:**
   - **Task name:** `beproduct_style_sync`
   - **Type:** Notebook
   - **Notebook path:** `/Repos/beproduct-data-browser/databricks/beproduct_style_sync`
   - **Cluster:** Choose one:
     - All-purpose cluster (existing)
     - Job cluster (create one for this job only) — recommended for cost control
   
   **Parameters:**
   ```
   refresh_mode = INCREMENTAL
   catalog = main
   schema = beproduct
   table_name = ktb_styles
   ```

   **Advanced options:**
   - **Timeout:** 1 hour (3600 seconds)
   - **Max retries:** 1
   - **Retry delay:** 60 seconds

4. **Schedule:**
   - Click **"Add schedule"**
   - **Frequency:** Daily
   - **Time:** 7:00 PM HKT = 11:00 AM UTC
     - If your UI defaults to a different timezone, convert:
       - HKT (UTC+8) → UTC: subtract 8 hours → **11:00 AM UTC**
     - Use a cron expression: `0 0 11 * * ?`
   - **Timezone:** UTC

5. **Permissions:**
   - Share with your team (optional)

6. Click **"Create job"**

### Option B: Using Databricks API

```bash
# Set these
PAT="dapi..."
WORKSPACE_URL="https://adb-xxxx.azuredatabricks.net"
NOTEBOOK_PATH="/Repos/beproduct-data-browser/databricks/beproduct_style_sync"
CLUSTER_ID="your-cluster-id"  # OR omit to use job cluster (see below)

# Create job via API
curl -X POST "$WORKSPACE_URL/api/2.1/jobs/create" \
  -H "Authorization: Bearer $PAT" \
  -H "Content-Type: application/json" \
  -d @databricks/job_config.json | jq .

# Response: { "job_id": 123 }
```

Or use Python:

```python
import requests
import json

PAT = "dapi..."
WORKSPACE_URL = "https://adb-xxxx.azuredatabricks.net"

with open("databricks/job_config.json") as f:
    config = json.load(f)

# Update notebook path if different
config["tasks"][0]["notebook_task"]["notebook_path"] = "/Repos/your-path/beproduct_style_sync"

headers = {
    "Authorization": f"Bearer {PAT}",
    "Content-Type": "application/json",
}

response = requests.post(
    f"{WORKSPACE_URL}/api/2.1/jobs/create",
    json=config,
    headers=headers,
)

job_id = response.json()["job_id"]
print(f"✅ Job created: {job_id}")
```

### Option C: Using Databricks CLI

```bash
databricks jobs create --json @databricks/job_config.json
```

---

## Step 4: Verify the Setup

### Test Manual Run

1. Go to **Workflows** → **Jobs** → find your job
2. Click **"Run now"** button
3. Monitor the run:
   - Watch the logs in real-time
   - Check for errors in the **"Logs"** tab
   - Once complete, verify data in the Delta table:

     ```sql
     SELECT COUNT(*) FROM main.beproduct.ktb_styles;
     ```

### Test Incremental Refresh

After the first full sync, run again with `refresh_mode=INCREMENTAL`:

1. Click **"Run now"** (should use INCREMENTAL by default)
2. Verify that fewer records are synced
3. Check that only recently modified styles are in the table

### Override Refresh Mode

To manually trigger a **FULL refresh** (e.g., after data corruption):

1. Click **"Run now"** button
2. In the **"Parameters"** section, override: `refresh_mode = FULL`
3. Click **"Run"**

---

## Data Schema

The generated Delta table has the following structure:

### System Fields
| Column | Type | Description |
|--------|------|-------------|
| `id` | STRING | BeProduct unique ID |
| `folder_name` | STRING | Always "KTB" for this job |
| `created_at` | STRING | ISO 8601 creation timestamp |
| `modified_at` | STRING | ISO 8601 last modification timestamp |
| `synced_at` | STRING | ISO 8601 sync timestamp |
| `data_json` | STRING | Full API response as JSON |

### Compulsory Fields (from BeProduct)
| Column | Description |
|--------|-------------|
| `lf_style_number` | LF Style Number |
| `description` | Description |
| `team` | Team |
| `season` | Season |
| `year` | Year |

### Interested Fields (from BeProduct)
| Column | Description |
|--------|-------------|
| `product_status` | Product Status |
| `customer_style_number` | Customer Style Number |
| `product_category` | Product Category |
| `product_sub_category` | Product Sub Category |
| `division` | Division |
| `brands` | Brands |
| `garment_finish` | Garment Finish |
| `techpack_stage` | Techpack Stage |
| `lot_code` | Lot Code |
| `parent_vendor` | Parent Vendor |
| `factory` | Factory |

### Example Query

```sql
-- Get all active styles with their key attributes
SELECT
    lf_style_number,
    description,
    team,
    season,
    year,
    product_status,
    customer_style_number,
    product_category,
    division,
    modified_at,
    synced_at
FROM main.beproduct.ktb_styles
WHERE product_status = 'Active'
ORDER BY modified_at DESC
LIMIT 100;

-- Parse JSON for other fields
SELECT
    lf_style_number,
    get_json_object(data_json, '$.attributes.Brand') as brands,
    get_json_object(data_json, '$.attributes."Lot code"') as lot_code,
    synced_at
FROM main.beproduct.ktb_styles
LIMIT 10;
```

---

## Job Parameters

### `refresh_mode` (Default: `INCREMENTAL`)

- **`INCREMENTAL`** (Recommended for daily runs)
  - Fetches only records modified since last sync
  - Appends/updates rows to the existing table
  - Fast and efficient for recurring jobs
  - Falls back to FULL if no sync metadata exists

- **`FULL`** (Use for manual recovery)
  - Fetches all records from KTB folder
  - Replaces entire table contents
  - Useful after data corruption or schema changes
  - Slower but ensures clean state

### `catalog` (Default: `main`)
Databricks Unity Catalog name. Use `hive_metastore` for non-UC workspaces.

### `schema` (Default: `beproduct`)
Schema/database name where table will be created.

### `table_name` (Default: `ktb_styles`)
Table name (without catalog/schema prefix).

Full table path will be: `{catalog}.{schema}.{table_name}`

---

## Monitoring & Troubleshooting

### Check Job Run History

```bash
databricks jobs get-run --run-id <run_id>
```

Or via UI:
1. Go to **Workflows** → **Jobs** → select job
2. Click **"Runs"** tab to see all past executions

### Common Issues

#### ❌ `Failed to retrieve BeProduct credentials`
- **Cause:** Secrets not set up correctly
- **Solution:** 
  1. Verify secrets exist: go to **User Settings** → **Secret Keys**
  2. Check secret scope is named `beproduct` (case-sensitive)
  3. Verify all 4 keys exist: `client_id`, `client_secret`, `refresh_token`, `company_domain`

#### ❌ `401 Unauthorized`
- **Cause:** Refresh token expired or invalid
- **Solution:**
  1. Re-run `get_refresh_token.py` from the main app to obtain a new token
  2. Update the secret: `databricks secrets put-secret beproduct refresh_token ...`

#### ❌ `Failed to write to Delta table`
- **Cause:** Missing schema or write permissions
- **Solution:**
  1. Ensure you have CREATE TABLE privileges on the target schema
  2. Verify catalog and schema names are correct
  3. Check table doesn't have conflicting structure

#### ❌ Job takes too long (timeout)
- **Cause:** Large dataset or network issues
- **Solution:**
  1. Increase job timeout (default 1 hour)
  2. Check network connectivity to BeProduct API
  3. For very large datasets, consider filtering by date range

### Check Sync Metadata

The job maintains a metadata table for incremental sync state:

```sql
SELECT * FROM main.beproduct.ktb_styles_sync_meta;
```

To force a full refresh, delete this metadata:

```sql
DROP TABLE IF EXISTS main.beproduct.ktb_styles_sync_meta;
```

Then run the job with `refresh_mode=FULL`.

---

## Performance Tuning

### Cluster Configuration

The default `job_config.json` uses:
- **Spark version:** 14.3.x
- **Node type:** i3.xlarge (2 cores, 30 GB RAM)
- **Workers:** 2
- **Spot instances:** Enabled (cost-effective)

For **larger datasets** (>100k styles):
- Increase workers to 4-8
- Use larger node types (i3.2xlarge)

For **frequent incremental syncs**:
- Use smaller cluster (1 worker)
- All-purpose cluster is fine

### Optimization Tips

1. **Increase page size** in `fetch_styles()` to reduce API calls:
   ```python
   page_size = 500  # Default: 100
   ```

2. **Parallel API requests** (if rate limits allow):
   - Use `ThreadPoolExecutor` to fetch multiple pages concurrently
   - Requires careful rate-limit handling

3. **Incremental-only mode**:
   - Remove the FULL refresh option if not needed
   - Saves storage and compute

---

## Getting BeProduct Credentials

### If you have existing credentials from the Streamlit app

1. Check your `.env` file in the beproduct-data-browser repo
2. Use the values for `BEPRODUCT_CLIENT_ID`, `BEPRODUCT_CLIENT_SECRET`, `BEPRODUCT_REFRESH_TOKEN`, `BEPRODUCT_COMPANY_DOMAIN`

### If you need new credentials

1. Contact [support@beproduct.com](mailto:support@beproduct.com) with:
   - Your company name
   - Intended use case (Databricks integration)
   - OAuth callback URL (e.g., `http://localhost:8765/callback` for local, or leave blank for server-side)

2. They will provide:
   - `client_id`
   - `client_secret`

3. To get the `refresh_token`, run the main app's token bootstrap:
   ```bash
   python scripts/get_refresh_token.py
   ```
   This opens a browser → you log in → the script outputs your `refresh_token`

---

## Best Practices

1. ✅ **Use INCREMENTAL by default** — more efficient, reduces API calls
2. ✅ **Monitor rate limits** — check API response headers in logs
3. ✅ **Set max concurrent runs to 1** — avoids duplicate syncs
4. ✅ **Test full refresh periodically** — catch schema changes early
5. ✅ **Archive old data** — consider retention policies for the Delta table
6. ✅ **Use Databricks Secrets** — never hardcode credentials
7. ✅ **Set up alerts** — notify on job failures via Databricks alerts

---

## SQL Queries for Common Tasks

### Count records by status

```sql
SELECT product_status, COUNT(*) as cnt
FROM main.beproduct.ktb_styles
GROUP BY product_status
ORDER BY cnt DESC;
```

### Find recently modified styles

```sql
SELECT
    lf_style_number,
    description,
    modified_at,
    synced_at
FROM main.beproduct.ktb_styles
WHERE DATE(modified_at) = CURRENT_DATE()
ORDER BY modified_at DESC;
```

### Export to CSV

```sql
SELECT * FROM main.beproduct.ktb_styles
LIMIT 1000
```

Then use Databricks UI to download as CSV, or:

```python
df = spark.sql("SELECT * FROM main.beproduct.ktb_styles")
df.coalesce(1).write.mode("overwrite").option("header", "true").csv("/tmp/styles")
```

---

## Support & Feedback

For issues or suggestions:
- Report on GitHub: https://github.com/beproduct-data-browser/issues
- Contact: [support@beproduct.com](mailto:support@beproduct.com)

---

**Last updated:** 2026-05-15
