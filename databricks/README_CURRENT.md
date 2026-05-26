# BeProduct ↔ Databricks Bidirectional Sync

**Status:** Production-ready for STYLE master data with KTB and other folders

## 3-Step Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                   BEPRODUCT ↔ DATABRICKS SYNC                    │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  📥 PULL (Daily 7pm HKT)      📊 MASTER DATA (Daily 10am UTC)   │
│  beproduct_style_sync.py      beproduct_master_data_sync.py     │
│                                                                  │
│  Fetches styles from           Fetches valid dropdown values    │
│  BeProduct folder (e.g., KTB)  (BRANDS, TEAM, SEASON, etc)     │
│                                                                  │
│  Creates:                      Creates:                         │
│  • ktb_styles                  • beproduct_master_brands        │
│  • ktb_styles_sync_meta        • beproduct_master_teams         │
│                                • beproduct_master_seasons       │
│                                • ... (10 more tables)           │
│                                                                  │
│                    ⬇️  (edit in Databricks)                     │
│                                                                  │
│  📤 PUSH (Manual or Hourly)                                    │
│  beproduct_style_push.py                                       │
│                                                                  │
│  Detects changes (modified_at > synced_at) and pushes to       │
│  BeProduct using field ID mapping extracted from data_json     │
│                                                                  │
│  Creates:                                                       │
│  • ktb_styles_push_log                                         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Notebooks

### 1. Pull Job: `beproduct_style_sync.py`

**Fetches styles from BeProduct → Databricks**

- **Schedule:** Daily 7pm HKT (11am UTC)
- **Modes:** FULL (on first run) or INCREMENTAL (default)
- **Tables created:**
  - `{table_name}_styles` — The data (e.g., `ktb_styles`)
  - `{table_name}_sync_meta` — Audit trail of syncs

**Parameters:**
- `folder_name` — BeProduct folder (default: KTB)
- `refresh_mode` — FULL or INCREMENTAL (default: INCREMENTAL)
- `catalog` — Target catalog (default: lft)
- `schema` — Target schema (default: beproduct)
- `table_name` — Table name (default: ktb_styles)

**Extracts:**
- 5 compulsory fields: LF Style Number, Description, Team, Season, Year
- 11 interested fields: Product Status, Customer Style Number, Product Category, etc.
- Full JSON in `data_json` column (for field ID mapping during push)

**Features:**
- ✅ Automatic schema creation (creates table on first run)
- ✅ TIMESTAMP columns for `synced_at`, `created_at`, `modified_at`
- ✅ Audit trail with last 5 sync operations logged
- ✅ 52+ KTB styles synced (as of May 2026)

**Setup:** See [QUICK_START.md](QUICK_START.md)

---

### 2. Master Data Job: `beproduct_master_data_sync.py`

**Fetches valid dropdown values from BeProduct → Databricks**

- **Schedule:** Daily 10am UTC (before pull job)
- **Tables created:** One per master data type (12 tables total)

**Master Data Types:**
- BRANDS, TEAM, SEASON, YEAR
- PRODUCT STATUS, PRODUCT CATEGORY, PRODUCT SUB CATEGORY
- DIVISION, TECHPACK STAGE, GARMENT FINISH
- PARENT VENDOR, FACTORY

**Tables Format:**
- `value` — Internal code (use when pushing)
- `label` — Human-readable name
- `data_json` — Full API response
- `synced_at` — When fetched

**Why Important:**
Dropdown/MultiSelect fields require values from Master Data. Invalid values are silently rejected by BeProduct (API returns 200 but field is set to blank).

**Setup:** See [MASTER_DATA_QUICK_START.md](MASTER_DATA_QUICK_START.md)

---

### 3. Push Job: `beproduct_style_push.py`

**Pushes changes from Databricks → BeProduct**

- **Trigger:** Manual or scheduled (hourly recommended)
- **Detection:** Records where `modified_at > synced_at`
- **Tables created:**
  - `{table_name}_push_log` — Audit trail of pushes

**Parameters:**
- `folder_name` — BeProduct folder (default: KTB)
- `source_table_name` — Source Delta table (default: ktb_styles)
- `catalog` — Catalog (default: lft)
- `schema` — Schema (default: beproduct)
- `dry_run` — true (preview) or false (actual push, default: false)

**Features:**
- ✅ Field ID mapping extracted from `data_json` (BeProduct API requires field IDs, not names)
- ✅ Dry-run mode to preview changes before pushing
- ✅ Updates all extracted fields (full strategy, not field-level diffing)
- ✅ Automatic `synced_at` timestamp update on success
- ✅ Comprehensive logging with last 5 push operations

**How It Works:**
1. Query `modified_at > synced_at` to find changed records
2. Extract field ID mapping from each record's `data_json`
3. Build update payloads using field IDs (not field names)
4. Push to BeProduct using SDK
5. Update `synced_at` on success

**Setup:** See [PUSH_QUICK_START.md](PUSH_QUICK_START.md)

---

## Workflow Example

### Scenario: Update a style description locally

**Step 1: Edit in Databricks**
```sql
UPDATE lft.beproduct.ktb_styles
SET description = 'Updated description'
WHERE lf_style_number = 'TEST'
```

This automatically sets `modified_at = current_timestamp()`.

**Step 2: Preview with dry_run=true**
```
Run push job with dry_run=true
↓
Output shows: "would push 1 record with 9 fields"
Fields: {header_name: 'Updated description', ...}
```

**Step 3: Push with dry_run=false**
```
Run push job with dry_run=false
↓
Output shows: "Pushed: 1, Failed: 0"
synced_at updated to current_timestamp()
Push log recorded
```

**Step 4: Verify in BeProduct**
Check BeProduct UI → Description is now "Updated description"

---

## Key Features

### 1. Bidirectional Sync
- ✅ Pull: BeProduct → Databricks (scheduled daily)
- ✅ Push: Databricks → BeProduct (on-demand or scheduled)
- ✅ Conflict detection: Uses `modified_at` vs `synced_at` timestamps

### 2. Field Mapping
- ✅ Extracts 16 key fields as columns
- ✅ Stores full JSON for all 40+ BeProduct fields in `data_json`
- ✅ Field ID mapping (critical for push) embedded in `data_json`

### 3. Audit Trail
- ✅ `{table}_sync_meta` — Records all pull syncs (timestamp, count, mode)
- ✅ `{table}_push_log` — Records all pushes (timestamp, success/fail, summary)
- ✅ Last 5 operations shown in job output

### 4. Master Data Validation
- ✅ Pulls valid values for all dropdown fields
- ✅ Can validate against master data before pushing
- ✅ Prevents silent failures from invalid dropdown values

### 5. Multi-Folder Support
- ✅ Parametrized `folder_name` allows creating jobs for KTB, WMT, etc.
- ✅ Each folder gets its own tables and audit trails
- ✅ One notebook template, infinite folders

---

## Folder Structure

```
databricks/
├── README_CURRENT.md                    # This file
├── QUICK_START.md                       # Pull job quick start
├── SETUP.md                             # Pull job detailed setup
├── PUSH_QUICK_START.md                  # Push job quick start
├── PUSH_SETUP.md                        # Push job detailed setup
├── MASTER_DATA_QUICK_START.md           # Master data quick start
├── MASTER_DATA_SETUP.md                 # Master data detailed setup
├── AUDIT_TRAIL.md                       # Audit trail documentation
├── AUDIT_QUICK_REFERENCE.md             # Audit queries
├── EXAMPLES.md                          # Usage examples
├── QUICK_REFERENCE.md                   # Cheat sheet
│
├── beproduct_style_sync.py              # Pull job (cells 1-2)
├── beproduct_style_push.py              # Push job (cells 1-2)
├── beproduct_master_data_sync.py        # Master data job (cells 1-2)
│
├── job_config.json                      # Example job config
└── [other files from old architecture]
```

---

## Getting Started

### For Pull Job
1. Read [QUICK_START.md](QUICK_START.md) (5 min)
2. Upload `beproduct_style_sync.py`
3. Create job in Databricks Workflows
4. Run manually to test

### For Push Job
1. Read [PUSH_QUICK_START.md](PUSH_QUICK_START.md) (5 min)
2. Upload `beproduct_style_push.py`
3. Create job in Databricks Workflows
4. Test with `dry_run=true` first

### For Master Data Job
1. Read [MASTER_DATA_QUICK_START.md](MASTER_DATA_QUICK_START.md) (2 min)
2. Upload `beproduct_master_data_sync.py`
3. Create job with schedule (10am UTC daily)
4. Use master data tables for validation

---

## Best Practices

1. **Schedule in order:**
   - 10:00 UTC → Master Data Sync
   - 11:00 UTC → Pull Job
   - 14:00 UTC → Push Job (optional, can be hourly)

2. **Always test push with dry_run=true first**

3. **Monitor audit trails:**
   ```sql
   -- See last 5 syncs
   SELECT * FROM lft.beproduct.ktb_styles_sync_meta ORDER BY last_sync_at DESC LIMIT 5;
   
   -- See last 5 pushes
   SELECT * FROM lft.beproduct.ktb_styles_push_log ORDER BY pushed_at DESC LIMIT 5;
   ```

4. **Validate dropdown values before pushing**
   ```sql
   SELECT DISTINCT brands FROM lft.beproduct.ktb_styles 
   WHERE brands NOT IN (SELECT value FROM lft.beproduct.beproduct_master_brands);
   ```

5. **Create jobs per folder:**
   - KTB styles → `beproduct_style_sync` with `folder_name=KTB`
   - WMT styles → `beproduct_style_sync` with `folder_name=WMT`
   - etc.

---

## Troubleshooting

**Pull job fails with "PARSE_SYNTAX_ERROR"**
- Fix: SQL quote escaping issue (already fixed in latest version)

**Push shows "API response" is blank/null for dropdown fields**
- Cause: Invalid dropdown value not in Master Data
- Fix: Validate against `beproduct_master_{field}` tables before pushing

**"synced_at not updated after successful push"**
- Cause: Table is read-only or no write permissions
- Fix: Check table ACLs or manually update `synced_at`

**"Row object has no attribute 'get'"**
- Cause: Using dictionary methods on Spark Row objects
- Fix: Convert to dict with `.asDict()` first

See [PUSH_SETUP.md](PUSH_SETUP.md) for comprehensive troubleshooting.

---

## Next Steps

- [ ] Create jobs for WMT folder (parametrize notebook with `folder_name=WMT`)
- [ ] Create jobs for other folders as needed
- [ ] Schedule pull + push jobs (daily pull, hourly push)
- [ ] Add validation rules using master data
- [ ] Set up monitoring/alerting for failed pushes
- [ ] Document folder-specific field mappings if they differ from KTB

---

## Contact & Support

For issues or questions:
1. Check the relevant doc file (QUICK_START, SETUP, PUSH_*, MASTER_DATA_*)
2. Review job logs in Databricks Workflows
3. Check audit tables for recent sync/push history

---

*Last updated: May 26, 2026*  
*STYLE master data KTB sync verified working*
