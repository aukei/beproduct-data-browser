# BeProduct Data Browser

A **Streamlit** web app that keeps a local sync copy of your [BeProduct](https://www.beproduct.com/) PLM data and lets you browse, edit, and push changes back to the SaaS.

## Features

| Feature | Detail |
|---|---|
| **Local sync** | Downloads Styles, Materials, Color Palettes, Directory records into SQLite |
| **Incremental sync** | Every 15 min (configurable) — only fetches records changed since last sync |
| **Browser UI** | Search, filter by folder, click-to-detail with full nested data |
| **Edit + push-back** | Edit attribute fields locally, push to BeProduct with one click |
| **Rate limit display** | Shows API requests used / limit from response headers in real time |
| **Dirty tracking** | Locally-modified records are flagged; conflicts with remote changes are handled gracefully |

---

## Quick Start

### 1. Prerequisites

- Python 3.11+
- BeProduct API credentials (request from [support@beproduct.com](mailto:support@beproduct.com)):
  - `client_id`
  - `client_secret`
  - `callback_url` (use `http://localhost:8765/callback` for local use)

### 2. Install

```bash
git clone <this-repo>
cd beproduct-data-browser
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure

```bash
cp .env.example .env
# Edit .env — fill in CLIENT_ID, CLIENT_SECRET, COMPANY_DOMAIN
```

### 4. Get your Refresh Token (one-time)

```bash
python scripts/get_refresh_token.py
```

This opens a browser → you log in to BeProduct → the script captures the OAuth callback and prints your `refresh_token`. Paste it into `.env`:

```env
BEPRODUCT_REFRESH_TOKEN=paste_here
```

> The refresh token **does not expire** unless revoked. You only need to do this once.

### 5. Run the app

```bash
streamlit run app/ui/main.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## First Run

1. Click **⬇ Full Sync** in the sidebar to download all your BeProduct data
2. Navigate to **Styles**, **Materials**, **Colors**, or **Directory** using the sidebar
3. Click any row to open the detail view
4. Edit fields and click **💾 Save Locally**, then **🚀 Push to BeProduct**

---

## Export to Databricks

The `scripts/upload_to_databricks.py` script uploads all 6 models (Styles, Materials, Colors, Images, Blocks, Directory) from your local SQLite database to Azure Databricks Delta tables.

### Prerequisites

1. Install the Databricks SQL connector:
   ```bash
   pip install -r requirements-scripts.txt
   ```

2. Add Databricks credentials to `.env`:
   ```env
   DATABRICKS_HOST=https://adb-xxxxxxxx.azuredatabricks.net
   DATABRICKS_PAT=dapi...
   DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id>
   ```

   - **DATABRICKS_HOST**: Your workspace URL
   - **DATABRICKS_PAT**: Personal Access Token (generate in Databricks workspace → User Settings → Access Tokens)
   - **DATABRICKS_HTTP_PATH**: SQL Warehouse HTTP path (from Databricks → SQL Warehouses → Connection details)

### Usage

```bash
python scripts/upload_to_databricks.py
```

The script is **fully interactive**:

1. **Shows counts**: Displays how many records exist locally for each of the 6 models
2. **Asks for destination**: Prompts for catalog, schema, and a table **prefix**
   - Tables will be created as: `{prefix}_styles`, `{prefix}_materials`, `{prefix}_colors`, `{prefix}_images`, `{prefix}_blocks`, `{prefix}_directory`
3. **Checks privileges**: Verifies access to the catalog/schema and checks if target tables already exist
4. **Asks for conflict mode** (if tables exist): 
   - `[O]`verwrite — drops and recreates all tables
   - `[A]`ppend — adds rows to existing tables
   - `[C]`ancel — aborts
5. **Uploads data**: Transfers rows in batches of 500, with progress bar per model
6. **Summary table**: Shows rows uploaded/failed per model + total duration

### Table Schema

Each table contains:
- **Common fields**: `id`, `folder_id`, `folder_name`, `header_number`, `header_name`, `active`, `created_at`, `modified_at`, `synced_at`, `is_dirty`
- **Full JSON**: `data_json` (STRING) — complete API response, suitable for unpacking nested data in downstream analytics

### Example

```bash
$ python scripts/upload_to_databricks.py

════════════════════════════════════════════════════
  BeProduct  →  Databricks Upload Helper
  (Bulk: Styles, Materials, Colors, Images, Blocks, Directory)
════════════════════════════════════════════════════

✅ Connected as: user@company.com
─  Current catalog: hive_metastore

Local record counts:
  ✓  Styles               (1,234 records)
  ✓  Materials            (  456 records)
  ✓  Colors               (  789 records)
  ✓  Images               (  321 records)
  ✓  Blocks               (   98 records)
  ✓  Directory            (   45 records)

Destination (Unity Catalog):
  Catalog: main
  Schema: beproduct
  Table prefix: bp

Target catalog : main
Target schema  : beproduct
Table prefix   : bp_<model>

── Privilege Check (Bulk Upload) ──────────────────
  ✅ Catalog 'main' is accessible
  ✅ Schema 'main.beproduct' is accessible

  ─  styles       `main`.`beproduct`.`bp_styles` (will be created)
  ─  materials    `main`.`beproduct`.`bp_materials` (will be created)
  ─  colors       `main`.`beproduct`.`bp_colors` (will be created)
  ─  images       `main`.`beproduct`.`bp_images` (will be created)
  ─  blocks       `main`.`beproduct`.`bp_blocks` (will be created)
  ─  directory    `main`.`beproduct`.`bp_directory` (will be created)

  ─  Write access will be verified at transfer start.
     Required privileges: CREATE TABLE, MODIFY (on the schema)

── Schema / Table Setup ──────────────────────────
  Creating schema main.beproduct…
  ✅ Schema created: main.beproduct

── Data Transfer ─────────────────────────────────

  Styles:
    Loaded 1,234 rows (11 columns)
    ✅ Table created
    Uploading in batches of 500…
    ✅ 1,234 rows uploaded

  Materials:
    Loaded 456 rows (11 columns)
    ✅ Table created
    Uploading in batches of 500…
    ✅ 456 rows uploaded

  [... more models ...]

── Summary ───────────────────────────────────────
  Model         Table                 Uploaded  Failed
  ──────────────────────────────────────────────────
  Styles        `main`.`beproduct`.`bp_styles`       1,234      ✓
  Materials     `main`.`beproduct`.`bp_materials`      456      ✓
  Colors        `main`.`beproduct`.`bp_colors`         789      ✓
  Images        `main`.`beproduct`.`bp_images`         321      ✓
  Blocks        `main`.`beproduct`.`bp_blocks`          98      ✓
  Directory     `main`.`beproduct`.`bp_directory`       45      ✓
  ──────────────────────────────────────────────────
  Total                                         2,943      ✓
  Duration: 42.3 seconds

  ✅ All uploads completed successfully!
```

---

## Project Structure

```
beproduct-data-browser/
├── app/
│   ├── config.py               # Settings from .env
│   ├── beproduct_client.py     # SDK singleton + rate-limit header capture
│   ├── db.py                   # SQLite schema and CRUD
│   ├── sync.py                 # Full + incremental sync engine
│   ├── push.py                 # Push-back: local → BeProduct SaaS
│   └── ui/
│       ├── main.py             # Streamlit entrypoint
│       ├── sidebar.py          # Navigation, sync controls, rate-limit widget
│       ├── overview_page.py    # Home / summary dashboard
│       ├── styles_page.py      # Style list + detail + edit
│       ├── materials_page.py   # Material list + detail + edit
│       ├── colors_page.py      # Color palette list + detail + edit
│       └── directory_page.py   # Directory list + detail
├── scripts/
│   ├── get_refresh_token.py          # One-time OAuth token bootstrap helper
│   └── upload_to_databricks.py       # Bulk export: all 6 models to Databricks Delta
├── plans/
│   └── beproduct-data-browser-plan.md
├── data/                       # SQLite database lives here (gitignored)
├── .env.example
├── .gitignore
├── requirements.txt            # App dependencies
├── requirements-scripts.txt    # Helper script dependencies (Databricks)
└── README.md
```

---

## Configuration Reference (`.env`)

| Variable | Required | Description |
|---|---|---|
| `BEPRODUCT_CLIENT_ID` | ✅ | Your API application client ID |
| `BEPRODUCT_CLIENT_SECRET` | ✅ | Your API application client secret |
| `BEPRODUCT_REFRESH_TOKEN` | ✅ | Long-lived OAuth refresh token (from `get_refresh_token.py`) |
| `BEPRODUCT_COMPANY_DOMAIN` | ✅ | Your company domain from URL: `https://us.beproduct.com/YourDomain` |
| `BEPRODUCT_CALLBACK_URL` | ❌ | OAuth callback URL (default: `http://localhost:8765/callback`) |
| `SYNC_INTERVAL_MINUTES` | ❌ | Background sync frequency in minutes (default: `15`) |
| `DB_PATH` | ❌ | SQLite database path (default: `data/beproduct.db`) |

---

## Authentication Deep Dive

BeProduct uses **OAuth 2.0 Authorization Code** flow via `https://id.winks.io`:

```
1. Browser → id.winks.io/authorize?client_id=...  →  User logs in
2. id.winks.io → YOUR_CALLBACK_URL?code=xxxx       ←  Auth code
3. Your app → id.winks.io/token {code, client_id, client_secret}
4. Response: { access_token (8h), refresh_token (never expires) }
```

At **runtime**, the BeProduct Python SDK uses `client_id + client_secret + refresh_token` to silently obtain new access tokens as needed. No user interaction is required after the initial bootstrap.

---

## Data Entities in Scope

| Entity | Read | Write |
|---|---|---|
| Styles | ✅ List + Get | ✅ `attributes_update` |
| Materials | ✅ List + Get | ✅ `attributes_update` |
| Color Palettes | ✅ List + Get | ✅ `attributes_update` |
| Directory | ✅ List + Get | ✅ `directory_add` |

---

## Sync Strategy

- **Full sync**: downloads every record (used on first run or manual "Full Sync" button)
- **Incremental sync**: uses `FolderModifiedAt > last_sync_at` filter, only fetches changed records
- **Background scheduler**: runs incremental sync every N minutes via APScheduler
- **Conflict handling**: records with `is_dirty=1` (locally edited) are not overwritten by remote sync unless the remote version is newer

---

## Rate Limits

The sidebar shows real-time API usage parsed from `X-RateLimit-*` response headers. If headers aren't available in the BeProduct API version you're using, the display shows "not yet available — make an API call first."

---

## Troubleshooting

| Problem | Solution |
|---|---|
| "Required environment variable not set" | Copy `.env.example` to `.env` and fill in all required fields |
| "Import 'beproduct' could not be resolved" | Run `pip install -r requirements.txt` in your venv |
| Browser doesn't open for token flow | Run the auth URL manually from `get_refresh_token.py` output |
| Sync returns 401 Unauthorized | Your refresh token may be revoked — re-run `get_refresh_token.py` |
| DB locked errors | Only run one Streamlit instance at a time per database file |
