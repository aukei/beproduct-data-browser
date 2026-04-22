# BeProduct Data Browser — Agent Knowledge Base

This document captures everything learned through direct testing, Swagger analysis, and
live API exploration. An agent can use this as a complete reference without re-running
any API probes.

---

## Table of Contents

1. [SDK vs REST API — Core Gap Analysis](#1-sdk-vs-rest-api--core-gap-analysis)
2. [raw_api Calling Convention — Critical](#2-raw_api-calling-convention--critical)
3. [DataTable API — Fully Documented](#3-datatable-api--fully-documented)
4. [Entity JSON Shapes (API Response Structures)](#4-entity-json-shapes-api-response-structures)
5. [Field Type Classification (Read-Only vs Editable)](#5-field-type-classification-read-only-vs-editable)
6. [Cross-Entity Relationships (Verified from Live Data)](#6-cross-entity-relationships-verified-from-live-data)
7. [Local SQLite Schema](#7-local-sqlite-schema)
8. [Sync Engine Design](#8-sync-engine-design)
9. [Push / Write-Back Layer](#9-push--write-back-layer)
10. [Known API Quirks & Gotchas](#10-known-api-quirks--gotchas)
11. [UI / Streamlit Layer](#11-ui--streamlit-layer)
12. [SDK Schema Module](#12-sdk-schema-module)

---

## 1. SDK vs REST API — Core Gap Analysis

**SDK version**: `beproduct` Python SDK v0.6.30  
**API spec**: `https://developers.beproduct.com/swagger/v1/swagger.json`  
**Total REST endpoints**: ~262 across 18 tags  
**SDK-wrapped endpoints**: ~150 across 10 handlers

### SDK Resource Handlers

| Handler | Attribute | CRUD Support | Notable Gaps |
|---------|-----------|--------------|--------------|
| Style | `client.style` | Full: create, read, update, delete, colorway_delete | FlatBom, Move, Block Link/Unlink, CBOM, MultiMeasurements, Sets, TextList, ImagesGrid |
| Material | `client.material` | Full: create, read, update, delete, colorway_delete | Move, ColorwaySchema, SKU ops, TextList, ImagesGrid, U3M upload |
| Color | `client.color` | Full: create, read, update, delete | ColorChipSchema, CompanyColors, Attachment ops, Page Reset |
| Image | `client.image` | Full: create, read, update, delete | Attachment ops, Page Reset, Image processing status |
| Block | `client.block` | Full: create, read, update, delete | All Page CRUD (Pages/Page/PageForm/PageGrid/PageList), Attachment ops |
| Directory | `client.directory` | Read + Upsert (`directory_add`) | **No delete**, **No update** (directory_add is the only write — it upserts by directoryId) |
| Users | `client.user` | Read + Create + Update | **No delete** |
| Tracking | `client.tracking` | Partial read | Plan Style/Material Add, Delete, Progress, Revisions, Archive |
| Tag | via entity mixins | Full | Folder-scoped tag CRUD via `_common_tags` mixin |
| Schema | `client.schema` | Read only | Convenience schema fetch for Style/Material/Color only |

### API Groups with No SDK Wrapper

These must be called via `client.raw_api`:

| Group | Endpoints | Status in App |
|-------|-----------|---------------|
| **DataTable** | 5 | **Implemented** — sync, list, row edit |
| **Inbox** | 10 | Not implemented |
| **MasterData** | 5 | Not implemented |
| **LineSheet** | 4 | Not implemented |
| **Report** | 4 | Not implemented |
| **RateLimit** | 1 | Handled via response headers instead |
| **Info** | 1 | Not implemented |
| **Request** | 1 | Not implemented |

---

## 2. raw_api Calling Convention — Critical

The `client.raw_api` class (`beproduct._raw_api.RawApi`) wraps `requests.post/get`.

### How `raw_api.post()` Works

```python
def post(self, url, body, **kwargs):
    full_url = f"{client.public_api_url}/{url.lstrip('/')}"
    # **kwargs are appended as URL query parameters
    if kwargs:
        full_url += "?" + "&".join(f"{k}={v}" for k, v in kwargs.items())
    response = requests.post(url=full_url, json=body, headers=...)
    return response.json()
```

### Critical Rule: Pagination Goes in `**kwargs`, NOT `body`

Many endpoints accept `pageSize` and `pageNumber` as **URL query parameters**, not body
fields. Putting them in the body causes them to be silently ignored.

**Wrong** (pagination ignored, defaults to ~0 results):
```python
client.raw_api.post("DataTable/List", body={"filters": [], "pageSize": 100})
```

**Correct**:
```python
client.raw_api.post("DataTable/List", body={"filters": []}, pageSize=100, pageNumber=0)
```

This was the root cause of DataTable/List returning `total: 2` but `result: []`.

### How `raw_api.get()` Works

```python
def get(self, url, **kwargs):
    full_url = f"{client.public_api_url}/{url.lstrip('/')}"
    # kwargs become query params
    response = requests.get(url=full_url, headers=..., params=kwargs)
    return response.json()
```

### Base URL Construction

The SDK builds the base URL as:
```
https://developers.beproduct.com/api/{company_domain}/
```

So `client.raw_api.post("DataTable/List", ...)` resolves to:
```
POST https://developers.beproduct.com/api/lifung/DataTable/List
```

You do **not** include the company domain in the URL path — the SDK injects it.

### Authentication

`raw_api` automatically injects `Authorization: Bearer {access_token}` headers.
Token refresh is handled transparently by the OAuth2 client. No manual token management needed.

---

## 3. DataTable API — Fully Documented

The BeProduct Python SDK has **no wrapper** for DataTables. All operations use `raw_api`.

### Endpoints

| Method | Path | Query Params | Body Schema | Response |
|--------|------|-------------|-------------|----------|
| POST | `DataTable/List` | `pageSize`, `pageNumber` | `RawSearch` | `PageResult<DataTableResult>` |
| GET | `DataTable/{id}/Schema` | — | — | `SchemaField[]` |
| POST | `DataTable/{id}/Data` | `pageSize`, `pageNumber` | `RawSearch` | `PageResult<DataTableRowResult>` |
| POST | `DataTable/{id}/Update` | — | `DataTableRowUpdateRequest[]` | `DataTableRowsUpdateResult` |
| POST | `DataTable/{id}/Reset` | — | — | `MessageResponse` |

### Request Bodies

**RawSearch** (used by List and Data):
```json
{
  "filters": [
    {
      "field": "name",
      "operator": "Contains",
      "value": "fabric"
    }
  ]
}
```
`operator` enum: `Any`, `Contains`, `Eq`, `Lt`, `Lte`, `Ne`, `Neq`, `Gt`, `Gte`, `ElemMatch`

Pass empty filters to return all: `{"filters": []}`

**DataTableRowUpdateRequest** (array — used by Update):
```json
[
  {
    "rowId": null,              // null = insert new row; UUID = update existing
    "rowFields": [
      {"id": "field_id", "value": "new_value"}
    ],
    "deleteRow": false          // true = delete this row
  }
]
```

### Response Shapes

**PageResult** (both List and Data responses):
```json
{
  "result": [...],   // the items array — key is "result", NOT "items"
  "total": 2         // total count of matching records (not items in this page)
}
```

**DataTableResult** (table definition):
```json
{
  "id": "uuid",
  "name": "E1 Color Codes",
  "description": "...",
  "active": true,
  "createdBy": {"id": "uuid", "name": "User Name"},
  "createdAt": "2024-01-01T00:00:00Z",
  "modifiedBy": {"id": "uuid", "name": "User Name"},
  "modifiedAt": "2024-01-01T00:00:00Z"
}
```

**DataTableRowResult** (a row of data):
```json
{
  "id": "uuid",
  "fields": [
    {"id": "e1_color_code", "name": "E1 COLOR CODE", "type": "Text", "value": "001"}
  ],
  "createdBy": {"id": "uuid", "name": "User Name"},
  "createdAt": "...",
  "modifiedBy": {"id": "uuid", "name": "User Name"},
  "modifiedAt": "..."
}
```

**DataTableRowsUpdateResult** (response from Update):
```json
{
  "updated": ["uuid", ...],
  "added": ["uuid", ...],
  "deleted": ["uuid", ...]
}
```

### Working Code Examples

```python
from app.beproduct_client import get_client
client = get_client()

# List all data tables — pageSize/pageNumber are QUERY PARAMS not body
tables_resp = client.raw_api.post("DataTable/List", body={"filters": []}, pageSize=1000, pageNumber=0)
tables = tables_resp.get("result", [])  # key is "result" not "items"
total = tables_resp.get("total", 0)

# Get schema for a table
schema = client.raw_api.get(f"DataTable/{table_id}/Schema")
# returns: [{"id": "col_id", "name": "Column Name", "type": "Text", ...}]

# Get rows from a data table
rows_resp = client.raw_api.post(f"DataTable/{table_id}/Data", body={"filters": []}, pageSize=5000, pageNumber=0)
rows = rows_resp.get("result", [])

# Add a new row (rowId=null)
result = client.raw_api.post(f"DataTable/{table_id}/Update", body=[
    {"rowId": None, "rowFields": [{"id": "col_id", "value": "val"}], "deleteRow": False}
])
new_id = result.get("added", [None])[0]

# Update an existing row
result = client.raw_api.post(f"DataTable/{table_id}/Update", body=[
    {"rowId": "existing-uuid", "rowFields": [{"id": "col_id", "value": "new_val"}], "deleteRow": False}
])

# Delete a row
result = client.raw_api.post(f"DataTable/{table_id}/Update", body=[
    {"rowId": "existing-uuid", "rowFields": [], "deleteRow": True}
])
```

---

## 4. Entity JSON Shapes (API Response Structures)

### Common Pattern: Style, Material, Image, Block

All four share the same top-level structure:

```json
{
  "id": "uuid",
  "headerNumber": "STYLE-001",
  "headerName": "My Style",
  "folder": {"id": "uuid", "name": "Folder Name"},
  "createdBy": {"id": "uuid", "name": "User"},
  "createdAt": "ISO datetime",
  "modifiedBy": {"id": "uuid", "name": "User"},
  "modifiedAt": "ISO datetime",
  "headerData": {
    "fields": [
      {"id": "field_id", "name": "Field Label", "type": "Text", "value": "...", "required": false}
    ]
  },
  "colorways": [
    {
      "id": "uuid",
      "colorNumber": "001",
      "colorName": "Red",
      "primaryColor": "#FF0000",
      "secondaryColor": "#000000",
      "colorSourceId": "uuid-of-color-palette-color",
      "imageHeaderId": "uuid-of-image-record",
      "hideColorway": false
    }
  ],
  "sizeRange": [
    {
      "name": "S",
      "price": 25.00,
      "currency": "USD",
      "unitOfMeasure": "each",
      "isSampleSize": false,
      "sizeIndex": 0,
      "comments": ""
    }
  ]
}
```

### Color Palette Differences

Color palettes use **different top-level field names**:

```json
{
  "id": "uuid",
  "colorPaletteNumber": "PAL-001",   // NOT headerNumber
  "colorPaletteName": "Spring 2025",  // NOT headerName
  "folder": {...},
  "headerData": {
    "fields": [...],
    "colors": {
      "colors": [
        {
          "color_source_id": "uuid",    // Referenced by colorway.colorSourceId
          "color_number": "001",
          "color_name": "Red",
          "hex": "FF0000",              // No leading #
          "Schema": {"e1_color_code": null}  // Extension fields from Data Tables
        }
      ]
    }
  }
}
```

**In db.py**, colors are stored with `header_number` mapped from `colorPaletteNumber`
and `header_name` from `colorPaletteName`. When updating, use `colorPaletteName` not
`headerName`.

### Directory Record

```json
{
  "id": "uuid",
  "directoryId": "VEN-001",      // User-assigned human-readable ID
  "name": "Vendor Name",
  "partnerType": "VENDOR",       // VENDOR, FACTORY, AGENT, RETAILER, SUPPLIER, OTHER
  "country": "US",
  "active": true,
  "address": "123 Main St",
  "city": "New York",
  "state": "NY",
  "zip": "10001",
  "phone": "555-1234",
  "website": "https://...",
  "contacts": [
    {
      "firstName": "Jane",
      "lastName": "Doe",
      "email": "jane@vendor.com",
      "title": "Sales Manager",
      "mobilePhone": "...",
      "workPhone": "...",
      "role": "Primary"
    }
  ]
}
```

**Note**: `fax` field appeared in old API docs but is **no longer returned** by the API.

### User Record

```json
{
  "id": "uuid",
  "email": "user@company.com",
  "username": "jdoe",
  "firstName": "Jane",
  "lastName": "Doe",
  "title": "Designer",
  "accountType": "PRIVATE",
  "role": "User",
  "registerdOn": "ISO datetime",   // NOTE: typo in API — "registerd" not "registered"
  "active": true
}
```

**Known API typo**: the field is `registerdOn` (missing 'e'), not `registeredOn`.
The `db.py` `upsert_user()` intentionally uses `record.get("registerdOn")`.

### Block Extras

Blocks additionally have `sizeClasses` inside `headerData`:

```json
{
  "headerData": {
    "fields": [...],
    "sizeClasses": [
      {
        "id": "uuid",            // Same UUID as in Style.sizeRange if Style inherits this Block
        "name": "XS-XXL (S)",
        "active": true,
        "sizeRange": "XS-XXL",
        "sizes": [
          {"name": "XS", "price": null, "currency": "USD", "isSampleSize": false}
        ]
      }
    ],
    "frontImage": {
      "preview": "https://..."   // URL to front image thumbnail
    }
  }
}
```

---

## 5. Field Type Classification (Read-Only vs Editable)

Every field in `headerData.fields[]` has this shape:
```python
{"id": "field_id", "name": "Display Label", "type": "FieldType", "value": ..., "required": bool}
```

There is **no `readOnly` property** in the field data. Read-only status is determined entirely
by the field `type` or field `id`.

### Always Read-Only (render as disabled input)

```python
READONLY_FIELD_TYPES = frozenset({
    "UserLabel",        # created_by, modified_by — audit trail
    "LabelText",        # season_year, factory_id_no — computed display values
    "LabelMaterial",    # core_main_material — auto-populated from BOM
    "LabelSize",        # core_size_range — auto-populated from size range
    "LabelStyleGroup",  # core_style_group — auto-populated
    "Label3dStyle",     # core_3d_style — auto-populated from 3D linking
    "Label3dMaterial",  # core_3d_material — auto-populated from 3D linking
    "FormulaField",     # Calculated field — server-computed
    "Auto",             # Auto-generated value
})

READONLY_FIELD_IDS = frozenset({
    "created_by", "modified_by", "version"
})
```

### Editable Field Types and Streamlit Widgets

| Field Type | Widget | Notes |
|---|---|---|
| `Text` | `st.text_input` | Default for unknown types too |
| `Memo` | `st.text_area` | Multi-line |
| `TrueFalse` | `st.checkbox` | Value is `"Yes"`/`"No"` string, not bool |
| `DropDown` | `st.selectbox` | Options from folder schema `possible_values` |
| `MultiSelect` | `st.multiselect` | Options from folder schema |
| `ComboBox` | `st.selectbox` + custom text fallback | Dropdown with free text option |
| `PartnerDropDown` | `st.selectbox` | Options from `db.get_directory_records()` |
| `Users` | `st.selectbox` | Options from `db.get_users()` |
| `Date` | `st.date_input` | Value is ISO date string |
| `DateTime` | `st.date_input` + `st.time_input` | Value is ISO datetime string |
| `Number` | `st.number_input(step=1)` | Returns int |
| `Decimal`, `Currency`, `Percent`, `Weight`, `Measure` | `st.number_input(step=0.01)` | Returns float |
| `CompositeControl` | Custom: paired `{code, value}` inputs | e.g., fabric content |

### PartnerDropDown Value Shape

When a PartnerDropDown is populated, its `value` is:
```python
{"code": "uuid-of-directory-record", "value": "Vendor Display Name"}
```
When empty: `""` or `None`.

### Users Field Value Shape

When a Users field is populated:
```python
{"id": "uuid-of-user", "name": "Jane Doe"}
```

---

## 6. Cross-Entity Relationships (Verified from Live Data)

All relationships were confirmed against actual live data across 13 styles, 15 materials,
6 colors, 4 images, 5 blocks, 5 directory records, 14 users.

### Confirmed Foreign Keys

| Source | Field | Target | Match Rate | Notes |
|--------|-------|--------|---------|-------|
| Style/Material `colorways[]` | `colorSourceId` (UUID) | Color palette `headerData.colors.colors[].color_source_id` | 12/15 colorways matched | Same color referenced by both Style AND Material |
| Style/Material `colorways[]` | `imageHeaderId` (UUID) | Image `id` | Confirmed | Image UUIDs appear in colorways |
| Style/Material `headerData.fields[]` | `PartnerDropDown.value.code` (UUID) | Directory `id` | Confirmed | Factory, vendor fields reference directory |
| Style/Material `headerData.fields[]` | `Users` type field | User `id` | Confirmed | tech_designer, designer reference users |
| All entities | `createdBy.id` / `modifiedBy.id` | User `id` | 10/10 | Audit trail |

### Confirmed Non-Relationships

| Hypothesized | Finding |
|---|---|
| Block → Material | **No FK found.** Block JSON has no colorways, no supplier fields, no material references. |
| Material → Block | **No FK found.** Material JSON has no block references. |
| Style → Block (FK) | Field `reference_style_no` is **plain text** (e.g., "5342"), NOT a UUID. It's a human label. |

### Block → Style: Indirect Relationship via Shared sizeClass UUIDs

When a Style is created referencing a Block as size template, the Style **copies** the
Block's `sizeClasses` by inheriting the same UUIDs. After creation, the linkage is
UUID equality — if the Block's sizeClass UUID matches a Style's sizeRange entry, they
were created from the same template.

This is a **copy-on-create** relationship, not an ongoing enforced FK.

### Color `colorSourceId` Lookup Path

To resolve a colorway's color source:
1. Colorway has `colorSourceId: "abc-uuid"`
2. Find the Color palette where `headerData.colors.colors[].color_source_id == "abc-uuid"`
3. That individual color chip in the palette is the referenced color

The Color palette's top-level `id` is different from `color_source_id` of individual chips.

### Data Table → Color Connection

Individual color chips have a `Schema` field whose keys map to Data Table columns:
```json
{"Schema": {"e1_color_code": null, "el_color_name": null}}
```
The "E1 Color Codes" Data Table provides lookup values for these extension fields.
Empty (`null`) means no value has been set yet.

---

## 7. Local SQLite Schema

Database path: `data/beproduct.db` (configured via `DB_PATH` env var)  
Connection: WAL mode, foreign keys ON, `sqlite3.Row` factory

### Entity Tables (styles, materials, colors, images, blocks)

All 5 share the same structure:

```sql
CREATE TABLE IF NOT EXISTS {entity} (
    id            TEXT PRIMARY KEY,   -- BeProduct UUID
    folder_id     TEXT,               -- folder.id from API response
    folder_name   TEXT,               -- folder.name from API response
    header_number TEXT,               -- headerNumber (colorPaletteNumber for colors)
    header_name   TEXT,               -- headerName (colorPaletteName for colors)
    active        INTEGER,            -- 0 or 1, extracted from headerData.fields[id=active]
    created_at    TEXT,               -- ISO datetime from API createdAt
    modified_at   TEXT,               -- ISO datetime from API modifiedAt
    synced_at     TEXT,               -- ISO datetime of last local sync
    is_dirty      INTEGER DEFAULT 0,  -- 1 = locally edited, not yet pushed
    data_json     TEXT NOT NULL       -- Complete API response JSON blob
);
CREATE INDEX IF NOT EXISTS idx_{entity}_folder   ON {entity}(folder_id);
CREATE INDEX IF NOT EXISTS idx_{entity}_modified ON {entity}(modified_at);
CREATE INDEX IF NOT EXISTS idx_{entity}_dirty    ON {entity}(is_dirty);
```

**Dirty tracking**: `is_dirty=1` means the record has local edits not yet pushed to BeProduct.
Sync will not overwrite a dirty record unless the remote `modifiedAt` is strictly newer.

**Color palette quirk**: `header_number` is sourced from `colorPaletteNumber` (not `headerNumber`)
and `header_name` from `colorPaletteName`. The `update_color_local()` function must use
`colorPaletteName` when updating the name.

### Users Table

```sql
CREATE TABLE IF NOT EXISTS users (
    id            TEXT PRIMARY KEY,
    email         TEXT,
    username      TEXT,
    first_name    TEXT,   -- API field: firstName
    last_name     TEXT,   -- API field: lastName
    title         TEXT,
    account_type  TEXT,   -- API field: accountType
    role          TEXT,
    registered_on TEXT,   -- API field: registerdOn (NOTE: typo in API)
    active        INTEGER,
    synced_at     TEXT,
    data_json     TEXT NOT NULL
);
```

No `is_dirty` — users have no local edit workflow (read from API, can create, cannot delete).

### Directory Table

```sql
CREATE TABLE IF NOT EXISTS directory (
    id            TEXT PRIMARY KEY,
    directory_id  TEXT,               -- API field: directoryId (user-assigned, e.g. "VEN-001")
    name          TEXT,
    partner_type  TEXT,               -- VENDOR, FACTORY, AGENT, RETAILER, SUPPLIER, OTHER
    country       TEXT,
    active        INTEGER,
    modified_at   TEXT,
    synced_at     TEXT,
    is_dirty      INTEGER DEFAULT 0,
    data_json     TEXT NOT NULL
);
```

Note: Directory has `is_dirty` for tracking push-back intent, but the SDK only supports
upsert (not update), so push is implemented via `directory_add()`.

### Data Tables

```sql
CREATE TABLE IF NOT EXISTS data_tables (
    id            TEXT PRIMARY KEY,   -- BeProduct UUID
    name          TEXT,               -- e.g. "E1 Color Codes"
    description   TEXT,
    active        INTEGER DEFAULT 1,
    created_at    TEXT,
    modified_at   TEXT,
    synced_at     TEXT,
    data_json     TEXT
);

CREATE TABLE IF NOT EXISTS data_table_rows (
    id              TEXT PRIMARY KEY,     -- Row UUID from BeProduct
    data_table_id   TEXT NOT NULL,        -- FK to data_tables.id
    created_at      TEXT,
    modified_at     TEXT,
    synced_at       TEXT,
    is_dirty        INTEGER DEFAULT 0,    -- 1 = locally edited
    data_json       TEXT,                 -- Full DataTableRowResult JSON
    FOREIGN KEY (data_table_id) REFERENCES data_tables(id)
);
CREATE INDEX IF NOT EXISTS idx_dt_rows_table ON data_table_rows(data_table_id);
CREATE INDEX IF NOT EXISTS idx_dt_rows_dirty ON data_table_rows(is_dirty);
```

Row data is stored as `data_json` containing the full `DataTableRowResult` shape.
Fields are accessed via `json.loads(row["data_json"])["fields"]`.

### Sync Metadata

```sql
CREATE TABLE IF NOT EXISTS sync_meta (
    entity       TEXT PRIMARY KEY,   -- "styles", "materials", "colors", etc.
    last_sync_at TEXT,               -- ISO datetime of last successful sync
    sync_type    TEXT                -- "full" or "incremental"
);
```

Used by the incremental sync engine to determine the `FolderModifiedAt` filter cutoff.

### Rate Limit Log

```sql
CREATE TABLE IF NOT EXISTS rate_limit_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity          TEXT,
    timestamp       TEXT,
    requests_used   INTEGER,
    requests_limit  INTEGER,
    reset_at        TEXT
);
```

### db.py Key Functions

| Function | Purpose |
|---|---|
| `init_schema()` | Create all tables (idempotent). Call on app startup. |
| `get_conn()` | Context manager — yields WAL-mode SQLite connection with auto commit/rollback |
| `upsert_{entity}(record)` | Insert/update from API response. Skips dirty records unless remote is newer. |
| `get_{entity}(record_id)` | Fetch single record by UUID |
| `get_{entities}(folder_id, search, limit, dirty_only)` | List records with filters |
| `update_{entity}_local(id, json)` | Set `is_dirty=1` with new JSON |
| `mark_{entity}_clean(id)` | Set `is_dirty=0` after successful push |
| `delete_{entity}(id)` | Hard remove from local DB (after API delete) |
| `get_colorways_referencing_color(color_source_id)` | Cross-ref: find styles/materials using a color |
| `get_colorways_referencing_image(image_header_id)` | Cross-ref: find styles/materials using an image |
| `get_entities_by_partner(directory_id)` | Cross-ref: find styles/materials using a directory partner |
| `get_row_counts()` | Count all entities for the sidebar status panel |
| `get_sync_meta(entity)` / `set_sync_meta(entity)` | Track last sync timestamps |

---

## 8. Sync Engine Design

File: `app/sync.py`

### Sync Strategy per Entity

| Entity | Incremental Filter | Full Sync |
|--------|-------------------|-----------|
| Styles | `FolderModifiedAt > last_sync_at` passed as query param | All folders, no filter |
| Materials | Same | Same |
| Colors | Same | Same |
| Images | Same | Same |
| Blocks | Same | Same |
| Users | Always full (no incremental filter available) | Full list |
| Directory | Always full (no incremental filter available) | Full list |
| Data Tables | Always full (via `raw_api`) | `pageSize=1000, pageNumber=0` as query params |

### Sync Flow

1. `sync_all(force_full=False)` calls all entity sync functions
2. Each sync function acquires a per-entity `threading.Lock()`
3. Results are a `dict[str, tuple[bool, str]]` — `{entity: (success, message)}`
4. Sync status is written to a JSON status file (not memory) so background threads
   can communicate completion to the Streamlit frontend
5. The sidebar polls the status file every 2 seconds via `time.sleep(2) + st.rerun()`

### Pagination in sync.py

Standard entity sync (styles, materials, etc.) uses SDK methods that handle pagination
internally. For `raw_api` calls (DataTable), pagination must be done manually:

```python
# DataTable sync — correct pattern
tables_resp = client.raw_api.post(
    "DataTable/List",
    body={"filters": []},
    pageSize=1000,      # query param
    pageNumber=0,       # query param
)
tables = tables_resp.get("result", [])  # key is "result" not "items"
```

---

## 9. Push / Write-Back Layer

File: `app/push.py`

### SDK Method Signatures

```python
# Style / Material / Color / Image / Block
client.{entity}.attributes_create(folder_id, fields, ...)  # Returns new record dict
client.{entity}.attributes_update(header_id, fields, ...)  # Returns updated record dict
client.{entity}.attributes_delete(header_id)               # Returns nothing

# Directory (upsert only — no update, no delete)
client.directory.directory_add(fields)  # Creates or updates by directoryId

# Users
client.user.user_create(fields)         # Returns new user dict
client.user.user_update(user_id, fields)
# No user delete

# DataTable rows (via raw_api)
client.raw_api.post(f"DataTable/{table_id}/Update", body=[...])
```

### Push Function Signatures in push.py

All push functions return `tuple[bool, str]` or `tuple[bool, str, Optional[str]]` for
create functions (the third element is the newly created record ID).

```python
push_style(record_id: str) -> tuple[bool, str]
push_material(record_id: str) -> tuple[bool, str]
push_color(record_id: str) -> tuple[bool, str]
push_image(record_id: str) -> tuple[bool, str]
push_block(record_id: str) -> tuple[bool, str]
push_directory(record_id: str) -> tuple[bool, str]

create_style(folder_id: str, fields: dict) -> tuple[bool, str, Optional[str]]
create_material(folder_id: str, fields: dict) -> tuple[bool, str, Optional[str]]
create_color(folder_id: str, fields: dict) -> tuple[bool, str, Optional[str]]
create_image(folder_id: str, fields: dict) -> tuple[bool, str, Optional[str]]
create_block(folder_id: str, fields: dict) -> tuple[bool, str, Optional[str]]
create_directory_entry(fields: dict) -> tuple[bool, str, Optional[str]]
create_user(fields: dict) -> tuple[bool, str, Optional[str]]

delete_style(record_id: str) -> tuple[bool, str]
delete_material(record_id: str) -> tuple[bool, str]
delete_color(record_id: str) -> tuple[bool, str]
delete_image(record_id: str) -> tuple[bool, str]
delete_block(record_id: str) -> tuple[bool, str]

push_data_table_row(table_id, row_id, row_fields) -> tuple[bool, str]
add_data_table_row(table_id, row_fields) -> tuple[bool, str, Optional[str]]
delete_data_table_row(table_id, row_id) -> tuple[bool, str]
```

### How Attributes Update Works

The SDK's `attributes_update()` takes a `fields` dict mapping `field_id → value`.
The helper in push.py converts the `headerData.fields` list to this dict:

```python
fields_dict = {f["id"]: f["value"] for f in edited_fields}
client.style.attributes_update(header_id=record_id, fields=fields_dict)
```

After a successful push, the record is re-fetched from the API and upserted locally
to ensure the local DB reflects the server's canonical state (including auto-computed fields).

---

## 10. Known API Quirks & Gotchas

### API Typo: `registerdOn`

The Users API response has a field named `registerdOn` (missing 'e'). This is a server-side
typo that has been live for years and will not be fixed. `db.py` intentionally maps it:

```python
record.get("registerdOn")   # correct spelling in code, wrong in API
```

### Color Palette Display Keys

Colors use `colorPaletteNumber` and `colorPaletteName` instead of `headerNumber` and
`headerName`. Any code reading colors from the API must account for both possible key names:

```python
header_number = record.get("colorPaletteNumber") or record.get("headerNumber")
header_name   = record.get("colorPaletteName") or record.get("headerName")
```

### `active` is Inside `headerData.fields`, Not Top-Level

For Style, Material, Color, Image, Block: the `active` value is **not** a top-level
key in the API response. It lives inside `headerData.fields` as a `TrueFalse` field:

```python
def _extract_active_from_fields(record):
    for f in record.get("headerData", {}).get("fields", []):
        if f.get("id") == "active":
            return 1 if str(f.get("value", "")).lower() in ("yes", "true", "1") else 0
    return 0
```

### Directory Has No `is_dirty` Skip Logic

Unlike style/material sync which skips dirty records, directory sync always overwrites
since directory records cannot be locally edited (only pushed via upsert).

### DataTable `result` vs `items`

The DataTable API (and all BeProduct paginated endpoints) return `{"result": [...], "total": N}`.
The key is `result`, **not** `items`. Early code that used `.get("items", [])` returned
empty lists even when data existed.

### Pages via `raw_api` — URL Format

When using `raw_api.post()` with pagination:
```python
# CORRECT — pageSize and pageNumber are kwargs, not body fields
client.raw_api.post("DataTable/List", body={"filters": []}, pageSize=1000, pageNumber=0)

# WRONG — body can only contain schema-defined fields for that endpoint
client.raw_api.post("DataTable/List", body={"filters": [], "pageSize": 1000})
```

### Rate Limit Headers

The rate limit is tracked via monkey-patched `requests.get/post` in `beproduct_client.py`.
Standard header names tried (in order):
- `X-RateLimit-Limit`, `RateLimit-Limit`, `X-Ratelimit-Limit`, `Ratelimit-Limit`

### Streamlit Hot-Reload and Module Caching

If you add new functions to `push.py`, pages that imported them at module level will
get `ImportError` until Streamlit is restarted (Python caches the old module object).

**Pattern to avoid this**: Use lazy imports inside functions that need them:
```python
# In page files — import inside the handler, not at module level
if st.button("Create"):
    from app.push import create_style   # lazy — always picks up latest module
    ...
```

All three new pages (`users_page.py`, `directory_page.py`, `data_tables_page.py`) use
lazy imports for this reason.

### Sync Status Auto-Poll

The sidebar uses `time.sleep(2) + st.rerun()` to auto-poll sync completion.
While sync is running, Streamlit renders "Sync in progress", sleeps 2 seconds,
then reruns to check the status file. This blocks Streamlit's render thread for 2s
per poll cycle — acceptable for sync UX, but don't use this pattern elsewhere.

---

## 11. UI / Streamlit Layer

### Page Routing (app/ui/main.py)

Navigation is a `st.radio` in the sidebar. Pages are imported lazily via `if/elif` blocks.

| Sidebar Label | Module | Render Function |
|---|---|---|
| 🏠 Overview | `app.ui.overview_page` | `render_overview_page()` |
| 👗 Styles | `app.ui.styles_page` | `render_styles_page()` |
| 🧵 Materials | `app.ui.materials_page` | `render_materials_page()` |
| 🎨 Colors | `app.ui.colors_page` | `render_colors_page()` |
| 🖼️ Images | `app.ui.images_page` | `render_images_page()` |
| 🧱 Blocks | `app.ui.blocks_page` | `render_blocks_page()` |
| 📒 Directory | `app.ui.directory_page` | `render_directory_page()` |
| 👤 Users | `app.ui.users_page` | `render_users_page()` |
| 📊 Data Tables | `app.ui.data_tables_page` | `render_data_tables_page()` |

### Session State Navigation Pattern

Each entity page uses `st.session_state["{entity}_selected_id"]` to navigate between
list and detail views. Setting this key then calling `st.rerun()` opens the detail view.
Removing it returns to the list.

```python
# Navigate to detail
st.session_state["style_selected_id"] = "some-uuid"
st.rerun()

# Navigate back to list
st.session_state.pop("style_selected_id", None)
st.rerun()
```

### Shared UI Components

| File | Purpose |
|---|---|
| `app/ui/_field_editor.py` | `render_field(field, key_prefix, schema, users_list, directory_list)` → updated field dict. `render_field_form(fields, form_key, ...)` → `(updated_fields, submit_clicked)` |
| `app/ui/_create_dialog.py` | `show_create_entity_dialog(entity_type, on_create_callback, ...)` — shown via `st.session_state["show_create_{entity}"] = True` |
| `app/ui/_delete_dialog.py` | `show_delete_confirmation_dialog(entity_type, record_id, display_name, on_delete_callback, referential_impacts)` |

### List Page Pattern (consistent across all entities)

1. Create button → sets `show_create_{entity} = True`
2. If `show_create` flag set → call `show_create_entity_dialog()` → clear flag
3. Filter row: search text + folder selectbox + dirty-only checkbox
4. `st.dataframe(on_select="rerun")` — left column
5. Right column: JSON preview + "Edit/View" button on selection
6. "Edit/View" button → set `{entity}_selected_id` + `st.rerun()`

### Detail Page Pattern

1. Back button → clear selected_id + `st.rerun()`
2. Delete button → set `show_delete_{entity} = True`
3. If delete flag → call `show_delete_confirmation_dialog()` → clear flag
4. Status metrics (folder, active, modified_at)
5. `render_field_form()` inside a `with st.form()` block
6. "Save Locally" → `db.update_{entity}_local()` + `st.rerun()`
7. "Push to BeProduct" → `push_{entity}()` + show success/error + `st.rerun()`
8. Cross-reference sections (colorways, sizes, size classes, preview images)
9. Raw JSON expander

---

## 12. SDK Schema Module

`client.schema.get_folder_schema(master_folder, folder_id)` returns:

```python
[
    {
        "field_id": "season",
        "field_name": "SEASON",
        "field_type": "DropDown",
        "required": False,
        "formula": "",
        "possible_values": [
            {"id": "Spring", "code": "Spring", "value": "Spring"},
            ...
        ],
        "data_type": str,       # Python type
        "properties": {...},    # Raw API properties
    },
    ...
]
```

Convenience methods (calling `get_folder_schema` internally):
- `client.schema.get_style_folder_schema(folder_id)`
- `client.schema.get_material_folder_schema(folder_id)`
- `client.schema.get_color_folder_schema(folder_id)`

**No convenience for Block or Image** — use `client.schema.get_folder_schema("Block", folder_id)`
and `client.schema.get_folder_schema("Image", folder_id)` directly.

### Schema Caching

Folder schemas rarely change. The detail pages currently fetch the schema fresh on each
render (via a try/except that silently ignores failures). For production use, cache with:

```python
@st.cache_data(ttl=600)
def get_cached_folder_schema(master_folder: str, folder_id: str) -> dict:
    client = get_client()
    schema_list = client.schema.get_folder_schema(master_folder, folder_id)
    return {s["field_id"]: s for s in schema_list}
```

The `schema_dict` parameter in `render_field_form()` and `render_field()` accepts this
format (field_id → schema dict).

### Schema Data for DropDown/MultiSelect

When schema is available, the `_field_editor.py` renders proper selectboxes.
When schema is NOT available (schema fetch failed), all DropDown/MultiSelect fields
fall back to plain `st.text_input` with a help note saying to sync the schema.

The schema `possible_values` list has this shape:
```python
[{"id": "Spring", "code": "Spring", "value": "Spring"}, ...]
```
Extract display values with: `pv.get("value", pv.get("id", ""))`.

---

### Required Fields Per Folder (Confirmed from Live Schema)

The `required` flag is **folder-level configuration** set by the BeProduct admin,
not enforced at the API level. The create dialog must always call
`client.schema.get_folder_schema()` after folder selection and render every field
where `s.get("required") == True`.

**Do not hardcode a fixed field list for create forms.**

Confirmed live results (all folders, all entity types):

| Entity | Folder | Required fields |
|--------|--------|-----------------|
| Style | Apparel | `header_number`, `header_name`, `year` DropDown, `season` DropDown, `team` DropDown |
| Style | LFMU Licensed Brands | same as Apparel |
| Style | LFMU Private Labels | same as Apparel |
| Style | LFMU Walmart | same as Apparel |
| Style | Templates | same as Apparel |
| Material | 01 Knits | `header_number`, `header_name`, `material_type` DropDown, `material_category` DropDown |
| Material | 02 Wovens – 08 Stitches | same as 01 Knits |
| Material | ZZ ARTWORKS | same + `customer_retailer` DropDown |
| Color | Apparel | `header_number`, `header_name`, `year`, `season`, `palette_type` DropDown, `team` DropDown |
| Color | LFMU * folders | same as Apparel |
| Color | 00 CSI WALMART SEASONAL PALETTES | `header_number`, `header_name`, `year`, `season`, `palette_type` (no `team`) |
| Image | all folders | `header_number`, `header_name` only |
| Block | all folders | `header_number`, `header_name` only |
| Directory | N/A (no folder) | `directoryId`, `name`, `partnerType` |
| User | N/A (no folder) | `email`, `username`, `firstName`, `lastName` |

The `_create_dialog.py` implementation fetches schema dynamically after folder
selection via `_get_schema_fields(entity_lower, folder_id)`, so new required
fields added in the portal are automatically included without code changes.

---

*Last updated: 2026-04-22. Generated from live API testing and Swagger analysis.*
*Swagger spec version: v1 (2026.3.31.1001)*
