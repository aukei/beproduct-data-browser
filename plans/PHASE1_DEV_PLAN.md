# Phase 1 Development Plan — Revised

**Based on clarifications from 2026-06-05**  
**Goal**: Sync BeProduct Styles → DTC WIP Requests  
**Customer**: KTB (unified workspace)  
**Environments**: UAT then PRD  
**Deploy target**: Databricks scheduled job  

---

## Architecture Overview

```
 ┌──────────────────────────────────────────────────────────┐
 │               sync_hub/python/connectors/                │
 │  base.py ──── AppConnector(ABC)                          │
 │  dtc.py  ──── DTCConnector(AppConnector)                 │
 │  beproduct.py ─ BeProductConnector(AppConnector)         │
 └──────────────────────────────────────────────────────────┘
 ┌──────────────────────────────────────────────────────────┐
 │               sync_hub/python/client/                    │
 │  rest_client.py ── Generic HTTP client (exists)          │
 │  auth.py ────────── Auth helpers (new, extracted)        │
 └──────────────────────────────────────────────────────────┘
 ┌──────────────────────────────────────────────────────────┐
 │               sync_hub/python/diffing/                   │
 │  snapshot.py ──── Snapshot manage (exists, refactor)     │
 │  hasher.py ────── Row hashing (extract from snapshot)    │
 │  detector.py ──── Change detection (refactor to          │
 │                   composite key + timestamp compare)     │
 └──────────────────────────────────────────────────────────┘
 ┌──────────────────────────────────────────────────────────┐
 │               sync_hub/python/mapping/                   │
 │  fields.py ────── 14-field BeProduct ↔ DTC mapping      │
 │  season.py ────── SeasonCode resolution (SS/FW lookup)   │
 │  timestamps.py ── Timestamp conversion (+0800 HKT)      │
 └──────────────────────────────────────────────────────────┘
 ┌──────────────────────────────────────────────────────────┐
 │               sync_hub/notebooks/                        │
 │  00_init/                                                │
 │   ├── init_delta_schema.py     (Delta tables)            │
 │   ├── init_season_mapping.py   (already exists)          │
 │   ├── init_customer_mapping.py (customer KTB config)     │
 │  ├── 01_pull_beproduct.py      pull BeProduct to Delta  │
 │  ├── 02_pull_dtc.py            pull DTC to Delta        │
 │  ├── 03_join_and_transform.py  composite key join       │
 │  ├── 04_detect_changes.py      timestamp-based diff     │
 │  ├── 05_push_to_beproduct.py   push changes back        │
 │  ├── 05_push_to_dtc.py         PATCH rows to DTC        │
 │  └── 06_push_images.py         (separate, later) DTC    │
 └──────────────────────────────────────────────────────────┘
```

---

## Phase 1a: Framework Foundation (Week 1)

### Task A1 — Create sync_hub Directory Structure

```
mkdir -p sync_hub/python/{connectors,client,diffing,mapping}
mkdir -p sync_hub/notebooks/00_init
touch sync_hub/python/__init__.py
touch sync_hub/python/connectors/__init__.py
touch sync_hub/python/client/__init__.py
touch sync_hub/python/diffing/__init__.py
touch sync_hub/python/mapping/__init__.py
```

**Deliverable**: Clean folder tree matching the requirement spec.

---

### Task A2 — AppConnector Base Class

**File**: `sync_hub/python/connectors/base.py`

```python
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class AppConnector(ABC):
    app_name: str  # "dtc", "beproduct"

    @abstractmethod
    def pull(self, since_timestamp=None) -> DataFrame:
        """Fetch all entities changed since timestamp.
        Returns: app_id, entity_type, entity_json, fetched_at, last_modified
        """

    @abstractmethod
    def push(self, records: DataFrame, change_type: str) -> DataFrame:
        """Push INSERT/UPDATE/DELETE changes back.
        Returns: app_id, push_status, error
        """

    @abstractmethod
    def get_field_schema(self) -> dict:
        """Return field definitions: {name: {type, writable, required}}"""
```

**Why**: Establishes the pluggable-contract for ALL future connectors. DTCConnector and BeProductConnector will extend this.

---

### Task A3 — Extract auth.py from RestClient

**File**: `sync_hub/python/client/auth.py`

Extract the `x-api-key` handling and any future auth strategies (OAuth2, bearer token) into a standalone module so clients can choose auth methods without modifying RestClient.

**Deliverable**: `AuthProvider` base class + `ApiKeyAuth` concrete class. RestClient accepts an AuthProvider instance.

---

### Task A4 — Refactor RestClient (minor)

- Extract auth to AuthProvider (Task A3)
- Move file to `sync_hub/python/client/rest_client.py`
- Add `PUT` method if not present (already is)

---

## Phase 1b: BeProduct Connector (Week 1-2)

### Task B1 — BeProductConnector

**File**: `sync_hub/python/connectors/beproduct.py`

```python
class BeProductConnector(AppConnector):
    app_name = "beproduct"

    def pull(self, since_timestamp=None) -> DataFrame:
        """
        For each folder/customer (KTB):
          1. List styles with lastModified filtering
          2. For each style, extract headerData:
             - fields: brand, LF Style#, SEASON, description,
               product_category, product_sub_category, division,
               garment_finish, tech_pack_stage, frontImage.origin
             - colorways[].colorName (→ N rows)
             - headerData fields: core_main_material.value (Main Fabric)
             - headerData fields: core_main_material_2.value (Fabric)
          3. Flatten: 1 color + fabric pair → 1 row
          4. Return DataFrame
        """
```

**Key Implementation Details**:

- **BeProduct SDK**: Use `https://python.beproduct.com/` SDK if available, else raw BeProduct API
- **Style retrieval**: `GET /styles?folder=KTB` filtered by `lastModified`
- **ColorWay expansion**: For each style with N colors → N rows
- **BOM fields**:
  - `$.headerData.fields[id="core_main_material"].value` → Group="Main Fabric", Fabric Placement=$value
  - `$.headerData.fields[id="core_main_material_2"].value` → Group="Fabric", Fabric Placement=$value
  - These are header-level denormalized BOM fields (not full BOM grid)
- **Fields to extract** (14 total):

| # | BeProduct Field | Extraction Path | Target DTC Column |
|---|----------------|-----------------|-------------------|
| 1 | Product Status | `$.headerData.fields[name="Product Status"].value` | Product Status |
| 2 | Style Image | `$.headerData.frontImage.origin` | Style Image |
| 3 | LF Style Number | `$.headerData.fields[name="LF Style Number"].value` | LF Style# |
| 4 | Description | `$.headerData.fields[name="Description"].value` | Style Description |
| 5 | Product Category | `$.headerData.fields[name="Product Category"].value` | Class |
| 6 | Product Sub Category | `$.headerData.fields[name="Product Sub Category"].value` | Sub Class |
| 7 | Division | `$.headerData.fields[name="Division"].value` | Division |
| 8 | Brand | `$.headerData.fields[name="Brand"].value` | Brand |
| 9 | Color | `$.colorways[].colorName` | Color / Wash |
| 10 | Garment Finish | `$.headerData.fields[name="Garment Finish"].value` | Garment Finish |
| 11 | Tech Pack Stage | `$.headerData.fields[name="Tech Pack Stage"].value` | Tech Pack Stage |
| 12 | Fabric Group (Main) | Hard-coded "Main Fabric" | Fabric Group |
| 13 | Fabric Placement (Main) | `$.headerData.fields[id="core_main_material"].value` | Placement |
| 14 | Mill Fabric Article # | Will be populated via FlatBOM (future) | Mill Fabric Article # |

**Deliverable**: Pull BeProduct KTB styles → Delta table `lft.beproduct.raw_beproduct_styles_uat`

---

### Task B2 — Field Mapping Module

**File**: `sync_hub/python/mapping/fields.py`

```python
BEPRODUCT_TO_DTC_FIELD_MAP = {
    "Product Status": {"bp_path": "...", "dtc_col": "Product_Status", "type": "string"},
    "Style Image":    {"bp_path": "...", "dtc_col": "Style_Image", "type": "url"},
    "LF Style Number": {"bp_path": "...", "dtc_col": "LF_Style", "type": "string"},
    # ... all 14 fields
    "Fabric Group":   {"bp_path": "HARDCODED_Main_Fabric", "dtc_col": "Fabric_Group", "type": "string"},
    "Placement":      {"bp_path": "...core_main_material...", "dtc_col": "Placement", "type": "string"},
    "Mill Fabric Article #": {"bp_path": "FLATBOM_FUTURE", "dtc_col": "Mill_Fabric_Article", "type": "string"},
}
```

**Also register here**: DTC column → normalized Delta column name mapping for push payload construction.

---

### Task B3 — SeasonCode Resolution Module

**File**: `sync_hub/python/mapping/season.py`

```python
SEASON_CODE_MAP = {
    ("KTB", "SPRING"): "SS",
    ("KTB", "FALL"):   "FW",
}
# Reverse: DTC code → BeProduct season
DTC_SEASON_REVERSE = {
    ("KTB", "SS"): "SPRING",
    ("KTB", "FW"): "FALL",
}

def resolve_season(customer: str, season_code: str) -> tuple[str, int]:
    """
    Look up mapping for ui beproduct.dtc_seasoncode_mapping table.
    Returns (beproduct_season, beproduct_year).
    All codes uppercase normalized.
    SS=Spring, FW=Fall. Year = 20 + last 2 digits of code.
    Raises ValueError for invalid codes.
    """

def validate_dtc_request_name(request_name: str) -> tuple[str, str, str]:
    """
    Parse "<customer> <seasonCode> <brand>".
    Validate seasonCode in {SS, FW}.
    Return (customer, season_code, brand) or raise/log error.
    """
```

---

## Phase 1c: DTC Connector Refactor (Week 2)

### Task C1 — DTCConnector extends AppConnector

**File**: `sync_hub/python/connectors/dtc.py`

```python
class DTCConnector(AppConnector):
    app_name = "dtc"

    def pull(self, since_timestamp=None) -> DataFrame:
        """
        1. Get all KTB WIP requests from DTC
        2. Filter: only {SS, FW} seasonCodes in request name
        3. For each valid request:
           - Get "Full Version" view
           - Pull sheet data
           - Flatten to row-level DataFrame
        4. Return unioned DataFrame across all valid requests
        """

    def push(self, records: DataFrame, change_type: str) -> DataFrame:
        """
        change_type = "INSERT":
           If sheet for this (customer, brand, season) exists:
             PATCH to add row
           Else:
             POST /v1/sheets to create new sheet + request
             Then PATCH to add row

        change_type = "UPDATE":
           PATCH /v1/sheets/{sheetId}/views/{viewId}
           With column values for changed fields
           Convert timestamps to +0800 HKT

        change_type = "DELETE" → N/A for DTC:
           Instead set "Product_Status = Drop" (UPDATE semantics)
        """
```

**Key changes from existing code**:

1. Switch from hardcoded single request ID → discover all KTB WIP requests
2. Use new request `6a26581854e92e7acd8fa71b` as sample
3. Pull ALL valid requests, not just one
4. `push()` unifies create + update + Drop-marking into one method
5. Sheet creation via POST `/v1/sheets` returns `requestId` + `sheetId`
6. Row creation via PATCH `/v1/sheets/{sheetId}/views/{viewId}`

---

### Task C2 — Change Detection Refactor

**File**: `sync_hub/python/diffing/detector.py`

Replace row_id matching with **composite key matching**:

```python
COMPOSITE_KEY = ("customer", "brand", "season_code", "lf_style_number", "color")

def detect_changes(beproduct_df: DataFrame, dtc_df: DataFrame) -> DataFrame:
    """
    Full outer join on COMPOSITE_KEY.

    BP-only rows   → INSERT to DTC (if no matching DTC sheet, create one first)
    DTC-only rows  → UPDATE: set "Product_Status" = "Drop"
    Both sides, BP.modifiedAt > DTC.updatedAt  → UPDATE to DTC
    Both sides, DTC.updatedAt > BP.modifiedAt  → (future: push to BeProduct)
    NoChange       → skip
    """
```

**Add timestamp comparison**: Convert both to UTC, compare BeProduct.modifiedAt > DTC.updatedAt to determine push direction.

---

### Task C3 — ImageInsert (Deferred)

**File**: `sync_hub/notebooks/06_push_images.py` (separate notebook)

```python
# POST /v1/sheets/{sheetId}/views/{viewId}/images
#   ?rowindex={number}&columnname=Style_Image
#   multipart/form-data with binary from BeProduct CDN
#   headerData.frontImage.origin
```

**Separate from main pipeline** — tackle after core field sync is stable.

---

## Notebook Pipeline (Databricks Job)

### Notebook 00: Init Tables (one-time)

| Notebook | Purpose | Status |
|----------|---------|--------|
| `00_init/init_delta_schema.py` | Create raw, change_log, push_queue tables | **NEW** |
| `00_init/init_season_mapping.py` | `dtc_seasoncode_mapping` table | ✅ EXISTS |

### Notebook 01-05: Sync Pipeline

| # | Notebook | Input | Output | Est. Time |
|---|----------|-------|--------|-----------|
| 01 | `01_pull_beproduct.py` | BeProduct API | `raw_beproduct_styles_uat` Delta table | 1-2 hrs |
| 02 | `02_pull_dtc.py` | DTC API | `raw_dtc_sheets_uat` Delta table | 0.5 hr (exists, refit) |
| 03 | `03_join_and_transform.py` | Both raw tables + mapping | Unified master view with composite keys | 1 hr |
| 04 | `04_detect_changes.py` | Unified master + prev snapshot | `change_log` table with diff rows | 1 hr |
| 05a | `05_push_to_beproduct.py` | Change log → BeProduct PATCH | Push status + new snapshot | 1 hr |
| 05b | `05_push_to_dtc.py` | Change log → DTC PATCH/POST | Push status + new snapshot | 1-2 hrs |
| 06 | `06_push_images.py` | Deferred | DTC image upload | — |

---

## Delta Table Schema (Aligned to Requirements)

### `raw_beproduct_styles_uat`
```sql
CREATE TABLE IF NOT EXISTS main.sync_hub.raw_beproduct_styles_uat (
  app_id STRING,
  style_id STRING,
  customer STRING,          -- "KTB"
  brand STRING,
  lf_style_number STRING,
  season STRING,             -- BeProduct season name
  year INT,
  color STRING,
  entity_json STRING,        -- Full BeProduct style JSON
  modified_at TIMESTAMP,
  fetched_at TIMESTAMP
) PARTITIONED BY (customer, season);
```

### `raw_dtc_sheets_uat`
```sql
CREATE TABLE IF NOT EXISTS main.sync_hub.raw_dtc_sheets_uat (
  app_id STRING,
  request_id STRING,
  sheet_id STRING,
  row_index INT,
  row_id STRING,
  customer STRING,           -- "KTB"
  season_code STRING,        -- "SS26", "FW27"
  brand STRING,
  lf_style_number STRING,
  color STRING,
  entity_json STRING,        -- Full DTC row JSON
  row_hash STRING,
  updated_at TIMESTAMP,      -- DTC's last update timestamp
  fetched_at TIMESTAMP
) PARTITIONED BY (customer, season_code);
```

### `change_log`
```sql
CREATE TABLE IF NOT EXISTS main.sync_hub.change_log (
  change_id STRING,
  change_type STRING,         -- "BP_TO_DTC", "DTC_TO_BP", "DROP_DTC"
  composite_key MAP<STRING,STRING>,  -- {customer, brand, season_code, lf_style#, color}
  request_id STRING,
  sheet_id STRING,
  row_id STRING,
  operation STRING,           -- "INSERT", "UPDATE", "DROP"
  payload STRING,             -- JSON for push
  push_status STRING,         -- "pending", "pushed", "failed"
  push_error STRING,
  detected_at TIMESTAMP,
  pushed_at TIMESTAMP
);
```

### `push_queue_dtc`
```sql
CREATE TABLE IF NOT EXISTS main.sync_hub.push_queue_dtc (
  request_id STRING,
  sheet_id STRING,
  row_id STRING,
  row_index INT,
  change_type STRING,         -- "INSERT", "UPDATE", "DROP"
  payload STRING,             -- PATCH body JSON
  season_code STRING,
  brand STRING,
  push_status STRING          -- "pending", "succes", "failed"
);
```

---

## Implementation Order (Revised Timeline)

### Week 1: Framework + BeProduct

| Day | Tasks | Deliverable |
|-----|-------|-------------|
| 1 | A1 + A2: Directory structure + AppConnector base class | `sync_hub/` scaffold + `base.py` |
| 2 | A3 + A4: Extract auth + RestClient refactor | `auth.py`, updated `rest_client.py` |
| 3 | B1 (start): BeProductConnector.pull() | Can pull styles from KTB |
| 4 | B1 (finish): ColorWay expansion + BOM fields | First pull working |
| 5 | B2 + B3: Field mapping module + SeasonCode module | `mapping/fields.py`, `mapping/season.py` |

### Week 2: DTC Refactor + Pipeline

| Day | Tasks | Deliverable |
|-----|-------|-------------|
| 6-7 | C1: DTCConnector refactor (extend AppConnector, discover all KTB WIP requests, pull updated sample) | New DTC pull working |
| 8 | C2: Change detection refactor (composite key + timestamp compare + Drop-marking) | `detector.py` |
| 9 | Notebook pipeline: 00 (init schema) + 01 (pull BP) + 02 (pull DTC) + 03 (join) | First end-to-end pull |
| 10 | Notebook pipeline: 04 (detect changes) + 05b (push to DTC) + sheet creation | End-to-end push working |

### Week 3: Validation + Deployment

| Day | Tasks | Deliverable |
|-----|-------|-------------|
| 11-12 | Test with new DTC sample request `6a26581854e92e7acd8fa71b` | Validated pull/push |
| 13 | Databricks job creation + scheduling | Production-ready job |
| 14 | Documentation + ImageInsert deferred notebook stub | Complete Phase 1 |
| Future | ImageInsert notebook + FlatBOM guidelines integration | Phase 1b |

---

## Success Criteria (Final)

- [ ] `AppConnector(ABC)` base class exists in `sync_hub/python/connectors/base.py`
- [ ] `DTCConnector` extends `AppConnector` — discovers all KTB WIP requests, filters to SS/FW only
- [ ] `BeProductConnector` extends `AppConnector` — pulls KTB styles with color expansion + BOM headerFields
- [ ] 14-field mapping resolved and transformable
- [ ] Composite key `(KTB, brand, season_code, lf_style#, color)` used for matching (not DTC row_id)
- [ ] Extra DTC rows marked "Product_Status = Drop" (not DELETEd)
- [ ] SeasonCode validation: only SS and FW; mapping via `dtc_seasoncode_mapping` UC table
- [ ] Timestamps: compare in UTC, DTC push converts to +0800 HKT
- [ ] Sheet creation: POST `/v1/sheets` works for new SeasonCode/Brand combos
- [ ] Code under `sync_hub/` (not `databricks/dtc/`)
- [ ] ImageInsert: deferred to `06_push_images.py` notebook (separate)
- [ ] Delta tables: `raw_beproduct_styles_uat`, `raw_dtc_sheets_uat`, `change_log`, `push_queue_dtc`
- [ ] Full pipeline runs as Databricks scheduled job

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| BeProduct SDK/API access not yet provisioned | Blocks B1 | Fallback: manual export+upload stub |
| DTC PATCH for new rows requires pre-calculated rowIndex | Integration fails | Test with sample request first; probe API behavior |
| FlatBOM endpoint not available by Phase 1 deadline | Mill Fabric Article # missing | Document as Known Gap, mark field "TBD" |
| DTC sheet creation API has undocumented auth/permissions | New sheet creation fails | Manual sheet creation as fallback, document requirement |
| ImageInsert multipart/form-data boundary issues | Image push fails | Deferred; won't block main pipeline |

---

## Quick Start Commands

```bash
# 1. Create sync_hub folder
cd ~/Documents/GitHub/beproduct-data-browser
mkdir -p sync_hub/python/{connectors,client,diffing,mapping}
mkdir -p sync_hub/notebooks/00_init
touch sync_hub/python/__init__.py
touch sync_hub/python/connectors/__init__.py
touch sync_hub/python/client/__init__.py
touch sync_hub/python/diffing/__init__.py
touch sync_hub/python/mapping/__init__.py

# 2. Copy existing DTC code (will refactor)
cp databricks/dtc/python/client/rest_client.py sync_hub/python/client/
cp databricks/dtc/python/connectors/dtc.py sync_hub/python/connectors/dtc_refactor_this.py

# 3. Start with AppConnector base class
```

---

## Reference Mapping

| BeProduct Field | DTC Column (Normalized) |
|----------------|-------------------------|
| Product Status | `Product_Status` |
| frontImage.origin | `Style_Image` |
| LF Style Number | `LF_Style` |
| Description | `Style_Description` |
| Product Category | `Class` |
| Product Sub Category | `Sub_Class` |
| Division | `Division` |
| Brand | `Brand` |
| colorName | `Color_Wash` |
| Garment Finish | `Garment_Finish` |
| Tech Pack Stage | `Tech_Pack_Stage` |
| HARDCODED: "Main Fabric" | `Fabric_Group` |
| core_main_material.value | `Placement` |
| (FlatBOM future) | `Mill_Fabric_Article` |

**Note**: DTC column names are normalized per `DTCConnector._normalize_column_name()` (HTML/space→underscore). The actual DTC field names in the "Full Version" view should be verified against request `6a26581854e92e7acd8fa71b` and the `DTC-api-2026-05-08.json` Postman dump.