# Phase 1 Gap Analysis: Requirement vs. Code

**Date**: 2026-06-05  
**Status**: ✅ Pull (DTC → Databricks) exists but significant gaps remain  
**Reviewer**: Automated Analysis

---

## How to Read This Document

| Section | What It Covers |
|---------|---------------|
| §1 | Code coverage against Phase 1 requirements (gap = missing/impartial) |
| §2 | Assessment against `dataexchage_requirement.md` (the authoritative spec) |
| §3 | Ambiguous / underspecified points in the requirement |
| §4 | Recommendations to close gaps |
| §5 | Summary table |

---

## 1. Code Coverage vs. Requirement

### 1.1 Folder Structure

| Required (PHASE_1_KICKOFF.md) | Actual Code Location | Gap |
|-------------------------------|---------------------|-----|
| `sync_hub/python/connectors/base.py` | ❌ Does not exist | **CRITICAL**: `AppConnector` abstract base class never created |
| `sync_hub/python/connectors/dtc.py` | `databricks/dtc/python/connectors/dtc.py` | ✅ Exists, but under wrong directory |
| `sync_hub/python/client/rest_client.py` | `databricks/dtc/python/client/rest_client.py` | ✅ Exists, under wrong directory |
| `sync_hub/python/client/auth.py` | ❌ Does not exist | 🔶 Missing — auth baked into RestClient directly |
| `sync_hub/python/diffing/snapshot.py` | `databricks/dtc/python/sync/snapshot.py` | 🔶 Exists under `sync/` not `diffing/` |
| `sync_hub/python/diffing/hasher.py` | ❌ Does not exist | 🔶 Missing — hashing logic embedded in SnapshotManager |
| `sync_hub/notebooks/00_init/init_delta_schema.py` | `databricks/dtc/notebooks/01_create_sync_tables.py` | 🔶 Exists but not at expected path |

**Finding**: The code is all under `databricks/dtc/` instead of the `sync_hub/` structure the requirement demands. No `base.py` (AppConnector abstract class) exists anywhere.

---

### 1.2 Core Architectural Components

#### Task 1: RestClient Wrapper
| Requirement | Status | Notes |
|-------------|--------|-------|
| Authentication headers (x-api-key) | ✅ | RestClient `_get_headers()` |
| Automatic retry (exponential backoff) | ✅ | urllib3 Retry + HTTPAdapter |
| Timeout handling | ✅ | `timeout` parameter, default 30s |
| Response parsing | ✅ | `.json()` |
| HTTP methods (GET, POST, PUT, PATCH, DELETE) | ✅ | All 5 implemented |
| Error handling / raise_for_status | ✅ | On RequestException |
| Standalone `auth.py` module | ❌ | Auth is inline in RestClient |

#### Task 2: AppConnector Base Class
| Requirement | Status | Notes |
|-------------|--------|-------|
| Abstract `AppConnector(ABC)` | ❌ | **Never created** |
| `pull(since_timestamp=None) -> DataFrame` | ❌ | DTCConnector has `pull_request_to_dataframe()` but no abstract contract |
| `push(records, change_type) -> DataFrame` | ❌ | DTCConnector has `create_row()` / `update_row()` / `delete_row()` but no abstract contract |
| `get_field_schema() -> dict` | ❌ | Not implemented |

#### Task 3: DTCConnector
| Requirement | Status | Notes |
|-------------|--------|-------|
| DTCConnector class | ✅ | Exists |
| Extends AppConnector(ABC) | ❌ | Standalone class, no base inheritance |
| `pull()` with `since_timestamp` | 🔶 Partial | `pull_request_to_dataframe()` exists but takes request_id+view_id, no incremental timestamp filter |
| `push()` with change_type routing | 🔶 Partial | Individual CRUD methods (`create_row`, `update_row`, `delete_row`) but no unified `push()` |
| `get_field_schema()` | ❌ | Not implemented |

#### Task 4: Databricks Delta Schema
| Required Table | Status | Notes |
|----------------|--------|-------|
| `raw_dtc_snapshots` (partitioned by snapshot_date) | ❌ | Not created — data written directly to `dtc_master_chart_uat` |
| `change_log` (INSERT/UPDATE/DELETE tracking) | 🔶 Partial | `dtc_master_chart_changes_{env}` exists in `01_create_sync_tables.py` |
| `push_queue_dtc` (per app push payloads) | ❌ | Not created |
| `dtc_season_code_mapping` | ✅ | Created by `00_init_season_mapping.py` |
| `dtc_sync_metadata` | ✅ | Created by `01_create_sync_tables.py` |

The requirement-specified schema (raw_dtc_snapshots, change_log, push_queue_dtc) does **not** match the actual schema (dtc_master_chart_uat, dtc_master_chart_changes, dtc_sync_metadata).

#### Task 5: Change Detection
| Requirement | Status | Notes |
|-------------|--------|-------|
| `detect_changes(prev, curr, business_key_cols)` | ✅ | `ChangeDetector.detect_changes()` exists |
| Hash-based diffing (row_hash) | ✅ | SHA256 in SnapshotManager |
| INSERT/UPDATE/DELETE classification | ✅ | Implemented |
| Business key composite matching | ❌ | Uses DTC `row_id` instead of `(customer, brand, season_code, style#)` as the business key |
| `hasher.py` module | ❌ | Hashing embedded in SnapshotManager |

---

### 1.3 BeProduct ↔ DTC Mapping (from dataexchage_requirement.md)

This is the **core Phase 1 feature**: sync BeProduct "Style" to DTC "WIP" Requests.

| Feature | Status | Notes |
|---------|--------|-------|
| Download BeProduct Styles | ❌ | **No BeProduct connector exists at all** |
| Download BeProduct ColorWays | ❌ | Missing |
| Download BeProduct BOM/Material | ❌ | Missing — requirement itself notes "no direct API" |
| Download DTC WIP documents | 🔶 Partial | Pulls one hardcoded request ID, not all WIP docs |
| Customer mapping (KTB ↔ KON) | 🔶 Partial | Passed as notebook parameter but not validated in code |
| Field mapping (BeProduct fields → DTC columns) | ❌ | **No field-level mapping logic exists** |
| Match on [customer+brand+style#+season+color+material] | ❌ | Comparison uses DTC `row_id`, not composite key |
| New SeasonCode/Brand → create new DTC Sheet | ❌ | No sheet creation logic |
| New rows → PATCH with max(rowIndex)+1 | ❌ | Uses POST `/v1/sheets/{id}/rows` instead |
| Update existing rows → PATCH with rowId | ✅ | DTCConnector.update_row() exists |
| Extra DTC rows → mark "Product Status = Drop" | ❌ | Change detector marks them as DELETE |
| ImageInsert flow (BeProduct CDN → DTC) | ❌ | Not implemented |
| Timestamp comparison (BP modifiedAt > DTC updated_at) | ❌ | Not implemented |

---

### 1.4 Specific Field Mapping Gap

The requirement explicitly lists these fields to sync:

| BeProduct Field | Target DTC Column | Implemented? |
|----------------|-------------------|-------------|
| Product Status | Product Status | ❌ |
| Image (frontImage) | Style Image | ❌ |
| LF Style Number | LF Style# | ❌ |
| Description | Style Description | ❌ |
| Product Category | Class | ❌ |
| Product Sub Category | Sub Class | ❌ |
| Division | Division | ❌ |
| Brand | Brand | ❌ |
| Color | Color / Wash | ❌ |
| Garment Finish | Garment Finish | ❌ |
| Tech Pack Stage | Tech Pack Stage | ❌ |
| Group (BOM) | Fabric Group | ❌ (BOM access is blocked) |
| Fabric Placement (BOM) | Placement | ❌ (BOM access is blocked) |
| Fabric Article (BOM) | Mill Fabric Article # | ❌ (BOM access is blocked) |

**Finding**: Zero field mapping code exists. The existing code pulls *all* DTC columns (114) without filtering or transforming. The BeProduct side has no connector at all.

---

## 2. Assessment Against dataexchage_requirement.md

### 2.1 Suprised Architectural Gaps

| Requirement Clause | Status | Finding |
|-------------------|--------|---------|
| "BeProduct to Databricks: project artifacts in ./databricks/" | ✅ | Exists (master_data_sync, style_sync, etc.) |
| "DTC to Databricks: project artifacts in ./databricks/dtc/" | ✅ | Exists |
| "The integration workflow to be scheduled and run entirely in Databricks as job" | 🔶 Partial | Notebook is Databricks-ready, but **no BeProduct pull** exists to form the integration |
| "Batch based: download BeProduct → download DTC → join → compare → push" | 🔶 Partial | DTC pull exists; BeProduct download, join, compare-all missing |
| "Use DTC 'patch' API for existing (style + color + material)" | ✅ | update_row() uses PATCH |
| "Use DTC 'patch' API for new (style + color + material)" | ❌ | Uses POST instead of PATCH with rowIndex assignment |
| "Mark (extra DTC rows) Product Status = Drop, do NOT DELETE rowid" | ❌ | Missing rows treated as DELETE; no "Drop" marking |
| "If BP 'frontImage'.'origin' is a proper url, use DTC API to push binary" | ❌ | Not implemented |

### 2.2 What Does Work (DTC Pull Only)

- ✅ RestClient with retry/exponential-backoff, all HTTP methods
- ✅ DTCConnector can pull a specific request by ID
- ✅ Sheet data → Pandas DataFrame → Spark DataFrame
- ✅ Document metadata stored as table properties
- ✅ Column name normalization (HTML tags, spaces → clean names)
- ✅ Change detection on DTC `row_id` (INSERT/UPDATE/DELETE)
- ✅ Snapshot hash comparison
- ✅ Databricks notebook with widget parameters
- ✅ SeasonCode mapping table setup
- ✅ Change log and sync metadata tables

---

## 3. Ambiguous / Underspecified Points in Requirement

### 3.1 ⚠️ BOM Data Access (HIGH Impact)

> "There is currently NO direct API to retrieve BOM data"  
> "Material fields [Group, Fabric Placement, Fabric Article] are not in headerData"

**Problem**: The requirement identifies that 3 of the 14 fields to sync live in BOM data that has no API. The code has scripts exploring this (`find_bom_styles.py`, `test_flattbom_endpoint.py`, etc.) but no conclusion or workaround is documented.

**What's missing**: A decision on:
- Use BeProduct SDK's hidden endpoints?
- Accept that these 3 fields cannot be synced?
- Manual mapping via FlatBOM endpoint (being explored)?
- Fallback to `core_main_material` fields from headerData (even though they don't match)?

### 3.2 ⚠️ Multiple Colors in One ColorWay

> "1 Style links with 1 Color(way) and 1 Material"  
> "A Colorway can contain more than 1 color name"

**Problem**: If a Style has one Colorway with multiple color names, does that produce 1 DTC row or N rows? The requirement is silent on how to denormalize.

### 3.3 ⚠️ SeasonCode Validation Rules

> "SeasonCode = SSYY = 2-characters season + 2-digit year. Hardcode for now"  
> "all other season code log errors"  
> "HD = Holiday" explicitly listed as valid

**Problem**: The valid season codes listed are "SS, HD, FW" but the validation rule isn't specified. Is "HD" included in "all other season code log errors" or treated as valid? Need a definitive list.

### 3.4 ⚠️ "patch" vs "post" for New Rows

> "Use DTC 'patch' API for new (style + color + material) >> assign a new rowIndex"

**Problem**: The existing DTC `create_row()` uses POST `/v1/sheets/{sheetId}/rows`. The requirement says to use PATCH. These are semantically different — POST auto-assigns rowId/rowIndex, PATCH would need to pre-calculate. Which does the DTC API actually support for new rows?

### 3.5 ⚠️ Customer Code Ambiguity

> "Customer to focus on = KTB (Kontoor)"  
> "Workspace = '<Customer>', i.e. KTB"  
> But the actual DTC workspace is "Kontoor" and customer code in DTC requests is "KON"

**Problem**: The requirement uses "KTB" as the DTC workspace/customer but the actual DTC data uses "KON". The mapping (KTB ↔ KON) is in DATA_MODEL.md but NOT in the requirement document. This creates confusion.

### 3.6 ⚠️ DTC Sheet Creation API

> "Create sheet () return request id + sheet id"

**Problem**: The DTC API endpoint to create a new sheet/request is not documented. Is it `POST /v1/requests`? Is there an existing DTC document that backs it? The implementation can't proceed without this.

### 3.7 ⚠️ ImageInsert Flow Binary Handling

> "Take 'origin' from BeProduct CDN. Treat as binary data."
> "DTC API /v1/sheets/{sheetId}/views/{viewId}/images?rowindex={number}&columnname={text}"

**Problem**: The API endpoint shown uses `sheetId` + `viewId` + `rowIndex` + `columnName` as parameters. The requirement doesn't specify:
- Whether the image is base64-encoded or raw binary in the body
- Expected Content-Type
- Maximum image size
- Whether the viewId must be "Full Version" or any view

### 3.8 ⚠️ Timestamp Comparison for Updates

> "BeProduct_Style_modifiedAt > DTC_row_Updated_at  *beware of timezone*"

**Problem**: BeProduct and DTC store timestamps in different systems with potentially different timezone handling. The requirement doesn't specify:
- Which timezone to convert both to
- How to handle null timestamps on first sync
- Timezone strategy when user timezone is still unknown

### 3.9 ⚠️ Error Recovery for Non-Matching Requests

> "4.1.1. Determine if the DTC Request exists. If not, LOG ERROR"

**Problem**: What happens after logging? Is the row silently skipped? Retried? Queued for manual intervention? The requirement doesn't specify the recovery path.

### 3.10 ⚠️ Existing BeProduct → Databricks Sync

The repo has `databricks/beproduct_master_data_sync.py` and `beproduct_style_sync.py` — but these appear to be separate initiatives. The requirement's Phase 1 is about BeProduct → DTC *directly*, not BeProduct → Databricks independently. The relationship between these existing BeProduct scripts and Phase 1 is unclear.

---

## 4. Recommendations to Close Gaps

### Priority 1 (Blocking Phase 1 Completion)

1. **Create `AppConnector` base class** at `sync_hub/python/connectors/base.py` per the spec
2. **Refactor `DTCConnector`** to extend `AppConnector(ABC)` and implement `pull()` / `push()` / `get_field_schema()`
3. **Build BeProduct connector** — At minimum a connector that can pull Style + ColorWay data via BeProduct SDK/API
4. **Implement field mapping** — The 14-field BeProduct ↔ DTC mapping logic
5. **Resolve BOM access** — Document a decision: use FlatBOM, headerData fallback, or mark as not-syncable

### Priority 2 (Supporting Gaps)

6. **Change matching from DTC `row_id` to composite key** `(customer, brand, season_code, lf_style_number)`
7. **Replace DELETE with "Product Status = Drop"** for extra DTC rows
8. **Implement new-sheet creation** for new SeasonCode/Brand combinations
9. **Document DTC sheet creation API** to enable automated new-request creation
10. **Implement timestamp comparison** for update-prioritization

### Priority 3 (Clarity / Quality)

11. **Restructure to `sync_hub/`** per the architecture plan (or update the plan if `databricks/dtc/` is accepted as canonical)
12. **Resolve seasonCode validation** — definitive list of valid codes
13. **Clarify multi-color ColorWay → DTC row mapping**
14. **Document ImageInsert API contract** (binary format, Content-Type, size limits)
15. **Define error recovery** for non-matching DTC Requests

---

## 5. Summary Table

| Area | Status | Implementation (%) | Notes |
|------|--------|--------------------|-------|
| RestClient | ✅ | 90% | All methods, retry, auth. Missing standalone `auth.py`. |
| AppConnector base class | ❌ | 0% | **Never created** — the most architecturally critical gap |
| DTCConnector | 🔶 | 50% | Exists but doesn't extend AppConnector. CRUD methods exist but no unified push(). |
| BeProduct Connector | ❌ | 0% | **Entirely missing** — core of Phase 1 |
| Field Mapping | ❌ | 0% | No field-level transformation |
| BOM/Material Access | ❌ | 0% | Blocked by no API; exploration scripts exist but no conclusion |
| Change Detection | ✅ | 70% | Works on DTC `row_id`; needs composite key matching |
| Delta Schema | 🔶 | 60% | Tables exist but don't match spec exactly |
| Push Logic | 🔶 | 30% | Individual CRUD methods work; no unified push or sheet creation |
| ImageInsert | ❌ | 0% | Not implemented |
| SeasonCode Mapping | ✅ | 80% | Table created, mapping sample inserted |
| Timestamp Diffing | ❌ | 0% | Not implemented |

**Overall Phase 1 Readiness**: ⚠️ **~30%** — fundamental gaps remain

The DTC pull infrastructure is solid, but:
- The **BeProduct side is entirely missing**
- The **field mapping/tranformation layer is absent**
- The **abstract AppConnector contract isn't implemented**
- The **composite key matching, error recovery, image handling, and Drop-marking** are missing

This means the integration pipeline (BeProduct → join → compare → push) cannot be assembled.
