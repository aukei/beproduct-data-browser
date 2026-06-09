# Phase 1 Gap Analysis — REVISED

**Date**: 2026-06-05 (Updated with clarifications)  
**Status**: ✅ All ambiguous points resolved for Phase 1

---

## How Clarifications Resolve Prior Ambiguities

| # | Ambiguity | Resolution | Impact |
|---|-----------|-----------|--------|
| 1 | BOM data no API | Hard-code 2 BOM lines from `headerData.fields`: `core_main_material` → (Main Fabric, $value) and `core_main_material_2` → (Fabric, $value). Future: FlatBOM guidelines from vendor. | **Unblocks 3 BOM fields** for Phase 1 |
| 2 | Multi-color ColorWay → rows | **N rows** — each color = 1 DTC row | Defines shape of denormalization |
| 3 | SeasonCode validation | Only **SS** and **FW**. Mapping table `beproduct.dtc_seasoncode_mapping` with customer-specific tuples `(KTB, SPRING, SS)`, `(KTB, FALL, FW)`. Uppercase normalization. Others → log error + skip. | Clear validation logic |
| 4 | PATCH vs POST for new rows | **PATCH** `/v1/sheets/{sheetId}/views/{viewId}` (page 18 of PDF) — same endpoint for existing + new rows | Simplifies push logic |
| 5 | Customer code (KTB vs KON) | **Unified to KTB**. Workspace = KTB, request prefix = KTB. New sample request ID `6a26581854e92e7acd8fa71b`. Old Kontoor/KON data deprecated. | Requires **re-pulling** the sample table |
| 6 | Sheet creation API | **POST** `/v1/sheets` (p.15 PDF) → returns requestId + sheetId | Can now implement auto-sheet creation |
| 7 | ImageInsert contract | **POST** `/v1/sheets/{sheetId}/views/{viewId}/images?rowindex={n}&columnname={text}` using `multipart/form-data`. **Deferred** to dedicated notebook. | Can split ImageInsert into Phase 1b |
| 8 | Timestamp timezone | BeProduct = UTC; DTC output = UTC; DTC expects **user timezone** (+0800 HKT). Convert on push. | Clear TZ handling — hardcode HKT for now |
| 9 | Error recovery | **No automated recovery**. Human review after each run. | Subjective — just log + continue |
| 10 | Precursor scripts | `beproduct_style_sync` = precursor. Pipeline: bulk BeProduct → bulk DTC → update both masters → timestamp-based push to both. | Clarifies architecture: **bidirectional** |

---

## Revised Gap Analysis

### Fixed Gaps (resolved by clarifications)

| Gap | Before | After |
|-----|--------|-------|
| BOM fields blocked | ❌ No API | ✅ `core_main_material` + `core_main_material_2` from headerData |
| Customer code confusion | ❌ KTB vs KON | ✅ Unified to KTB |
| ImageInsert blocked | ❌ No spec | ✅ Spec known; deferred notebook |
| SeasonCode validation | ❌ Unclear rules | ✅ Mapping table with SS/FW only |
| DTC sheet creation | ❌ No API | ✅ POST `/v1/sheets` known |

### Remaining Gaps (unchanged)

| # | Gap | Severity | Owner |
|---|-----|----------|-------|
| 1 | **BeProduct connector doesn't exist** | 🔴 Critical | Must build |
| 2 | **AppConnector base class doesn't exist** | 🔴 Critical | Must build |
| 3 | **No field mapping transformation** (14 fields) | 🔴 Critical | Must build |
| 4 | **Composite key matching** (uses row_id not composite key) | 🔴 Critical | Must refactor |
| 5 | **Extra DTC rows → DELETE, not "Drop"** | 🟡 Medium | Refactor change detection |
| 6 | **No push → DTC auto-sheet-creation** | 🟡 Medium | Add to push pipeline |
| 7 | **No timestamp comparison for push prioritization** | 🟡 Medium | Add to push pipeline |
| 8 | **No ImageInsert notebook** | 🟢 Low | Separate notebook; tackle later |
| 9 | **No unified pull() / push() methods** on DTCConnector | 🟡 Medium | Refactor to extend AppConnector |
| 10 | **Table schema mismatch** (spec vs actual) | 🟡 Medium | Align to requirement spec |

---

## Revised Architecture (Based on Clarifications)

```
                 ┌──────────────────────┐
                 │   BeProduct SDK/API   │
                 │   (Style + ColorWay   │
                 │    + headerData BOM)  │
                 └──────────┬───────────┘
                            │
                 ┌──────────▼───────────┐
                 │   STAGE 1: Pull BP   │
                 │   to Delta (raw)     │
                 │ lft.beproduct.raw_   │
                 │   beproduct_styles   │
                 └──────────┬───────────┘
                            │
                 ┌──────────▼───────────┐
                 │   STAGE 2: Pull DTC  │
                 │   to Delta (raw)     │
                 │ lft.beproduct.raw_   │
                 │   dtc_sheets         │
                 └──────────┬───────────┘
                            │
                 ┌──────────▼───────────┐
                 │  STAGE 3: JOIN +     │
                 │  TRANSFORM           │
                 │  Match on composite  │
                 │  key: (KTB, brand,   │
                 │   season, style#,    │
                 │   color)             │
                 │  → unified master    │
                 └──────────┬───────────┘
                            │
           ┌────────────────┼────────────────┐
           │                │                │
  ┌────────▼──────┐ ┌──────▼───────┐ ┌──────▼───────────┐
  │ STAGE 4A:     │ │ STAGE 4B:    │ │ STAGE 4C:        │
  │ Detect BP     │ │ Detect DTC   │ │ New sheet?        │
  │ changes by    │ │ changes by   │ │ POST /v1/sheets   │
  │ timestamp     │ │ timestamp    │ │ → requestId       │
  └────────┬──────┘ └──────┬───────┘ │ → sheetId         │
           │               │         └──────────────────┘
           │               │                  │
  ┌────────▼──────┐ ┌──────▼───────┐         │
  │ STAGE 5A:     │ │ STAGE 5B:    │         │
  │ Push BP       │ │ Push DTC     │         │
  │ changes to    │ │ changes to   │         │
  │ BeProduct     │ │ DTC via      │         │
  │               │ │ PATCH rows + │         │
  │               │ │ POST sheets  │         │
  └───────────────┘ └──────────────┘         │
                                             │
                                   ┌─────────▼─────────┐
                                   │  ImageInsert NB    │
                                   │  (separate, later) │
                                   └───────────────────┘
```

---

## Revised Success Criteria (Updated)

- [ ] `AppConnector(ABC)` base class in `sync_hub/python/connectors/base.py`
- [ ] `BeProductConnector(AppConnector)` pulls Style + ColorWay + BOM headerData fields
- [ ] `DTCConnector(AppConnector)` pulled new sample (request `6a26581854e92e7acd8fa71b`)
- [ ] **Field mapping**: BeProduct 14 fields → DTC 14 columns implemented
- [ ] **Composite key matching**: `(customer, brand, season_code, lf_style#, color)` not DTC row_id
- [ ] **Change detection**: Uses BeProduct_modifiedAt > DTC_updatedAt for update prioritization
- [ ] **DTC push**: Uses PATCH `/v1/sheets/{sheetId}/views/{viewId}` for both new and existing rows
- [ ] **SeasonCode validation**: Only SS/FW; mapping to BeProduct seasons via UC table
- [ ] **Extra DTC rows**: Marked "Product Status = Drop", not DELETEd
- [ ] **Sheet creation**: POST `/v1/sheets` for new SeasonCode/Brand combos
- [ ] **BOM fields**: Populated from `core_main_material` / `core_main_material_2` in headerData
- [ ] **Timestamp handling**: BeProduct UTC → compare → convert to +0800 HKT for DTC push
- [ ] **ImageInsert**: Deferred to dedicated notebook
- [ ] **Scheduled in Databricks**: Notebook chain runs as job