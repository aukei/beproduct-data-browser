# BeProduct BOM API Investigation

**Date:** 2026-06-17  
**Test Style:** `LFBP-WW-AI FILE TEST` (LF Style#)  
**API Base:** `https://developers.beproduct.com/api/lifung`  
**SDK:** `beproduct >= 0.6.30`

---

## 1. Locating the Style

The style number `LFBP-WW-AI FILE TEST` (user referred to it as `LFBP-WW-AI FILE TES`) was located by iterating all styles via `client.style.attributes_list()`.

> **Discovery:** The `attributes_list()` filter parameter accepts a **list of dicts**, not a single dict. The field names in the API use the raw field IDs (`header_number`, `header_name`), not camelCase aliases. Filtering by `headerNumber` silently ignores the filter and returns all records.

```
header_id  : a3b6f8b9-f35d-4517-9d79-5b5c2b678c62
headerNumber: LFBP-WW-AI FILE TEST
headerName : med wash jeans
folder     : KTB  (id: 37dcc63a-4754-4bb8-8d34-8c07a4145fcd)
```

---

## 2. Apps Associated with a Style

`client.style.app_list(header_id='a3b6f8b9...')` maps to:

```
GET /api/lifung/Style/Pages?headerId={header_id}
```

The style has **23 apps**:

| title | type |
|-------|------|
| Artboard | Artboard |
| Styling Details | Artboard |
| Reference Photos | Presentation |
| Artwork | List |
| Additional Details | List |
| **HK BOM** | **BOMVariations** |
| Measurements | MultiMeasurements |
| Proto Sample | SampleRequestMulti |
| PreLine Sample | SampleRequestMulti |
| SMS Sample | SampleRequestMulti |
| Fit Sample | SampleRequestMulti |
| PP Sample | SampleRequestMulti |
| TOP Sample | SampleRequestMulti |
| Tech Pack | TechPack |
| Board | StyleBoard |
| Revision | Revisions |
| 3D Style | 3DStyle |
| 3D Pattern | Pattern |
| 3D Shape | Shape |
| DXF | Attachments |
| Attachments | Attachments |
| Change Log (Beta) | ChangeLog |
| **BOM** | **BOMVariations** |

**Key observation:** App IDs are **folder-level constants**, not per-style. Every style in the same folder (`KTB`) has identical app IDs. The SDK docs explicitly state this; it is confirmed here.

The two BOM apps:

| app_id | title | type | marketId |
|--------|-------|------|----------|
| `225a3377-af42-4013-8195-d4b3bf1b8899` | HK BOM | BOMVariations | `9ae3a106-...` |
| `cb349508-23ee-4122-81c4-8071cd54db3e` | BOM | BOMVariations | `7dc37e1d-...` |

---

## 3. `client.style.app_get()` Fails for BOMVariations

The SDK method `app_get()` maps to:

```
GET /api/lifung/Style/Page?headerId={header_id}&pageId={app_id}
```

Calling this against either BOM app returns **HTTP 500**:

```
System.InvalidOperationException: Page template 'BOMVariations' is managed
by PostgreSQL and must not be created via PageHelper.
  at BeProduct.Infrastructure.Helpers.PageHelper._CreateNewPage(...)
  at BeProduct.Infrastructure.Helpers.PageHelper.GetPageWithPermissions(...)
  at BeProduct.Public.API.StyleController.Page(...)
```

### What this reveals about BeProduct's internal architecture

This server-side stack trace exposes a two-tier storage architecture:

| Storage | Page Types |
|---------|-----------|
| **MongoDB** (via `PageHelper`) | Form, Grid, List, Artboard, TechPack, SampleRequestMulti, Attachments, 3DStyle, Pattern, Shape, Presentation, StyleBoard, ChangeLog, Revisions |
| **PostgreSQL** | **BOMVariations** (BOM apps) |

The `GET /Style/Page` endpoint is MongoDB-only. When it encounters a `BOMVariations` page, it attempts to auto-create a record via `PageHelper._CreateNewPage`, which correctly rejects the PostgreSQL-managed type. This is a backend routing gap — the endpoint does not know to delegate to PostgreSQL.

This means: **`app_get()` is structurally broken for all `BOMVariations` apps across the entire BeProduct SDK.** This is not a data problem for this one style — it is a missing API route.

### Endpoint availability summary for BOM (from Swagger spec)

| Method | Endpoint | Purpose | Status |
|--------|----------|---------|--------|
| `GET` | `/Style/Page?headerId=&pageId=` | Read any page | **500 for BOMVariations** |
| `POST` | `/Style/PageCBOM?headerId=&pageId=` | Write BOM rows | Update-only; also 500 if page uninitialised |
| `GET` | `/Style/PageCBOM?headerId=&pageId=` | (attempted) | **405 Method Not Allowed** |
| `POST` | `/Style/FlatBom` | Read BOM as flat list | Works, but see §4 |
| `POST` | `/Style/{headerId}/PageBOMDetails/{pageId}` | BOM detail update | Write-only (requires `materials` body) |
| `DELETE` | `/Style/PageCBOMItemDelete` | Remove BOM row | Write-only |
| `POST` | `/Style/{headerId}/CBOM/{pageId}/Reset` | Reset BOM | Write-only |
| `GET` | `/Style/PageSchema?pageId=` | App field schema | **400: "not supported in this API version"** for BOMVariations |

> **Implication:** There is no public READ endpoint for BOM variation page content other than `FlatBom`. The BeProduct UI reads BOM data through an internal path that is not exposed in the public API.

---

## 4. The Correct BOM Read Endpoint: `FlatBom`

### Endpoint

```
POST /api/{company}/Style/FlatBom?pageSize={n}&pageNumber={n}
```

### Request body (`BomSearch` schema)

```json
{
  "pageIds":        ["<bom-app-id>", ...],   // BOM app IDs to scope (optional)
  "filters":        [{ "field": "...", "operator": "...", "value": "...", "type": "..." }],
  "colorwayFilters": [...]
}
```

### Response structure (one item per BOM row / style)

```json
{
  "result": [
    {
      "header": {
        "id": "...",
        "headerNumber": "LFBP-WW-AI FILE TEST",
        "headerName": "med wash jeans",
        "folder": { "id": "...", "name": "KTB" },
        "headerData": { "fields": [...], "frontImage": {...}, ... },
        "createdBy": { "id": "...", "name": "WendyLamWY" },
        "createdAt": "2026-05-28T00:22:32.229Z",
        "modifiedBy": { "id": "...", "name": "lfplmadmin" },
        "modifiedAt": "2026-06-17T13:44:00.043Z",
        "colorways": [],
        "sizeRange": [...],
        "sizeClasses": [...]
      },
      "bom": null,            // ← material row data, or null if no BOM
      "applicationId": null   // ← which BOM app this row belongs to
    }
  ]
}
```

The `FlatBom` is a **flat export**: each item represents one BOM material row from one style. When a style has `bom: null`, it has no material rows in any BOM app.

### Filter behaviour (empirical)

| Filter body | Observed result |
|-------------|----------------|
| `{}` (no filter) | Returns all styles with any BOM app, paginated |
| `{"pageIds": ["<bom-app-id>"]}` | Returns `{"result": null}` — pageIds do NOT scope by style, possibly scopes to variation IDs not app IDs |
| `{"filters": [{"field": "header_number", "value": "..."}]}` | Returns 10 results regardless of value — filter applies to **material attributes**, not style number |
| `{"filters": [{"field": "id", "value": "..."}]}` | HTTP 500 — "Unrecognised property" from MongoDB |

> **Discovery:** The `FlatBom` filter fields map to **BOM material attributes**, not style header fields. Filtering by style is not directly supported; the caller must page through all results and match `header.id` client-side.

---

## 5. BOM Page for `LFBP-WW-AI FILE TEST` is Uninitialised

Scanning all `FlatBom` pages (7 pages × 20 items) found the target style at page 7 with:

```json
{
  "header": { "headerNumber": "LFBP-WW-AI FILE TEST", ... },
  "bom": null,
  "applicationId": null
}
```

No BOM variation named `BOMV_TEST_KEI1` was found, and material numbers `WV-0005` and `BT-000069` produced no results.

### Root cause: PostgreSQL record uninitialised

Attempting `POST /Style/PageCBOM` (the write endpoint) also returns the same 500 error:

```
System.InvalidOperationException: Page template 'BOMVariations' is managed
by PostgreSQL and must not be created via PageHelper.
  at PageHelper._CreateNewPage(...)
```

This error fires before any rows are processed, at the record-creation step. It means **the BOM page record for this style does not yet exist in PostgreSQL**. BeProduct initialises the PostgreSQL record the first time the BOM page is opened in the UI. Until that happens, neither reads nor writes succeed via the API.

### Confusing indicator: `PageModified` field

The style's attribute data includes:

```json
{ "id": "PageModified", "name": "Page Modified", "value": "HK BOM" }
```

This field appears to record which app was *last opened/saved* per style. Despite showing `"HK BOM"`, the HK BOM app is also unreadable — the same 500 error occurs. This field is likely a display hint for the BeProduct UI, not a reliable indicator of data existence.

---

## 6. Internal Architecture Summary

From probe results, BeProduct's internal data organisation is:

```
BeProduct Backend
├── MongoDB  ──── most page types (Form, Grid, List, Artboard, TechPack, etc.)
│                  Read/write: GET+POST /Style/Page, /Style/PageForm, /Style/PageGrid, etc.
│
└── PostgreSQL ── BOMVariations pages only
                   Read:  POST /Style/FlatBom  (flat export, all styles)
                   Write: POST /Style/PageCBOM (per-style BOM row updates)
                   Init:  Opening the BOM page in the BeProduct UI
                          (no public API to initialise the PostgreSQL record)
```

The public API Swagger exposes the following BOM-related endpoint count:

| Category | Count |
|----------|-------|
| BOM read endpoints | 1 (`FlatBom` — flat export only) |
| BOM write endpoints | 3 (`PageCBOM`, `PageCBOMItemDelete`, `PageBOMDetails`) |
| BOM admin endpoints | 2 (`CBOM/Reset`, `PageBOMDetails`) |
| BOM schema/metadata endpoint | 0 (PageSchema returns 400 for BOMVariations) |

---

## 7. Practical Guidance for SDK Usage

### Reading BOM data

`client.style.app_get()` **cannot** be used for BOM apps. Use the raw API:

```python
import requests

def get_flat_bom_for_style(client, header_id: str) -> dict | None:
    """Fetch BOM rows for a specific style via FlatBom endpoint."""
    base = client.public_api_url
    headers = client.raw_api._RawApi__get_headers()

    page = 1
    while True:
        resp = requests.post(
            f"{base}/Style/FlatBom?pageSize=50&pageNumber={page}",
            headers=headers,
            json={},
            timeout=30
        )
        result = resp.json().get("result", [])
        if not result:
            return None  # not found / no data
        for item in result:
            if item.get("header", {}).get("id") == header_id:
                return item  # {"header": {...}, "bom": <data or null>, "applicationId": <id or null>}
        page += 1
```

### Writing BOM data (SDK method)

```python
client.style.app_bom_update(
    header_id='a3b6f8b9-f35d-4517-9d79-5b5c2b678c62',
    app_id='cb349508-23ee-4122-81c4-8071cd54db3e',   # BOM app id
    rows=[
        # Insert by material header_id
        {'materialIdToInsert': '<material-header-id>'},

        # Or update/create ad-hoc row
        {
            'materialUpdate': {
                'rowId': None,           # None = create new; UUID = update existing
                'rowFields': [
                    {'id': 'field_id', 'value': 'value'}
                ]
            },
            'colorUpdate': [...]         # optional colorway mapping
        }
    ]
)
```

> **Prerequisite:** The BOM page must be opened in the BeProduct UI at least once before `app_bom_update` will succeed. If the PostgreSQL record is uninitialised, the API returns HTTP 500.

---

## 8. Open Questions

1. **Is there a public API to initialise a BOM page without UI interaction?**  
   Not found in the Swagger spec. The `POST /Style/{headerId}/CBOM/{pageId}/Reset` endpoint might serve this purpose — untested.

2. **What is the structure of a non-null `bom` in `FlatBom` results?**  
   No style in the test environment had `bom != null` across 140+ styles scanned. The schema would need to be observed from a populated instance.

3. **What are BOM Variations (`BOMVariations` type)?**  
   The type name implies multiple named variation pages per BOM app (e.g., `BOMV_TEST_KEI1`). The mechanism to list, create, or access individual variations is not exposed in the public API Swagger spec.

4. **`pageIds` in `BomSearch` — what IDs does it accept?**  
   Passing BOM app IDs returns `null`. These IDs may refer to variation-level page IDs, not app-level IDs. The variation ID namespace is not accessible without a PostgreSQL-level initialised BOM page.
