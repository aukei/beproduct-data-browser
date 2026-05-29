# DTC API Schema Analysis

**Date**: 2026-05-28  
**Status**: API Key Authentication Issue - Awaiting Verification

---

## 1. Authentication Status

| Item | Value | Status |
|------|-------|--------|
| API Key | `49A127E0942071B4BD440DD00386C6B3` | ❌ Returns 401 Unauthorized |
| Environment | `https://dtc-api.lfapps.net/api` | ✅ Reachable |
| Header Format | `x-api-key: {key}` | ✅ Correct per spec |
| CORS | All methods allowed | ✅ Verified |

**Action Required**: Contact DTC developer to:
- Verify API key is still valid
- Confirm no workspace-level restrictions
- Check if key needs regeneration
- Verify correct base URL

---

## 2. API Endpoints Summary

### Requests (25 endpoints)
- **POST** `/v1/requests` - Create request
- **GET** `/v1/requests` - List requests with filters (business_key discovery)
- **GET** `/v1/requests/{requestId}` - Get single request details
- **GET** `/v1/requests/{requestId}/amendmentlogs` - Get change history (with logDatStart/logDatEnd filters)
- **PUT** `/v1/requests/{requestId}` - Update request metadata

### Sheets (7 endpoints - **most important for data sync**)
- **POST** `/v1/sheets` - Create sheet with initial data
- **GET** `/v1/sheets/{sheetId}/views/{viewId}` - Fetch sheet data (rows)
- **PATCH** `/v1/sheets/{sheetId}/views/{viewId}` - Partial update (specific rows only) ⭐
- **PUT** `/v1/sheets/{sheetId}/views/{viewId}` - Full replacement (all rows required)
- **DELETE** `/v1/sheets/{sheetId}/views/{viewId}/rows` - Delete specific rows

### Documents (5 endpoints - schema definition)
- **GET** `/v1/documents` - List documents with field definitions
- **GET** `/v1/documents/{documentId}` - Get document schema (field types, constraints)
- **PUT** `/v1/documents/{documentId}` - Update document and field definitions

### Other Key Endpoints
- **GET** `/v1/users/{userId}` - Get user timezone info (needed for date conversion)
- **GET** `/v1/users/me` - Current user profile
- **GET** `/v1/workspaces/{workspaceId}/users` - List workspace users

---

## 3. Field Types and Data Type Mapping

From the DTC spec and Create Sheet example:

| DTC Type | Spark SQL Type | Notes |
|----------|----------------|-------|
| `string` | StringType | Text fields |
| `number` | DoubleType | Numeric fields with optional format |
| `date` | TimestampType | **UTC on GET, User TZ on PUT** ⚠️ |
| `checkbox` | BooleanType | Stored as Y/N or true/false |
| `array` | StringType or ArrayType | Dropdown list (fixed values) |
| `contact` | StringType | Image metadata (URL/path) |
| `binary` | BinaryType | Attachment file (max 50MB) |
| `lookup` | StringType | Cross-reference to another request |

---

## 4. Date Field Handling - CRITICAL 🔴

### On GET (Read from DTC)
```json
{
  "Quotation Validity Until": "2026-07-01T00:00:00.000Z"  // ISO 8601 UTC string
}
```
- Format: **ISO 8601** with timezone `Z` (UTC)
- Type: **String** in JSON response
- Must be parsed as: `datetime.fromisoformat("2026-07-01T00:00:00.000Z".replace("Z", "+00:00"))`

### On PUT/PATCH (Write to DTC)
```json
{
  "Quotation Validity Until": "2026-07-01"  // User's local date/time
}
```
- Format: **User's Local Timezone** (NOT UTC)
- Example: If user is in Hong Kong (UTC+8), and wants July 1, 2026 midnight:
  - GET returns: `2026-06-30T16:00:00.000Z` (UTC)
  - Must PUSH as: `2026-07-01` (HK time, user perceives it as 07/01)

### User Timezone Mapping Required
**Need to fetch and store**:
```sql
CREATE TABLE dtc_user_timezones (
    user_id STRING,
    user_email STRING,
    timezone STRING,  -- Example: "Asia/Hong_Kong", "America/New_York"
    last_synced TIMESTAMP
);
```

**Discovery**: User timezone likely available in:
- `GET /v1/users/{userId}` response
- `GET /v1/users/me` (current user)
- User profile/preferences in workspace

---

## 5. Amendment Logs (Change Detection)

### Endpoint
```
GET /v1/requests/{requestId}/amendmentlogs
```

### Filters Supported
```json
{
  "filters": {
    "viewId": "string (optional)",
    "logDatStart": "2026-01-01T00:00:00.000Z (optional)",
    "logDatEnd": "2026-05-28T00:00:00.000Z (optional)",
    "logByEmail": "user@example.com (optional)"
  }
}
```

### What It Provides
- Timestamp of when row changed
- Which user made the change (email)
- Which fields were modified (needs verification with live API)
- **Does NOT provide** the actual field values (only that they changed)

### Implications for Sync
- Can detect "something changed" with date filters
- Still need to fetch full sheet to get actual values
- Enables incremental change detection (compare logDatStart to yesterday's sync time)

---

## 6. Row Update Strategies

### Option A: PATCH (Recommended for incremental sync) ⭐
```
PATCH /v1/sheets/{sheetId}/views/{viewId}
Content-Type: application/json

{
  "sheetData": [
    { "rowIndex": 1, "Item No.": "I00001", "Unit Price (USD)": 6 },
    { "rowIndex": 3, "Item No.": "I00023", "Unit Price (USD)": 9 }
  ]
}
```
**Advantages**:
- Only send changed rows
- Lower API payload
- Faster

**Limitations**:
- Must specify all columns being updated (or they become null?)
- Need to track which rows changed in Databricks

### Option B: PUT (Full Replacement)
```
PUT /v1/sheets/{sheetId}/views/{viewId}
Content-Type: application/json

{
  "sheetData": [
    { "rowIndex": 1, ... all columns ... },
    { "rowIndex": 2, ... all columns ... },
    { "rowIndex": 3, ... all columns ... }
  ]
}
```
**Advantages**:
- Simpler (send all rows every time)
- No ambiguity about which rows exist

**Disadvantages**:
- Large payloads
- Risk of accidental deletion (rows not in payload are deleted)
- Slower

---

## 7. Data Example from Spec

### Sample Sheet Data (from Create Sheet endpoint)
```json
{
  "requestReference": "REQ-2026-001",
  "requestDescription": "Marvel Action Figures Sourcing",
  "sheetData": [
    {
      "rowIndex": 1,
      "Item No.": "I00001",
      "Item Description": "Marvel Avengers Captain America",
      "Country of Origin": "China",
      "Harmonized System (HS) Code": "906200000",
      "Minimum Order Quantity": 1000,
      "Production Lead Time (week)": 8,
      "Unit Price (USD)": 5,
      "Quotation Validity Until": "01-JUL-2025"
    },
    {
      "rowIndex": 2,
      "Item No.": "I00202",
      "Item Description": "Marvel Spider-Man Action Figure",
      "Country of Origin": "China",
      "Harmonized System (HS) Code": "907",
      "Minimum Order Quantity": 2000,
      "Production Lead Time (week)": 5,
      "Unit Price (USD)": 4,
      "Quotation Validity Until": "01-JUL-2025"
    }
  ]
}
```

**Observations**:
- `rowIndex` is mandatory (1-based)
- All fields must be present in every row (or they'll be null/deleted)
- Date format in spec: "01-JUL-2025" (user-friendly), but actual API format needs verification

---

## 8. Deduplication & Business Keys

### Recommended Business Key Fields
Based on the spec examples:
- **Item-level**: `Item No.` (SKU/code)
- **Request-level**: `requestReference` (unique identifier per request)
- **Workspace-level**: `workspaceName` + `documentName` + `requestReference`

### Example Dedup Logic
```
For matching items across apps:
- BeProduct:  product_id + color_code
- DTC:        "Item No." + "Item Description"
- Miro:       Board name + Card title
- XTS:        item_code + variant

→ Normalized to: global_business_key = SHA256(normalized_item_name)
```

---

## 9. Schema Design for Databricks

### Raw Layer (Daily Snapshots)
```sql
CREATE TABLE main.sync_hub.raw_dtc_snapshots (
  snapshot_date DATE,           -- Partition key
  request_id STRING,            -- DTC request ID
  sheet_id STRING,
  view_id STRING,
  row_index INT,
  field_name STRING,
  field_value STRING,           -- All values as strings initially
  row_hash STRING,              -- SHA256 of entire row
  fetched_at TIMESTAMP,
  user_id STRING,
  user_timezone STRING          -- For date conversion later
);
```

### Curated Layer (Normalized)
```sql
CREATE TABLE main.sync_hub.dtc_requests (
  request_id STRING,
  workspace_name STRING,
  document_name STRING,
  request_reference STRING,     -- Business key
  request_description STRING,
  data JSON,                    -- Full row data
  synced_at TIMESTAMP,
  source_app STRING = "dtc"
);
```

### Change Log
```sql
CREATE TABLE main.sync_hub.change_log (
  change_id STRING,
  source_app STRING,
  request_id STRING,
  row_index INT,
  change_type STRING,           -- INSERT, UPDATE, DELETE
  detected_at TIMESTAMP,
  push_status STRING,           -- pending, synced, failed
  push_error STRING
);
```

---

## 10. Next Steps

### Immediate
1. ✅ **Verify API Key** - Contact DTC developer
   - Is key valid?
   - Any workspace restrictions?
   - Need to regenerate?

### Once API Access Confirmed
2. Run exploration script with valid credentials
3. Verify:
   - Actual date format in GET responses (ISO 8601 UTC confirmed?)
   - User timezone storage (where is it?)
   - Amendment log structure
   - Field type mappings
   - Pagination behavior

### Implementation
4. Build DTCConnector following AppConnector pattern
5. Implement snapshot-based diffing
6. Test date conversion logic (UTC ↔ user timezone)
7. Implement PATCH-based incremental updates

---

## 11. Known Unknowns (To Verify with Live API)

| Question | Importance | Status |
|----------|-----------|--------|
| Where is user timezone stored? | 🔴 High | Unknown |
| What's the exact date format in PATCH/PUT? | 🔴 High | Unknown |
| Can PATCH omit columns, or must all columns be sent? | 🔴 High | Unknown |
| What pagination does amendment logs use? | 🟡 Medium | Unknown |
| What happens if row doesn't exist in PATCH? | 🟡 Medium | Unknown |
| Max rows per sheet? | 🟡 Medium | Unknown (spec shows 1000) |
| Does API support field-level change logs? | 🟡 Medium | Probably not |
| Rate limits? | 🟡 Medium | Not in headers |

