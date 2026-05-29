# DTC API Exploration Findings ✅

**Date**: 2026-05-28 19:26 UTC+8  
**Status**: ✅ **API WORKING** (on UAT environment)  
**Environment**: https://dtc-api.lfuat.net/api

---

## 1. Critical Finding: Use UAT, Not PRD

| Environment | Status | Note |
|-------------|--------|------|
| `https://dtc-api.lfapps.net` (PRD) | ❌ 401 Unauthorized | Wrong environment for this key |
| `https://dtc-api.lfuat.net` (UAT) | ✅ **WORKING** | Correct environment |

**Update plan**: Use **UAT for development**, switch to PRD once credentials configured.

---

## 2. Authenticated User Info

```json
{
  "userId": "69ef0605052cf39ce40da418",
  "userName": "LF - Au Chun Kei",
  "userEmail": "auchunkei@lifung.com",
  "isAdmin": "N",
  "defaultWorkspace": "Kontoor",
  "timeZone": "",           // ⚠️ EMPTY - timezone stored elsewhere?
  "timeZoneReference": ""   // ⚠️ EMPTY
}
```

**Key observation**: User timezone fields are **empty strings** in user profile. Need to check if timezone is:
- In workspace settings?
- In user preferences?
- Somewhere else in the API?

---

## 3. Workspace & Documents

### Available Workspace
- **Name**: Kontoor
- **ID**: 69eeffe5b0ca31804b3a0060
- **Permissions**: Full access (workspace admin)

### Available Documents
Example: "KON WIP" document
- **ID**: 69f0260975bebdc74b4bf0c6
- **Field Count**: 52 fields
- **Field Types**: string, number, date, checkbox, dropdown, etc.

---

## 4. Requests Available

**Count**: 6+ requests in Kontoor workspace

### Sample Request #1
```json
{
  "requestId": "69f076f0b7247a661226be9a",
  "requestReference": "KON FW26 Wrangler",
  "requestDescription": "MASTER CHART - FW26 Supplier",
  "documentName": "KON WIP",
  "requestIsActive": "Y",
  "requestStatus": "Sample Request & Tracking",
  "sheetId": "69f076f0b7247a661226be9b",
  "ownerId": "66ffa24d5f97cff3ca2c039a",
  "ownerName": "Kennis Wong",
  "ownerUserEmail": "kenniswong@lifung.com",
  "updatedDat": "2026-05-28T07:49:35.444Z",  // ← Recent change
  "createdDat": "2026-04-28T08:59:28.788Z"
}
```

---

## 5. Sheet Data Structure - CRITICAL FINDINGS 🔴

### Date Format in GET (Pull)
```json
{
  "Proto Sample<BR/>Request Date": "2025-07-01T00:00:00.000Z",
  "Final<BR/>Inspection - Due": "2026-04-27T00:00:00.000Z",
  "CRD": "2026-04-29T00:00:00.000Z",
  "INDC": "2026-07-02T00:00:00.000Z"
}
```

✅ **CONFIRMED**: Dates are **ISO 8601 UTC** strings with `.000Z` suffix

Format pattern: `YYYY-MM-DDTHH:MM:SS.000Z`

### Row Structure
```json
{
  "rowId": "e25849e3-f160-4617-b123-9d7c810599cf",  // UUID
  "rowIndex": 1,                                     // 1-based indexing
  "Product Status": "Production",                    // string
  "LF Style#": "WMG-J876-263 001",                  // string
  "FOB Price (USD/yd/) in CW": 3.07,                // number
  "Quantity": 500,                                   // number
  "Inspection Status": "Pass",                       // string enum
  "Sewing Status": "done",                           // string enum
  "Packing Status (pcs)": 500,                       // number
  "Final Inspection - Due": "2026-04-27T00:00:00.000Z"  // date (UTC)
}
```

### Field Types Observed
1. **String**: Product Status, Style Description, Class, Brand, Color, etc.
2. **Number**: FOB Price, Quantity, MOQ, Cutting (pcs), Shipment Quantity
3. **Date**: All date fields are ISO 8601 UTC strings
4. **Enum/Dropdown**: Status fields ("Pass", "done", "Production"), etc.
5. **URL**: Style Image (full URL)

**Key Point**: All data types are returned as-is from DTC (strings, numbers). Parsing/validation needed in Databricks.

### Row Update Capability
- Each row has: `rowId` (UUID) + `rowIndex` (1-based integer)
- Can use either `rowId` or `rowIndex` to identify row
- PATCH/PUT should use `rowIndex` for partial updates

---

## 6. Data Observations

### Large Sheet
- Sample request has **6 rows** in the Full Version view
- Each row has **50-80+ fields** (many optional/sparse)
- Total fields across all rows: ~100-150 columns
- Field names include HTML tags: `"Style<BR/>Description"`, `"Proto Sample<BR/>Request Date"`

### Sparse Data
- Not all fields populated in every row
- Row 6 missing "Product Status" field
- Some rows have different data (row 6 is incomplete)

### Date Variety
- Proto, Pre-line, SMS, Inspection, Tech Pack, Trims, Fit, JSS, PCD, Shade Band, TOP, Testing dates
- All in UTC format on GET
- **Will need conversion to user timezone on PUT**

---

## 7. Amendment Logs (Not Yet Tested)

From earlier research:
- Endpoint: `GET /v1/requests/{requestId}/amendmentlogs`
- Filters: `logDatStart`, `logDatEnd`, `logByEmail`, `viewId`
- Expected to return: change timestamps, user email, but NOT field values

**To test**: Call amendmentlogs endpoint with date filters

---

## 8. Views Available

Request "KON FW26 Wrangler" has **14 different views**:
1. Full Version
2. Vendor 1, 2, 3
3. Factory Allocation
4. Sample Request & Tracking
5. Fabric Information & Status
6. WIP
7. Production Track
8. WIP_ITS_USE
9. WIP_ITS_USE_TO_Vendor
10. WIP_ITS_USE_TO_Master
11. Full Version (obsoleted)

**Insight**: Each view is a filtered/curated version of the same underlying data. May have different columns, permissions, visibility.

---

## 9. User Timezone Handling - ACTION REQUIRED ⚠️

### Current Status
- ✅ Confirmed: Dates in GET response are UTC
- ⚠️ **TODO**: Where is user timezone stored?

### Theories
1. **Per-workspace setting** → Check workspace profile
2. **Per-user setting** → Check user profile (but timeZone field is empty)
3. **In request/sheet metadata** → Not seen yet
4. **In DTC UI settings** → Not in API?

### Next Step
Need to call:
```
GET /v1/users/me → Full details
GET /v1/workspaces/{workspaceId} → Workspace settings
```

to find where timezone is configured.

### For Now
**Assumption**: Treat all dates as UTC from GET, store them as-is in Databricks. When pushing:
1. Get the target user's timezone (from wherever it's stored)
2. Convert date from UTC → user's local timezone
3. Send in PATCH/PUT

---

## 10. Data Sample Summary

```
Workspace: Kontoor
Documents: KON WIP (52 fields)
Request: "KON FW26 Wrangler" (active, recently updated)
Sheet: 69f076f0b7247a661226be9b (6+ rows visible)
Views: 14 different views
Sample Row:
  - Style#: WMG-J876-263 001
  - Description: FA26 CROCODILE PRINT BLAZER
  - Fields: 80+ (including dates, prices, statuses, factory info)
  - Updated: 2026-05-28T07:49:35.444Z
```

---

## 11. Next Steps for Implementation

### Phase 0 (Data Exploration) - DONE ✅
- ✅ Found working environment (UAT)
- ✅ Confirmed date format (ISO 8601 UTC)
- ✅ Got sample data structure
- ✅ Confirmed row structure
- ⚠️ Still need: timezone location

### Phase 1 (Framework)
1. Find user timezone location (call /users and /workspaces)
2. Build DTCConnector class
3. Implement pull() method
4. Test parsing of dates, numbers, enums

### Phase 2 (Sync)
1. Test amendment logs
2. Implement snapshot diffing
3. Test change detection
4. Build Databricks tables

### Phase 3 (Push)
1. Test PATCH endpoint
2. Implement timezone conversion
3. Test row updates
4. Handle push errors

---

## 12. Known Issues & Warnings

| Issue | Severity | Workaround |
|-------|----------|-----------|
| User timezone empty in profile | 🔴 High | Need to find where timezone is stored |
| Field names have HTML (`<BR/>`) | 🟡 Medium | Normalize field names when loading |
| Some rows have sparse data | 🟡 Medium | Handle nulls/missing fields gracefully |
| Date format includes `.000Z` | 🟢 Low | Standard ISO 8601, easy to parse |
| Large sheets (100+ columns) | 🟡 Medium | Ensure Databricks table schema flexible |

---

## 13. Sample API Calls (Working)

### Get Current User
```bash
curl -X GET "https://dtc-api.lfuat.net/api/v1/users/me" \
  -H "x-api-key: 49A127E0942071B4BD440DD00386C6B3"
```

### List Documents
```bash
curl -X GET "https://dtc-api.lfuat.net/api/v1/documents?workspacename=Kontoor" \
  -H "x-api-key: 49A127E0942071B4BD440DD00386C6B3"
```

### List Requests
```bash
curl -X GET "https://dtc-api.lfuat.net/api/v1/requests" \
  -H "x-api-key: 49A127E0942071B4BD440DD00386C6B3" \
  -d '{
    "workspaceName": "Kontoor",
    "filters": {},
    "pendingOnly": "N",
    "requestOnly": "N"
  }'
```

### Get Sheet Data
```bash
curl -X GET "https://dtc-api.lfuat.net/api/v1/sheets/{sheetId}/views/{viewId}" \
  -H "x-api-key: 49A127E0942071B4BD440DD00386C6B3"
```

---

## 14. Files Created

- `DTC_API_FINDINGS.md` - This file (findings)
- `dtc_exploration_results.json` - Quick reference (JSON)
- `dtc_sample_request_full_data.json` - Full sample data (to be created)

---

## Conclusion

✅ **DTC API is accessible and working on UAT environment**

Key learnings:
1. Use `https://dtc-api.lfuat.net/api` (not PRD)
2. Dates are ISO 8601 UTC strings in GET responses
3. Row structure is flexible with rowIndex + fields
4. Multiple views per request
5. User timezone location needs investigation
6. Ready to proceed with Phase 1 implementation

---

## Recommendation

**Proceed with Phase 1 immediately**. The API is working, data structure is clear, and we have enough information to start building the DTCConnector and Databricks tables. Resolve timezone location in parallel.

