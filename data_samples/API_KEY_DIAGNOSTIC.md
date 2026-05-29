# DTC API Authentication Diagnostic Report

**Date**: 2026-05-28 11:21 UTC  
**Status**: ❌ API Key Returns 401 Unauthorized

---

## Summary

The provided DTC API key is being **rejected with 401 Unauthorized**. The API endpoint is reachable and properly configured, but authentication failed.

---

## Diagnostic Results

### ✅ What Works
- API endpoint reachable: `https://dtc-api.lfapps.net/api` (responds to requests)
- HTTPS/TLS connection: Valid certificate (expires Dec 2026)
- CORS headers: Properly configured
- Server responds: Returns 401 error message (not 502/timeout)

### ❌ What Failed
```
Request:  GET /v1/documents
Header:   x-api-key: 49A127E0942071B4BD440DD00386C6B3
Response: HTTP 401 {"message":"Unauthorized","statusCode":401}
```

### Testing Done
1. **curl with API key** → 401
2. **curl without API key** → 401 (expected)
3. **Python requests library** → 401
4. **OPTIONS request** → 200 OK (CORS validation successful)

---

## Possible Causes

| Cause | Likelihood | How to Check |
|-------|-----------|-------------|
| API key invalid/expired | 🔴 High | Ask DTC dev: "Is this key still active?" |
| API key needs regeneration | 🔴 High | Check if there's a key management UI |
| Workspace-level restriction | 🟡 Medium | Ask: "Does this key need workspaceName param?" |
| Wrong environment (not prod) | 🟡 Medium | Confirm `https://dtc-api.lfapps.net` is production |
| API key format issue | 🟡 Medium | Verify key has no whitespace/special chars |
| Additional auth required | 🟡 Medium | Check if Bearer token or OAuth2 needed instead |

---

## What to Do Now

### Contact DTC Developer with This Info

**Subject**: DTC API Key Authentication Issue

**Message**:
```
Hi,

I'm testing the DTC API key you provided:
- Key: 49A127E0942071B4BD440DD00386C6B3
- Environment: https://dtc-api.lfapps.net/api
- Endpoint: GET /v1/documents
- Result: 401 Unauthorized

Questions:
1. Is this API key still active/valid?
2. Do I need to regenerate it?
3. Are there any workspace-level restrictions?
4. Is https://dtc-api.lfapps.net the correct production endpoint?
5. Do I need any additional parameters (workspaceName, etc.)?
6. Is there an alternative authentication method (Bearer token, OAuth2)?

Can you verify the key and provide a working one if needed?

Thanks,
[Your name]
```

### In the Meantime

While waiting for the DTC developer to confirm the API key:

1. **Use the API Spec Analysis** (see `DTC_SCHEMA_ANALYSIS.md`)
   - Understand endpoint structure
   - Plan connector implementation
   - Design Databricks schema

2. **Start Phase 1 with mock data**
   - Create AppConnector base class
   - Build DTC connector skeleton
   - Design change detection algorithm
   - Build push queue formatting

3. **Validate other app connectors**
   - BeProduct: You already have SDK + existing code
   - Miro: Free tier available
   - XTS: Check if credentials available

---

## Next Steps After Key is Fixed

Once you have a working API key:

1. Run `explore_dtc_api.py` with valid key
2. Verify our schema assumptions
3. Start Phase 1 implementation
4. Build DTCConnector with real API responses

---

**Estimated Time to Resolution**: 1-2 business days (waiting on DTC dev response)
