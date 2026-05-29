#!/usr/bin/env python3
"""
DTC API Exploration Script - READ-ONLY MODE

Discovers API structure, data types, field definitions, timezone handling.
"""

import requests
import json
from datetime import datetime
import sys

API_KEY = "49A127E0942071B4BD440DD00386C6B3"
API_URL = "https://dtc-api.lfapps.net/api"
HEADERS = {"x-api-key": API_KEY, "Content-Type": "application/json"}

def call_api(method, endpoint, data=None):
    """Make API call, print response, return parsed JSON"""
    url = f"{API_URL}{endpoint}"
    print(f"\n{'='*80}")
    print(f"REQUEST: {method} {endpoint}")
    print(f"{'='*80}")
    
    try:
        if method == "GET":
            resp = requests.get(url, headers=HEADERS, timeout=10)
        elif method == "POST":
            resp = requests.post(url, json=data, headers=HEADERS, timeout=10)
        
        print(f"Status: {resp.status_code}")
        print(f"Response Headers:")
        for k, v in resp.headers.items():
            if k.lower().startswith('x-') or k.lower().startswith('rate'):
                print(f"  {k}: {v}")
        
        if resp.status_code in [200, 201]:
            result = resp.json()
            # Pretty print first 1000 chars
            full_json = json.dumps(result, indent=2)
            print(f"Response:\n{full_json[:1500]}")
            if len(full_json) > 1500:
                print(f"\n... ({len(full_json) - 1500} more characters)")
            return result
        else:
            print(f"Error: {resp.status_code}")
            print(f"Response: {resp.text[:500]}")
            return None
    except Exception as e:
        print(f"Exception: {e}")
        import traceback
        traceback.print_exc()
        return None

# EXPLORATION SEQUENCE
print("\n" + "="*80)
print("DTC API EXPLORATION - READ-ONLY MODE")
print("="*80)
print(f"API Key: {API_KEY[:16]}...{API_KEY[-4:]}")
print(f"Base URL: {API_URL}")
print(f"Timestamp: {datetime.utcnow().isoformat()}Z")

# Step 1: List documents
print("\n\n" + "="*80)
print("STEP 1: List Documents")
print("="*80)
docs = call_api("GET", "/v1/documents")

doc_id = None
if docs and isinstance(docs, list) and len(docs) > 0:
    first_doc = docs[0]
    doc_id = first_doc.get("id")
    print(f"\n✓ Found {len(docs)} document(s)")
    print(f"  First document ID: {doc_id}")
    print(f"  First document name: {first_doc.get('name')}")

# Step 2: Get document schema (for field definitions)
if doc_id:
    print("\n\n" + "="*80)
    print(f"STEP 2: Get Document Schema (ID: {doc_id})")
    print("="*80)
    doc_detail = call_api("GET", f"/v1/documents/{doc_id}")
    
    if doc_detail and "dynamicFields" in doc_detail:
        print(f"\n✓ Document has {len(doc_detail.get('dynamicFields', []))} fields:")
        for field in doc_detail.get("dynamicFields", []):
            fname = field.get("fieldName")
            ftype = field.get("type")
            print(f"  - {fname}: type={ftype}")

# Step 3: List requests
print("\n\n" + "="*80)
print("STEP 3: List Requests")
print("="*80)
requests_result = call_api("POST", "/v1/requests", {
    "filters": {},
    "pendingOnly": "N",
    "requestOnly": "N"
})

req_id = None
sheet_id = None
view_id = None

if requests_result and isinstance(requests_result, list) and len(requests_result) > 0:
    first_req = requests_result[0]
    req_id = first_req.get("id")
    print(f"\n✓ Found {len(requests_result)} request(s)")
    print(f"  First request ID: {req_id}")
    print(f"  Request details: {json.dumps(first_req, indent=2)[:500]}")
    
    # Step 4: Get specific request
    if req_id:
        print("\n\n" + "="*80)
        print(f"STEP 4: Get Request Details (ID: {req_id})")
        print("="*80)
        req_detail = call_api("GET", f"/v1/requests/{req_id}")
        
        if req_detail:
            sheet_id = req_detail.get("sheetId")
            view_id = req_detail.get("defaultViewId") or req_detail.get("viewId")
            print(f"\n✓ Request loaded")
            print(f"  Sheet ID: {sheet_id}")
            print(f"  View ID: {view_id}")
            print(f"  Status: {req_detail.get('status')}")
            print(f"  Owner: {req_detail.get('ownerEmail')}")
else:
    print("\n✗ No requests found")

# Step 5: Get sheet data
if sheet_id and view_id:
    print("\n\n" + "="*80)
    print(f"STEP 5: Get Sheet Data (sheetId: {sheet_id}, viewId: {view_id})")
    print("="*80)
    sheet = call_api("GET", f"/v1/sheets/{sheet_id}/views/{view_id}")
    
    if sheet:
        print(f"\n✓ Sheet loaded")
        data = sheet.get("sheetData", [])
        print(f"  Total rows: {len(data)}")
        
        if data:
            # Step 5a: Analyze first row
            print("\n" + "-"*80)
            print("STEP 5a: First Row Analysis - Field Types & Values")
            print("-"*80)
            first_row = data[0]
            print(f"\nRow data: {json.dumps(first_row, indent=2)}")
            
            print(f"\nField type analysis:")
            date_fields = []
            for key, value in first_row.items():
                value_type = type(value).__name__
                # Check if it looks like a date
                is_date = False
                if isinstance(value, str):
                    if "T" in value or "-" in value or value.startswith("20"):
                        is_date = True
                        date_fields.append((key, value))
                
                print(f"  {key}: {value_type:8} = {str(value)[:100]}")
                if is_date:
                    print(f"           ^^^ POSSIBLE DATE FIELD (value looks like date)")
            
            if date_fields:
                print(f"\n✓ Detected {len(date_fields)} potential date field(s):")
                for fname, fval in date_fields:
                    print(f"  - {fname}: {fval}")

# Step 6: Get amendment logs (if request ID available)
if req_id:
    print("\n\n" + "="*80)
    print(f"STEP 6: Get Amendment Logs (requestId: {req_id})")
    print("="*80)
    logs = call_api("GET", f"/v1/requests/{req_id}/amendmentlogs")
    if logs:
        print(f"\n✓ Amendment logs retrieved")
        if isinstance(logs, dict) and "logs" in logs:
            actual_logs = logs["logs"]
            print(f"  Total log entries: {len(actual_logs)}")
            if actual_logs:
                print(f"  First log entry:")
                print(f"    {json.dumps(actual_logs[0], indent=2)[:300]}")

# Summary
print("\n\n" + "="*80)
print("EXPLORATION SUMMARY")
print("="*80)
print("\n✓ Successfully explored DTC API structure")
print("\nKey Findings to Verify:")
print("  1. Date field format (ISO 8601 UTC vs. other)?")
print("  2. Any timezone information in request/sheet data?")
print("  3. Nested objects or arrays in sheet rows?")
print("  4. Document field schema matches actual data?")
print("  5. Amendment logs include which metadata?")

print("\n" + "="*80)
