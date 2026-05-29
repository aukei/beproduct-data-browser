#!/usr/bin/env python3
"""
Analyze DTC API Specification from Postman JSON
Extracts endpoints, schemas, field types without needing live API
"""

import json
from pathlib import Path

spec_file = Path("/home/aukei/Documents/GitHub/beproduct-data-browser/DTC-api-2026-05-08.json")

with open(spec_file) as f:
    spec = json.load(f)

print("\n" + "="*80)
print("DTC API SPECIFICATION ANALYSIS")
print("="*80)
print(f"API Name: {spec['info']['name']}")
print(f"Base URL (PRD): https://dtc-api.lfapps.net/api")
print(f"Base URL (UAT): https://dtc-sit.lfuat.net/api")

# Extract all endpoints
endpoints = {}
for item in spec['item']:
    section = item.get('name')
    endpoints[section] = []
    for request in item.get('item', []):
        method = request['request'].get('method')
        path = request['request']['url']['raw'].replace('{{baseURL}}', '').split('?')[0]
        name = request.get('name')
        endpoints[section].append({
            'name': name,
            'method': method,
            'path': path
        })

print(f"\n\n{'='*80}")
print("AVAILABLE ENDPOINTS")
print("="*80)
for section, reqs in endpoints.items():
    print(f"\n{section}:")
    for req in reqs:
        print(f"  {req['method']:6} {req['path']}")

# Find all POST endpoints with request body examples
print(f"\n\n{'='*80}")
print("REQUEST/RESPONSE EXAMPLES")
print("="*80)

# Find Create Sheet example (has most detailed data)
for item in spec['item']:
    if item.get('name') == 'Sheets':
        for req in item.get('item', []):
            if 'Create Sheet' in req.get('name'):
                print(f"\n{req['name']}:")
                body = req['request'].get('body', {}).get('raw', '')
                if body:
                    try:
                        body_json = json.loads(body)
                        print(f"Sample request body:")
                        print(json.dumps(body_json, indent=2)[:1000])
                        print("\n... (truncated)")
                    except:
                        print(body[:500])

# Analyze field types from Create Sheet example
print(f"\n\n{'='*80}")
print("FIELD TYPE MAPPING (from spec)")
print("="*80)
print("""
Text Field         → string
Number Field       → number
Date Field         → date (ISO 8601 in GET, user timezone in PUT)
Checkbox Field     → checkbox (Y/N or boolean)
Dropdown List      → array/enum (fixed set of values)
Image Field        → contact (metadata/URL)
Attachment Field   → binary (file, max 50MB)
Lookup Field       → lookup (cross-reference to another request)
Dynamic Dropdown   → array with dataSource (values from another request)
""")

# Extract date-related information
print(f"\n{'='*80}")
print("DATE FIELD REQUIREMENTS (from API spec)")
print("="*80)
print("""
GET /v1/sheets/{sheetId}/views/{viewId}:
  - Date fields returned as ISO 8601 UTC strings
  - Example: "2026-05-28T14:30:00.000Z"

POST/PATCH /v1/sheets:
  - Date fields must be in USER'S LOCAL TIMEZONE
  - User timezone stored in: (needs verification from live API)
  - Example: "2026-05-28" (date only) or with time in user TZ

GET /v1/requests/{id}/amendmentlogs:
  - Returns log entries with timestamps
  - logDatStart, logDatEnd filters use ISO 8601 format
  - Allows incremental change detection
""")

# List all date-related fields mentioned in spec
print(f"\n{'='*80}")
print("DATE FIELDS FOUND IN SPEC")
print("="*80)
spec_text = json.dumps(spec, indent=2)
for line in spec_text.split('\n'):
    if 'date' in line.lower() or 'datetime' in line.lower():
        print(f"  {line.strip()[:100]}")

print(f"\n{'='*80}")
