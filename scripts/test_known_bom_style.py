#!/usr/bin/env python3
"""
Test FlatBom endpoint with the known BOM style.

Style: LFBP-127LY1MO-001
StyleId: 80733ff7-9c24-46ac-bf95-7546b9d18cc6
Folder: KTB
FolderId: 37dcc63a-4754-4bb8-8d34-8c07a4145fcd

This style has confirmed BOM data populated in BeProduct.
"""

import json
import requests
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
from beproduct.sdk import BeProduct

# Load .env from project root
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

def get_credentials():
    """Get BeProduct credentials from environment"""
    creds = {
        "client_id": os.getenv("BEPRODUCT_CLIENT_ID"),
        "client_secret": os.getenv("BEPRODUCT_CLIENT_SECRET"),
        "refresh_token": os.getenv("BEPRODUCT_REFRESH_TOKEN"),
        "company_domain": os.getenv("BEPRODUCT_COMPANY_DOMAIN"),
    }
    
    if not all(creds.values()):
        print("❌ Missing BeProduct credentials in .env")
        sys.exit(1)
    
    return creds

def test_endpoint(token, company_domain, endpoint_type, request_body, description):
    """Test FlatBom endpoint with specific body"""
    
    if endpoint_type == "style":
        url = f"https://developers.beproduct.com/api/{company_domain}/Style/FlatBom"
    elif endpoint_type == "report":
        url = f"https://developers.beproduct.com/api/{company_domain}/Report/FlatBom"
    else:
        return False, None
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    query_params = {
        "pageSize": 10,
        "pageNumber": 1
    }
    
    print(f"\n{'='*80}")
    print(f"Test: {description}")
    print(f"Endpoint: {endpoint_type.upper()}/FlatBom")
    print(f"{'='*80}")
    print(f"Request body:")
    print(json.dumps(request_body, indent=2))
    
    try:
        response = requests.post(
            url,
            headers=headers,
            json=request_body,
            params=query_params,
            timeout=30
        )
        
        print(f"\nStatus: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            if isinstance(data, dict) and "result" in data:
                result = data["result"]
                
                if result is None:
                    print(f"Result: ⚠️ null")
                    return False, None
                
                elif isinstance(result, list):
                    print(f"✅ SUCCESS! Result is list with {len(result)} items")
                    
                    if result:
                        print(f"\nFirst item type: {type(result[0]).__name__}")
                        
                        if isinstance(result[0], dict):
                            keys = list(result[0].keys())
                            print(f"First item keys ({len(keys)} total): {keys[:15]}")
                            
                            print(f"\nFirst item content:")
                            for key in keys[:10]:
                                val = result[0][key]
                                if isinstance(val, str) and len(val) > 60:
                                    print(f"  {key}: {val[:57]}...")
                                elif isinstance(val, (str, int, float, bool)):
                                    print(f"  {key}: {val}")
                                elif isinstance(val, list):
                                    print(f"  {key}: [{len(val)} items]")
                                elif isinstance(val, dict):
                                    print(f"  {key}: {{...}}")
                                else:
                                    print(f"  {key}: {type(val).__name__}")
                        
                        print(f"\nFull response (first item):")
                        print(json.dumps(result[0], indent=2, default=str)[:1000])
                    
                    return True, result
                
                elif isinstance(result, dict):
                    print(f"✅ Result is dict with {len(result)} keys")
                    print(f"Keys: {list(result.keys())}")
                    return True, result
                
                else:
                    print(f"Result type: {type(result).__name__}")
                    return False, None
        
        elif response.status_code == 500:
            print(f"❌ HTTP 500 - Server Error")
            error_text = response.text[:300]
            print(f"Error: {error_text}")
            return False, None
        
        else:
            print(f"Status: {response.status_code}")
            print(f"Response: {response.text[:300]}")
            return False, None
    
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False, None

def main():
    print("=" * 80)
    print("Testing FlatBom with Known BOM Style")
    print("=" * 80)
    print("\nStyle: LFBP-127LY1MO-001")
    print("StyleId: 80733ff7-9c24-46ac-bf95-7546b9d18cc6")
    print("Folder: KTB (37dcc63a-4754-4bb8-8d34-8c07a4145fcd)")
    print("\nThis style has confirmed BOM data in BeProduct UI")
    
    creds = get_credentials()
    print(f"\n✅ Domain: {creds['company_domain']}")
    
    print("\n🔐 Authenticating...")
    client = BeProduct(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        refresh_token=creds["refresh_token"],
        company_domain=creds["company_domain"]
    )
    
    token = client.oauth2_client.get_access_token()
    print("✅ Authenticated")
    
    # Known BOM style
    known_style_id = "80733ff7-9c24-46ac-bf95-7546b9d18cc6"
    known_folder_id = "37dcc63a-4754-4bb8-8d34-8c07a4145fcd"
    
    # Test 1: Empty body with Style endpoint
    print(f"\n\n{'#'*80}")
    print("TEST GROUP 1: Style/FlatBom with Empty Body")
    print(f"{'#'*80}")
    
    test_endpoint(
        token,
        creds["company_domain"],
        "style",
        {
            "pageIds": [],
            "colorwayFilters": [],
            "filters": []
        },
        "Empty body (expect all styles with BOM?)"
    )
    
    # Test 2: With known style ID as pageId
    print(f"\n\n{'#'*80}")
    print("TEST GROUP 2: Style/FlatBom with Known Style ID")
    print(f"{'#'*80}")
    
    success, result = test_endpoint(
        token,
        creds["company_domain"],
        "style",
        {
            "pageIds": [known_style_id],
            "colorwayFilters": [],
            "filters": []
        },
        f"With known BOM style ID: {known_style_id}"
    )
    
    # Test 3: Report endpoint with known style ID
    if not success:
        print(f"\n\n{'#'*80}")
        print("TEST GROUP 3: Report/FlatBom with Known Style ID")
        print(f"{'#'*80}")
        
        test_endpoint(
            token,
            creds["company_domain"],
            "report",
            {
                "pageIds": [known_style_id],
                "colorwayFilters": [],
                "filters": []
            },
            f"Report endpoint with known BOM style"
        )
    
    # Test 4: With folder ID in filters
    print(f"\n\n{'#'*80}")
    print("TEST GROUP 4: With Folder Filter")
    print(f"{'#'*80}")
    
    test_endpoint(
        token,
        creds["company_domain"],
        "style",
        {
            "pageIds": [known_style_id],
            "colorwayFilters": [],
            "filters": [
                {
                    "field": "folderId",
                    "operator": "=",
                    "value": known_folder_id,
                    "type": "String"
                }
            ]
        },
        f"With folder filter: {known_folder_id}"
    )
    
    print(f"\n\n{'='*80}")
    print("All tests completed")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
