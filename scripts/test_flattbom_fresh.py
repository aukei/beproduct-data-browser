#!/usr/bin/env python3
"""
Fresh test of FlatBom endpoint with proper BomSearch schema.
Tests /api/<company>/Style/FlatBom with various request body formats.

Usage: python3 scripts/test_flattbom_fresh.py
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

def fetch_sample_styles(client, limit=5):
    """Fetch sample styles to extract pageIds"""
    try:
        styles = []
        for idx, style in enumerate(client.style.attributes_list()):
            styles.append(style)
            if idx >= limit - 1:
                break
        return styles
    except Exception as e:
        print(f"❌ Error fetching styles: {e}")
        return []

def test_flattbom_with_body(access_token, company_domain, request_body, description):
    """Test FlatBom endpoint with specific request body"""
    url = f"https://developers.beproduct.com/api/{company_domain}/Style/FlatBom"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    query_params = {
        "pageSize": 10,
        "pageNumber": 1
    }
    
    print(f"\n{'='*80}")
    print(f"Test: {description}")
    print(f"{'='*80}")
    print(f"URL: {url}")
    print(f"Query params: {query_params}")
    print(f"Request body:")
    print(json.dumps(request_body, indent=2))
    print()
    
    try:
        response = requests.post(
            url,
            headers=headers,
            json=request_body,
            params=query_params,
            timeout=30
        )
        
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response type: {type(data).__name__}")
            
            if isinstance(data, dict):
                print(f"Response keys: {list(data.keys())}")
                
                if "result" in data:
                    result = data["result"]
                    if result is None:
                        print(f"Result: ⚠️ null")
                    elif isinstance(result, list):
                        print(f"Result: ✅ List with {len(result)} items")
                        if result:
                            print(f"First item type: {type(result[0]).__name__}")
                            if isinstance(result[0], dict):
                                print(f"First item keys: {list(result[0].keys())[:10]}")  # First 10 keys
                                print(f"First item sample:")
                                # Print first few fields
                                for key in list(result[0].keys())[:5]:
                                    val = result[0][key]
                                    if isinstance(val, (str, int, float)):
                                        print(f"  {key}: {val}")
                                    else:
                                        print(f"  {key}: {type(val).__name__}")
                            return True, result
                    elif isinstance(result, dict):
                        print(f"Result: ✅ Dict with keys: {list(result.keys())}")
                        return True, result
                    else:
                        print(f"Result type: {type(result).__name__}")
                        return False, None
                else:
                    print(f"No 'result' key in response")
                    print(f"Response: {json.dumps(data, indent=2)[:500]}")
                    return False, None
            else:
                print(f"Unexpected response type: {type(data).__name__}")
                return False, None
        
        elif response.status_code == 500:
            print(f"Status: 500 - Server Error")
            error_text = response.text[:200]
            if "NullReferenceException" in error_text:
                print(f"Error: NullReferenceException (backend bug)")
            else:
                print(f"Error: {error_text}")
            return False, None
        
        elif response.status_code == 400:
            print(f"Status: 400 - Bad Request")
            print(f"Error: {response.text[:200]}")
            return False, None
        
        else:
            print(f"Status: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            return False, None
    
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False, None

def main():
    print("=" * 80)
    print("BeProduct FlatBom Fresh Test")
    print("Testing /api/<company>/Style/FlatBom with BomSearch schema")
    print("=" * 80)
    
    creds = get_credentials()
    print(f"\n✅ Loaded credentials for domain: {creds['company_domain']}")
    
    print("\n🔐 Authenticating...")
    client = BeProduct(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        refresh_token=creds["refresh_token"],
        company_domain=creds["company_domain"]
    )
    print("✅ Authenticated")
    
    print("\n📥 Fetching sample styles...")
    styles = fetch_sample_styles(client, limit=5)
    
    if not styles:
        print("❌ Could not fetch styles")
        sys.exit(1)
    
    # Extract pageIds
    page_ids = []
    for style in styles:
        if isinstance(style, dict):
            for field in ["id", "styleId", "header_id", "headerId"]:
                if field in style:
                    page_ids.append(style[field])
                    break
    
    print(f"✅ Extracted {len(page_ids)} page IDs")
    if page_ids:
        print(f"   Sample: {page_ids[:2]}")
    
    # Get token for direct HTTP calls
    token = client.oauth2_client.get_access_token()
    
    # Test 1: Empty body
    print(f"\n\n{'#'*80}")
    print("TEST GROUP 1: Empty/Minimal Bodies")
    print(f"{'#'*80}")
    
    test_flattbom_with_body(
        token,
        creds["company_domain"],
        {
            "pageIds": [],
            "colorwayFilters": [],
            "filters": []
        },
        "Empty arrays"
    )
    
    # Test 2: No keys at all
    test_flattbom_with_body(
        token,
        creds["company_domain"],
        {},
        "Completely empty object"
    )
    
    # Test 3: With pageIds
    if page_ids:
        print(f"\n\n{'#'*80}")
        print("TEST GROUP 2: With PageIds")
        print(f"{'#'*80}")
        
        test_flattbom_with_body(
            token,
            creds["company_domain"],
            {
                "pageIds": page_ids[:2],
                "colorwayFilters": [],
                "filters": []
            },
            f"With 2 pageIds: {page_ids[:2]}"
        )
    
    # Test 3: With null values
    print(f"\n\n{'#'*80}")
    print("TEST GROUP 3: With null/undefined")
    print(f"{'#'*80}")
    
    test_flattbom_with_body(
        token,
        creds["company_domain"],
        {
            "pageIds": None,
            "colorwayFilters": None,
            "filters": None
        },
        "All null values"
    )
    
    # Test 4: Only pageIds field
    test_flattbom_with_body(
        token,
        creds["company_domain"],
        {
            "pageIds": page_ids[:1] if page_ids else []
        },
        "Only pageIds field"
    )
    
    print(f"\n\n{'='*80}")
    print("All tests completed")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
