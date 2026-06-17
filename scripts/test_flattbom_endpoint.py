#!/usr/bin/env python3
"""
Test BeProduct FlatBom endpoint with proper Swagger-defined parameters.
Tests POST /api/{company}/Report/FlatBom and /api/{company}/Style/FlatBom
Loads credentials from .env file in project root

Usage: python3 scripts/test_flattbom_endpoint.py
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
        print("   Required: BEPRODUCT_CLIENT_ID, BEPRODUCT_CLIENT_SECRET, BEPRODUCT_REFRESH_TOKEN, BEPRODUCT_COMPANY_DOMAIN")
        sys.exit(1)
    
    return creds

def get_access_token(creds):
    """Get OAuth2 access token"""
    auth_url = "https://id.winks.io/ids/connect/token"
    payload = {
        "grant_type": "refresh_token",
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
        "refresh_token": creds["refresh_token"]
    }
    
    response = requests.post(auth_url, data=payload, timeout=10)
    if response.status_code != 200:
        print(f"❌ Auth failed: {response.text}")
        sys.exit(1)
    
    return response.json()["access_token"]

def fetch_styles(client, limit=10):
    """Fetch a sample of styles to test with using BeProduct SDK"""
    try:
        styles = []
        for idx, style in enumerate(client.style.attributes_list()):
            styles.append(style)
            if idx >= limit - 1:
                break
        
        if not styles:
            print("⚠️  No styles returned from API")
            return []
        
        print(f"✅ Fetched {len(styles)} styles")
        return styles
    
    except Exception as e:
        print(f"❌ Error fetching styles: {e}")
        return []

def test_flattbom_endpoint(access_token, company_domain, endpoint_type="Report", style_ids=None, use_filters=False):
    """Test FlatBom endpoint with proper Swagger parameters or filters-based approach"""
    # Query params
    query_params = {
        "pageSize": 1,
        "pageNumber": 1
    }
    
    url = f"https://developers.beproduct.com/api/{company_domain}/{endpoint_type}/FlatBom"
    
    request_type = "filters-based" if use_filters else "styleId-based"
    
    print(f"\n{'='*80}")
    print(f"Testing {endpoint_type}/FlatBom endpoint ({request_type})")
    print(f"{'='*80}")
    print(f"URL: {url}")
    print(f"Query params: {query_params}")
    print(f"Content-Type: {'application/json-patch+json' if use_filters else 'application/json'}")
    
    if not style_ids:
        print("⚠️  No style IDs provided")
        return
    
    results = []
    
    for idx, style_id in enumerate(style_ids[:5], 1):  # Test first 5 styles
        # Build request body based on approach
        if use_filters:
            body = {
                "filters": [
                    {
                        "field": "styleID",
                        "operator": "=",
                        "value": style_id,
                        "type": "String"
                    }
                ]
            }
            content_type = "application/json-patch+json"
        else:
            body = {
                "styleId": style_id
            }
            content_type = "application/json"
        
        print(f"\n[{idx}] Testing styleId: {style_id}")
        print(f"    Body: {json.dumps(body)}")
        
        try:
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": content_type
            }
            
            response = requests.post(
                url,
                headers=headers,
                json=body,
                params=query_params,
                timeout=30
            )
            
            print(f"    Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"    Response structure: {type(data).__name__}")
                
                # Check for result field
                if isinstance(data, dict):
                    if "result" in data:
                        result = data["result"]
                        if result is None:
                            print(f"    ⚠️  Result is null")
                        elif isinstance(result, list):
                            print(f"    ✅ Result is list with {len(result)} items")
                            if result:
                                print(f"       First item keys: {list(result[0].keys()) if isinstance(result[0], dict) else 'N/A'}")
                        elif isinstance(result, dict):
                            print(f"    ✅ Result is dict with keys: {list(result.keys())}")
                        else:
                            print(f"    Result type: {type(result).__name__}")
                    else:
                        print(f"    Response keys: {list(data.keys())}")
                        print(f"    Full response: {json.dumps(data, indent=2)[:500]}")
                
                results.append({
                    "styleId": style_id,
                    "status": response.status_code,
                    "hasResult": "result" in data if isinstance(data, dict) else False,
                    "resultIsNull": data.get("result") is None if isinstance(data, dict) else None,
                    "requestType": request_type
                })
            else:
                print(f"    ❌ Error: {response.text[:200]}")
                results.append({
                    "styleId": style_id,
                    "status": response.status_code,
                    "hasResult": False,
                    "resultIsNull": None,
                    "requestType": request_type
                })
        
        except Exception as e:
            print(f"    ❌ Exception: {e}")
            results.append({
                "styleId": style_id,
                "status": None,
                "hasResult": False,
                "resultIsNull": None,
                "requestType": request_type
            })
    
    return results

def main():
    print("=" * 80)
    print("BeProduct FlatBom Endpoint Test")
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
    styles = fetch_styles(client, limit=20)
    
    if not styles:
        print("❌ Could not fetch styles to test with")
        sys.exit(1)
    
    # Extract styleIds from the returned objects
    style_ids = []
    for style in styles:
        if isinstance(style, dict):
            # Try different field names that might contain the ID
            for field in ["styleId", "id", "header_id", "headerId"]:
                if field in style:
                    style_ids.append(style[field])
                    break
    
    print(f"✅ Extracted {len(style_ids)} style IDs for testing")
    if style_ids:
        print(f"   Sample IDs: {style_ids[:3]}")
    
    # Get token for direct HTTP calls
    token = client.oauth2_client.get_access_token()
    
    # Test both endpoints with both request formats
    all_results = {}
    
    for endpoint_type in ["Report", "Style"]:
        # Test styleId-based approach
        print(f"\n{'#'*80}")
        print(f"TESTING {endpoint_type}/FlatBom WITH STYLEID-BASED APPROACH")
        print(f"{'#'*80}")
        results_styleid = test_flattbom_endpoint(
            token,
            creds["company_domain"],
            endpoint_type=endpoint_type,
            style_ids=style_ids,
            use_filters=False
        )
        all_results[f"{endpoint_type}_styleId"] = results_styleid
        
        # Test filters-based approach
        print(f"\n{'#'*80}")
        print(f"TESTING {endpoint_type}/FlatBom WITH FILTERS-BASED APPROACH")
        print(f"{'#'*80}")
        results_filters = test_flattbom_endpoint(
            token,
            creds["company_domain"],
            endpoint_type=endpoint_type,
            style_ids=style_ids,
            use_filters=True
        )
        all_results[f"{endpoint_type}_filters"] = results_filters
    
    # Summary
    print(f"\n\n{'='*80}")
    print("SUMMARY - All Test Results")
    print("="*80)
    
    summary_by_approach = {}
    
    for test_key, results in all_results.items():
        if not results:
            print(f"\n{test_key}: No results")
            continue
        
        success_count = sum(1 for r in results if r["status"] == 200)
        null_count = sum(1 for r in results if r.get("resultIsNull") is True)
        has_data = sum(1 for r in results if r.get("resultIsNull") is False)
        
        approach = results[0].get("requestType", "unknown")
        if approach not in summary_by_approach:
            summary_by_approach[approach] = {"endpoints": {}}
        
        print(f"\n{test_key}:")
        print(f"  HTTP 200: {success_count}/{len(results)}")
        print(f"  Result is null: {null_count}")
        print(f"  Result has data: {has_data}")
        
        if has_data > 0:
            print(f"  ✅ At least one request returned data!")
        else:
            print(f"  ⚠️  All responses have null result")
    
    # Approach comparison
    print(f"\n\n{'='*80}")
    print("Approach Comparison")
    print("="*80)
    
    for approach in ["styleId-based", "filters-based"]:
        matching_results = [
            (k, v) for k, v in all_results.items() 
            if v and v[0].get("requestType") == approach
        ]
        
        if matching_results:
            print(f"\n{approach.upper()}:")
            total_with_data = sum(1 for k, v in matching_results for r in v if r.get("resultIsNull") is False)
            if total_with_data > 0:
                print(f"  ✅ SUCCESS! Found data with {approach}")
            else:
                print(f"  ⚠️  No data returned with {approach}")

if __name__ == "__main__":
    main()
