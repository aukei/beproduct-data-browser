#!/usr/bin/env python3
"""
Test various request/query parameter combinations for FlatBom.
Try to get BOM data for the known style with PageBomVariation.
"""

import json
import requests
import os
from pathlib import Path
from dotenv import load_dotenv
from beproduct.sdk import BeProduct

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

def get_credentials():
    return {
        "client_id": os.getenv("BEPRODUCT_CLIENT_ID"),
        "client_secret": os.getenv("BEPRODUCT_CLIENT_SECRET"),
        "refresh_token": os.getenv("BEPRODUCT_REFRESH_TOKEN"),
        "company_domain": os.getenv("BEPRODUCT_COMPANY_DOMAIN"),
    }

def test_variant(token, company_domain, query_params, request_body, description):
    """Test a specific variant"""
    url = f"https://developers.beproduct.com/api/{company_domain}/Style/FlatBom"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    print(f"\n{'='*80}")
    print(f"{description}")
    print(f"{'='*80}")
    print(f"Query: {query_params}")
    print(f"Body: {json.dumps(request_body)}")
    
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
            if isinstance(data, dict) and "result" in data:
                result = data["result"]
                if result is None:
                    print("Result: null ❌")
                elif isinstance(result, list) and len(result) > 0:
                    print(f"✅ DATA FOUND! List with {len(result)} items")
                    return True
                else:
                    print(f"Result: {type(result).__name__} - {str(result)[:100]}")
        else:
            print(f"Error: {response.text[:100]}")
    
    except Exception as e:
        print(f"Exception: {e}")
    
    return False

def main():
    print("=" * 80)
    print("Testing BOM Variations")
    print("=" * 80)
    
    creds = get_credentials()
    client = BeProduct(**creds)
    token = client.oauth2_client.get_access_token()
    
    known_style_id = "80733ff7-9c24-46ac-bf95-7546b9d18cc6"
    
    # Test 1: No pageSize/pageNumber
    test_variant(
        token,
        creds["company_domain"],
        {},
        {"pageIds": [known_style_id], "colorwayFilters": [], "filters": []},
        "Test 1: No query params"
    )
    
    # Test 2: Different pageSize values
    test_variant(
        token,
        creds["company_domain"],
        {"pageSize": 100, "pageNumber": 1},
        {"pageIds": [known_style_id], "colorwayFilters": [], "filters": []},
        "Test 2: pageSize=100"
    )
    
    # Test 3: Very large pageSize
    test_variant(
        token,
        creds["company_domain"],
        {"pageSize": 1000, "pageNumber": 1},
        {"pageIds": [known_style_id], "colorwayFilters": [], "filters": []},
        "Test 3: pageSize=1000"
    )
    
    # Test 4: pageNumber=0
    test_variant(
        token,
        creds["company_domain"],
        {"pageSize": 10, "pageNumber": 0},
        {"pageIds": [known_style_id], "colorwayFilters": [], "filters": []},
        "Test 4: pageNumber=0 (zero-indexed)"
    )
    
    # Test 5: Without explicit fields
    test_variant(
        token,
        creds["company_domain"],
        {"pageSize": 10, "pageNumber": 1},
        {"pageIds": [known_style_id]},
        "Test 5: Only pageIds field"
    )
    
    # Test 6: Empty pageIds (should return all BOM?)
    test_variant(
        token,
        creds["company_domain"],
        {"pageSize": 10, "pageNumber": 1},
        {"pageIds": []},
        "Test 6: Empty pageIds"
    )
    
    # Test 7: null pageIds
    test_variant(
        token,
        creds["company_domain"],
        {"pageSize": 10, "pageNumber": 1},
        {"pageIds": None},
        "Test 7: null pageIds"
    )
    
    # Test 8: String instead of array
    test_variant(
        token,
        creds["company_domain"],
        {"pageSize": 10, "pageNumber": 1},
        {"pageIds": known_style_id},
        "Test 8: pageIds as string"
    )
    
    # Test 9: Check full response structure
    print(f"\n\n{'='*80}")
    print("Test 9: Full Response Analysis")
    print(f"{'='*80}")
    
    url = f"https://developers.beproduct.com/api/{creds['company_domain']}/Style/FlatBom"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(
        url,
        headers=headers,
        json={"pageIds": [known_style_id], "colorwayFilters": [], "filters": []},
        params={"pageSize": 10, "pageNumber": 1},
        timeout=30
    )
    
    print(f"Full response: {response.text}")
    
    print(f"\n\n{'='*80}")
    print("Completed all variations")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
