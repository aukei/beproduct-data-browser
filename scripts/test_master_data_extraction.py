#!/usr/bin/env python3
"""
Test BeProduct Master Data extraction locally
Loads credentials from .env file in project root
Usage: python3 scripts/test_master_data_extraction.py
"""

import json
import requests
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

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

def fetch_master_data(access_token, company_domain, field_id):
    """Fetch master data for a field"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    url = f"https://developers.beproduct.com/api/{company_domain}/MasterData/{field_id}"
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code != 200:
        print(f"❌ Failed to fetch {field_id}: {response.status_code}")
        return None
    
    return response.json()

def test_extraction(field_name, data):
    """Test the extraction logic"""
    print(f"\n{'='*80}")
    print(f"Field: {field_name}")
    print("="*80)
    
    choices = []
    
    if isinstance(data, dict) and "properties" in data:
        choices_data = data["properties"].get("Choices", [])
        
        print(f"\n📋 Choices structure:")
        print(f"   Type: {type(choices_data).__name__}")
        if isinstance(choices_data, (list, dict)):
            print(f"   Count: {len(choices_data)}")
        
        if isinstance(choices_data, list) and len(choices_data) > 0:
            choices = choices_data
        elif isinstance(choices_data, dict):
            choices = list(choices_data.values())
    
    if not choices:
        print("⚠️  No choices found")
        return []
    
    # Show first 3 choices structure
    print(f"\n📋 First 3 choices structure:")
    for idx, choice in enumerate(choices[:3]):
        if isinstance(choice, dict):
            print(f"\n   [{idx}]")
            for key in ["value", "name", "code", "id"]:
                val = choice.get(key)
                if isinstance(val, str) and len(val) > 40:
                    print(f"      {key}: {val[:37]}...")
                else:
                    print(f"      {key}: {repr(val)}")
            
            # Test extraction
            extracted = (
                choice.get("value") or 
                choice.get("name") or 
                choice.get("code") or 
                choice.get("id")
            )
            print(f"      → Extracted: {repr(extracted)}")
        else:
            print(f"\n   [{idx}] {repr(choice)}")
    
    # Process all choices
    rows = []
    for choice in choices:
        if isinstance(choice, dict):
            choice_value = (
                choice.get("value") or 
                choice.get("name") or 
                choice.get("code") or 
                choice.get("id")
            )
            rows.append({
                "value": choice_value,
                "label": choice_value,
            })
    
    print(f"\n📊 Results:")
    print(f"   Total rows: {len(rows)}")
    print(f"   Distinct values: {len(set(r['value'] for r in rows))}")
    if rows:
        samples = [r['value'] for r in rows[:3]]
        print(f"   Sample values: {samples}")
    
    return rows

def main():
    print("=" * 80)
    print("BeProduct Master Data Extraction Test")
    print("=" * 80)
    
    creds = get_credentials()
    print(f"\n✅ Loaded credentials for domain: {creds['company_domain']}")
    
    print("\n🔐 Authenticating...")
    token = get_access_token(creds)
    print("✅ Authenticated")
    
    # Test these fields
    test_fields = {
        "brands": "brands_multi",
        "factory": "factory",
        "garment_finish": "garment_finish",
        "parent_vendor": "parent_vendor",
    }
    
    results = {}
    
    for name, field_id in test_fields.items():
        print(f"\n📥 Fetching {name}...")
        data = fetch_master_data(token, creds["company_domain"], field_id)
        
        if data:
            print(f"✅ Fetched {field_id}")
            results[name] = test_extraction(name, data)
        else:
            print(f"❌ Failed to fetch {field_id}")
    
    # Summary
    print(f"\n{'='*80}")
    print("Summary")
    print("="*80)
    
    for name, rows in results.items():
        distinct = len(set(r['value'] for r in rows)) if rows else 0
        print(f"{name:20} {len(rows):3} rows, {distinct:3} distinct values")

if __name__ == "__main__":
    main()

