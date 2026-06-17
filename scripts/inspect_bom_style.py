#!/usr/bin/env python3
"""
Inspect the known BOM style to understand its structure.
Check what fields and data it contains.
"""

import json
import os
from pathlib import Path
from dotenv import load_dotenv
from beproduct.sdk import BeProduct

# Load .env from project root
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

def get_credentials():
    """Get BeProduct credentials from environment"""
    return {
        "client_id": os.getenv("BEPRODUCT_CLIENT_ID"),
        "client_secret": os.getenv("BEPRODUCT_CLIENT_SECRET"),
        "refresh_token": os.getenv("BEPRODUCT_REFRESH_TOKEN"),
        "company_domain": os.getenv("BEPRODUCT_COMPANY_DOMAIN"),
    }

def main():
    print("=" * 80)
    print("Inspecting Known BOM Style")
    print("=" * 80)
    print("\nStyle: LFBP-127LY1MO-001")
    print("StyleId: 80733ff7-9c24-46ac-bf95-7546b9d18cc6")
    
    creds = get_credentials()
    client = BeProduct(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        refresh_token=creds["refresh_token"],
        company_domain=creds["company_domain"]
    )
    
    known_style_id = "80733ff7-9c24-46ac-bf95-7546b9d18cc6"
    
    print(f"\n🔍 Searching for style in API...")
    
    # Find the style by iterating through list
    style = None
    try:
        for idx, s in enumerate(client.style.attributes_list()):
            if isinstance(s, dict):
                # Check various ID fields
                if s.get("id") == known_style_id or s.get("styleId") == known_style_id:
                    style = s
                    print(f"✅ Found at position {idx}")
                    break
            
            if idx >= 100:  # Limit search
                break
        
        if not style:
            print(f"❌ Style not found in first 100 styles")
            print(f"Trying with API token directly...")
            import requests
            
            token = client.oauth2_client.get_access_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            # Try direct API call
            response = requests.get(
                f"https://developers.beproduct.com/api/{creds['company_domain']}/Style/{known_style_id}",
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                style = response.json()
                print(f"✅ Retrieved via direct API call")
            else:
                print(f"❌ Direct API call failed: {response.status_code}")
                return
        
        print(f"\n✅ Retrieved style successfully")
        print(f"\nTop-level keys ({len(style.keys())} total):")
        for key in sorted(style.keys()):
            val = style[key]
            if isinstance(val, (str, int, float, bool)):
                if isinstance(val, str) and len(val) > 60:
                    print(f"  {key}: {val[:57]}... ({type(val).__name__})")
                else:
                    print(f"  {key}: {val} ({type(val).__name__})")
            elif isinstance(val, list):
                print(f"  {key}: [{len(val)} items]")
            elif isinstance(val, dict):
                print(f"  {key}: {{{len(val)} keys}}")
            else:
                print(f"  {key}: {type(val).__name__}")
        
        # Check for BOM-related fields
        print(f"\n🔎 Searching for BOM-related fields:")
        bom_fields = []
        for key in style.keys():
            if "bom" in key.lower():
                bom_fields.append(key)
        
        if bom_fields:
            print(f"✅ Found {len(bom_fields)} BOM-related fields:")
            for field in bom_fields:
                val = style[field]
                print(f"  {field}: {type(val).__name__}")
                if isinstance(val, dict):
                    print(f"    Keys: {list(val.keys())[:10]}")
                elif isinstance(val, list):
                    print(f"    Length: {len(val)}")
        else:
            print(f"⚠️  No BOM-related fields found")
        
        # Check headerData for PageModified or BOM indicator
        print(f"\n🔎 Checking headerData:")
        header_data = style.get("headerData")
        if isinstance(header_data, dict):
            print(f"  headerData keys: {list(header_data.keys())}")
            
            fields = header_data.get("fields")
            if isinstance(fields, dict):
                print(f"  fields ({len(fields)} total):")
                for field_key, field_val in list(fields.items())[:20]:
                    if "bom" in str(field_key).lower() or "bom" in str(field_val).lower():
                        print(f"    ✅ {field_key}: {field_val}")
                    else:
                        print(f"    {field_key}: {field_val}")
        
        # Show full structure (first 2000 chars)
        print(f"\n\nFull style object (JSON, first 2000 chars):")
        print("=" * 80)
        full_json = json.dumps(style, indent=2, default=str)
        print(full_json[:2000])
        if len(full_json) > 2000:
            print(f"\n... ({len(full_json) - 2000} more characters)")
    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
