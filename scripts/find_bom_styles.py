#!/usr/bin/env python3
"""
Find styles that have BOM data populated.
Check for PageBomVariation indicator in headerData.
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
    print("Finding Styles with BOM Data")
    print("=" * 80)
    
    creds = get_credentials()
    print(f"\n✅ Domain: {creds['company_domain']}")
    
    print("\n🔐 Authenticating...")
    client = BeProduct(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        refresh_token=creds["refresh_token"],
        company_domain=creds["company_domain"]
    )
    
    print("📥 Scanning styles for BOM data...")
    
    styles_with_bom = []
    styles_checked = 0
    
    for style in client.style.attributes_list():
        styles_checked += 1
        
        if styles_checked % 50 == 0:
            print(f"   Checked {styles_checked} styles so far...")
        
        # Look for BOM indicators
        has_bom = False
        bom_indicators = []
        
        # Check 1: PageBomVariation in headerData
        if isinstance(style, dict):
            header_data = style.get("headerData")
            if isinstance(header_data, dict):
                fields = header_data.get("fields")
                if isinstance(fields, dict):
                    for field_key, field_value in fields.items():
                        if "PageBomVariation" in str(field_key) or "PageBomVariation" in str(field_value):
                            has_bom = True
                            bom_indicators.append(f"PageBomVariation in {field_key}")
            
            # Check 2: Any field with "bom" in the name
            for key in style.keys():
                if "bom" in key.lower():
                    has_bom = True
                    bom_indicators.append(f"BOM field: {key}")
            
            if has_bom:
                style_id = style.get("id") or style.get("styleId")
                styles_with_bom.append({
                    "styleId": style_id,
                    "indicators": bom_indicators,
                    "style": style
                })
        
        # Stop after checking 100 styles
        if styles_checked >= 100:
            break
    
    print(f"\n{'='*80}")
    print(f"Results")
    print(f"{'='*80}")
    print(f"Styles checked: {styles_checked}")
    print(f"Styles with BOM indicators: {len(styles_with_bom)}")
    
    if styles_with_bom:
        print(f"\n✅ Found {len(styles_with_bom)} styles with BOM indicators:")
        for item in styles_with_bom[:10]:  # Show first 10
            print(f"\n  Style ID: {item['styleId']}")
            print(f"  Indicators: {', '.join(item['indicators'])}")
    else:
        print(f"\n⚠️ No styles found with BOM indicators in first {styles_checked} styles")
        print("\nThis suggests:")
        print("1. BOM data is not populated in the test database")
        print("2. OR the BOM indicator field names are different")
        print("3. OR BOM is stored in a way we haven't detected")

if __name__ == "__main__":
    main()
