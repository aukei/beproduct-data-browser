import os
import json
from app.beproduct_client import get_client

def check_schema():
    try:
        client = get_client()
    except Exception as e:
        print(f"FAILED_TO_GET_CLIENT: {e}")
        return

    # Mappings based on discovered methods
    folder_schema_map = {
        "Style": "get_style_folder_schema",
        "Material": "get_material_folder_schema",
        "Color": "get_color_folder_schema",
        "Image": "get_folder_schema" # Image might use general one or I can check for get_image_folder_schema
    }
    
    # Check if 'get_image_folder_schema' exists
    if hasattr(client.schema, 'get_image_folder_schema'):
        folder_schema_map["Image"] = "get_image_folder_schema"

    folders = ["Style", "Material", "Color", "Image"]
    
    for folder_name in folders:
        print(f"\n--- {folder_name} Schema ---")
        try:
            method_name = folder_schema_map.get(folder_name) or "get_folder_schema"
            schema_method = getattr(client.schema, method_name, None)
            
            if not schema_method:
                 print(f"Method {method_name} not found.")
                 continue
                 
            # Some methods might need the folder name as an argument, others (specialized) might not.
            # get_style_folder_schema likely doesn't need "Style", but get_folder_schema does.
            if "get_folder_schema" == method_name:
                schema = schema_method(folder_name)
            else:
                schema = schema_method()
            
            if not schema:
                print(f"No schema returned for {folder_name}")
                continue
                
            total_entries = len(schema)
            required_fields = [f for f in schema if f.get('required')]
            
            print(f"Total entries: {total_entries}")
            print(f"Required fields count: {len(required_fields)}")
            print("Required fields detail:")
            for rf in required_fields:
                has_values = "Yes" if rf.get('possible_values') or rf.get('choices') or rf.get('enum') else "No"
                print(f"  - ID: {rf.get('id')}, Type: {rf.get('type') or rf.get('dataType')}, Has Possible Values: {has_values}")

            # Sample record
            print(f"Sampling {folder_name} records...")
            folder_api = getattr(client, folder_name.lower(), None)
            if folder_api:
                # Try 'get_master_list' or 'search' or 'list'
                # Based on previous output, Style has methods... let's check
                list_method = getattr(folder_api, 'get_master_list', None) or getattr(folder_api, 'get_list', None)
                if not list_method:
                     # Check what's available
                     methods = [m for m in dir(folder_api) if not m.startswith('_')]
                     if "get_master_list" in methods: list_method = folder_api.get_master_list
                     elif "search" in methods: list_method = folder_api.search
                
                if list_method:
                    try:
                        # Some list methods might use 'pageSize', 'pagesize', or just positional args
                        records = list_method(pageSize=1)
                    except:
                        try: records = list_method(pagesize=1)
                        except: records = list_method()

                    rec_list = []
                    if isinstance(records, list):
                        rec_list = records
                    elif isinstance(records, dict):
                        rec_list = records.get('results') or records.get('data') or []
                    
                    if rec_list:
                        sample = rec_list[0]
                        print(f"Top-level keys sample: {list(sample.keys())[:20]}")
                        found = [c for c in ['headerNumber', 'headerName', 'colorPaletteNumber', 'colorPaletteName'] if c in sample]
                        print(f"Specific fields found: {found if found else 'None'}")
                    else:
                        print(f"No records found in {folder_name}")
                else:
                    print(f"No list method found. Methods: {[m for m in dir(folder_api) if not m.startswith('_')]}")
            else:
                print(f"No specific API attribute for {folder_name}")

        except Exception as e:
            print(f"Error processing {folder_name}: {e}")

if __name__ == "__main__":
    check_schema()
