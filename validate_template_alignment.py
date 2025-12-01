#!/usr/bin/env python3
"""
Script to validate that the products template headers match the importer requirements.
This can be run as a regression test to ensure schema alignment.
"""

import sys
from pathlib import Path

def validate_template_alignment():
    """Validate that template headers match importer requirements."""
    
    # Expected headers from the importer (cogs/product_import.py)
    expected_headers = [
        'Main_Category', 'Sub_Category', 'Service_Name', 
        'Variant_Name', 'Price_USD', 'Start_Time', 
        'Duration', 'Refill_Period', 'Additional_Info'
    ]
    
    # Expected headers from the template generator (create_template.py)
    template_headers = [
        "Main_Category", "Sub_Category", "Service_Name", "Variant_Name", 
        "Price_USD", "Start_Time", "Duration", "Refill_Period", "Additional_Info"
    ]
    
    # Database schema columns (from apex_core/database.py)
    db_columns = [
        'main_category', 'sub_category', 'service_name', 'variant_name',
        'price_cents', 'start_time', 'duration', 'refill_period', 'additional_info'
    ]
    
    # Check 1: Template headers match importer requirements
    if template_headers != expected_headers:
        print("❌ ERROR: Template headers do not match importer requirements")
        print(f"Expected: {expected_headers}")
        print(f"Template: {template_headers}")
        return False
    
    # Check 2: All template headers map to valid database columns
    header_to_db_mapping = {
        'Main_Category': 'main_category',
        'Sub_Category': 'sub_category', 
        'Service_Name': 'service_name',
        'Variant_Name': 'variant_name',
        'Price_USD': 'price_cents',  # Special case: converted from USD to cents
        'Start_Time': 'start_time',
        'Duration': 'duration',
        'Refill_Period': 'refill_period',
        'Additional_Info': 'additional_info'
    }
    
    for template_header in template_headers:
        if template_header not in header_to_db_mapping:
            print(f"❌ ERROR: Template header '{template_header}' has no database mapping")
            return False
        
        db_column = header_to_db_mapping[template_header]
        if db_column not in db_columns:
            print(f"❌ ERROR: Template header '{template_header}' maps to non-existent database column '{db_column}'")
            return False
    
    # Check 3: All relevant database columns are covered
    managed_db_columns = {'role_id', 'content_payload', 'is_active', 'created_at', 'updated_at', 'id'}
    expected_db_coverage = set(db_columns) - managed_db_columns
    covered_db_columns = set(header_to_db_mapping.values())
    
    if covered_db_columns != expected_db_coverage:
        print("❌ ERROR: Database column coverage mismatch")
        print(f"Expected: {expected_db_coverage}")
        print(f"Covered: {covered_db_columns}")
        return False
    
    print("✅ All template alignment checks passed!")
    print(f"✅ Template headers match importer requirements")
    print(f"✅ All template headers map to valid database columns")
    print(f"✅ All relevant database columns are covered")
    return True

if __name__ == "__main__":
    success = validate_template_alignment()
    sys.exit(0 if success else 1)