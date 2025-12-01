#!/usr/bin/env python3
"""
Regression test to ensure template headers match importer requirements.
This script can be used in CI/CD pipelines to catch schema mismatches.
"""

def check_template_alignment():
    """Check that template headers align with importer and database schema."""
    
    # Headers from create_template.py (lines 29-32)
    template_headers = [
        "Main_Category", "Sub_Category", "Service_Name", "Variant_Name", 
        "Price_USD", "Start_Time", "Duration", "Refill_Period", "Additional_Info"
    ]
    
    # Headers from cogs/product_import.py (lines 35-39) 
    importer_headers = [
        'Main_Category', 'Sub_Category', 'Service_Name', 
        'Variant_Name', 'Price_USD', 'Start_Time', 
        'Duration', 'Refill_Period', 'Additional_Info'
    ]
    
    # Database fields from apex_core/database.py (products table, lines 135-143)
    db_fields = [
        'main_category', 'sub_category', 'service_name', 'variant_name',
        'price_cents', 'start_time', 'duration', 'refill_period', 'additional_info'
    ]
    
    # Check alignment
    if template_headers != importer_headers:
        print("❌ MISMATCH: Template headers don't match importer requirements")
        print(f"Template: {template_headers}")
        print(f"Importer: {importer_headers}")
        return False
    
    # Map CSV headers to database fields
    csv_to_db = {
        'Main_Category': 'main_category',
        'Sub_Category': 'sub_category',
        'Service_Name': 'service_name', 
        'Variant_Name': 'variant_name',
        'Price_USD': 'price_cents',  # Converted from USD to cents
        'Start_Time': 'start_time',
        'Duration': 'duration',
        'Refill_Period': 'refill_period',
        'Additional_Info': 'additional_info'
    }
    
    # Check all CSV headers map to valid database fields
    for csv_header in template_headers:
        if csv_header not in csv_to_db:
            print(f"❌ ERROR: CSV header '{csv_header}' has no database mapping")
            return False
        
        db_field = csv_to_db[csv_header]
        if db_field not in db_fields:
            print(f"❌ ERROR: CSV header '{csv_header}' maps to unknown database field '{db_field}'")
            return False
    
    print("✅ Template headers are properly aligned with importer and database schema")
    return True

if __name__ == "__main__":
    import sys
    success = check_template_alignment()
    sys.exit(0 if success else 1)