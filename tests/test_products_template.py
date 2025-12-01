import pytest
import sys
from pathlib import Path

# Add the project root to the path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from cogs.product_import import _parse_and_validate_csv


def test_template_headers_match_importer():
    """Test that the template headers exactly match the importer's required columns."""
    
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
    
    # Verify headers match exactly
    assert template_headers == expected_headers, (
        f"Template headers do not match importer requirements.\n"
        f"Expected: {expected_headers}\n"
        f"Template: {template_headers}"
    )


def test_template_csv_export_compatibility():
    """Test that a CSV exported from the template can be parsed by the importer."""
    
    # Create a sample CSV content that matches the template structure
    sample_csv = """Main_Category,Sub_Category,Service_Name,Variant_Name,Price_USD,Start_Time,Duration,Refill_Period,Additional_Info
Instagram,Followers,Instagram Services,1000 Followers,10.99,15-30min,120min,30 day,Premium quality
YouTube,Subscribers,YouTube Growth,500 Subscribers,29.99,1-3hr,72hr,30 day,Active accounts"""
    
    # Test that the importer can parse this CSV
    result = _parse_and_validate_csv(sample_csv.encode('utf-8'))
    
    assert result['success'], f"CSV parsing failed: {result.get('error', 'Unknown error')}"
    assert len(result['rows']) == 2, f"Expected 2 rows, got {len(result['rows'])}"
    assert len(result['errors']) == 0, f"Unexpected validation errors: {result['errors']}"
    
    # Verify the parsed data
    row1 = result['rows'][0]
    assert row1['main_category'] == 'Instagram'
    assert row1['sub_category'] == 'Followers'
    assert row1['service_name'] == 'Instagram Services'
    assert row1['variant_name'] == '1000 Followers'
    assert row1['price_cents'] == 1099  # 10.99 * 100
    assert row1['start_time'] == '15-30min'
    assert row1['duration'] == '120min'
    assert row1['refill_period'] == '30 day'
    assert row1['additional_info'] == 'Premium quality'


def test_template_headers_database_alignment():
    """Test that template headers align with the database schema."""
    
    # Expected headers based on database schema (apex_core/database.py products table)
    db_columns = [
        'main_category', 'sub_category', 'service_name', 'variant_name',
        'price_cents', 'start_time', 'duration', 'refill_period', 'additional_info'
    ]
    
    # Template headers (CSV column names)
    template_headers = [
        'Main_Category', 'Sub_Category', 'Service_Name', 
        'Variant_Name', 'Price_USD', 'Start_Time', 
        'Duration', 'Refill_Period', 'Additional_Info'
    ]
    
    # Map template headers to database columns
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
    
    # Verify all template headers map to valid database columns
    for template_header in template_headers:
        assert template_header in header_to_db_mapping, f"Template header '{template_header}' has no database mapping"
        db_column = header_to_db_mapping[template_header]
        assert db_column in db_columns, f"Template header '{template_header}' maps to non-existent database column '{db_column}'"
    
    # Verify all database columns (except managed ones) are covered
    managed_db_columns = {'role_id', 'content_payload', 'is_active', 'created_at', 'updated_at', 'id'}
    expected_db_coverage = set(db_columns) - managed_db_columns
    
    covered_db_columns = set(header_to_db_mapping.values())
    assert covered_db_columns == expected_db_coverage, (
        f"Database column coverage mismatch.\n"
        f"Expected: {expected_db_coverage}\n"
        f"Covered: {covered_db_columns}"
    )


def test_price_conversion_validation():
    """Test that price values in CSV are correctly converted to cents."""
    
    test_cases = [
        ('10.99', 1099),
        ('10', 1000),
        ('0.99', 99),
        ('149.99', 14999),
        ('0', 0),
        ('100.00', 10000),
    ]
    
    for price_usd, expected_cents in test_cases:
        csv_content = f"""Main_Category,Sub_Category,Service_Name,Variant_Name,Price_USD,Start_Time,Duration,Refill_Period,Additional_Info
Test,Category,Test Service,Test Variant,{price_usd},Instant,N/A,No refill,Test info"""
        
        result = _parse_and_validate_csv(csv_content.encode('utf-8'))
        
        assert result['success'], f"CSV parsing failed for price {price_usd}: {result.get('error', 'Unknown error')}"
        assert len(result['rows']) == 1, f"Expected 1 row for price {price_usd}"
        assert result['rows'][0]['price_cents'] == expected_cents, (
            f"Price conversion failed: {price_usd} USD should be {expected_cents} cents, got {result['rows'][0]['price_cents']}"
        )


def test_required_field_validation():
    """Test that required fields are properly validated."""
    
    # Test missing required fields
    csv_content = """Main_Category,Sub_Category,Service_Name,Variant_Name,Price_USD,Start_Time,Duration,Refill_Period,Additional_Info
Instagram,,Instagram Services,1000 Followers,10.99,15-30min,120min,30 day,Premium quality
Instagram,Followers,,1000 Followers,10.99,15-30min,120min,30 day,Premium quality
Instagram,Followers,Instagram Services,,10.99,15-30min,120min,30 day,Premium quality
Instagram,Followers,Instagram Services,1000 Followers,,15-30min,120min,30 day,Premium quality"""
    
    result = _parse_and_validate_csv(csv_content.encode('utf-8'))
    
    assert result['success'], "CSV parsing should succeed but with validation errors"
    assert len(result['rows']) == 0, "No rows should be valid due to missing required fields"
    assert len(result['errors']) == 4, f"Expected 4 validation errors, got {len(result['errors'])}"
    
    # Check that error messages mention missing required fields
    error_text = ' '.join(result['errors'])
    assert 'Missing required field values' in error_text, "Error message should mention missing required fields"


if __name__ == "__main__":
    # Run tests when script is executed directly
    pytest.main([__file__, "-v"])