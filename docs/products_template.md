# Products CSV Template Guide

## Overview

This guide explains how to use the products CSV template for bulk product import into the Apex Core Discord Bot. The template provides a structured way to add multiple products at once using the `/import_products` command.

## Template Location

The official template is located at: `templates/products_template.xlsx`

You can regenerate this template at any time by running:
```bash
python3 create_template.py
```

## CSV Column Mapping

The CSV contains 9 columns that map directly to the database schema:

| CSV Column | Database Field | Required? | Description | Examples |
|------------|----------------|-----------|-------------|----------|
| **Main_Category** | `main_category` | ✅ Yes | Primary platform or service type | `Instagram`, `YouTube`, `TikTok`, `Xbox`, `ChatGPT` |
| **Sub_Category** | `sub_category` | ✅ Yes | Specific service type within the main category | `Followers`, `Likes`, `Comments`, `Subscribers`, `Game Pass` |
| **Service_Name** | `service_name` | ✅ Yes | Internal grouping name for related products | `Instagram Services`, `YouTube Growth`, `TikTok Services` |
| **Variant_Name** | `variant_name` | ✅ Yes | Customer-facing product name (what they see in storefront) | `1000 Followers`, `Xbox Game Pass Ultimate - 1 Month` |
| **Price_USD** | `price_cents` | ✅ Yes | Price in US dollars (decimal format) | `5.99`, `10`, `149.99` |
| **Start_Time** | `start_time` | ❌ No | When delivery begins after purchase | `10-25min`, `1-3hr`, `24-48hr`, `Instant` |
| **Duration** | `duration` | ❌ No | How long delivery takes to complete | `100min`, `72hr`, `N/A` |
| **Refill_Period** | `refill_period` | ❌ No | Guarantee/warranty period for the service | `30 day`, `60 day`, `No refill` |
| **Additional_Info** | `additional_info` | ❌ No | Extra details, requirements, or special notes | `High quality, real looking accounts` |

## Required vs Optional Fields

### Required Fields (must be non-empty):
- **Main_Category**, **Sub_Category**, **Service_Name**, **Variant_Name**: These identify the product and enable storefront grouping
- **Price_USD**: Must be a positive number (0 or greater)

### Optional Fields (can be empty):
- **Start_Time**, **Duration**, **Refill_Period**, **Additional_Info**: Leave empty if not applicable

## Price Conversion

Prices are entered in US dollars as decimal numbers and automatically converted to cents for storage:

| Input (USD) | Stored (cents) | Display |
|-------------|----------------|---------|
| `5.99` | `599` | $5.99 |
| `10` | `1000` | $10.00 |
| `149.99` | `14999` | $149.99 |

## Storefront Grouping Logic

The products are organized in the Discord storefront using this hierarchy:

1. **Main_Category** → Top-level grouping (e.g., "Instagram")
2. **Sub_Category** → Service type within category (e.g., "Followers") 
3. **Service_Name** → Internal grouping (not shown to users, used for organization)
4. **Variant_Name** → Individual product that customers can purchase

**Example hierarchy:**
```
Instagram (Main_Category)
├── Followers (Sub_Category)
│   ├── 100 Followers (Variant_Name)
│   ├── 500 Followers (Variant_Name)
│   └── 1000 Followers (Variant_Name)
└── Likes (Sub_Category)
    ├── 100 Likes (Variant_Name)
    └── 500 Likes (Variant_Name)
```

## Step-by-Step Instructions

### 1. Get the Template
- Download `templates/products_template.xlsx` from the repository
- Or regenerate it: `python3 create_template.py`

### 2. Review the Excel Template
The template contains 3 sheets:
- **Products**: Main data entry sheet with example rows
- **Instructions**: Step-by-step usage guide
- **Column Guide**: Detailed field explanations

### 3. Add Your Products
1. Open the template in Excel, Google Sheets, or LibreOffice
2. Delete the example rows (keep the header row)
3. Add your products starting from row 2
4. Follow the column format exactly

### 4. Format Guidelines
- **Price_USD**: Enter as decimal number (e.g., `20` for $20.00, `5.99` for $5.99)
- **Start_Time**: Examples: `10-25min`, `1-3hr`, `24-48hr`, `Instant`
- **Duration**: Examples: `100min`, `72hr`, `N/A` for instant delivery
- **Refill_Period**: Examples: `30 day`, `60 day`, `No refill`
- **Additional_Info**: Any special notes or requirements

### 5. Export to CSV
1. Make sure the "Products" sheet is active
2. File → Save As
3. Choose "CSV (Comma delimited) (*.csv)" format
4. Click "Yes" when Excel warns about features not being compatible
5. Save the CSV file

### 6. Import to Discord
1. In Discord, use the `/import_products` command (admin only)
2. Attach your CSV file
3. The bot will validate and import all products

## Validation Rules

The bot enforces these validation rules during import:

### Required Field Validation
- All required fields must be non-empty after trimming whitespace
- Price must be a valid positive number

### Data Type Validation
- **Price_USD**: Must parse as a float and be ≥ 0
- **All fields**: Text fields are stripped of leading/trailing whitespace

### Duplicate Detection
- Products with identical Main_Category, Sub_Category, Service_Name, and Variant_Name are treated as updates
- Existing products are updated with new price and metadata
- Products not in the CSV are deactivated (set to inactive)

## Example CSV Row

```csv
Main_Category,Sub_Category,Service_Name,Variant_Name,Price_USD,Start_Time,Duration,Refill_Period,Additional_Info
Instagram,Followers,Instagram Services,1000 Followers,10.99,15-30min,120min,30 day,Premium quality with engagement
```

## Database Fields Not Managed by CSV

These database fields are managed separately, not through CSV import:

- **role_id**: Discord role to assign on purchase (managed via database/admin commands)
- **content_payload**: Delivery content/URLs (managed via database/admin commands)  
- **is_active**: Product availability status (automatically managed by import)
- **created_at/updated_at**: Timestamps (automatically managed)

## Common Issues and Solutions

### CSV Import Fails
- **Check encoding**: Ensure CSV is saved as UTF-8
- **Verify headers**: All 9 columns must be present with exact spelling
- **Check for empty required fields**: Main_Category, Sub_Category, Service_Name, Variant_Name, Price_USD

### Products Not Showing in Storefront
- **Check is_active status**: Only active products appear
- **Verify category structure**: Products need proper Main_Category and Sub_Category
- **Check user permissions**: Some products may require specific roles

### Price Issues
- **Use decimal format**: Enter `10.99` not `1099`
- **No currency symbols**: Just the number, no `$` or other symbols
- **Positive numbers only**: Negative prices are rejected

## Regenerating the Template

If you need to update the template structure:

1. Edit `create_template.py` to modify headers, example data, or instructions
2. Run the script: `python3 create_template.py`
3. Commit the updated `products_template.xlsx` to the repository

The template generator script includes:
- Professional formatting (colored headers, frozen panes)
- 16 diverse example rows covering multiple platforms
- Comprehensive instructions and column guides
- Auto-fitted column widths

## Testing Your CSV

Before importing, you can validate your CSV format:

1. Open in a text editor to check headers match exactly
2. Verify no extra columns or missing columns
3. Check that all required fields are filled
4. Ensure price values are valid positive numbers

## Support

For issues with product import:
1. Check this guide for common solutions
2. Review the Excel template's built-in instructions
3. Verify your CSV matches the required format exactly
4. Check bot logs for detailed error messages

The import command provides detailed feedback on what was added, updated, or had validation errors.

## Regression Testing

A regression test script is provided to ensure template alignment:

```bash
python3 check_template_alignment.py
```

This script validates:
- Template headers match importer requirements
- All CSV headers map to valid database fields  
- Database schema coverage is complete

For comprehensive testing, the test suite includes:
```bash
pytest tests/test_products_template.py -v
```