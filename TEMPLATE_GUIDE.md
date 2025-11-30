# Products Template Guide

## Overview

The `products_template.xlsx` file is a professional Excel template designed for easy bulk product management in the Apex Core Discord Bot.

## File Structure

The template contains three sheets:

### 1. Products Sheet
- **Purpose**: Main data entry area for product information
- **Headers**: 9 columns with formatted headers (bold, blue background, size 12)
- **Features**:
  - Frozen header row for easy scrolling
  - Auto-fitted column widths
  - 16 example rows with real-world data
- **Example Categories**:
  - Instagram (Followers, Likes, Comments)
  - YouTube (Subscribers, Watch Time, Likes)
  - TikTok (Followers, Likes, Views)
  - Xbox (Game Pass variants)
  - ChatGPT (Subscription tiers)

### 2. Instructions Sheet
- **Purpose**: Step-by-step user guide
- **Content**:
  - How to review and use example data
  - Deleting example rows
  - Adding custom products
  - Format guidelines for each field
  - Saving as Excel (.xlsx)
  - Converting to CSV for Discord import
  - Using the `/import_products` command
  - Example row format
  - Tips and best practices

### 3. Column Guide Sheet
- **Purpose**: Detailed explanation of each column
- **Content**: Field-by-field reference with examples for:
  - Main_Category
  - Sub_Category
  - Service_Name
  - Variant_Name
  - Price_USD
  - Start_Time
  - Duration
  - Refill_Period
  - Additional_Info

## Column Descriptions

| Column | Description | Example |
|--------|-------------|---------|
| **Main_Category** | Primary platform/service | Instagram, YouTube, TikTok |
| **Sub_Category** | Specific service type | Followers, Likes, Subscribers |
| **Service_Name** | Internal grouping name | "Instagram Services" |
| **Variant_Name** | Customer-facing product name | "1000 Followers" |
| **Price_USD** | Price in US Dollars | 10.99 |
| **Start_Time** | Delivery start timeframe | "10-25min", "1-3hr" |
| **Duration** | Delivery completion time | "100min", "72hr", "N/A" |
| **Refill_Period** | Guarantee period | "30 day", "No refill" |
| **Additional_Info** | Extra details/notes | "High quality accounts" |

## Usage Workflow

### For End Users:
1. Open `products_template.xlsx` in Excel, Google Sheets, or LibreOffice
2. Review example data in the Products sheet
3. Read Instructions and Column Guide sheets
4. Delete example rows (keep headers)
5. Add your product data
6. Save as .xlsx to preserve work
7. When ready: File → Save As → CSV format
8. Upload CSV to Discord using `/import_products` command

### For Developers:
To regenerate the template:
```bash
python3 create_template.py
```

This will create a fresh `products_template.xlsx` file with all formatting and example data.

## Technical Details

- **File Format**: Excel 2007+ (.xlsx / OpenXML)
- **File Size**: ~10KB
- **Library Used**: openpyxl (Python)
- **Generator Script**: `create_template.py`
- **Sheets**: 3 (Products, Instructions, Column Guide)
- **Example Rows**: 16 diverse product examples
- **Frozen Panes**: Header row frozen at A2

## Quality Assurance

The template has been validated for:
- ✓ Opens without errors in Excel/compatible software
- ✓ All 3 sheets present and properly named
- ✓ Header row frozen for scrolling
- ✓ Headers properly formatted (bold, colored, larger font)
- ✓ 10-15 example rows with diverse categories
- ✓ Comprehensive instructions for non-technical users
- ✓ Detailed column explanations
- ✓ CSV export compatibility
- ✓ Auto-fitted column widths

## Integration with Discord Bot

The template is designed to work seamlessly with the `/import_products` command (admin-only):

1. User fills template with products
2. Exports to CSV format
3. Uploads CSV via Discord command
4. Bot parses CSV and adds products to database
5. Products become available for purchase

## Support

For issues or questions about the template:
- Review the Instructions sheet in the Excel file
- Check the Column Guide sheet for field explanations
- Ensure CSV export uses UTF-8 encoding
- Verify all required columns are present

## Maintenance

The template can be regenerated at any time using `create_template.py`. This is useful for:
- Updating example data
- Adding new fields
- Refreshing instructions
- Fixing formatting issues

Simply run the generator script to create a new template with the latest structure.
