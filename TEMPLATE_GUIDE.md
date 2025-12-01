# Products Template Guide

## Overview

This document provides a high-level overview of the `products_template.xlsx` file used for bulk product management in the Apex Core Discord Bot.

**📋 For the complete comprehensive guide, see: [`docs/products_template.md`](docs/products_template.md)**

The comprehensive guide includes:
- Detailed column mapping to database fields
- Step-by-step usage instructions  
- Validation rules and error handling
- Price conversion examples
- Storefront grouping logic
- Troubleshooting and support

## Quick Reference

### File Location
- **Template**: `templates/products_template.xlsx`
- **Generator**: `create_template.py`
- **Documentation**: `docs/products_template.md`

### Template Structure
The Excel template contains three sheets:

1. **Products**: Main data entry with 16 example rows
2. **Instructions**: Step-by-step usage guide  
3. **Column Guide**: Detailed field explanations

### CSV Columns (9 total)

| Column | Required? | Description |
|--------|-----------|-------------|
| **Main_Category** | ✅ Yes | Platform (Instagram, YouTube, TikTok, etc.) |
| **Sub_Category** | ✅ Yes | Service type (Followers, Likes, Subscribers, etc.) |
| **Service_Name** | ✅ Yes | Internal grouping name |
| **Variant_Name** | ✅ Yes | Customer-facing product name |
| **Price_USD** | ✅ Yes | Price in dollars (converted to cents) |
| **Start_Time** | ❌ No | Delivery start time |
| **Duration** | ❌ No | Delivery duration |
| **Refill_Period** | ❌ No | Guarantee period |
| **Additional_Info** | ❌ No | Extra notes or requirements |

### Basic Workflow

1. **Get template**: `templates/products_template.xlsx`
2. **Add products**: Replace example data with your products
3. **Export CSV**: File → Save As → CSV format
4. **Import**: Use `/import_products` Discord command (admin only)

### Key Points

- **Price conversion**: Enter `10.99` for $10.99 (automatically converted to cents)
- **Required fields**: All category/name fields and price must be filled
- **Storefront grouping**: Products organized by Main_Category → Sub_Category → Variant_Name
- **Database fields**: Role assignments and content delivery are managed post-import

## Technical Details

- **File Format**: Excel 2007+ (.xlsx)
- **Generator Script**: `create_template.py`
- **Import Command**: `/import_products` (admin-only)
- **Validation**: Enforced by `cogs/product_import.py`
- **Database**: Products table in SQLite database

## Regenerating Template

```bash
python3 create_template.py
```

This creates a fresh template with current schema alignment and example data.

---

**For complete documentation, examples, and troubleshooting, see the comprehensive guide: [`docs/products_template.md`](docs/products_template.md)**
