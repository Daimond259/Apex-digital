# Apex Core Discord Bot

A feature-rich Discord bot for automated product distribution, ticketing, and VIP management.

## Features

- **Automated Product Distribution**: SQLite-backed product catalog with role assignments and content delivery
- **VIP System**: Multi-tier VIP system with automatic discount application
- **Wallet System**: Internal wallet with transaction ledger tracking and lifetime spending history
- **Order History**: Complete order and transaction history with pagination and ticket linking
- **Ticket Lifecycle Automation**: Automatic ticket closure with inactivity warnings and HTML transcript export
- **Operating Hours**: Configurable business hours with Discord timestamp formatting
- **Payment Methods**: Flexible payment method configuration with custom instructions
- **Logging**: Comprehensive logging channels for audit, payments, tickets, and errors

## Setup

### Prerequisites

- Python 3.11 or higher
- Discord bot token (from [Discord Developer Portal](https://discord.com/developers/applications))

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd apex-core
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure the bot:
    ```bash
    cp config.example.json config.json
    cp config/payments.example.json config/payments.json
    ```

    Edit `config.json` with your bot token, guild IDs, role IDs, and other settings.
    Edit `config/payments.json` with your payment methods and confirmation templates.

4. Run the bot:
   ```bash
   python bot.py
   ```

   > **Tip:** Set `CONFIG_PATH` and `DISCORD_TOKEN` environment variables to override the default configuration file path and token.

### Running Tests

Automated tests are powered by `pytest` and `pytest-asyncio`. After installing the project requirements, run:

```bash
pytest
```

## Configuration

### Main Configuration (`config.json`)

The `config.json` file contains core bot settings:

- **token**: Your Discord bot token
- **bot_prefix**: Command prefix (default: `!`)
- **guild_ids**: List of Discord server IDs where the bot operates
- **role_ids**: Role IDs for admin and VIP tiers
- **ticket_categories**: Category IDs for support, billing, and sales tickets
- **operating_hours**: Start and end hours in UTC (24-hour format)
- **payment_methods**: Legacy payment methods (kept for backward compatibility)
- **vip_thresholds**: Spending thresholds for VIP tiers (in cents) and discount percentages
- **logging_channels**: Channel IDs for logging audit, payments, tickets, errors, and optional transcript archives

### Payments Configuration (`config/payments.json`)

The `config/payments.json` file contains payment-related settings:

- **payment_methods**: List of accepted payment methods with instructions, emoji, and metadata
- **order_confirmation_template**: Template for order confirmation messages with placeholders
- **refund_policy**: Default refund policy string

#### Payment Method Structure

Each payment method supports:
- **name**: Display name for the payment method
- **instructions**: User-facing instructions for how to pay
- **emoji**: Optional emoji icon for the payment method
- **metadata**: Additional data like URLs, addresses, or special flags

#### Available Payment Methods

The system supports various payment types:
- **Wallet**: Internal wallet balance for instant payments
- **Binance**: Binance Pay integration with Pay ID
- **Atto**: Atto payments with automatic cashback
- **PayPal**: PayPal email for manual payments
- **Tip.cc**: Discord tipbot integration
- **CryptoJar**: Alternative Discord tipbot
- **Bitcoin/Ethereum/Solana**: Cryptocurrency addresses with network metadata

#### Template Placeholders

The `order_confirmation_template` supports these placeholders:
- `{order_id}`: Unique order identifier
- `{service_name}`: Name of the purchased service
- `{variant_name}`: Specific variant or tier
- `{price}`: Formatted price with currency
- `{eta}`: Estimated delivery time

#### Adding/Removing Payment Methods

To add a new payment method:

```json
{
  "name": "New Payment Method",
  "instructions": "How to use this payment method",
  "emoji": "🆕",
  "metadata": {
    "url": "https://example.com",
    "additional_info": "Extra details"
  }
}
```

To remove a payment method, simply delete its object from the `payment_methods` array. The system will automatically update the available options.

## Project Structure

```
apex-core/
├── bot.py                    # Main bot entrypoint
├── config.example.json       # Example main configuration
├── config.json              # Your main configuration (gitignored)
├── config/                  # Configuration directory
│   ├── payments.example.json # Example payments configuration
│   └── payments.json        # Your payments configuration (gitignored)
├── requirements.txt         # Python dependencies
├── apex_core/              # Core modules
│   ├── __init__.py
│   ├── config.py           # Configuration loader
│   ├── database.py         # SQLite data layer
│   └── utils/              # Shared utilities
│       ├── __init__.py
│       ├── currency.py     # Currency formatting
│       ├── embeds.py       # Embed factory
│       ├── timestamps.py   # Discord timestamp helpers
│       └── vip.py          # VIP tier calculations
├── cogs/                   # Bot cogs
│   ├── wallet.py           # Wallet and deposit commands
│   ├── orders.py           # Order management commands
│   ├── storefront.py       # Product browsing and purchasing
│   ├── notifications.py    # Background notifications
│   └── ticket_management.py # Ticket lifecycle management
└── tests/                  # Test suite
    ├── test_payments_config.py # Payments configuration tests
    ├── test_database.py    # Database layer tests
    └── test_wallet_transactions.py # Wallet transaction tests
```

## Database Schema and Migrations

### Schema Versioning

The bot uses a schema versioning system to manage database structural changes safely. A `schema_migrations` table tracks all applied migrations, ensuring:

- **New installations** apply all migrations in order and reach the latest schema version
- **Existing installations** apply only pending migrations and skip already-applied ones
- **No duplications**: Each migration is recorded and only runs once

#### Viewing Migration Status

The bot logs migration progress on startup:

```
INFO:apex_core.database:Current database schema version: 2
INFO:apex_core.database:Applying migration v3: migrate_discounts_indexes
INFO:apex_core.database:Migration v3 applied successfully
INFO:apex_core.database:Database schema migration complete. Final version: 3
```

#### Adding New Migrations

To add a new migration:

1. Create a new migration method in `apex_core/database.py` with the pattern `async def _migration_vN(self):`
2. Increment `self.target_schema_version` in the `Database.__init__` method
3. Add the migration to the `migrations` dict in `_apply_pending_migrations` with the format `N: ("migration_name", self._migration_vN)`
4. The bot will automatically detect and apply the migration on next startup

Example migration:

```python
async def _migration_v4(self) -> None:
    """Migration v4: Add new column to users table."""
    if self._connection is None:
        raise RuntimeError("Database connection not initialized.")
    
    await self._connection.execute("ALTER TABLE users ADD COLUMN new_field TEXT")
    await self._connection.commit()
```

### Current Schema Versions

- **Version 1**: Base schema (users, products, discounts, tickets, orders tables)
- **Version 2**: Migrate products table from old single-name schema to categorized schema
- **Version 3**: Create performance indexes on discounts, tickets, and orders tables
- **Version 4**: Extend tickets table with type, order_id, assigned_staff_id, closed_at, and priority columns
- **Version 5**: Create wallet_transactions table for ledger tracking

### Users Table
- `id`: Primary key
- `discord_id`: Unique Discord user ID
- `wallet_balance_cents`: Current wallet balance in cents
- `total_lifetime_spent_cents`: Total amount spent lifetime
- `has_client_role`: Whether user has the client role assigned
- `manually_assigned_roles`: JSON list of manually assigned role names
- `created_at`: Account creation timestamp
- `updated_at`: Last update timestamp

### Products Table
- `id`: Primary key
- `main_category`: Main category (e.g., Instagram, YouTube)
- `sub_category`: Sub-category (e.g., Followers, Likes)
- `service_name`: Service grouping name
- `variant_name`: Product display name
- `price_cents`: Price in cents
- `start_time`: Delivery start time
- `duration`: Delivery duration
- `refill_period`: Guarantee/refill period
- `additional_info`: Extra product information
- `role_id`: Discord role ID to assign on purchase
- `content_payload`: Content URL or delivery instructions
- `is_active`: Whether product is available for purchase
- `created_at`: Product creation timestamp
- `updated_at`: Last update timestamp

### Discounts Table
- `id`: Primary key
- `user_id`: Specific user (NULL for global/VIP discounts)
- `product_id`: Specific product (NULL for all products)
- `vip_tier`: VIP tier requirement (NULL for user-specific)
- `discount_percent`: Discount percentage (0-100)
- `description`: Discount description
- `expires_at`: Expiration timestamp (NULL for no expiration)
- `is_stackable`: Whether discount can stack with others
- `created_at`: Discount creation timestamp

### Tickets Table
- `id`: Primary key
- `user_discord_id`: Discord ID of ticket creator
- `channel_id`: Discord channel ID (unique per ticket)
- `status`: Ticket status (open, closed, etc.)
- `type`: Ticket type (support, billing, sales)
- `order_id`: Related order ID (NULL if not order-related)
- `assigned_staff_id`: Discord ID of assigned staff member
- `priority`: Ticket priority (low, medium, high, critical)
- `last_activity`: Last activity timestamp
- `created_at`: Ticket creation timestamp
- `closed_at`: Ticket closure timestamp

### Orders Table
- `id`: Primary key
- `user_discord_id`: Discord ID of purchaser
- `product_id`: Product ID (0 for manual orders)
- `price_paid_cents`: Amount paid in cents
- `discount_applied_percent`: Discount percentage applied
- `order_metadata`: JSON metadata about the order
- `created_at`: Order creation timestamp

### Wallet Transactions Table
- `id`: Primary key
- `user_discord_id`: Discord ID of the user
- `amount_cents`: Transaction amount in cents (positive for credits, negative for debits)
- `balance_after_cents`: Wallet balance after this transaction
- `transaction_type`: Type of transaction (admin_credit, purchase, deposit, refund)
- `description`: Human-readable transaction description
- `order_id`: Related order ID (NULL if not order-related)
- `ticket_id`: Related ticket ID (NULL if not ticket-related)
- `staff_discord_id`: Discord ID of staff member who performed the transaction (for admin actions)
- `metadata`: JSON metadata for proof, source, or additional information
- `created_at`: Transaction timestamp

## Product Management

### Products CSV Template and Import Guide

For comprehensive documentation on using the products CSV template, see:
**[`docs/products_template.md`](docs/products_template.md)** - Complete guide with column mapping, validation rules, and step-by-step instructions

#### Quick Start

A professional Excel template (`templates/products_template.xlsx`) is provided for easy bulk product management:

1. **Get the template**: Download `templates/products_template.xlsx` or regenerate with `python3 create_template.py`

2. **Add your products**: The template includes 3 sheets:
   - **Products**: Main data entry sheet with 16 example rows
   - **Instructions**: Step-by-step usage guide  
   - **Column Guide**: Detailed field explanations

3. **Export to CSV**: File → Save As → CSV (Comma delimited)

4. **Import to Discord**: Use `/import_products` command (admin-only) and attach your CSV

#### Template Fields Overview

| CSV Column | Database Field | Required? | Description |
|------------|----------------|-----------|-------------|
| **Main_Category** | `main_category` | ✅ Yes | Platform (Instagram, YouTube, TikTok, etc.) |
| **Sub_Category** | `sub_category` | ✅ Yes | Service type (Followers, Likes, Subscribers, etc.) |
| **Service_Name** | `service_name` | ✅ Yes | Internal grouping name (e.g., "Instagram Services") |
| **Variant_Name** | `variant_name` | ✅ Yes | Customer-facing display name (e.g., "1000 Followers") |
| **Price_USD** | `price_cents` | ✅ Yes | Price in dollars (converted to cents internally) |
| **Start_Time** | `start_time` | ❌ No | Delivery start time (e.g., "10-25min", "1-3hr") |
| **Duration** | `duration` | ❌ No | Delivery duration (e.g., "100min", "72hr", "N/A") |
| **Refill_Period** | `refill_period` | ❌ No | Guarantee period (e.g., "30 day", "No refill") |
| **Additional_Info** | `additional_info` | ❌ No | Extra notes or requirements |

#### Schema Notes

- **Role assignments** (`role_id`) and **content delivery** (`content_payload`) are managed post-import via database/admin commands
- **Price conversion**: USD values are automatically converted to cents (10.99 → 1099)
- **Storefront grouping**: Products are organized by Main_Category → Sub_Category → Variant_Name
- **Validation**: The importer enforces required fields and positive pricing

#### Regenerating Template

To update the template structure or example data:
```bash
python3 create_template.py
```

This creates a fresh `products_template.xlsx` with current schema alignment.

## Development

### Adding New Cogs

1. Create a new file in the `cogs/` directory (e.g., `cogs/shop.py`)
2. Implement your cog class extending `commands.Cog`
3. Add a `setup()` function to load the cog
4. The bot will automatically load it on startup

Example:
```python
from discord.ext import commands

class MyCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
    
    @commands.hybrid_command()
    async def mycommand(self, ctx):
        await ctx.send("Hello!")

async def setup(bot):
    await bot.add_cog(MyCog(bot))
```

### Utilities

The `apex_core.utils` module provides:

- `format_usd(cents)`: Format cents as USD string (e.g., 1999 → "$19.99")
- `discord_timestamp(dt, style)`: Generate Discord timestamp strings
- `operating_hours_window(operating_hours)`: Get Discord timestamps for configured hours
- `render_operating_hours(operating_hours)`: Get human-readable operating hours string
- `create_embed(...)`: Create standardized embeds
- `calculate_vip_tier(total_spent_cents, config)`: Calculate VIP tier based on spending

### Database Access

Access the database in cogs via `self.bot.db`:

```python
# Ensure user exists
user = await self.bot.db.ensure_user(discord_id)

# Update wallet
new_balance = await self.bot.db.update_wallet_balance(discord_id, delta_cents=1000)

# Create product
product_id = await self.bot.db.create_product(
    name="Premium Access",
    price_cents=2999,
    role_id=123456789,
    content_payload="https://example.com/content"
)
```

## Commands

### User Commands

- `/deposit` - Open a private deposit ticket with staff
- `/balance` - Check your wallet balance and lifetime spending
- `/orders [page]` - View your order history with pagination (10 orders per page)
- `/transactions [page]` - View your wallet transaction history with pagination (10 transactions per page)
- `/buy` - Browse and purchase products from the storefront

### Admin Commands

- `/addbalance <member> <amount> <reason>` - Credit funds to a member's wallet
- `/balance <member>` - Check any member's wallet balance
- `/orders <member> [page]` - View any member's order history
- `/transactions <member> [page]` - View any member's wallet transaction history
- `/import_products` - Bulk import products from CSV template
- `/manualorder <member> <product_name> <price> [notes]` - Create a manual order (doesn't affect wallet)

### Ticket Commands

- `/close [reason]` - Close the current ticket with optional reason
- `/assign <staff_member>` - Assign a ticket to a staff member
- `/priority <level>` - Set ticket priority (low, medium, high, critical)

## Production Deployment

### Systemd Service (Ubuntu/Debian)

The bot includes a systemd service unit for production deployment on Ubuntu and other systemd-based distributions.

#### Installation Steps

1. **Set up the bot directory**:
   ```bash
   sudo mkdir -p /opt/apex-core
   sudo cp -r * /opt/apex-core/
   ```

2. **Create a virtual environment and install dependencies**:
   ```bash
   cd /opt/apex-core
   sudo python3 -m venv venv
   sudo venv/bin/pip install -r requirements.txt
   ```

3. **Create a dedicated system user**:
   ```bash
   sudo useradd -r -s /bin/false apexcore
   sudo chown -R apexcore:apexcore /opt/apex-core
   ```

4. **Create environment file**:
   ```bash
   sudo mkdir -p /etc/apex-core
   sudo nano /etc/apex-core/environment
   ```
   
   Add the following:
   ```
   DISCORD_TOKEN=your_bot_token_here
   CONFIG_PATH=/opt/apex-core/config.json
   ```

5. **Install and enable the service**:
   ```bash
   sudo cp apex-core.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable apex-core.service
   sudo systemctl start apex-core.service
   ```

6. **Verify the service is running**:
   ```bash
   sudo systemctl status apex-core.service
   ```

7. **View logs**:
   ```bash
   sudo journalctl -u apex-core.service -f
   ```

#### Service Management

- **Start the bot**: `sudo systemctl start apex-core.service`
- **Stop the bot**: `sudo systemctl stop apex-core.service`
- **Restart the bot**: `sudo systemctl restart apex-core.service`
- **Check status**: `sudo systemctl status apex-core.service`
- **Disable auto-start**: `sudo systemctl disable apex-core.service`

#### Updating the Bot

```bash
cd /opt/apex-core
sudo -u apexcore git pull
sudo -u apexcore venv/bin/pip install -r requirements.txt
sudo systemctl restart apex-core.service
```

## Ticket Lifecycle Automation

The bot includes automatic ticket management with the following features:

- **Inactivity Warnings**: Tickets inactive for 48 hours receive an automatic warning with a countdown timestamp
- **Auto-Closure**: Tickets inactive for 49 hours are automatically closed
- **HTML Transcripts**: Complete chat history exported as HTML using chat-exporter
- **Transcript Delivery**: Transcripts sent via DM to ticket opener and logged to configured channels
- **Operating Hours Integration**: Warnings include staff availability information

### Configuration

Add the optional `transcript_archive` channel to your `logging_channels` configuration:

```json
"logging_channels": {
  "audit": 123456789012345678,
  "payments": 123456789012345678,
  "tickets": 123456789012345678,
  "errors": 123456789012345678,
  "order_logs": 123456789012345678,
  "transcript_archive": 123456789012345678
}
```

## License

See LICENSE file for details.
