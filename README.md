# Apex Core Discord Bot

A feature-rich Discord bot for automated product distribution, ticketing, and VIP management.

## Features

- **Automated Product Distribution**: SQLite-backed product catalog with role assignments and content delivery
- **VIP System**: Multi-tier VIP system with automatic discount application
- **Wallet System**: Internal wallet with transaction tracking and lifetime spending
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
   ```
   
   Edit `config.json` with your bot token, guild IDs, role IDs, and other settings.

4. Run the bot:
   ```bash
   python bot.py
   ```

   > **Tip:** Set `CONFIG_PATH` and `DISCORD_TOKEN` environment variables to override the default configuration file path and token.

## Configuration

The `config.json` file contains all bot settings:

- **token**: Your Discord bot token
- **bot_prefix**: Command prefix (default: `!`)
- **guild_ids**: List of Discord server IDs where the bot operates
- **role_ids**: Role IDs for admin and VIP tiers
- **ticket_categories**: Category IDs for support, billing, and sales tickets
- **operating_hours**: Start and end hours in UTC (24-hour format)
- **payment_methods**: List of accepted payment methods with instructions and metadata
- **vip_thresholds**: Spending thresholds for VIP tiers (in cents) and discount percentages
- **logging_channels**: Channel IDs for logging audit, payments, tickets, errors, and optional transcript archives

## Project Structure

```
apex-core/
├── bot.py                    # Main bot entrypoint
├── config.example.json       # Example configuration
├── config.json              # Your configuration (gitignored)
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
└── cogs/                   # Bot cogs (to be implemented)
    └── __init__.py
```

## Database Schema

### Users Table
- `id`: Primary key
- `discord_id`: Unique Discord user ID
- `wallet_balance_cents`: Current wallet balance in cents
- `total_lifetime_spent_cents`: Total amount spent lifetime
- `vip_tier`: Current VIP tier (tier1, tier2, tier3, or NULL)
- `created_at`: Account creation timestamp
- `updated_at`: Last update timestamp

### Products Table
- `id`: Primary key
- `name`: Product name
- `price_cents`: Price in cents
- `role_id`: Discord role ID to assign on purchase
- `content_payload`: Content URL or instructions
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

## Product Management

### Using the Products Template

A professional Excel template (`products_template.xlsx`) is provided for easy bulk product management:

1. **Open the template**: The file contains three sheets:
   - **Products**: Main data entry sheet with example rows
   - **Instructions**: Step-by-step guide for using the template
   - **Column Guide**: Detailed explanations of each field

2. **Add your products**: Follow the example format:
   - Delete example rows (keep headers)
   - Fill in your product data
   - Categories: Instagram, YouTube, TikTok, Xbox, ChatGPT, etc.
   - Sub-categories: Followers, Likes, Views, Subscriptions, etc.
   - Price in USD (e.g., 10.99 for $10.99)

3. **Export to CSV**:
   - File → Save As → CSV (Comma delimited)
   - Only the Products sheet will be exported

4. **Import to Discord**:
   - Use the `/import_products` command (admin-only)
   - Attach your CSV file
   - Products will be automatically added to the database

### Template Fields

- **Main_Category**: Platform (Instagram, YouTube, TikTok, etc.)
- **Sub_Category**: Service type (Followers, Likes, Subscribers, etc.)
- **Service_Name**: Grouping name (e.g., "Instagram Services")
- **Variant_Name**: Display name (e.g., "1000 Followers")
- **Price_USD**: Price in dollars (converted to cents internally)
- **Start_Time**: Delivery start time (e.g., "10-25min", "1-3hr")
- **Duration**: Delivery duration (e.g., "100min", "72hr", "N/A")
- **Refill_Period**: Guarantee period (e.g., "30 day", "No refill")
- **Additional_Info**: Extra notes or requirements

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
