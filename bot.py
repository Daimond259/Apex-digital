"""
Apex Core - Discord Bot for automated product distribution and ticketing.

This is the main entrypoint for the bot.
"""

import asyncio
import logging
import os
import sys
from dataclasses import replace
from pathlib import Path

import discord
from discord.ext import commands

from apex_core import load_config, load_payment_settings, Database, TranscriptStorage

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s:%(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class ApexCoreBot(commands.Bot):
    """Extended Bot class with database and config management."""

    def __init__(self, *args, **kwargs):
        self.config = kwargs.pop("config")
        self.db = Database()
        self.storage = TranscriptStorage()
        super().__init__(*args, **kwargs)

    async def setup_hook(self):
        await self.db.connect()
        logger.info("Database connected and schema initialized.")
        
        self.storage.initialize()
        logger.info("Transcript storage initialized.")

        await self._load_cogs()

        for guild_id in self.config.guild_ids:
            guild = discord.Object(id=guild_id)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            logger.info(f"Command tree synced for guild {guild_id}")

    async def _load_cogs(self):
        cogs_dir = Path("cogs")
        if not cogs_dir.exists():
            logger.warning("No cogs directory found. Skipping cog loading.")
            return

        for cog_file in cogs_dir.glob("*.py"):
            if cog_file.stem.startswith("_"):
                continue

            extension = f"cogs.{cog_file.stem}"
            try:
                await self.load_extension(extension)
                logger.info(f"Loaded extension: {extension}")
            except Exception as e:
                logger.error(f"Failed to load extension {extension}: {e}", exc_info=True)

    async def on_ready(self):
        logger.info(f"Logged in as {self.user} (ID: {self.user.id})")
        logger.info("Apex Core is ready!")

    async def close(self):
        await self.db.close()
        logger.info("Database connection closed.")
        await super().close()


async def main():
    config_path = os.environ.get("CONFIG_PATH", "config.json")
    token = os.environ.get("DISCORD_TOKEN")
    
    try:
        config = load_config(config_path)
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    if token:
        config = replace(config, token=token)
        logger.info("Using token from environment variable")
    
    # Validate payments configuration if it exists
    try:
        payment_settings = load_payment_settings()
        logger.info("Payments configuration loaded successfully")
        if not config.payment_settings:
            # This shouldn't happen if load_payment_settings succeeded
            logger.warning("Payments config loaded but not attached to main config")
    except FileNotFoundError:
        logger.info("Payments configuration file not found - using legacy inline payment methods")
    except Exception as e:
        logger.error("Payments configuration validation failed: %s", e)
        sys.exit(1)

    intents = discord.Intents.default()
    intents.message_content = True
    intents.members = True
    intents.guilds = True

    bot = ApexCoreBot(
        command_prefix=config.bot_prefix,
        intents=intents,
        config=config
    )

    async with bot:
        await bot.start(config.token)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot shut down by user.")
