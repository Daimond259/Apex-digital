from __future__ import annotations

import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

from apex_core.utils import create_embed

logger = logging.getLogger(__name__)


class NotificationsCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.warranty_notification_task.start()

    def cog_unload(self) -> None:
        self.warranty_notification_task.cancel()

    def _is_admin(self, member: discord.Member | None) -> bool:
        if member is None:
            return False
        admin_role_id = self.bot.config.role_ids.admin
        return any(role.id == admin_role_id for role in getattr(member, "roles", []))

    @tasks.loop(hours=6)  # Check every 6 hours
    async def warranty_notification_task(self) -> None:
        """Check for expiring warranties and send notifications."""
        try:
            # Get orders expiring in the next 3 days
            expiring_orders = await self.bot.db.get_orders_expiring_soon(3)
            
            if not expiring_orders:
                return

            # Group orders by user for consolidated notifications
            user_orders = {}
            for order in expiring_orders:
                user_id = order["user_discord_id"]
                if user_id not in user_orders:
                    user_orders[user_id] = []
                user_orders[user_id].append(order)

            # Send notifications to each user
            for user_id, orders in user_orders.items():
                await self._send_warranty_expiry_notification(user_id, orders)

            # Send summary to admins if there are expiring orders
            await self._send_admin_warranty_summary(expiring_orders)

        except Exception as e:
            logger.error(f"Error in warranty notification task: {e}")

    @warranty_notification_task.before_loop
    async def before_warranty_notification_task(self) -> None:
        """Wait until the bot is ready before starting the task."""
        await self.bot.wait_until_ready()

    async def _send_warranty_expiry_notification(self, user_id: int, orders: list) -> None:
        """Send a DM to a user about their expiring warranties."""
        try:
            # Try to get the user from the cache/fetch
            user = self.bot.get_user(user_id)
            if not user:
                # If not in cache, try to fetch
                try:
                    user = await self.bot.fetch_user(user_id)
                except discord.NotFound:
                    logger.warning(f"Could not find user {user_id} for warranty notification")
                    return
                except discord.Forbidden:
                    logger.warning(f"Cannot fetch user {user_id} for warranty notification")
                    return

            # Create the notification embed
            embed = create_embed(
                title="⚠️ Warranty Expiry Notice",
                description="You have orders with warranties expiring soon!",
                color=discord.Color.orange(),
            )

            for order in orders:
                # Get product info
                product = None
                if order["product_id"] != 0:
                    product = await self.bot.db.get_product(order["product_id"])

                if order["product_id"] == 0:
                    import json
                    try:
                        metadata = json.loads(order["order_metadata"]) if order["order_metadata"] else {}
                        product_name = metadata.get("product_name", "Manual Order")
                    except (json.JSONDecodeError, TypeError):
                        product_name = "Manual Order"
                elif product:
                    product_name = f"{product['service_name']} - {product['variant_name']}"
                else:
                    product_name = f"Product #{order['product_id']} (deleted)"

                renewals = order.get("renewal_count", 0)
                renewal_info = f" ({renewals} renewals)" if renewals > 0 else ""

                value = (
                    f"**Product:** {product_name}\n"
                    f"**Order ID:** #{order['id']}\n"
                    f"**Status:** {order['status'].title()}\n"
                    f"**Expires:** {order['warranty_expires_at']}{renewal_info}\n"
                    f"Please contact support if you need to extend your warranty."
                )

                embed.add_field(
                    name=f"Order #{order['id']}",
                    value=value,
                    inline=False,
                )

            embed.set_footer(text="This is an automated notification. Please contact staff if you have questions.")

            try:
                await user.send(embed=embed)
                logger.info(f"Sent warranty expiry notification to user {user_id}")
            except discord.Forbidden:
                logger.warning(f"Cannot send DM to user {user_id} - DMs may be disabled")
            except Exception as e:
                logger.error(f"Error sending DM to user {user_id}: {e}")

        except Exception as e:
            logger.error(f"Error creating warranty notification for user {user_id}: {e}")

    async def _send_admin_warranty_summary(self, expiring_orders: list) -> None:
        """Send a summary of expiring warranties to admins."""
        try:
            # Get the main guild (assuming there's one configured)
            if not self.bot.guilds:
                return

            guild = self.bot.guilds[0]  # Use the first guild the bot is in
            
            # Find a channel to send the notification (you might want to configure this)
            # For now, look for a channel named "admin" or "staff"
            notification_channel = None
            for channel in guild.text_channels:
                if channel.name in ["admin", "staff", "warranty-alerts"]:
                    notification_channel = channel
                    break

            if not notification_channel:
                logger.warning("No suitable channel found for admin warranty notifications")
                return

            # Create summary embed
            embed = create_embed(
                title="📋 Warranty Expiry Summary",
                description=f"Found {len(expiring_orders)} order(s) with warranties expiring in the next 3 days",
                color=discord.Color.red(),
            )

            # Group by status for better overview
            status_counts = {}
            for order in expiring_orders:
                status = order["status"]
                status_counts[status] = status_counts.get(status, 0) + 1

            status_summary = "\n".join([f"**{status.title()}:** {count}" for status, count in status_counts.items()])
            embed.add_field(name="Status Breakdown", value=status_summary, inline=False)

            # Add some example orders (limit to avoid too long messages)
            sample_orders = expiring_orders[:5]  # Show first 5 orders
            for order in sample_orders:
                user = guild.get_member(order["user_discord_id"])
                user_mention = user.mention if user else f"<@{order['user_discord_id']}>"

                value = (
                    f"**User:** {user_mention}\n"
                    f"**Order:** #{order['id']}\n"
                    f"**Expires:** {order['warranty_expires_at']}"
                )

                embed.add_field(
                    name=f"Order #{order['id']}",
                    value=value,
                    inline=False,
                )

            if len(expiring_orders) > 5:
                embed.set_footer(text=f"And {len(expiring_orders) - 5} more orders...")

            await notification_channel.send(embed=embed)
            logger.info(f"Sent warranty expiry summary to admins in {notification_channel.name}")

        except Exception as e:
            logger.error(f"Error sending admin warranty summary: {e}")

    @app_commands.command(name="test-warranty-notification", description="Test warranty notification system (admin only)")
    async def test_warranty_notification(self, interaction: discord.Interaction) -> None:
        """Manually trigger the warranty notification check for testing."""
        if interaction.guild is None:
            await interaction.response.send_message(
                "This command must be used in a server.", ephemeral=True
            )
            return

        requester = interaction.user
        if not isinstance(requester, discord.Member) or not self._is_admin(requester):
            await interaction.response.send_message(
                "Only admins can test the warranty notification system.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            # Manually trigger the notification task
            await self.warranty_notification_task()

            await interaction.followup.send(
                "Warranty notification check completed successfully!", ephemeral=True
            )

        except Exception as e:
            logger.error(f"Error in test warranty notification: {e}")
            await interaction.followup.send(
                "An error occurred during the warranty notification test.", ephemeral=True
            )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(NotificationsCog(bot))