from __future__ import annotations

import json
import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from apex_core.utils import create_embed, format_usd

logger = logging.getLogger(__name__)


class OrdersCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    def _is_admin(self, member: discord.Member | None) -> bool:
        if member is None:
            return False
        admin_role_id = self.bot.config.role_ids.admin
        return any(role.id == admin_role_id for role in getattr(member, "roles", []))

    def _resolve_member(self, interaction: discord.Interaction) -> discord.Member | None:
        if isinstance(interaction.user, discord.Member):
            return interaction.user
        if interaction.guild:
            return interaction.guild.get_member(interaction.user.id)
        return None

    async def _format_order_embed(
        self,
        order: dict,
        product: Optional[dict],
        ticket: Optional[dict],
        user_mention: str,
    ) -> discord.Embed:
        is_manual = order["product_id"] == 0
        
        if is_manual:
            try:
                metadata = json.loads(order["order_metadata"]) if order["order_metadata"] else {}
                product_name = metadata.get("product_name", "Manual Order")
                notes = metadata.get("notes", "N/A")
            except (json.JSONDecodeError, TypeError):
                product_name = "Manual Order"
                notes = "N/A"
            
            embed = create_embed(
                title=f"Order #{order['id']} (Manual)",
                color=discord.Color.orange(),
            )
            embed.add_field(name="Product", value=product_name, inline=False)
            embed.add_field(name="Notes", value=notes, inline=False)
        else:
            if product:
                product_name = f"{product['service_name']} - {product['variant_name']}"
                category = f"{product['main_category']} > {product['sub_category']}"
            else:
                product_name = f"Product ID #{order['product_id']} (deleted)"
                category = "N/A"
            
            embed = create_embed(
                title=f"Order #{order['id']}",
                color=discord.Color.blue(),
            )
            embed.add_field(name="Product", value=product_name, inline=False)
            embed.add_field(name="Category", value=category, inline=False)

        embed.add_field(name="User", value=user_mention, inline=True)
        embed.add_field(name="Price Paid", value=format_usd(order["price_paid_cents"]), inline=True)
        
        if order["discount_applied_percent"] > 0:
            embed.add_field(
                name="Discount Applied",
                value=f"{order['discount_applied_percent']:.1f}%",
                inline=True,
            )
        
        embed.add_field(name="Order Date", value=order["created_at"], inline=False)
        
        # Status field with color coding
        status_emoji = {
            "pending": "⏳",
            "fulfilled": "✅", 
            "refill": "🔄",
            "refunded": "❌"
        }
        status_display = f"{status_emoji.get(order['status'], '❓')} {order['status'].title()}"
        embed.add_field(name="Status", value=status_display, inline=True)
        
        # Warranty information
        if order.get("warranty_expires_at"):
            warranty_info = f"Expires: {order['warranty_expires_at']}"
            if order.get("renewal_count", 0) > 0:
                warranty_info += f"\nRenewals: {order['renewal_count']}"
                if order.get("last_renewed_at"):
                    warranty_info += f"\nLast renewed: {order['last_renewed_at']}"
            embed.add_field(name="Warranty", value=warranty_info, inline=True)
        
        if ticket:
            ticket_info = f"Ticket #{ticket['id']} (Channel ID: {ticket['channel_id']})"
            if ticket["status"]:
                ticket_info += f"\nStatus: {ticket['status']}"
            embed.add_field(name="Related Ticket", value=ticket_info, inline=False)
        
        return embed

    @app_commands.command(name="orders", description="View order history")
    @app_commands.describe(
        member="Member to view orders for (admin only)",
        page="Page number (10 orders per page)",
    )
    async def orders(
        self,
        interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        page: int = 1,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "This command must be used in a server.", ephemeral=True
            )
            return

        requester = self._resolve_member(interaction)
        if requester is None:
            await interaction.response.send_message(
                "Unable to resolve your member profile.", ephemeral=True
            )
            return

        target = member or requester
        if member and not self._is_admin(requester):
            await interaction.response.send_message(
                "Only admins can view other members' orders.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        if page < 1:
            page = 1

        per_page = 10
        offset = (page - 1) * per_page

        orders = await self.bot.db.get_orders_for_user(
            target.id, limit=per_page, offset=offset
        )
        total_orders = await self.bot.db.count_orders_for_user(target.id)

        if not orders:
            if page == 1:
                await interaction.followup.send(
                    f"No orders found for {target.mention}.", ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"No orders on page {page}.", ephemeral=True
                )
            return

        total_pages = (total_orders + per_page - 1) // per_page

        embed = create_embed(
            title=f"Order History • {target.display_name}",
            description=f"Page {page} of {total_pages} • {total_orders} total orders",
            color=discord.Color.gold(),
        )

        for order in orders:
            product = None
            if order["product_id"] != 0:
                product = await self.bot.db.get_product(order["product_id"])
            
            ticket = await self.bot.db.get_ticket_by_order_id(order["id"])
            
            is_manual = order["product_id"] == 0
            if is_manual:
                try:
                    metadata = json.loads(order["order_metadata"]) if order["order_metadata"] else {}
                    product_name = metadata.get("product_name", "Manual Order")
                except (json.JSONDecodeError, TypeError):
                    product_name = "Manual Order"
                order_type = " (Manual)"
            else:
                if product:
                    product_name = f"{product['service_name']} - {product['variant_name']}"
                else:
                    product_name = f"Product #{order['product_id']} (deleted)"
                order_type = ""

            price_str = format_usd(order["price_paid_cents"])
            discount_str = ""
            if order["discount_applied_percent"] > 0:
                discount_str = f" ({order['discount_applied_percent']:.1f}% off)"

            ticket_str = ""
            if ticket:
                ticket_str = f" 🎫"

            # Status emoji
            status_emoji = {
                "pending": "⏳",
                "fulfilled": "✅", 
                "refill": "🔄",
                "refunded": "❌"
            }
            status_emoji_str = status_emoji.get(order['status'], "❓")
            
            # Warranty info for active orders
            warranty_str = ""
            if order.get("warranty_expires_at") and order['status'] in ['fulfilled', 'refill']:
                warranty_str = f" 🔒"

            value = f"{product_name}\n{price_str}{discount_str} {status_emoji_str}{warranty_str}{ticket_str}\n{order['created_at']}"
            
            embed.add_field(
                name=f"Order #{order['id']}{order_type}",
                value=value,
                inline=False,
            )

        if total_pages > 1:
            embed.set_footer(text=f"Use /orders page:{page+1} to see the next page")

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="transactions", description="View wallet transaction history")
    @app_commands.describe(
        member="Member to view transactions for (admin only)",
        page="Page number (10 transactions per page)",
    )
    async def transactions(
        self,
        interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        page: int = 1,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "This command must be used in a server.", ephemeral=True
            )
            return

        requester = self._resolve_member(interaction)
        if requester is None:
            await interaction.response.send_message(
                "Unable to resolve your member profile.", ephemeral=True
            )
            return

        target = member or requester
        if member and not self._is_admin(requester):
            await interaction.response.send_message(
                "Only admins can view other members' transaction history.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        if page < 1:
            page = 1

        per_page = 10
        offset = (page - 1) * per_page

        transactions = await self.bot.db.get_wallet_transactions(
            target.id, limit=per_page, offset=offset
        )
        total_transactions = await self.bot.db.count_wallet_transactions(target.id)

        if not transactions:
            if page == 1:
                await interaction.followup.send(
                    f"No wallet transactions found for {target.mention}.", ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"No transactions on page {page}.", ephemeral=True
                )
            return

        total_pages = (total_transactions + per_page - 1) // per_page

        embed = create_embed(
            title=f"Wallet Transactions • {target.display_name}",
            description=f"Page {page} of {total_pages} • {total_transactions} total transactions",
            color=discord.Color.green(),
        )

        for txn in transactions:
            amount = txn["amount_cents"]
            amount_str = format_usd(abs(amount))
            if amount >= 0:
                amount_display = f"+{amount_str}"
                emoji = "💰"
            else:
                amount_display = f"-{amount_str}"
                emoji = "💸"

            txn_type = txn["transaction_type"].replace("_", " ").title()
            description = txn["description"] or "N/A"
            
            balance_str = format_usd(txn["balance_after_cents"])
            
            value_parts = [
                f"{emoji} **{amount_display}** ({txn_type})",
                f"Balance: {balance_str}",
            ]
            
            if description != "N/A":
                value_parts.append(f"*{description}*")
            
            if txn["order_id"]:
                value_parts.append(f"Order: #{txn['order_id']}")
            
            if txn["ticket_id"]:
                value_parts.append(f"Ticket: #{txn['ticket_id']}")
            
            if txn["metadata"]:
                try:
                    metadata = json.loads(txn["metadata"])
                    if isinstance(metadata, dict) and "proof" in metadata:
                        value_parts.append(f"Proof: {metadata['proof']}")
                except (json.JSONDecodeError, TypeError):
                    pass
            
            value = "\n".join(value_parts)
            
            embed.add_field(
                name=f"Transaction #{txn['id']} • {txn['created_at']}",
                value=value,
                inline=False,
            )

        if total_pages > 1:
            embed.set_footer(text=f"Use /transactions page:{page+1} to see the next page")

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="order-status", description="Update order status (admin only)")
    @app_commands.describe(
        order_id="Order ID to update",
        status="New status for the order"
    )
    @app_commands.choices(
        status=[
            app_commands.Choice(name="⏳ Pending", value="pending"),
            app_commands.Choice(name="✅ Fulfilled", value="fulfilled"),
            app_commands.Choice(name="🔄 Refill", value="refill"),
            app_commands.Choice(name="❌ Refunded", value="refunded"),
        ]
    )
    async def order_status(
        self,
        interaction: discord.Interaction,
        order_id: int,
        status: str,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "This command must be used in a server.", ephemeral=True
            )
            return

        requester = self._resolve_member(interaction)
        if requester is None or not self._is_admin(requester):
            await interaction.response.send_message(
                "Only admins can update order status.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            # Get the order first to verify it exists
            order = await self.bot.db.get_order_by_id(order_id)
            if not order:
                await interaction.followup.send(
                    f"Order #{order_id} not found.", ephemeral=True
                )
                return

            old_status = order["status"]
            await self.bot.db.update_order_status(order_id, status)

            # Get user info for the response
            user = interaction.guild.get_member(order["user_discord_id"])
            user_mention = user.mention if user else f"<@{order['user_discord_id']}>"

            embed = create_embed(
                title=f"Order #{order_id} Status Updated",
                description=f"Status changed from **{old_status.title()}** to **{status.title()}**",
                color=discord.Color.green(),
            )
            embed.add_field(name="User", value=user_mention, inline=True)
            embed.add_field(name="Updated by", value=requester.mention, inline=True)
            embed.add_field(name="Order Date", value=order["created_at"], inline=False)

            await interaction.followup.send(embed=embed, ephemeral=True)

        except ValueError as e:
            await interaction.followup.send(f"Error: {e}", ephemeral=True)
        except Exception as e:
            logger.error(f"Error updating order status: {e}")
            await interaction.followup.send(
                "An error occurred while updating the order status.", ephemeral=True
            )

    @app_commands.command(name="renew-warranty", description="Renew order warranty (admin only)")
    @app_commands.describe(
        order_id="Order ID to renew warranty for",
        days="Warranty duration in days from now"
    )
    async def renew_warranty(
        self,
        interaction: discord.Interaction,
        order_id: int,
        days: int,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "This command must be used in a server.", ephemeral=True
            )
            return

        requester = self._resolve_member(interaction)
        if requester is None or not self._is_admin(requester):
            await interaction.response.send_message(
                "Only admins can renew warranties.", ephemeral=True
            )
            return

        if days <= 0:
            await interaction.response.send_message(
                "Days must be a positive number.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            # Get the order first to verify it exists
            order = await self.bot.db.get_order_by_id(order_id)
            if not order:
                await interaction.followup.send(
                    f"Order #{order_id} not found.", ephemeral=True
                )
                return

            # Calculate warranty expiry date
            from datetime import datetime, timedelta
            expiry_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

            old_renewal_count = order.get("renewal_count", 0)
            await self.bot.db.renew_order_warranty(
                order_id, expiry_date, interaction.user.id
            )

            # Get user info for the response
            user = interaction.guild.get_member(order["user_discord_id"])
            user_mention = user.mention if user else f"<@{order['user_discord_id']}>"

            embed = create_embed(
                title=f"Order #{order_id} Warranty Renewed",
                description=f"Warranty extended by **{days} days**",
                color=discord.Color.blue(),
            )
            embed.add_field(name="User", value=user_mention, inline=True)
            embed.add_field(name="New expiry", value=expiry_date, inline=True)
            embed.add_field(name="Renewal count", value=str(old_renewal_count + 1), inline=True)
            embed.add_field(name="Renewed by", value=requester.mention, inline=False)

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error renewing warranty: {e}")
            await interaction.followup.send(
                "An error occurred while renewing the warranty.", ephemeral=True
            )

    @app_commands.command(name="warranty-expiry", description="Check orders with warranties expiring soon (admin only)")
    @app_commands.describe(
        days="Number of days ahead to check (default: 7)"
    )
    async def warranty_expiry(
        self,
        interaction: discord.Interaction,
        days: int = 7,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "This command must be used in a server.", ephemeral=True
            )
            return

        requester = self._resolve_member(interaction)
        if requester is None or not self._is_admin(requester):
            await interaction.response.send_message(
                "Only admins can check warranty expiry.", ephemeral=True
            )
            return

        if days <= 0:
            await interaction.response.send_message(
                "Days must be a positive number.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            expiring_orders = await self.bot.db.get_orders_expiring_soon(days)

            if not expiring_orders:
                await interaction.followup.send(
                    f"No orders with warranties expiring in the next {days} days.", ephemeral=True
                )
                return

            embed = create_embed(
                title=f"Orders Expiring in Next {days} Days",
                description=f"Found {len(expiring_orders)} order(s) with expiring warranties",
                color=discord.Color.orange(),
            )

            for order in expiring_orders:
                user = interaction.guild.get_member(order["user_discord_id"])
                user_mention = user.mention if user else f"<@{order['user_discord_id']}>"

                # Get product info
                product = None
                if order["product_id"] != 0:
                    product = await self.bot.db.get_product(order["product_id"])

                if order["product_id"] == 0:
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
                    f"**User:** {user_mention}\n"
                    f"**Product:** {product_name}\n"
                    f"**Status:** {order['status'].title()}\n"
                    f"**Expires:** {order['warranty_expires_at']}{renewal_info}"
                )

                embed.add_field(
                    name=f"Order #{order['id']}",
                    value=value,
                    inline=False,
                )

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error checking warranty expiry: {e}")
            await interaction.followup.send(
                "An error occurred while checking warranty expiry.", ephemeral=True
            )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(OrdersCog(bot))
