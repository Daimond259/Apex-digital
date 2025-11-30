from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from apex_core.utils import (
    calculate_vip_tier,
    create_embed,
    format_usd,
    handle_vip_promotion,
    operating_hours_window,
    process_post_purchase,
    render_operating_hours,
)

logger = logging.getLogger(__name__)


# ============================================================================
# LEVEL 1: Main Category Selection (Persistent)
# ============================================================================

class CategorySelect(discord.ui.Select["CategorySelectView"]):
    def __init__(self, categories: list[str]) -> None:
        options = [
            discord.SelectOption(
                label=category[:100],
                value=category[:100],
            )
            for category in categories[:25]
        ]
        
        if not options:
            options = [discord.SelectOption(label="No categories available", value="none")]
        
        super().__init__(
            placeholder="Select Category (Scroll down for more)",
            options=options,
            custom_id="storefront:category_select",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if self.values[0] == "none":
            await interaction.response.send_message(
                "No products available at this time.",
                ephemeral=True,
            )
            return
        
        main_category = self.values[0]
        cog: StorefrontCog = interaction.client.get_cog("StorefrontCog")  # type: ignore
        if not cog:
            await interaction.response.send_message(
                "Storefront cog not loaded.", ephemeral=True
            )
            return
        
        await cog._show_sub_categories(interaction, main_category)


class CategorySelectView(discord.ui.View):
    PAGE_SIZE = 25

    def __init__(self, categories: list[str], page: int = 0) -> None:
        super().__init__(timeout=None)
        self.categories = categories
        self.page = page
        self.total_pages = max(1, (len(categories) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        self._build_view()

    def _current_slice(self) -> list[str]:
        start = self.page * self.PAGE_SIZE
        end = start + self.PAGE_SIZE
        return self.categories[start:end]

    def _build_view(self) -> None:
        self.add_item(CategorySelect(self._current_slice()))
        if self.total_pages > 1:
            self.add_item(CategoryPaginatorButton(direction="previous"))
            self.add_item(CategoryPaginatorButton(direction="next"))


class CategoryPaginatorButton(discord.ui.Button["CategorySelectView"]):
    def __init__(self, direction: str) -> None:
        label = "◀️ Previous" if direction == "previous" else "Next ▶️"
        custom_id = f"storefront:category_page:{direction}"
        super().__init__(label=label, style=discord.ButtonStyle.secondary, custom_id=custom_id)
        self.direction = direction

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, CategorySelectView):
            await interaction.response.send_message(
                "Pagination unavailable. Please run the setup command again.",
                ephemeral=True,
            )
            return
        
        total_pages = view.total_pages
        if total_pages <= 1:
            await interaction.response.defer()
            return
        
        if self.direction == "next":
            new_page = (view.page + 1) % total_pages
        else:
            new_page = (view.page - 1) % total_pages
        
        new_view = CategorySelectView(view.categories, page=new_page)
        await interaction.response.edit_message(view=new_view)


# ============================================================================
# LEVEL 2: Sub Category Selection (Ephemeral)
# ============================================================================

class SubCategorySelect(discord.ui.Select["SubCategorySelectView"]):
    def __init__(self, main_category: str, sub_categories: list[str]) -> None:
        self.main_category = main_category
        options = [
            discord.SelectOption(
                label=sub_cat[:100],
                value=sub_cat[:100],
            )
            for sub_cat in sub_categories[:25]
        ]
        
        super().__init__(
            placeholder="Select Sub-Category...",
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        sub_category = self.values[0]
        cog: StorefrontCog = interaction.client.get_cog("StorefrontCog")  # type: ignore
        if not cog:
            await interaction.response.send_message(
                "Storefront cog not loaded.", ephemeral=True
            )
            return
        
        await cog._show_products(interaction, self.main_category, sub_category)


class SubCategorySelectView(discord.ui.View):
    def __init__(self, main_category: str, sub_categories: list[str]) -> None:
        super().__init__(timeout=120)
        self.add_item(SubCategorySelect(main_category, sub_categories))


# ============================================================================
# LEVEL 3: Product Display with Ticket Button (Ephemeral)
# ============================================================================

class VariantSelect(discord.ui.Select["ProductDisplayView"]):
    def __init__(self, products: list[dict]) -> None:
        options: list[discord.SelectOption] = []
        for index, product in enumerate(products[:25]):
            option = discord.SelectOption(
                label=product["variant_name"][:100],
                value=str(product["id"]),
                description=f"{format_usd(product['price_cents'])}"[:100],
                default=index == 0,
            )
            options.append(option)

        super().__init__(
            placeholder="Select a service to include in your ticket...",
            options=options,
            custom_id="storefront:variant_select",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if isinstance(view, ProductDisplayView):
            view.selected_product_id = int(self.values[0])
        await interaction.response.defer()


class OpenTicketButton(discord.ui.Button["ProductDisplayView"]):
    def __init__(self, main_category: str, sub_category: str) -> None:
        super().__init__(
            label="📩 Open Ticket",
            style=discord.ButtonStyle.primary,
        )
        self.main_category = main_category
        self.sub_category = sub_category

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: StorefrontCog = interaction.client.get_cog("StorefrontCog")  # type: ignore
        if not cog:
            await interaction.response.send_message(
                "Storefront cog not loaded.", ephemeral=True
            )
            return
        
        view = self.view
        selected_product_id = None
        if isinstance(view, ProductDisplayView):
            selected_product_id = view.selected_product_id
        
        if selected_product_id is None:
            await interaction.response.send_message(
                "Please select a service before opening a ticket.",
                ephemeral=True,
            )
            return
        
        await cog._handle_open_ticket(
            interaction,
            self.main_category,
            self.sub_category,
            selected_product_id,
        )


class ProductDisplayView(discord.ui.View):
    def __init__(self, products: list[dict], main_category: str, sub_category: str) -> None:
        super().__init__(timeout=120)
        self.selected_product_id: int | None = products[0]["id"] if products else None
        if products:
            self.add_item(VariantSelect(products))
        self.add_item(OpenTicketButton(main_category, sub_category))


# ============================================================================
# Payment Method Buttons (for ticket channel)
# ============================================================================

class PaymentMethodButton(discord.ui.Button["PaymentMethodView"]):
    def __init__(self, method_name: str, emoji: str | None = None) -> None:
        label = f"{emoji} {method_name}" if emoji else method_name
        super().__init__(
            label=label,
            style=discord.ButtonStyle.secondary,
        )
        self.method_name = method_name

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message(
            f"Selected payment method: **{self.method_name}**\n\n"
            "A staff member will assist you with this payment method shortly.",
            ephemeral=True,
        )


class PaymentMethodView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)
        self.add_item(PaymentMethodButton("Binance Pay", "💳"))
        self.add_item(PaymentMethodButton("Tip.cc", "💰"))
        self.add_item(PaymentMethodButton("Crypto", "₿"))


# ============================================================================
# Legacy Components (kept for backward compatibility)
# ============================================================================

class ProductSelect(discord.ui.Select["ProductSelectView"]):
    def __init__(self) -> None:
        super().__init__(
            placeholder="Select a product to view details...",
            options=[
                discord.SelectOption(
                    label="Loading products...",
                    value="loading",
                )
            ],
            custom_id="storefront:product_select",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        product_id = self.values[0]
        
        if product_id == "loading":
            await interaction.response.send_message(
                "Products are still loading. Please try again in a moment.",
                ephemeral=True,
            )
            return

        cog: StorefrontCog = interaction.client.get_cog("StorefrontCog")  # type: ignore
        if not cog:
            await interaction.response.send_message(
                "Storefront cog not loaded.", ephemeral=True
            )
            return

        product = await interaction.client.db.get_product(int(product_id))  # type: ignore
        if not product or not product["is_active"]:
            await interaction.response.send_message(
                "Product not found or no longer active.", ephemeral=True
            )
            return

        user_row = await interaction.client.db.get_user(interaction.user.id)  # type: ignore
        vip_tier = None
        if user_row:
            vip_tier = calculate_vip_tier(
                user_row["total_lifetime_spent_cents"], interaction.client.config  # type: ignore
            )

        base_price_cents = product["price_cents"]
        discount_percent = await cog._calculate_discount(
            interaction.user.id, int(product_id), vip_tier
        )
        final_price_cents = int(base_price_cents * (1 - discount_percent / 100))

        # Use variant_name for new products, fallback to name for legacy products
        product_name = product.get("variant_name", product.get("name", "Unknown Product"))
        
        embed = create_embed(
            title=product_name,
            description=f"**Base Price:** {format_usd(base_price_cents)}\n"
            f"**Your Discount:** {discount_percent:.1f}%\n"
            f"**Final Price:** {format_usd(final_price_cents)}",
            color=discord.Color.blue(),
        )

        operating_hours_text = render_operating_hours(interaction.client.config.operating_hours)  # type: ignore
        embed.add_field(
            name="Operating Hours",
            value=operating_hours_text,
            inline=False,
        )

        view = ProductActionView(product_id=int(product_id))
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class ProductSelectView(discord.ui.View):
    def __init__(self, products: list[dict] | None = None) -> None:
        super().__init__(timeout=None)
        select = ProductSelect()
        if products:
            select.options = [
                discord.SelectOption(
                    label=product.get("variant_name", product.get("name", "Unknown Product"))[:100],
                    value=str(product["id"]),
                    description=f"{format_usd(product['price_cents'])} • ID: {product['id']}"[:100],
                )
                for product in products[:25]
            ]
        self.add_item(select)


class BuyButton(discord.ui.Button["ProductActionView"]):
    def __init__(self, product_id: int) -> None:
        super().__init__(
            label="Buy with Wallet",
            style=discord.ButtonStyle.green,
            custom_id=f"storefront:buy:{product_id}",
        )
        self.product_id = product_id

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: StorefrontCog = interaction.client.get_cog("StorefrontCog")  # type: ignore
        if not cog:
            await interaction.response.send_message(
                "Storefront cog not loaded.", ephemeral=True
            )
            return

        await cog._handle_purchase_initiate(interaction, self.product_id)


class TicketButton(discord.ui.Button["ProductActionView"]):
    def __init__(self, product_id: int) -> None:
        super().__init__(
            label="Open Support Ticket",
            style=discord.ButtonStyle.gray,
            custom_id=f"storefront:ticket:{product_id}",
        )
        self.product_id = product_id

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: StorefrontCog = interaction.client.get_cog("StorefrontCog")  # type: ignore
        if not cog:
            await interaction.response.send_message(
                "Storefront cog not loaded.", ephemeral=True
            )
            return

        await cog._handle_support_ticket(interaction, self.product_id)


class ProductActionView(discord.ui.View):
    def __init__(self, product_id: int) -> None:
        super().__init__(timeout=300)
        self.product_id = product_id
        self.add_item(BuyButton(product_id))
        self.add_item(TicketButton(product_id))


class PurchaseConfirmModal(discord.ui.Modal):
    def __init__(self, product_id: int, product_name: str, final_price_cents: int) -> None:
        super().__init__(title=f"Confirm Purchase: {product_name[:50]}")  # Truncate for title limit
        self.product_id = product_id
        self.product_name = product_name
        self.final_price_cents = final_price_cents

        self.confirmation = discord.ui.TextInput(
            label=f"Type 'CONFIRM' to purchase for {format_usd(final_price_cents)}",
            placeholder="CONFIRM",
            required=True,
            max_length=7,
        )
        self.add_item(self.confirmation)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if self.confirmation.value.strip().upper() != "CONFIRM":
            await interaction.response.send_message(
                "Purchase cancelled. You must type 'CONFIRM' to proceed.",
                ephemeral=True,
            )
            return

        cog: StorefrontCog = interaction.client.get_cog("StorefrontCog")  # type: ignore
        if not cog:
            await interaction.response.send_message(
                "Storefront cog not loaded.", ephemeral=True
            )
            return

        await cog._complete_purchase(
            interaction, self.product_id, self.product_name, self.final_price_cents
        )


class StorefrontCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        self.bot.add_view(ProductSelectView())
        
        categories = await self.bot.db.get_distinct_main_categories()
        if categories:
            self.bot.add_view(CategorySelectView(categories))

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

    def _is_within_operating_hours(self) -> bool:
        now = datetime.now(timezone.utc)
        hours = self.bot.config.operating_hours
        current_hour = now.hour

        if hours.start_hour_utc <= hours.end_hour_utc:
            return hours.start_hour_utc <= current_hour < hours.end_hour_utc
        else:
            return current_hour >= hours.start_hour_utc or current_hour < hours.end_hour_utc

    async def _calculate_discount(
        self, user_id: int, product_id: int, vip_tier: Optional[object]
    ) -> float:
        from apex_core.utils.roles import get_user_roles
        
        total_discount = 0.0

        # Check all applicable roles for discounts
        user_roles = await get_user_roles(user_id, self.bot.db, self.bot.config)
        for role in user_roles:
            total_discount = max(total_discount, role.discount_percent)

        # Check legacy discount system
        user_row = await self.bot.db.get_user(user_id)
        db_user_id = user_row["id"] if user_row else None

        vip_tier_name = vip_tier.name if vip_tier else None
        discounts = await self.bot.db.get_applicable_discounts(
            user_id=db_user_id,
            product_id=product_id,
            vip_tier=vip_tier_name,
        )

        for discount in discounts:
            if discount["is_stackable"]:
                total_discount += discount["discount_percent"]
            else:
                total_discount = max(total_discount, discount["discount_percent"])

        return min(total_discount, 100.0)

    async def _show_sub_categories(
        self, interaction: discord.Interaction, main_category: str
    ) -> None:
        sub_categories = await self.bot.db.get_distinct_sub_categories(main_category)
        
        if not sub_categories:
            await interaction.response.send_message(
                f"No sub-categories found for **{main_category}**.",
                ephemeral=True,
            )
            return
        
        embed = create_embed(
            title=f"Select Sub-Category: {main_category}",
            description="Choose a sub-category to view available products.",
            color=discord.Color.blue(),
        )
        
        view = SubCategorySelectView(main_category, sub_categories)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def _show_products(
        self, interaction: discord.Interaction, main_category: str, sub_category: str
    ) -> None:
        products = await self.bot.db.get_products_by_category(main_category, sub_category)
        
        if not products:
            await interaction.response.send_message(
                f"No products found for **{main_category} - {sub_category}**.",
                ephemeral=True,
            )
            return
        
        product_dicts = [dict(product) for product in products]
        
        embed = create_embed(
            title=f"{main_category} {sub_category} (Service Selected)",
            description="Browse the available services below.",
            color=discord.Color.green(),
        )
        
        product_list = []
        for product in product_dicts:
            variant_name = product["variant_name"]
            price_usd = format_usd(product["price_cents"])
            start_time = product.get("start_time") or "N/A"
            duration = product.get("duration") or "N/A"
            refill_period = product.get("refill_period") or "N/A"
            additional_info = product.get("additional_info") or "N/A"
            
            product_text = (
                f"**{variant_name}**\n"
                f"Price: {price_usd}\n"
                f"Start Time: {start_time}\n"
                f"Duration: {duration}\n"
                f"Refill: {refill_period}\n"
                f"Additional: {additional_info}\n"
                f"───────────"
            )
            product_list.append(product_text)
        
        description_text = "\n".join(product_list)
        
        if len(description_text) > 4096:
            description_text = description_text[:4090] + "..."
        
        embed.description = description_text
        embed.set_footer(text="Select a service from the dropdown below, then press Open Ticket")
        
        view = ProductDisplayView(product_dicts, main_category, sub_category)
        await interaction.response.edit_message(embed=embed, view=view)

    async def _handle_open_ticket(
        self, interaction: discord.Interaction, main_category: str, sub_category: str, product_id: int
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "This command can only be used inside a server.", ephemeral=True
            )
            return

        member = self._resolve_member(interaction)
        if member is None:
            await interaction.response.send_message(
                "Unable to resolve your member profile. Please try again.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.bot.db.ensure_user(member.id)

        product = await self.bot.db.get_product(product_id)
        if not product or not product["is_active"]:
            await interaction.followup.send(
                "The selected product is no longer available.",
                ephemeral=True,
            )
            return
        
        if (
            product["main_category"] != main_category
            or product["sub_category"] != sub_category
        ):
            await interaction.followup.send(
                "Product selection mismatch. Please try again.",
                ephemeral=True,
            )
            return

        existing_ticket = await self.bot.db.get_open_ticket_for_user(member.id)
        if existing_ticket:
            channel = interaction.guild.get_channel(existing_ticket["channel_id"])
            if channel:
                await interaction.followup.send(
                    f"You already have an open ticket: {channel.mention}",
                    ephemeral=True,
                )
                return
            await self.bot.db.update_ticket_status(existing_ticket["channel_id"], "closed")

        support_category_id = self.bot.config.ticket_categories.support
        category = interaction.guild.get_channel(support_category_id)
        if not isinstance(category, discord.CategoryChannel):
            await interaction.followup.send(
                "Support category is not configured correctly. Please contact an admin.",
                ephemeral=True,
            )
            return

        admin_role = interaction.guild.get_role(self.bot.config.role_ids.admin)
        if admin_role is None:
            await interaction.followup.send(
                "Admin role is missing or misconfigured. Please notify the server owner.",
                ephemeral=True,
            )
            return

        channel_name = f"ticket-{member.name}"[:95]
        overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
            interaction.guild.default_role: discord.PermissionOverwrite(view_channel=False),
            member: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                attach_files=True,
                read_message_history=True,
            ),
            admin_role: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True,
                read_message_history=True,
            ),
        }
        if interaction.guild.me:
            overwrites[interaction.guild.me] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True,
                read_message_history=True,
            )

        try:
            channel = await category.create_text_channel(
                name=channel_name,
                overwrites=overwrites,
                reason=(
                    f"Storefront ticket for {main_category}/{sub_category}/"
                    f"{product['variant_name']} by {member.display_name}"
                ),
            )
        except discord.HTTPException as error:
            logger.error("Failed to create support ticket: %s", error)
            await interaction.followup.send(
                "Unable to create a ticket channel right now. Please try again later.",
                ephemeral=True,
            )
            return

        price_text = format_usd(product["price_cents"])
        start_time = product.get("start_time") or "N/A"
        duration = product.get("duration") or "N/A"
        refill = product.get("refill_period") or "N/A"
        additional_info = product.get("additional_info") or "N/A"

        embed = create_embed(
            title=f"Service Selected: {main_category} • {sub_category}",
            description=(
                f"{member.mention}, thanks for opening a support ticket.\n\n"
                f"We'll help you with **{product['service_name']} — {product['variant_name']}**."
            ),
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Price",
            value=price_text,
            inline=True,
        )
        embed.add_field(name="Start Time", value=start_time, inline=True)
        embed.add_field(name="Duration", value=duration, inline=True)
        embed.add_field(name="Refill", value=refill, inline=True)
        embed.add_field(name="Additional Info", value=additional_info, inline=False)
        embed.add_field(
            name="Operating Hours",
            value=render_operating_hours(self.bot.config.operating_hours),
            inline=False,
        )
        embed.set_footer(text="Apex Core • Support")

        payment_view = PaymentMethodView()

        await channel.send(
            content=(
                f"{admin_role.mention} {member.mention} — New product inquiry ticket opened."
            ),
            embed=embed,
            view=payment_view,
        )

        await self.bot.db.create_ticket(user_discord_id=member.id, channel_id=channel.id)

        try:
            dm_embed = create_embed(
                title="Support Ticket Created",
                description=(
                    f"Your support ticket for **{product['variant_name']}** is live: {channel.mention}\n"
                    f"Price: {price_text}"
                ),
                color=discord.Color.green(),
            )
            await member.send(embed=dm_embed)
        except discord.Forbidden:
            logger.warning("Could not DM user %s about ticket creation", member.id)

        await interaction.followup.send(
            f"Your support ticket is ready: {channel.mention}",
            ephemeral=True,
        )

    async def _handle_purchase_initiate(
        self, interaction: discord.Interaction, product_id: int
    ) -> None:
        product = await self.bot.db.get_product(product_id)
        if not product or not product["is_active"]:
            await interaction.response.send_message(
                "This product is no longer available.", ephemeral=True
            )
            return

        user_row = await self.bot.db.get_user(interaction.user.id)
        if not user_row:
            await self.bot.db.ensure_user(interaction.user.id)
            user_row = await self.bot.db.get_user(interaction.user.id)

        if not user_row:
            await interaction.response.send_message(
                "Unable to retrieve your user data. Please try again.", ephemeral=True
            )
            return

        vip_tier = calculate_vip_tier(
            user_row["total_lifetime_spent_cents"], self.bot.config
        )

        base_price_cents = product["price_cents"]
        discount_percent = await self._calculate_discount(
            interaction.user.id, product_id, vip_tier
        )
        final_price_cents = int(base_price_cents * (1 - discount_percent / 100))

        current_balance = user_row["wallet_balance_cents"]
        if current_balance < final_price_cents:
            await interaction.response.send_message(
                f"Insufficient balance. You need {format_usd(final_price_cents)} "
                f"but only have {format_usd(current_balance)}.",
                ephemeral=True,
            )
            return

        product_name = product.get("variant_name", product.get("name", "Unknown Product"))
        modal = PurchaseConfirmModal(product_id, product_name, final_price_cents)
        await interaction.response.send_modal(modal)

    async def _complete_purchase(
        self,
        interaction: discord.Interaction,
        product_id: int,
        product_name: str,
        final_price_cents: int,
    ) -> None:
        await interaction.response.defer(ephemeral=True, thinking=True)

        if not self._is_within_operating_hours():
            hours_text = render_operating_hours(self.bot.config.operating_hours)
            await interaction.followup.send(
                f"Purchases can only be made during operating hours: {hours_text}",
                ephemeral=True,
            )
            return

        product = await self.bot.db.get_product(product_id)
        if not product or not product["is_active"]:
            await interaction.followup.send(
                "This product is no longer available.", ephemeral=True
            )
            return

        user_row = await self.bot.db.get_user(interaction.user.id)
        if not user_row:
            await interaction.followup.send(
                "Unable to retrieve your user data. Please try again.", ephemeral=True
            )
            return

        vip_tier = calculate_vip_tier(
            user_row["total_lifetime_spent_cents"], self.bot.config
        )
        discount_percent = await self._calculate_discount(
            interaction.user.id, product_id, vip_tier
        )
        recalc_final_price = int(product["price_cents"] * (1 - discount_percent / 100))

        if recalc_final_price != final_price_cents:
            await interaction.followup.send(
                "Price has changed since you started this purchase. Please try again.",
                ephemeral=True,
            )
            return

        product_name = product.get("variant_name", product.get("name", "Unknown Product"))
        try:
            order_metadata = json.dumps({
                "product_name": product_name,
                "base_price_cents": product["price_cents"],
                "discount_percent": discount_percent,
                "vip_tier": vip_tier.name if vip_tier else None,
            })
            order_id, new_balance = await self.bot.db.purchase_product(
                user_discord_id=interaction.user.id,
                product_id=product_id,
                price_paid_cents=final_price_cents,
                discount_applied_percent=discount_percent,
                order_metadata=order_metadata,
            )
        except ValueError as e:
            await interaction.followup.send(
                f"Purchase failed: {str(e)}", ephemeral=True
            )
            return
        except Exception as e:
            logger.error("Purchase error for user %s: %s", interaction.user.id, e)
            await interaction.followup.send(
                "An error occurred while processing your purchase. Please contact support.",
                ephemeral=True,
            )
            return

        member = self._resolve_member(interaction)
        if member and product["role_id"]:
            role = interaction.guild.get_role(product["role_id"]) if interaction.guild else None
            if role:
                try:
                    await member.add_roles(role, reason=f"Purchased {product['name']}")
                except discord.HTTPException as e:
                    logger.error("Failed to assign role %s to user %s: %s", role.id, member.id, e)

        success_embed = create_embed(
            title="Purchase Complete!",
            description=(
                f"You have successfully purchased **{product_name}**.\n\n"
                f"**Amount Paid:** {format_usd(final_price_cents)}\n"
                f"**New Balance:** {format_usd(new_balance)}\n"
                f"**Order ID:** #{order_id}"
            ),
            color=discord.Color.green(),
        )
        await interaction.followup.send(embed=success_embed, ephemeral=True)

        if product["content_payload"]:
            dm_embed = create_embed(
                title=f"Product Fulfillment: {product_name}",
                description=product["content_payload"],
                color=discord.Color.gold(),
            )
            dm_embed.add_field(name="Order ID", value=f"#{order_id}", inline=True)
            dm_embed.add_field(
                name="Amount Paid", value=format_usd(final_price_cents), inline=True
            )
            try:
                await interaction.user.send(embed=dm_embed)
            except discord.Forbidden:
                logger.warning("Could not DM user %s with fulfillment content", interaction.user.id)
                await interaction.followup.send(
                    "⚠️ Could not send fulfillment details via DM. Please enable DMs from server members.",
                    ephemeral=True,
                )

        if interaction.guild:
            log_channel_id = (
                self.bot.config.logging_channels.order_logs
                or self.bot.config.logging_channels.payments
            )
            log_channel = interaction.guild.get_channel(log_channel_id)
            if isinstance(log_channel, discord.TextChannel):
                log_embed = create_embed(
                    title="Order Completed",
                    description=(
                        f"**Customer:** {interaction.user.mention} ({interaction.user.id})\n"
                        f"**Product:** {product_name}\n"
                        f"**Base Price:** {format_usd(product['price_cents'])}\n"
                        f"**Discount Applied:** {discount_percent:.1f}%\n"
                        f"**Final Price:** {format_usd(final_price_cents)}\n"
                        f"**Order ID:** #{order_id}"
                    ),
                    color=discord.Color.dark_green(),
                )
                log_embed.set_footer(text=f"User ID: {interaction.user.id}")
                try:
                    await log_channel.send(embed=log_embed)
                except discord.HTTPException as e:
                    logger.error("Failed to log order to channel %s: %s", log_channel_id, e)

        # Process post-purchase actions including VIP promotion
        old_vip_tier, new_vip_tier = await process_post_purchase(
            user_discord_id=interaction.user.id,
            amount_cents=final_price_cents,
            db=self.bot.db,
            config=self.bot.config,
            guild=interaction.guild,
        )
        
        # Handle VIP promotion notifications and role assignments
        if new_vip_tier and (not old_vip_tier or new_vip_tier.name != old_vip_tier.name):
            await handle_vip_promotion(
                user_discord_id=interaction.user.id,
                old_vip_tier=old_vip_tier,
                new_vip_tier=new_vip_tier,
                config=self.bot.config,
                guild=interaction.guild,
            )
        
        # Check and update all roles for the user
        if interaction.guild:
            from apex_core.utils import check_and_update_roles
            try:
                await check_and_update_roles(
                    interaction.user.id,
                    self.bot.db,
                    interaction.guild,
                    self.bot.config,
                )
            except Exception as e:
                logger.error("Error updating roles for user %s: %s", interaction.user.id, e)

    async def _handle_support_ticket(
        self, interaction: discord.Interaction, product_id: int
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "This command can only be used inside a server.", ephemeral=True
            )
            return

        member = self._resolve_member(interaction)
        if member is None:
            await interaction.response.send_message(
                "Unable to resolve your member profile. Please try again.", ephemeral=True
            )
            return

        product = await self.bot.db.get_product(product_id)
        if not product:
            await interaction.response.send_message(
                "Product not found.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.bot.db.ensure_user(member.id)

        existing_ticket = await self.bot.db.get_open_ticket_for_user(member.id)
        if existing_ticket:
            channel = interaction.guild.get_channel(existing_ticket["channel_id"])
            if channel:
                await interaction.followup.send(
                    f"You already have an open ticket: {channel.mention}",
                    ephemeral=True,
                )
                return
            await self.bot.db.update_ticket_status(existing_ticket["channel_id"], "closed")

        support_category_id = self.bot.config.ticket_categories.support
        category = interaction.guild.get_channel(support_category_id)
        if not isinstance(category, discord.CategoryChannel):
            await interaction.followup.send(
                "Support category is not configured correctly. Please contact an admin.",
                ephemeral=True,
            )
            return

        admin_role = interaction.guild.get_role(self.bot.config.role_ids.admin)
        if admin_role is None:
            await interaction.followup.send(
                "Admin role is missing or misconfigured. Please notify the server owner.",
                ephemeral=True,
            )
            return

        channel_name = f"support-{member.name}"[:95]
        overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
            interaction.guild.default_role: discord.PermissionOverwrite(view_channel=False),
            member: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                attach_files=True,
                read_message_history=True,
            ),
            admin_role: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True,
                read_message_history=True,
            ),
        }
        if interaction.guild.me:
            overwrites[interaction.guild.me] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True,
                read_message_history=True,
            )

        try:
            channel = await category.create_text_channel(
                name=channel_name,
                overwrites=overwrites,
                reason=f"Support ticket for {product['name']} by {member.display_name}",
            )
        except discord.HTTPException as error:
            logger.error("Failed to create support ticket: %s", error)
            await interaction.followup.send(
                "Unable to create a ticket channel right now. Please try again later.",
                ephemeral=True,
            )
            return

        product_name = product.get("variant_name", product.get("name", "Unknown Product"))
        embed = create_embed(
            title="Support Ticket",
            description=(
                f"{member.mention}, thanks for opening a support ticket.\n\n"
                f"**Product Inquiry:** {product_name}\n"
                f"**Price:** {format_usd(product['price_cents'])}\n\n"
                "Our team will assist you shortly."
            ),
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Operating Hours",
            value=render_operating_hours(self.bot.config.operating_hours),
            inline=False,
        )
        embed.set_footer(text="Apex Core • Support")

        await channel.send(
            content=f"{admin_role.mention} {member.mention} — New support ticket opened.",
            embed=embed,
        )
        await self.bot.db.create_ticket(user_discord_id=member.id, channel_id=channel.id)

        await interaction.followup.send(
            f"Your support ticket is ready: {channel.mention}",
            ephemeral=True,
        )

    @commands.command(name="setup_store")
    async def setup_store(self, ctx: commands.Context) -> None:
        """Setup the permanent storefront message with cascading dropdowns."""
        if ctx.guild is None:
            await ctx.send("This command can only be used in a server.")
            return

        member = ctx.author if isinstance(ctx.author, discord.Member) else None
        if not self._is_admin(member):
            await ctx.send("You do not have permission to use this command.")
            return

        categories = await self.bot.db.get_distinct_main_categories()
        if not categories:
            await ctx.send("No active products found in the database.")
            return

        embed = create_embed(
            title="Apex Core: Products",
            description="Select a product from the drop-down menu to view details and open a support ticket.",
            color=discord.Color.gold(),
        )
        embed.set_footer(text="Apex Core • Storefront")

        view = CategorySelectView(categories)
        await ctx.send(embed=embed, view=view)
        
        await ctx.send("✅ Store setup complete in this channel", delete_after=5)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(StorefrontCog(bot))
