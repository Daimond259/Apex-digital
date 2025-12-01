from __future__ import annotations

import logging
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Sequence

import discord
from discord import app_commands
from discord.ext import commands

from apex_core.config import PaymentMethod, PaymentSettings
from apex_core.utils import create_embed, format_usd, render_operating_hours

logger = logging.getLogger(__name__)

REQUIRED_PAYMENT_METHODS = ("Binance Pay", "Tip.cc", "Crypto Address")


def _slugify(value: str, *, fallback: str = "value", max_length: int = 80) -> str:
    slug = re.sub(r"[^a-z0-9-]+", "-", value.lower())
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    if not slug:
        slug = fallback
    return slug[:max_length]


def _metadata_lines(metadata: dict[str, str]) -> str:
    parts: list[str] = []
    for key, value in metadata.items():
        if key.lower() == "url":
            continue
        pretty_key = key.replace("_", " ").title()
        parts.append(f"**{pretty_key}:** {value}")
    return "\n".join(parts)


def _method_label(method: PaymentMethod) -> str:
    prefix = f"{method.emoji} " if method.emoji else ""
    return f"{prefix}{method.name}"


class PaymentInstructionButton(discord.ui.Button["PaymentInstructionsView"]):
    def __init__(self, method: PaymentMethod) -> None:
        label = _method_label(method)
        custom_id = f"wallet:payment:{_slugify(method.name, fallback='method', max_length=40)}"
        super().__init__(label=label, style=discord.ButtonStyle.secondary, custom_id=custom_id)
        self.method = method

    async def callback(self, interaction: discord.Interaction) -> None:  # type: ignore[override]
        metadata = _metadata_lines(self.method.metadata)
        description = f"**{self.method.name} Instructions**\n{self.method.instructions}"
        if metadata:
            description += f"\n\n{metadata}"
        await interaction.response.send_message(description, ephemeral=True)


class PaymentInstructionsView(discord.ui.View):
    def __init__(self, methods: Sequence[PaymentMethod]) -> None:
        super().__init__(timeout=None)
        for method in methods:
            label = _method_label(method)
            url = method.metadata.get("url")
            if url:
                self.add_item(
                    discord.ui.Button(label=label, style=discord.ButtonStyle.link, url=url)
                )
            else:
                self.add_item(PaymentInstructionButton(method))


class WalletCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    @property
    def payment_settings(self) -> PaymentSettings | None:
        """Get the payment settings, falling back to None if not configured."""
        return self.bot.config.payment_settings

    # region Helpers
    def _operating_hours_text(self) -> str:
        return render_operating_hours(self.bot.config.operating_hours)

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

    def _get_payment_methods(self, required: Sequence[str]) -> list[PaymentMethod]:
        # Use payment settings if available, otherwise fall back to legacy payment methods
        if self.bot.config.payment_settings:
            lookup = {method.name.lower(): method for method in self.bot.config.payment_settings.payment_methods}
        else:
            lookup = {method.name.lower(): method for method in self.bot.config.payment_methods}
        
        missing = [name for name in required if name.lower() not in lookup]
        if missing:
            raise RuntimeError(
                "Missing payment methods in configuration: " + ", ".join(missing)
            )
        return [lookup[name.lower()] for name in required]

    def _format_method_value(self, method: PaymentMethod) -> str:
        metadata = _metadata_lines(method.metadata)
        if metadata:
            return f"{method.instructions}\n\n{metadata}"
        return method.instructions

    def _build_ticket_embed(
        self,
        member: discord.Member,
        methods: Sequence[PaymentMethod],
    ) -> discord.Embed:
        embed = create_embed(
            title="Wallet Deposit Ticket",
            description=(
                f"{member.mention}, thanks for opening a deposit ticket.\n"
                "Choose a payment option below and attach your transaction details for faster crediting."
            ),
            color=discord.Color.blurple(),
        )
        embed.add_field(name="Operating Hours", value=self._operating_hours_text(), inline=False)
        for method in methods:
            name = _method_label(method)
            embed.add_field(name=name, value=self._format_method_value(method), inline=False)
        embed.set_footer(text="Apex Core • Wallet Deposits")
        return embed

    def _channel_overwrites(
        self,
        guild: discord.Guild,
        member: discord.Member,
        admin_role: discord.Role,
    ) -> dict[discord.abc.Snowflake, discord.PermissionOverwrite]:
        overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
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
        if guild.me:
            overwrites[guild.me] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True,
                read_message_history=True,
            )
        return overwrites

    def _ticket_channel_name(self, member: discord.Member) -> str:
        base = _slugify(member.name, fallback=str(member.id))
        return f"deposit-{base}"[:95]

    def _to_cents(self, amount: float) -> int:
        quantized = Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return int(quantized * 100)

    # endregion

    @app_commands.command(name="deposit", description="Open a private deposit ticket with staff.")
    async def deposit(self, interaction: discord.Interaction) -> None:
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

        try:
            payment_methods = self._get_payment_methods(REQUIRED_PAYMENT_METHODS)
        except RuntimeError as exc:
            logger.error("Deposit command blocked: %s", exc)
            await interaction.response.send_message(
                "Deposit methods are not configured. Please contact staff.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.bot.db.ensure_user(member.id)

        existing_ticket = await self.bot.db.get_open_ticket_for_user(member.id)
        if existing_ticket:
            channel = interaction.guild.get_channel(existing_ticket["channel_id"])
            if channel:
                await interaction.followup.send(
                    f"You already have an open deposit ticket: {channel.mention}",
                    ephemeral=True,
                )
                return
            await self.bot.db.update_ticket_status(existing_ticket["channel_id"], "closed")

        billing_category_id = self.bot.config.ticket_categories.billing
        category = interaction.guild.get_channel(billing_category_id)
        if not isinstance(category, discord.CategoryChannel):
            await interaction.followup.send(
                "Billing category is not configured correctly. Please contact an admin.",
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

        channel_name = self._ticket_channel_name(member)
        overwrites = self._channel_overwrites(interaction.guild, member, admin_role)

        try:
            channel = await category.create_text_channel(
                name=channel_name,
                overwrites=overwrites,
                reason=f"Deposit ticket opened by {member.display_name}",
            )
        except discord.HTTPException as error:
            logger.error("Failed to create deposit ticket: %s", error)
            await interaction.followup.send(
                "Unable to create a ticket channel right now. Please try again later.",
                ephemeral=True,
            )
            return

        embed = self._build_ticket_embed(member, payment_methods)
        view = PaymentInstructionsView(payment_methods)

        await channel.send(
            content=f"{admin_role.mention} {member.mention} — New deposit ticket opened.",
            embed=embed,
            view=view,
        )
        await self.bot.db.create_ticket(user_discord_id=member.id, channel_id=channel.id)

        await interaction.followup.send(
            f"Your deposit ticket is ready: {channel.mention}",
            ephemeral=True,
        )

    @app_commands.command(name="balance", description="Check wallet balances.")
    @app_commands.describe(member="Optional member to inspect (admins only)")
    async def balance(
        self,
        interaction: discord.Interaction,
        member: discord.Member | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "This command must be used in a server.", ephemeral=True
            )
            return

        requester = self._resolve_member(interaction)
        if requester is None:
            await interaction.response.send_message(
                "Unable to resolve your member profile. Please try again.", ephemeral=True
            )
            return

        target = member or requester
        if member and not self._is_admin(requester):
            await interaction.response.send_message(
                "Only admins can view other members' balances.", ephemeral=True
            )
            return

        await self.bot.db.ensure_user(target.id)
        user_row = await self.bot.db.get_user(target.id)
        if user_row is None:
            await interaction.response.send_message(
                "Unable to locate that wallet record.", ephemeral=True
            )
            return

        from apex_core.utils import get_user_roles
        
        user_roles = await self.bot.db.get_manually_assigned_roles(target.id)
        highest_auto_role = None
        if interaction.guild:
            roles = await get_user_roles(target.id, self.bot.db, self.bot.config)
            # Get highest priority automatic role
            auto_roles = [r for r in roles if r.assignment_mode != "manual"]
            if auto_roles:
                highest_auto_role = min(auto_roles, key=lambda r: r.tier_priority)

        embed = create_embed(
            title=f"Wallet Balance • {target.display_name}",
            color=discord.Color.gold(),
        )
        embed.add_field(
            name="Available Balance",
            value=format_usd(user_row["wallet_balance_cents"]),
            inline=True,
        )
        embed.add_field(
            name="Lifetime Spend",
            value=format_usd(user_row["total_lifetime_spent_cents"]),
            inline=True,
        )
        role_value = highest_auto_role.name if highest_auto_role else "None"
        embed.add_field(name="Highest Rank", value=role_value, inline=True)
        embed.add_field(name="Operating Hours", value=self._operating_hours_text(), inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="addbalance", description="Credit funds to a member's wallet.")
    @app_commands.describe(
        member="Member that should receive the funds",
        amount="Amount in USD to credit",
        reason="Reason for the credit",
        notify_user="DM the member about this deposit",
    )
    async def add_balance(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        amount: app_commands.Range[float, 0.01, 100000.0],
        reason: str,
        notify_user: bool = True,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "This command must be used in a server.", ephemeral=True
            )
            return

        requester = self._resolve_member(interaction)
        if not self._is_admin(requester):
            await interaction.response.send_message(
                "You do not have permission to use this command.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            cents = self._to_cents(amount)
        except (InvalidOperation, ValueError):
            await interaction.followup.send("Invalid amount provided.", ephemeral=True)
            return

        reason = reason.strip()
        if not reason:
            await interaction.followup.send(
                "A reason is required for auditing purposes.", ephemeral=True
            )
            return

        await self.bot.db.ensure_user(member.id)
        new_balance_cents = await self.bot.db.update_wallet_balance(member.id, cents)

        await self.bot.db.log_wallet_transaction(
            user_discord_id=member.id,
            amount_cents=cents,
            balance_after_cents=new_balance_cents,
            transaction_type="admin_credit",
            description=reason,
            staff_discord_id=requester.id if requester else None,
        )

        amount_str = format_usd(cents)
        new_balance_str = format_usd(new_balance_cents)

        embed = create_embed(
            title="Balance Updated",
            description=(
                f"{amount_str} added to {member.mention}'s wallet.\n"
                f"Reason: {reason}\nNew Balance: {new_balance_str}"
            ),
            color=discord.Color.green(),
        )
        embed.add_field(name="Operating Hours", value=self._operating_hours_text(), inline=False)

        await interaction.followup.send(embed=embed, ephemeral=True)

        audit_channel_id = self.bot.config.logging_channels.audit
        audit_channel = interaction.guild.get_channel(audit_channel_id)
        if isinstance(audit_channel, discord.TextChannel):
            audit_embed = create_embed(
                title="Wallet Credit Logged",
                description=(
                    f"{requester.mention} credited {amount_str} to {member.mention}.\n"
                    f"Reason: {reason}\nNew Balance: {new_balance_str}"
                ),
                color=discord.Color.dark_green(),
            )
            audit_embed.add_field(
                name="Operating Hours", value=self._operating_hours_text(), inline=False
            )
            await audit_channel.send(embed=audit_embed)
        else:
            logger.warning("Audit channel %s not found", audit_channel_id)

        if notify_user:
            dm_embed = create_embed(
                title="Wallet Credit Received",
                description=(
                    f"{amount_str} has been added to your wallet for: {reason}.\n"
                    f"Your new balance is {new_balance_str}."
                ),
                color=discord.Color.green(),
            )
            dm_embed.add_field(
                name="Operating Hours", value=self._operating_hours_text(), inline=False
            )
            try:
                await member.send(embed=dm_embed)
            except discord.Forbidden:
                await interaction.followup.send(
                    "Could not DM the member about this credit.", ephemeral=True
                )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(WalletCog(bot))
