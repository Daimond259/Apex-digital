"""Ticket lifecycle automation with transcript export."""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import TYPE_CHECKING, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

if TYPE_CHECKING:
    from bot import ApexCoreBot

try:
    import chat_exporter
    CHAT_EXPORTER_AVAILABLE = True
except ImportError:
    CHAT_EXPORTER_AVAILABLE = False

from apex_core.utils import (
    create_embed,
    discord_timestamp,
    operating_hours_window,
    render_operating_hours,
)

logger = logging.getLogger(__name__)

INACTIVITY_WARNING_HOURS = 48
INACTIVITY_CLOSE_HOURS = 49
CHECK_INTERVAL_MINUTES = 10


class TicketManagementCog(commands.Cog):
    def __init__(self, bot: ApexCoreBot) -> None:
        self.bot = bot
        self.warned_tickets: set[int] = set()
        self.ticket_lifecycle_task.start()

    def cog_unload(self) -> None:
        self.ticket_lifecycle_task.cancel()

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

    async def _get_text_channel(self, channel_id: int) -> discord.TextChannel | None:
        channel = self.bot.get_channel(channel_id)
        if isinstance(channel, discord.TextChannel):
            return channel

        try:
            fetched = await self.bot.fetch_channel(channel_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

        return fetched if isinstance(fetched, discord.TextChannel) else None

    def _parse_timestamp(self, value: str | datetime) -> datetime:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str):
            normalized = value.replace(" ", "T") if " " in value and "T" not in value else value
            dt = datetime.fromisoformat(normalized)
        else:
            raise ValueError(f"Unsupported timestamp type: {type(value)!r}")

        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @tasks.loop(minutes=CHECK_INTERVAL_MINUTES)
    async def ticket_lifecycle_task(self) -> None:
        try:
            await self._process_stale_tickets()
        except Exception as e:
            logger.error("Error in ticket lifecycle task: %s", e, exc_info=True)

    @ticket_lifecycle_task.before_loop
    async def before_ticket_lifecycle_task(self) -> None:
        await self.bot.wait_until_ready()
        logger.info("Ticket lifecycle task started")

    async def _process_stale_tickets(self) -> None:
        open_tickets = await self.bot.db.get_open_tickets()
        
        if not open_tickets:
            return

        now = datetime.now(timezone.utc)
        
        for ticket in open_tickets:
            try:
                last_activity = self._parse_timestamp(ticket["last_activity"])
                hours_inactive = (now - last_activity).total_seconds() / 3600
                
                channel_id = ticket["channel_id"]
                channel = await self._get_text_channel(channel_id)
                
                if not channel:
                    logger.warning("Ticket channel %s not found or not accessible", channel_id)
                    await self.bot.db.update_ticket_status(channel_id, "resolved")
                    continue
                
                if hours_inactive >= INACTIVITY_CLOSE_HOURS:
                    await self._close_ticket(ticket, channel)
                elif hours_inactive >= INACTIVITY_WARNING_HOURS:
                    if channel_id not in self.warned_tickets:
                        await self._send_inactivity_warning(ticket, channel, last_activity)
                        self.warned_tickets.add(channel_id)
                else:
                    self.warned_tickets.discard(channel_id)
                    
            except Exception as e:
                logger.error("Error processing ticket %s: %s", ticket["id"], e, exc_info=True)

    async def _export_and_log_ticket(
        self,
        ticket: dict,
        channel: discord.TextChannel,
        reason: str = "Inactivity",
    ) -> None:
        """Export ticket transcript and log to channels."""
        user_discord_id = ticket["user_discord_id"]
        
        transcript_file = None
        transcript_html = None
        
        if CHAT_EXPORTER_AVAILABLE:
            try:
                transcript_html = await chat_exporter.export(
                    channel,
                    limit=None,
                    tz_info="UTC",
                    bot=self.bot,
                )
                
                if transcript_html:
                    transcript_bytes = transcript_html.encode("utf-8")
                    transcript_file = discord.File(
                        BytesIO(transcript_bytes),
                        filename=f"ticket-{ticket['id']}-{channel.name}.html"
                    )
            except Exception as e:
                logger.error("Failed to generate transcript for ticket %s: %s", ticket["id"], e)
        else:
            logger.warning("chat_exporter not available, generating fallback transcript")
            transcript_html = await self._generate_fallback_transcript(channel, ticket)
        
        log_channel_id = self.bot.config.logging_channels.tickets
        log_channel = channel.guild.get_channel(log_channel_id)
        if not isinstance(log_channel, discord.TextChannel):
            global_channel = self.bot.get_channel(log_channel_id)
            log_channel = global_channel if isinstance(global_channel, discord.TextChannel) else None
        
        if isinstance(log_channel, discord.TextChannel):
            created_at = self._parse_timestamp(ticket["created_at"])
            
            log_embed = create_embed(
                title="Ticket Closed",
                description=(
                    f"**Ticket ID:** #{ticket['id']}\n"
                    f"**Channel:** {channel.mention} ({channel.id})\n"
                    f"**User:** <@{user_discord_id}> ({user_discord_id})\n"
                    f"**Reason:** {reason}\n"
                    f"**Created:** {discord_timestamp(created_at, 'f')}\n"
                    f"**Closed:** {discord_timestamp(datetime.now(timezone.utc), 'f')}"
                ),
                color=discord.Color.dark_gray(),
                timestamp=True,
            )
            
            try:
                if transcript_file and CHAT_EXPORTER_AVAILABLE:
                    await log_channel.send(embed=log_embed, file=transcript_file)
                else:
                    await log_channel.send(embed=log_embed)
                logger.info("Logged ticket closure to channel %s", log_channel_id)
            except discord.HTTPException as e:
                logger.error("Failed to log ticket closure to channel %s: %s", log_channel_id, e)
        
        archive_channel_id = self.bot.config.logging_channels.transcript_archive
        if archive_channel_id and transcript_html and CHAT_EXPORTER_AVAILABLE:
            archive_channel = channel.guild.get_channel(archive_channel_id)
            if isinstance(archive_channel, discord.TextChannel):
                archive_file = discord.File(
                    BytesIO(transcript_html.encode("utf-8")),
                    filename=f"ticket-{ticket['id']}-{channel.name}.html"
                )
                archive_embed = create_embed(
                    title=f"Ticket #{ticket['id']} Transcript",
                    description=(
                        f"**Channel:** {channel.name}\n"
                        f"**User:** <@{user_discord_id}>\n"
                        f"**Closed:** {reason}"
                    ),
                    color=discord.Color.blue(),
                    timestamp=True,
                )
                try:
                    await archive_channel.send(embed=archive_embed, file=archive_file)
                    logger.info("Archived transcript to channel %s", archive_channel_id)
                except discord.HTTPException as e:
                    logger.error("Failed to archive transcript to channel %s: %s", archive_channel_id, e)
        
        if transcript_html:
            try:
                storage_path, file_size = await self.bot.storage.save_transcript(
                    ticket_id=ticket["id"],
                    channel_name=channel.name,
                    content=transcript_html,
                )
                
                await self.bot.db.save_transcript(
                    ticket_id=ticket["id"],
                    user_discord_id=user_discord_id,
                    channel_id=channel.id,
                    storage_type=self.bot.storage.storage_type,
                    storage_path=storage_path,
                    file_size_bytes=file_size,
                )
                
                logger.info(
                    "Saved transcript for ticket %s to %s storage: %s",
                    ticket["id"],
                    self.bot.storage.storage_type,
                    storage_path
                )
            except Exception as e:
                logger.error("Failed to save transcript to storage for ticket %s: %s", ticket["id"], e)
        
        return transcript_html

    async def _generate_fallback_transcript(
        self,
        channel: discord.TextChannel,
        ticket: dict,
    ) -> str:
        """Generate a simple text-based transcript when chat_exporter is unavailable."""
        messages = []
        try:
            async for message in channel.history(limit=None, oldest_first=True):
                timestamp = message.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
                author = f"{message.author.name}#{message.author.discriminator}"
                content = message.content or "[No text content]"
                
                if message.attachments:
                    attachments = ", ".join([att.url for att in message.attachments])
                    content += f" [Attachments: {attachments}]"
                
                messages.append(f"[{timestamp}] {author}: {content}")
        except Exception as e:
            logger.error("Failed to fetch messages for fallback transcript: %s", e)
            messages.append(f"[Error] Failed to fetch complete message history: {e}")
        
        html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Ticket #{ticket['id']} Transcript</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #2c2f33; color: #dcddde; }}
        .header {{ background: #23272a; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .message {{ margin: 10px 0; padding: 10px; background: #23272a; border-radius: 3px; }}
        .timestamp {{ color: #72767d; font-size: 0.85em; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Ticket #{ticket['id']} - {channel.name}</h1>
        <p>User: {ticket['user_discord_id']}</p>
        <p>Created: {ticket['created_at']}</p>
        <p><em>Note: This is a fallback transcript generated without chat_exporter</em></p>
    </div>
    <div class="messages">
"""
        
        for msg in messages:
            html_content += f'        <div class="message">{msg}</div>\n'
        
        html_content += """    </div>
</body>
</html>"""
        
        return html_content

    async def _send_inactivity_warning(
        self,
        ticket: dict,
        channel: discord.TextChannel,
        last_activity: datetime,
    ) -> None:
        close_time = last_activity + timedelta(hours=INACTIVITY_CLOSE_HOURS)
        close_timestamp = discord_timestamp(close_time, style="R")
        operating_hours_text = render_operating_hours(self.bot.config.operating_hours)
        hours_start, hours_end = operating_hours_window(self.bot.config.operating_hours)

        member = channel.guild.get_member(ticket["user_discord_id"])
        user_mention = member.mention if member else f"<@{ticket['user_discord_id']}>"
        
        embed = create_embed(
            title="⚠️ Ticket Inactivity Warning",
            description=(
                f"This ticket has been inactive for {INACTIVITY_WARNING_HOURS} hours.\n\n"
                f"If there is no activity, this ticket will be automatically closed {close_timestamp}.\n\n"
                f"Our staff typically returns between {hours_start} and {hours_end} UTC."
            ),
            color=discord.Color.orange(),
            timestamp=True,
        )
        embed.add_field(name="Operating Hours", value=operating_hours_text, inline=False)
        embed.add_field(
            name="Need help?",
            value="Send any message in this channel to keep the ticket open.",
            inline=False,
        )
        
        try:
            await channel.send(content=user_mention, embed=embed)
            logger.info("Sent inactivity warning for ticket %s (channel %s)", ticket["id"], channel.id)
        except discord.HTTPException as e:
            logger.error("Failed to send inactivity warning to channel %s: %s", channel.id, e)

    async def _close_ticket(self, ticket: dict, channel: discord.TextChannel) -> None:
        logger.info("Auto-closing ticket %s (channel %s)", ticket["id"], channel.id)
        
        user_discord_id = ticket["user_discord_id"]
        
        try:
            closing_embed = create_embed(
                title="Ticket Closed",
                description=(
                    "This ticket has been automatically closed due to inactivity.\n\n"
                    f"If you need further assistance, please open a new ticket.\n\n"
                    f"**Operating Hours:** {render_operating_hours(self.bot.config.operating_hours)}"
                ),
                color=discord.Color.red(),
                timestamp=True,
            )
            
            try:
                await channel.send(embed=closing_embed)
            except discord.HTTPException as e:
                logger.warning("Failed to send closing message in channel %s: %s", channel.id, e)
            
            await self.bot.db.update_ticket_status(channel.id, "resolved")
            await self.bot.db.update_ticket(channel.id, closed_at=datetime.now(timezone.utc).isoformat())
            self.warned_tickets.discard(channel.id)
            
            transcript_html = await self._export_and_log_ticket(
                ticket, 
                channel, 
                reason=f"Inactivity ({INACTIVITY_CLOSE_HOURS}h)"
            )
            
            try:
                user = await self.bot.fetch_user(user_discord_id)
                
                dm_embed = create_embed(
                    title="Ticket Closed",
                    description=(
                        f"Your support ticket **{channel.name}** has been closed due to inactivity.\n\n"
                        "If you need further assistance, please open a new ticket.\n\n"
                        f"**Operating Hours:** {render_operating_hours(self.bot.config.operating_hours)}"
                    ),
                    color=discord.Color.red(),
                    timestamp=True,
                )
                
                if transcript_html and CHAT_EXPORTER_AVAILABLE:
                    transcript_file_copy = discord.File(
                        BytesIO(transcript_html.encode("utf-8")),
                        filename=f"ticket-{ticket['id']}-transcript.html"
                    )
                    try:
                        await user.send(embed=dm_embed, file=transcript_file_copy)
                        logger.info("Sent transcript DM to user %s for ticket %s", user_discord_id, ticket["id"])
                    except discord.HTTPException as e:
                        logger.warning("Failed to DM user %s with transcript: %s", user_discord_id, e)
                        await user.send(embed=dm_embed)
                else:
                    await user.send(embed=dm_embed)
                    logger.info("Sent closure DM to user %s for ticket %s", user_discord_id, ticket["id"])
                    
            except discord.Forbidden:
                logger.warning("Could not DM user %s about ticket closure", user_discord_id)
            except discord.NotFound:
                logger.warning("User %s not found for ticket closure notification", user_discord_id)
            except discord.HTTPException as e:
                logger.error("Error sending DM to user %s: %s", user_discord_id, e)
            
            try:
                await channel.delete(reason=f"Ticket auto-closed due to inactivity (ID: {ticket['id']})")
                logger.info("Deleted ticket channel %s", channel.id)
            except discord.HTTPException as e:
                logger.error("Failed to delete channel %s: %s", channel.id, e)
                
        except Exception as e:
            logger.error("Error closing ticket %s: %s", ticket["id"], e, exc_info=True)

    ticket_group = app_commands.Group(name="ticket", description="Ticket management commands")

    @ticket_group.command(name="order", description="Open an order support ticket")
    @app_commands.describe(
        order_id="Your order ID (optional)",
        description="Brief description of your issue",
    )
    async def ticket_order(
        self,
        interaction: discord.Interaction,
        description: str,
        order_id: Optional[int] = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "This command must be used in a server.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        user_id = interaction.user.id
        existing_ticket = await self.bot.db.get_open_ticket_for_user(user_id)
        if existing_ticket:
            channel_mention = f"<#{existing_ticket['channel_id']}>"
            await interaction.followup.send(
                f"You already have an open ticket: {channel_mention}\n"
                "Please close it before opening a new one.",
                ephemeral=True
            )
            return

        category_id = self.bot.config.ticket_categories.support
        category = interaction.guild.get_channel(category_id)
        if not isinstance(category, discord.CategoryChannel):
            await interaction.followup.send(
                "Unable to create ticket: support category not found.",
                ephemeral=True
            )
            logger.error("Support category %s not found", category_id)
            return

        try:
            channel = await interaction.guild.create_text_channel(
                name=f"order-{user_id}",
                category=category,
                reason=f"Order support ticket for {interaction.user.name}",
            )

            ticket_id = await self.bot.db.create_ticket(
                user_discord_id=user_id,
                channel_id=channel.id,
                status="open",
                ticket_type="order",
                order_id=order_id,
            )

            embed = create_embed(
                title="Order Support Ticket",
                description=f"{interaction.user.mention} opened a support ticket for order assistance.\n\n**Issue:** {description}",
                color=discord.Color.blue(),
                timestamp=True,
            )
            if order_id:
                embed.add_field(name="Order ID", value=str(order_id), inline=True)
            embed.add_field(name="Ticket ID", value=f"#{ticket_id}", inline=True)

            await channel.send(embed=embed)
            
            member = interaction.user
            if isinstance(member, discord.Member):
                try:
                    await channel.set_permissions(
                        member,
                        read_messages=True,
                        send_messages=True,
                        view_channel=True,
                    )
                except discord.HTTPException as e:
                    logger.warning("Failed to set channel permissions for ticket %s: %s", ticket_id, e)

            await interaction.followup.send(
                f"✅ Ticket created: {channel.mention}",
                ephemeral=True
            )
            logger.info("Created order ticket #%s for user %s", ticket_id, user_id)

        except discord.HTTPException as e:
            await interaction.followup.send(
                "Failed to create ticket. Please try again.",
                ephemeral=True
            )
            logger.error("Failed to create ticket channel: %s", e)

    @ticket_group.command(name="warranty", description="Open a warranty support ticket")
    @app_commands.describe(
        product_name="Name of the product",
        description="Brief description of your warranty issue",
    )
    async def ticket_warranty(
        self,
        interaction: discord.Interaction,
        product_name: str,
        description: str,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "This command must be used in a server.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        user_id = interaction.user.id
        existing_ticket = await self.bot.db.get_open_ticket_for_user(user_id)
        if existing_ticket:
            channel_mention = f"<#{existing_ticket['channel_id']}>"
            await interaction.followup.send(
                f"You already have an open ticket: {channel_mention}\n"
                "Please close it before opening a new one.",
                ephemeral=True
            )
            return

        category_id = self.bot.config.ticket_categories.support
        category = interaction.guild.get_channel(category_id)
        if not isinstance(category, discord.CategoryChannel):
            await interaction.followup.send(
                "Unable to create ticket: support category not found.",
                ephemeral=True
            )
            logger.error("Support category %s not found", category_id)
            return

        try:
            channel = await interaction.guild.create_text_channel(
                name=f"warranty-{user_id}",
                category=category,
                reason=f"Warranty support ticket for {interaction.user.name}",
            )

            ticket_id = await self.bot.db.create_ticket(
                user_discord_id=user_id,
                channel_id=channel.id,
                status="open",
                ticket_type="warranty",
                priority="high",
            )

            embed = create_embed(
                title="Warranty Support Ticket",
                description=f"{interaction.user.mention} opened a warranty support ticket.\n\n**Product:** {product_name}\n**Issue:** {description}",
                color=discord.Color.yellow(),
                timestamp=True,
            )
            embed.add_field(name="Ticket ID", value=f"#{ticket_id}", inline=True)
            embed.add_field(name="Priority", value="High", inline=True)

            await channel.send(embed=embed)
            
            member = interaction.user
            if isinstance(member, discord.Member):
                try:
                    await channel.set_permissions(
                        member,
                        read_messages=True,
                        send_messages=True,
                        view_channel=True,
                    )
                except discord.HTTPException as e:
                    logger.warning("Failed to set channel permissions for ticket %s: %s", ticket_id, e)

            await interaction.followup.send(
                f"✅ Warranty ticket created: {channel.mention}",
                ephemeral=True
            )
            logger.info("Created warranty ticket #%s for user %s", ticket_id, user_id)

        except discord.HTTPException as e:
            await interaction.followup.send(
                "Failed to create ticket. Please try again.",
                ephemeral=True
            )
            logger.error("Failed to create ticket channel: %s", e)

    @ticket_group.command(name="general", description="Open a general support ticket")
    @app_commands.describe(
        description="Brief description of your issue",
    )
    async def ticket_general(
        self,
        interaction: discord.Interaction,
        description: str,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "This command must be used in a server.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        user_id = interaction.user.id
        existing_ticket = await self.bot.db.get_open_ticket_for_user(user_id)
        if existing_ticket:
            channel_mention = f"<#{existing_ticket['channel_id']}>"
            await interaction.followup.send(
                f"You already have an open ticket: {channel_mention}\n"
                "Please close it before opening a new one.",
                ephemeral=True
            )
            return

        category_id = self.bot.config.ticket_categories.support
        category = interaction.guild.get_channel(category_id)
        if not isinstance(category, discord.CategoryChannel):
            await interaction.followup.send(
                "Unable to create ticket: support category not found.",
                ephemeral=True
            )
            logger.error("Support category %s not found", category_id)
            return

        try:
            channel = await interaction.guild.create_text_channel(
                name=f"support-{user_id}",
                category=category,
                reason=f"General support ticket for {interaction.user.name}",
            )

            ticket_id = await self.bot.db.create_ticket(
                user_discord_id=user_id,
                channel_id=channel.id,
                status="open",
                ticket_type="support",
            )

            embed = create_embed(
                title="General Support Ticket",
                description=f"{interaction.user.mention} opened a support ticket.\n\n**Issue:** {description}",
                color=discord.Color.blue(),
                timestamp=True,
            )
            embed.add_field(name="Ticket ID", value=f"#{ticket_id}", inline=True)

            await channel.send(embed=embed)
            
            member = interaction.user
            if isinstance(member, discord.Member):
                try:
                    await channel.set_permissions(
                        member,
                        read_messages=True,
                        send_messages=True,
                        view_channel=True,
                    )
                except discord.HTTPException as e:
                    logger.warning("Failed to set channel permissions for ticket %s: %s", ticket_id, e)

            await interaction.followup.send(
                f"✅ Support ticket created: {channel.mention}",
                ephemeral=True
            )
            logger.info("Created general support ticket #%s for user %s", ticket_id, user_id)

        except discord.HTTPException as e:
            await interaction.followup.send(
                "Failed to create ticket. Please try again.",
                ephemeral=True
            )
            logger.error("Failed to create ticket channel: %s", e)

    @ticket_group.command(name="close", description="Close a support ticket (admin only)")
    @app_commands.describe(
        channel="The ticket channel to close",
        reason="Reason for closing the ticket",
    )
    async def ticket_close(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        reason: str = "Manually closed by staff",
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

        ticket = await self.bot.db.get_ticket_by_channel(channel.id)
        if not ticket:
            await interaction.followup.send(
                f"No ticket found for {channel.mention}.",
                ephemeral=True
            )
            return

        if ticket["status"] != "open":
            await interaction.followup.send(
                f"Ticket is not open (status: {ticket['status']}).",
                ephemeral=True
            )
            return

        try:
            closing_embed = create_embed(
                title="Ticket Closed",
                description=f"This ticket has been closed by staff.\n\n**Reason:** {reason}\n\nIf you need further assistance, please open a new ticket.",
                color=discord.Color.red(),
                timestamp=True,
            )

            try:
                await channel.send(embed=closing_embed)
            except discord.HTTPException as e:
                logger.warning("Failed to send closing message in channel %s: %s", channel.id, e)

            await self.bot.db.update_ticket_status(channel.id, "resolved")
            await self.bot.db.update_ticket(
                channel.id,
                closed_at=datetime.now(timezone.utc).isoformat(),
                assigned_staff_id=interaction.user.id,
            )
            self.warned_tickets.discard(channel.id)

            transcript_html = await self._export_and_log_ticket(ticket, channel, reason=reason)

            user_id = ticket["user_discord_id"]
            try:
                user = await self.bot.fetch_user(user_id)

                dm_embed = create_embed(
                    title="Ticket Closed",
                    description=(
                        f"Your support ticket **{channel.name}** has been closed by staff.\n\n"
                        f"**Reason:** {reason}\n\n"
                        "If you need further assistance, please open a new ticket."
                    ),
                    color=discord.Color.red(),
                    timestamp=True,
                )

                if transcript_html and CHAT_EXPORTER_AVAILABLE:
                    transcript_file = discord.File(
                        BytesIO(transcript_html.encode("utf-8")),
                        filename=f"ticket-{ticket['id']}-transcript.html"
                    )
                    try:
                        await user.send(embed=dm_embed, file=transcript_file)
                        logger.info("Sent transcript DM to user %s for closed ticket %s", user_id, ticket["id"])
                    except discord.HTTPException as e:
                        logger.warning("Failed to DM user %s with transcript: %s", user_id, e)
                        await user.send(embed=dm_embed)
                else:
                    await user.send(embed=dm_embed)
                    logger.info("Sent closure DM to user %s for closed ticket %s", user_id, ticket["id"])

            except (discord.Forbidden, discord.NotFound):
                logger.warning("Could not DM user %s about ticket closure", user_id)
            except discord.HTTPException as e:
                logger.error("Error sending DM to user %s: %s", user_id, e)

            try:
                await channel.delete(reason=f"Ticket closed (ID: {ticket['id']})")
                logger.info("Deleted ticket channel %s", channel.id)
            except discord.HTTPException as e:
                logger.error("Failed to delete channel %s: %s", channel.id, e)

            await interaction.followup.send(
                f"✅ Ticket #{ticket['id']} has been closed and archived.",
                ephemeral=True
            )

        except Exception as e:
            await interaction.followup.send(
                "An error occurred while closing the ticket. Please try again.",
                ephemeral=True
            )
            logger.error("Error closing ticket %s: %s", ticket["id"], e, exc_info=True)

    @ticket_group.command(name="transcript", description="Retrieve a transcript for a closed ticket (admin only)")
    @app_commands.describe(
        ticket_id="The ticket ID to retrieve the transcript for",
    )
    async def ticket_transcript(
        self,
        interaction: discord.Interaction,
        ticket_id: int,
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
            transcript_record = await self.bot.db.get_transcript_by_ticket_id(ticket_id)
            
            if not transcript_record:
                await interaction.followup.send(
                    f"No transcript found for ticket #{ticket_id}. The ticket may not have been closed yet, or the transcript may not have been saved.",
                    ephemeral=True
                )
                return

            storage_type = transcript_record["storage_type"]
            storage_path = transcript_record["storage_path"]
            
            transcript_bytes = await self.bot.storage.retrieve_transcript(storage_path, storage_type)
            
            if not transcript_bytes:
                await interaction.followup.send(
                    f"Transcript file not found for ticket #{ticket_id}. The file may have been deleted or moved.",
                    ephemeral=True
                )
                return

            ticket = await self.bot.db.get_ticket(ticket_id)
            if ticket:
                filename = f"ticket-{ticket_id}-transcript.html"
            else:
                filename = f"ticket-{ticket_id}-transcript.html"

            transcript_file = discord.File(
                BytesIO(transcript_bytes),
                filename=filename
            )

            file_size_kb = len(transcript_bytes) / 1024
            created_at = self._parse_timestamp(transcript_record["created_at"])

            embed = create_embed(
                title=f"Ticket #{ticket_id} Transcript",
                description=(
                    f"**Storage Type:** {storage_type}\n"
                    f"**File Size:** {file_size_kb:.2f} KB\n"
                    f"**Created:** {discord_timestamp(created_at, 'f')}\n"
                    f"**User:** <@{transcript_record['user_discord_id']}>"
                ),
                color=discord.Color.blue(),
                timestamp=True,
            )

            await interaction.followup.send(
                embed=embed,
                file=transcript_file,
                ephemeral=True
            )
            logger.info("Retrieved transcript for ticket %s (requested by %s)", ticket_id, interaction.user.id)

        except Exception as e:
            await interaction.followup.send(
                "An error occurred while retrieving the transcript. Please try again.",
                ephemeral=True
            )
            logger.error("Error retrieving transcript for ticket %s: %s", ticket_id, e, exc_info=True)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        
        if not isinstance(message.channel, discord.TextChannel):
            return
        
        ticket = await self.bot.db.get_ticket_by_channel(message.channel.id)
        if ticket and ticket["status"] == "open":
            await self.bot.db.touch_ticket_activity(message.channel.id)
            self.warned_tickets.discard(message.channel.id)


async def setup(bot: ApexCoreBot) -> None:
    if not CHAT_EXPORTER_AVAILABLE:
        logger.warning(
            "chat_exporter library not found. Transcript generation will be disabled. "
            "Install with: pip install chat-exporter"
        )
    cog = TicketManagementCog(bot)
    bot.tree.add_command(cog.ticket_group)
    await bot.add_cog(cog)
