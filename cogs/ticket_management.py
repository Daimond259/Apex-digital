"""Ticket lifecycle automation with transcript export."""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import TYPE_CHECKING

import discord
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
            transcript_file = None
            transcript_html = None
            
            if CHAT_EXPORTER_AVAILABLE:
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
            else:
                logger.warning("chat_exporter not available, skipping transcript generation")
            
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
            self.warned_tickets.discard(channel.id)
            
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
                
                if transcript_file and CHAT_EXPORTER_AVAILABLE:
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
            
            log_channel_id = self.bot.config.logging_channels.tickets
            log_channel = channel.guild.get_channel(log_channel_id)
            if not isinstance(log_channel, discord.TextChannel):
                global_channel = self.bot.get_channel(log_channel_id)
                log_channel = global_channel if isinstance(global_channel, discord.TextChannel) else None
            
            if isinstance(log_channel, discord.TextChannel):
                created_at = self._parse_timestamp(ticket["created_at"])
                last_activity = self._parse_timestamp(ticket["last_activity"])
                
                log_embed = create_embed(
                    title="Ticket Auto-Closed",
                    description=(
                        f"**Ticket ID:** #{ticket['id']}\n"
                        f"**Channel:** {channel.mention} ({channel.id})\n"
                        f"**User:** <@{user_discord_id}> ({user_discord_id})\n"
                        f"**Reason:** Inactivity ({INACTIVITY_CLOSE_HOURS}h)\n"
                        f"**Created:** {discord_timestamp(created_at, 'f')}\n"
                        f"**Last Activity:** {discord_timestamp(last_activity, 'f')}"
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
                            f"**Closed:** Auto-closed due to inactivity"
                        ),
                        color=discord.Color.blue(),
                        timestamp=True,
                    )
                    try:
                        await archive_channel.send(embed=archive_embed, file=archive_file)
                        logger.info("Archived transcript to channel %s", archive_channel_id)
                    except discord.HTTPException as e:
                        logger.error("Failed to archive transcript to channel %s: %s", archive_channel_id, e)
            
            try:
                await channel.delete(reason=f"Ticket auto-closed due to inactivity (ID: {ticket['id']})")
                logger.info("Deleted ticket channel %s", channel.id)
            except discord.HTTPException as e:
                logger.error("Failed to delete channel %s: %s", channel.id, e)
                
        except Exception as e:
            logger.error("Error closing ticket %s: %s", ticket["id"], e, exc_info=True)

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
    await bot.add_cog(TicketManagementCog(bot))
