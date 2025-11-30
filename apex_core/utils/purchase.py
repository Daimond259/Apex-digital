"""Post-purchase processing utilities."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import Config, Role

logger = logging.getLogger(__name__)


async def process_post_purchase(
    user_discord_id: int,
    amount_cents: int,
    db,
    config: Config,
    guild=None,
) -> tuple[Role | None, Role | None]:
    """
    Process post-purchase actions including role updates.
    
    Args:
        user_discord_id: Discord user ID
        amount_cents: Purchase amount in cents
        db: Database instance
        config: Bot configuration
        guild: Discord guild (optional, for role assignments)
    
    Returns:
        Tuple of (old_vip_tier, new_vip_tier)
    """
    from .vip import calculate_vip_tier
    
    # Get current user data
    user_row = await db.get_user(user_discord_id)
    if not user_row:
        logger.warning("User %s not found for post-purchase processing", user_discord_id)
        return None, None
    
    # Calculate current and new VIP tiers (highest automatic_spend role)
    old_vip_tier = calculate_vip_tier(user_row["total_lifetime_spent_cents"], config)
    new_lifetime_spend = user_row["total_lifetime_spent_cents"] + amount_cents
    new_vip_tier = calculate_vip_tier(new_lifetime_spend, config)
    
    return old_vip_tier, new_vip_tier


async def handle_vip_promotion(
    user_discord_id: int,
    old_vip_tier: Role | None,
    new_vip_tier: Role | None,
    config: Config,
    guild=None,
) -> None:
    """
    Handle role promotion notifications and role assignments.
    
    Args:
        user_discord_id: Discord user ID
        old_vip_tier: Previous role (can be None)
        new_vip_tier: New role (can be None)
        config: Bot configuration
        guild: Discord guild for role assignments
    """
    if not new_vip_tier or (old_vip_tier and new_vip_tier.name == old_vip_tier.name):
        return  # No promotion occurred
    
    try:
        import discord
        from .embeds import create_embed
        from .timestamps import discord_timestamp
        
        user = None
        if guild:
            user = guild.get_member(user_discord_id)
        
        # Assign role
        if guild and user:
            role = guild.get_role(new_vip_tier.role_id)
            if role:
                await user.add_roles(role, reason=f"Role promotion to {new_vip_tier.name}")
                logger.info("Assigned role %s to user %s", role.id, user_discord_id)
            else:
                logger.warning("Role %s not found in guild", new_vip_tier.role_id)
        
        # Send congratulatory DM
        if user:
            from datetime import datetime, timezone
            
            benefits_text = "\n".join(f"• {b}" for b in new_vip_tier.benefits) if new_vip_tier.benefits else "No specific benefits listed."
            
            embed = create_embed(
                title=f"🎉 {new_vip_tier.name} Promotion!",
                description=(
                    f"Congratulations! You've been promoted to **{new_vip_tier.name}** status!\n\n"
                    f"**Benefits:**\n{benefits_text}\n\n"
                    f"**Discount Benefit:** {new_vip_tier.discount_percent:.2f}% off purchases\n"
                    f"**Promotion Date:** {discord_timestamp(datetime.now(timezone.utc), 'R')}"
                ),
                color=discord.Color.gold(),
            )
            
            try:
                await user.send(embed=embed)
                logger.info("Sent promotion DM to user %s", user_discord_id)
            except discord.Forbidden:
                logger.warning("Could not DM user %s about promotion", user_discord_id)
        
        # Log to staff channel
        if guild:
            log_channel_id = config.logging_channels.audit
            log_channel = guild.get_channel(log_channel_id)
            if isinstance(log_channel, discord.TextChannel):
                log_embed = create_embed(
                    title="Role Promotion",
                    description=(
                        f"**User:** {user.mention if user else f'<@{user_discord_id}>'} ({user_discord_id})\n"
                        f"**Previous Role:** {old_vip_tier.name if old_vip_tier else 'None'}\n"
                        f"**New Role:** {new_vip_tier.name}\n"
                        f"**Discount:** {new_vip_tier.discount_percent:.2f}%"
                    ),
                    color=discord.Color.gold(),
                )
                await log_channel.send(embed=log_embed)
                logger.info("Logged role promotion for user %s", user_discord_id)
    
    except Exception as e:
        logger.error("Error handling role promotion for user %s: %s", user_discord_id, e)