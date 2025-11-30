"""Embed factory utilities."""

from typing import Optional

import discord


def create_embed(
    title: Optional[str] = None,
    description: Optional[str] = None,
    color: discord.Color = discord.Color.blue(),
    footer: Optional[str] = None,
    timestamp: bool = False
) -> discord.Embed:
    """
    Create a standardized Discord embed.
    
    Args:
        title: Embed title
        description: Embed description
        color: Embed color (default: blue)
        footer: Footer text
        timestamp: Whether to add current timestamp
    
    Returns:
        Configured Discord Embed
    """
    embed = discord.Embed(
        title=title,
        description=description,
        color=color
    )
    
    if footer:
        embed.set_footer(text=footer)
    
    if timestamp:
        embed.timestamp = discord.utils.utcnow()
    
    return embed
