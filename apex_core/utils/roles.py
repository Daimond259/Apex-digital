"""Role assignment and management utilities."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import Config, Role
    from ..database import Database
    import discord

logger = logging.getLogger(__name__)


def get_role_by_name(config: Config, role_name: str) -> Role | None:
    """Get a role configuration by name."""
    for role in config.roles:
        if role.name == role_name:
            return role
    return None


def get_role_by_id(config: Config, role_id: int) -> Role | None:
    """Get a role configuration by role ID."""
    for role in config.roles:
        if role.role_id == role_id:
            return role
    return None


async def get_user_roles(user_id: int, db: Database, config: Config) -> list[Role]:
    """Get all roles a user should have based on spending and assignments."""
    user = await db.get_user(user_id)
    if not user:
        return []

    applicable_roles: list[Role] = []
    total_spent = user["total_lifetime_spent_cents"]
    has_client_role = bool(user["has_client_role"])
    manually_assigned = await db.get_manually_assigned_roles(user_id)

    # First pass: get all non-zenith roles
    for role in config.roles:
        if role.assignment_mode == "automatic_all_ranks":
            continue

        # Check automatic_spend roles
        if role.assignment_mode == "automatic_spend":
            if isinstance(role.unlock_condition, int):
                if total_spent >= role.unlock_condition:
                    applicable_roles.append(role)

        # Check automatic_first_purchase role
        elif role.assignment_mode == "automatic_first_purchase":
            if has_client_role or total_spent > 0:
                applicable_roles.append(role)

        # Check manual roles
        elif role.assignment_mode == "manual":
            if role.name in manually_assigned:
                applicable_roles.append(role)

    # Second pass: check automatic_all_ranks (Zenith) - must have all other roles
    for role in config.roles:
        if role.assignment_mode == "automatic_all_ranks":
            non_zenith_roles = [r for r in config.roles if r.assignment_mode != "automatic_all_ranks"]
            user_role_names = {r.name for r in applicable_roles}

            has_all = all(
                r.name in user_role_names or r.name in manually_assigned
                for r in non_zenith_roles
            )

            if has_all:
                applicable_roles.append(role)

    return applicable_roles


async def update_user_roles(
    user: discord.Member,
    applicable_roles: list[Role],
    config: Config,
) -> tuple[list[Role], list[Role]]:
    """
    Update user roles on Discord.
    
    Returns:
        Tuple of (roles_added, roles_removed)
    """
    # Get current roles
    current_role_ids = {role.id for role in user.roles}
    target_role_ids = {role.role_id for role in applicable_roles}

    # Determine what to add and remove
    role_ids_to_add = target_role_ids - current_role_ids
    role_ids_to_remove = current_role_ids & {role.role_id for role in config.roles}
    role_ids_to_remove -= target_role_ids

    roles_added: list[Role] = []
    roles_removed: list[Role] = []

    # Add roles
    for role_id in role_ids_to_add:
        role_config = get_role_by_id(config, role_id)
        if role_config:
            discord_role = user.guild.get_role(role_id)
            if discord_role:
                try:
                    await user.add_roles(discord_role, reason=f"Role assignment: {role_config.name}")
                    roles_added.append(role_config)
                    logger.info(
                        "Added role %s (%s) to user %s",
                        role_config.name,
                        role_id,
                        user.id,
                    )
                except Exception as e:
                    logger.error(
                        "Failed to add role %s to user %s: %s",
                        role_id,
                        user.id,
                        e,
                    )

    # Remove roles
    for role_id in role_ids_to_remove:
        role_config = get_role_by_id(config, role_id)
        if role_config:
            discord_role = user.guild.get_role(role_id)
            if discord_role:
                try:
                    await user.remove_roles(discord_role, reason=f"Role revocation: {role_config.name}")
                    roles_removed.append(role_config)
                    logger.info(
                        "Removed role %s (%s) from user %s",
                        role_config.name,
                        role_id,
                        user.id,
                    )
                except Exception as e:
                    logger.error(
                        "Failed to remove role %s from user %s: %s",
                        role_id,
                        user.id,
                        e,
                    )

    return roles_added, roles_removed


async def check_and_update_roles(
    user_id: int,
    db: Database,
    guild: discord.Guild,
    config: Config,
) -> tuple[list[Role], list[Role]]:
    """
    Check all role conditions and update user roles accordingly.
    
    Returns:
        Tuple of (roles_added, roles_removed)
    """
    user = guild.get_member(user_id)
    if not user:
        logger.warning("User %s not found in guild %s", user_id, guild.id)
        return [], []

    # Get applicable roles based on spending/assignments
    applicable_roles = await get_user_roles(user_id, db, config)

    # Update Discord roles
    roles_added, roles_removed = await update_user_roles(user, applicable_roles, config)

    # Update database for first purchase
    user_row = await db.get_user(user_id)
    if user_row and not user_row["has_client_role"]:
        client_role = get_role_by_name(config, "Client")
        if client_role and client_role in applicable_roles:
            await db.mark_client_role_assigned(user_id)

    return roles_added, roles_removed
