"""VIP tier calculation utilities."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import Config, Role


def calculate_vip_tier(total_spent_cents: int, config: Config) -> Role | None:
    """
    Calculate highest VIP tier (automatic_spend) based on lifetime spending.
    
    Args:
        total_spent_cents: Total amount spent in cents
        config: Bot configuration with roles
    
    Returns:
        Highest applicable automatic_spend role or None if user doesn't qualify
    """
    qualified_role: Role | None = None
    
    for role in config.roles:
        if role.assignment_mode != "automatic_spend":
            continue
        
        if isinstance(role.unlock_condition, int):
            if total_spent_cents >= role.unlock_condition:
                if qualified_role is None or role.tier_priority < qualified_role.tier_priority:
                    qualified_role = role
    
    return qualified_role
