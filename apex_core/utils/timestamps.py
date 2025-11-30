"""Discord timestamp helpers."""

from datetime import datetime, timedelta, timezone

from ..config import OperatingHours


def discord_timestamp(dt: datetime, style: str = "f") -> str:
    """Return Discord formatted timestamp."""
    if dt.tzinfo is None:
        aware = dt.replace(tzinfo=timezone.utc)
    else:
        aware = dt.astimezone(timezone.utc)
    timestamp = int(aware.timestamp())
    return f"<t:{timestamp}:{style}>"


def _next_occurrence(hour: int, reference: datetime | None = None) -> datetime:
    if reference is None:
        reference = datetime.now(timezone.utc)
    next_dt = reference.replace(hour=hour, minute=0, second=0, microsecond=0)
    if next_dt <= reference:
        next_dt += timedelta(days=1)
    return next_dt


def operating_hours_window(
    operating_hours: OperatingHours, *, style: str = "t"
) -> tuple[str, str]:
    """Return Discord timestamps for operating hours."""
    now = datetime.now(timezone.utc)
    start_dt = _next_occurrence(operating_hours.start_hour_utc, now)
    end_dt = _next_occurrence(operating_hours.end_hour_utc, start_dt)
    return discord_timestamp(start_dt, style=style), discord_timestamp(end_dt, style=style)


def render_operating_hours(operating_hours: OperatingHours, *, style: str = "t") -> str:
    """Return a human-readable operating hours string using Discord timestamps."""
    start, end = operating_hours_window(operating_hours, style=style)
    return f"{start} — {end} UTC"
