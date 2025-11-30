from datetime import datetime, timedelta, timezone

import pytest
from cogs.ticket_management import TicketManagementCog


@pytest.fixture
def ticket_management_cog():
    return object.__new__(TicketManagementCog)


def test_parse_timestamp_with_datetime_object_aware(ticket_management_cog):
    dt = datetime(2024, 5, 1, 12, 30, 15, tzinfo=timezone.utc)
    result = ticket_management_cog._parse_timestamp(dt)
    assert result == dt
    assert result.tzinfo == timezone.utc


def test_parse_timestamp_with_datetime_object_naive(ticket_management_cog):
    dt = datetime(2024, 5, 1, 12, 30, 15)
    result = ticket_management_cog._parse_timestamp(dt)
    assert result.year == 2024
    assert result.month == 5
    assert result.day == 1
    assert result.hour == 12
    assert result.minute == 30
    assert result.second == 15
    assert result.tzinfo == timezone.utc


def test_parse_timestamp_with_iso_format_string(ticket_management_cog):
    result = ticket_management_cog._parse_timestamp("2024-05-01T12:30:15+02:00")
    assert result.tzinfo == timezone.utc
    assert result.hour == 10
    assert result.minute == 30


def test_parse_timestamp_with_space_separated_string(ticket_management_cog):
    result = ticket_management_cog._parse_timestamp("2024-05-01 12:30:15")
    assert result.tzinfo == timezone.utc
    assert result.hour == 12
    assert result.minute == 30


def test_parse_timestamp_with_timezone_aware_string(ticket_management_cog):
    result = ticket_management_cog._parse_timestamp("2024-05-01T12:30:15+00:00")
    assert result.tzinfo == timezone.utc
    assert result.hour == 12
    assert result.minute == 30


def test_parse_timestamp_with_invalid_type_raises(ticket_management_cog):
    with pytest.raises(ValueError, match="Unsupported timestamp type"):
        ticket_management_cog._parse_timestamp(12345)


def test_parse_timestamp_preserves_microseconds(ticket_management_cog):
    result = ticket_management_cog._parse_timestamp("2024-05-01T12:30:15.123456")
    assert result.microsecond == 123456
    assert result.tzinfo == timezone.utc


def test_parse_timestamp_normalizes_to_utc(ticket_management_cog):
    offset_dt = datetime(2024, 5, 1, 5, 30, 15, tzinfo=timezone(timedelta(hours=-5)))
    result = ticket_management_cog._parse_timestamp(offset_dt)
    assert result.tzinfo == timezone.utc
    assert result.hour == 10
    assert result.minute == 30
