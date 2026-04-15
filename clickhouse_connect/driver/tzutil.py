import os
from datetime import datetime, tzinfo

import pytz

tzlocal = None
try:
    import tzlocal  # Maybe we can use the tzlocal module to get a safe timezone
except ImportError:
    pass

# Set the local timezone for DateTime conversions.  Note in most cases we want to use either UTC or the server
# timezone, but if someone insists on using the local timezone we will try to convert.  The problem is we
# never have anything but an epoch timestamp returned from ClickHouse, so attempts to convert times when the
# local timezone is "DST" aware (like 'CEST' vs 'CET') will be wrong approximately half the time
local_tz: pytz.timezone
local_tz_dst_safe: bool = False

# Timezone names that are equivalent to UTC
UTC_EQUIVALENTS = ("UTC", "Etc/UTC", "GMT", "Universal", "GMT-0", "Zulu", "Greenwich", "UCT")


def normalize_timezone(timezone: pytz.timezone) -> tuple[pytz.timezone, bool]:
    if timezone.tzname(None) in UTC_EQUIVALENTS:
        return pytz.UTC, True

    if timezone.tzname(None) in pytz.common_timezones:
        return timezone, True

    if tzlocal is not None:  # Maybe we can use the tzlocal module to get a safe timezone
        local_name = tzlocal.get_localzone_name()
        if local_name in pytz.common_timezones:
            return pytz.timezone(local_name), True

    return timezone, False


def is_utc_timezone(tz: tzinfo | str | None) -> bool:
    """Check if timezone is UTC or an equivalent (Etc/UTC, GMT, etc.).

    This handles the issue where pytz.timezone('Etc/UTC') != pytz.UTC despite
    being semantically equivalent. Also accepts timezone name strings.
    """
    if tz is None:
        return False
    if isinstance(tz, str):
        return tz in UTC_EQUIVALENTS
    if tz == pytz.UTC:
        return True
    return tz.tzname(None) in UTC_EQUIVALENTS


def utc_equivalent_tzaware_datetime(ts: float, microseconds: int, tz_info: tzinfo) -> datetime:
    """Build a UTC-equivalent timezone-aware datetime using arithmetic.

    For UTC-equivalent timezones (UTC, Etc/UTC, GMT, etc.), construct the datetime
    using epoch arithmetic rather than datetime.fromtimestamp(), then attach the
    timezone. This avoids timezone conversion machinery that's unnecessary for UTC.

    Args:
        ts: Unix timestamp (seconds since epoch)
        microseconds: Microsecond component
        tz_info: A UTC-equivalent timezone object

    Returns:
        Timezone-aware datetime in the specified timezone
    """
    if not isinstance(ts, float):
        ts = float(ts)

    seconds = int(ts)

    if seconds >= 0:
        days = seconds // 86400
        secs_in_day = seconds % 86400
    else:
        days = (seconds + 1) // 86400 - 1
        secs_in_day = seconds - days * 86400

    year, month, day = _epoch_days_to_date_components(days)

    hour = secs_in_day // 3600
    secs_in_day %= 3600
    minute = secs_in_day // 60
    second = secs_in_day % 60

    return datetime(year, month, day, hour, minute, second, microseconds, tzinfo=tz_info)


def utcfromtimestamp_with_microseconds(ts: float, microseconds: int = 0) -> datetime:
    """Convert Unix timestamp to naive UTC datetime with explicit microseconds.

    This is more efficient than calling utcfromtimestamp() and then .replace(microsecond=...)
    because it constructs the datetime once with all components.

    Args:
        ts: Unix timestamp (seconds since epoch)
        microseconds: Microsecond component (0-999999)

    Returns:
        Naive UTC datetime with specified microseconds
    """
    if not isinstance(ts, float):
        ts = float(ts)

    seconds = int(ts)

    if seconds >= 0:
        days = seconds // 86400
        secs_in_day = seconds % 86400
    else:
        days = (seconds + 1) // 86400 - 1
        secs_in_day = seconds - days * 86400

    year, month, day = _epoch_days_to_date_components(days)

    hour = secs_in_day // 3600
    secs_in_day %= 3600
    minute = secs_in_day // 60
    second = secs_in_day % 60

    return datetime(year, month, day, hour, minute, second, microseconds)


def utcfromtimestamp(ts: float) -> datetime:
    """Convert Unix timestamp to naive UTC datetime using arithmetic, avoiding
    the expensive datetime.fromtimestamp() + replace() round-trip."""
    if not isinstance(ts, float):
        ts = float(ts)

    seconds = int(ts)

    if seconds >= 0:
        days = seconds // 86400
        secs_in_day = seconds % 86400
    else:
        days = (seconds + 1) // 86400 - 1
        secs_in_day = seconds - days * 86400

    year, month, day = _epoch_days_to_date_components(days)

    hour = secs_in_day // 3600
    secs_in_day %= 3600
    minute = secs_in_day // 60
    second = secs_in_day % 60

    return datetime(year, month, day, hour, minute, second, 0)


def _epoch_days_to_date_components(days: int) -> tuple[int, int, int]:
    """Convert days since epoch to (year, month, day).

    This is a pure Python implementation of the same algorithm as
    the Cython epoch_days_to_date, but returns components instead of a date object.
    """
    # Month days arrays (non-leap and leap year)
    month_days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]
    month_days_leap = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366]

    if 0 <= days < 47482:
        cycles = (days + 365) // 1461
        rem = (days + 365) - cycles * 1461
        years = rem // 365
        rem -= years * 365
        year = (cycles << 2) + years + 1969
        if years == 4:
            return year - 1, 12, 31
        if years == 3:
            m_list = month_days_leap
        else:
            m_list = month_days
    else:
        cycles400 = (days + 134774) // 146097
        rem = days + 134774 - (cycles400 * 146097)
        cycles100 = rem // 36524
        rem -= cycles100 * 36524
        cycles = rem // 1461
        rem -= cycles * 1461
        years = rem // 365
        rem -= years * 365
        year = (cycles << 2) + cycles400 * 400 + cycles100 * 100 + years + 1601
        if years == 4 or cycles100 == 4:
            return year - 1, 12, 31
        if years == 3 and (year == 2000 or year % 100 != 0):
            m_list = month_days_leap
        else:
            m_list = month_days

    month = (rem + 24) >> 5
    prev = m_list[month]
    while rem < prev:
        month -= 1
        prev = m_list[month]

    return year, month + 1, rem + 1 - prev


try:
    local_tz = pytz.timezone(os.environ.get("TZ", ""))
except pytz.UnknownTimeZoneError:
    local_tz = datetime.now().astimezone().tzinfo

local_tz, local_tz_dst_safe = normalize_timezone(local_tz)
