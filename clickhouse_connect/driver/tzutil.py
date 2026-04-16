import os
import re
from datetime import datetime, timedelta, timezone, tzinfo

import pytz

# Matches ClickHouse Fixed timezone strings like Fixed/UTC+05:30:00 or Fixed/UTC-03:00:00.
# Hours are 0-23, minutes and seconds are 00-59.
_FIXED_TZ_RE = re.compile(r"^Fixed/UTC([+-])([01]?\d|2[0-3]):([0-5]\d):([0-5]\d)$")

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


def utcfromtimestamp(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=pytz.UTC).replace(tzinfo=None)


def parse_timezone(tz_str: str) -> tzinfo:
    """Parse a ClickHouse timezone string into a tzinfo object.

    Handles standard pytz timezone names as well as ClickHouse Fixed offset
    timezones of the form ``Fixed/UTC±HH:MM:SS`` (e.g. ``Fixed/UTC+05:30:00``),
    which pytz does not recognise natively.
    """
    match = _FIXED_TZ_RE.match(tz_str)
    if match:
        sign, hours, minutes, seconds = match.groups()
        offset = timedelta(hours=int(hours), minutes=int(minutes), seconds=int(seconds))
        if sign == "-":
            offset = -offset
        return timezone(offset)
    return pytz.timezone(tz_str)


try:
    local_tz = pytz.timezone(os.environ.get("TZ", ""))
except pytz.UnknownTimeZoneError:
    local_tz = datetime.now().astimezone().tzinfo

local_tz, local_tz_dst_safe = normalize_timezone(local_tz)
