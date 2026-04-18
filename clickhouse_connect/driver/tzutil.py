import os
from datetime import datetime, tzinfo

try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo


tzlocal = None
try:
    import tzlocal  # Maybe we can use the tzlocal module to get a safe timezone
except ImportError:
    pass

# Set the local timezone for DateTime conversions.  Note in most cases we want to use either UTC or the server
# timezone, but if someone insists on using the local timezone we will try to convert.  The problem is we
# never have anything but an epoch timestamp returned from ClickHouse, so attempts to convert times when the
# local timezone is "DST" aware (like 'CEST' vs 'CET') will be wrong approximately half the time
local_tz: zoneinfo.ZoneInfo
local_tz_dst_safe: bool = False

# Timezone names that are equivalent to UTC
UTC_EQUIVALENTS = ("UTC", "Etc/UTC", "GMT", "Universal", "GMT-0", "Zulu", "Greenwich", "UCT")


def normalize_timezone(timezone: zoneinfo.ZoneInfo) -> tuple[zoneinfo.ZoneInfo, bool]:
    if timezone.tzname(None) in UTC_EQUIVALENTS:
        return zoneinfo.ZoneInfo("UTC"), True

    if timezone.tzname(None) in zoneinfo.available_timezones():
        return timezone, True

    if tzlocal is not None:  # Maybe we can use the tzlocal module to get a safe timezone
        local_name = tzlocal.get_localzone_name()
        if local_name in zoneinfo.available_timezones():
            return zoneinfo.ZoneInfo(local_name), True

    return timezone, False


def is_utc_timezone(tz: tzinfo | str | None) -> bool:
    """Check if timezone is UTC or an equivalent (Etc/UTC, GMT, etc.).

    This handles the issue where zoneinfo.ZoneInfo('Etc/UTC') != zoneinfo.ZoneInfo("UTC") despite
    being semantically equivalent. Also accepts timezone name strings.
    """
    if tz is None:
        return False
    if isinstance(tz, str):
        return tz in UTC_EQUIVALENTS
    if tz == zoneinfo.ZoneInfo("UTC"):
        return True
    return tz.tzname(None) in UTC_EQUIVALENTS


def utcfromtimestamp(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=zoneinfo.ZoneInfo("UTC")).replace(tzinfo=None)


def local_tz() -> tzinfo:
    if os.getenv("TZ"):
        try:
            return zoneinfo.ZoneInfo(os.environ.get("TZ", ""))
        except zoneinfo.ZoneInfoNotFoundError:
            pass
    return datetime.now().astimezone().tzinfo


local_tz, local_tz_dst_safe = normalize_timezone(local_tz())
