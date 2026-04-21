import os
import zoneinfo
from datetime import datetime, timezone, tzinfo

tzlocal = None
try:
    import tzlocal  # Maybe we can use the tzlocal module to get a safe timezone
except ImportError:
    pass

# Set the local timezone for DateTime conversions.  Note in most cases we want to use either UTC or the server
# timezone, but if someone insists on using the local timezone we will try to convert.  The problem is we
# never have anything but an epoch timestamp returned from ClickHouse, so attempts to convert times when the
# local timezone is "DST" aware (like 'CEST' vs 'CET') will be wrong approximately half the time
local_tz: tzinfo
local_tz_dst_safe: bool = False

# Timezone names that are equivalent to UTC
UTC_EQUIVALENTS = ("UTC", "Etc/UTC", "GMT", "Universal", "GMT-0", "Zulu", "Greenwich", "UCT")

# Appended to error/warning messages when a named IANA zone cannot be resolved. On systems without
# a system zoneinfo database (slim containers, Windows without tzdata), users can install the tzdata
# extra to get the IANA zone data.
TZDATA_HINT = "install the tzdata package (e.g. `pip install clickhouse-connect[tzdata]`) if no system zoneinfo database is available"


def normalize_timezone(tz: tzinfo) -> tuple[tzinfo, bool]:
    # ZoneInfo exposes the IANA key on `.key`; fall back to tzname(None) for other tzinfo
    # subclasses (datetime.timezone, fixed offsets). pytz used to return the IANA name from
    # tzname(None), but ZoneInfo returns None, which would collapse every named zone into the
    # "unsafe" fallback branch.
    tz_key = getattr(tz, "key", None) or tz.tzname(None)

    if tz_key in UTC_EQUIVALENTS:
        return timezone.utc, True

    if tz_key in zoneinfo.available_timezones():
        return tz, True

    if tzlocal is not None:  # Maybe we can use the tzlocal module to get a safe timezone
        local_name = tzlocal.get_localzone_name()
        if local_name in zoneinfo.available_timezones():
            return zoneinfo.ZoneInfo(local_name), True

    return tz, False


def is_utc_timezone(tz: tzinfo | str | None) -> bool:
    """Check if timezone is UTC or an equivalent (Etc/UTC, GMT, etc.).

    This handles the issue where zoneinfo.ZoneInfo('Etc/UTC') != zoneinfo.ZoneInfo("UTC") despite
    being semantically equivalent. Also accepts timezone name strings.
    """
    if tz is None:
        return False
    if isinstance(tz, str):
        return tz in UTC_EQUIVALENTS
    if tz is timezone.utc:
        return True
    return tz.tzname(None) in UTC_EQUIVALENTS


def utcfromtimestamp(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)


def _detect_local_tz() -> tzinfo:
    env_tz = os.environ.get("TZ")
    if env_tz:
        try:
            return zoneinfo.ZoneInfo(env_tz)
        except zoneinfo.ZoneInfoNotFoundError:
            pass
    return datetime.now().astimezone().tzinfo


local_tz, local_tz_dst_safe = normalize_timezone(_detect_local_tz())
