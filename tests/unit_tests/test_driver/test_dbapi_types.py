"""PEP 249 type constructors on clickhouse_connect.dbapi (#919)."""

from datetime import date, datetime, time

from clickhouse_connect import dbapi


def test_binary_constructor_is_bytes():
    """SQLAlchemy LargeBinary bind processor calls dialect.dbapi.Binary(value)."""
    assert dbapi.Binary is bytes
    payload = b"\x00\xff"
    assert dbapi.Binary(payload) == payload
    assert dbapi.Binary([0, 255]) == payload


def test_date_time_timestamp_constructors():
    assert dbapi.Date(2023, 6, 1) == date(2023, 6, 1)
    assert dbapi.Time(14, 30, 0) == time(14, 30, 0)
    assert dbapi.Timestamp(2023, 6, 1, 14, 30, 0) == datetime(2023, 6, 1, 14, 30, 0)


def test_from_ticks_constructors():
    # 2020-01-01 00:00:00 UTC epoch-ish; use a stable local-time-independent approach
    # DateFromTicks uses localtime, so only check types and rough ordering.
    d = dbapi.DateFromTicks(0)
    t = dbapi.TimeFromTicks(0)
    ts = dbapi.TimestampFromTicks(0)
    assert isinstance(d, date)
    assert isinstance(t, time)
    assert isinstance(ts, datetime)
