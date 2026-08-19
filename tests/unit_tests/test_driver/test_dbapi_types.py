from datetime import date, datetime, time

import pytest

import clickhouse_connect.dbapi as dbapi


def test_binary_constructor():
    assert dbapi.Binary(b"\x00\xff") == b"\x00\xff"
    assert dbapi.Binary([0, 255]) == b"\x00\xff"


def test_date_time_timestamp_constructors():
    assert dbapi.Date(2026, 8, 5) == date(2026, 8, 5)
    assert dbapi.Time(13, 14, 15) == time(13, 14, 15)
    assert dbapi.Timestamp(2026, 8, 5, 13, 14, 15) == datetime(2026, 8, 5, 13, 14, 15)


@pytest.mark.parametrize("ticks", [0, 1_799_821_913.792341])
def test_tick_constructors_use_local_time(ticks):
    local_datetime = datetime.fromtimestamp(ticks)
    assert dbapi.DateFromTicks(ticks) == local_datetime.date()
    assert dbapi.TimeFromTicks(ticks) == local_datetime.time().replace(microsecond=0)
    assert dbapi.TimestampFromTicks(ticks) == local_datetime.replace(microsecond=0)


def test_sqlalchemy_dialect_import_dbapi_exposes_binary():
    pytest.importorskip("sqlalchemy")

    from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect

    assert ClickHouseDialect.import_dbapi().Binary(b"x") == b"x"
