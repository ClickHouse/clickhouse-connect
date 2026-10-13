from datetime import datetime, timedelta, timezone

import pytest
import sqlalchemy as sa

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Array, DateTime64, Nullable, Tuple, UInt32

STAMP = datetime(2024, 6, 13, 7, 8, 9, 123456, tzinfo=timezone.utc)
NAIVE_STAMP = STAMP.replace(tzinfo=None)


class Wrapper(sa.TypeDecorator):
    impl = sa.String
    cache_ok = True

    def __init__(self, child):
        super().__init__()
        self.impl = child


def wrapped(child):
    return Wrapper(Wrapper(Wrapper(child)))


@pytest.fixture
def decorator_engine(client_mode, call, test_engine):
    if client_mode == "async":
        pytest.importorskip("sqlalchemy", minversion="2.0.44")
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine(test_engine.url.set(drivername="clickhousedb+async"))
    else:
        engine = sa.create_engine(test_engine.url)
    yield engine
    call(engine.dispose)


@pytest.mark.parametrize(
    ("type_", "value", "expected"),
    [
        (DateTime64(6, "UTC"), STAMP, NAIVE_STAMP),
        (Array(DateTime64(6, "UTC")), [STAMP], [NAIVE_STAMP]),
        (Tuple(UInt32, DateTime64(6, "UTC")), (13, STAMP), (13, NAIVE_STAMP)),
        (Array(Tuple(UInt32, DateTime64(6, "UTC"))), [(13, STAMP)], [(13, NAIVE_STAMP)]),
        (Nullable(DateTime64(6, "UTC")), STAMP, NAIVE_STAMP),
    ],
    ids=["scalar", "array", "tuple", "array-tuple", "nullable"],
)
def test_repeated_type_decorators_preserve_datetime64(decorator_engine, call, type_, value, expected):
    statement = sa.select(sa.cast(sa.bindparam("stamp", value, wrapped(type_)), type_))
    connection = call(decorator_engine.connect)
    try:
        assert call(connection.execute, statement).scalar_one() == expected
    finally:
        call(connection.close)


def test_repeated_type_decorators_preserve_expanding_predicate(decorator_engine, call):
    column = sa.cast(sa.literal(STAMP, DateTime64(6, "UTC")), DateTime64(6, "UTC"))
    statement = sa.select(column.in_(sa.bindparam("stamps", expanding=True, type_=wrapped(DateTime64(6, "UTC")))))
    connection = call(decorator_engine.connect)
    try:
        for values, expected in [
            ([STAMP], 1),
            ([STAMP + timedelta(seconds=1)], 0),
            ([], 0),
        ]:
            assert call(connection.execute, statement, {"stamps": values}).scalar_one() == expected
    finally:
        call(connection.close)
