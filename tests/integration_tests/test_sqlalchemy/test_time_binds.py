from datetime import time, timedelta

import pytest
import sqlalchemy as db
from pytest import fixture
from sqlalchemy import MetaData, text
from sqlalchemy.engine import Engine

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Time, Time64, UInt32
from clickhouse_connect.driver import Client
from tests.integration_tests.conftest import TestConfig

TABLE_NAME = "sqla_time_binds"


@fixture(scope="module", autouse=True)
def time_support(test_client: Client, test_config: TestConfig):
    if test_config.cloud:
        pytest.skip("Time/Time64 types require settings change, but settings are locked in cloud")
    if not test_client.min_version("25.6"):
        pytest.skip("Time and Time64 types require ClickHouse 25.6+")


@fixture(scope="module", name="time_table")
def time_table_fixture(test_engine: Engine, test_db: str, test_table_engine: str):
    metadata = MetaData(schema=test_db)
    table = db.Table(
        TABLE_NAME,
        metadata,
        db.Column("id", UInt32),
        db.Column("t", Time),
        db.Column("t64", Time64(6)),
    )
    with test_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))
        conn.execute(
            text(
                f"CREATE TABLE {TABLE_NAME} (id UInt32, t Time, t64 Time64(6)) "
                f"ENGINE {test_table_engine} ORDER BY id SETTINGS enable_time_time64_type = 1"
            )
        )
    yield table
    with test_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))


def test_time_bind_roundtrip(test_engine: Engine, time_table):
    with test_engine.begin() as conn:
        conn.execute(db.insert(time_table).values(id=1, t=time(1, 2, 3), t64=time(0, 0, 13, 250306)))
        conn.execute(
            db.insert(time_table).values(id=2, t=timedelta(seconds=-2), t64=timedelta(hours=26, seconds=7, microseconds=79)),
        )
        rows = conn.execute(db.select(time_table).order_by(time_table.c.id)).all()
    assert [tuple(row) for row in rows] == [
        (1, timedelta(hours=1, minutes=2, seconds=3), timedelta(seconds=13, microseconds=250306)),
        (2, timedelta(seconds=-2), timedelta(hours=26, seconds=7, microseconds=79)),
    ]

    with test_engine.begin() as conn:
        result = conn.execute(db.select(time_table.c.id).where(time_table.c.t64 == timedelta(seconds=13, microseconds=250306)))
        assert [row[0] for row in result] == [1]

        result = conn.execute(
            db.select(time_table.c.id).where(time_table.c.t.in_([time(1, 2, 3), timedelta(seconds=-2)])).order_by(time_table.c.id),
        )
        assert [row[0] for row in result] == [1, 2]


def test_time_literal_binds(test_engine: Engine, time_table):
    stmt = db.select(time_table.c.id).where(time_table.c.t == timedelta(seconds=-2))
    compiled = str(stmt.compile(test_engine, compile_kwargs={"literal_binds": True}))
    assert "'-00:00:02'" in compiled

    stmt = db.select(time_table.c.id).where(time_table.c.t64 == time(1, 2, 3, 250306))
    compiled = str(stmt.compile(test_engine, compile_kwargs={"literal_binds": True}))
    assert "'01:02:03.250306'" in compiled
