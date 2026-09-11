from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from clickhouse_connect.cc_sqlalchemy import engines, types


def test_memory_engine_reflection(test_engine: Engine, test_db: str):
    metadata = sa.MetaData(schema=test_db)
    table = sa.Table(
        f"memory_engine_{uuid4().hex[:8]}", metadata, sa.Column("id", types.UInt64), engines.Memory(settings={"max_rows_to_keep": 13})
    )
    with test_engine.begin() as conn:
        try:
            table.create(conn)
            reflected = sa.Table(table.name, sa.MetaData(schema=test_db), autoload_with=conn)
            assert reflected.engine.settings["max_rows_to_keep"] == 13
            replay = sa.Table(table.name + "_replay", metadata, sa.Column("id", types.UInt64), eval(repr(reflected.engine), vars(engines)))
            replay.create(conn)
            for target in (table, replay):
                conn.execute(target.insert(), [{"id": 13}, {"id": 79}])
                assert conn.execute(sa.select(target).order_by(target.c.id)).all() == [(13,), (79,)]
        finally:
            metadata.drop_all(conn, checkfirst=True)


@pytest.mark.parametrize("amount_name", ["amount", "net amount, total"])
def test_summing_engine_reflection(test_engine: Engine, test_db: str, amount_name: str):
    metadata = sa.MetaData(schema=test_db)
    amount = sa.Column(amount_name, types.UInt64)
    table = sa.Table(
        f"summing_engine_{uuid4().hex[:8]}",
        metadata,
        sa.Column("id", types.UInt64),
        amount,
        sa.Column("other", types.UInt64),
        engines.SummingMergeTree("id", columns=[amount], settings={"index_granularity": 1024}),
    )
    with test_engine.begin() as conn:
        try:
            table.create(conn)
            reflected = sa.Table(table.name, sa.MetaData(schema=test_db), autoload_with=conn)
            replay = sa.Table(
                table.name + "_replay",
                metadata,
                sa.Column("id", types.UInt64),
                sa.Column(amount_name, types.UInt64),
                sa.Column("other", types.UInt64),
                eval(repr(reflected.engine), vars(engines)),
            )
            replay.create(conn)
            for target in (table, replay):
                conn.execute(target.insert(), [{"id": 13, amount_name: 13, "other": 7}, {"id": 13, amount_name: 79, "other": 11}])
                rows = conn.execute(sa.select(target).final()).all()
                assert len(rows) == 1
                assert rows[0][:2] == (13, 92)
                assert rows[0][2] in (7, 11)
        finally:
            metadata.drop_all(conn, checkfirst=True)
