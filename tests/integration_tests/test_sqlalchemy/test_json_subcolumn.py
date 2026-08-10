import pytest
from sqlalchemy import Column, MetaData, Table, select
from sqlalchemy.engine import Engine

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import JSON, UInt32
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import engine_map


@pytest.fixture(scope="module")
def json_subcolumn_table(test_engine: Engine, test_db: str, test_table_engine: str):
    engine_cls = engine_map[test_table_engine]
    metadata = MetaData(schema=test_db)
    events = Table(
        "json_subcolumn_test",
        metadata,
        Column("id", UInt32),
        Column("payload", JSON),
        engine_cls(order_by="id"),
    )

    with test_engine.begin() as conn:
        client = conn.connection.driver_connection.client
        if not client.min_version("25.3"):
            pytest.skip("JSON subcolumns require ClickHouse 25.3 or newer")
        events.drop(conn, checkfirst=True)
        events.create(conn)
        conn.execute(
            events.insert(),
            [
                {
                    "id": 13,
                    "payload": {
                        "severity": "warning",
                        "context": {"request": {"id": 79}},
                        ":kind": "colon",
                        "^kind": "caret",
                        "@kind": "at",
                        "quoted`key": "quoted",
                        "`wrapped`": "wrapped",
                        "a%2Eb": "encoded dot",
                    },
                }
            ],
        )

    yield events

    with test_engine.begin() as conn:
        events.drop(conn, checkfirst=True)


def test_json_subcolumn_live_execution(test_engine: Engine, json_subcolumn_table: Table):
    events = json_subcolumn_table.alias("evt")
    statement = select(
        events.c.payload["severity"].label("severity"),
        events.c.payload["context"]["request"].subcolumn("id", type_=UInt32).label("request_id"),
        events.c.payload[":kind"].label("colon_kind"),
        events.c.payload["^kind"].label("caret_kind"),
        events.c.payload["@kind"].label("at_kind"),
        events.c.payload["quoted`key"].label("quoted_key"),
        events.c.payload["`wrapped`"].label("wrapped_key"),
        events.c.payload["a%2Eb"].label("encoded_dot"),
    ).where(events.c.payload["severity"] == "warning")

    sql = str(statement.compile(dialect=test_engine.dialect))
    assert "getSubcolumn" not in sql
    assert "CAST(`evt`.`payload`.`context`.`request`.`id` AS UInt32)" in sql

    with test_engine.connect() as conn:
        row = conn.execute(statement).one()

    assert row == ("warning", 79, "colon", "caret", "at", "quoted", "wrapped", "encoded dot")
