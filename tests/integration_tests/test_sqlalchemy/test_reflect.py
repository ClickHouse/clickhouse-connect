import pytest
import sqlalchemy as db
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from clickhouse_connect import common
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Geometry, Point, SimpleAggregateFunction, UInt32
from clickhouse_connect.driver.exceptions import DatabaseError


def test_basic_reflection(test_engine: Engine):
    common.set_setting("invalid_setting_action", "drop")
    with test_engine.begin() as conn:
        metadata = db.MetaData(schema="system")
        table = db.Table("tables", metadata, autoload_with=test_engine)
        query = db.select(table.columns.create_table_query)
        result = conn.execute(query)
        rows = result.fetchmany(100)
        assert rows


def test_full_table_reflection(test_engine: Engine, test_db: str):
    common.set_setting("invalid_setting_action", "drop")
    with test_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.reflect_test"))
        conn.execute(
            text(
                f"CREATE TABLE {test_db}.reflect_test (key UInt32, value FixedString(20),"
                + "agg SimpleAggregateFunction(anyLast, String))"
                + "ENGINE AggregatingMergeTree ORDER BY key"
            )
        )
        metadata = db.MetaData(schema=test_db)
        table = db.Table("reflect_test", metadata, autoload_with=test_engine)
        assert table.columns.key.type.__class__ == UInt32
        assert table.columns.agg.type.__class__ == SimpleAggregateFunction
        assert "MergeTree" in table.engine.name


def test_types_reflection(test_engine: Engine, test_db: str):
    common.set_setting("invalid_setting_action", "drop")
    with test_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.sqlalchemy_types_test"))
        conn.execute(text(f"CREATE TABLE {test_db}.sqlalchemy_types_test (key UInt32, pt Point) ENGINE MergeTree ORDER BY key"))
        metadata = db.MetaData(schema=test_db)
        table = db.Table("sqlalchemy_types_test", metadata, autoload_with=test_engine)
        assert table.columns.key.type.__class__ == UInt32
        assert table.columns.pt.type.__class__ == Point
        assert "MergeTree" in table.engine.name


def test_geometry_reflection(test_engine: Engine, test_db: str, test_client):
    try:
        resolved_type = test_client.command("SELECT toTypeName(CAST(NULL, 'Geometry'))")
    except DatabaseError as ex:
        if ex.name != "UNKNOWN_TYPE":
            raise
        pytest.skip(f"Geometry is not supported by server {test_client.server_version}")
    if resolved_type != "Geometry":
        pytest.skip(f"Geometry is not supported by server {test_client.server_version}")

    common.set_setting("invalid_setting_action", "drop")
    with test_engine.begin() as conn:
        try:
            conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.sqlalchemy_geometry_test"))
            conn.execute(
                text(f"CREATE TABLE {test_db}.sqlalchemy_geometry_test (key UInt32, geometry Geometry) ENGINE MergeTree ORDER BY key")
            )
            metadata = db.MetaData(schema=test_db)
            table = db.Table("sqlalchemy_geometry_test", metadata, autoload_with=test_engine)
            assert table.columns.geometry.type.__class__ == Geometry
            assert table.columns.geometry.type.name == "Geometry"
        finally:
            conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.sqlalchemy_geometry_test"))


def test_table_exists(test_engine: Engine):
    common.set_setting("invalid_setting_action", "drop")
    inspector = inspect(test_engine)
    assert inspector.has_table(table_name="columns", schema="system")
    assert not inspector.has_table(table_name="nope", schema="fake_db")


def test_get_schema_names(test_engine: Engine):
    common.set_setting("invalid_setting_action", "drop")
    inspector = inspect(test_engine)
    schema_names = inspector.get_schema_names()
    assert isinstance(schema_names, list)
    assert "system" in schema_names
    assert "fake_db" not in schema_names


def test_get_table_names(test_engine: Engine, test_db: str):
    common.set_setting("invalid_setting_action", "drop")
    inspector = inspect(test_engine)
    system_tables = inspector.get_table_names(schema="system")
    assert isinstance(system_tables, list)
    assert "columns" in system_tables
    assert "fake_table" not in system_tables


def test_metadata_reflect(test_engine: Engine, test_db: str):
    """Dialect-level reflection. MetaData.reflect() exercises the
    Dialect.get_multi_columns -> Dialect.get_columns path (not
    Inspector.get_columns), which previously raised NotImplementedError.
    The dialect does not reflect a primary key: ClickHouse PRIMARY KEY /
    ORDER BY is not a uniqueness guarantee, so the identity key is left for
    application code to declare explicitly."""
    common.set_setting("invalid_setting_action", "drop")
    with test_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.reflect_pk_test"))
        conn.execute(
            text(
                f"CREATE TABLE {test_db}.reflect_pk_test (org_id UInt32, id UInt64, payload String) ENGINE MergeTree ORDER BY (org_id, id)"
            )
        )

    metadata = db.MetaData(schema=test_db)
    metadata.reflect(bind=test_engine, only=["reflect_pk_test"])
    table = metadata.tables[f"{test_db}.reflect_pk_test"]

    assert {c.name for c in table.columns} == {"org_id", "id", "payload"}
    assert list(table.primary_key.columns) == []

    # Direct autoload should also populate columns without a reflected PK.
    table2 = db.Table("reflect_pk_test", db.MetaData(schema=test_db), autoload_with=test_engine)
    assert {c.name for c in table2.columns} == {"org_id", "id", "payload"}
    assert list(table2.primary_key.columns) == []


def test_user_declared_primary_key(test_engine: Engine, test_db: str):
    """A user-declared primary key on a pre-declared column survives reflection."""
    common.set_setting("invalid_setting_action", "drop")
    with test_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.reflect_user_pk_test"))
        conn.execute(
            text(
                f"CREATE TABLE {test_db}.reflect_user_pk_test (org_id UInt32, id UInt64, payload String) "
                "ENGINE MergeTree ORDER BY (org_id, id)"
            )
        )

    table = db.Table(
        "reflect_user_pk_test",
        db.MetaData(schema=test_db),
        db.Column("org_id", UInt32, primary_key=True),
        db.Column("id", db.BigInteger, primary_key=True),
        autoload_with=test_engine,
    )
    assert [c.name for c in table.primary_key.columns] == ["org_id", "id"]
    assert {c.name for c in table.columns} == {"org_id", "id", "payload"}
