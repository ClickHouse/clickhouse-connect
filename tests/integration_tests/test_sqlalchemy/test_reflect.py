import pytest
import sqlalchemy as db
from sqlalchemy import MetaData, Table, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import NoResultFound

from clickhouse_connect import common
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Geometry, Point, SimpleAggregateFunction, UInt32
from clickhouse_connect.datatypes.format import clear_all_formats, set_default_formats
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
                + "ENGINE AggregatingMergeTree ORDER BY (key, value)"
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
        resolved_type = test_client.command("SELECT toTypeName(defaultValueOfTypeName('Geometry'))")
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


def test_variant_reflection(test_engine: Engine, test_db: str):
    table_name = "sqlalchemy_variant_reflection_test"
    with test_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.{table_name}"))
        conn.execute(text(f"CREATE TABLE {test_db}.{table_name} (id UInt32, value Variant(UInt32, String)) ENGINE MergeTree ORDER BY id"))

    try:
        variant_type = sqla_type_from_name("Variant(UInt32, String)")
        columns = inspect(test_engine).get_columns(table_name, schema=test_db)
        reflected_type = next(column["type"] for column in columns if column["name"] == "value")
        assert reflected_type.__class__ is type(variant_type)
        assert reflected_type.name == "Variant(String, UInt32)"

        table = Table(table_name, MetaData(schema=test_db), autoload_with=test_engine)
        assert table.c.value.type.__class__ is type(variant_type)
        assert table.c.value.type.name == "Variant(String, UInt32)"
    finally:
        with test_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.{table_name}"))


def test_table_exists(test_engine: Engine):
    common.set_setting("invalid_setting_action", "drop")
    inspector = inspect(test_engine)
    assert inspector.has_table(table_name="columns", schema="system")
    assert not inspector.has_table(table_name="nope", schema="fake_db")


def test_direct_inspector_reflection(test_engine: Engine, monkeypatch):
    common.set_setting("invalid_setting_action", "drop")

    inspector = inspect(test_engine)
    pool_events = []

    def record_checkout(*_args):
        pool_events.append("checkout")

    def record_checkin(*_args):
        pool_events.append("checkin")

    db.event.listen(test_engine, "checkout", record_checkout)
    db.event.listen(test_engine, "checkin", record_checkin)
    try:
        assert "name" in {column["name"] for column in inspector.get_columns("tables", schema="system")}
        assert pool_events == ["checkout", "checkin"]

        pool_events.clear()
        with pytest.raises(NoResultFound):
            inspector.get_columns("missing_reflection_table", schema="system")
        assert pool_events == ["checkout", "checkin"]

        inspector_class = type(inspector)
        original_get_columns = inspector_class.get_columns
        dispatched_binds = []

        def tracking_get_columns(self, table_name, schema=None, **kwargs):
            dispatched_binds.append(self.bind)
            return original_get_columns(self, table_name, schema, **kwargs)

        monkeypatch.setattr(inspector_class, "get_columns", tracking_get_columns)
        pool_events.clear()
        table = Table("tables", MetaData(schema="system"))
        inspector.reflect_table(table)
        assert "name" in table.columns
        assert pool_events == ["checkout", "checkin"]
        assert len(dispatched_binds) == 1
        assert isinstance(dispatched_binds[0], Connection)

        with test_engine.connect() as connection:
            pool_events.clear()
            inspector = inspect(connection)
            assert "name" in {column["name"] for column in inspector.get_columns("tables", schema="system")}
            table = Table("tables", MetaData(schema="system"))
            inspector.reflect_table(table)
            assert "name" in table.columns
            assert pool_events == []
            assert not connection.closed
    finally:
        db.event.remove(test_engine, "checkout", record_checkout)
        db.event.remove(test_engine, "checkin", record_checkin)


def test_reflection_column_filters(test_engine: Engine):
    common.set_setting("invalid_setting_action", "drop")

    table = Table("tables", MetaData(schema="system"), autoload_with=test_engine, include_columns=["name"])
    assert [column.name for column in table.columns] == ["name"]

    table = Table("tables", MetaData(schema="system"))
    inspect(test_engine).reflect_table(table, ["name", "engine"], ["engine"], True)
    assert [column.name for column in table.columns] == ["name"]


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


def test_reflection_with_string_bytes_format(test_engine: Engine, test_db: str):
    """Global set_default_formats("String", "bytes") must not break SQLAlchemy reflection.

    Metadata queries force String -> string decode (same as core _INTERNAL_QUERY_FORMATS).
    User SELECT data still returns bytes. Closes #920.
    """
    common.set_setting("invalid_setting_action", "drop")
    table_name = "reflect_bytes_fmt"
    try:
        with test_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.{table_name}"))
            conn.execute(text(f"CREATE TABLE {test_db}.{table_name} (k UInt32, s String) ENGINE MergeTree ORDER BY k"))
            conn.execute(text(f"INSERT INTO {test_db}.{table_name} VALUES (1, 'hello')"))

        set_default_formats("String", "bytes")

        with test_engine.connect() as conn:
            insp = inspect(conn)
            table_names = insp.get_table_names(schema=test_db)
            assert table_name in table_names
            assert all(isinstance(name, str) for name in table_names)

            columns = insp.get_columns(table_name, schema=test_db)
            assert [(c["name"], type(c["name"])) for c in columns] == [
                ("k", str),
                ("s", str),
            ]
            assert all(isinstance(c["name"], str) for c in columns)

            table = Table(table_name, MetaData(schema=test_db), autoload_with=conn)
            assert {c.name for c in table.columns} == {"k", "s"}

            # User data still honors the global bytes format.
            row = conn.execute(text(f"SELECT s FROM {test_db}.{table_name} WHERE k = 1")).fetchone()
            assert row[0] == b"hello"
    finally:
        clear_all_formats()
        with test_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.{table_name}"))
