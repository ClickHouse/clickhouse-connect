import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import NoResultFound, NoSuchTableError

from clickhouse_connect.datatypes.format import clear_all_formats, set_default_formats
from clickhouse_connect.driver.binding import quote_identifier


@pytest.fixture
def reflection_connection(client_mode, call, test_engine):
    if client_mode == "async":
        pytest.importorskip("sqlalchemy", minversion="2.0.44")
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine(test_engine.url.set(drivername="clickhousedb+async"))
    else:
        engine = sa.create_engine(test_engine.url)
    connection = call(engine.connect)
    try:
        yield lambda operation: call(connection.run_sync, operation) if client_mode == "async" else operation(connection)
    finally:
        call(connection.close)
        call(engine.dispose)


@pytest.mark.parametrize("string_bytes", [False, True])
@pytest.mark.parametrize("views", [False, True])
def test_view_discovery_preserves_metadata_reflection(reflection_connection, string_bytes, views):
    database = f"reflection views' {uuid.uuid4().hex}"
    quoted_database = quote_identifier(database)
    names = {"source", "target", "regular view", "materialized", "owned"}

    def check(connection):
        connection.execute(sa.text(f"CREATE DATABASE {quoted_database}"))
        try:
            for name in ("source", "target"):
                connection.execute(sa.text(f"CREATE TABLE {quoted_database}.{name} (value UInt32) ENGINE Memory"))
            connection.execute(sa.text(f"CREATE VIEW {quoted_database}.`regular view` AS SELECT value FROM {quoted_database}.source"))
            connection.execute(
                sa.text(
                    f"CREATE MATERIALIZED VIEW {quoted_database}.materialized TO {quoted_database}.target "
                    f"AS SELECT value FROM {quoted_database}.source"
                )
            )
            connection.execute(
                sa.text(f"CREATE MATERIALIZED VIEW {quoted_database}.owned ENGINE Memory AS SELECT value FROM {quoted_database}.source")
            )
            if string_bytes:
                set_default_formats("String", "bytes")
            inspector = sa.inspect(connection)
            assert names <= set(inspector.get_table_names(schema=database))
            assert inspector.get_view_names(schema=database) == ["regular view"]
            assert inspector.get_materialized_view_names(schema=database) == ["materialized", "owned"]
            metadata = sa.MetaData()
            metadata.reflect(bind=connection, schema=database, views=views)
            assert {table.name for table in metadata.tables.values()} >= names
            for name in ("regular view", "materialized", "owned"):
                metadata = sa.MetaData()
                metadata.reflect(bind=connection, schema=database, only=[name])
                assert [table.name for table in metadata.tables.values()] == [name]
            if hasattr(inspector, "get_multi_columns"):
                from sqlalchemy.engine.reflection import ObjectKind

                for kind, expected in [
                    (ObjectKind.TABLE, names),
                    (ObjectKind.VIEW, {"regular view"}),
                    (ObjectKind.MATERIALIZED_VIEW, {"materialized", "owned"}),
                    (ObjectKind.ANY, names),
                ]:
                    columns = inspector.get_multi_columns(schema=database, kind=kind)
                    reflected = {name for _, name in columns}
                    if kind in (ObjectKind.TABLE, ObjectKind.ANY):
                        assert reflected >= expected
                    else:
                        assert reflected == expected
        finally:
            clear_all_formats()
            connection.execute(sa.text(f"DROP DATABASE {quoted_database}"))

    reflection_connection(check)


@pytest.mark.parametrize("path", ["dialect", "inspector", "autoload"])
@pytest.mark.parametrize("explicit_schema", [False, True])
def test_missing_table_reflection_keeps_existing_handlers(reflection_connection, path, explicit_schema):
    name = f"missing_{uuid.uuid4().hex}"

    def check(connection):
        database = connection.execute(sa.text("SELECT currentDatabase()")).scalar()
        schema = database if explicit_schema else None
        with pytest.raises(NoSuchTableError) as error:
            if path == "dialect":
                connection.dialect.get_columns(connection, name, schema=schema)
            elif path == "inspector":
                sa.inspect(connection).get_columns(name, schema=schema)
            else:
                sa.Table(name, sa.MetaData(), schema=schema, autoload_with=connection)
        assert isinstance(error.value, NoResultFound)
        assert str(error.value) == f"Table {database}.{name} does not exist"

    reflection_connection(check)
