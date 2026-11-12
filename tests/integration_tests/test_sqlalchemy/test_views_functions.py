import time
import uuid

import pytest
import sqlalchemy as sa
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy.exc import DatabaseError, NoResultFound, NoSuchTableError

from clickhouse_connect.datatypes.format import clear_read_format, set_read_format
from clickhouse_connect.driver.binding import quote_identifier

# ruff: noqa: F811
from tests.integration_tests.conftest import TEST_DATABASE_PREFIX
from tests.integration_tests.test_sqlalchemy.test_skip_indexes import run_sqlalchemy  # noqa: F401


@pytest.mark.parametrize("string_bytes", [False, True])
def test_ordinary_view_lifecycle(run_sqlalchemy, string_bytes):
    database = f"view db {uuid.uuid4().hex}"
    quoted_database = quote_identifier(database)
    name = "(view`quoted100%%)"
    qualified = f"{quoted_database}.{quote_identifier(name)}"

    def workflow(connection):
        connection.exec_driver_sql(f"CREATE DATABASE {quoted_database}")
        migration = Operations(MigrationContext.configure(connection))
        inspector = sa.inspect(connection)
        try:
            migration.create_clickhouse_view(name, "SELECT 13 AS value, '100%%' AS label", schema=database)
            if string_bytes:
                set_read_format("String", "bytes")
            assert inspector.get_view_names(database) == [name]
            assert inspector.get_view_definition(name, database) == "SELECT 13 AS value, '100%%' AS label"
            assert connection.exec_driver_sql(f"SELECT value FROM {qualified.replace('%', '%%')}").scalar_one() == 13
            label = connection.exec_driver_sql(f"SELECT label FROM {qualified.replace('%', '%%')}").scalar_one()
            assert label == (b"100%%" if string_bytes else "100%%")
            migration.create_clickhouse_view(name, "SELECT 79 AS value", schema=database, if_not_exists=True)
            assert inspector.get_view_definition(name, database) == "SELECT 13 AS value, '100%%' AS label"
            with pytest.raises(DatabaseError):
                migration.create_clickhouse_view(name, "SELECT 79 AS value", schema=database)
            migration.create_clickhouse_view(name, "SELECT 79 AS value", schema=database, or_replace=True)
            assert inspector.get_view_definition(name, database) == "SELECT 79 AS value"
            assert connection.exec_driver_sql(f"SELECT value FROM {qualified.replace('%', '%%')}").scalar_one() == 79
            with pytest.raises(DatabaseError):
                migration.create_clickhouse_view("invalid", "SELECT missing_column", schema=database)
            migration.drop_clickhouse_view(name, schema=database)
            assert inspector.get_view_names(database) == []
            with pytest.raises(NoSuchTableError):
                inspector.get_view_definition(name, database)
            migration.drop_clickhouse_view(name, schema=database, if_exists=True)
            with pytest.raises(DatabaseError):
                migration.drop_clickhouse_view(name, schema=database)
        finally:
            clear_read_format("String")
            connection.exec_driver_sql(f"DROP DATABASE {quoted_database}")

    run_sqlalchemy(workflow)


def test_view_name_collision_has_server_semantics(run_sqlalchemy):
    database = f"view_collision_{uuid.uuid4().hex}"
    quoted_database = quote_identifier(database)

    def workflow(connection):
        connection.exec_driver_sql(f"CREATE DATABASE {quoted_database}")
        migration = Operations(MigrationContext.configure(connection))
        try:
            connection.exec_driver_sql(f"CREATE TABLE {quoted_database}.collision (value UInt32) ENGINE Memory")
            connection.exec_driver_sql(f"INSERT INTO {quoted_database}.collision VALUES (13)")
            with pytest.raises(DatabaseError):
                migration.drop_clickhouse_view("collision", schema=database)
            assert connection.exec_driver_sql(f"SELECT value FROM {quoted_database}.collision").scalar_one() == 13
            with pytest.raises(DatabaseError):
                migration.create_clickhouse_view("collision", "SELECT 79 AS value", schema=database)
            migration.create_clickhouse_view("collision", "SELECT 79 AS value", schema=database, or_replace=True)
            assert sa.inspect(connection).get_view_names(database) == ["collision"]
            assert connection.exec_driver_sql(f"SELECT value FROM {quoted_database}.collision").scalar_one() == 79
            migration.drop_clickhouse_view("collision", schema=database)
        finally:
            connection.exec_driver_sql(f"DROP DATABASE {quoted_database}")

    run_sqlalchemy(workflow)


@pytest.mark.parametrize("string_bytes", [False, True])
def test_sql_function_lifecycle_and_global_scope(run_sqlalchemy, string_bytes, test_db):
    function = f"{TEST_DATABASE_PREFIX}udf__{int(time.time() * 1000)}__{uuid.uuid4().hex}__quoted`100%%"
    quoted_function = quote_identifier(function)
    database = f"{TEST_DATABASE_PREFIX}{uuid.uuid4().int % 900000 + 100000}__{int(time.time() * 1000)}"
    quoted_database = quote_identifier(database)

    def create(connection):
        assert connection.exec_driver_sql("SELECT currentDatabase()").scalar_one() == test_db
        connection.exec_driver_sql(f"CREATE DATABASE {quoted_database}")
        for _ in range(30):
            if database in sa.inspect(connection).get_schema_names():
                break
            time.sleep(0.1)
        else:
            pytest.fail(f"Database {database} not visible after waiting")
        migration = Operations(MigrationContext.configure(connection))
        migration.create_clickhouse_function(function, "(value) -> if(value = 13, value + 13, length('100%%'))")
        if string_bytes:
            set_read_format("String", "bytes")
        inspector = sa.inspect(connection)
        assert function in inspector.get_clickhouse_function_names()
        assert "plus" not in inspector.get_clickhouse_function_names()
        definition = inspector.get_clickhouse_function_definition(function)
        stored = connection.execute(
            sa.text("SELECT create_query FROM system.functions WHERE name = :name"), {"name": function}
        ).scalar_one()
        assert definition == (stored.decode() if isinstance(stored, bytes) else stored)
        assert isinstance(definition, str) and definition.startswith("CREATE FUNCTION")
        assert connection.exec_driver_sql(f"SELECT {quoted_function.replace('%', '%%')}(13)").scalar_one() == 26
        assert connection.exec_driver_sql(f"SELECT {quoted_function.replace('%', '%%')}(79)").scalar_one() == 5
        return definition

    def other_database(connection):
        current_database = connection.exec_driver_sql("SELECT currentDatabase()").scalar_one()
        assert current_database == (database.encode() if string_bytes else database)
        assert database != test_db
        migration = Operations(MigrationContext.configure(connection))
        inspector = sa.inspect(connection)
        assert function in inspector.get_clickhouse_function_names()
        assert inspector.get_clickhouse_function_definition(function) == definition
        assert connection.exec_driver_sql(f"SELECT {quoted_function.replace('%', '%%')}(13)").scalar_one() == 26
        with pytest.raises(DatabaseError):
            migration.create_clickhouse_function(function, "value -> value + 79")
        migration.create_clickhouse_function(function, "value -> value + 79", if_not_exists=True)
        assert inspector.get_clickhouse_function_definition(function) == definition
        migration.create_clickhouse_function(function, "value -> value + 79", or_replace=True)
        assert connection.exec_driver_sql(f"SELECT {quoted_function.replace('%', '%%')}(13)").scalar_one() == 92
        assert inspector.get_clickhouse_function_definition(function) != definition
        with pytest.raises(DatabaseError):
            migration.create_clickhouse_function(function, "value -> (", or_replace=True)
        for built_in in ("plus", "missing_sql_udf"):
            with pytest.raises(NoResultFound):
                inspector.get_clickhouse_function_definition(built_in)
        migration.drop_clickhouse_function(function)
        assert function not in inspector.get_clickhouse_function_names()
        with pytest.raises(NoResultFound):
            inspector.get_clickhouse_function_definition(function)
        migration.drop_clickhouse_function(function, if_exists=True)
        with pytest.raises(DatabaseError):
            migration.drop_clickhouse_function(function)

    def cleanup(connection):
        try:
            Operations(MigrationContext.configure(connection)).drop_clickhouse_function(function, if_exists=True)
        finally:
            connection.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted_database}")

    try:
        definition = run_sqlalchemy(create)
        run_sqlalchemy(other_database, database=database)
    finally:
        clear_read_format("String")
        run_sqlalchemy(cleanup)
