from collections.abc import Callable

from pytest import fixture

from clickhouse_connect import dbapi
from tests.integration_tests.conftest import TestConfig


@fixture(name="dbapi_connection")
def dbapi_connection_fixture(test_config: TestConfig, test_db: str):
    connection = dbapi.connect(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        database=test_db,
    )
    yield connection
    connection.close()


def test_executemany_with_tuple_rows(dbapi_connection, table_context: Callable):
    """Regression test: executemany with sequence rows (e.g. Airflow's
    DbApiHook.insert_rows) used to crash with AttributeError in _try_bulk_insert.
    """
    with table_context("dbapi_executemany_tuples", ["id UInt32", "name String"]):
        cursor = dbapi_connection.cursor()
        cursor.executemany(
            "INSERT INTO dbapi_executemany_tuples (id, name) VALUES (%s, %s)",
            [(13, "user_1"), (79, "user_2")],
        )
        cursor.execute("SELECT id, name FROM dbapi_executemany_tuples ORDER BY id")
        assert cursor.fetchall() == [(13, "user_1"), (79, "user_2")]


def test_executemany_with_dict_rows(dbapi_connection, table_context: Callable):
    with table_context("dbapi_executemany_dicts", ["id UInt32", "name String"]):
        cursor = dbapi_connection.cursor()
        cursor.executemany(
            "INSERT INTO dbapi_executemany_dicts (id, name) VALUES (%(id)s, %(name)s)",
            [{"id": 13, "name": "user_1"}, {"id": 79, "name": "user_2"}],
        )
        cursor.execute("SELECT id, name FROM dbapi_executemany_dicts ORDER BY id")
        assert cursor.fetchall() == [(13, "user_1"), (79, "user_2")]
