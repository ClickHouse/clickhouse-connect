from collections.abc import Callable

from pytest import fixture

from clickhouse_connect import dbapi
from tests.integration_tests.conftest import TestConfig


@fixture(name="dbapi_connection")
def dbapi_connection_fixture(test_config: TestConfig, test_db: str):
    settings = {}
    if test_config.insert_quorum:
        settings["insert_quorum"] = test_config.insert_quorum
    elif test_config.cloud:
        settings["select_sequential_consistency"] = 1
    connection = dbapi.connect(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        database=test_db,
        compress=test_config.compress,
        **settings,
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


def test_description_precision_and_scale(dbapi_connection, table_context: Callable):
    """Cursor.description exposes precision and scale for DateTime64 and Decimal
    columns queried from a real server, and leaves them None for other types.
    https://github.com/ClickHouse/clickhouse-connect/issues/881
    """
    columns = ["dt DateTime64(3)", "d Decimal(18, 4)", "n UInt64", "s String"]
    with table_context("dbapi_description_precision", columns):
        cursor = dbapi_connection.cursor()
        cursor.execute("SELECT dt, d, n, s FROM dbapi_description_precision")
        assert cursor.description == [
            ("dt", "DateTime64(3)", None, None, 3, 3, True),
            ("d", "Decimal(18, 4)", None, None, 18, 4, True),
            ("n", "UInt64", None, None, None, None, True),
            ("s", "String", None, None, None, None, True),
        ]
