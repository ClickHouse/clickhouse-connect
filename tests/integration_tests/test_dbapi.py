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


def test_description_null_ok_reflects_nullability(dbapi_connection):
    """Regression for https://github.com/ClickHouse/clickhouse-connect/issues/902:
    the description null_ok field (7th tuple element) must reflect the column's
    actual Nullable() wrapper, not a hardcoded True.
    """
    cursor = dbapi_connection.cursor()
    cursor.execute("SELECT CAST(13, 'UInt32') AS not_null_col, CAST(NULL, 'Nullable(String)') AS null_col")

    assert cursor.description == [
        ("not_null_col", "UInt32", None, None, None, None, False),
        ("null_col", "Nullable(String)", None, None, None, None, True),
    ]
