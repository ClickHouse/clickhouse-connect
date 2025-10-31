from contextlib import contextmanager
from typing import Iterator
from pytest import fixture
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine

from tests.integration_tests.conftest import TestConfig


@fixture(scope='module', name='test_engine')
def test_engine_fixture(test_config: TestConfig) -> Iterator[Engine]:
    test_engine: Engine = create_engine(
        f'clickhousedb://{test_config.username}:{test_config.password}@{test_config.host}:' +
        f'{test_config.port}/{test_config.test_database}?ch_http_max_field_name_size=99999' +
        '&use_skip_indexes=0&ca_cert=certifi&query_limit=2333&compression=zstd'
    )

    yield test_engine
    test_engine.dispose()


def create_test_table(conn, metadata, table_name, columns, engine_params):
    test_table = Table(table_name, metadata, *columns, engine_params)
    test_table.drop(conn, checkfirst=True)
    test_table.create(conn)
    return test_table


@contextmanager
def table_context(engine, test_db, table_name, columns, engine_params):
    with engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        test_table = create_test_table(conn, metadata, table_name, columns, engine_params)
        yield conn, test_table
