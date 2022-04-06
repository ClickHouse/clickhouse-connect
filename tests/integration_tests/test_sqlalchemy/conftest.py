from typing import Iterator

from pytest import fixture

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine

from clickhouse_connect.cc_sqlalchemy.ddl.custom import CreateDatabase, DropDatabase


@fixture(scope='module', name='test_engine')
def test_engine_fixture(request) -> Iterator[Engine]:
    host = request.config.getoption('host')
    port = request.config.getoption('port')
    docker = request.config.getoption('docker')
    cleanup = request.config.getoption('cleanup')
    if not port:
        port = 10723 if docker else 8123
    test_engine: Engine = create_engine(f'clickhousedb://{host}:{port}')
    conn = test_engine.connect()
    if not test_engine.dialect.has_database(conn, 'sqla_test'):
        conn.execute(CreateDatabase('sqla_test'))
    yield test_engine
    if cleanup:
        conn.execute(DropDatabase('sqla_test'))
    test_engine.dispose()
