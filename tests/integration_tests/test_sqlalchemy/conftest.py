from typing import Iterator

from pytest import fixture

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine

from clickhouse_connect.cc_sqlalchemy.ddl.custom import CreateDatabase, DropDatabase


@fixture(scope='module')
def test_engine(request) -> Iterator[Engine]:
    host = request.config.getoption('host')
    port = request.config.getoption('port')
    docker = request.config.getoption('docker', True)
    if not port:
        port = 10723 if docker else 8123
    test_engine: Engine = create_engine(f'clickhousedb://{host}:{port}')
    conn = test_engine.connect()
    if not test_engine.dialect.has_database(conn, 'sqla_test'):
        cs = CreateDatabase('sqla_test')
        conn.execute(cs)
    yield test_engine
    ds = DropDatabase('sqla_test')
    conn.execute(ds)
    test_engine.dispose()



