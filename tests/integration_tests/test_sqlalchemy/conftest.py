from typing import Iterator

from pytest import fixture

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine


@fixture(scope='module')
def test_engine(request) -> Iterator[Engine]:
    host = request.config.getoption('host')
    port = request.config.getoption('port')
    docker = request.config.getoption('docker', True)
    if not port:
        port = 10723 if docker else 8123
    test_engine: Engine = create_engine(f'clickhousedb://{host}:{port}')
    yield test_engine
    test_engine.dispose()
