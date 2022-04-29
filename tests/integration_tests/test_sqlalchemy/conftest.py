from typing import Iterator
from pytest import fixture

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine

from tests.integration_tests.conftest import ClientConfig


@fixture(scope='module', name='test_engine')
def test_engine_fixture(client_config: ClientConfig) -> Iterator[Engine]:
    test_engine: Engine = create_engine(f'clickhousedb://{client_config.username}:{client_config.password}@{client_config.host}:' +
                                        f'{client_config.port}/{client_config.test_database}')
    yield test_engine
    test_engine.dispose()
