from typing import Iterator
from pytest import fixture

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine

from tests.integration_tests.conftest import TestConfig


@fixture(scope='module', name='test_engine')
def test_engine_fixture(test_config: TestConfig) -> Iterator[Engine]:
    test_engine: Engine = create_engine(f'clickhousedb://{test_config.username}:{test_config.password}@{test_config.host}:' +
                                        f'{test_config.port}/{test_config.test_database}?allow_experimental_object_type=1')
    yield test_engine
    test_engine.dispose()
