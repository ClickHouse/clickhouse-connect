from typing import Iterator
from pytest import fixture

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
