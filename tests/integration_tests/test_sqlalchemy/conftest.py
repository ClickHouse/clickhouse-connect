import time
from typing import Iterator

from pytest import fixture
from sqlalchemy import text
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Connection, Engine

from tests.integration_tests.conftest import TestConfig


@fixture(scope='module', name='test_engine')
def test_engine_fixture(test_config: TestConfig) -> Iterator[Engine]:
    test_engine: Engine = create_engine(
        f'clickhousedb://{test_config.username}:{test_config.password}@{test_config.host}:' +
        f'{test_config.port}/{test_config.test_database}?ch_http_max_field_name_size=99999' +
        '&use_skip_indexes=0&ca_cert=certifi&query_limit=2333&compression=zstd&select_sequential_consistency=1'
    )

    yield test_engine
    test_engine.dispose()


def verify_tables_ready(conn: Connection, table_checks: dict[str, int], max_retries: int = 30, delay: float = 0.1) -> None:
    """Verify that tables are queryable and have expected row counts.

    This is helpful for cloud envs where there can be a delay between
    table creation/insertion and when the data becomes queryable.
    """
    retry_count = 0
    while retry_count < max_retries:
        try:
            counts = {}
            for table_name, _ in table_checks.items():
                actual_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                counts[table_name] = actual_count

            if all(counts[table] == expected for table, expected in table_checks.items()):
                return

            retry_count += 1
            if retry_count >= max_retries:
                count_strs = [f"{table}={counts[table]}" for table in table_checks]
                raise RuntimeError(f"Data verification failed after {max_retries} retries: {', '.join(count_strs)}")

            time.sleep(delay)

        except Exception as e:  # pylint: disable=broad-exception-caught
            retry_count += 1
            if retry_count >= max_retries:
                raise RuntimeError(f"Failed to verify test data after {max_retries} retries.") from e
            time.sleep(delay)
