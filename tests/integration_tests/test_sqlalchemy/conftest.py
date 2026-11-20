import sys
import sysconfig
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from pytest import fixture
from sqlalchemy import text
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Connection, Engine

from tests.integration_tests.conftest import TestConfig

# greenlet hides suspended frames from the free-threaded GC before 3.15, so asyncio SQLAlchemy can use freed memory
_GREENLET_FRAMES_HIDDEN_FROM_GC = bool(sysconfig.get_config_var("Py_GIL_DISABLED")) and sys.version_info < (3, 15)
_CLIENT_ONLY_FIXTURES = {"param_client", "client_factory"}


def _uses_async_sqlalchemy(item: pytest.Item) -> bool:
    if item.get_closest_marker("asyncio") is not None:
        return True
    callspec = getattr(item, "callspec", None)
    if callspec is None or callspec.params.get("client_mode") != "async":
        return False
    return not _CLIENT_ONLY_FIXTURES.intersection(getattr(item, "fixturenames", ()))


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    if not _GREENLET_FRAMES_HIDDEN_FROM_GC:
        return
    here = Path(__file__).parent
    skip = pytest.mark.skip(reason="asyncio SQLAlchemy crashes on free-threaded 3.14, see python/cpython#150766")
    for item in items:
        if item.path.parent == here and _uses_async_sqlalchemy(item):
            item.add_marker(skip)


@fixture(scope="module", name="test_engine")
def test_engine_fixture(test_config: TestConfig) -> Iterator[Engine]:
    conn_str = (
        f"clickhousedb://{test_config.username}:{test_config.password}@{test_config.host}:"
        f"{test_config.port}/{test_config.test_database}?ch_http_max_field_name_size=99999"
        "&use_skip_indexes=0&ca_cert=certifi&query_limit=2333&compression=zstd"
    )
    if test_config.cloud:
        conn_str += "&select_sequential_consistency=1&alter_sync=2"
    if test_config.insert_quorum:
        conn_str += f"&insert_quorum={test_config.insert_quorum}"

    test_engine: Engine = create_engine(conn_str)

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

        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                raise RuntimeError(f"Failed to verify test data after {max_retries} retries.") from e
            time.sleep(delay)
