import clickhouse_connect
from clickhouse_connect.common import version


def test_version_is_string():
    """Verify that clickhouse_connect.__version__ is a string."""
    assert isinstance(clickhouse_connect.__version__, str)


def test_version_matches_common_version():
    """Verify that __version__ and common.version() return the same value."""
    assert clickhouse_connect.__version__ == version()


def test_version_not_empty():
    """Verify that version is not empty."""
    assert len(clickhouse_connect.__version__) > 0
