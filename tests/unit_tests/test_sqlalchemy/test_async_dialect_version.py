import importlib
import re

import pytest
import sqlalchemy


def test_async_dialect_sqlalchemy_version_guard():
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", sqlalchemy.__version__)
    assert match is not None
    version = tuple(int(part) for part in match.groups())
    if version < (2, 0, 44) or version >= (3, 0, 0):
        with pytest.raises(ImportError, match=r"requires SQLAlchemy >=2\.0\.44,<3\.0"):
            importlib.import_module("clickhouse_connect.cc_sqlalchemy.asyncio")
    else:
        module = importlib.import_module("clickhouse_connect.cc_sqlalchemy.asyncio")
        assert module.ClickHouseAsyncDialect.is_async is True
