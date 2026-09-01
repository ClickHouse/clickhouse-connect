import importlib
import re
import subprocess
import sys
import textwrap

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


def test_async_dialect_greenlet_guard():
    script = textwrap.dedent(
        """
        import importlib
        import importlib.metadata
        import importlib.util

        real_version = importlib.metadata.version
        importlib.metadata.version = lambda name: "2.0.44" if name == "sqlalchemy" else real_version(name)
        real_find_spec = importlib.util.find_spec
        importlib.util.find_spec = lambda name, package=None: (
            None if name == "greenlet" else real_find_spec(name, package)
        )

        try:
            importlib.import_module("clickhouse_connect.cc_sqlalchemy.asyncio")
        except ImportError as ex:
            assert "requires greenlet" in str(ex)
            assert "clickhouse-connect[sqlalchemy-async]" in str(ex)
        else:
            raise AssertionError("missing greenlet was accepted")

        importlib.import_module("clickhouse_connect.cc_sqlalchemy.dialect")
        """
    )
    subprocess.run([sys.executable, "-c", script], check=True)
