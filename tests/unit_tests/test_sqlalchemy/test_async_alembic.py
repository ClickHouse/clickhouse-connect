import runpy
from contextlib import nullcontext
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

pytest.importorskip("sqlalchemy", minversion="2.0.44")
pytest.importorskip("alembic", minversion="1.18")

from alembic import context as alembic_context
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import text

from clickhouse_connect.cc_sqlalchemy import alembic as ch_alembic
from tests.helpers import run_in_new_loop


def test_async_alembic_offline_context_compiles_without_client():
    buffer = StringIO()

    with patch(
        "clickhouse_connect.cc_sqlalchemy.asyncio.create_async_client",
        Mock(side_effect=AssertionError("offline migration created a client")),
    ) as create_async_client:
        context = MigrationContext.configure(
            url="clickhousedb+async://user:password@localhost:8123/default",
            opts={
                "as_sql": True,
                "output_buffer": buffer,
                "literal_binds": True,
                "dialect_opts": {"paramstyle": "named"},
            },
        )
        Operations(context).execute(text("CREATE TABLE events (id UInt32) ENGINE MergeTree ORDER BY id"))

    create_async_client.assert_not_called()
    assert buffer.getvalue() == "CREATE TABLE events (id UInt32) ENGINE MergeTree ORDER BY id;\n\n"


def test_checked_in_async_alembic_environment_carries_clickhouse_hooks():
    env_path = Path(__file__).parents[3] / "examples" / "alembic_async" / "env.py"
    configure = Mock()
    run_migrations = Mock()
    config = SimpleNamespace(
        config_file_name=None,
        config_ini_section="alembic",
        get_main_option=Mock(return_value="clickhousedb+async://user:password@localhost:8123/default"),
        get_section=Mock(return_value={"sqlalchemy.url": "clickhousedb+async://user:password@localhost:8123/default"}),
    )

    class SyncConnection:
        def exec_driver_sql(self, statement):
            assert statement == "SELECT currentDatabase()"
            return SimpleNamespace(scalar=lambda: "default")

    class AsyncConnection:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_value, traceback):
            return None

        async def run_sync(self, callback):
            callback(SyncConnection())

    class AsyncEngine:
        def connect(self):
            return AsyncConnection()

        dispose = AsyncMock()

    async_engine = AsyncEngine()
    with (
        patch.object(alembic_context, "config", config, create=True),
        patch.object(alembic_context, "is_offline_mode", return_value=True),
        patch.object(alembic_context, "configure", configure),
        patch.object(alembic_context, "begin_transaction", return_value=nullcontext()),
        patch.object(alembic_context, "run_migrations", run_migrations),
        patch("sqlalchemy.ext.asyncio.async_engine_from_config", return_value=async_engine) as engine_factory,
    ):
        environment = runpy.run_path(str(env_path))
        offline_options = configure.call_args.kwargs
        assert offline_options["compare_server_default"] is True
        assert offline_options["include_object"] is ch_alembic.include_object
        assert offline_options["dialect_name"] == "clickhousedb"
        assert offline_options["version_table"] == "alembic_version"

        configure.reset_mock()
        run_in_new_loop(environment["run_migrations_online"]())

    engine_factory.assert_called_once_with(
        {"sqlalchemy.url": "clickhousedb+async://user:password@localhost:8123/default"},
        prefix="sqlalchemy.",
        poolclass=environment["pool"].NullPool,
    )
    online_options = configure.call_args.kwargs
    assert online_options["include_schemas"] is True
    assert online_options["compare_server_default"] is True
    assert online_options["include_object"] is ch_alembic.include_object
    assert online_options["process_revision_directives"] is ch_alembic.clickhouse_writer
    assert online_options["version_table"] == "alembic_version"
    assert online_options["include_name"]("default", "schema", {}) is True
    assert online_options["include_name"]("other", "schema", {}) is False
    async_engine.dispose.assert_awaited_once()
