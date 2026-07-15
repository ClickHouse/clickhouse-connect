from __future__ import annotations

import asyncio
import operator
from collections.abc import Iterable
from datetime import timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.backend.models import ClientConfig, ServerInfo
from clickhouse_connect.driver.backend.operations import CommandOp, Operation, QueryOp, RawQueryOp
from clickhouse_connect.driver.backend.orchestration import (
    InitializationResult,
    init_sequence,
    insert_context_sequence,
    run_async,
    run_sync,
)
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.constants import PROTOCOL_VERSION_WITH_LOW_CARD
from clickhouse_connect.driver.exceptions import OperationalError, ProgrammingError
from clickhouse_connect.driver.models import SettingDef

PROTOCOL_RESPONSE = b"\x00" * 8 + b"\x01\x01\x05check"


class FakeSyncExecutor:
    """Scripted stand-in for the client's _execute_operation method."""

    def __init__(self, responses: Iterable[object]):
        self._responses = iter(responses)
        self.operations: list[Operation] = []

    def execute(self, operation: Operation) -> object:
        self.operations.append(operation)
        response = next(self._responses)
        if isinstance(response, Exception):
            raise response
        return response


class FakeAsyncExecutor:
    """Scripted stand-in for the async client's _execute_operation method."""

    def __init__(self, responses: Iterable[object]):
        self._responses = iter(responses)
        self.operations: list[Operation] = []

    async def execute(self, operation: Operation) -> object:
        self.operations.append(operation)
        response = next(self._responses)
        if isinstance(response, Exception):
            raise response
        return response


def _server_settings(*, writable: bool = True) -> list[dict[str, object]]:
    readonly = 0 if writable else 1
    return [
        {"name": "date_time_input_format", "value": "basic", "readonly": readonly},
        {"name": "allow_experimental_json_type", "value": "1", "readonly": 0},
        {"name": "cast_string_to_dynamic_use_inference", "value": "0", "readonly": readonly},
        {"name": "max_threads", "value": "8", "readonly": 0},
    ]


def _run_both(
    responses: Iterable[object], config: ClientConfig | None = None
) -> tuple[InitializationResult, FakeSyncExecutor, FakeAsyncExecutor]:
    script = tuple(responses)
    config = config or ClientConfig()
    sync_backend = FakeSyncExecutor(script)
    async_backend = FakeAsyncExecutor(script)

    sync_result = run_sync(init_sequence(config), sync_backend.execute)
    async_result = asyncio.run(run_async(init_sequence(config), async_backend.execute))

    assert sync_backend.operations == async_backend.operations
    assert sync_result == async_result
    return sync_result, sync_backend, async_backend


def test_sync_and_async_init_sequences_have_identical_operations_and_results():
    responses = [("25.6.3.116", "UTC"), _server_settings(), PROTOCOL_RESPONSE]
    config = ClientConfig(settings={"max_threads": 13}, timezone_policy="server")
    result, sync_backend, async_backend = _run_both(responses, config)

    expected_operations: list[Operation] = [
        CommandOp("SELECT version(), timezone()", use_database=False),
        QueryOp("SELECT name, value, readonly as readonly FROM system.settings LIMIT 10000"),
        RawQueryOp(
            "SELECT 1 AS check",
            settings={"client_protocol_version": PROTOCOL_VERSION_WITH_LOW_CARD},
            fmt="Native",
        ),
    ]
    assert sync_backend.operations == expected_operations
    assert async_backend.operations == expected_operations
    assert result.protocol_version == PROTOCOL_VERSION_WITH_LOW_CARD
    assert result.client_setting_writes == (
        ("date_time_input_format", "best_effort"),
        ("cast_string_to_dynamic_use_inference", "1"),
    )


def test_unresolvable_server_timezone_falls_back_to_utc():
    result, _, _ = _run_both([("25.6.3.116", "Not/A-Timezone"), _server_settings(), PROTOCOL_RESPONSE])

    assert result.server_info.timezone is timezone.utc
    assert result.timezone_dst_safe
    assert result.apply_server_timezone


def test_old_server_skips_protocol_probe_and_uses_legacy_readonly_setting():
    result, sync_backend, async_backend = _run_both([("19.16.9.12", "UTC"), _server_settings()])

    expected_operations: list[Operation] = [
        CommandOp("SELECT version(), timezone()", use_database=False),
        QueryOp("SELECT name, value, 0 as readonly FROM system.settings LIMIT 10000"),
    ]
    assert sync_backend.operations == expected_operations
    assert async_backend.operations == expected_operations
    assert result.protocol_version == 0


def test_unwritable_default_settings_are_skipped():
    result, _, _ = _run_both([("25.6.3.116", "UTC"), _server_settings(writable=False), PROTOCOL_RESPONSE])

    assert result.client_setting_writes == ()


def test_dynamic_json_server_range_is_returned_as_local_state():
    result, _, _ = _run_both([("24.8.12.28", "UTC"), _server_settings(), PROTOCOL_RESPONSE])

    assert result.json_serialization_format == 0


def test_protocol_probe_errors_are_handled_identically():
    result, _, _ = _run_both([("25.6.3.116", "UTC"), _server_settings(), RuntimeError("probe failed")])

    assert result.protocol_version == 0


def test_fatal_errors_propagate_identically_through_both_drivers():
    responses = [OperationalError("connection refused")]
    config = ClientConfig()

    with pytest.raises(OperationalError, match="connection refused"):
        run_sync(init_sequence(config), FakeSyncExecutor(responses).execute)
    with pytest.raises(OperationalError, match="connection refused"):
        asyncio.run(run_async(init_sequence(config), FakeAsyncExecutor(responses).execute))


@pytest.mark.parametrize(
    "user_settings,expected_writes",
    [
        (
            {"date_time_input_format": "basic", "cast_string_to_dynamic_use_inference": "0"},
            (),
        ),
        (
            {"date_time_input_format": "basic"},
            (("cast_string_to_dynamic_use_inference", "1"),),
        ),
        (
            {"cast_string_to_dynamic_use_inference": "0"},
            (("date_time_input_format", "best_effort"),),
        ),
    ],
)
def test_user_settings_suppress_generated_defaults(user_settings, expected_writes):
    config = ClientConfig(settings=user_settings)

    result, _, _ = _run_both([("25.6.3.116", "UTC"), _server_settings(), PROTOCOL_RESPONSE], config)

    assert result.client_setting_writes == expected_writes


def _describe_rows() -> list[dict[str, object]]:
    def row(name: str, type_name: str, default_type: str = "") -> dict[str, object]:
        return {
            "name": name,
            "type": type_name,
            "default_type": default_type,
            "default_expression": "",
            "comment": "",
            "codec_expression": "",
            "ttl_expression": "",
        }

    return [
        row("user_id", "UInt32"),
        row("label", "String"),
        row("computed", "UInt32", default_type="ALIAS"),
        row("derived", "String", default_type="MATERIALIZED"),
    ]


@pytest.mark.parametrize("column_names", [None, "*"])
def test_insert_context_sequence_describes_table_identically(column_names):
    sync_backend = FakeSyncExecutor([_describe_rows()])
    async_backend = FakeAsyncExecutor([_describe_rows()])

    sync_context = run_sync(insert_context_sequence("target_table", column_names=column_names), sync_backend.execute)
    async_context = asyncio.run(run_async(insert_context_sequence("target_table", column_names=column_names), async_backend.execute))

    assert sync_backend.operations == async_backend.operations == [QueryOp("DESCRIBE TABLE `target_table`")]
    for context in (sync_context, async_context):
        assert context.table == "`target_table`"
        assert context.column_names == ["user_id", "label"]
        assert [t.name for t in context.column_types] == ["UInt32", "String"]


def test_insert_context_sequence_rejects_empty_column_list():
    with pytest.raises(ValueError, match="Column names must be specified"):
        run_sync(insert_context_sequence("target_table"), FakeSyncExecutor([[]]).execute)


def test_insert_context_sequence_describe_errors_propagate_through_both_drivers():
    responses = [OperationalError("table does not exist")]

    with pytest.raises(OperationalError, match="table does not exist"):
        run_sync(insert_context_sequence("target_table"), FakeSyncExecutor(responses).execute)
    with pytest.raises(OperationalError, match="table does not exist"):
        asyncio.run(run_async(insert_context_sequence("target_table"), FakeAsyncExecutor(responses).execute))


def test_insert_context_sequence_skips_describe_with_explicit_types():
    backend = FakeSyncExecutor([])
    context = run_sync(
        insert_context_sequence("db2.target_table", column_names=["user_id"], column_type_names=["UInt32"]),
        backend.execute,
    )

    assert backend.operations == []
    assert context.table == "db2.target_table"
    assert [t.name for t in context.column_types] == ["UInt32"]


def test_insert_context_sequence_rejects_unknown_column():
    with pytest.raises(ProgrammingError, match="Unrecognized column"):
        run_sync(
            insert_context_sequence("target_table", column_names=["missing_col"]),
            FakeSyncExecutor([_describe_rows()]).execute,
        )


def test_client_execute_operation_dispatch():
    client = Mock()
    result = Client._execute_operation(client, CommandOp("SELECT version()", use_database=False))
    assert result is client.command.return_value
    client.command.assert_called_once_with("SELECT version()", settings=None, use_database=False)
    Client._execute_operation(client, QueryOp("SELECT 1", settings={"max_threads": "4"}))
    client.query.assert_called_once_with("SELECT 1", settings={"max_threads": "4"}, query_formats={"String": "string"})
    Client._execute_operation(client, RawQueryOp("SELECT 1", fmt="Native"))
    client.raw_query.assert_called_once_with("SELECT 1", settings=None, fmt="Native")
    with pytest.raises(TypeError, match="Unsupported operation type"):
        Client._execute_operation(client, SimpleNamespace(settings={}))


def test_async_client_execute_operation_dispatch():
    client = AsyncMock()
    result = asyncio.run(AsyncClient._execute_operation(client, CommandOp("SELECT version()", use_database=False)))
    assert result is client.command.return_value
    client.command.assert_awaited_once_with("SELECT version()", settings=None, use_database=False)
    asyncio.run(AsyncClient._execute_operation(client, QueryOp("SELECT 1", settings={"max_threads": "4"})))
    client.query.assert_awaited_once_with("SELECT 1", settings={"max_threads": "4"}, query_formats={"String": "string"})
    asyncio.run(AsyncClient._execute_operation(client, RawQueryOp("SELECT 1", fmt="Native")))
    client.raw_query.assert_awaited_once_with("SELECT 1", settings=None, fmt="Native")
    with pytest.raises(TypeError, match="Unsupported operation type"):
        asyncio.run(AsyncClient._execute_operation(client, SimpleNamespace(settings={})))


def test_backend_values_copy_and_protect_settings_mappings():
    config_settings = {"max_threads": 13}
    operation_settings = {"client_protocol_version": PROTOCOL_VERSION_WITH_LOW_CARD}
    server_settings = {"max_threads": SettingDef("max_threads", "8", 0)}
    config = ClientConfig(settings=config_settings)
    operation = RawQueryOp("SELECT 1", settings=operation_settings)
    server_info = ServerInfo("25.6.3.116", timezone.utc, server_settings)

    config_settings["max_threads"] = 79
    operation_settings["client_protocol_version"] = 0
    server_settings.clear()

    assert config.settings == {"max_threads": 13}
    assert operation.settings == {"client_protocol_version": PROTOCOL_VERSION_WITH_LOW_CARD}
    assert server_info.settings == {"max_threads": SettingDef("max_threads", "8", 0)}
    with pytest.raises(TypeError):
        operator.setitem(operation.settings, "client_protocol_version", 0)
