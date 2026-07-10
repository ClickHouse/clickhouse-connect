from __future__ import annotations

import logging
from collections.abc import Generator, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timezone, tzinfo
from typing import Any, Protocol, TypeVar, cast, runtime_checkable
from zoneinfo import ZoneInfoNotFoundError

from clickhouse_connect import common
from clickhouse_connect.driver import tzutil
from clickhouse_connect.driver.backend.contracts import AsyncBackend, SyncBackend
from clickhouse_connect.driver.backend.models import Capabilities, ClientConfig, ServerInfo
from clickhouse_connect.driver.backend.operations import CommandOp, Operation, QueryOp, RawQueryOp
from clickhouse_connect.driver.common import version_at_least
from clickhouse_connect.driver.constants import CH_VERSION_WITH_PROTOCOL, PROTOCOL_VERSION_WITH_LOW_CARD
from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.driver.models import SettingDef, setting_status

logger = logging.getLogger(__name__)

ClientSettingWrite = tuple[str, Any]


@dataclass(frozen=True)
class InitializationResult:
    server_info: ServerInfo
    client_setting_writes: tuple[ClientSettingWrite, ...]
    protocol_version: int
    json_serialization_format: int | None
    timezone_dst_safe: bool
    apply_server_timezone: bool


InitializationSequence = Generator[Operation, object, InitializationResult]
SequenceResult = TypeVar("SequenceResult")
OperationSequence = Generator[Operation, object, SequenceResult]


@runtime_checkable
class _NamedResults(Protocol):
    def named_results(self) -> Iterable[Mapping[str, object]]: ...


def _version_timezone(result: object) -> tuple[str, str]:
    if not isinstance(result, Sequence) or isinstance(result, (str, bytes, bytearray)) or len(result) < 2:
        raise OperationalError(f"Unexpected response to server version query: {result!r}")
    version, server_timezone = result[0], result[1]
    if not isinstance(version, str) or not isinstance(server_timezone, str):
        raise OperationalError(f"Unexpected response to server version query: {result!r}")
    return version, server_timezone


def _setting_rows(result: object) -> Iterable[Mapping[str, object]]:
    if isinstance(result, _NamedResults):
        return result.named_results()
    if isinstance(result, Iterable) and not isinstance(result, (str, bytes, bytearray)):
        return cast(Iterable[Mapping[str, object]], result)
    raise OperationalError(f"Unexpected response to server settings query: {result!r}")


def _setting_definitions(result: object) -> dict[str, SettingDef]:
    definitions: dict[str, SettingDef] = {}
    for row in _setting_rows(result):
        if not isinstance(row, Mapping):
            raise OperationalError(f"Unexpected row in server settings query: {row!r}")
        try:
            name = row["name"]
            value = row["value"]
            readonly = row["readonly"]
        except KeyError as ex:
            raise OperationalError(f"Unexpected row in server settings query: {row!r}") from ex
        if not isinstance(name, str) or not isinstance(value, str) or not isinstance(readonly, int):
            raise OperationalError(f"Unexpected row in server settings query: {row!r}")
        setting = SettingDef(name=name, value=value, readonly=readonly)
        definitions[setting.name] = setting
    return definitions


def init_sequence(config: ClientConfig, capabilities: Capabilities) -> InitializationSequence:
    version_result = yield CommandOp("SELECT version(), timezone()", use_database=False)
    server_version, server_timezone_name = _version_timezone(version_result)

    server_timezone: tzinfo = timezone.utc
    timezone_dst_safe = True
    try:
        resolved_timezone = tzutil.resolve_zone(server_timezone_name)
        server_timezone, timezone_dst_safe = tzutil.normalize_timezone(resolved_timezone, trust_fixed_offset=True)
    except ZoneInfoNotFoundError:
        logger.warning(
            "Server timezone %s could not be resolved, falling back to UTC; %s",
            server_timezone_name,
            tzutil.TZDATA_HINT,
        )

    if config.timezone_policy == "auto":
        apply_server_timezone = timezone_dst_safe
    else:
        apply_server_timezone = config.timezone_policy == "server"
    if not apply_server_timezone and not tzutil.local_tz_dst_safe:
        logger.warning(
            "local timezone %s may return unexpected times due to Daylight Savings Time/Summer Time differences",
            tzutil.local_tz.tzname(None),
        )

    readonly = "readonly" if version_at_least(server_version, "19.17") else str(common.get_setting("readonly"))
    settings_result = yield QueryOp(f"SELECT name, value, {readonly} as readonly FROM system.settings LIMIT 10000")
    server_settings = _setting_definitions(settings_result)

    protocol_version = 0
    if version_at_least(server_version, CH_VERSION_WITH_PROTOCOL) and common.get_setting("use_protocol_version"):
        # Probe failures leave protocol_version at 0. Matches AsyncClient._initialize;
        # Client._init_common_settings currently propagates them.
        try:
            protocol_result = yield RawQueryOp(
                "SELECT 1 AS check",
                settings={"client_protocol_version": PROTOCOL_VERSION_WITH_LOW_CARD},
                fmt="Native",
            )
            if isinstance(protocol_result, (bytes, bytearray)) and protocol_result[8:16] == b"\x01\x01\x05check":
                protocol_version = PROTOCOL_VERSION_WITH_LOW_CARD
        except Exception:
            pass

    client_settings: dict[str, Any] = {}
    if setting_status(server_settings, "date_time_input_format").is_writable:
        client_settings["date_time_input_format"] = "best_effort"
    if (
        setting_status(server_settings, "allow_experimental_json_type").is_set
        and setting_status(server_settings, "cast_string_to_dynamic_use_inference").is_writable
    ):
        client_settings["cast_string_to_dynamic_use_inference"] = "1"
    # User settings override generated defaults. Matches the sync client;
    # AsyncClient._initialize currently overwrites user values with the defaults.
    client_settings.update(config.settings)

    json_serialization_format = 0 if version_at_least(server_version, "24.8") and not version_at_least(server_version, "24.10") else None
    server_info = ServerInfo(
        version=server_version,
        timezone=server_timezone,
        settings=server_settings,
        capabilities=capabilities,
    )
    return InitializationResult(
        server_info=server_info,
        client_setting_writes=tuple(client_settings.items()),
        protocol_version=protocol_version,
        json_serialization_format=json_serialization_format,
        timezone_dst_safe=timezone_dst_safe,
        apply_server_timezone=apply_server_timezone,
    )


def run_sync(sequence: OperationSequence[SequenceResult], backend: SyncBackend) -> SequenceResult:
    response: object = None
    execution_error: Exception | None = None
    while True:
        try:
            if execution_error is None:
                operation = sequence.send(response)
            else:
                operation = sequence.throw(execution_error)
        except StopIteration as stop:
            return stop.value
        try:
            response = backend.execute(operation)
            execution_error = None
        except Exception as ex:
            execution_error = ex


async def run_async(sequence: OperationSequence[SequenceResult], backend: AsyncBackend) -> SequenceResult:
    response: object = None
    execution_error: Exception | None = None
    while True:
        try:
            if execution_error is None:
                operation = sequence.send(response)
            else:
                operation = sequence.throw(execution_error)
        except StopIteration as stop:
            return stop.value
        try:
            response = await backend.execute(operation)
            execution_error = None
        except Exception as ex:
            execution_error = ex
