"""
In-process chdb backend for clickhouse-connect.

ChdbClient implements the Client contract on top of the embedded ClickHouse engine
exposed by the `chdb` Python package. The same Native byte format that the HTTP
server emits is consumed verbatim, so all of clickhouse-connect's existing type,
dtype, and result conversion machinery is reused.
"""

from __future__ import annotations

import io
import logging
import os
import sys
import tempfile
import threading
from collections.abc import Generator, Sequence
from typing import TYPE_CHECKING, Any, BinaryIO

from clickhouse_connect import common
from clickhouse_connect.driver.binding import bind_query, quote_identifier
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.common import coerce_int
from clickhouse_connect.driver.ctypes import RespBuffCls
from clickhouse_connect.driver.exceptions import (
    DatabaseError,
    NotSupportedError,
    ProgrammingError,
)
from clickhouse_connect.driver.external import ExternalData
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext, QueryResult, TzMode, TzSource
from clickhouse_connect.driver.summary import QuerySummary
from clickhouse_connect.driver.transform import NativeTransform

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class _BytesSource:
    """
    Minimal stand-in for the HTTP `ResponseSource` that the response buffer
    expects. Yields a single chunk of bytes and exposes the attributes the
    transform layer reads.
    """

    __slots__ = ("data", "last_message", "exception_tag")

    def __init__(self, data: bytes):
        self.data = data
        self.last_message = None
        self.exception_tag = None

    @property
    def gen(self):
        def _gen():
            yield self.data

        return _gen()

    def close(self):
        return None


def _format_error_message(message: str) -> str:
    """Extract a clean ClickHouse exception message from a chdb error string."""
    if not message:
        return ""
    idx = message.find("Code: ")
    if idx > 0:
        return message[idx:].strip()
    return message.strip()


def _build_conn_string(chdb_path: str, chdb_options: dict[str, Any] | None) -> str:
    path = chdb_path or ":memory:"
    if not chdb_options:
        return path
    from urllib.parse import urlencode

    query = urlencode({k: str(v) for k, v in chdb_options.items()})
    sep = "&" if "?" in path else "?"
    return f"{path}{sep}{query}"


class ChdbClient(Client):
    """ClickHouse Connect client backed by the in-process chdb engine."""

    # HTTP-style transport settings: accepted by setting validation but stripped
    # before being forwarded to chdb (they have no in-process equivalent).
    valid_transport_settings: set[str] = {
        "client_protocol_version",
        "session_id",
        "session_timeout",
        "session_check",
        "query_id",
        "quota_key",
        "compress",
        "decompress",
        "wait_end_of_query",
        "buffer_size",
        "role",
        "send_progress_in_http_headers",
        "http_headers_progress_interval_ms",
        "enable_http_compression",
    }

    def __init__(
        self,
        chdb_path: str = ":memory:",
        chdb_options: dict[str, Any] | None = None,
        database: str | None = None,
        settings: dict[str, Any] | None = None,
        query_limit: int = 0,
        tz_source: TzSource | None = None,
        tz_mode: TzMode | None = None,
        show_clickhouse_errors: bool | None = None,
        **ignored,
    ):
        if sys.platform.startswith("win"):
            raise NotSupportedError("chdb backend is not supported on Windows")

        import chdb

        self._chdb_path = chdb_path or ":memory:"
        self._chdb_options = dict(chdb_options) if chdb_options else {}
        self._connection_string = _build_conn_string(self._chdb_path, self._chdb_options)
        self._chdb_module = chdb
        self._conn = chdb.connect(self._connection_string)
        self._lock = threading.Lock()
        self._closed = False
        self._client_settings: dict[str, str] = {}
        self._initial_settings = dict(settings or {})
        self._read_format = "Native"
        self._write_format = "Native"
        self._transform = NativeTransform()
        self._integration_libs: set[str] = set()
        self.uri = f"chdb://{self._chdb_path}"
        self.write_compression = None
        self.compression = None

        # coerce_int handles None-or-string flexibility
        super().__init__(
            database=database,
            uri=self.uri,
            query_limit=coerce_int(query_limit),
            query_retries=0,
            server_host_name=None,
            tz_source=tz_source,
            tz_mode=tz_mode,
            show_clickhouse_errors=show_clickhouse_errors,
            autoconnect=True,
        )

        for k, v in self._initial_settings.items():
            self.set_client_setting(k, v)

        if self.database:
            self._exec_raw_query(f"USE {quote_identifier(self.database)}")

        logger.info(
            "ChdbClient connected: chdb=%s, server_version=%s, path=%s",
            getattr(chdb, "__version__", "?"),
            self.server_version,
            self._chdb_path,
        )

    # ---- helpers -------------------------------------------------------

    @property
    def chdb_connection(self):
        """Underlying chdb connection. Escape hatch for advanced users."""
        return self._conn

    def _ensure_open(self) -> None:
        if self._closed:
            raise ProgrammingError("ChdbClient is closed") from None

    def _filter_per_call_settings(self, settings: dict[str, Any] | None) -> dict[str, str]:
        """Validate per-call settings and drop transport-only ones."""
        out: dict[str, str] = {}
        if not settings:
            return out
        invalid_action = common.get_setting("invalid_setting_action")
        for k, v in settings.items():
            str_v = self._validate_setting(k, v, invalid_action)
            if str_v is None:
                continue
            if k in self.valid_transport_settings:
                continue
            out[k] = str_v
        return out

    def _append_settings_clause(self, sql: str, settings: dict[str, str]) -> str:
        if not settings:
            return sql
        extras = ", ".join(f"{k} = {v}" for k, v in settings.items())
        if " SETTINGS " in sql.upper():
            return f"{sql}, {extras}"
        return f"{sql} SETTINGS {extras}"

    def _persist_setting(self, key: str, value: str) -> None:
        """Apply a setting to the underlying chdb session via SET."""
        try:
            with self._lock:
                self._conn.query(f"SET {key} = {value}", "TabSeparated")
        except Exception as ex:  # noqa: BLE001
            logger.debug("Failed to apply SET %s=%s to chdb session: %s", key, value, ex)

    def _snapshot_settings(self, keys: Sequence[str]) -> dict[str, tuple[str, bool]]:
        """Read current value and 'changed' flag for each key from system.settings.

        Returns a dict: {name -> (value, was_explicitly_set)}.
        """
        if not keys:
            return {}
        quoted = ", ".join(f"'{k}'" for k in keys)
        body = self._exec_raw_query(
            f"SELECT name, value, changed FROM system.settings WHERE name IN ({quoted})",
            "TabSeparated",
        )
        result: dict[str, tuple[str, bool]] = {}
        if body:
            for line in body.decode().rstrip("\n").split("\n"):
                parts = line.split("\t")
                if len(parts) == 3:
                    name, value, changed = parts
                    result[name] = (value, changed == "1")
        return result

    def _restore_settings(self, snapshot: dict[str, tuple[str, bool]]) -> None:
        """Restore settings to the state captured by `_snapshot_settings`."""
        for name, (value, was_changed) in snapshot.items():
            try:
                if was_changed:
                    self._persist_setting(name, value)
                else:
                    with self._lock:
                        self._conn.query(f"SET {name} = DEFAULT", "TabSeparated")
            except Exception:  # noqa: BLE001
                logger.debug("Failed to restore setting %s after command()", name, exc_info=True)

    @staticmethod
    def _strip_param_prefix(bind_params: dict[str, Any]) -> dict[str, Any]:
        """chdb's `params` kwarg expects bare names (`x`); bind_query produces `param_x`."""
        return {(k[6:] if k.startswith("param_") else k): v for k, v in bind_params.items()} if bind_params else {}

    def _exec_raw_query(self, sql: str, fmt: str = "Native", params: dict[str, Any] | None = None) -> bytes:
        """Run a query against chdb under the per-client lock and return raw bytes."""
        self._ensure_open()
        with self._lock:
            try:
                result = self._conn.query(sql, fmt, params=params or {})
            except Exception as ex:  # noqa: BLE001
                raise self._wrap_exception(ex) from ex
            return result.bytes() if hasattr(result, "bytes") else bytes(result)

    def _wrap_exception(self, ex: Exception) -> Exception:
        message = _format_error_message(str(ex))
        if not self.show_clickhouse_errors:
            message = "ClickHouse error"
        return DatabaseError(message)

    def _format_for_command(self) -> str:
        return "TabSeparated"

    # ---- abstract method implementations -------------------------------

    def set_client_setting(self, key: str, value: Any) -> None:
        str_value = self._validate_setting(key, value, common.get_setting("invalid_setting_action"))
        if str_value is None:
            return
        self._client_settings[key] = str_value
        if key in self.valid_transport_settings:
            return
        self._persist_setting(key, str_value)

    def get_client_setting(self, key: str) -> str | None:
        return self._client_settings.get(key)

    def set_access_token(self, access_token: str) -> None:
        # chdb has no auth concept; accept silently for HTTP-mode drop-in compatibility.
        return None

    def _query_with_context(self, context: QueryContext) -> QueryResult:
        self._ensure_open()
        if context.external_data is not None:
            raise NotSupportedError("external_data is not supported by the chdb backend")
        # chdb's Native output does not include the 8-byte block_info prefix that the
        # HTTP server emits when client_protocol_version is set.
        context.block_info = False
        final_query = self._prep_query(context)
        if isinstance(final_query, bytes):
            final_query = final_query.decode()
        params = self._strip_param_prefix(context.bind_params)
        if context.is_insert:
            # INSERT ... VALUES carries its data inline and has no result block to parse;
            # appending `FORMAT Native` to a VALUES statement is a syntax error.
            sql = self._append_settings_clause(final_query, self._filter_per_call_settings(context.settings))
            self._exec_raw_query(sql, "TabSeparated", params=params)
            return QueryResult([])
        sql = f"{final_query}\n FORMAT Native"
        sql = self._append_settings_clause(sql, self._filter_per_call_settings(context.settings))
        data = self._exec_raw_query(sql, "Native", params=params)
        byte_source = RespBuffCls(_BytesSource(data))
        query_result = self._transform.parse_response(byte_source, context)
        query_result.summary = {}
        return query_result

    def raw_query(
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        fmt: str | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> bytes:
        if external_data is not None:
            raise NotSupportedError("external_data is not supported by the chdb backend")
        final_query, bound = bind_query(query, parameters, self.server_tz)
        if isinstance(final_query, bytes):
            final_query = final_query.decode()
        final_query = self._append_settings_clause(final_query, self._filter_per_call_settings(settings))
        return self._exec_raw_query(final_query, fmt or "Native", params=self._strip_param_prefix(bound))

    def raw_stream(
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        fmt: str | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> io.IOBase:
        if external_data is not None:
            raise NotSupportedError("external_data is not supported by the chdb backend")
        final_query, bound = bind_query(query, parameters, self.server_tz)
        if isinstance(final_query, bytes):
            final_query = final_query.decode()
        final_query = self._append_settings_clause(final_query, self._filter_per_call_settings(settings))
        params = self._strip_param_prefix(bound)
        self._ensure_open()
        # Acquire the lock for the lifetime of the streaming read so concurrent
        # callers don't interleave queries on the same chdb connection.
        self._lock.acquire()
        try:
            streaming = self._conn.send_query(final_query, fmt or "Native", params=params or {})
        except Exception as ex:  # noqa: BLE001
            self._lock.release()
            raise self._wrap_exception(ex) from ex
        return _ChdbStreamFile(streaming, self._lock)

    def command(
        self,
        cmd: str,
        parameters: Sequence | dict[str, Any] | None = None,
        data: str | bytes | None = None,
        settings: dict[str, Any] | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> str | int | Sequence[str] | QuerySummary:
        if external_data is not None:
            raise NotSupportedError("external_data is not supported by the chdb backend")
        cmd, bound = bind_query(cmd, parameters, self.server_tz)
        if isinstance(cmd, bytes):
            cmd = cmd.decode()
        params = self._strip_param_prefix(bound)
        if data is not None:
            if isinstance(data, bytes):
                data_str = data.decode()
            else:
                data_str = data
            cmd = f"{cmd}\n{data_str}"
        per_call = self._filter_per_call_settings(settings)
        # ClickHouse DDL doesn't accept a SETTINGS clause; apply per-call settings to
        # the chdb session via SET before running the command, then restore them
        # afterwards so they don't leak into the session.
        snapshot: dict[str, tuple[str, bool]] = {}
        if per_call:
            snapshot = self._snapshot_settings(list(per_call.keys()))
            for k, v in per_call.items():
                self._persist_setting(k, v)
        try:
            body = self._exec_raw_query(cmd, self._format_for_command(), params=params)
        finally:
            if snapshot:
                self._restore_settings(snapshot)
        if not body:
            return QuerySummary({})
        try:
            text = body.decode()
        except UnicodeDecodeError:
            return str(body)
        # Match HTTP client semantics: strip trailing newline, split by tab, single
        # token tries to coerce to int.
        if text.endswith("\n"):
            text = text[:-1]
        result = text.split("\t")
        if len(result) == 1:
            try:
                return int(result[0])
            except ValueError:
                return result[0]
        return result

    def ping(self) -> bool:
        try:
            self._exec_raw_query("SELECT 1", "TabSeparated")
            return True
        except Exception:  # noqa: BLE001
            logger.debug("chdb ping failed", exc_info=True)
            return False

    def data_insert(self, context: InsertContext) -> QuerySummary:
        if context.empty:
            return QuerySummary()
        return self._insert_via_infile(context)

    def raw_insert(
        self,
        table: str | None = None,
        column_names: Sequence[str] | None = None,
        insert_block: str | bytes | Generator[bytes, None, None] | BinaryIO | None = None,
        settings: dict[str, Any] | None = None,
        fmt: str | None = None,
        compression: str | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> QuerySummary:
        if insert_block is None or not table:
            raise ProgrammingError("raw_insert requires a table and insert_block")
        if compression:
            raise NotSupportedError("compression is not supported for raw_insert in chdb mode. Provide uncompressed bytes.")

        fmt = fmt or self._write_format
        cols = ""
        if column_names:
            cols = f" ({', '.join(quote_identifier(c) for c in column_names)})"

        # Drain insert_block to a temp file, then INSERT FROM INFILE.
        tmp = tempfile.NamedTemporaryFile(suffix=f".{fmt.lower()}", delete=False)
        try:
            try:
                if isinstance(insert_block, (bytes, bytearray, memoryview)):
                    tmp.write(bytes(insert_block))
                elif isinstance(insert_block, str):
                    tmp.write(insert_block.encode())
                elif hasattr(insert_block, "to_pybytes"):
                    # pyarrow.Buffer and friends — buffer protocol holder
                    tmp.write(insert_block.to_pybytes())
                elif hasattr(insert_block, "read"):
                    while True:
                        chunk = insert_block.read(1 << 20)
                        if not chunk:
                            break
                        tmp.write(chunk if isinstance(chunk, (bytes, bytearray)) else chunk.encode())
                else:
                    for chunk in insert_block:
                        tmp.write(chunk if isinstance(chunk, (bytes, bytearray)) else chunk.encode())
            finally:
                tmp.close()

            sql = f"INSERT INTO {table}{cols} FROM INFILE '{tmp.name}' FORMAT {fmt}"
            sql = self._append_settings_clause(sql, self._filter_per_call_settings(settings))
            self._exec_raw_query(sql, "TabSeparated")
            return QuerySummary({})
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

    def close(self) -> None:
        if self._closed:
            return
        try:
            with self._lock:
                self._conn.close()
        except Exception:  # noqa: BLE001
            logger.debug("Error closing chdb connection", exc_info=True)
        self._closed = True

    def close_connections(self) -> None:
        # chdb only has a single embedded connection per client.
        self.close()

    # ---- insert implementations ----------------------------------------

    def _insert_via_infile(self, context: InsertContext) -> QuerySummary:
        tmp = tempfile.NamedTemporaryFile(suffix=".native", delete=False)
        try:
            try:
                first_chunk = True
                # NativeTransform.build_insert prepends an `INSERT INTO ... FORMAT Native\n`
                # statement to the first chunk for the HTTP request body. We're going to
                # write only the Native bytes to a file and INSERT FROM INFILE, so the
                # prefix must be skipped.
                for chunk in self._transform.build_insert(context):
                    if context.insert_exception is not None:
                        ex = context.insert_exception
                        context.insert_exception = None
                        raise ex
                    if first_chunk:
                        nl = chunk.find(b"\n")
                        if nl >= 0:
                            chunk = chunk[nl + 1 :]
                        first_chunk = False
                    tmp.write(chunk)
            finally:
                tmp.close()

            cols = ", ".join(quote_identifier(c) for c in context.column_names)
            sql = f"INSERT INTO {context.table} ({cols}) FROM INFILE '{tmp.name}' FORMAT Native"
            sql = self._append_settings_clause(sql, self._filter_per_call_settings(context.settings))
            self._exec_raw_query(sql, "TabSeparated")
            return QuerySummary({})
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
            context.data = None

    # ---- integration tagging ------------------------------------------

    def _add_integration_tag(self, name: str) -> None:
        # No User-Agent header to update for in-process chdb; just record for
        # potential future use.
        self._integration_libs.add(name)


class _ChdbStreamFile(io.RawIOBase):
    """
    File-like adapter wrapping chdb's StreamingResult iterator so callers in
    clickhouse-connect (which expect an io.IOBase / aiohttp-style stream) can
    iterate bytes block-by-block.

    Holds a per-client lock for its lifetime so the chdb connection is not used
    concurrently by another caller while a stream is in flight.
    """

    def __init__(self, streaming_result, lock: threading.Lock):
        super().__init__()
        self._sr = streaming_result
        self._lock = lock
        self._buf = b""
        self._eof = False
        self._closed_flag = False

    def readable(self) -> bool:
        return True

    def _pull(self) -> bytes:
        while True:
            try:
                chunk = next(self._sr)
            except StopIteration:
                self._eof = True
                return b""
            payload = chunk.bytes() if hasattr(chunk, "bytes") else bytes(chunk)
            if payload:
                return payload

    def read(self, size: int | None = -1) -> bytes:
        if self._closed_flag:
            return b""
        if size is None or size < 0:
            parts = [self._buf]
            self._buf = b""
            while not self._eof:
                chunk = self._pull()
                if not chunk:
                    break
                parts.append(chunk)
            return b"".join(parts)
        while len(self._buf) < size and not self._eof:
            chunk = self._pull()
            if not chunk:
                break
            self._buf += chunk
        if not self._buf:
            return b""
        out = self._buf[:size]
        self._buf = self._buf[size:]
        return out

    def readinto(self, buf) -> int:
        data = self.read(len(buf))
        n = len(data)
        if n:
            buf[:n] = data
        return n

    def close(self) -> None:
        if self._closed_flag:
            return
        self._closed_flag = True
        try:
            close = getattr(self._sr, "close", None)
            if close:
                close()
        except Exception:  # noqa: BLE001
            logger.debug("Error closing chdb StreamingResult", exc_info=True)
        finally:
            try:
                self._lock.release()
            except RuntimeError:
                pass
            super().close()
