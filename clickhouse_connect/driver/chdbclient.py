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
import uuid
from collections.abc import Generator, Sequence
from typing import TYPE_CHECKING, Any, BinaryIO

from clickhouse_connect import common
from clickhouse_connect.driver import options
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

# HTTP-only kwargs accepted (and ignored) so users can switch interface without
# editing the rest of their connection config.
_HTTP_ONLY_KWARGS = frozenset(
    {
        "compress",
        "compression",
        "connect_timeout",
        "send_receive_timeout",
        "client_name",
        "verify",
        "ca_cert",
        "client_cert",
        "client_cert_key",
        "session_id",
        "pool_mgr",
        "http_proxy",
        "https_proxy",
        "tls_mode",
        "proxy_path",
        "form_encode_query_params",
        "rename_response_column",
        "autogenerate_session_id",
        "autogenerate_query_id",
        "connector_limit",
        "connector_limit_per_host",
        "keepalive_timeout",
        "server_host_name",
    }
)


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


class _ChunkIterSource:
    """Source backed by an iterator of byte chunks, used for streaming reads."""

    __slots__ = ("_chunks", "last_message", "exception_tag")

    def __init__(self, chunks):
        self._chunks = iter(chunks)
        self.last_message = None
        self.exception_tag = None

    @property
    def gen(self):
        return self._chunks

    def close(self):
        try:
            close = getattr(self._chunks, "close", None)
            if close:
                close()
        except Exception:  # noqa: BLE001
            pass


# Module globals used to expose user-provided Python objects (DataFrames, PyArrow
# tables) to chdb's `Python(name)` table function. chdb walks frames and module
# globals looking for the bare name passed to `Python(...)`, so we register
# objects under a uuid-suffixed name and clean up afterwards.
_chdb_ref_lock = threading.Lock()


def _register_chdb_object(obj) -> str:
    name = f"_chdb_ref_{uuid.uuid4().hex}"
    with _chdb_ref_lock:
        globals()[name] = obj
    return name


def _unregister_chdb_object(name: str) -> None:
    with _chdb_ref_lock:
        globals().pop(name, None)


def _format_error_message(message: str) -> str:
    """Extract a clean ClickHouse exception message from a chdb error string."""
    if not message:
        return ""
    idx = message.find("Code: ")
    if idx > 0:
        return message[idx:].strip()
    return message.strip()


def _build_conn_string(chdb_path: str, chdb_options: dict[str, Any] | None) -> str:
    if not chdb_path or chdb_path in (":memory:", "memory"):
        path = ":memory:"
    elif chdb_path.startswith("file:"):
        return chdb_path
    else:
        path = chdb_path
    if not chdb_options:
        return path
    from urllib.parse import urlencode

    query = urlencode({k: str(v) for k, v in chdb_options.items()})
    if path == ":memory:":
        return f"file::memory:?{query}"
    return f"file:{path}?{query}"


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
    }
    optional_transport_settings: set[str] = {
        "send_progress_in_http_headers",
        "http_headers_progress_interval_ms",
        "enable_http_compression",
    }

    def __init__(
        self,
        chdb_path: str = ":memory:",
        chdb_options: dict[str, Any] | None = None,
        database: str = "__default__",
        settings: dict[str, Any] | None = None,
        query_limit: int = 0,
        query_retries: int = 0,
        tz_source: TzSource | None = None,
        tz_mode: TzMode | None = None,
        show_clickhouse_errors: bool | None = None,
        **ignored,
    ):
        if sys.platform.startswith("win"):
            raise NotSupportedError("chdb backend is not supported on Windows")

        try:
            import chdb
        except ImportError as ex:
            raise ImportError("chdb backend requires the chdb package. Install with: pip install 'clickhouse-connect[chdb]'") from ex

        for key in ignored:
            if key in _HTTP_ONLY_KWARGS:
                continue
            logger.warning("ChdbClient: ignoring unrecognized kwarg %r", key)

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
            query_retries=coerce_int(query_retries),
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
            if k in self.valid_transport_settings or k in self.optional_transport_settings:
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

    def _exec_raw_query(self, sql: str, fmt: str = "Native") -> bytes:
        """Run a query against chdb under the per-client lock and return raw bytes."""
        self._ensure_open()
        with self._lock:
            try:
                result = self._conn.query(sql, fmt)
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
        if key in self.valid_transport_settings or key in self.optional_transport_settings:
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
        if context.is_insert:
            # INSERT ... VALUES carries its data inline and has no result block to parse;
            # appending `FORMAT Native` to a VALUES statement is a syntax error.
            sql = self._append_settings_clause(final_query, self._filter_per_call_settings(context.settings))
            self._exec_raw_query(sql, "TabSeparated")
            return QueryResult([])
        sql = f"{final_query}\n FORMAT Native"
        sql = self._append_settings_clause(sql, self._filter_per_call_settings(context.settings))
        data = self._exec_raw_query(sql, "Native")
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
        final_query, _ = bind_query(query, parameters, self.server_tz)
        if isinstance(final_query, bytes):
            final_query = final_query.decode()
        if fmt:
            final_query = f"{final_query}\n FORMAT {fmt}"
        final_query = self._append_settings_clause(final_query, self._filter_per_call_settings(settings))
        return self._exec_raw_query(final_query, fmt or "Native")

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
        final_query, _ = bind_query(query, parameters, self.server_tz)
        if isinstance(final_query, bytes):
            final_query = final_query.decode()
        if fmt:
            final_query = f"{final_query}\n FORMAT {fmt}"
        final_query = self._append_settings_clause(final_query, self._filter_per_call_settings(settings))
        self._ensure_open()
        # Acquire the lock for the lifetime of the streaming read so concurrent
        # callers don't interleave queries on the same chdb connection.
        self._lock.acquire()
        try:
            streaming = self._conn.send_query(final_query, fmt or "Native")
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
        cmd, _ = bind_query(cmd, parameters, self.server_tz)
        if isinstance(cmd, bytes):
            cmd = cmd.decode()
        if data is not None:
            if isinstance(data, bytes):
                data_str = data.decode()
            else:
                data_str = data
            cmd = f"{cmd}\n{data_str}"
        per_call = self._filter_per_call_settings(settings)
        # ClickHouse DDL doesn't accept a SETTINGS clause; apply per-call settings to the
        # chdb session via SET before running the command. Client-level settings are
        # already applied at set time, so no extra work needed for them.
        for k, v in per_call.items():
            self._persist_setting(k, v)
        body = self._exec_raw_query(cmd, self._format_for_command())
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

        # DataFrame fast path: hand the DataFrame to chdb directly via the
        # `Python(name)` table function. This skips serialization and disk I/O.
        if self._can_use_dataframe_fast_path(context):
            df = context.data
            return self._insert_dataframe_fast(context, df)

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
                if isinstance(insert_block, (bytes, bytearray)):
                    tmp.write(bytes(insert_block))
                elif isinstance(insert_block, str):
                    tmp.write(insert_block.encode())
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

    def _can_use_dataframe_fast_path(self, context: InsertContext) -> bool:
        if options.pd is None:
            return False
        data = context.data
        if not isinstance(data, options.pd.DataFrame):
            return False
        return True

    def _insert_dataframe_fast(self, context: InsertContext, df) -> QuerySummary:
        # Reorder/rename DataFrame columns to match the target schema so the
        # `SELECT * FROM Python(df)` projection lines up with the destination.
        try:
            chdb_df = df[list(context.column_names)] if list(df.columns) != list(context.column_names) else df
        except KeyError as ex:
            raise ProgrammingError(f"DataFrame is missing target column {ex}") from None

        ref_name = _register_chdb_object(chdb_df)
        try:
            sql = (
                f"INSERT INTO {context.table} ({', '.join(quote_identifier(c) for c in context.column_names)}) "
                f"SELECT * FROM Python({ref_name})"
            )
            sql = self._append_settings_clause(sql, self._filter_per_call_settings(context.settings))
            self._exec_raw_query(sql, "TabSeparated")
        finally:
            _unregister_chdb_object(ref_name)
            context.data = None
        return QuerySummary({})

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
