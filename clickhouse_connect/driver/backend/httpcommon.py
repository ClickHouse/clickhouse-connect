"""HTTP semantics shared by the sync (urllib3) and async (aiohttp) transports.

Everything here is transport-library neutral: pure functions over response
headers, bodies, and client configuration that were previously duplicated
between httpclient.py and asyncclient.py.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import re
import zlib
from collections.abc import Mapping, Sequence
from importlib import import_module
from importlib.metadata import version as dist_version
from typing import TYPE_CHECKING, Any, Protocol

import lz4.frame
import zstandard

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

from clickhouse_connect import common
from clickhouse_connect.driver.binding import quote_identifier
from clickhouse_connect.driver.common import coerce_bool
from clickhouse_connect.driver.compression import available_compression
from clickhouse_connect.driver.exceptions import (
    DatabaseError,
    OperationalError,
    ProgrammingError,
    error_code_from_header,
    error_name_from_body,
)

logger = logging.getLogger(__name__)

ex_header = "X-ClickHouse-Exception-Code"
ex_tag_header = "X-ClickHouse-Exception-Tag"
auth_failed_ex_code = "516"  # ClickHouse AUTHENTICATION_FAILED
retryable_http_statuses = (429, 503, 504)

columns_only_re = re.compile(r"LIMIT 0\s*$", re.IGNORECASE)

if "br" in available_compression:
    import brotli
else:
    brotli = None


def summary_from_headers(headers: Mapping[str, str]) -> dict[str, Any]:
    """Extract the query summary from ClickHouse response headers."""
    summary = {}
    if "X-ClickHouse-Summary" in headers:
        try:
            summary = json.loads(headers["X-ClickHouse-Summary"])
        except json.JSONDecodeError:
            pass
    summary["query_id"] = headers.get("X-ClickHouse-Query-Id", "")
    return summary


def build_http_error(
    status: int,
    err_code: str | None,
    full_body: str,
    show_clickhouse_errors: bool,
    url: str,
    retried: bool,
) -> DatabaseError:
    """Build the exception for a failed HTTP response from its already-read body."""
    code = error_code_from_header(err_code)
    name = error_name_from_body(full_body) if show_clickhouse_errors else None
    body = ""
    try:
        body = common.format_error(full_body).strip()
    except Exception:
        logger.warning("Failed to format error response body", exc_info=True)

    if show_clickhouse_errors:
        if err_code:
            err_str = f"Received ClickHouse exception, code: {err_code}"
        else:
            err_str = f"HTTP driver received HTTP status {status}"
        if body:
            err_str = f"{err_str}, server response: {body}"
    else:
        err_str = "The ClickHouse server returned an error"

    err_str = f"{err_str} (for url {url})"
    err_type = OperationalError if retried else DatabaseError
    return err_type(err_str, code=code, name=name)


def parse_command_body(body: bytes) -> str | int | Sequence[str]:
    """Convert a non-empty command response body to the command return value."""
    try:
        result = body.decode()[:-1].split("\t")
        if len(result) == 1:
            try:
                return int(result[0])
            except ValueError:
                return result[0]
        return result
    except UnicodeDecodeError:
        return str(body)


def negotiate_compression(compress: bool | str) -> tuple[str | None, str | None]:
    """Resolve the compress constructor param to (accept_encoding, write_compression)."""
    if coerce_bool(compress):
        return ",".join(available_compression), available_compression[0]
    if compress and compress not in ("False", "false", "0"):
        if compress not in available_compression:
            raise ProgrammingError(f"Unsupported compression method {compress}")
        return compress, compress
    return None, None


def decompress_response(data: bytes, encoding: str | None) -> bytes:
    """Decompress a fully-read response body based on its Content-Encoding header."""
    if not encoding or encoding == "identity":
        return data

    if encoding == "lz4":
        lz4_decom = lz4.frame.LZ4FrameDecompressor()
        return lz4_decom.decompress(data, len(data))
    if encoding == "zstd":
        zstd_decom = zstandard.ZstdDecompressor()
        return zstd_decom.stream_reader(io.BytesIO(data)).read()
    if encoding == "br":
        if brotli is not None:
            return brotli.decompress(data)
        raise OperationalError("Brotli compression requested but not installed.")
    if encoding == "gzip":
        return gzip.decompress(data)
    if encoding == "deflate":
        return zlib.decompress(data)
    raise OperationalError(f"Unsupported compression type: '{encoding}'. Supported compression: {', '.join(available_compression)}")


def embed_insert_query(
    table: str, column_names: Sequence[str] | None, fmt: str, compression: str | None, insert_block: Any
) -> tuple[Any, str | None]:
    """Combine a raw insert query with its data block.

    Returns (body, query_param). String and bytes blocks get the INSERT
    statement prepended; generators, file-like objects, and compressed data
    keep the statement as a URL parameter and stream the body as-is.
    """
    cols = f" ({', '.join([quote_identifier(x) for x in column_names])})" if column_names is not None else ""
    query = f"INSERT INTO {table}{cols} FORMAT {fmt}"
    if not compression and isinstance(insert_block, str):
        return query + "\n" + insert_block, None
    if not compression and isinstance(insert_block, (bytes, bytearray)):
        return (query + "\n").encode() + insert_block, None
    return insert_block, query


class ProgressTransport(Protocol):
    """Transport slots for the progress-header keep-alive parameters."""

    send_progress: bool | None
    progress_interval: str | None


def apply_http_server_settings(client: Client, transport: ProgressTransport, compression: str | None, send_receive_timeout: int) -> None:
    """Apply HTTP-specific client setting defaults after server settings discovery.

    Sets the readonly-query cancel default (unless user-supplied), response
    compression, and the progress-header keep-alive parameters.
    """
    cancel_setting = client._setting_status("cancel_http_readonly_queries_on_client_close")
    if (
        cancel_setting.is_writable
        and not cancel_setting.is_set
        and "cancel_http_readonly_queries_on_client_close" not in (client._initial_settings or {})
    ):
        client.set_client_setting("cancel_http_readonly_queries_on_client_close", "1")
    comp_setting = client._setting_status("enable_http_compression")
    client._send_comp_setting = not comp_setting.is_set and comp_setting.is_writable
    if comp_setting.is_set or comp_setting.is_writable:
        client.compression = compression
    send_setting = client._setting_status("send_progress_in_http_headers")
    transport.send_progress = not send_setting.is_set and send_setting.is_writable
    if (send_setting.is_set or send_setting.is_writable) and client._setting_status("http_headers_progress_interval_ms").is_writable:
        transport.progress_interval = str(min(120000, max(10000, (send_receive_timeout - 5) * 1000)))


def add_integration_tag(headers: dict[str, str], reported_libs: set[str], name: str) -> str | None:
    """Add a product (like pandas or sqlalchemy) to the User-Agent details section.

    Mutates headers in place and returns the new User-Agent string when it changed.
    """
    if not common.get_setting("send_integration_tags") or name in reported_libs:
        return None

    try:
        ver = "unknown"
        try:
            ver = dist_version(name)
        except Exception:
            try:
                mod = import_module(name)
                ver = getattr(mod, "__version__", "unknown")
            except Exception:
                pass

        product_info = f"{name}/{ver}"

        ua = headers.get("User-Agent", "")
        start = ua.find("(")
        if start == -1:
            return None
        end = ua.find(")", start + 1)
        if end == -1:
            return None

        details = ua[start + 1 : end].strip()

        if product_info in details:
            reported_libs.add(name)
            return None

        new_details = f"{product_info}; {details}" if details else product_info
        new_ua = f"{ua[: start + 1]}{new_details}{ua[end:]}".strip()
        headers["User-Agent"] = new_ua

        reported_libs.add(name)
        logger.debug("Added '%s' to User-Agent", product_info)
        return new_ua

    except Exception as e:
        logger.debug("Problem adding '%s' to User-Agent: %s", name, e)
        return None
