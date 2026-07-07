import logging
from collections.abc import Generator
from typing import Any, Literal

from clickhouse_connect import common
from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.base import ch_read_formats, ch_write_formats
from clickhouse_connect.driver import options
from clickhouse_connect.driver.compression import get_compressor
from clickhouse_connect.driver.exceptions import NotSupportedError, ProgrammingError, StreamFailureError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.npquery import NumpyResult
from clickhouse_connect.driver.query import QueryContext, QueryResult
from clickhouse_connect.driver.rustnumpy import build_converters, convert_block
from clickhouse_connect.driver.streaming import ReadAheadSource
from clickhouse_connect.driver.transform import NativeTransform, Transform, extract_error_message, extract_exception_with_tag
from clickhouse_connect.driver.types import ByteSource

logger = logging.getLogger(__name__)

NativeCodec = Literal["python", "rust", "rust_strict"]

_VALID_CODECS = ("python", "rust", "rust_strict")
_rust_unavailable_warned = False


def _ch_core_module() -> Any:
    """Return the imported _ch_core module, or None if it is not available."""
    try:
        import _ch_core
    except ImportError:
        return None
    return _ch_core


def resolve_native_codec(native_codec: str | None) -> str:
    """Resolve the effective codec name, applying availability fallback rules."""
    if native_codec is None:
        resolved = common.get_setting("native_codec")
    else:
        resolved = native_codec.strip().lower()
    if resolved not in _VALID_CODECS:
        raise ProgrammingError(f"Invalid native_codec {native_codec!r}; expected one of {', '.join(_VALID_CODECS)}")
    if resolved == "python":
        return resolved
    if _ch_core_module() is not None:
        return resolved
    if resolved == "rust_strict":
        raise NotSupportedError(
            'native_codec="rust_strict" requires the compiled _ch_core module, which is not installed. '
            'Install a build that includes it (maturin build) or use native_codec="python".'
        )
    global _rust_unavailable_warned
    if not _rust_unavailable_warned:
        _rust_unavailable_warned = True
        logger.warning('native_codec="rust" requested but _ch_core is not available; using the Python codec')
    return "python"


def make_native_transform(native_codec: NativeCodec | None = None) -> Transform:
    """Build the Transform for the resolved codec."""
    resolved = resolve_native_codec(native_codec)
    if resolved == "python":
        return NativeTransform()
    return RustNativeTransform(strict=(resolved == "rust_strict"))


def rust_query_ineligible_reason(context: QueryContext) -> str | None:
    """Return a short reason the rust decoder cannot serve this query, or None if it can.

    column_oriented, streaming, block_info, column_renamer, numpy/pandas output, and
    settings/transport_settings/external_data are honored and not listed here. The columns-only LIMIT 0 branch is
    answered from FORMAT JSON metadata in both clients before any transform runs.
    """
    if context.use_numpy and options.arrow is None:
        # numpy and pandas output route through the zero-copy Arrow exit, which needs pyarrow.
        return "pyarrow not installed"
    if context.query_formats or context.column_formats:
        return "query/column formats"
    if ch_read_formats:
        return "global read format override"
    if not context.use_none:
        return "use_none=False"
    if context.encoding:
        return "custom encoding"
    if context.query_tz is not None or context.column_tzs:
        return "query_tz/column_tzs"
    if context.response_tz is not None:
        return "server timezone header"
    if context.tz_mode != "naive_utc":
        return "tz_mode"
    # With the checks above excluded, active_tz(None) is None exactly when bare DateTime renders naive-UTC
    # (query.py active_tz), the only ambient timezone behavior the rust decoder reproduces.
    if context.active_tz(None) is not None:
        return "ambient timezone"
    return None


class _ExceptionTagScanner:
    """Scans raw stream chunks for a complete tagged exception block, mirroring ResponseBuffer._check_for_exception."""

    def __init__(self, exception_tag: str):
        tag_bytes = exception_tag.encode()
        self._open_marker = b"__exception__" + tag_bytes
        self._close_marker = tag_bytes + b"__exception__"
        self._carryover = b""
        self._exception_buf: bytearray | None = None

    def push(self, chunk: bytes) -> bytes | None:
        if self._exception_buf is not None:
            self._exception_buf += chunk
            if self._close_marker in self._exception_buf:
                return bytes(self._exception_buf)
            return None
        search_data = self._carryover + chunk
        marker_pos = search_data.find(self._open_marker)
        if marker_pos != -1:
            self._exception_buf = bytearray(search_data[marker_pos:])
            if self._close_marker in self._exception_buf:
                return bytes(self._exception_buf)
            return None
        carry_size = len(self._open_marker) - 1
        if len(search_data) >= carry_size:
            self._carryover = search_data[-carry_size:]
        else:
            self._carryover = search_data
        return None


def _unsupported_decode_error(ex: Exception) -> NotSupportedError:
    return NotSupportedError(
        f'The rust native codec cannot decode this response: {ex}. Use native_codec="python" to fall back to the Python codec'
    )


def _chunk_has_server_error(chunk: bytes, exception_tag: str | None) -> bool:
    if not chunk:
        return False
    # On tag-capable servers (v25.11+) a real server error is caught by the scanner before decoding, so a
    # tagged block reaching here has no error. Only apply the byte heuristic for untagged servers, otherwise
    # block data containing b"Code: " would misclassify an unsupported-type error as a stream failure.
    if exception_tag:
        return bool(extract_exception_with_tag(chunk, exception_tag))
    return b"Code: " in chunk


class RustNativeTransform:
    """FORMAT Native codec backed by the compiled _ch_core module."""

    threaded_insert = True

    def __init__(self, strict: bool = False):
        self.strict = strict

    def parse_response(self, source: ByteSource, context: QueryContext) -> NumpyResult | QueryResult:
        reason = rust_query_ineligible_reason(context)
        if reason is not None:
            if self.strict:
                source.close()
                raise NotSupportedError(f'native_codec="rust_strict" does not support {reason}; use native_codec="python" or "rust"')
            logger.debug("Rust native decoder ineligible (%s); using the Python codec", reason)
            return NativeTransform.parse_response(source, context)

        core = _ch_core_module()
        if core is None:
            source.close()
            raise NotSupportedError('The rust native codec is unavailable (_ch_core not importable); use native_codec="python"')

        # Read-ahead: a daemon thread pulls transport chunks into a bounded queue so the network read overlaps
        # decode. The consumer generator re-raises producer errors verbatim, in stream order, so the exception
        # mapping, tag scanning, and last-chunk heuristics below run unchanged on the consuming thread.
        read_source = ReadAheadSource(source)
        decoder = core.StreamDecoder(has_block_info=context.block_info)
        exception_tag = read_source.exception_tag
        scanner = _ExceptionTagScanner(exception_tag) if exception_tag else None
        last_chunk = b""

        def raw_blocks() -> Generator[Any, None, None]:
            # Decode blocks lazily so streaming queries keep bounded memory. Errors surface here on the
            # consuming thread with the same mapping the Python codec uses; the source is closed before every raise.
            nonlocal last_chunk
            try:
                for chunk in read_source.gen:
                    last_chunk = chunk
                    if scanner is not None:
                        hit = scanner.push(chunk)
                        if hit is not None:
                            read_source.close()
                            raise StreamFailureError(extract_exception_with_tag(hit, exception_tag) or extract_error_message(hit))
                    yield from decoder.feed(chunk)
                yield from decoder.finish()
            except StreamFailureError:
                raise
            except EOFError as ex:
                read_source.close()
                if _chunk_has_server_error(last_chunk, exception_tag):
                    raise StreamFailureError(extract_error_message(last_chunk)) from ex
                raise StreamFailureError("Stream ended unexpectedly (connection closed by server)") from ex
            except ValueError as ex:
                read_source.close()
                if _chunk_has_server_error(last_chunk, exception_tag):
                    raise StreamFailureError(extract_error_message(last_chunk)) from ex
                raise _unsupported_decode_error(ex) from ex
            except Exception as ex:
                read_source.close()
                if _chunk_has_server_error(last_chunk, exception_tag):
                    raise StreamFailureError(extract_error_message(last_chunk)) from ex
                if ex.__class__.__name__ == "ClientPayloadError":
                    raise StreamFailureError("Stream failed during read (connection closed by server)") from ex
                raise

        blocks = raw_blocks()
        try:
            first = next(blocks)
        except StopIteration:
            read_source.close()
            return NumpyResult() if context.use_numpy else QueryResult([])

        renamer = context.column_renamer
        try:
            names = tuple(renamer(name) if renamer is not None else name for name in first.column_names)
            col_types = tuple(registry.get_from_name(type_name) for type_name in first.column_type_names)
            if context.use_numpy:
                converters = build_converters(col_types, context)
                first_columns = convert_block(first, converters)
            else:
                first_columns = first.to_python_columns()
        except NotImplementedError as ex:
            read_source.close()
            raise _unsupported_decode_error(ex) from ex
        except Exception:
            read_source.close()
            raise

        if context.use_numpy:
            d_types = [col.dtype if hasattr(col, "dtype") else "O" for col in first_columns]

            def np_block_gen() -> Generator[list, None, None]:
                yield first_columns
                for batch in blocks:
                    try:
                        columns = convert_block(batch, converters)
                    except NotImplementedError as ex:
                        read_source.close()
                        raise _unsupported_decode_error(ex) from ex
                    except Exception:
                        read_source.close()
                        raise
                    yield columns

            return NumpyResult(np_block_gen(), names, col_types, d_types, read_source)

        def block_gen() -> Generator[list, None, None]:
            yield first_columns
            for batch in blocks:
                try:
                    columns = batch.to_python_columns()
                except NotImplementedError as ex:
                    read_source.close()
                    raise _unsupported_decode_error(ex) from ex
                except Exception:
                    read_source.close()
                    raise
                yield columns

        return QueryResult(None, block_gen(), names, col_types, context.column_oriented, read_source)

    def build_insert(self, context: InsertContext) -> Generator[bytes, None, None]:
        core = _ch_core_module()
        if core is None:
            raise NotSupportedError('The rust native codec is unavailable (_ch_core not importable); use native_codec="python"')

        if ch_write_formats:
            # The rust encoder does not consult the global write-format registry, so per-value conversions
            # (e.g. set_write_format) would be ignored. Route these to the Python encoder.
            if self.strict:
                raise NotSupportedError(
                    'native_codec="rust_strict" does not support global write format overrides; use native_codec="python" or "rust"'
                )
            logger.debug("Global write format override set; using the Python codec")
            return NativeTransform.build_insert(context)

        if context.col_simple_formats or context.col_type_formats or context.type_formats:
            # The rust encoder ignores user column/query formats. Gate on the compiled format dicts, which are
            # built from the user dict at init. _convert_pandas injects a harmless column_formats["int"] hint
            # for datetime columns post-init, and the rust encoder already accepts the raw int values it feeds.
            if self.strict:
                raise NotSupportedError(
                    'native_codec="rust_strict" does not support per-column or per-type write formats; use native_codec="python" or "rust"'
                )
            logger.debug("Per-column or per-type write format set; using the Python codec")
            return NativeTransform.build_insert(context)

        column_names = list(context.column_names)
        type_names = [col_type.insert_name for col_type in context.column_types]
        try:
            core.encode_native_block(column_names, type_names, [[] for _ in column_names], 0, None)
        except NotImplementedError as ex:
            if self.strict:
                raise NotSupportedError(
                    f'native_codec="rust_strict" cannot insert unsupported column type: {ex}; use native_codec="python" or "rust"'
                ) from ex
            logger.debug("Rust native encoder cannot serialize this insert (%s); using the Python codec", ex)
            return NativeTransform.build_insert(context)

        compressor = get_compressor(context.compression)

        def chunk_gen() -> Generator[bytes, None, None]:
            for block in context.next_block():
                try:
                    output = core.encode_native_block(
                        list(block.column_names),
                        [col_type.insert_name for col_type in block.column_types],
                        list(block.column_data),
                        block.row_count,
                        block.prefix,
                    )
                except Exception as ex:
                    logger.error("Error serializing insert with Rust Native encoder", exc_info=True)
                    context.insert_exception = ex
                    yield b"INTERNAL EXCEPTION WHILE SERIALIZING"
                    return
                yield compressor.compress_block(output)
            footer = compressor.flush()
            if footer:
                yield footer

        return chunk_gen()
