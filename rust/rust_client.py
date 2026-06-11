"""End-to-end query path over the Rust decoder.

query_rust(client, query) fetches FORMAT Native bytes over the existing
clickhouse-connect transport (client.raw_stream) and decodes them with
_ch_core.StreamDecoder on the caller thread while a producer thread keeps
pulling network chunks. feed() releases the GIL during decode, so transport
and decode overlap. The per-block batches are merged once at the end with
ColBatch.from_batches, giving a result whose shape is identical to a
decode_native batch on every destination.

This module does not modify the clickhouse_connect package. It mirrors the
query params client.query() actually sends (captured empirically): the
client-level settings, client_protocol_version, and for compressed clients
the Accept-Encoding header plus enable_http_compression. wait_end_of_query
is intentionally absent so the server streams blocks as they are produced.

The decompressed-chunk generator here deliberately does NOT reuse
httputil.ResponseSource.gen: that generator swallows read exceptions once
any data has been received (httputil.py:248), which would let a mid-stream
transport failure surface as a truncated-stream EOFError or, at a block
boundary, as a silently truncated result. This one re-raises.

After a mid-stream failure the closed connection's query is cancelled server
side, so an immediate retry on the same client can transiently raise
SESSION_IS_LOCKED until the cancel lands.
"""

from __future__ import annotations

import json
import queue
import threading

import _ch_core
from clickhouse_connect.driver.httpclient import columns_only_re
from clickhouse_connect.driver.query import remove_sql_comments

QUEUE_MAX = 16
PUT_TIMEOUT = 0.1
JOIN_TIMEOUT = 5.0
CHUNK_SIZE = 1024 * 1024


def _decompressed_chunks(response, chunk_size=CHUNK_SIZE):
    """Yield decompressed body chunks, re-raising any mid-stream read error.

    Applies the same lz4/zstd decompression objects httputil uses so the
    bytes match the v1 path exactly.
    """
    encoding = response.headers.get("content-encoding")
    if encoding == "lz4":
        import lz4.frame

        decom = lz4.frame.LZ4FrameDecompressor()
        fed = False
        for chunk in response.stream(chunk_size, False):
            if not chunk:
                continue
            fed = True
            if decom.unused_data:
                chunk = decom.unused_data + chunk
            block = decom.decompress(chunk)
            if block:
                yield block
        # Whole frames parked in unused_data at EOF would otherwise be
        # silently dropped: drain them. The decompressor restarts on the
        # next frame automatically.
        while decom.unused_data:
            block = decom.decompress(decom.unused_data)
            if block:
                yield block
        if fed and not decom.eof:
            raise EOFError("Response ended mid lz4 frame")
    elif encoding == "zstd":
        import zstandard

        decom = zstandard.ZstdDecompressor().decompressobj()
        for chunk in response.stream(chunk_size, False):
            if not chunk:
                continue
            block = decom.decompress(chunk)
            if block:
                yield block
    elif encoding is None:
        yield from response.stream(chunk_size, True)
    else:
        raise ValueError(f"Unsupported content encoding {encoding}")


def _mirrored_request(client, query, settings):
    """Issue the query via raw_stream with the params client.query() sends."""
    merged = dict(settings or {})
    transport = {}
    if client.protocol_version:
        merged["client_protocol_version"] = client.protocol_version
    if client.compression:
        transport["Accept-Encoding"] = client.compression
        if client._send_comp_setting:
            merged["enable_http_compression"] = "1"
    return client.raw_stream(query, fmt="Native", settings=merged, transport_settings=transport)


def _write_varint(buf, value):
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            buf.append(byte | 0x80)
        else:
            buf.append(byte)
            return


def _empty_batch_from_meta(client, query, settings):
    """Schema-bearing empty batch for a columns-only query.

    Mirrors the v1 LIMIT 0 branch: the server sends zero Native bytes for
    these, so v1 fetches FORMAT JSON metadata instead (httpclient.py:263).
    The server's own name and type strings are framed as a zero-row Native
    header and decoded by the core, so the type parsing and the unsupported
    type boundary stay in Rust.
    """
    meta = json.loads(client.raw_query(query, settings=settings, fmt="JSON"))["meta"]
    has_info = bool(client.protocol_version)
    buf = bytearray()
    if has_info:
        buf += bytes((1, 0, 2, 0xFF, 0xFF, 0xFF, 0xFF, 0))
    _write_varint(buf, len(meta))
    _write_varint(buf, 0)
    for col in meta:
        for text in (col["name"], col["type"]):
            encoded = text.encode()
            _write_varint(buf, len(encoded))
            buf += encoded
    return _ch_core.ColBatch.decode_native(bytes(buf), has_block_info=has_info)


def _put(q, item, stop):
    """Bounded put that gives up once the consumer has signaled stop."""
    while not stop.is_set():
        try:
            q.put(item, timeout=PUT_TIMEOUT)
            return True
        except queue.Full:
            pass
    return False


def query_rust(client, query, settings=None):
    """Run a SELECT down the Rust decode path. Returns RustQueryResult.

    Streams the response on a producer thread into a bounded queue while the
    caller thread decodes. On any error the response connection is closed,
    not returned to the pool, and the original exception propagates.
    """
    if columns_only_re.search(remove_sql_comments(query)):
        return RustQueryResult(_empty_batch_from_meta(client, query, settings))
    response = _mirrored_request(client, query, settings)
    q = queue.Queue(maxsize=QUEUE_MAX)
    stop = threading.Event()

    def produce():
        try:
            for chunk in _decompressed_chunks(response):
                if not _put(q, ("data", chunk), stop):
                    return
        except BaseException as exc:  # noqa: BLE001 - propagated to consumer
            _put(q, ("error", exc), stop)
        finally:
            _put(q, ("eof", None), stop)

    producer = threading.Thread(target=produce, daemon=True)
    producer.start()

    decoder = _ch_core.StreamDecoder(has_block_info=bool(client.protocol_version))
    batches = []
    try:
        while True:
            tag, payload = q.get()
            if tag == "data":
                batches.extend(decoder.feed(payload))
            elif tag == "error":
                raise payload
            else:
                batches.extend(decoder.finish())
                break
    except BaseException:
        stop.set()
        response.close()
        producer.join(timeout=JOIN_TIMEOUT)
        raise
    producer.join(timeout=JOIN_TIMEOUT)
    response.release_conn()

    if not batches:
        # The server sent zero bytes (empty result without a LIMIT 0 suffix).
        # v1 query() yields a schema-less empty result for these too.
        return RustQueryResult(None)
    batch = _ch_core.ColBatch.from_batches(batches)
    del batches
    return RustQueryResult(batch)


class RustQueryResult:
    """Materialized query result over a merged ColBatch.

    batch is None for a schema-less empty result, matching v1 behavior when
    the server sends no Native block at all.
    """

    def __init__(self, batch):
        self.batch = batch

    @property
    def column_names(self):
        if self.batch is None:
            return ()
        return tuple(self.batch.column_names)

    @property
    def result_rows(self):
        if self.batch is None:
            return []
        return self.batch.to_python_rows()

    @property
    def result_columns(self):
        if self.batch is None:
            return []
        return self.batch.to_python_columns()

    def arrow_table(self):
        import pyarrow as pa

        if self.batch is None:
            return pa.table({})
        return pa.RecordBatchReader.from_stream(self.batch).read_all()

    def to_pandas(self):
        return self.arrow_table().to_pandas()

    def to_polars(self):
        import polars as pl

        return pl.from_arrow(self.arrow_table())
