import asyncio
import logging
import threading
import zlib
from collections.abc import Callable, Iterable, Iterator

import lz4.frame

from clickhouse_connect.driver.asyncqueue import EOF_SENTINEL, AsyncSyncQueue
from clickhouse_connect.driver.common import StreamContext
from clickhouse_connect.driver.compression import _zstd_decompressor, available_compression
from clickhouse_connect.driver.exceptions import OperationalError, StreamFailureError
from clickhouse_connect.driver.types import Closable

logger = logging.getLogger(__name__)

__all__ = [
    "StreamingResponseSource",
    "StreamingFileAdapter",
    "StreamingInsertSource",
    "QueuedStreamSource",
    "StreamExceptionScanner",
    "start_streaming_response",
    "guarded_arrow_stream",
]

if "br" in available_compression:
    import brotli
else:
    brotli = None


class StreamingResponseSource(Closable):
    """Streaming source that feeds chunks from async producer to sync consumer."""

    READ_BUFFER_SIZE = 1024 * 1024

    def __init__(self, response, encoding: str | None = None, exception_tag: str | None = None):
        self.response = response
        self.encoding = encoding
        self.exception_tag = exception_tag

        # maxsize=10 means max ~10 socket reads buffered
        self.queue: AsyncSyncQueue[bytes | Exception] = AsyncSyncQueue(maxsize=10)

        self._decompressor = None
        self._decompressor_initialized = False

        # Multiple accesses to .gen must return the same generator, not create new ones
        self._gen_cache: Iterator[bytes] | None = None

        self._producer_task: asyncio.Task | None = None
        self._producer_started = threading.Event()
        self._producer_error: Exception | None = None
        self._producer_completed = False

    def _release_lease(self):
        release = getattr(self.response, "_lease_release", None)
        if release is not None:
            release()

    async def start_producer(self, loop: asyncio.AbstractEventLoop):
        """Start the async producer task.
        Must be called from the event loop thread before consuming.
        """

        async def producer():
            """Async producer: reads chunks from response, feeds queue."""
            data_sent = False
            try:
                while True:
                    chunk = await self.response.content.read(self.READ_BUFFER_SIZE)
                    if not chunk:
                        break
                    data_sent = True
                    await self.queue.async_q.put(chunk)

                await self.queue.async_q.put(EOF_SENTINEL)
                self._producer_completed = True

            except Exception as e:
                logger.error("Producer error while streaming response: %s", e, exc_info=True)
                if not data_sent:
                    e = OperationalError("Failed to read response data from server")
                self._producer_error = e

                try:
                    await self.queue.async_q.put(e)
                except RuntimeError:
                    pass

            finally:
                self.queue.shutdown()
                self._release_lease()

        self._producer_task = loop.create_task(producer())
        self._producer_started.set()

    @property
    def gen(self) -> Iterator[bytes]:
        """Generator that yields decompressed chunks.

        CRITICAL: Returns cached generator to prevent multiple generators
        from competing to read from the same queue.
        """
        if self._gen_cache is not None:
            return self._gen_cache

        self._gen_cache = self._create_generator()
        return self._gen_cache

    def _create_generator(self) -> Iterator[bytes]:
        """Creates the actual generator function."""
        if not self._producer_started.wait(timeout=5.0):
            raise RuntimeError("Producer failed to start within timeout")

        if self.encoding and not self._decompressor_initialized:
            self._decompressor_initialized = True
            try:
                self._decompressor = self._create_decompressor(self.encoding)
            except Exception as e:
                logger.error("Failed to create decompressor for %s: %s", self.encoding, e)
                raise

        while True:
            chunk = self.queue.sync_q.get()

            if chunk is EOF_SENTINEL:
                if self._decompressor:
                    try:
                        if hasattr(self._decompressor, "flush"):
                            final = self._decompressor.flush()
                            if final:
                                yield final
                    except Exception as e:
                        logger.error("Error flushing decompressor: %s", e, exc_info=True)
                        raise
                break

            if isinstance(chunk, Exception):
                raise chunk

            if self._decompressor:
                try:
                    if hasattr(self._decompressor, "decompress"):
                        decompressed = self._decompressor.decompress(chunk)
                    else:
                        decompressed = self._decompressor.process(chunk)
                    if decompressed:
                        yield decompressed
                except Exception as e:
                    logger.error("Decompression error: %s", e, exc_info=True)
                    raise
            else:
                yield chunk

    @staticmethod
    def _create_decompressor(encoding: str):
        """Create incremental decompressor for encoding."""
        if encoding == "gzip":
            return zlib.decompressobj(16 + zlib.MAX_WBITS)

        if encoding == "deflate":
            return zlib.decompressobj()

        if encoding == "br":
            if brotli is not None:
                return brotli.Decompressor()
            raise ImportError("brotli compression requires 'brotli' package. Install with: pip install brotli")

        if encoding == "zstd":
            return _zstd_decompressor()

        if encoding == "lz4":
            return lz4.frame.LZ4FrameDecompressor()

        raise ValueError(f"Unsupported compression encoding: {encoding}")

    async def aclose(self):
        """Async cleanup resources"""
        self.queue.shutdown()

        if self._producer_task and not self._producer_task.done():
            self._producer_task.cancel()
            try:
                await self._producer_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        if self.response and not self.response.closed:
            if not self._producer_completed:
                self.response.close()
                await asyncio.sleep(0.05)
        self._release_lease()

    def close(self):
        """Synchronous cleanup resources"""
        self.queue.shutdown()

        if self._producer_task and not self._producer_task.done():
            self._producer_task.cancel()

        if self.response and not self.response.closed:
            if not self._producer_completed:
                self.response.close()
        self._release_lease()


async def start_streaming_response(response, encoding: str | None = None, exception_tag: str | None = None) -> StreamingResponseSource:
    """Create a StreamingResponseSource and start its producer on the running loop.

    This is the async byte bridge: an async producer reads response chunks onto
    a bounded queue that a sync consumer (usually parsing in an executor) drains.
    """
    source = StreamingResponseSource(response, encoding=encoding, exception_tag=exception_tag)
    await source.start_producer(asyncio.get_running_loop())
    return source


class QueuedStreamSource(Closable):
    """A streaming source paired with the bounded queue that feeds parsed items
    from a sync producer (running in an executor) to an async consumer."""

    def __init__(self, source: StreamingResponseSource, maxsize: int = 10):
        self.source = source
        self.queue: AsyncSyncQueue = AsyncSyncQueue(maxsize=maxsize)

    def pump(self, produce: Callable[[], Iterable]) -> None:
        """Run produce() in an executor, feeding its items into the queue.
        Must be called from the event loop thread. A RuntimeError from a queue
        put means the queue was shut down and ends the producer quietly; any
        error from produce() itself is queued for the consumer to raise."""
        queue = self.queue

        def producer():
            try:
                for item in produce():
                    try:
                        queue.sync_q.put(item)
                    except RuntimeError:
                        return
                try:
                    queue.sync_q.put(EOF_SENTINEL)
                except RuntimeError:
                    return
            except Exception as e:
                try:
                    queue.sync_q.put(e)
                except Exception:
                    pass
            finally:
                queue.shutdown()

        asyncio.get_running_loop().run_in_executor(None, producer)

    async def items(self):
        """Async generator yielding queued items without blocking the event loop."""
        while True:
            item = await self.queue.async_q.get()
            if item is EOF_SENTINEL:
                break
            if isinstance(item, Exception):
                raise item
            yield item

    async def aclose(self):
        self.queue.shutdown()
        await self.source.aclose()

    def close(self):
        self.queue.shutdown()
        self.source.close()


def _extract_exception_with_tag(message: bytes, exception_tag: str) -> str | None:
    # Imported lazily: transform imports driver.query (and, transitively, driver.client,
    # which imports this module), so a module-level import would create a cycle.
    from clickhouse_connect.driver.transform import extract_exception_with_tag

    return extract_exception_with_tag(message, exception_tag)


class StreamExceptionScanner:
    """Detects an in-band ClickHouse exception appended to an HTTP 200 response body.

    When a query fails after the server has already started streaming a 200 response,
    the server appends a tagged block of the form

        \\r\\n__exception__\\r\\n<tag>\\r\\n<message>\\r\\n<len> <tag>\\r\\n__exception__\\r\\n

    where <tag> is the value of the X-ClickHouse-Exception-Tag response header. The
    native ResponseBuffer parser detects this while reading columns, but the Arrow
    paths hand raw bytes straight to pyarrow, which cannot see it. This scanner
    reproduces the detection for those byte streams: feed it the raw chunks and it
    holds back any bytes that belong to the exception block, exposing the extracted
    server error through error_message.
    """

    _MARKER = b"__exception__"

    def __init__(self, exception_tag: str):
        self.exception_tag = exception_tag
        self._tag = exception_tag.encode()
        self._carry = b""
        self.armed = False
        self._block = bytearray()
        self.error_message: str | None = None

    def feed(self, chunk: bytes) -> bytes:
        """Feed a raw chunk and return the leading bytes that are safe to forward to
        the consumer. Once the exception marker has been seen every subsequent byte is
        retained for extraction, so this returns b"" from that point on."""
        if self.armed:
            self._block += chunk
            self._finalize_if_complete()
            return b""
        search = self._carry + chunk
        idx = self._find_marker(search)
        if idx is not None:
            self.armed = True
            self._block = bytearray(search[idx:])
            self._carry = b""
            self._finalize_if_complete()
            return search[:idx]
        # No marker yet: hold back a small tail so an opening marker split across a
        # chunk boundary is still found once the next chunk arrives. The tail must span
        # the whole opener the matcher needs to confirm (the marker, the tag, and the
        # CRLF that may separate them), so keep MARKER + tag plus a few bytes of slack.
        keep = len(self._MARKER) + len(self._tag) + 8
        if len(search) > keep:
            self._carry = search[-keep:]
            return search[:-keep]
        self._carry = search
        return b""

    def finish(self) -> None:
        """Signal end of input, finalizing the message from whatever block bytes
        arrived. Used when the transport aborts before the closing marker."""
        if self.armed and self.error_message is None:
            self.error_message = _extract_exception_with_tag(bytes(self._block), self.exception_tag)

    def flush(self) -> bytes:
        """Return any bytes held back when the stream ends without an exception."""
        if self.armed:
            return b""
        tail = self._carry
        self._carry = b""
        return tail

    def _finalize_if_complete(self) -> None:
        if self.error_message is None and self._block.count(self._MARKER) >= 2:
            self.error_message = _extract_exception_with_tag(bytes(self._block), self.exception_tag)

    def _find_marker(self, data: bytes) -> int | None:
        """Return the index of an opening marker whose tag matches, or None.

        The wire format separates __exception__ from the tag with CRLF, so we look
        for the literal marker and then require our tag to follow it (after an
        optional run of CR/LF), which also guards against matching the literal bytes
        inside legitimate result data.
        """
        start = 0
        while True:
            pos = data.find(self._MARKER, start)
            if pos == -1:
                return None
            probe = pos + len(self._MARKER)
            while probe < len(data) and data[probe : probe + 1] in (b"\r", b"\n"):
                probe += 1
            candidate = data[probe : probe + len(self._tag)]
            if candidate == self._tag:
                return pos
            if len(candidate) < len(self._tag) and self._tag.startswith(candidate):
                # The tag may be split across the chunk boundary; wait for more bytes.
                return None
            start = pos + 1


class StreamingFileAdapter:
    """File-like adapter for PyArrow streaming.

    When an exception_tag is present the adapter guards the byte stream: a mid-stream
    server exception is surfaced as a clean StreamFailureError instead of being fed to
    pyarrow as truncated or garbled Arrow data.
    """

    def __init__(self, source, exception_tag: str | None = None):
        raw_gen = source.gen if hasattr(source, "gen") else iter(source)
        self.exception_tag = exception_tag if exception_tag is not None else getattr(source, "exception_tag", None)
        self._scanner = StreamExceptionScanner(self.exception_tag) if self.exception_tag else None
        self.gen = self._guarded_gen(raw_gen) if self._scanner else raw_gen
        self.buffer = b""
        self.closed = False
        self.eof = False

    def _guarded_gen(self, source_gen):
        """Wrap the raw chunk generator, forwarding data until an in-band exception
        marker appears, then raising it as a StreamFailureError."""
        scanner = self._scanner
        source_gen = iter(source_gen)
        while True:
            try:
                chunk = next(source_gen)
            except StopIteration:
                tail = scanner.flush()
                if tail:
                    yield tail
                return
            except Exception:
                # Transport aborted mid-stream (ClientPayloadError, ProtocolError,
                # OperationalError). ClickHouse may have written the real error into the
                # body before the connection dropped, so prefer that over the raw error.
                scanner.finish()
                if scanner.error_message is not None:
                    raise StreamFailureError(scanner.error_message) from None
                raise
            if not chunk:
                continue
            safe = scanner.feed(chunk)
            if safe:
                yield safe
            if scanner.armed:
                break
        # Marker seen: drain the rest of the input into the scanner (without forwarding
        # it to the consumer) so the complete exception block is captured, then surface
        # the server error.
        while scanner.error_message is None:
            try:
                chunk = next(source_gen)
            except StopIteration:
                break
            except Exception:
                break
            if chunk:
                scanner.feed(chunk)
        scanner.finish()
        raise StreamFailureError(scanner.error_message or "ClickHouse server reported an error during streaming")

    def read(self, size: int = -1) -> bytes:
        """Read up to size bytes from stream"""
        if self.closed or self.eof:
            return b""

        if size != -1 and len(self.buffer) >= size:
            result = self.buffer[:size]
            self.buffer = self.buffer[size:]
            return result

        chunks = [self.buffer] if self.buffer else []
        current_len = len(self.buffer)
        self.buffer = b""

        while (size == -1 or current_len < size) and not self.eof:
            try:
                chunk = next(self.gen)
                if chunk:
                    chunks.append(chunk)
                    current_len += len(chunk)
                else:
                    self.eof = True
                    break
            except StopIteration:
                self.eof = True
                break

        full_data = b"".join(chunks)

        if size == -1 or len(full_data) <= size:
            return full_data

        result = full_data[:size]
        self.buffer = full_data[size:]
        return result

    def raise_if_pending(self):
        """Ensure an in-band server exception is surfaced even if the Arrow reader
        stopped short of it (for example by treating padding before the exception
        block as end-of-stream). Safe to call after a clean parse; a no-op when no
        exception tag is in play."""
        if self._scanner is None:
            return
        while next(self.gen, None) is not None:
            pass

    def close(self):
        self.closed = True


def guarded_arrow_stream(response, converter: Callable | None = None) -> StreamContext:
    """Wrap a synchronous streaming HTTP response as a StreamContext of pyarrow
    RecordBatches, guarding it so a mid-stream ClickHouse exception surfaces as a
    clean StreamFailureError instead of a truncated Arrow stream or a raw transport
    error. Mirrors the in-band detection the async Arrow paths get through
    StreamingResponseSource. When ``converter`` is provided each RecordBatch is passed
    through it (used by the DataFrame streaming variants)."""
    # Imported lazily to avoid an import cycle (httpcommon imports driver.client,
    # which imports this module) and to keep pyarrow optional at import time.
    from clickhouse_connect.driver.options import check_arrow

    pyarrow = check_arrow()
    if hasattr(response, "headers") and hasattr(response, "stream"):
        from clickhouse_connect.driver._backend.httpcommon import ex_tag_header

        exception_tag = response.headers.get(ex_tag_header)
        adapter: StreamingFileAdapter | None = StreamingFileAdapter(
            response.stream(StreamingResponseSource.READ_BUFFER_SIZE, True),
            exception_tag=exception_tag,
        )
        reader = pyarrow.ipc.open_stream(adapter)
    else:
        # Not an HTTP streaming response (for example the in-process chdb backend):
        # there is no in-band exception tag to guard, so parse the response directly.
        adapter = None
        reader = pyarrow.ipc.open_stream(response)

    def batches():
        for batch in reader:
            yield converter(batch) if converter is not None else batch
        if adapter is not None:
            adapter.raise_if_pending()

    return StreamContext(response, batches())


class StreamingInsertSource:
    """Streaming source for async inserts (reverse bridge)"""

    def __init__(self, transform, context, loop: asyncio.AbstractEventLoop, maxsize: int = 10):
        self.transform = transform
        self.context = context
        self.loop = loop
        self.queue: AsyncSyncQueue[bytes | bytearray | Exception] = AsyncSyncQueue(maxsize=maxsize)
        self._producer_future = None
        self._started = False

    def start_producer(self):
        if self._started:
            raise RuntimeError("Producer already started")
        self._started = True

        def producer():
            try:
                for block in self.transform.build_insert(self.context):
                    self.queue.sync_q.put(block)

                self.queue.sync_q.put(EOF_SENTINEL)

            except Exception as e:
                logger.error("Insert producer error: %s", e, exc_info=True)
                try:
                    self.queue.sync_q.put(e)
                except Exception:
                    pass
            finally:
                self.queue.shutdown()

        self._producer_future = self.loop.run_in_executor(None, producer)

    async def async_generator(self):
        """Async generator that yields blocks for aiohttp streaming."""
        if not self._started:
            raise RuntimeError("Producer not started, call start_producer() first")

        try:
            while True:
                chunk = await self.queue.async_q.get()

                if chunk is EOF_SENTINEL:
                    break

                if isinstance(chunk, Exception):
                    raise chunk

                yield chunk

        except Exception as e:
            logger.error("Insert consumer error: %s", e, exc_info=True)
            raise
        finally:
            if self._producer_future and not self._producer_future.done():
                try:
                    await self._producer_future
                except Exception:
                    pass

    async def close(self, timeout: float | None = 1.0):
        """Shut down the queue and wait for the producer thread to terminate. Pass ``timeout=None`` to wait without a deadline."""
        self.queue.shutdown()
        if self._producer_future and not self._producer_future.done():
            try:
                if timeout is None:
                    await self._producer_future
                else:
                    await asyncio.wait_for(asyncio.shield(self._producer_future), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("Insert producer did not finish within timeout")
            except Exception:
                pass
