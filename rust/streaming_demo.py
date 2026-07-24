"""Streaming transport prototype for the rust core — sync AND async.

Demonstrates pull/parse overlap, following the same architecture as v1's
aiohttp async client:

  * SYNC : a producer thread reads HTTP chunks into a bounded queue.Queue;
           the main thread pulls chunks and feeds _ch_core.StreamDecoder.
  * ASYNC: an aiohttp task reads chunks on the event loop into v1's
           AsyncSyncQueue (the bidirectional bridge); a thread executor
           pulls the sync side and feeds the same StreamDecoder.

The decode engine is identical in both cases — _ch_core.StreamDecoder.feed(),
which releases the GIL during decode so pull and parse genuinely overlap.

Run against the local ClickHouse on localhost:8123.
"""
import asyncio
import os
import queue
import sys
import threading
import time
from urllib.parse import quote

sys.path.insert(0, os.environ.get("CHC_BASELINE_PATH", os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))))

import _ch_core  # noqa: E402
import aiohttp  # noqa: E402
import urllib3  # noqa: E402

# v1's proven async<->sync bridge queue — the same one the aiohttp client uses.
from clickhouse_connect.driver.asyncqueue import EOF_SENTINEL, AsyncSyncQueue  # noqa: E402

HOST, PORT = "localhost", 8123
CHUNK = 1 << 20  # 1 MiB network reads
QUEUE_MAX = 16   # bounded -> backpressure


def _url(query: str) -> str:
    return f"http://{HOST}:{PORT}/?query={quote(query + ' FORMAT Native')}"


_http = urllib3.PoolManager()


# ---------------------------------------------------------------------------
# Sequential baseline (fetch fully, then decode) — what bench.py measured.
# ---------------------------------------------------------------------------
def fetch_full(query: str) -> bytes:
    resp = _http.request("GET", _url(query), preload_content=True)
    return resp.data


# ---------------------------------------------------------------------------
# SYNC streaming: producer thread -> queue.Queue -> main thread feeds decoder
# ---------------------------------------------------------------------------
def stream_sync(query: str):
    resp = _http.request("GET", _url(query), preload_content=False)
    q: queue.Queue = queue.Queue(maxsize=QUEUE_MAX)

    def producer():
        try:
            for chunk in resp.stream(CHUNK):
                q.put(chunk)
        finally:
            q.put(None)  # EOF sentinel
            resp.release_conn()

    t = threading.Thread(target=producer, daemon=True)
    t.start()

    dec = _ch_core.StreamDecoder()
    while True:
        chunk = q.get()
        if chunk is None:
            break
        for block in dec.feed(chunk):
            yield block
    for block in dec.finish():
        yield block
    t.join()


# ---------------------------------------------------------------------------
# ASYNC streaming: aiohttp loop producer -> AsyncSyncQueue -> executor decoder
# ---------------------------------------------------------------------------
async def stream_async(query: str):
    bridge: AsyncSyncQueue = AsyncSyncQueue(maxsize=QUEUE_MAX)
    loop = asyncio.get_running_loop()

    # Request identity encoding: aiohttp defaults to Accept-Encoding: gzip,
    # and ClickHouse will honor it — then aiohttp burns CPU decompressing in
    # Python. v1's async client disables this for the same reason.
    async with aiohttp.ClientSession(headers={"Accept-Encoding": "identity"}) as session:
        async with session.get(_url(query)) as resp:

            async def producer():
                try:
                    async for chunk in resp.content.iter_chunked(CHUNK):
                        await bridge.async_q.put(chunk)
                finally:
                    bridge.shutdown()  # wakes the sync consumer with EOF

            def consume():
                # Runs in a thread executor: blocking pulls + GIL-releasing decode,
                # overlapping with the aiohttp producer on the event loop.
                dec = _ch_core.StreamDecoder()
                blocks = []
                while True:
                    chunk = bridge.sync_q.get()
                    if chunk is EOF_SENTINEL:
                        break
                    blocks.extend(dec.feed(chunk))
                blocks.extend(dec.finish())
                return blocks

            prod = asyncio.create_task(producer())
            blocks = await loop.run_in_executor(None, consume)
            await prod
            return blocks


# ---------------------------------------------------------------------------
# ASYNC streaming, executor-driven (#2): no aiohttp at all.
#
# Drive the proven SYNC pipeline (urllib3 producer thread + GIL-releasing
# decode) inside a thread executor and await it. Overlap happens between the
# two worker threads (both release the GIL); the event loop stays free the
# whole time, so it remains genuinely non-blocking AND overlaps fetch+decode.
# ---------------------------------------------------------------------------
async def stream_async_executor(query: str):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: list(stream_sync(query)))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def rows_of(blocks) -> list:
    out = []
    for b in blocks:
        out.extend(tuple(r) for r in b.to_python_rows())
    return out


def ms(s):
    return f"{s * 1000:7.1f} ms"


# ---------------------------------------------------------------------------
# Correctness: streamed (sync + async) must equal a single full decode.
# ---------------------------------------------------------------------------
def correctness():
    print("== correctness (deterministic ORDER BY, exact equality) ==")
    q = "SELECT id, val, name, flag, small_int, big_uint FROM bench_types ORDER BY id LIMIT 100000"
    raw = fetch_full(q)
    baseline = rows_of([_ch_core.ColBatch.decode_native(raw, has_block_info=False)])

    sync_rows = rows_of(list(stream_sync(q)))
    async_rows = rows_of(asyncio.run(stream_async(q)))
    async_exec_rows = rows_of(asyncio.run(stream_async_executor(q)))

    print(f"  baseline rows : {len(baseline)}")
    print(f"  sync           == baseline: {sync_rows == baseline}")
    print(f"  async(aiohttp) == baseline: {async_rows == baseline}")
    print(f"  async(executor)== baseline: {async_exec_rows == baseline}")


# ---------------------------------------------------------------------------
# Overlap: streaming total should approach max(fetch, decode), not the sum.
# ---------------------------------------------------------------------------
def overlap():
    print("\n== overlap (streaming total vs sequential fetch+decode) ==")
    workloads = {
        "mixed_6col_10M": "SELECT id, val, name, flag, small_int, big_uint FROM bench_types",
        "string_1col_10M": "SELECT name FROM bench_types",
        "int_3col_10M": "SELECT id, small_int, big_uint FROM bench_types",
    }
    for name, q in workloads.items():
        # sequential baseline
        t0 = time.perf_counter()
        raw = fetch_full(q)
        t_fetch = time.perf_counter() - t0
        t0 = time.perf_counter()
        base = _ch_core.ColBatch.decode_native(raw, has_block_info=False)
        t_dec = time.perf_counter() - t0
        n = base.num_rows
        seq = t_fetch + t_dec

        # sync streaming (count rows to force full consumption)
        t0 = time.perf_counter()
        rows_sync = sum(b.num_rows for b in stream_sync(q))
        t_sync = time.perf_counter() - t0

        # async streaming, aiohttp + bridge (GIL-bound loop — doesn't overlap)
        t0 = time.perf_counter()
        rows_async = sum(b.num_rows for b in asyncio.run(stream_async(q)))
        t_async = time.perf_counter() - t0

        # async streaming, executor-driven sync pipeline (#2 — should match sync)
        t0 = time.perf_counter()
        rows_aexec = sum(b.num_rows for b in asyncio.run(stream_async_executor(q)))
        t_aexec = time.perf_counter() - t0

        assert rows_sync == n and rows_async == n and rows_aexec == n, "row count mismatch"
        print(f"\n  {name} ({n:,} rows)")
        print(f"    sequential        : fetch {ms(t_fetch)} + decode {ms(t_dec)} = {ms(seq)}")
        print(f"    sync stream       : {ms(t_sync)}   ({(seq / t_sync):.2f}x vs sequential)")
        print(f"    async (aiohttp)   : {ms(t_async)}   ({(seq / t_async):.2f}x vs sequential)")
        print(f"    async (executor)  : {ms(t_aexec)}   ({(seq / t_aexec):.2f}x vs sequential)")


# ---------------------------------------------------------------------------
# Non-blocking proof: the event loop must keep running other tasks while the
# executor-driven query overlaps fetch+decode on worker threads.
# ---------------------------------------------------------------------------
def non_blocking():
    print("\n== non-blocking event loop (executor-driven async) ==")
    q = "SELECT id, val, name, flag, small_int, big_uint FROM bench_types"

    async def run():
        ticks = 0

        async def heartbeat():
            nonlocal ticks
            while True:
                await asyncio.sleep(0.005)  # 5ms heartbeat
                ticks += 1

        hb = asyncio.create_task(heartbeat())
        t0 = time.perf_counter()
        blocks = await stream_async_executor(q)
        dt = time.perf_counter() - t0
        hb.cancel()
        rows = sum(b.num_rows for b in blocks)
        return dt, rows, ticks

    dt, rows, ticks = asyncio.run(run())
    expected = int(dt / 0.005)
    print(f"  query: {rows:,} rows in {ms(dt)}")
    print(f"  heartbeat ticked {ticks} times during the query "
          f"(~{expected} expected if loop never blocked)")
    print(f"  loop stayed responsive: {ticks > 0.5 * expected}")


if __name__ == "__main__":
    correctness()
    overlap()
    non_blocking()
