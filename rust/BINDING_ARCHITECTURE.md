# The _ch_core binding: from decoded columns to Python

How the PyO3 binding layer works, how it sits on the Rust core, and how to
use it to expose query data efficiently. The core itself is documented in
ch-core-rs `DECODER_CONTRACT.md`. This document covers the two layers above
it: the binding crate `ch-core-py` and the Python integration module
`rust_client.py`.

## The high-level picture

The core does the hard part once: it turns ClickHouse Native wire bytes into
immutable columnar memory, one decoded chunk per Native block, with the full
ClickHouse type system resolved into a schema. The binding's entire job is to
move that columnar memory into Python along the cheapest legal path for
whatever the caller wants. It contains no decode logic and never inspects
wire bytes.

Think of the binding as a building with one loading dock and two exits:

```
                      network bytes
                           |
              [intake: decode_native or StreamDecoder.feed]
                  GIL released while the core decodes
                           |
                        ColBatch
          Arc-shared, immutable, chunked columnar memory
                       /          \
        [Arrow exit]                [Python object exit]
   __arrow_c_stream__              to_python_rows / columns
   pointer handoff, no copy        one PyObject per cell
   cost: ~zero                     cost: dominated by object creation
        |                                 |
   pyarrow / polars / pandas        list of tuples / lists
```

Three ideas carry the whole design:

1. **One result object, many views.** A `ColBatch` is an `Arc` over decoded
   chunks. Every consumption path, Arrow stream, row list, column list, is
   a view or conversion of the same memory. Nothing is re-decoded and the
   chunks are never concatenated. Merging streamed batches with
   `ColBatch.from_batches` is also just reference counting, no data moves.

2. **You pay only at the exit you choose.** The Arrow exit hands pyarrow
   raw buffer pointers and costs effectively nothing regardless of row
   count. The Python object exit allocates one object per cell and its cost
   dwarfs decode itself. Decode is no longer the bottleneck, the destination
   is, so the API makes the destination cost explicit.

3. **The GIL is released wherever Python memory is not touched.** Intake
   calls copy the chunk out of Python-owned memory first, then decode with
   the GIL released. That single property is what lets a plain Python
   producer thread keep reading the socket while Rust decodes, which is
   where the end-to-end streaming win comes from.

The division of labor between layers is strict and worth preserving:

| layer | owns | examples |
|---|---|---|
| core (ch-core-rs) | ClickHouse knowledge | wire framing, type parsing, schema rules, Arrow layout |
| binding (ch-core-py) | Python value policy | what a `DateTime64(6,'America/New_York')` becomes, GIL rules, exception types |
| integration (rust_client.py) | transport | HTTP, decompression, threads, queues, connection cleanup |

The same core serves a Node binding with different host policy. The binding
is the layer you rewrite per language, and at roughly a quarter of the
core's size, it is the cheap layer.

## Using it well

### Pick the exit before you pick anything else

Measured end to end on localhost, the exits differ by more than an order of
magnitude. Medians from `results_query_e2e.txt`, 1M-row mixed workload,
uncompressed: arrow 21 ms, numpy via arrow 42 ms, python rows 146 ms. The
10M-row workloads run the arrow exit in 35 to 166 ms while row
materialization at that scale is mostly Python allocator time on any engine.

Practical rules:

- Going to pandas, polars, numpy, or any arrow consumer: use the Arrow exit,
  always. `pa.RecordBatchReader.from_stream(batch)` imports the capsule
  directly.
- Need actual Python values: `to_python_rows` for row tuples,
  `to_python_columns` or `column_data(i)` when the consumer is columnar,
  which skips the per-row tuple allocations and runs noticeably faster.
- Never round-trip through Python objects to reach a dataframe.

### Buffered intake

For bytes already in hand, one call:

```python
batch = _ch_core.ColBatch.decode_native(data, has_block_info=True)
```

`has_block_info=True` whenever the request pinned
`client_protocol_version`, which it should, since that preserves bare
DateTime timezones. Passing `bytes` decodes straight from the borrowed
buffer with no copy. Passing `bytearray` or `memoryview` copies once and
then decodes with the GIL released.

### Streamed intake, the query_rust pattern

The high-water pattern proven in `rust_client.py`:

```python
decoder = _ch_core.StreamDecoder(has_block_info=True)
q = queue.Queue(maxsize=16)

# producer thread: socket reads release the GIL on their own
for chunk in decompressed_chunks(response):
    q.put(("data", chunk))
q.put(("eof", None))

# consumer thread: feed releases the GIL while Rust decodes
batches = []
while (item := q.get())[0] == "data":
    batches.extend(decoder.feed(item[1]))
batches.extend(decoder.finish())
batch = _ch_core.ColBatch.from_batches(batches)
```

Both threads make progress at once because neither holds the GIL during its
expensive part. On decode-heavy workloads the streamed pipeline beat
fetch-then-decode by 12 to 30 percent in the e2e benchmark, and it never
holds the full encoded payload in memory.

The sketch above omits the error protocol. The full version in
`rust_client.py` tags queue items `data`, `error`, `eof`, uses a timeout
put-loop with a stop flag so the producer can never block forever on a full
queue, closes the response instead of pooling it on any failure, and only
calls `finish()` on a clean EOF so a transport error is never misread as a
truncated stream. Use that version as the reference, not the sketch.

### Things the binding will not do for you

- Unsupported ClickHouse types raise `ValueError` at decode time with the
  column name. There is no partial decode.
- Truncated input raises `EOFError`, distinct from corruption, so callers
  can retry or diagnose.
- The Arrow exit maps Date to raw `uint16` days and DateTime to raw
  `uint32` epoch seconds, the core's documented zero-copy choice. DateTime64
  and Date32 carry real Arrow temporal types. If end users need temporal
  dtypes for Date and DateTime, convert after import or use the Python
  object exit, which applies full temporal policy.
- One capsule, one consumer. `__arrow_c_stream__` follows the Arrow
  PyCapsule protocol: the importer moves the stream and the capsule
  destructor handles the unconsumed case.

## Details

### Object model

`ColBatch` wraps `Arc<ChunkedBatch>` where `ChunkedBatch` is the core's
schema plus `Vec<Arc<RustColBatch>>`, one entry per decoded Native block.
Immutability plus `Arc` is what makes every exit safe to run repeatedly and
concurrently and what makes merging free.

`from_batches` validates that every input shares the first batch's schema
using the core schema's `PartialEq`, then flattens chunk lists by cloning
`Arc`s. Zero-row chunks participate in the schema check and are then
dropped, matching `decode_all_bytes`, so an all-empty input still produces
a schema-bearing result that exports a valid empty Arrow stream. This is
how a streamed LIMIT-0-style result keeps working column names and types.

### Intake surfaces

Four entry points, all funneling into the same core decode:

| surface | shape | use |
|---|---|---|
| `ColBatch.decode_native(data)` | whole payload to whole result | buffered fetch |
| `StreamDecoder.feed/finish` | push chunks, get completed blocks | async or thread streaming |
| `BlockDecoder(data)` | iterate blocks of an in-memory buffer | block-at-a-time over buffered bytes |
| `PipeDecoder(read_fd)` | iterate blocks read from a pipe fd | producer writes to a pipe |

`StreamDecoder` is the workhorse. The core side is sans-IO: it owns
partial-block buffering, scans for a complete block boundary before
allocating anything, and only materializes a block's columns once its last
byte has arrived. The binding side copies each fed chunk out of
Python-owned memory and then calls the core inside `allow_threads`. The
copy is mandatory for correctness, not a shortcut: with the GIL released,
another thread may mutate or free a `bytearray` mid-decode. A `memoryview`
or `bytes` could in principle be borrowed, but a uniform copy keeps the
unsafe surface at zero for a cost that is noise next to the network.

`decode_native` makes the one targeted exception: a `bytes` input is
immutable, so it decodes from the borrowed buffer with no copy at the price
of holding the GIL. For the buffered one-shot path that trade wins, since
there is no concurrent reader to unblock.

`PipeDecoder` duplicates the file descriptor at construction and owns the
duplicate. The caller keeps ownership of the original fd. An invalid fd
raises `OSError` instead of aborting the process, which matters because
`BorrowedFd::borrow_raw` would otherwise assert.

Schema uniformity across blocks is enforced in the core for every surface.
A mid-stream schema change raises
`ValueError: Block N schema differs from the first block` rather than
producing a result that would corrupt row materialization. The binding adds
a chunk-width recheck before its raw-pointer fill loops as defense in
depth.

Error mapping is one shared function:

| core error | Python exception |
|---|---|
| UnsupportedType, InvalidBlockInfo, UnsupportedSerialization, BlockSchemaMismatch | `ValueError` |
| Io UnexpectedEof | `EOFError`, truncation is retryable |
| other Io | `RuntimeError` |

### The Arrow exit

`__arrow_c_stream__` builds an `ArrowArrayStream` whose private data owns
the schema and a clone of the chunk `Vec`. Cloning the `Vec` clones `Arc`s,
no column data moves. Each chunk surfaces as one Arrow record batch, so
consumers see the natural block structure, and the exported buffers are
pointers straight into the Rust-owned column memory. The `Arc` references
held by the stream's private data keep that memory alive until the consumer
invokes the Arrow `release` callback, which is the C Data Interface's
ownership contract doing the lifetime work.

The capsule carries a destructor for the unconsumed case. A consumer like
pyarrow imports the stream by moving it and clearing `release` in place, so
the destructor sees a released stream and does nothing. If the capsule is
dropped without ever being imported, the destructor calls the release
callback itself. The destructor can run on any thread and must not panic,
both verified properties of the implementation, and a 50k-cycle RSS test
pins the no-leak behavior for both the consumed and unconsumed paths.

### The Python object exit

This is the expensive exit, so it is built directly on the CPython C API
where the costs are explicit:

- `PyList_New(total_rows)` preallocates the result list once, then
  `PyList_SET_ITEM` and `PyTuple_SET_ITEM` transfer ownership of each new
  reference with no bounds re-checks or refcount churn beyond the single
  creation reference.
- The raw list pointer is bound into a managed reference immediately after
  allocation, so any error or panic mid-fill drops a partially filled list
  instead of leaking it. CPython's deallocators tolerate the NULL slots
  that have not been filled yet.
- The validity branch is hoisted out of the per-cell loop in the column
  paths. A non-nullable column runs a tight loop with no per-cell null
  check at all. The row path interleaves columns so it checks per cell,
  which is part of why columns beat rows.

Value policy, the binding's reason to exist, lives here:

- **Strings.** CPython's own UTF-8 decode is the validation, a single scan.
  Only on a `UnicodeDecodeError` does the fallback render the raw bytes as
  lowercase hex, matching clickhouse-connect's String policy exactly.
- **Temporals.** A per-column `TemporalCtx` is resolved once per call, not
  per cell, so a tz-aware column imports its `ZoneInfo` exactly once per
  table. Naive columns, meaning Date, Date32, and any DateTime in UTC or
  an equivalent, are built by pure epoch arithmetic with Howard Hinnant's
  civil-date algorithm, no Python datetime parsing in the loop. Named
  non-UTC zones go through `datetime.fromtimestamp` for DST correctness,
  using a single float call while `|secs| < 2^32` keeps the float error
  under one microsecond, and an exact integer-plus-replace path beyond
  that boundary. Pre-epoch values floor correctly via euclidean division.
  The resulting values match clickhouse-connect v1 cell for cell, which the
  parity gate asserts over a million rows per table.
- **Everything else** is a direct `PyLong`, `PyFloat`, `PyBool`, or
  `PyBytes` constructor call. Wide integers become exact `PyLong` values
  directly from their 16-byte or 32-byte little-endian buffers.

All three object paths, rows, columns, and single column, share one cell
constructor, so the policy cannot drift between them.

### The integration layer

`rust_client.py` shows what a real client wraps around the binding, in
about 250 lines of plain Python:

- **Request mirroring.** It sends exactly what `client.query()` sends,
  captured empirically rather than guessed: client-level settings,
  `client_protocol_version`, and the compression headers. It deliberately
  omits `wait_end_of_query` so the server streams blocks as they are
  produced instead of buffering the whole result first.
- **Own decompression generator.** It applies the same lz4 and zstd
  decompressors the v1 path uses but re-raises mid-stream read errors that
  httputil's `ResponseSource.gen` would swallow, and it drains whole lz4
  frames parked in `unused_data` at EOF, raising `EOFError` if the stream
  ends mid-frame. Both are silent-truncation holes a streaming consumer
  cannot afford.
- **The producer protocol.** Tagged queue items, a timeout put-loop
  guarded by a stop flag on every tag, `finish()` only after a clean EOF,
  `response.close()` on any failure so a half-read connection never
  returns to the pool, `release_conn()` only on success.
- **One merge at the end.** Per-block batches accumulate as lightweight
  `Arc` wrappers and `from_batches` folds them into the single result the
  destinations consume. The result is shape-identical to a
  `decode_native` result, so every exit works unchanged on streamed data.

### Where compression sits

HTTP `Accept-Encoding` compression is the only compression in this stack
and it lives entirely in the integration layer, upstream of the decoder:

```
server lz4 body -> socket read -> decompress (producer thread) -> feed
```

The core and the binding never see a compressed byte. Decompression uses
the same python-lz4 and zstandard libraries the v1 path uses, and both
release the GIL during the heavy call, so decompression rides the existing
producer-consumer overlap for free. The parity gate passes identically
under `compress='lz4'`.

This placement is deliberate, not provisional. lz4 frames and zstd streams
are generic transport formats, not ClickHouse knowledge, so they do not
belong in the zero-dependency core. Benchmark numbers quote the
uncompressed runs because localhost lz4 is bound by the server's own
compressor at roughly 175 MB/s and client-side decompression measured as
noise, so compressed localhost numbers describe the server, not either
client path. On a real network the same mechanism runs and earns its
bandwidth win.

Two possible futures, with different owners. Moving the HTTP codecs into
Rust, for example a `feed_compressed` that skips one buffer copy and the
Python dependencies, would be a binding-layer optimization with near-zero
measured payoff today. ClickHouse's own compressed-block framing, used by
the TCP protocol and the `compress=1` HTTP mode with per-block codecs and
CityHash checksums, is ClickHouse protocol knowledge and would belong in
the core behind a cargo feature if support is ever needed. clickhouse-connect
does not use that mode today.

### Why the layering is the point

The GIL-released `feed` is binding code. The sans-IO incremental decoder it
calls is core code. The two threads that exploit it are integration code.
Remove any layer's contribution and the overlap disappears, but no layer
needed to know the others' details: the core knows nothing about Python,
the binding knows nothing about HTTP, and the integration layer never sees
a wire byte. That same seam is what lets the identical core sit under a
Node binding with an event loop instead of threads.
