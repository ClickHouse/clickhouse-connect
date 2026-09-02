# The _ch_core binding

How the PyO3 binding layer works and how to consume it efficiently. The
core's wire and Arrow contract is documented in ch-core-rs
`DECODER_CONTRACT.md`. This document covers the binding crate `ch-core-py`
and the integration pattern above it.

## Design

The core turns ClickHouse Native wire bytes into immutable columnar memory,
one decoded chunk per Native block, with the type system resolved into a
schema. The binding moves that memory into Python along the cheapest legal
path for what the caller wants. It contains no decode logic and never
inspects wire bytes.

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
        |                                 |
   pyarrow / polars / pandas        list of tuples / lists
```

Three ideas carry the design:

1. **One result object, many views.** `ColBatch` wraps an `Arc` over the
   decoded chunks. Every consumption path is a view or conversion of the
   same memory. Nothing is re-decoded, chunks are never concatenated, and
   merging streamed batches with `ColBatch.from_batches` is reference
   counting only.

2. **You pay only at the exit you choose.** The Arrow exit hands consumers
   raw buffer pointers and costs near zero at any row count. The Python
   object exit allocates one object per cell and dominates decode itself.
   Going to pandas, polars, numpy, or any Arrow consumer, use the Arrow
   exit. Never round-trip through Python objects to reach a dataframe.

3. **The GIL is released wherever Python memory is not touched.** Intake
   copies each fed chunk out of Python-owned memory, then decodes with the
   GIL released. That is what lets a producer thread keep reading the
   socket while Rust decodes.

The division of labor is strict:

| layer | owns | examples |
|---|---|---|
| core (ch-core-rs) | ClickHouse knowledge | wire framing, type parsing, schema rules, Arrow layout |
| binding (ch-core-py) | Python value policy | what a `DateTime64(6,'America/New_York')` becomes, GIL rules, exception types |
| integration (driver `rustcodec.py`, POC `rust_client.py`) | transport | HTTP, decompression, threads, queues, connection cleanup |

The core knows nothing about Python, the binding knows nothing about HTTP,
and the integration layer never sees a wire byte. The binding is the layer
rewritten per language.

## Intake

Four entry points, all funneling into the same core decode:

| surface | shape | use |
|---|---|---|
| `ColBatch.decode_native(data)` | whole payload to whole result | buffered fetch |
| `StreamDecoder.feed/finish` | push chunks, get completed blocks | async or thread streaming |
| `BlockDecoder(data)` | iterate blocks of an in-memory buffer | block at a time over buffered bytes |
| `PipeDecoder(read_fd)` | iterate blocks read from a pipe fd | producer writes to a pipe |

`StreamDecoder` copies each fed chunk before releasing the GIL. The copy is
mandatory for correctness: with the GIL released, another thread may mutate
or free a `bytearray` mid-decode. `decode_native` makes the one exception,
a `bytes` input is immutable so it decodes from the borrowed buffer with no
copy at the price of holding the GIL, the right trade for a one-shot
buffered call.

Pass `has_block_info=True` whenever the request pinned
`client_protocol_version`, which preserves bare DateTime timezones.

Schema uniformity across blocks is enforced in the core for every surface.
A mid-stream schema change raises `ValueError` rather than producing a
result that would corrupt row materialization. `from_batches` validates
schema equality across inputs and keeps working column names and types even
when every chunk is empty.

## Exits

### Arrow

`__arrow_c_stream__` exports one Arrow record batch per decoded chunk. The
exported buffers point straight into Rust-owned column memory, kept alive
by `Arc` references held in the stream's private data until the consumer
invokes the Arrow `release` callback. The capsule follows the Arrow
PyCapsule protocol: one capsule, one consumer, and the capsule destructor
releases the stream if it is dropped unconsumed.

Date exports as raw `uint16` days and DateTime as raw `uint32` epoch
seconds, the core's documented zero-copy choice. DateTime64 and Date32
carry real Arrow temporal types. Consumers needing temporal dtypes for Date
and DateTime convert after import or use the object exit.

### Python objects

Built directly on the CPython C API: result lists are preallocated once and
filled with `PyList_SET_ITEM`/`PyTuple_SET_ITEM` ownership transfer, the
raw list pointer is bound into a managed reference immediately so a panic
mid-fill drops a partially filled list instead of leaking it, and the
validity branch is hoisted out of the per-cell loop so non-nullable columns
run with no per-cell null check. Rows, columns, and single-column paths
share one cell constructor, so value policy cannot drift between them.

## Value policy

The binding's reason to exist. The values match the Python codec cell for
cell except where documented in `docs/rust-codec.mdx`.

- **Strings.** CPython's own UTF-8 decode is the validation. Only on
  failure does the fallback render the raw bytes as lowercase hex, matching
  the driver's String policy.
- **Temporals.** A per-column context resolves timezone policy once, not
  per cell. Naive columns are built by pure epoch arithmetic with no
  datetime parsing in the loop. Named non-UTC zones go through
  `datetime.fromtimestamp` for DST correctness.
- **Aggregate states.** Exact serialized state `bytes`, uninterpreted. The
  Arrow exit exports them zero copy as LargeBinary.
- **Variant and Dynamic.** Intrinsic NULL becomes `None`, unambiguous
  alternatives use ordinary Python values, and alternatives sharing a
  Python type use `typed_variant`. The Arrow exit is the core's zero-copy
  dense union.
- **Everything else** is a direct `PyLong`, `PyFloat`, `PyBool`, or
  `PyBytes` constructor call, with wide integers built exactly from their
  little-endian buffers.

## Errors

One shared mapping from core errors:

| core error | Python exception |
|---|---|
| unsupported type, invalid block, schema mismatch, corruption | `ValueError`, named column where known |
| unexpected EOF | `EOFError`, truncation is distinct and retryable |
| other IO | `RuntimeError` |

The driver layer translates these into `DataError` for its public surface.

## Streaming pattern

A producer thread reads and decompresses the socket while a consumer thread
calls `StreamDecoder.feed`. Both make progress at once because neither
holds the GIL during its expensive part. The reference implementation is
the driver's `rustcodec.py`, with `rust_client.py` as the standalone POC
version. The essentials of the protocol:

- Queue items are tagged data, error, or EOF, and the producer uses a
  timeout put-loop with a stop flag so it can never block forever on a full
  queue.
- `finish()` runs only after a clean EOF, so a transport error is never
  misread as a truncated stream.
- The response is closed rather than pooled on any failure, so a half-read
  connection never returns to the pool.
- Per-block batches accumulate as `Arc` wrappers and one `from_batches`
  merge at the end produces a result shape-identical to `decode_native`,
  so every exit works unchanged on streamed data.

## Compression

HTTP `Accept-Encoding` compression lives entirely in the integration layer,
upstream of the decoder. The core and the binding never see a compressed
byte. lz4 frames and zstd streams are generic transport formats, not
ClickHouse knowledge, so they do not belong in the zero-dependency core.
ClickHouse's own compressed-block framing, used by the TCP protocol and the
`compress=1` HTTP mode, is protocol knowledge and would belong in the core
behind a cargo feature if ever needed. clickhouse-connect does not use that
mode.
