# The _ch_core binding

How the PyO3 binding layer works and how ClickHouse Connect integrates it.
The core's wire and Arrow contract is documented in ch-core-rs
`DECODER_CONTRACT.md`. This document covers the binding crate `ch-core-py`
and the integration layer above it.

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
            /                     |                      \
      [Arrow exit]       [Column buffer exit]      [Python object exit]
   __arrow_c_stream__      column_buffers          to_python_rows / columns
   pointer handoff         read-only buffers       one PyObject per cell
            |                     |                      |
   Arrow consumers       memoryview / np.frombuffer   tuples / lists
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
   The driver uses the private column buffer exit for primitive numeric
   NumPy/Pandas conversion. It exposes decoded memory without an Arrow
   Python package. Converters that haven't migrated still use the Arrow exit.
   Use these exits for buffer-compatible dataframe columns. Never round-trip
   those columns through Python objects to reach a dataframe.

3. **The GIL is released wherever Python memory is not touched.** Intake
   copies each fed chunk out of Python-owned memory, then decodes with the
   GIL released. That is what lets a producer thread keep reading the
   socket while Rust decodes.

The division of labor is strict:

| layer | owns | examples |
|---|---|---|
| core (ch-core-rs) | ClickHouse knowledge | wire framing, type parsing, schema rules, Arrow layout |
| binding (ch-core-py) | Python value policy | what a `DateTime64(6,'America/New_York')` becomes, GIL rules, exception types |
| integration (driver `rustcodec.py`) | transport | HTTP, decompression, threads, queues, connection cleanup |

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

### Private column buffers

`COLUMN_BUFFER_API_VERSION = 1` identifies an additive capability alongside
the unchanged binding API 3, first packaged in core 0.2.1. The driver checks
both versions before it selects either Rust codec mode. Non-nullable
8-64-bit integer, Float32/64, and Boolean converters consume these buffers directly.
Extended Pandas output uses the public `IntegerArray` values/mask constructor
for nullable integers and float64 arrays with NaN for nullable Float32/64.
Primitive numeric SimpleAggregateFunction aliases retain their existing null-promotion
rules. BFloat16 converters widen the little-endian words to float32 in bulk.
Nullable extended Pandas BFloat16 output uses the public `FloatingArray`
values/mask constructor and preserves the active Pandas NaN policy.
Interval converters view signed Int64 counts and reuse the
nullable integer adapter for extended Pandas output. Existing nested alias
representations and errors are preserved. Scalar Date, Date32, DateTime,
DateTime64, Time, and Time64 adapters widen physical integers or view Int64
buffers with the units from driver metadata. They preserve timezone decisions,
precision validation, and nullable duration output. Outer nullable
SimpleAggregateFunction aliases of Time64 use this temporal path, so values keep
their integer ticks and NULLs become NaT. Conversion of nullable scalar
DateTime64(9), including SimpleAggregateFunction aliases, reads the buffers to
preserve nanoseconds. NumPy object fields contain numpy.datetime64 scalars in
UTC without timezone metadata, with None only at SQL NULL positions. This also
preserves valid NaT scalars separately from SQL NULL. Pandas retains the existing
all-null and timezone policies.
Timezone-local values outside the nanosecond range keep the original object
conversion, including its Pandas-version-dependent range errors.
Other nullable dates and timestamps keep their Python-object conversion paths.
Scalar DateTime64 precision validation also covers nullable and alias forms.
Other converters keep their Arrow or
Python-object exits, and the driver still requires PyArrow for NumPy/Pandas
queries during this migration.

The integer adapter can retain a read-only Rust values buffer and an owned
Boolean null mask. The existing result assembly preserves writable public
DataFrames. Converters assemble multiple chunks in order and preserve typed
empty outputs. Ordinary nullable integer and Boolean object paths stay in use
where they define the output policy.

`ColBatch.column_buffers(index)` returns a list
of read-only descriptors, one per decoded chunk. Supported types are
Int8/16/32/64, UInt8/16/32/64, Float32/64, Bool, BFloat16, Date, Date32,
DateTime, DateTime64, Time, Time64, and all Interval types, including their
nullable forms and SimpleAggregateFunction aliases. Array chains ending in
Time or Time64 are also supported, with optional nullable leaves and
SimpleAggregateFunction aliases at any level. LowCardinality(Time) supports
an optional nullable inner type and SimpleAggregateFunction aliases around
the column, inner type, or nullable leaf. Other types return `None`.
Consumers must treat an unrecognized `kind` as unsupported. Invalid indices
and malformed supported storage raise instead of selecting an object fallback.

Each descriptor contains `kind`, `itemsize`, `byteorder`, `length` in rows,
`null_count`, the optional buffers `values`, `validity`, and `offsets`, and
an optional `child` descriptor. Scalar descriptors always carry `values` and
report `None` for offsets and child. Logical ClickHouse types stay in the batch
schema. Scalar layouts are:

| ClickHouse type | `kind` | `itemsize` in bytes | `byteorder` |
|---|---|---|---|
| Int8/16/32/64, UInt8/16/32/64, Float32/64 | Lowercase type name | Primitive width | Host order, `little` or `big` |
| Date | `uint16` | 2 | Host order |
| Date32, Time | `int32` | 4 | Host order |
| DateTime | `uint32` | 4 | Host order |
| DateTime64, Time64, Interval types | `int64` | 8 | Host order |
| BFloat16 | `bfloat16` | 2 | Always `little` |
| Bool | `bool_bitmap` | 0, one bit per row | `not-applicable` |

Temporal and interval buffers contain raw counts. Their units, precision,
and timezone come from the schema. BFloat16 exposes raw two-byte words, not
NumPy float16 values. Bool values contain exactly `ceil(length / 8)` bytes,
least significant bit first, where 1 means true. Consumers must unpack
those bits before constructing a NumPy Boolean array. Unused tail bits have
no meaning.

The buffers expose contiguous read-only bytes through Python's buffer
protocol, so `memoryview(descriptor.values)` and `np.frombuffer` need no
Arrow package. Validity contains exactly `ceil(length / 8)` bytes, with one
bit per row, least significant bit first, where 1 means valid. Unused tail
bits have no meaning. Ignore values in null rows. Descriptors have no Python
constructor or writable properties.

Array descriptors report `kind="array"`, `itemsize=0`, and
`byteorder="not-applicable"`. Their length is the number of arrays, their
null count is zero, and values and validity are `None`. The offsets buffer
contains exactly `length + 1` native-endian signed Int64 values. Offsets
start at zero, never decrease, and count elements in the child descriptor.
The last offset equals `child.length`. Empty rows repeat an offset. A
preserved empty chunk has the single offset `[0]` and a typed empty child.

The child describes flattened elements and may itself be an array. For
example, `Array(Array(Nullable(Time64(9))))` has two offset levels and an
Int64 leaf with its own validity bitmap. Nullability applies only to the
leaf. Nullable array nodes and other nested types aren't supported.
Each chunk has its own offsets starting at zero. Consumers must rebuild
each chunk's rows before concatenating results.

Dictionary descriptors report `kind="dictionary"`, `itemsize=4`, and host
byte order. Their `values` buffer contains signed Int32 indices, their
`validity` describes rows, and their `child` is an Int32 descriptor of raw
Time dictionary values. `offsets` is `None`. The dictionary child has no
validity, offsets, or further child. Indices and dictionary values keep
their original chunk-local order. Consumers must gather values separately
for each chunk before concatenating results.

For a nullable dictionary, index zero denotes null and the corresponding
row-validity bit is zero. The sentinel dictionary entry remains in the
child, and its stored value doesn't determine nullness. A valid zero-valued
Time has its own nonzero index. For a nonnullable dictionary, index zero is
an ordinary valid index. A preserved empty chunk has empty indices and an
empty typed dictionary, with an empty validity bitmap if nullable.

Each buffer holds an `Arc` to its source chunk. Views and derived slices
remain valid after the batch or decoder is dropped. A retained view pins
the whole source chunk, including other columns, but doesn't pin other
chunks or transport resources. Array offset and child buffers, and dictionary
index, validity, and value buffers each retain the same source chunk
independently, without retaining parent descriptors.
The final view releases that ownership.
Chunks are never concatenated here. A supported schema with no chunks
returns `[]`. A preserved empty chunk gets a zero-length descriptor.

Array descriptor construction checks offset count, start, ordering, bounds,
final child extent, and buffer byte lengths. It also validates each child.
Each buffer stores a root column index, array depth, and buffer selection.
Exports re-resolve that selection through immutable storage. They don't
repeat the offset scan, and scalar exports don't scan array offsets.

Dictionary construction validates index and bitmap counts, initialized
storage, dictionary value type, and null-slot consistency. Every index must
be nonnegative and less than the dictionary length, including null rows.
This scans rows once per descriptor construction. Buffer exports re-resolve
the index, validity, or dictionary value selection without repeating the
scan. Adapters should call `column_buffers` once per column per batch and
reuse the descriptors for both array and dictionary conversion.

This capability doesn't change the driver converters, dependency requirements,
or public output writeability. Later adapters must copy where the public
output contract requires writable arrays.

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
the driver's `rustcodec.py`. The essentials of the protocol:

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
