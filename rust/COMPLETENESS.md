# ch-core-rs Binding Completeness

This document tracks what the `clickhouse-connect` binding has exposed from
`ch-core-rs`. The upstream core tracker lives in
`/Users/joe/dev/ch-core-rs/COMPLETENESS.md`; this file is the binding-side
handoff for Python integration work.

## Context Handoff

- **Current state:** `_ch_core` has the query decode path bound through
  `ColBatch`, `StreamDecoder`, `BlockDecoder`, and Unix `PipeDecoder`. Both the
  query decode and insert encode paths are now wired into the driver behind the
  public `native_codec` selector. The Python serializer remains the default
  compatibility path. The temporary `transport_settings={"rust_insert": ...}`
  hook has been removed.
- **Codec selector:** clients take `native_codec` with values `python`
  (default), `rust`, and `rust_strict`. There is a matching common setting and
  the `CLICKHOUSE_CONNECT_NATIVE_CODEC` environment variable that seeds the
  default. The seam lives in `clickhouse_connect/driver/rustcodec.py`. `rust`
  routes ineligible query contexts and unsupported insert types to Python;
  `rust_strict` raises. Query decode makes the Rust vs Python choice before
  reading the response body, so there is no mid-stream query fallback.
- **Build note:** the streamed Rust decode path reads decompressed chunks
  straight from the response buffer, so the compiled Cython `ResponseBuffer`
  must expose `.gen`. `driverc/buffer.pxd` now declares `readonly object gen`,
  which requires a rebuild (`python setup.py build_ext --inplace`). Environments
  running the Rust query path against a stale `.so` will hit `AttributeError` on
  `.gen`.
- **Upstream pin:** `ch-core-rs` currently tracks encode against
  ClickHouse `v26.6.1.1193-stable`, protocol revision `54485`. HTTP Native
  inserts use `EncodeOptions { protocol_revision: 0 }`, so there is no
  `BlockInfo` preamble and no per-column custom-serialization marker.
- **Current type scope:** the first binding encoder targets the upstream
  encodable set: `Bool`, fixed-width numerics, floats, `String`,
  `FixedString(N)`, `Date`, `Date32`, `DateTime`, `DateTime64(P[, tz])`, `UUID`,
  `IPv4`, `IPv6`, `Enum8`/`Enum16`, `Decimal(P, S)`, `Nullable(T)`, and
  `LowCardinality(T)` where the upstream core permits it.
- **numpy/pandas:** `query_np`, `query_df`, and their block and row stream variants
  now route through the Rust codec via the zero-copy Arrow exit
  (`clickhouse_connect/driver/rustnumpy.py`). Per-column dtype conversion is driven by
  the driver's own `ClickHouseType` (np_type, tzinfo, nullability) so dtypes match the
  Python codec. The Arrow export is raw (Date is uint16 days, DateTime is uint32 seconds
  with the timezone dropped, Enum is raw ints), so the converters, not a naive
  `to_pandas()`, produce the final columns. Nullable numeric columns in `query_df` build
  pandas extension arrays directly from the Arrow buffers. LowCardinality columns route through the object
  exit regardless of inner type (values are correct there). Note: the Python codec truncates
  `LowCardinality(<numeric>)` numpy/pandas output to the dictionary length (`ArrayType._build_lc_column` in
  `datatypes/base.py` passes `count=len(index)` where it should be `count=len(keys)`), so there is no clean
  parity target for those rare suspicious types; the rust object exit returns the full column.
- **Recommended next:** benchmark A/B with one client on `native_codec="rust_strict"`
  and one on the default while expanding decoder type coverage. The Rust decoder now
  covers every scalar type in the supported set on the Python-object and column-data
  exits, including `UUID`, `Decimal`, `IPv4`, and `IPv6`, plus `Array(T)` over any
  supported element type in both directions. `Tuple(...)` and `Map(K, V)` are now
  covered on DECODE only (object, column-data, row, and Arrow exits): an unnamed
  tuple materializes as a Python `tuple`, a named tuple as a `dict` keyed by the
  element names, and a map as a `dict` (wire order, last duplicate key wins),
  matching the Python codec's default read format. `Nullable(Tuple(...))` and the
  zero-element `Tuple()` decode correctly (both are cases the Python codec itself
  mishandles, so the Rust path is the reference, not a parity target). Tuple/Map
  ENCODE is still unsupported: the binding's insert type parser has no Tuple/Map
  branch, so those types are rejected before any encode, and `rust` mode routes
  such inserts to Python while `rust_strict` raises. Query decode picks Rust or
  Python before reading the response body, so once a query is eligible there is no
  mid-stream fallback: an unsupported type raises mid-stream in both `rust` and
  `rust_strict` mode rather than routing to Python. Growing decoder coverage is the
  main lever for widening the eligible query set.
- **Deferred (Tuple/Map + LowCardinality object identity):** a `LowCardinality`
  value nested inside a `Tuple` or `Map` is rebuilt as a fresh Python object per
  occurrence rather than sharing one object across rows. The Python codec and the
  top-level `Array(LowCardinality)` path (via `new_array_dict_cache`) share
  identity, so this is a memory/allocation regression precisely for the LC dedup
  workload, though values compare equal. Unlike the Array case it is not a
  one-liner: each LC sub-column under one Tuple/Map needs its own slot cache.
  Track before flipping the default; acceptable while the codec is opt-in.
- **insert_df bulk encode (follow-up, ch-core-py workstream):** `insert_df` is correct
  under the Rust codec but not yet faster than Python. `encode_native_block` takes Python
  columnar values one value at a time. A buffer-protocol or `ArrowArrayStream` import entry
  for `encode_native_block` would let the encoder consume numpy/Arrow buffers without the
  per-value Python round-trip. Track this in `ch-core-py`.

## Future Public Opt-In Design

Status: implemented as `native_codec` (see Context Handoff above). This section is
retained as the design rationale.

The release-facing opt-in should be a driver-level Native codec selector rather
than a transport setting. `transport_settings` reads as HTTP headers and
transport behavior, so it is not a good public home for "choose the Python or
Rust Native implementation".

Recommended API shape:

- Add a client/common setting named `native_codec`.
- Add an environment variable, for example
  `CLICKHOUSE_CONNECT_NATIVE_CODEC=rust`.
- Support initial values:
  - `python`: current behavior. This should remain the first public default.
  - `rust`: prefer Rust for eligible client-managed `FORMAT Native` query and
    insert paths, with Python fallback only where fallback is safe before bytes
    are consumed or sent.
  - `rust_strict` or `rust-only`: require Rust and fail fast when Rust is not
    available or the type/path is unsupported.

Suggested precedence:

1. Per-call override, if a public override is later added.
2. Client constructor option, for example `get_client(native_codec="rust")`.
3. Environment variable.
4. Library default.

Scope should stay precise: `native_codec` applies only to client-managed
`FORMAT Native` encode/decode. It should not affect Arrow, raw inserts, raw
queries, JSON, Parquet, or caller-provided byte streams.

Fallback policy differs by direction:

- Inserts can fall back from Rust to Python before the first encoded chunk is
  yielded or sent. After bytes have been sent, local failures should surface
  rather than silently switching encoders.
- Query decode cannot generally rewind the HTTP response once bytes have been
  consumed. The client should choose Rust or Python before reading a response
  stream. Mid-stream Rust decode failures should surface as errors, especially
  in strict mode.

Deprecation path: ship `native_codec="python"` as the default, encourage early
adopters and CI/performance testing to use `native_codec="rust"` or the env var,
then flip the default only after query and insert parity are stable. Keep a
temporary `python` escape hatch when the default changes.

## Binding Checklist

- [x] Decode bindings for buffered and streamed Native query results.
- [x] Arrow C Data Interface exit for decoded query results.
- [x] Python object exits for decoded rows and columns.
- [x] Insert block encoder binding from Python columnar values to
      `ch_core_rs::ColBatch`.
- [x] Streaming sync insert source with producer-thread encoding and bounded
      queue backpressure.
- [x] Async insert source selection for the Rust transform path.
- [x] Client integration with opt-in selector and Python fallback.
- [x] Binding unit coverage for supported inserts, malformed values, and
      unsupported types.
- [x] Driver unit coverage for framing parity, fallback behavior, and producer
      errors.
- [x] Sync and async integration coverage against a live ClickHouse server.

## Notes

- The binding owns Python value policy. The upstream core owns Native framing,
  type metadata, and encoded block bytes.
- Python values must be copied into Rust-owned buffers before the GIL is
  released. Encoding with `ch_core_rs::native::encode::encode_block` can then run
  without the GIL.
- Unsupported binding-side conversion should raise a clear local exception. In
  non-strict opt-in mode the driver falls back to the Python serializer when
  Rust fails before any Rust-encoded bytes are sent. Single-block inserts are
  fully encoded before the first yield so fallback remains safe. Multi-block
  inserts keep the streaming overlap; once a Rust chunk has been yielded, later
  Rust failures surface as local insert errors instead of switching encoders
  mid-body. In strict mode it should surface the Rust-path failure.
- Server-source review at ClickHouse `v26.3.9.8-lts` confirmed that Native
  `DateTime64` payloads are signed Int64 ticks at declared scale and that Native
  Decimal deserialization does not enforce declared precision. The binding
  therefore floor/euclidean-scales pre-epoch DateTime64 objects and rejects
  over-precision Decimal values before encoding.
- `LowCardinality(Decimal...)`, `LowCardinality(Enum...)`, and
  `LowCardinality(DateTime64...)` are not part of the current upstream LC
  encodable set because the ClickHouse server forbids those inner types.
