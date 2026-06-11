# POC results: Rust core decode path for clickhouse-connect

Measured results, type coverage, and caveats for the binding POC. Sources:
bench_native_decode_strict.py (decode
isolated, value-parity gated), e2e_query_check.py (end-to-end parity gate),
bench_query_e2e.py (end-to-end query vs query). All runs on localhost,
Apple Silicon, server 26.2.4.23, clickhouse-connect 1.2.0 with C extensions,
_ch_core 0.1.0 release build, Python 3.12.

## What was built

- ch-core-rs: zero-dependency Rust crate decoding FORMAT Native wire bytes
  into Arrow-shaped columnar buffers, with the ClickHouse type system
  (parsing, schema, per-block schema enforcement) implemented once.
- ch-core-py (_ch_core): PyO3 binding exposing ColBatch (decode_native,
  from_batches, to_python_rows/columns, column_data, Arrow C stream export)
  plus StreamDecoder, BlockDecoder, and PipeDecoder for streaming.
- rust_client.query_rust: end-to-end query over the existing
  clickhouse-connect transport with a producer thread and bounded queue;
  StreamDecoder.feed releases the GIL so transport and decode overlap.

## End-to-end query vs query (bench_query_e2e.py, 7 iters, medians)

Same client object, same query, start to finish: server execution, HTTP
transport, decompression, decode, conversion. v1 is client.query() and its
destination helpers as users call them. rust is query_rust. The arrow
destination compares different wire formats by design: v1 query_arrow asks
the server for FORMAT ArrowStream while the rust path decodes Native.

Uncompressed transport:

| workload          | rows  | columns | numpy | pandas | arrow |
|-------------------|-------|---------|-------|--------|-------|
| mixed_6col_1M     | 1.46x | 1.69x   | 5.80x | 1.38x  | 3.04x |
| temporal_3col_1M  | 1.65x | 2.04x   | 5.26x | 3.94x  | 4.98x |
| dt64_tz_1M        | 2.72x | 3.07x   | 3.63x | 4.32x  | 3.62x |
| nullable_3col_1M  | 1.49x | 1.93x   | 3.96x | 7.15x  | 2.82x |
| int_3col_10M      |       |         |       |        | 4.91x |
| string_1col_10M   |       |         |       |        | 3.66x |
| temporal_3col_10M |       |         |       |        | 7.69x |
| nullable_3col_10M |       |         |       |        | 2.74x |

Speedup is v1 median over rust median; blank cells are row-materialization
destinations skipped at 10M rows. Absolute numbers in the full output.
Notable absolutes: int_3col_10M arrow v1 209.5 ms vs rust 42.7 ms,
dt64_tz_1M rows v1 458.3 ms vs rust 168.3 ms.

Why the e2e arrow column beats the decode-isolated comparison: v1
non-streaming queries send wait_end_of_query=1 so the server buffers the
whole result before the first byte, and the fetch does not overlap decode.
query_rust streams with transport and decode overlapped.

lz4-compressed transport (localhost): the rust path loses its arrow lead
(0.18x to 0.93x) because the server-side lz4 compression of Native output
throttles the transfer to ~175 MB/s decompressed; v1 query_arrow's
ArrowStream avoids that wall. Row destinations stay rust-favored (1.05x to
2.72x) since both engines pay the same compressed Native transport.
Measured directly: reading the compressed int_3col_10M response takes
1.14 s before any client-side work; client decompression adds nothing
measurable on top (the rust path's chunk generator and httputil's produce
byte-identical output at the same speed). Compression on localhost is pure
cost; a realistic compressed comparison needs a real network.

Streamed pipeline vs buffer-then-decode, measured within the bench (same
bytes, same client): the streamed path is 25.1% faster on mixed_6col_1M,
29.5% on string_1col_10M, 24.2% on nullable_3col_10M, and near parity where
decode is already trivial relative to fetch (temporal workloads). This is a
whole-pipeline comparison (overlap plus the avoided full-buffer join), not
a pure overlap isolation.

Peak RSS snapshot (single uncompressed run per cell, whole-process
ru_maxrss):

| workload        | destination | v1      | rust    |
|-----------------|-------------|---------|---------|
| mixed_6col_1M   | rows        | 412 MB  | 431 MB  |
| mixed_6col_1M   | arrow       | 202 MB  | 152 MB  |
| string_1col_10M | rows        | 1183 MB | 1266 MB |
| string_1col_10M | arrow       | 382 MB  | 224 MB  |

Labeled a representative snapshot, not a memory profile. Row
materialization is dominated by Python object cost in both engines; the
columnar destinations hold one extra decoded copy in v1 but not in the
rust path.

## Decode-isolated results (bench_native_decode_strict.py, 5 iters)

Same Native bytes fed to both decoders, value parity asserted before timing.
Medians, decode plus destination conversion only:

| workload         | destination  | v1       | rust     | speedup |
|------------------|--------------|----------|----------|---------|
| mixed_6col_1M    | Python rows  | 141.2 ms | 144.6 ms | 0.98x   |
| mixed_6col_1M    | NumPy dict   | 174.5 ms | 25.6 ms  | 6.82x   |
| temporal_3col_1M | Python rows  | 160.5 ms | 100.4 ms | 1.60x   |
| temporal_3col_1M | NumPy dict   | 11.3 ms  | 0.4 ms   | 26.1x   |
| dt64_tz_1M       | Python rows  | 411.8 ms | 148.7 ms | 2.77x   |
| nullable_3col_1M | pandas NumPy | 186.6 ms | 18.6 ms  | 10.0x   |
| string_1col_10M  | Arrow table  | n/a      | 42.7 ms  | n/a     |

Reading: where the destination is Python objects, allocation dominates and
the engines converge, as the design predicted. Where the
destination is columnar, the Rust decode floor is 1 to 2 orders of magnitude
below the v1 row pipeline.

## Type coverage matrix

Supported by the core decoder (DECODER_CONTRACT.md, all Nullable-wrappable):

| ClickHouse type                   | Arrow export                                |
|-----------------------------------|---------------------------------------------|
| Bool/Boolean                      | boolean                                     |
| Int8/16/32/64, UInt8/16/32/64     | same-width integer                          |
| Float32/Float64                   | float/double                                |
| String                            | utf8 (binding renders invalid UTF-8 as hex) |
| FixedString(N)                    | fixed-size binary                           |
| Date                              | raw uint16 days since epoch                 |
| Date32                            | date32                                      |
| DateTime, DateTime('tz')          | raw uint32 epoch seconds                    |
| DateTime64(P), DateTime64(P,'tz') | timestamp s/ms/us/ns with tz                |

Everything else (Map, Array, Tuple, LowCardinality, Enum, Decimal, UUID,
IP types, JSON, Dynamic, aggregate states) is rejected at decode time with
`ValueError: Unsupported ClickHouse type '<type>' for column '<name>'`,
raised from the core's UnsupportedType through the binding. The parity gate
verifies the rejection path. Clean rejection is the POC boundary by design.

Python value conversion parity with v1 is exact for all supported types,
verified on 1M-row samples per table including NULL patterns, DST folds,
and sub-second tz-aware DateTime64.

## Protocol caveats

- The binding's has_block_info flag collapses protocol revisions: it can
  express revision 0 and revision 1 through 54453, but not the custom
  serialization markers of revision 54454 and above. Both query paths pin
  client_protocol_version 54405, which the transport capture confirmed is
  exactly what v1 client.query() negotiates today.
- At revision 0 the server omits the timezone from a bare DateTime('tz')
  type string while DateTime64(P,'tz') keeps it. At 54405 both carry it.
- v1 sends wait_end_of_query=1 on non-streaming queries, so the server
  buffers the result before the first byte. The rust path streams. This is
  a per-path design property, not a benchmark unfairness: each path is
  measured as users would run it.

## Observations that apply to v1 itself

- httputil ResponseSource.gen swallows read exceptions once any data has
  been received (httputil.py:248) and ends the stream instead. A mid-stream
  transport failure can surface as silent truncation. query_rust uses its
  own chunk generator that re-raises; v1 deserves the same fix.
- After a mid-stream failure the cancelled query holds the session lock
  briefly, so an immediate retry on the same client can raise
  SESSION_IS_LOCKED. Same class of behavior as v1 with a shared session.

## Known limitations

- The numbers attribute time as transport+server versus client decode, not
  a true client/server split. No per-phase instrumentation was added.
- No time-to-first-row claim is made: query_rust returns after full
  consumption by design. Row-streaming consumption is demonstrated
  architecturally by streaming_demo.py and is future work.
- Date and DateTime cross the Arrow boundary as raw integer storage, so
  arrow-derived pandas/numpy destinations carry ints where v1 carries
  datetime64. DateTime64 and Date32 carry real temporal types. Mapping
  Date/DateTime to date32/timestamp is future polish in the core.
- The ch-core-rs path dependency (`../../../ch-core-rs`) blocks sdist
  builds. Packaging is future work.
- LIMIT 0 queries take the same JSON-metadata branch v1 uses (the server
  sends zero Native bytes for them), with the schema parsed by the core
  from a synthesized zero-row Native header.
