# Rust core binding POC

Every ClickHouse client reimplements wire decoding and the type system
today. `ch-core-rs` is a shared Rust core that does it once: it decodes
`FORMAT Native` bytes into Arrow-shaped columnar memory, and each language
client wraps it with a thin binding. This directory is the Python proof of
concept: a PyO3 binding (`_ch_core`), an end-to-end `query_rust` path over
the existing clickhouse-connect transport, and the verification and
benchmark harnesses behind the measured results.

The mental model in five lines:

```
network bytes -> [decode in Rust, GIL released] -> ColBatch
                                                      |
                 +------------------------------------+
                 |                                    |
        Arrow C stream capsule              Python rows / columns
        zero copy, ~free                    one object per cell
        pyarrow / pandas / polars           list of tuples / lists
```

## Quick look

```python
import _ch_core
import pyarrow as pa

batch = _ch_core.ColBatch.decode_native(native_bytes, has_block_info=True)
batch.column_names, batch.num_rows
rows = batch.to_python_rows()
table = pa.RecordBatchReader.from_stream(batch).read_all()
```

Or end to end on an existing clickhouse-connect client, streamed with
transport and decode overlapped:

```python
from rust_client import query_rust

result = query_rust(client, "SELECT id, name FROM events ORDER BY id")
result.result_rows
result.arrow_table()
result.to_pandas()
```

## Results at a glance

Start-to-finish query vs query on the same client, localhost, medians of 7
runs, uncompressed transport. Full data, methodology, and caveats in
`RESULTS.md`:

| destination | speedup vs v1 |
|---|---|
| Python rows | 1.3x to 2.7x |
| Python columns | 1.7x to 3.1x |
| NumPy | 3.6x to 5.8x |
| pandas | 1.4x to 7.2x |
| Arrow vs `query_arrow` | 2.7x to 7.7x |

The Arrow path also used 25 to 42 percent less peak memory. Localhost
numbers, with scope notes in `RESULTS.md` that matter before quoting them.

## Supported types

Nothing, Bool, Int8 through Int64 plus Int128/256, UInt8 through UInt64 plus
UInt128/256, Float32/64, BFloat16, QBit, String,
FixedString, Date, Date32, DateTime, DateTime64 with precision and
timezone, Nullable, LowCardinality where ClickHouse permits it, Array, Tuple,
Map, Variant, the supported name-decoration aliases, and the function
signatures for `AggregateFunction` registered by the core. Dynamic query
decode is also supported, including nested shapes and Arrow's result-wide dense
union. Typed Dynamic children use their ordinary Python values, intrinsic NULL
uses `None`, and the Python object exits decode SharedVariant cells to the same
typed values as ordinary columns; AggregateFunction states and unsupported
descriptors stay exact Python `bytes`, and the Arrow exit keeps every shared
cell as `bytes` for schema stability. Variant uses `None`
for its intrinsic NULL, ordinary Python values for unambiguous alternatives,
and `typed_variant` for alternatives that share a Python type. Its Arrow exit
is the core's zero-copy dense union. Aggregate states materialize as exact
Python `bytes` and export zero-copy as Arrow LargeBinary. Dynamic insert
builds the driver's established String input column natively, with exact
`str(value)` parity (`None` becomes the literal `"NULL"`), so the server keeps
its setting-dependent text inference and both `native_codec="rust"` and
`"rust_strict"` insert Dynamic without a fallback. JSON query decode supports
the core's structured and text Native layouts, including typed, dynamic, and
shared paths. Python object exits reconstruct dictionaries with escaped path
segments preserved, while Arrow uses the core's structured zero-copy export.
JSON inserts accept dictionaries or JSON object strings and use the core's
text Native encoder; JSON also composes under Nullable, Array, Tuple, Map, and
Variant. ClickHouse 24.8-24.9 JSON inserts use the Python compatibility path in
`native_codec="rust"` and fail clearly in strict mode because those releases
require the legacy String column header. Other unsupported types, plus
unsupported aggregate signatures, are rejected at decode time with a clean
`ValueError` naming the column; malformed payloads raise a column-named
`ValueError` as well. Type coverage lives in the core, so new types land there
once and every binding gets them.

QBit object results materialize as fixed-length Python lists of floats, with
`None` at the parent level for nullable rows. Inserts accept row containers and
contiguous two-dimensional PEP 3118 buffers such as NumPy float32/float64
matrices. The buffer path builds one typed child allocation, and Arrow exports
the core's zero-copy FixedSizeList representation.

## Prerequisite: the core crate

The binding depends on `ch-core-rs`, pinned by release tag in
`ch-core-py/Cargo.toml`:

```toml
ch-core-rs = { git = "https://github.com/ClickHouse/ch-core-rs", tag = "v0.1.0" }
```

Release and CI builds resolve the tag directly, so the repository must be
reachable from the build environment. See `DISTRIBUTION_PLAN.md` at the repo
root for how the binding ships as the `clickhouse-connect-core` wheel.

To develop against a local core checkout, add an untracked
`.cargo/config.toml` at this repository's root that patches the git source to
your working tree:

```toml
[patch."https://github.com/ClickHouse/ch-core-rs"]
ch-core-rs = { path = "/path/to/ch-core-rs" }
```

With the patch in place, builds compile the core working tree as-is, which is
the intended inner loop for core changes. Remove or ignore the patch to build
what the tag pins.

The committed `Cargo.lock` must record the git source and revision for
`ch-core-rs`. Any cargo resolution while the patch is active rewrites that
entry to the path form in your working tree. Discard those lock changes and
never commit them. To regenerate the lock legitimately, for example after a
repin, move the patch aside first:

```sh
mv .cargo/config.toml .cargo/config.toml.disabled
cd rust && CARGO_NET_GIT_FETCH_WITH_CLI=true cargo fetch
mv ../.cargo/config.toml.disabled ../.cargo/config.toml
```

## Build and test

```sh
pip install maturin
maturin develop --release -m rust/ch-core-py/Cargo.toml
python -m pytest rust/ch-core-py/tests/
```

Test extras: `pytest`, and `pyarrow` for the Arrow round-trip tests.

If the shell exports `VIRTUAL_ENV` pointing at a different environment than
the one on `PATH`, maturin installs into the exported one. Set
`VIRTUAL_ENV=/path/to/repo/.venv` explicitly when in doubt.

## What is here

| file                            | purpose                                                                   |
|---------------------------------|---------------------------------------------------------------------------|
| `ch-core-py/`                   | the PyO3 binding crate, module `_ch_core`                                 |
| `rust_client.py`                | end-to-end `query_rust` over the existing clickhouse-connect transport    |
| `e2e_query_check.py`            | parity gate vs `client.query()` on a live server, run before benchmarking |
| `temporal_e2e_check.py`         | cell-by-cell temporal value policy check incl. named timezones            |
| `bench_native_decode_strict.py` | decode-isolated benchmark, value-parity gated                             |
| `bench_query_e2e.py`            | start-to-finish query vs query benchmark                                  |
| `results_*.txt`                 | raw benchmark outputs cited by `RESULTS.md`                               |
| `streaming_demo.py`             | sync and async streaming overlap demonstration                            |
| `BINDING_ARCHITECTURE.md`       | how the binding works and how to use it for best performance              |
| `RESULTS.md`                    | measured results, type coverage, caveats, known limitations               |

The live-server scripts expect ClickHouse on `localhost:8123` and a
clickhouse-connect checkout with compiled C extensions for the comparison
side, configurable via `CHC_BASELINE_PATH`.

## Where to read next

- `BINDING_ARCHITECTURE.md`: the layered design, the intake and exit paths,
  GIL rules, the streaming pattern, and the practical performance guidance.
- `RESULTS.md`: every measured number with methodology and the honest
  caveats, including the compression story and protocol-revision limits.
- The `ch-core-rs` repository: the core's own README, `ARCHITECTURE.md`,
  and `DECODER_CONTRACT.md`, the per-type wire and Arrow contract.

## Status

The Native codec supports query decode, streaming, Arrow export, Python
materialization, and insert encoding through the `clickhouse_connect`
`native_codec="rust"` and `native_codec="rust_strict"` client options. It
does not implement the TCP protocol. The binding still builds against a
local or git checkout of the private core and is not part of the default
package build.
