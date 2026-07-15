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
UInt128/256, Float32/64, BFloat16, String,
FixedString, Date, Date32, DateTime, DateTime64 with precision and
timezone, Nullable, LowCardinality where ClickHouse permits it, Array, Tuple,
Map, Variant, the supported name-decoration aliases, and the function
signatures for `AggregateFunction` registered by the core. Variant uses `None`
for its intrinsic NULL, ordinary Python values for unambiguous alternatives,
and `typed_variant` for alternatives that share a Python type. Its Arrow exit
is the core's zero-copy dense union. Aggregate states materialize as exact
Python `bytes` and export zero-copy as Arrow LargeBinary. Dynamic and other
unsupported types, plus unsupported aggregate signatures, are rejected at
decode time with a clean `ValueError` naming the column. Type coverage lives
in the core, so new types land there once and every binding gets them.

## Prerequisite: the core crate

The binding depends on `ch-core-rs`, a separate repository that is private
for now. Ask internally for access. Two ways to consume it, per the core's
own README:

Local path, what this branch declares. Clone the core as a **sibling of
this repository's checkout**:

```
parent/
  clickhouse-connect/   this repo
  ch-core-rs/           the core
```

matching the declared dependency in `ch-core-py/Cargo.toml`:

```toml
ch-core-rs = { path = "../../../ch-core-rs" }
```

Or switch the dependency to a pinned git revision if you have repo access
over ssh:

```toml
ch-core-rs = { git = "ssh://git@github.com/ClickHouse/ch-core-rs.git", rev = "<commit>" }
```

with a local `[patch]` in `.cargo/config.toml` to override back to a local
checkout during development. Without one of these, `cargo` and `maturin`
builds of `rust/ch-core-py` fail to resolve the dependency. Proper
packaging, sdist-safe and registry-published, is future work and out of
POC scope.

## Build and test

```sh
pip install maturin
cd rust/ch-core-py
maturin develop --release
cd ../..
python -m pytest rust/ch-core-py/tests/test_bindings.py
```

Test extras: `pytest`, and `pyarrow` for the Arrow round-trip tests.

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
