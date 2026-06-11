"""End-to-end query benchmark: client.query() vs query_rust() on one client.

Every line is a start-to-finish destination comparison: server execution,
HTTP transport, decompression, decode, and conversion, down two different
paths. The v1 path is clickhouse-connect as users call it. The rust path is
rust_client.query_rust. Decode-isolated numbers live in
bench_native_decode_strict.py. Run e2e_query_check.py first: timing means
nothing without the parity gate.

Method: one discarded warmup per cell (which also primes the User-Agent
integration tags the destination helpers mutate), then ITERS timed pairs
with AB/BA alternating order, reported as median with min..max.

Caveats printed with results: the v1 path sends wait_end_of_query=1 and the
rust path streams, by design of each path. Date and DateTime reach Arrow as
raw uint16/uint32 storage (DECODER_CONTRACT.md), so arrow-derived numpy and
pandas destinations carry raw ints for those columns where v1 carries
datetime64. The numbers attribute time as transport+server vs client decode,
not a true client/server split.

Usage: bench_query_e2e.py            full matrix
       bench_query_e2e.py --memprobe ENGINE DEST WORKLOAD COMPRESS  internal
"""

from __future__ import annotations

import os
import platform
import resource
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BASELINE_PATH = os.environ.get("CHC_BASELINE_PATH", REPO_ROOT)
sys.path.insert(0, BASELINE_PATH)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import _ch_core  # noqa: E402
import pandas as pd  # noqa: E402
import pyarrow as pa  # noqa: E402

import clickhouse_connect  # noqa: E402
from clickhouse_connect.driverc import dataconv  # noqa: E402
from rust_client import _decompressed_chunks, _mirrored_request, query_rust  # noqa: E402

ITERS = int(os.environ.get("BENCH_ITERS", "7"))


@dataclass(frozen=True)
class Workload:
    name: str
    query: str
    row_materialization: bool = True
    pandas_numpy: bool = True


WORKLOADS = (
    Workload("mixed_6col_1M", "SELECT id, val, name, flag, small_int, big_uint FROM bench_types LIMIT 1000000"),
    Workload("int_3col_10M", "SELECT id, small_int, big_uint FROM bench_types", False, False),
    Workload("string_1col_10M", "SELECT name FROM bench_types", False, False),
    Workload("temporal_3col_1M", "SELECT d, dt, dt64 FROM bench_temporal LIMIT 1000000"),
    Workload("temporal_3col_10M", "SELECT d, dt, dt64 FROM bench_temporal", False, False),
    Workload("dt64_tz_1M", "SELECT dt64_ny FROM bench_nullable LIMIT 1000000"),
    Workload("nullable_3col_1M", "SELECT n_int, n_float, n_str FROM bench_nullable LIMIT 1000000"),
    Workload("nullable_3col_10M", "SELECT n_int, n_float, n_str FROM bench_nullable", False, False),
)

MEM_WORKLOADS = ("mixed_6col_1M", "string_1col_10M")


def rust_numpy(client, query):
    table = query_rust(client, query).arrow_table()
    return {field.name: column.to_numpy(zero_copy_only=False) for field, column in zip(table.schema, table.columns)}


DESTINATIONS = (
    ("rows", "rows", lambda c, q: c.query(q).result_rows, lambda c, q: query_rust(c, q).result_rows),
    ("columns", "rows", lambda c, q: c.query(q).result_columns, lambda c, q: query_rust(c, q).result_columns),
    ("numpy", "numpy", lambda c, q: c.query_np(q), rust_numpy),
    ("pandas", "numpy", lambda c, q: c.query_df(q), lambda c, q: query_rust(c, q).arrow_table().to_pandas()),
    ("arrow", "always", lambda c, q: c.query_arrow(q), lambda c, q: query_rust(c, q).arrow_table()),
)


def summarize(times):
    return statistics.median(times), min(times), max(times)


def fmt(stats):
    med, lo, hi = stats
    return f"{med * 1000:8.1f} ms ({lo * 1000:7.1f}..{hi * 1000:7.1f})"


def bench_pair(v1_fn, rust_fn):
    """One warmup each, then ITERS timed pairs in alternating order."""
    v1_fn()
    rust_fn()
    v1_times, rust_times = [], []
    for i in range(ITERS):
        pair = ((v1_fn, v1_times), (rust_fn, rust_times))
        if i % 2:
            pair = pair[::-1]
        for fn, times in pair:
            start = time.perf_counter()
            fn()
            times.append(time.perf_counter() - start)
    return summarize(v1_times), summarize(rust_times)


def rust_streamed_batch(client, query):
    return query_rust(client, query).batch


def rust_sequential_batch(client, query):
    """Fetch fully, then decode: the buffered baseline for the same bytes."""
    response = _mirrored_request(client, query, None)
    try:
        data = b"".join(_decompressed_chunks(response))
    except BaseException:
        response.close()
        raise
    response.release_conn()
    return _ch_core.ColBatch.decode_native(data, has_block_info=bool(client.protocol_version))


def assert_encoding(client, expected):
    response = _mirrored_request(client, "SELECT 1", None)
    encoding = response.headers.get("content-encoding")
    b"".join(_decompressed_chunks(response))
    response.release_conn()
    assert encoding == expected, f"content-encoding {encoding!r}, expected {expected!r}"


def run_workload(client, workload):
    probe = query_rust(client, workload.query)
    rows = probe.batch.num_rows if probe.batch is not None else 0
    del probe
    print(f"\n### {workload.name} ({rows:,} rows)")

    for label, gate, v1_fn, rust_fn in DESTINATIONS:
        if gate == "rows" and not workload.row_materialization:
            continue
        if gate == "numpy" and not workload.pandas_numpy:
            continue
        v1_stats, rust_stats = bench_pair(lambda: v1_fn(client, workload.query), lambda: rust_fn(client, workload.query))
        ratio = v1_stats[0] / rust_stats[0] if rust_stats[0] else float("inf")
        print(f"    {label:8s} v1 {fmt(v1_stats)}  rust {fmt(rust_stats)}  speedup {ratio:5.2f}x")

    # Pipeline comparison, not pure overlap isolation: the buffered side
    # also pays the full-buffer join and a different decode entry point.
    seq_stats, stream_stats = bench_pair(
        lambda: rust_sequential_batch(client, workload.query),
        lambda: rust_streamed_batch(client, workload.query),
    )
    gain = (seq_stats[0] - stream_stats[0]) / seq_stats[0] * 100 if seq_stats[0] else 0.0
    print(f"    pipeline buffered {fmt(seq_stats)}  streamed {fmt(stream_stats)}  streamed gain {gain:4.1f}%")


def rss_mb():
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform != "darwin":
        peak *= 1024
    return peak / 1e6


def memprobe(engine, dest, workload_name, compress):
    query = next(w.query for w in WORKLOADS if w.name == workload_name)
    client = clickhouse_connect.get_client(host="localhost", port=8123, compress=compress if compress != "False" else False)
    if engine == "v1":
        result = client.query(query).result_rows if dest == "rows" else client.query_arrow(query)
    else:
        r = query_rust(client, query)
        result = r.result_rows if dest == "rows" else r.arrow_table()
    print(f"{rss_mb():.1f}")
    del result


def memory_snapshot():
    print("\n### peak RSS snapshot (single uncompressed run per cell, whole-process ru_maxrss in MB)")
    for workload_name in MEM_WORKLOADS:
        for dest in ("rows", "arrow"):
            line = f"    {workload_name:16s} {dest:6s}"
            for engine in ("v1", "rust"):
                proc = subprocess.run(
                    [sys.executable, os.path.abspath(__file__), "--memprobe", engine, dest, workload_name, "False"],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                line += f"  {engine} {float(proc.stdout.strip()):8.1f} MB"
            print(line)


def header(client, compress):
    settings = {"client_protocol_version": client.protocol_version}
    if client.compression and client._send_comp_setting:
        settings["enable_http_compression"] = "1"
    print(
        f"\n{'=' * 72}\n"
        f"compress={compress!r}  server {client.server_version}  iters {ITERS}\n"
        f"clickhouse-connect {clickhouse_connect.__version__} from {clickhouse_connect.__file__}\n"
        f"C extension {dataconv.__file__}\n"
        f"_ch_core {_ch_core.__version__} from {_ch_core.__file__}\n"
        f"pyarrow {pa.__version__}  pandas {pd.__version__}  python {platform.python_version()}\n"
        f"platform {platform.platform()} {platform.machine()}\n"
        f"mirrored settings {settings}  (v1 also sends wait_end_of_query=1, the rust path streams)"
    )


def main():
    for compress in (False, "lz4"):
        client = clickhouse_connect.get_client(host="localhost", port=8123, compress=compress)
        header(client, compress)
        assert_encoding(client, "lz4" if compress else None)
        for workload in WORKLOADS:
            run_workload(client, workload)
        client.close()
    memory_snapshot()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--memprobe":
        memprobe(*sys.argv[2:6])
    else:
        main()
