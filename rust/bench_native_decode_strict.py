"""Strict Native decode benchmark for clickhouse-connect vs _ch_core.

This benchmark fetches ClickHouse FORMAT Native bytes once per workload, then
feeds the exact same bytes to both decoders. That avoids estimating decode CPU
as "full query time - raw_query time", which is useful but noisy on localhost.

The comparison is intentionally destination-specific:

* Python rows/columns measure Python-native materialization.
* NumPy/pandas measure the existing clickhouse-connect Native path.
* Arrow/polars/pandas Arrow measure the Rust columnar export path.

Set CHC_BASELINE_PATH to a clickhouse-connect checkout with compiled C
extensions. Defaults to this repo's root, which only works if the extensions
are built there.
"""

from __future__ import annotations

import os
import statistics
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BASELINE_PATH = os.environ.get("CHC_BASELINE_PATH", REPO_ROOT)
sys.path.insert(0, BASELINE_PATH)

import _ch_core  # noqa: E402
import pandas as pd  # noqa: E402
import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402

import clickhouse_connect  # noqa: E402
from clickhouse_connect.driver.query import QueryContext  # noqa: E402
from clickhouse_connect.driver.transform import NativeTransform  # noqa: E402
from clickhouse_connect.driverc import dataconv  # noqa: E402,F401
from tests.helpers import bytes_source  # noqa: E402

ITERS = int(os.environ.get("BENCH_ITERS", "5"))
ROW_LIMIT = int(os.environ.get("BENCH_ROW_LIMIT", "1000000"))


@dataclass(frozen=True)
class Workload:
    name: str
    query: str
    row_materialization: bool = True
    pandas_numpy: bool = True


WORKLOADS = (
    Workload(
        "mixed_6col_100k",
        "SELECT id, val, name, flag, small_int, big_uint FROM bench_types LIMIT 100000",
    ),
    Workload(
        "mixed_6col_1M",
        "SELECT id, val, name, flag, small_int, big_uint FROM bench_types LIMIT 1000000",
    ),
    Workload(
        "int_3col_10M",
        "SELECT id, small_int, big_uint FROM bench_types",
        row_materialization=False,
        pandas_numpy=False,
    ),
    Workload(
        "string_1col_10M",
        "SELECT name FROM bench_types",
        row_materialization=False,
        pandas_numpy=False,
    ),
    # Temporal: dt is a bare DateTime (revision-0 Native drops a DateTime's
    # zone, so both decoders see naive UTC); dt64_ny keeps its zone in the
    # type string at revision 0 and exercises the tz-aware path.
    Workload(
        "temporal_3col_1M",
        "SELECT d, dt, dt64 FROM bench_temporal LIMIT 1000000",
    ),
    Workload(
        "temporal_3col_10M",
        "SELECT d, dt, dt64 FROM bench_temporal",
        row_materialization=False,
        pandas_numpy=False,
    ),
    Workload(
        "dt64_tz_1M",
        "SELECT dt64_ny FROM bench_nullable LIMIT 1000000",
    ),
    Workload(
        "nullable_3col_1M",
        "SELECT n_int, n_float, n_str FROM bench_nullable LIMIT 1000000",
    ),
    Workload(
        "nullable_3col_10M",
        "SELECT n_int, n_float, n_str FROM bench_nullable",
        row_materialization=False,
        pandas_numpy=False,
    ),
)

TABLE_ROWS = 10_000_000


def ensure_tables(ch_client):
    """Create the benchmark tables on first run. Deterministic data."""
    specs = {
        "bench_temporal": f"""
            CREATE TABLE bench_temporal ENGINE = MergeTree ORDER BY id AS
            SELECT
                number AS id,
                toDate('2000-01-01') + (number % 30000)                              AS d,
                toDateTime('2000-01-01 00:00:00') + number                           AS dt,
                addMilliseconds(toDateTime64('2000-01-01 00:00:00.000', 3), number)  AS dt64,
                toDateTime('2000-01-01 00:00:00', 'America/New_York') + number       AS dt_ny
            FROM numbers({TABLE_ROWS})
        """,
        "bench_nullable": f"""
            CREATE TABLE bench_nullable ENGINE = MergeTree ORDER BY id AS
            SELECT
                number AS id,
                IF(number % 10 = 0, NULL, toInt64(number))                            AS n_int,
                IF(number % 10 = 3, NULL, number / 7)                                 AS n_float,
                IF(number % 10 = 7, NULL, concat('user_', toString(number % 100000))) AS n_str,
                addMicroseconds(
                    toDateTime64('2020-06-01 00:00:00.000000', 6, 'America/New_York'),
                    number
                ) AS dt64_ny
            FROM numbers({TABLE_ROWS})
        """,
    }
    for table, ddl in specs.items():
        if ch_client.command(f"EXISTS TABLE {table}"):
            if int(ch_client.command(f"SELECT count() FROM {table}")) == TABLE_ROWS:
                continue
            ch_client.command(f"DROP TABLE {table}")
        print(f"creating {table} ({TABLE_ROWS:,} rows) ...")
        ch_client.command(ddl)


client = clickhouse_connect.get_client(host="localhost", port=8123, compress=False)
transform = NativeTransform()


def timeit(fn: Callable[[], Any], iters: int = ITERS) -> float:
    fn()
    fn()
    samples = []
    for _ in range(iters):
        t0 = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t0)
    return statistics.median(samples)


def ms(seconds: float | None) -> str:
    if seconds is None:
        return "       n/a"
    return f"{seconds * 1000:9.1f} ms"


def speedup(existing: float | None, rust: float | None) -> str:
    if existing is None or rust is None or rust == 0:
        return "    n/a"
    return f"{existing / rust:6.2f}x"


def source(raw: bytes):
    return bytes_source(raw, chunk_size=len(raw))


# apply_server_tz=True replicates client.query() against a UTC server: naive
# UTC datetimes unless the column type carries its own zone. A bare
# QueryContext would fall through to the local zone and skew temporal parity.
def v1_rows(raw: bytes):
    return transform.parse_response(source(raw), QueryContext(apply_server_tz=True)).result_rows


def v1_columns(raw: bytes):
    return transform.parse_response(source(raw), QueryContext(apply_server_tz=True)).result_columns


def v1_numpy(raw: bytes):
    ctx = QueryContext(use_numpy=True, apply_server_tz=True)
    return transform.parse_response(source(raw), ctx).np_result


def v1_pandas(raw: bytes):
    ctx = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True, apply_server_tz=True)
    return transform.parse_response(source(raw), ctx).df_result


def rust_batch(raw: bytes):
    return _ch_core.ColBatch.decode_native(raw, has_block_info=False)


def rust_arrow(raw: bytes):
    return pa.table(rust_batch(raw))


def rust_rows(raw: bytes):
    return rust_batch(raw).to_python_rows()


def rust_columns(raw: bytes):
    return rust_batch(raw).to_python_columns()


def rust_numpy_dict(raw: bytes):
    table = rust_arrow(raw)
    return {field.name: column.to_numpy(zero_copy_only=False) for field, column in zip(table.schema, table.columns)}


def rust_pandas_numpy(raw: bytes):
    return rust_arrow(raw).to_pandas()


def rust_pandas_arrow(raw: bytes):
    return rust_arrow(raw).to_pandas(types_mapper=pd.ArrowDtype)


def rust_polars(raw: bytes):
    return pl.from_arrow(rust_arrow(raw))


def report_line(label: str, rust: float | None, existing: float | None):
    print(f"    {label:18s} rust {ms(rust)}  existing {ms(existing)}  speedup {speedup(existing, rust)}")


def native_bytes(query: str):
    return client.raw_query(query, fmt="Native")


def rust_arrow_total(query: str):
    return rust_arrow(native_bytes(query))


def rust_polars_total(query: str):
    return rust_polars(native_bytes(query))


def rust_pandas_arrow_total(query: str):
    return rust_pandas_arrow(native_bytes(query))


def rust_pandas_numpy_total(query: str):
    return rust_pandas_numpy(native_bytes(query))


def rust_rows_total(query: str):
    return rust_rows(native_bytes(query))


def check_parity(raw: bytes, name: str):
    """Apples-to-apples gate: both decoders must produce identical row values
    from the same Native bytes before any timing is trusted."""
    rust = rust_rows(raw)
    v1 = v1_rows(raw)
    assert len(rust) == len(v1), f"{name}: row count {len(rust)} != {len(v1)}"
    for i, (r, v) in enumerate(zip(rust, v1)):
        assert r == tuple(v), f"{name}: row {i} mismatch: rust={r!r} v1={v!r}"
    print(f"    parity OK ({len(rust):,} rows identical)")


def run_workload(workload: Workload):
    raw = native_bytes(workload.query)
    batch = rust_batch(raw)
    rows = batch.num_rows
    nbytes = len(raw)
    if rows > ROW_LIMIT:
        row_enabled = False
    else:
        row_enabled = workload.row_materialization

    print(f"\n### {workload.name} ({rows:,} rows, {nbytes / 1e6:.1f} MB Native)")
    if row_enabled:
        check_parity(raw, workload.name)

    rust_decode_t = timeit(lambda: rust_batch(raw))
    rust_arrow_t = timeit(lambda: rust_arrow(raw))
    rust_polars_t = timeit(lambda: rust_polars(raw))
    rust_pandas_arrow_t = timeit(lambda: rust_pandas_arrow(raw))

    print(f"    rust decode floor {ms(rust_decode_t)}")
    report_line("Arrow table", rust_arrow_t, None)
    report_line("Polars", rust_polars_t, None)
    report_line("pandas ArrowDtype", rust_pandas_arrow_t, None)

    if row_enabled:
        report_line("Python columns", timeit(lambda: rust_columns(raw)), timeit(lambda: v1_columns(raw)))
        report_line("Python rows", timeit(lambda: rust_rows(raw)), timeit(lambda: v1_rows(raw)))
    else:
        print("    Python columns     skipped for this row count")
        print("    Python rows        skipped for this row count")

    if workload.pandas_numpy:
        report_line("NumPy dict/array", timeit(lambda: rust_numpy_dict(raw)), timeit(lambda: v1_numpy(raw)))
        report_line("pandas NumPy", timeit(lambda: rust_pandas_numpy(raw)), timeit(lambda: v1_pandas(raw)))
    else:
        print("    NumPy/pandas NumPy skipped for this row count")

    if row_enabled:
        print("    end-to-end totals, including localhost fetch")
        report_line("pyarrow.Table", timeit(lambda: rust_arrow_total(workload.query)), timeit(lambda: client.query_arrow(workload.query)))
        report_line(
            "Polars",
            timeit(lambda: rust_polars_total(workload.query)),
            timeit(lambda: client.query_df_arrow(workload.query, dataframe_library="polars")),
        )
        report_line(
            "pandas ArrowDtype",
            timeit(lambda: rust_pandas_arrow_total(workload.query)),
            timeit(lambda: client.query_df_arrow(workload.query, dataframe_library="pandas")),
        )
        report_line(
            "pandas NumPy", timeit(lambda: rust_pandas_numpy_total(workload.query)), timeit(lambda: client.query_df(workload.query))
        )
        report_line(
            "Python rows", timeit(lambda: rust_rows_total(workload.query)), timeit(lambda: client.query(workload.query).result_rows)
        )


def main():
    print(
        f"clickhouse-connect {clickhouse_connect.__version__} from {clickhouse_connect.__file__}\n"
        f"C extension {dataconv.__file__}\n"
        f"_ch_core {_ch_core.__version__} from {_ch_core.__file__}\n"
        f"pyarrow {pa.__version__}  pandas {pd.__version__}  polars {pl.__version__}  iters {ITERS}"
    )
    ensure_tables(client)
    for workload in WORKLOADS:
        run_workload(workload)


if __name__ == "__main__":
    main()
