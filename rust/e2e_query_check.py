"""End-to-end parity gate: query_rust vs client.query on a live server.

Exact result equality for the three bench tables, on both an uncompressed
and an lz4 client, plus the LIMIT 0 and unsupported-type boundaries. Run
before bench_query_e2e.py: timing numbers mean nothing without this gate.
"""

from __future__ import annotations

import datetime as dt
import os
import sys

BASELINE_PATH = os.environ.get("CHC_BASELINE_PATH", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, BASELINE_PATH)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import clickhouse_connect  # noqa: E402

from rust_client import query_rust  # noqa: E402

ROW_LIMIT = 1_000_000

EPOCH_DATE = dt.date(1970, 1, 1)


def arrow_consistent(arrow_val, row_val):
    """Value equality under the documented Arrow mapping.

    Date exports as raw uint16 days since epoch and DateTime as raw uint32
    epoch seconds (DECODER_CONTRACT.md), so those compare via epoch
    arithmetic. Everything else compares directly; tz-aware datetimes
    compare by instant.
    """
    if isinstance(row_val, dt.datetime):
        if isinstance(arrow_val, int):
            return arrow_val == int(row_val.replace(tzinfo=dt.timezone.utc).timestamp())
        return arrow_val == row_val
    if isinstance(row_val, dt.date) and isinstance(arrow_val, int):
        return arrow_val == (row_val - EPOCH_DATE).days
    return arrow_val == row_val

CHECKS = (
    ("bench_types", f"SELECT id, val, name, flag, small_int, big_uint FROM bench_types ORDER BY id LIMIT {ROW_LIMIT}"),
    ("bench_temporal", f"SELECT d, dt, dt64 FROM bench_temporal ORDER BY id LIMIT {ROW_LIMIT}"),
    ("bench_nullable", f"SELECT n_int, n_float, n_str, dt64_ny FROM bench_nullable ORDER BY id LIMIT {ROW_LIMIT}"),
)


def check_table(client, label, query):
    rust = query_rust(client, query)
    v1 = client.query(query)

    assert rust.column_names == v1.column_names, f"{label}: column names {rust.column_names} != {v1.column_names}"
    rust_rows = rust.result_rows
    v1_rows = v1.result_rows
    assert len(rust_rows) == len(v1_rows) == ROW_LIMIT, f"{label}: row counts {len(rust_rows)} vs {len(v1_rows)}"
    assert rust_rows == v1_rows, f"{label}: row values differ"
    rust_cols = rust.result_columns
    v1_cols = v1.result_columns
    assert [list(c) for c in rust_cols] == [list(c) for c in v1_cols], f"{label}: column values differ"

    table = rust.arrow_table()
    assert table.num_rows == ROW_LIMIT, f"{label}: arrow row count {table.num_rows}"
    assert table.column_names == list(rust.column_names), f"{label}: arrow names {table.column_names}"
    for idx, name in enumerate(table.column_names):
        spot = table.column(idx).to_pylist()[:1000]
        row_vals = [row[idx] for row in rust_rows[:1000]]
        for arrow_val, row_val in zip(spot, row_vals):
            assert arrow_consistent(arrow_val, row_val), f"{label}.{name}: arrow {arrow_val!r} vs row {row_val!r}"

    print(f"    {label}: parity OK ({ROW_LIMIT:,} rows, rows+columns+arrow)")


def check_limit_zero(client):
    r = query_rust(client, "SELECT id, name FROM bench_types LIMIT 0")
    assert r.column_names == ("id", "name"), f"LIMIT 0 names {r.column_names}"
    assert r.result_rows == []
    table = r.arrow_table()
    assert table.num_rows == 0 and table.column_names == ["id", "name"], "LIMIT 0 arrow shape"
    v1 = client.query("SELECT id, name FROM bench_types LIMIT 0")
    assert r.column_names == v1.column_names
    print("    LIMIT 0: schema-bearing empty result OK")


def check_unsupported(client):
    try:
        query_rust(client, "SELECT map('k', 13) AS m")
    except ValueError as e:
        assert "Unsupported ClickHouse type" in str(e), str(e)
        print(f"    unsupported type: clean ValueError OK ({e})")
        return
    raise AssertionError("unsupported type did not raise ValueError")


def main():
    for compress in (False, "lz4"):
        client = clickhouse_connect.get_client(host="localhost", port=8123, compress=compress)
        for table in ("bench_types", "bench_temporal", "bench_nullable"):
            if not client.command(f"EXISTS TABLE {table}"):
                raise SystemExit(f"{table} missing: run bench_native_decode_strict.py first to create it")
        print(f"compress={compress!r}")
        for label, query in CHECKS:
            check_table(client, label, query)
        check_limit_zero(client)
        check_unsupported(client)
        client.close()
    print("all parity checks passed")


if __name__ == "__main__":
    main()
