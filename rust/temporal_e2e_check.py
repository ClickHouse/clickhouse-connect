"""End-to-end temporal check: live ClickHouse -> Native -> _ch_core -> verify.

Validates the binding's temporal value policy (Date/Date32/DateTime/DateTime64,
naive vs tz-aware, DateTime64 precision) against:
  1. clickhouse-connect v1's own decode (value-level ground truth)
  2. pyarrow (via the zero-copy __arrow_c_stream__ export)

The v1 baseline is pinned to the compiled source tree and we hard-assert the C
extension loaded, so we never silently compare against pure-Python v1.
"""
import os
import sys

sys.path.insert(0, os.environ.get("CHC_BASELINE_PATH", os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))))

import _ch_core  # noqa: E402
import pyarrow as pa  # noqa: E402

import clickhouse_connect  # noqa: E402
from clickhouse_connect.driverc import dataconv  # noqa: E402,F401  (hard-load C ext)

client = clickhouse_connect.get_client(host="localhost", port=8123)

# Raw HTTP `FORMAT Native` with no protocol version (revision 0) makes the server
# drop the timezone from a bare DateTime type string, so it would decode naive.
# Requesting this protocol version (clickhouse-connect's PROTOCOL_VERSION_WITH_LOW_CARD)
# preserves the timezone and adds the block-info preamble, while staying below the
# custom-serialization revision (54454) that the bool `has_block_info` flag cannot
# express. Decode the resulting stream with has_block_info=True.
PROTOCOL_VERSION = 54405

# A spread of every temporal shape the core decodes: naive Date/Date32 (incl.
# pre-epoch), bare/UTC/named-zone DateTime, DateTime64 at precision 3/6/9 with
# and without a zone, and a nullable DateTime64.
QUERY = """
SELECT
    toDate('1970-01-01') + (number % 60000)                          AS d,
    toDate32('1925-01-01') + (number % 130000)                       AS d32,
    toDateTime('1970-01-01 00:00:00') + number                       AS dt,
    toDateTime('2000-01-01 00:00:00', 'UTC') + number                AS dt_utc,
    toDateTime('2000-01-01 00:00:00', 'America/New_York') + number   AS dt_ny,
    addMilliseconds(toDateTime64('2000-01-01 00:00:00.000', 3), number)            AS dt64_3,
    addMicroseconds(toDateTime64('2000-01-01 00:00:00.000000', 6, 'UTC'), number)  AS dt64_6_utc,
    toDateTime64('2000-01-01 00:00:00.000000000', 9, 'America/New_York') + number  AS dt64_9_ny,
    if(number % 7 = 0, NULL, addMilliseconds(toDateTime64('2010-06-15 08:00:00.000', 3), number)) AS dt64_nil
FROM numbers({n})
"""

N = 50_000


def main():
    q = QUERY.format(n=N)

    raw = client.raw_query(q, fmt="Native", settings={"client_protocol_version": PROTOCOL_VERSION})
    print(f"fetched {len(raw):,} bytes of Native data")

    batch = _ch_core.ColBatch.decode_native(raw, has_block_info=True)
    print(f"_ch_core decoded: {batch.num_rows} rows x {batch.num_columns} cols")
    print(f"  types: {batch.column_type_names}")
    assert batch.num_rows == N, f"row count {batch.num_rows} != {N}"

    v1 = client.query(q, column_oriented=True)
    v1_cols = v1.result_columns
    v1_names = v1.column_names
    assert list(batch.column_names) == list(v1_names), (
        f"name mismatch: {batch.column_names} vs {v1_names}"
    )

    # Value-level comparison: rust to_python_columns vs v1, cell by cell.
    chc_cols = batch.to_python_columns()
    mismatches = 0
    for ci, name in enumerate(v1_names):
        rust_col = list(chc_cols[ci])
        v1_col = list(v1_cols[ci])
        if rust_col == v1_col:
            print(f"OK   {name:12s} {len(rust_col):,} values match v1 (sample {rust_col[1]!r})")
            continue
        mismatches += 1
        for ri, (a, b) in enumerate(zip(rust_col, v1_col)):
            if a != b:
                print(f"FAIL {name:12s} row {ri}: rust={a!r} v1={b!r}")
                break

    # to_python_rows must agree with the columnar view it is built from.
    rows = batch.to_python_rows()
    assert len(rows) == N
    assert tuple(chc_cols[ci][13] for ci in range(batch.num_columns)) == tuple(rows[13])

    # Arrow zero-copy export sanity: Date32 maps to a real Arrow date32 and
    # round-trips to the same Python dates v1 produced.
    table = pa.table(batch)
    assert table.num_rows == N
    assert table.schema.field("d32").type == pa.date32()
    assert table.column("d32").to_pylist() == list(v1_cols[v1_names.index("d32")])
    print("OK   arrow date32 export matches v1")

    if mismatches == 0:
        print(f"\nTEMPORAL END-TO-END: PASS ({batch.num_columns} columns match v1 exactly)")
    else:
        print(f"\nTEMPORAL END-TO-END: FAIL ({mismatches} columns differ)")
        sys.exit(1)


if __name__ == "__main__":
    main()
