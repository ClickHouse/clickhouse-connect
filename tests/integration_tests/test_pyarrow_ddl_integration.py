from datetime import datetime, date, timezone

import pytest
import pyarrow as pa

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.ddl import (
    arrow_schema_to_column_defs,
    create_table,
    create_table_from_arrow_schema,
)

pytest.importorskip("pyarrow")


def test_arrow_create_table_and_insert(test_client: Client):
    if not test_client.min_version("20"):
        pytest.skip(
            f"Not supported server version {test_client.server_version}"
        )

    table_name = "test_arrow_basic_integration"

    test_client.command(f"DROP TABLE IF EXISTS {table_name}")

    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("score", pa.float32()),
            ("flag", pa.bool_()),
        ]
    )

    ddl = create_table_from_arrow_schema(
        table_name=table_name,
        schema=schema,
        engine="MergeTree",
        engine_params={"ORDER BY": "id"},
    )
    test_client.command(ddl)

    arrow_table = pa.table(
        {
            "id": [1, 2],
            "name": ["a", "b"],
            "score": [1.5, 2.5],
            "flag": [True, False],
        },
        schema=schema,
    )

    test_client.insert_arrow(table=table_name, arrow_table=arrow_table)

    result = test_client.query(
        f"SELECT id, name, score, flag FROM {table_name} ORDER BY id"
    )
    assert result.result_rows == [
        (1, "a", 1.5, True),
        (2, "b", 2.5, False),
    ]

    test_client.command(f"DROP TABLE IF EXISTS {table_name}")


def test_arrow_schema_to_column_defs(test_client: Client):
    table_name = "test_arrow_manual_integration"

    test_client.command(f"DROP TABLE IF EXISTS {table_name}")

    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
        ]
    )

    # check using the explicit helper path.
    col_defs = arrow_schema_to_column_defs(schema)

    ddl = create_table(
        table_name=table_name,
        columns=col_defs,
        engine="MergeTree",
        engine_params={"ORDER BY": "id"},
    )
    test_client.command(ddl)

    arrow_table = pa.table(
        {
            "id": [10, 20],
            "name": ["x", "y"],
        },
        schema=schema,
    )

    test_client.insert_arrow(table=table_name, arrow_table=arrow_table)

    result = test_client.query(f"SELECT id, name FROM {table_name} ORDER BY id")
    assert result.result_rows == [
        (10, "x"),
        (20, "y"),
    ]

    test_client.command(f"DROP TABLE IF EXISTS {table_name}")


def test_arrow_datetime_create_and_insert(test_client: Client):
    if not test_client.min_version("20"):
        pytest.skip(
            f"Not supported server version {test_client.server_version}"
        )

    table_name = "test_arrow_datetime_integration"

    test_client.command(f"DROP TABLE IF EXISTS {table_name}")

    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("event_date", pa.date32()),
            ("event_ts", pa.timestamp("ms")),
            ("event_ts_tz", pa.timestamp("ms", tz="UTC")),
        ]
    )

    ddl = create_table_from_arrow_schema(
        table_name=table_name,
        schema=schema,
        engine="MergeTree",
        engine_params={"ORDER BY": "id"},
    )
    test_client.command(ddl)

    arrow_table = pa.table(
        {
            "id": [1, 2],
            "event_date": [date(2025, 1, 1), date(2025, 1, 2)],
            "event_ts": [
                datetime(2025, 1, 1, 12, 0, 0, 123000),
                datetime(2025, 1, 1, 13, 0, 0, 456000),
            ],
            "event_ts_tz": [
                datetime(2025, 1, 1, 12, 0, 0, 123000, tzinfo=timezone.utc),
                datetime(2025, 1, 1, 13, 0, 0, 456000, tzinfo=timezone.utc),
            ],
        },
        schema=schema,
    )

    test_client.insert_arrow(table=table_name, arrow_table=arrow_table)

    result = test_client.query(
        f"SELECT id, event_date, event_ts, event_ts_tz "
        f"FROM {table_name} ORDER BY id"
    )
    rows = result.result_rows

    assert len(rows) == 2
    assert rows[0][0] == 1
    assert str(rows[0][1]) == "2025-01-01"
    assert rows[1][0] == 2
    assert str(rows[1][1]) == "2025-01-02"

    test_client.command(f"DROP TABLE IF EXISTS {table_name}")
