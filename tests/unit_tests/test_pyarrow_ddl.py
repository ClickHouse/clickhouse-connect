# pylint: disable=duplicate-code

import pytest
import pyarrow as pa

from clickhouse_connect.driver.ddl import (
    arrow_schema_to_column_defs,
    create_table,
    create_table_from_arrow_schema,
)

pytest.importorskip("pyarrow")


def test_arrow_schema_to_column_defs_basic_mappings():
    schema = pa.schema(
        [
            ("i8", pa.int8()),
            ("i16", pa.int16()),
            ("i32", pa.int32()),
            ("i64", pa.int64()),
            ("u8", pa.uint8()),
            ("u16", pa.uint16()),
            ("u32", pa.uint32()),
            ("u64", pa.uint64()),
            ("f16", pa.float16()),
            ("f32", pa.float32()),
            ("f64", pa.float64()),
            ("s", pa.string()),
            ("ls", pa.large_string()),
            ("b", pa.bool_()),
        ]
    )

    col_defs = arrow_schema_to_column_defs(schema)

    assert [c.name for c in col_defs] == [
        "i8",
        "i16",
        "i32",
        "i64",
        "u8",
        "u16",
        "u32",
        "u64",
        "f16",
        "f32",
        "f64",
        "s",
        "ls",
        "b",
    ]

    type_names = [c.ch_type.name for c in col_defs]

    assert type_names == [
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Float32",
        "Float32",
        "Float64",
        "String",
        "String",
        "Bool",
    ]


def test_arrow_schema_to_column_defs_datetime_mappings():
    schema = pa.schema(
        [
            ("d32", pa.date32()),
            ("d64", pa.date64()),
            ("ts_s", pa.timestamp("s")),
            ("ts_ms", pa.timestamp("ms")),
            ("ts_us", pa.timestamp("us")),
            ("ts_ns", pa.timestamp("ns")),
            ("ts_tz", pa.timestamp("ms", tz="UTC")),
        ]
    )

    col_defs = arrow_schema_to_column_defs(schema)
    type_names = [c.ch_type.name for c in col_defs]

    assert type_names == [
        "Date32",
        "DateTime64(3)",
        "DateTime",
        "DateTime64(3)",
        "DateTime64(6)",
        "DateTime64(9)",
        "DateTime64(3, 'UTC')",
    ]


def test_arrow_schema_to_column_defs_unsupported_type_raises():
    schema = pa.schema(
        [
            ("lst", pa.list_(pa.int64())),
        ]
    )

    with pytest.raises(TypeError, match="Unsupported Arrow type"):
        arrow_schema_to_column_defs(schema)


def test_arrow_schema_to_column_defs_invalid_input_type():
    with pytest.raises(TypeError, match="Expected pyarrow.Schema"):
        arrow_schema_to_column_defs("not a schema")


def test_create_table_from_arrow_schema_builds_expected_ddl():
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("score", pa.float32()),
            ("flag", pa.bool_()),
        ]
    )

    ddl = create_table_from_arrow_schema(
        table_name="arrow_basic_test",
        schema=schema,
        engine="MergeTree",
        engine_params={"ORDER BY": "id"},
    )

    assert (
        ddl
        == "CREATE TABLE arrow_basic_test "
           "(id Int64, name String, score Float32, flag Bool) "
           "ENGINE MergeTree  ORDER BY id"
    )


def test_create_table_from_arrow_schema_matches_manual_create_table():
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
        ]
    )

    col_defs = arrow_schema_to_column_defs(schema)

    ddl_manual = create_table(
        table_name="arrow_compare_test",
        columns=col_defs,
        engine="MergeTree",
        engine_params={"ORDER BY": "id"},
    )

    ddl_wrapper = create_table_from_arrow_schema(
        table_name="arrow_compare_test",
        schema=schema,
        engine="MergeTree",
        engine_params={"ORDER BY": "id"},
    )

    assert ddl_manual == ddl_wrapper
