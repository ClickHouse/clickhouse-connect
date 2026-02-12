from datetime import time, timedelta
from typing import Any, List

import pytest

from tests.integration_tests.conftest import TestConfig


# Module-level version and cloud checks
@pytest.fixture(autouse=True, scope="module")
def module_setup_and_checks(test_client, test_config: TestConfig):
    """Check prerequisites for Time/Time64 type tests."""
    if test_config.cloud:
        pytest.skip("Time/Time64 types require settings change, but settings are locked in cloud")

    version_str = test_client.query("SELECT version()").result_rows[0][0]
    major, minor, *_ = map(int, version_str.split("."))
    if (major, minor) < (25, 6):
        pytest.skip("Time and Time64 types require ClickHouse 25.6+")


class TimeTestData:
    """Centralized test data for time-related tests."""

    TIME_STRINGS = [
        "000:00:05",
        "-000:00:02",
        "001:02:03",
        "000:00:00",
    ]

    TIME_DELTAS = [
        timedelta(seconds=5),
        timedelta(seconds=-2),
        timedelta(hours=1, minutes=2, seconds=3),
        timedelta(seconds=0),
    ]

    TIME_INTS = [5, -2, 3723, 0]

    TIME64_US_STRINGS = [
        "001:02:03.123456",
        "-000:00:05.500000",
        "000:00:00.000001",
    ]

    TIME64_US_DELTAS = [
        timedelta(hours=1, minutes=2, seconds=3, microseconds=123456),
        timedelta(seconds=-5, microseconds=-500000),
        timedelta(microseconds=1),
    ]

    TIME64_US_TICKS = [3723123456, -5500000, 1]

    TIME64_NS_STRINGS = [
        "001:02:03.123456789",
        "-000:00:05.500000000",
        "000:00:00.000000001",
    ]

    TIME64_NS_DELTAS = [
        timedelta(hours=1, minutes=2, seconds=3, microseconds=123456),
        timedelta(seconds=-5, microseconds=-500000),
        timedelta(0),
    ]

    TIME64_NS_TICKS = [3723123456789, -5500000000, 1]


class ClockTimeData:
    """Test data for datetime.time objects."""

    TIME_OBJS = [
        time(0, 0, 5),
        time(1, 2, 3),
        time(23, 59, 59),
        time(0, 0, 0),
    ]

    TIME_DELTAS = [
        timedelta(seconds=5),
        timedelta(hours=1, minutes=2, seconds=3),
        timedelta(hours=23, minutes=59, seconds=59),
        timedelta(0),
    ]

    TIME64_US_OBJS = [
        time(1, 2, 3, 123456),
        time(0, 0, 0, 1),
        time(23, 59, 59, 999999),
    ]

    TIME64_NS_OBJS = [
        time(1, 2, 3, 123456),
        time(0, 0, 0),
        time(23, 59, 59, 123000),
    ]


TABLE_NAME = "temp_time_test"

STANDARD_TIME_TABLE_SCHEMA = [
    "id UInt32",
    "t Time",
    "t_nullable Nullable(Time)",
    "t64_us Time64(6)",
    "t64_us_nullable Nullable(Time64(6))",
    "t64_ns Time64(9)",
    "t64_ns_nullable Nullable(Time64(9))",
]


def create_test_row(row_id: int, time_val: Any) -> List[Any]:
    """Create a test row with the given ID and time value for all columns."""
    return [row_id, time_val, time_val, time_val, time_val, time_val, time_val]


def create_nullable_test_row(row_id: int, **column_values) -> List[Any]:
    """Create a test row with specific values for nullable columns."""
    return [
        row_id,
        column_values.get("t", "00:00:00"),
        column_values.get("t_nullable", None),
        column_values.get("t64_us", "00:00:00.000000"),
        column_values.get("t64_us_nullable", None),
        column_values.get("t64_ns", "00:00:00.000000000"),
        column_values.get("t64_ns_nullable", None),
    ]


def test_time_native_format(param_client, call, table_context):
    """Test Time round-trip with native timedelta format."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        rows = [create_test_row(i, td) for i, td in enumerate(TimeTestData.TIME_DELTAS)]
        call(param_client.insert, TABLE_NAME, rows)

        result = call(param_client.query, f"SELECT t FROM {TABLE_NAME} ORDER BY id")
        result_values = [row[0] for row in result.result_rows]
        assert result_values == TimeTestData.TIME_DELTAS


def test_time_string_format(param_client, call, table_context):
    """Test Time round-trip with string format."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        rows = [create_test_row(i, s) for i, s in enumerate(TimeTestData.TIME_STRINGS)]
        call(param_client.insert, TABLE_NAME, rows)

        result = call(param_client.query, f"SELECT t FROM {TABLE_NAME} ORDER BY id", query_formats={"Time": "string"})
        result_values = [row[0] for row in result.result_rows]
        assert result_values == TimeTestData.TIME_STRINGS


def test_time_int_format(param_client, call, table_context):
    """Test Time round-trip with integer format."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        rows = [create_test_row(i, val) for i, val in enumerate(TimeTestData.TIME_INTS)]
        call(param_client.insert, TABLE_NAME, rows)

        result = call(param_client.query, f"SELECT t FROM {TABLE_NAME} ORDER BY id", query_formats={"Time": "int"})
        result_values = [row[0] for row in result.result_rows]
        assert result_values == TimeTestData.TIME_INTS


@pytest.mark.parametrize(
    "column,strings,deltas,ticks,type_name",
    [
        (
            "t64_us",
            TimeTestData.TIME64_US_STRINGS,
            TimeTestData.TIME64_US_DELTAS,
            TimeTestData.TIME64_US_TICKS,
            "Time64",
        ),
        (
            "t64_ns",
            TimeTestData.TIME64_NS_STRINGS,
            TimeTestData.TIME64_NS_DELTAS,
            TimeTestData.TIME64_NS_TICKS,
            "Time64",
        ),
    ],
)
def test_time64_all_formats(
    param_client,
    call,
    table_context,
    column: str,
    strings: List[str],
    deltas: List[timedelta],
    ticks: List[int],
    type_name: str,
):
    """Test Time64 round-trip with all supported formats."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        rows = []
        for i, string_val in enumerate(strings):
            row = [i, timedelta(0), None, timedelta(0), None, timedelta(0), None]
            if column == "t64_us":
                row[3] = string_val
            elif column == "t64_ns":
                row[5] = string_val
            rows.append(row)

        call(param_client.insert, TABLE_NAME, rows)

        result = call(param_client.query, f"SELECT {column} FROM {TABLE_NAME} ORDER BY id")
        result_values = [row[0] for row in result.result_rows]
        assert result_values == deltas

        result = call(param_client.query, f"SELECT {column} FROM {TABLE_NAME} ORDER BY id", query_formats={type_name: "string"})
        result_values = [row[0] for row in result.result_rows]
        assert result_values == strings

        result = call(param_client.query, f"SELECT {column} FROM {TABLE_NAME} ORDER BY id", query_formats={type_name: "int"})
        result_values = [row[0] for row in result.result_rows]
        assert result_values == ticks


def test_nullable_time_columns(param_client, call, table_context):
    """Test that nullable columns handle None values correctly."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        rows = [
            create_nullable_test_row(
                0,
                t="01:00:00",
                t_nullable=None,
                t64_us="02:00:00.123000",
                t64_us_nullable=None,
                t64_ns="03:00:00.456000000",
                t64_ns_nullable=None,
            ),
            create_nullable_test_row(
                1,
                t="04:00:00",
                t_nullable="05:00:00",
                t64_us="06:00:00.789000",
                t64_us_nullable="06:00:00.789000",
                t64_ns="07:00:00.123000000",
                t64_ns_nullable="07:00:00.123000000",
            ),
        ]
        call(param_client.insert, TABLE_NAME, rows)

        result = call(param_client.query, f"SELECT t_nullable FROM {TABLE_NAME} ORDER BY id")
        result_values = [row[0] for row in result.result_rows]
        expected = [None, timedelta(hours=5)]
        assert result_values == expected

        result = call(param_client.query, f"SELECT t64_us_nullable FROM {TABLE_NAME} ORDER BY id")
        result_values = [row[0] for row in result.result_rows]
        expected = [None, timedelta(hours=6, microseconds=789000)]
        assert result_values == expected

        result = call(param_client.query, f"SELECT t64_ns_nullable FROM {TABLE_NAME} ORDER BY id")
        result_values = [row[0] for row in result.result_rows]
        expected = [None, timedelta(hours=7, microseconds=123000)]
        assert result_values == expected


@pytest.mark.parametrize(
    "invalid_value",
    [
        "1000:00:00",  # Out of range string
        3600000,  # Out of range int
    ],
)
def test_time_out_of_range_values(param_client, call, table_context, invalid_value: Any):
    """Test that out-of-range Time values raise ValueError."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        with pytest.raises(ValueError, match="out of range"):
            rows = [create_test_row(0, invalid_value)]
            call(param_client.insert, TABLE_NAME, rows)


@pytest.mark.parametrize(
    "time_val,time64_val",
    [
        ("1:2:3:4", "1:2:3:4"),  # Too many colons
        ("10:70:00", "10:70:00"),  # Invalid minutes
        ("10:00:00.123.456", "10:00:00.123.456"),  # Invalid fractional format
    ],
)
def test_invalid_time_formats(param_client, call, table_context, time_val: str, time64_val: str):
    """Test that invalid time formats raise ValueError."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        with pytest.raises(ValueError):
            rows = [create_nullable_test_row(0, t=time_val, t64_us=time64_val, t64_ns=time64_val)]
            call(param_client.insert, TABLE_NAME, rows)


def test_timedelta_input_conversion(param_client, call, table_context):
    """Test conversion of timedelta inputs to internal representation."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        test_deltas = TimeTestData.TIME_DELTAS[:3]
        rows = [create_test_row(i, td) for i, td in enumerate(test_deltas)]
        call(param_client.insert, TABLE_NAME, rows)

        result = call(param_client.query, f"SELECT t FROM {TABLE_NAME} ORDER BY id")
        result_values = [row[0] for row in result.result_rows]
        assert result_values == test_deltas
        assert all(isinstance(td, timedelta) for td in result_values)


def test_integer_input_conversion(param_client, call, table_context):
    """Test conversion of integer inputs to internal representation."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        test_ints = TimeTestData.TIME_INTS
        rows = [create_test_row(i, val) for i, val in enumerate(test_ints)]
        call(param_client.insert, TABLE_NAME, rows)

        result = call(param_client.query, f"SELECT t FROM {TABLE_NAME} ORDER BY id")
        result_values = [row[0] for row in result.result_rows]
        expected = TimeTestData.TIME_DELTAS
        assert result_values == expected


def test_time_roundtrip_time_format(param_client, call, table_context):
    """Ensure Time columns accept & return datetime.time when format='time'."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        rows = [create_test_row(i, t) for i, t in enumerate(ClockTimeData.TIME_OBJS)]
        call(param_client.insert, TABLE_NAME, rows)

        result = call(param_client.query, f"SELECT t FROM {TABLE_NAME} ORDER BY id", query_formats={"Time": "time"})
        result_values = [row[0] for row in result.result_rows]
        assert result_values == ClockTimeData.TIME_OBJS


def test_time_default_read_from_time_objects(param_client, call, table_context):
    """Writing time objects + default read still yields timedelta."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        rows = [create_test_row(i, t) for i, t in enumerate(ClockTimeData.TIME_OBJS)]
        call(param_client.insert, TABLE_NAME, rows)

        result = call(param_client.query, f"SELECT t FROM {TABLE_NAME} ORDER BY id")
        result_values = [row[0] for row in result.result_rows]
        assert result_values == ClockTimeData.TIME_DELTAS


@pytest.mark.parametrize(
    "column,objects",
    [
        ("t64_us", ClockTimeData.TIME64_US_OBJS),
        ("t64_ns", ClockTimeData.TIME64_NS_OBJS),
    ],
)
def test_time64_time_format(param_client, call, table_context, column: str, objects: List[time]):
    """Validate Time64(6/9) ⇄ datetime.time conversions."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        rows = [create_test_row(i, t) for i, t in enumerate(objects)]
        call(param_client.insert, TABLE_NAME, rows)

        result = call(param_client.query, f"SELECT {column} FROM {TABLE_NAME} ORDER BY id", query_formats={"Time64": "time"})
        result_values = [row[0] for row in result.result_rows]
        assert result_values == objects


def test_negative_value_cannot_be_coerced_to_time(param_client, call, table_context):
    """Database contains -2 s; asking for format='time' should fail."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        rows = [create_test_row(0, timedelta(seconds=-2))]
        call(param_client.insert, TABLE_NAME, rows)

        with pytest.raises(ValueError, match="outside valid range"):
            call(param_client.query, f"SELECT t FROM {TABLE_NAME}", query_formats={"Time": "time"})


def test_over_24h_value_cannot_be_coerced_to_time(param_client, call, table_context):
    """30 h is legal for ClickHouse but illegal for datetime.time."""
    with table_context(TABLE_NAME, STANDARD_TIME_TABLE_SCHEMA, settings={"enable_time_time64_type": 1}):
        rows = [create_test_row(0, "030:00:00")]
        call(param_client.insert, TABLE_NAME, rows)

        with pytest.raises(ValueError, match="outside valid range"):
            call(param_client.query, f"SELECT t FROM {TABLE_NAME}", query_formats={"Time": "time"})
