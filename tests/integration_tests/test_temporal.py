from datetime import timedelta
from typing import List, Any
import pytest

from clickhouse_connect.driver import Client
from tests.integration_tests.conftest import TestConfig

# pylint: disable=no-self-use


@pytest.fixture(autouse=True, scope="module")
def module_setup_and_checks(test_client, test_config: TestConfig):
    """
    Performs all module-level setup:
    - Skips if in a cloud environment where settings are locked.
    - Skips if the server version is too old for Time/Time64 types.
    """
    # First, check the cloud environment
    if test_config.cloud:
        pytest.skip(
            "Time/Time64 types require settings change, but settings are locked in cloud, skipping tests.",
            allow_module_level=True,
        )

    # Next, check the server version
    version_str = test_client.query("SELECT version()").result_rows[0][0]
    major, minor, *_ = map(int, version_str.split("."))
    if (major, minor) < (25, 6):
        pytest.skip(
            "Time and Time64 types require ClickHouse 25.6+", allow_module_level=True
        )


# Test Data Constants
class TimeTestData:
    """Centralized test data for time-related tests."""

    # Time constants
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

    # Time64(6) constants
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

    # Time64(9) constants
    TIME64_NS_STRINGS = [
        "001:02:03.123456789",
        "-000:00:05.500000000",
        "000:00:00.000000001",
    ]

    TIME64_NS_DELTAS = [
        timedelta(hours=1, minutes=2, seconds=3, microseconds=123456),
        timedelta(seconds=-5, microseconds=-500000),
        timedelta(0),  # nanoseconds truncated
    ]

    TIME64_NS_TICKS = [3723123456789, -5500000000, 1]


# Test Configuration
TABLE_NAME = "temp_time_test"
COLUMN_COUNT = 7


# Test Helpers
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


# Fixtures
@pytest.fixture(autouse=True)
def setup_time_table(test_client: Client):
    """Setup and teardown test table with Time and Time64 columns."""
    client = test_client

    # Enable native Time & Time64 support
    client.command("SET enable_time_time64_type = 1")

    # Create table
    client.command(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    client.command(
        f"""
        CREATE TABLE {TABLE_NAME} (
            id UInt32,
            t Time,
            t_nullable Nullable(Time),
            t64_us Time64(6),
            t64_us_nullable Nullable(Time64(6)),
            t64_ns Time64(9),
            t64_ns_nullable Nullable(Time64(9))
        ) ENGINE = MergeTree ORDER BY id
    """
    )

    yield client

    # Cleanup
    client.command(f"DROP TABLE IF EXISTS {TABLE_NAME}")


def insert_test_data(client: Client, rows: List[List[Any]]) -> None:
    """Insert test data into the test table."""
    client.insert(TABLE_NAME, rows)


def query_column(client: Client, column: str, **query_formats) -> List[Any]:
    """Query a single column ordered by ID with optional format specifications."""
    query_result = client.query(
        f"SELECT {column} FROM {TABLE_NAME} ORDER BY id", query_formats=query_formats
    )
    return [row[0] for row in query_result.result_rows]


# Test Classes
class TestTimeRoundtrip:
    """Test round-trip conversion for Time type."""

    def test_time_native_format(self, test_client: Client):
        """Test Time round-trip with native timedelta format."""
        rows = [create_test_row(i, td) for i, td in enumerate(TimeTestData.TIME_DELTAS)]
        insert_test_data(test_client, rows)

        result = query_column(test_client, "t")
        assert result == TimeTestData.TIME_DELTAS

    def test_time_string_format(self, test_client: Client):
        """Test Time round-trip with string format."""
        rows = [create_test_row(i, s) for i, s in enumerate(TimeTestData.TIME_STRINGS)]
        insert_test_data(test_client, rows)

        result = query_column(test_client, "t", Time="string")
        assert result == TimeTestData.TIME_STRINGS

    def test_time_int_format(self, test_client: Client):
        """Test Time round-trip with integer format."""
        rows = [create_test_row(i, val) for i, val in enumerate(TimeTestData.TIME_INTS)]
        insert_test_data(test_client, rows)

        result = query_column(test_client, "t", Time="int")
        assert result == TimeTestData.TIME_INTS


class TestTime64Roundtrip:
    """Test round-trip conversion for Time64 types."""

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
        self,
        test_client: Client,
        column: str,
        strings: List[str],
        deltas: List[timedelta],
        ticks: List[int],
        type_name: str,
    ):
        """Test Time64 round-trip with all supported formats."""
        # Create rows with test data only for the target column
        rows = []
        for i, string_val in enumerate(strings):
            row = [i, timedelta(0), None, timedelta(0), None, timedelta(0), None]
            # Set the target column based on its position
            if column == "t64_us":
                row[3] = string_val  # t64_us position
            elif column == "t64_ns":
                row[5] = string_val  # t64_ns position
            rows.append(row)

        insert_test_data(test_client, rows)

        # Test native format
        result = query_column(test_client, column)
        assert result == deltas

        # Test string format
        result = query_column(test_client, column, **{type_name: "string"})
        assert result == strings

        # Test int format
        result = query_column(test_client, column, **{type_name: "int"})
        assert result == ticks


class TestNullableColumns:
    """Test nullable Time and Time64 columns."""

    def test_nullable_time_columns(self, test_client: Client):
        """Test that nullable columns handle None values correctly."""
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
        insert_test_data(test_client, rows)

        # Test nullable Time column
        result = query_column(test_client, "t_nullable")
        expected = [None, timedelta(hours=5)]
        assert result == expected

        # Test nullable Time64(6) column
        result = query_column(test_client, "t64_us_nullable")
        expected = [None, timedelta(hours=6, microseconds=789000)]
        assert result == expected

        # Test nullable Time64(9) column
        result = query_column(test_client, "t64_ns_nullable")
        expected = [None, timedelta(hours=7, microseconds=123000)]
        assert result == expected


class TestErrorHandling:
    """Test error handling for invalid inputs."""

    @pytest.mark.parametrize(
        "invalid_value",
        [
            "1000:00:00",  # Out of range string
            3600000,  # Out of range int
        ],
    )
    def test_time_out_of_range_values(self, test_client: Client, invalid_value: Any):
        """Test that out-of-range Time values raise ValueError."""
        with pytest.raises(ValueError, match="out of range"):
            rows = [create_test_row(0, invalid_value)]
            insert_test_data(test_client, rows)

    @pytest.mark.parametrize(
        "time_val,time64_val",
        [
            ("1:2:3:4", "1:2:3:4"),  # Too many colons
            ("10:70:00", "10:70:00"),  # Invalid minutes
            ("10:00:00.123.456", "10:00:00.123.456"),  # Invalid fractional format
        ],
    )
    def test_invalid_time_formats(
        self, test_client: Client, time_val: str, time64_val: str
    ):
        """Test that invalid time formats raise ValueError."""
        with pytest.raises(ValueError):
            rows = [
                create_nullable_test_row(
                    0, t=time_val, t64_us=time64_val, t64_ns=time64_val
                )
            ]
            insert_test_data(test_client, rows)


class TestMixedInputTypes:
    """Test handling of mixed input types."""

    def test_timedelta_input_conversion(self, test_client: Client):
        """Test conversion of timedelta inputs to internal representation."""
        test_deltas = TimeTestData.TIME_DELTAS[:3]  # Use first 3 for testing
        rows = [create_test_row(i, td) for i, td in enumerate(test_deltas)]
        insert_test_data(test_client, rows)

        result = query_column(test_client, "t")
        assert result == test_deltas
        assert all(isinstance(td, timedelta) for td in result)

    def test_integer_input_conversion(self, test_client: Client):
        """Test conversion of integer inputs to internal representation."""
        test_ints = TimeTestData.TIME_INTS
        rows = [create_test_row(i, val) for i, val in enumerate(test_ints)]
        insert_test_data(test_client, rows)

        result = query_column(test_client, "t")
        expected = TimeTestData.TIME_DELTAS
        assert result == expected
