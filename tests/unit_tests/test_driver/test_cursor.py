from unittest.mock import Mock
import pytest

from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.driver.exceptions import ProgrammingError


# pylint: disable=protected-access
def create_mock_client(result_data):
    """Helper to create a mock client with query result"""
    client = Mock()
    query_result = Mock()
    query_result.result_set = result_data
    query_result.column_names = ["col1", "col2", "col3"]
    query_result.column_types = [Mock(name="String")] * 3
    query_result.summary = {"rows": len(result_data)}
    client.query.return_value = query_result
    return client


def test_fetchall_respects_cursor_position():
    """Test that fetchall() returns only unread rows and respects cursor position"""
    test_data = [
        ("row1_col1", "row1_col2", "row1_col3"),
        ("row2_col1", "row2_col2", "row2_col3"),
        ("row3_col1", "row3_col2", "row3_col3"),
        ("row4_col1", "row4_col2", "row4_col3"),
        ("row5_col1", "row5_col2", "row5_col3"),
    ]

    client = create_mock_client(test_data)
    cursor = Cursor(client)

    # Execute a query to populate cursor data
    cursor.execute("SELECT * FROM test_table")

    # Fetch first two rows
    row1 = cursor.fetchone()
    row2 = cursor.fetchone()

    assert row1 == test_data[0]
    assert row2 == test_data[1]
    assert cursor._ix == 2  # Cursor should be at position 2

    # fetchall() should return remaining rows, not all rows
    remaining_rows = cursor.fetchall()

    # Should only get rows 3, 4, and 5 (indices 2, 3, 4)
    expected_remaining = test_data[2:]
    assert remaining_rows == expected_remaining
    assert len(remaining_rows) == 3

    # Cursor should now be at the end
    assert cursor._ix == cursor._rowcount

    # Another fetchall() should return empty list since all rows consumed
    empty_result = cursor.fetchall()
    assert empty_result == []


def test_fetchmany_respects_size_parameter():
    """Test that fetchmany() correctly handles the size parameter"""
    test_data = [
        ("row1",),
        ("row2",),
        ("row3",),
        ("row4",),
        ("row5",),
        ("row6",),
        ("row7",),
        ("row8",),
        ("row9",),
        ("row10",),
    ]

    client = create_mock_client(test_data)
    cursor = Cursor(client)
    cursor.execute("SELECT * FROM test_table")

    # Test fetchmany with explicit size
    batch1 = cursor.fetchmany(size=3)
    assert len(batch1) == 3
    assert batch1 == test_data[0:3]
    assert cursor._ix == 3

    # Test fetchmany with size larger than remaining rows
    batch2 = cursor.fetchmany(size=10)
    assert len(batch2) == 7  # Only 7 rows remaining
    assert batch2 == test_data[3:10]
    assert cursor._ix == 10

    # Test fetchmany when no rows remain
    batch3 = cursor.fetchmany(size=5)
    assert batch3 == []
    assert cursor._ix == 10


def test_fetchmany_negative_values():
    """Test fetchmany with various negative values"""
    test_data = [("row1",), ("row2",), ("row3",), ("row4",), ("row5",)]

    client = create_mock_client(test_data)
    cursor = Cursor(client)
    cursor.execute("SELECT * FROM test_table")

    # Advance cursor partway
    cursor.fetchone()  # Now at index 1

    # Any negative value should fetch all remaining
    remaining = cursor.fetchmany(-999)
    assert len(remaining) == 4
    assert remaining == test_data[1:]


def test_fetchmany_w_no_size_parameter_fetches_all_remaining():
    """Test default behavior or fetchmany"""
    test_data = [("A", 1), ("B", 2), ("C", 3), ("D", 4), ("E", 5), ("F", 6)]

    client = create_mock_client(test_data)
    cursor = Cursor(client)
    cursor.execute("SELECT * FROM test_table")

    # Fetch many (no size parameter)
    batch = cursor.fetchmany()
    assert batch == test_data

    # Reset cursor
    cursor.execute("SELECT * FROM test_table")

    # Fetch one
    row1 = cursor.fetchone()
    assert row1 == test_data[0]

    # Fetch remaining (fetchmany with no size parameter)
    batch = cursor.fetchmany()
    assert batch == test_data[1:]


def test_mixed_fetch_operations():
    """Test mixing different fetch operations"""
    test_data = [("A", 1), ("B", 2), ("C", 3), ("D", 4), ("E", 5), ("F", 6)]

    client = create_mock_client(test_data)
    cursor = Cursor(client)
    cursor.execute("SELECT * FROM test_table")

    # Fetch one
    row1 = cursor.fetchone()
    assert row1 == test_data[0]

    # Fetch many
    batch = cursor.fetchmany(2)
    assert batch == test_data[1:3]

    # Fetch all remaining
    remaining = cursor.fetchall()
    assert remaining == test_data[3:6]

    # All subsequent fetches should return empty/None
    assert cursor.fetchone() is None
    assert cursor.fetchone() is None  # Should continue returning None
    assert cursor.fetchmany(10) == []
    assert cursor.fetchall() == []


def test_cursor_reset_on_new_execute():
    """Test that cursor position resets on new execute"""
    test_data = [("row1",), ("row2",), ("row3",)]

    client = create_mock_client(test_data)
    cursor = Cursor(client)

    # First query
    cursor.execute("SELECT * FROM test_table")
    cursor.fetchmany(2)
    assert cursor._ix == 2

    # New query should reset cursor
    cursor.execute("SELECT * FROM test_table")
    assert cursor._ix == 0

    # Should be able to fetch all rows again
    all_rows = cursor.fetchall()
    assert len(all_rows) == 3
    assert all_rows == test_data


def test_check_valid():
    """Test that operations fail when cursor is not valid"""
    client = Mock()
    cursor = Cursor(client)

    # Cursor should be invalid before execute
    with pytest.raises(ProgrammingError):
        cursor.fetchone()

    with pytest.raises(ProgrammingError):
        cursor.fetchall()

    with pytest.raises(ProgrammingError):
        cursor.fetchmany()


def test_empty_result_set():
    """Test cursor behavior with empty result set"""
    client = create_mock_client([])
    cursor = Cursor(client)
    cursor.execute("SELECT * FROM empty_table")

    assert cursor.rowcount == 0
    assert cursor.fetchone() is None
    assert cursor.fetchall() == []
    assert cursor.fetchmany(5) == []
