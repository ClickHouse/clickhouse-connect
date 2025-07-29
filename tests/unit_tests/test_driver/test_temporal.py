import array
import unittest
from datetime import timedelta, time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import numpy as np
import pandas as pd

from clickhouse_connect.datatypes.base import TypeDef
from clickhouse_connect.datatypes.temporal import Time, Time64
from clickhouse_connect.driver.exceptions import ProgrammingError

# pylint: disable=too-many-public-methods


# Helper function used only in these tests
def _dummy_write_array(array_type, ticks, dest, _col_name):
    """Mimics clickhouse_connect.datatypes.common.write_array by packing the
    list of ints into binary and appending to dest.
    """
    dest.extend(array.array(array_type, ticks).tobytes())


# pylint: disable=protected-access
class TestTimeDataType(unittest.TestCase):
    """Tests for the second-precision Time ClickHouse type, including edge cases."""

    def setUp(self):
        self.time_type = Time(TypeDef())
        self.insert_ctx = SimpleNamespace(column_name="test_time_col")
        self.base_read_ctx = SimpleNamespace(
            use_numpy=False,
            use_extended_dtypes=False,
            use_none=False,
        )

    # ------------------------------------------------------------------
    # Parsing and formatting
    # ------------------------------------------------------------------
    def test_parse_valid_string(self):
        self.assertEqual(self.time_type._string_to_ticks("012:34:56"), 45296)

    def test_parse_negative_string(self):
        self.assertEqual(self.time_type._string_to_ticks("-000:00:01"), -1)

    def test_parse_invalid_format_raises(self):
        with self.assertRaises(ValueError) as cm:
            self.time_type._string_to_ticks("12:34")
        self.assertIn("Invalid time literal", str(cm.exception))

    def test_parse_invalid_range_raises(self):
        with self.assertRaises(ValueError) as cm:
            self.time_type._string_to_ticks("000:60:00")
        self.assertIn("out of range", str(cm.exception))

    def test_fraction_truncation(self):
        cases = [("000:00:00.4", 0), ("000:00:59.6", 59)]
        for literal, expected in cases:
            with self.subTest(literal=literal):
                self.assertEqual(self.time_type._string_to_ticks(literal), expected)

    def test_bounds_max_min(self):
        self.assertEqual(
            self.time_type._string_to_ticks("999:59:59"), self.time_type.max_ticks
        )
        self.assertEqual(
            self.time_type._string_to_ticks("-999:59:59"), self.time_type.min_ticks
        )

    # ------------------------------------------------------------------
    # Tick<->timedelta round-trips
    # ------------------------------------------------------------------
    def test_timedelta_round_trip(self):
        td = timedelta(hours=1, minutes=2, seconds=3)
        ticks = self.time_type._timedelta_to_ticks(td)
        self.assertEqual(self.time_type._ticks_to_timedelta(ticks), td)

    def test_ticks_to_string(self):
        self.assertEqual(self.time_type._ticks_to_string(3723), "001:02:03")
        self.assertEqual(self.time_type._ticks_to_string(-3723), "-001:02:03")

    # ------------------------------------------------------------------
    # Binary write/read
    # ------------------------------------------------------------------
    def test_round_trip_write_read_native(self):
        column = [timedelta(hours=1, minutes=2, seconds=3)]
        expected_ticks = [3723]
        dest = bytearray()
        with patch(f"{Time.__module__}.write_array", _dummy_write_array), patch.object(
            self.time_type, "write_format", return_value="native"
        ):
            self.time_type._write_column_binary(column, dest, self.insert_ctx)
        self.assertEqual(dest, array.array("i", expected_ticks).tobytes())

        mock_source = MagicMock()
        mock_source.read_array.return_value = expected_ticks
        with patch.object(self.time_type, "read_format", return_value="native"):
            result = self.time_type._read_column_binary(
                mock_source, 1, self.base_read_ctx, None
            )
        self.assertEqual(result, column)

    def test_write_int_format(self):
        column = [5]
        expected = [5]
        dest = bytearray()
        with patch(f"{Time.__module__}.write_array", _dummy_write_array), patch.object(
            self.time_type, "write_format", return_value="int"
        ):
            self.time_type._write_column_binary(column, dest, self.insert_ctx)
        self.assertEqual(dest, array.array("i", expected).tobytes())

    def test_read_string_and_int_formats(self):
        ticks = [59]
        mock_source = MagicMock()
        mock_source.read_array.return_value = ticks
        with patch.object(self.time_type, "read_format", return_value="string"):
            res = self.time_type._read_column_binary(
                mock_source, 1, self.base_read_ctx, None
            )
            self.assertEqual(res, ["000:00:59"])
        with patch.object(self.time_type, "read_format", return_value="int"):
            res = self.time_type._read_column_binary(
                mock_source, 1, self.base_read_ctx, None
            )
            self.assertEqual(res, ticks)

    def test_read_numpy_format(self):
        ticks = [1, 2, 3]
        mock_source = MagicMock()
        mock_source.read_array.return_value = ticks
        np_ctx = SimpleNamespace(
            use_numpy=True, use_extended_dtypes=False, use_none=False
        )
        with patch.object(self.time_type, "read_format", return_value="native"):
            res = self.time_type._read_column_binary(mock_source, 3, np_ctx, None)
        self.assertIsInstance(res, np.ndarray)
        self.assertEqual(res.dtype, np.dtype("timedelta64[s]"))
        np.testing.assert_array_equal(res, np.array(ticks, dtype="timedelta64[s]"))

    # ------------------------------------------------------------------
    # Nullable and null-default behaviors
    # ------------------------------------------------------------------
    def test_to_ticks_array_all_none(self):
        self.time_type.nullable = True
        col = [None, None]
        with patch.object(self.time_type, "write_format", return_value="native"):
            ticks = self.time_type._to_ticks_array(col)
        self.assertEqual(ticks, [0, 0])

    def test_to_ticks_array_mixed_types_error(self):
        with patch.object(self.time_type, "write_format", return_value="native"):
            with self.assertRaises(ValueError):
                self.time_type._to_ticks_array([1, "000:00:01"])

    # pylint: disable=c-extension-no-member
    def test_active_null_with_extended_dtypes(self):
        self.time_type.nullable = True
        ctx = SimpleNamespace(use_extended_dtypes=True, use_none=False)
        with patch.object(self.time_type, "read_format", return_value="native"):
            null = self.time_type._active_null(ctx)
        self.assertTrue(isinstance(null, pd._libs.tslibs.nattype.NaTType))

    def test_active_null_with_use_none(self):
        self.time_type.nullable = True
        ctx = SimpleNamespace(use_extended_dtypes=False, use_none=True)
        with patch.object(self.time_type, "read_format", return_value="native"):
            self.assertIsNone(self.time_type._active_null(ctx))

    def test_build_lc_column_numpy(self):
        index = [timedelta(seconds=i) for i in range(5)]
        keys = array.array("I", [0, 2, 4])
        ctx = SimpleNamespace(use_numpy=True)
        arr = self.time_type._build_lc_column(index, keys, ctx)
        self.assertIsInstance(arr, np.ndarray)
        self.assertEqual(arr.dtype, np.dtype("timedelta64[s]"))
        np.testing.assert_array_equal(
            arr, np.array([index[i] for i in keys], dtype="timedelta64[s]")
        )

    # ------------------------------------------------------------------
    # datetime.time conversions
    # ------------------------------------------------------------------
    def test_time_to_ticks(self):
        """Test conversion of datetime.time object to ticks."""
        self.assertEqual(self.time_type._time_to_ticks(time(10, 20, 30)), 37230)
        self.assertEqual(self.time_type._time_to_ticks(time(0, 0, 0)), 0)
        self.assertEqual(self.time_type._time_to_ticks(time(23, 59, 59)), 86399)

    def test_ticks_to_time_round_trip(self):
        """Test round-trip conversion from ticks to datetime.time."""
        ticks = 37230
        t = self.time_type._ticks_to_time(ticks)
        self.assertEqual(t, time(10, 20, 30))
        self.assertEqual(self.time_type._time_to_ticks(t), ticks)

    def test_ticks_to_time_out_of_range_raises(self):
        """Test that converting out-of-range ticks to datetime.time raises ValueError."""
        with self.assertRaises(ValueError):
            self.time_type._ticks_to_time(-1)
        with self.assertRaises(ValueError):
            self.time_type._ticks_to_time(86400)

    def test_read_time_format(self):
        """Test reading data in the 'time' format."""
        ticks = [37230, 0, 86399]
        expected = [time(10, 20, 30), time(0, 0, 0), time(23, 59, 59)]
        mock_source = MagicMock()
        mock_source.read_array.return_value = ticks

        with patch.object(self.time_type, "read_format", return_value="time"):
            result = self.time_type._read_column_binary(
                mock_source, len(ticks), self.base_read_ctx, None
            )
        self.assertEqual(result, expected)

    def test_write_time_format(self):
        """Test writing datetime.time objects to binary."""
        column = [time(10, 20, 30)]
        expected = [37230]
        dest = bytearray()

        with patch(f"{Time.__module__}.write_array", _dummy_write_array):
            self.time_type._write_column_binary(column, dest, self.insert_ctx)
        self.assertEqual(dest, array.array("i", expected).tobytes())


class TestTime64DataType(unittest.TestCase):
    """Tests for the Time64 type."""

    def setUp(self):
        self.base_read_ctx = SimpleNamespace(
            use_numpy=False, use_extended_dtypes=False, use_none=False
        )

    @staticmethod
    def make(scale, nullable=False):
        td = TypeDef(values=(scale,))
        inst = Time64(td)
        inst.nullable = nullable
        return inst

    # ------------------------------------------------------------------
    # Scale=3,6,9 parsing/formatting
    # ------------------------------------------------------------------
    def test_valid_scale3_string_round_trip(self):
        t3 = self.make(3)
        s = "000:00:01.123"
        ticks = t3._string_to_ticks(s)
        self.assertEqual(ticks, 1_123)
        self.assertEqual(t3._ticks_to_string(ticks), s)

    def test_parse_micro_string(self):
        t6 = self.make(6)
        expect = 1_000_000 + 123
        self.assertEqual(t6._string_to_ticks("000:00:01.000123"), expect)

    def test_parse_negative_nanoseconds_string(self):
        t9 = self.make(9)
        expect = -(2 * 10**9 + 5)
        self.assertEqual(t9._string_to_ticks("-000:00:02.000000005"), expect)

    def test_string_fraction_padding_and_truncation(self):
        t6 = self.make(6)
        self.assertEqual(t6._string_to_ticks("000:00:01.1"), 1_100_000)
        self.assertEqual(t6._string_to_ticks("000:00:01.1234567"), 1_123_456)

    def test_ticks_to_string_negative(self):
        t6 = self.make(6)
        self.assertEqual(t6._ticks_to_string(-1_234_000), "-000:00:01.234000")

    def test_bounds_max_min(self):
        for scale in (3, 6, 9):
            t = self.make(scale)
            max_str = t._ticks_to_string(t.max_ticks)
            self.assertTrue(max_str.startswith("999:59:59"))
            min_str = t._ticks_to_string(t.min_ticks)
            self.assertTrue(min_str.startswith("-999:59:59"))

    # ------------------------------------------------------------------
    # Timedelta conversion
    # ------------------------------------------------------------------
    def test_timedelta_round_trip(self):
        for scale in (6, 9):
            t = self.make(scale)
            td = timedelta(seconds=3, microseconds=250_500)
            ticks = t._timedelta_to_ticks(td)
            back = t._ticks_to_timedelta(ticks)
            self.assertEqual(back, td)

    def test_timedelta_negative_and_out_of_range(self):
        t6 = self.make(6)
        td_neg = -timedelta(seconds=2)
        self.assertEqual(t6._timedelta_to_ticks(td_neg), -2_000_000)
        td_big = timedelta(hours=1000)
        with self.assertRaises(ValueError):
            t6._timedelta_to_ticks(td_big)

    # ------------------------------------------------------------------
    # Binary write/read and numpy
    # ------------------------------------------------------------------
    def test_round_trip_write_read_nullable_native(self):
        t6 = self.make(6, nullable=True)
        column = [timedelta(seconds=1, microseconds=1), None]
        expected_ticks = [1_000_001, 0]
        dest = bytearray()
        with patch(
            f"{Time64.__module__}.write_array", _dummy_write_array
        ), patch.object(t6, "write_format", return_value="native"):
            t6._write_column_binary(column, dest, SimpleNamespace(column_name="c"))
        self.assertEqual(dest, array.array("q", expected_ticks).tobytes())

        mock_source = MagicMock()
        mock_source.read_array.return_value = expected_ticks
        with patch.object(t6, "read_format", return_value="string"):
            res = t6._read_column_binary(mock_source, 2, self.base_read_ctx, None)
        self.assertEqual(res, ["000:00:01.000001", "000:00:00.000000"])

    def test_read_numpy_format(self):
        t6 = self.make(6)
        ticks = [0, 10**6]
        mock_source = MagicMock()
        mock_source.read_array.return_value = ticks
        np_ctx = SimpleNamespace(
            use_numpy=True, use_extended_dtypes=False, use_none=False
        )
        with patch.object(t6, "read_format", return_value="native"):
            res = t6._read_column_binary(mock_source, 2, np_ctx, None)
        self.assertIsInstance(res, np.ndarray)
        self.assertEqual(res.dtype, np.dtype("timedelta64[us]"))
        np.testing.assert_array_equal(res, np.array(ticks, dtype="timedelta64[us]"))

    def test_write_int_format(self):
        t6 = self.make(6)
        column = [2_000_000]
        expected = [2_000_000]
        dest = bytearray()
        with patch(
            f"{Time64.__module__}.write_array", _dummy_write_array
        ), patch.object(t6, "write_format", return_value="int"):
            t6._write_column_binary(column, dest, SimpleNamespace(column_name="c"))
        self.assertEqual(dest, array.array("q", expected).tobytes())

    # ------------------------------------------------------------------
    # Nullable behavior
    # ------------------------------------------------------------------
    # pylint: disable=c-extension-no-member
    def test_active_null_with_extended_dtypes(self):
        t6 = self.make(6, nullable=True)
        ctx = SimpleNamespace(use_extended_dtypes=True, use_none=False)
        with patch.object(t6, "read_format", return_value="native"):
            null = t6._active_null(ctx)
        self.assertTrue(isinstance(null, pd._libs.tslibs.nattype.NaTType))

    def test_active_null_with_use_none(self):
        t6 = self.make(6, nullable=True)
        ctx = SimpleNamespace(use_extended_dtypes=False, use_none=True)
        with patch.object(t6, "read_format", return_value="native"):
            self.assertIsNone(t6._active_null(ctx))

    # ------------------------------------------------------------------
    # Invalid constructor scale
    # ------------------------------------------------------------------
    def test_invalid_scale_raises(self):
        with self.assertRaises(ProgrammingError):
            Time64(TypeDef(values=(2,)))

    # ------------------------------------------------------------------
    # datetime.time conversions
    # ------------------------------------------------------------------
    def test_time_to_ticks_scale6(self):
        """Test conversion of datetime.time to ticks for Time64(6)."""
        t6 = self.make(6)
        t = time(10, 20, 30, 123456)
        self.assertEqual(t6._time_to_ticks(t), 37230123456)

    def test_ticks_to_time_round_trip_scale6(self):
        """Test round-trip conversion from ticks to datetime.time for Time64(6)."""
        t6 = self.make(6)
        ticks = 37230123456
        t = t6._ticks_to_time(ticks)
        self.assertEqual(t, time(10, 20, 30, 123456))
        self.assertEqual(t6._time_to_ticks(t), ticks)

    def test_ticks_to_time_out_of_range_raises_scale6(self):
        """Test that out-of-range ticks raise ValueError for Time64(6)."""
        t6 = self.make(6)
        with self.assertRaises(ValueError):
            t6._ticks_to_time(-1)
        with self.assertRaises(ValueError):
            t6._ticks_to_time(86400 * 10**6)

    def test_read_time_format_scale6(self):
        """Test reading Time64(6) data in the 'time' format."""
        t6 = self.make(6)
        ticks = [37230123456]
        expected = [time(10, 20, 30, 123456)]
        mock_source = MagicMock()
        mock_source.read_array.return_value = ticks

        with patch.object(t6, "read_format", return_value="time"):
            result = t6._read_column_binary(mock_source, 1, self.base_read_ctx, None)
        self.assertEqual(result, expected)

    def test_write_time_format_scale6(self):
        """Test writing datetime.time objects to Time64(6) binary."""
        t6 = self.make(6)
        column = [time(10, 20, 30, 123456)]
        expected = [37230123456]
        dest = bytearray()

        with patch(f"{Time64.__module__}.write_array", _dummy_write_array):
            t6._write_column_binary(column, dest, SimpleNamespace(column_name="c"))
        self.assertEqual(dest, array.array("q", expected).tobytes())


if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False)
