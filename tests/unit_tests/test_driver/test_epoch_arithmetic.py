"""Tests for epoch seconds arithmetic helper functions."""

import unittest
from datetime import datetime, date

# Try to import from Cython extension, fall back to pure Python if unavailable
try:
    from clickhouse_connect.driverc.dataconv import epoch_seconds_to_components
except ImportError:
    # Fallback: will implement pure Python version in tzutil module
    epoch_seconds_to_components = None


class TestEpochSecondsToComponents(unittest.TestCase):
    """Tests for epoch_seconds_to_components helper function."""

    def setUp(self):
        if epoch_seconds_to_components is None:
            self.skipTest("Cython extension not available")

    def test_epoch_zero(self):
        """Epoch 0 should be 1970-01-01 00:00:00."""
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(0)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (1970, 1, 1, 0, 0, 0, 0))

    def test_epoch_one_second(self):
        """Epoch 1 should be 1970-01-01 00:00:01."""
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(1)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (1970, 1, 1, 0, 0, 1, 0))

    def test_epoch_one_minute(self):
        """60 seconds should be 1970-01-01 00:01:00."""
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(60)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (1970, 1, 1, 0, 1, 0, 0))

    def test_epoch_one_hour(self):
        """3600 seconds should be 1970-01-01 01:00:00."""
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(3600)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (1970, 1, 1, 1, 0, 0, 0))

    def test_epoch_one_day(self):
        """86400 seconds should be 1970-01-02 00:00:00."""
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(86400)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (1970, 1, 2, 0, 0, 0, 0))

    def test_typical_datetime(self):
        """Test a known datetime: 2020-01-01 12:34:56."""
        # 2020-01-01 12:34:56 UTC = 1577882096 unix timestamp
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(1577882096)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (2020, 1, 1, 12, 34, 56, 0))

    def test_leap_year_feb_29(self):
        """Test Feb 29 in a leap year (2020-02-29 00:00:00)."""
        # 2020-02-29 00:00:00 UTC = 1582934400
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(1582934400)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (2020, 2, 29, 0, 0, 0, 0))

    def test_before_leap_day(self):
        """Test Feb 28 before a leap day."""
        # 2020-02-28 00:00:00 UTC = 1582848000
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(1582848000)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (2020, 2, 28, 0, 0, 0, 0))

    def test_after_leap_day(self):
        """Test Mar 1 after a leap day."""
        # 2020-03-01 00:00:00 UTC = 1583020800
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(1583020800)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (2020, 3, 1, 0, 0, 0, 0))

    def test_non_leap_year_feb(self):
        """Test Feb in a non-leap year."""
        # 2019-02-28 00:00:00 UTC = 1551312000
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(1551312000)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (2019, 2, 28, 0, 0, 0, 0))

    def test_year_boundary(self):
        """Test transition from 2019 to 2020."""
        # 2019-12-31 23:59:59 UTC = 1577836799
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(1577836799)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (2019, 12, 31, 23, 59, 59, 0))

        # 2020-01-01 00:00:00 UTC = 1577836800
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(1577836800)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (2020, 1, 1, 0, 0, 0, 0))

    def test_century_boundary(self):
        """Test transition from 1999 to 2000."""
        # 1999-12-31 23:59:59 UTC = 946684799
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(946684799)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (1999, 12, 31, 23, 59, 59, 0))

        # 2000-01-01 00:00:00 UTC = 946684800
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(946684800)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (2000, 1, 1, 0, 0, 0, 0))

    def test_negative_epoch_one_second_before_epoch(self):
        """Test -1 seconds (1969-12-31 23:59:59)."""
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(-1)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (1969, 12, 31, 23, 59, 59, 0))

    def test_negative_epoch_one_day_before_epoch(self):
        """Test -86400 seconds (1969-12-31 00:00:00)."""
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(-86400)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (1969, 12, 31, 0, 0, 0, 0))

    def test_negative_epoch_arbitrary_date(self):
        """Test a negative epoch value from well before 1970."""
        # 1960-01-01 00:00:00 UTC = -315619200
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(-315619200)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (1960, 1, 1, 0, 0, 0, 0))

    def test_far_future_datetime(self):
        """Test a date far in the future (2100-01-01 00:00:00)."""
        # 2100-01-01 00:00:00 UTC = 4102444800
        year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(4102444800)
        self.assertEqual((year, month, day, hour, minute, second, microsecond), (2100, 1, 1, 0, 0, 0, 0))

    def test_roundtrip_via_datetime(self):
        """Verify results match datetime.utcfromtimestamp for various values."""
        test_timestamps = [
            0,
            1,
            86400,
            1577882096,  # 2020-01-01 12:34:56
            1582934400,  # 2020-02-29 00:00:00
            -1,
            -86400,
            -315619200,  # 1960-01-01 00:00:00
        ]

        for ts in test_timestamps:
            with self.subTest(timestamp=ts):
                # Get components
                year, month, day, hour, minute, second, microsecond = epoch_seconds_to_components(ts)

                # Build datetime from components
                dt_from_components = datetime(year, month, day, hour, minute, second, microsecond)

                # Compare to datetime.utcfromtimestamp (deprecated but still works for testing)
                dt_from_timestamp = datetime.utcfromtimestamp(ts)

                self.assertEqual(dt_from_components, dt_from_timestamp)


if __name__ == "__main__":
    unittest.main()
