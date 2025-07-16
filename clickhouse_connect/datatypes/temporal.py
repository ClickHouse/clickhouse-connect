import pytz

import array
from datetime import date, datetime, tzinfo, timedelta
from typing import Union, Sequence, MutableSequence, Any, NamedTuple, Optional
from abc import abstractmethod
import re

from clickhouse_connect.datatypes.base import TypeDef, ClickHouseType
from clickhouse_connect.driver.common import (
    write_array,
    np_date_types,
    int_size,
    first_value,
    get_homogeneous_column_type,

)
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.ctypes import data_conv, numpy_conv
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource
from clickhouse_connect.driver.options import np, pd

epoch_start_date = date(1970, 1, 1)
epoch_start_datetime = datetime(1970, 1, 1)


class Date(ClickHouseType):
    _array_type = 'H'
    np_type = 'datetime64[D]'
    nano_divisor = 86400 * 1000000000
    valid_formats = 'native', 'int'
    python_type = date
    byte_size = 2

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state:Any):
        if self.read_format(ctx) == 'int':
            return source.read_array(self._array_type, num_rows)
        if ctx.use_numpy:
            return numpy_conv.read_numpy_array(source, '<u2', num_rows).astype(self.np_type)
        return data_conv.read_date_col(source, num_rows)

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        first = first_value(column, self.nullable)
        if isinstance(first, int) or self.write_format(ctx) == 'int':
            if self.nullable:
                column = [x if x else 0 for x in column]
        else:
            if isinstance(first, datetime):
                esd = epoch_start_datetime
            else:
                esd = epoch_start_date
            if self.nullable:
                column = [0 if x is None else (x - esd).days for x in column]
            else:
                column = [(x - esd).days for x in column]
        write_array(self._array_type, column, dest, ctx.column_name)

    def _active_null(self, ctx: QueryContext):
        fmt = self.read_format(ctx)
        if ctx.use_extended_dtypes:
            return pd.NA if fmt == 'int' else pd.NaT
        if ctx.use_none:
            return None
        if fmt == 'int':
            return 0
        if ctx.use_numpy:
            return np.datetime64(0)
        return epoch_start_date

    def _finalize_column(self, column: Sequence, ctx: QueryContext) -> Sequence:
        if self.read_format(ctx) == 'int':
            return column
        if ctx.use_numpy and self.nullable and not ctx.use_none:
            return np.array(column, dtype=self.np_type)
        return column


class Date32(Date):
    byte_size = 4
    _array_type = 'l' if int_size == 2 else 'i'

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any):
        if ctx.use_numpy:
            return numpy_conv.read_numpy_array(source, '<i4', num_rows).astype(self.np_type)
        if self.read_format(ctx) == 'int':
            return source.read_array(self._array_type, num_rows)
        return data_conv.read_date32_col(source, num_rows)


class DateTimeBase(ClickHouseType, registered=False):
    __slots__ = ('tzinfo',)
    valid_formats = 'native', 'int'
    python_type = datetime

    def _active_null(self, ctx: QueryContext):
        fmt = self.read_format(ctx)
        if ctx.use_extended_dtypes:
            return pd.NA if fmt == 'int' else pd.NaT
        if ctx.use_none:
            return None
        if self.read_format(ctx) == 'int':
            return 0
        if ctx.use_numpy:
            return np.datetime64(0)
        return epoch_start_datetime


class DateTime(DateTimeBase):
    _array_type = 'L' if int_size == 2 else 'I'
    np_type = 'datetime64[s]'
    nano_divisor = 1000000000
    byte_size = 4

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str
        if len(type_def.values) > 0:
            self.tzinfo = pytz.timezone(type_def.values[0][1:-1])
        else:
            self.tzinfo = None

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any) -> Sequence:
        if self.read_format(ctx) == 'int':
            return source.read_array(self._array_type, num_rows)
        active_tz = ctx.active_tz(self.tzinfo)
        if ctx.use_numpy:
            np_array = numpy_conv.read_numpy_array(source, '<u4', num_rows).astype(self.np_type)
            if ctx.as_pandas and active_tz:
                return pd.DatetimeIndex(np_array, tz='UTC').tz_convert(active_tz)
            return np_array
        return data_conv.read_datetime_col(source, num_rows, active_tz)

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        first = first_value(column, self.nullable)
        if isinstance(first, int) or self.write_format(ctx) == 'int':
            if self.nullable:
                column = [x if x else 0 for x in column]
        else:
            if self.nullable:
                column = [int(x.timestamp()) if x else 0 for x in column]
            else:
                column = [int(x.timestamp()) for x in column]
        write_array(self._array_type, column, dest, ctx.column_name)


class DateTime64(DateTimeBase):
    __slots__ = 'scale', 'prec', 'unit'
    byte_size = 8

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str
        self.scale = type_def.values[0]
        self.prec = 10 ** self.scale
        self.unit = np_date_types.get(self.scale)
        if len(type_def.values) > 1:
            self.tzinfo = pytz.timezone(type_def.values[1][1:-1])
        else:
            self.tzinfo = None

    @property
    def np_type(self):
        if self.unit:
            return f'datetime64{self.unit}'
        raise ProgrammingError(f'Cannot use {self.name} as a numpy or Pandas datatype. Only milliseconds(3), ' +
                               'microseconds(6), or nanoseconds(9) are supported for numpy based queries.')

    @property
    def nano_divisor(self):
        return 1000000000 // self.prec

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any) -> Sequence:
        if self.read_format(ctx) == 'int':
            return source.read_array('q', num_rows)
        active_tz = ctx.active_tz(self.tzinfo)
        if ctx.use_numpy:
            np_array = numpy_conv.read_numpy_array(source, self.np_type, num_rows)
            if ctx.as_pandas and active_tz and active_tz != pytz.UTC:
                return pd.DatetimeIndex(np_array, tz='UTC').tz_convert(active_tz)
            return np_array
        column = source.read_array('q', num_rows)
        if active_tz and active_tz != pytz.UTC:
            return self._read_binary_tz(column, active_tz)
        return self._read_binary_naive(column)

    def _read_binary_tz(self, column: Sequence, tz_info: tzinfo):
        new_col = []
        app = new_col.append
        dt_from = datetime.fromtimestamp
        prec = self.prec
        for ticks in column:
            seconds = ticks // prec
            dt_sec = dt_from(seconds, tz_info)
            app(dt_sec.replace(microsecond=((ticks - seconds * prec) * 1000000) // prec))
        return new_col

    def _read_binary_naive(self, column: Sequence):
        new_col = []
        app = new_col.append
        dt_from = datetime.utcfromtimestamp
        prec = self.prec
        for ticks in column:
            seconds = ticks // prec
            dt_sec = dt_from(seconds)
            app(dt_sec.replace(microsecond=((ticks - seconds * prec) * 1000000) // prec))
        return new_col

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        first = first_value(column, self.nullable)
        if isinstance(first, int) or self.write_format(ctx) == 'int':
            if self.nullable:
                column = [x if x else 0 for x in column]
        elif isinstance(first, str):
            original_column = column
            column = []

            for x in original_column:
                if not x and self.nullable:
                    v = 0
                else:
                    dt = datetime.fromisoformat(x)
                    v = ((int(dt.timestamp()) * 1000000 + dt.microsecond) * self.prec) // 1000000

                column.append(v)
        else:
            prec = self.prec
            if self.nullable:
                column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 if x else 0
                          for x in column]
            else:
                column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 for x in column]
        write_array('q', column, dest, ctx.column_name)


class _HMSParts(NamedTuple):
    """Internal structure for parsed HMS time components."""

    hours: int
    minutes: int
    seconds: int
    frac: Optional[str]
    is_negative: bool


class TimeBase(ClickHouseType, registered=False):
    """
    Abstract base for ClickHouse Time and Time64 types.

    Subclasses must define:
      - _array_type: Array type specifier (e.g. 'i' or 'q')
      - byte_size: Size in bytes for binary representation
      - np_type: NumPy array type (e.g. 'timedelta64[s]' or 'timedelta64[ns]')

    And implement these abstract methods:
      - _string_to_ticks(self, str) -> int
      - _timedelta_to_ticks(self, timedelta) -> int
      - _ticks_to_timedelta(self, int) -> timedelta
      - _ticks_to_string(self, int) -> str
      - max_ticks and min_ticks properties
    """

    _HMS_RE = re.compile(
        r"""^\s*
        (?P<sign>-?)
        (?P<hours>\d+):
        (?P<minutes>\d+):
        (?P<seconds>\d+)
        (?:\.(?P<frac>\d+))?
        \s*$""",
        re.VERBOSE,
    )

    MAX_TIME_SECONDS = 999 * 3600 + 59 * 60 + 59  # 999:59:59
    MIN_TIME_SECONDS = -MAX_TIME_SECONDS  # -999:59:59
    _MICROS_PER_SECOND = 1_000_000

    _array_type: str
    byte_size: int
    np_type: str
    valid_formats = ("native", "string", "int")
    python_type = timedelta

    def _read_column_binary(
        self,
        source: ByteSource,
        num_rows: int,
        ctx: QueryContext,
        _read_state: Any,
    ) -> Sequence:
        """Read binary column data and convert to requested format."""
        # Pull raw ticks
        ticks = source.read_array(self._array_type, num_rows)
        fmt = self.read_format(ctx)

        if fmt == "int":
            return ticks

        if fmt == "string":
            return [self._ticks_to_string(t) for t in ticks]

        if ctx.use_numpy:
            return np.array(ticks, dtype=self.np_type)

        # Default to native Python type of timedelta
        return [self._ticks_to_timedelta(t) for t in ticks]

    def _write_column_binary(
        self,
        column: Sequence,
        dest: bytearray,
        ctx: InsertContext,
    ):
        """Write column data in binary format."""
        ticks = self._to_ticks_array(column)
        write_array(self._array_type, ticks, dest, ctx.column_name)

    def _parse_core(self, time_str: str) -> _HMSParts:
        """
        Parse an hhh:mm:ss[.fff] time literal.

        Returns an _HMSParts tuple; raises ValueError on ill-formed input.
        """
        match = self._HMS_RE.match(time_str)
        if not match:
            raise ValueError(f"Invalid time literal {time_str}")

        hours = int(match["hours"])
        minutes = int(match["minutes"])
        seconds = int(match["seconds"])

        if hours > 999:
            raise ValueError(
                f"Hours out of range; cannot exceed 999: got {hours} in '{time_str}'"
            )
        if not 0 <= minutes < 60:
            raise ValueError(
                f"Minutes out of range; must be 0-59: got {minutes} in '{time_str}'"
            )
        if not 0 <= seconds < 60:
            raise ValueError(
                f"Seconds out of range; must be 0-59: got {seconds} in '{time_str}'"
            )

        return _HMSParts(
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            frac=match["frac"],
            is_negative=bool(match["sign"]),
        )

    def _to_ticks_array(self, column: Sequence) -> Sequence[int]:
        """Convert column data to internal tick representation."""
        expected_type = get_homogeneous_column_type(column)

        # Handle empty or all-None columns
        if expected_type is None:
            if self.nullable:
                return [0] * len(column)
            return []  # Empty non-nullable column

        # Map detected type to correct converter function
        converter_map = {
            timedelta: self._timedelta_to_ticks,
            int: self._int_to_ticks,
            str: self._string_to_ticks,
        }
        converter = converter_map.get(expected_type, None)

        if converter is None:
            raise TypeError(
                f"Unsupported column type '{expected_type.__name__}' for {self.__class__.__name__}. "
                "Expected 'int', 'str', or 'timedelta'."
            )

        # Apply converter
        if self.nullable:
            return [converter(x) if x is not None else 0 for x in column]

        return [converter(x) for x in column]

    def _validate_range(self, ticks: int, original: Any) -> None:
        """
        Validate that ticks is within valid range.

        Args:
            ticks: The tick value to validate
            original: Original value for error reporting

        Raises:
            ValueError: If ticks is out of range
        """
        if not self.min_ticks <= ticks <= self.max_ticks:
            raise ValueError(f"{original} out of range for {self.__class__.__name__}")

    def _int_to_ticks(self, value: int) -> int:
        """Convert integer value to ticks, with range validation."""
        self._validate_range(value, value)
        return value

    def _active_null(self, ctx: QueryContext):
        """Return appropriate null value based on context."""
        if ctx.use_extended_dtypes:
            return pd.NaT
        if ctx.use_none:
            return None
        if self.read_format(ctx) == "int":
            return 0
        if ctx.use_numpy:
            return np.timedelta64("NaT")

        return timedelta(0)

    def _finalize_column(self, column: Sequence, ctx: QueryContext) -> Sequence:
        """Finalize column data based on context requirements."""
        if self.read_format(ctx) == "int":
            return column
        if ctx.use_extended_dtypes and self.nullable:
            return pd.array(
                [pd.Timedelta(seconds=s) if s is not None else pd.NaT for s in column],
                dtype=pd.TimedeltaIndex,
            )
        if ctx.use_numpy and self.nullable and not ctx.use_none:
            return np.array(column, dtype=self.np_type)

        return column

    def _build_lc_column(self, index: Sequence, keys: array.array, ctx: QueryContext):
        """Build low-cardinality column from index and keys."""
        if ctx.use_numpy:
            return np.array([index[k] for k in keys], dtype=self.np_type)

        return super()._build_lc_column(index, keys, ctx)

    @abstractmethod
    def _string_to_ticks(self, time_str: str) -> int:
        """Parse a string into integer ticks."""
        raise NotImplementedError

    @abstractmethod
    def _timedelta_to_ticks(self, td: timedelta) -> int:
        """Convert a timedelta into integer ticks."""
        raise NotImplementedError

    @abstractmethod
    def _ticks_to_timedelta(self, ticks: int) -> timedelta:
        """Convert integer ticks into a timedelta."""
        raise NotImplementedError

    @abstractmethod
    def _ticks_to_string(self, ticks: int) -> str:
        """Format integer ticks as a string."""
        raise NotImplementedError

    @property
    @abstractmethod
    def max_ticks(self) -> int:
        """Maximum tick value representable by this type."""
        raise NotImplementedError

    @property
    @abstractmethod
    def min_ticks(self) -> int:
        """Minimum tick value representable by this type."""
        raise NotImplementedError


class Time(TimeBase):
    """ClickHouse Time type with second precision."""

    _array_type = "i"
    byte_size = 4
    np_type = "timedelta64[s]"

    @property
    def max_ticks(self) -> int:
        return self.MAX_TIME_SECONDS

    @property
    def min_ticks(self) -> int:
        return self.MIN_TIME_SECONDS

    def _string_to_ticks(self, time_str: str) -> int:
        """Parse string format 'HHH:MM:SS[.fff]' to ticks (seconds)."""
        parts = self._parse_core(time_str)

        # For consistency with timedelta inserts, we ignore
        # any fractional part, effectively flooring to the whole second.
        ticks = parts.hours * 3600 + parts.minutes * 60 + parts.seconds

        if parts.is_negative:
            ticks = -ticks
        self._validate_range(ticks, time_str)

        return ticks

    def _ticks_to_string(self, ticks: int) -> str:
        """Format ticks (seconds) as 'HHH:MM:SS' string."""
        sign = "-" if ticks < 0 else ""
        t = abs(ticks)
        h, rem = divmod(t, 3600)
        m, s = divmod(rem, 60)

        return f"{sign}{h:03d}:{m:02d}:{s:02d}"

    def _timedelta_to_ticks(self, td: timedelta) -> int:
        """Convert timedelta to ticks (seconds), flooring fractional seconds."""
        # Just call int on total seconds. Note this effectively floors any
        # fractional parts of a second included in the timedelta object.
        total = int(td.total_seconds())
        self._validate_range(total, td)

        return total

    def _ticks_to_timedelta(self, ticks: int) -> timedelta:
        """Convert ticks (seconds) to timedelta."""
        return timedelta(seconds=ticks)


class Time64(TimeBase):
    """ClickHouse Time64 type with configurable sub-second precision."""

    __slots__ = ("scale", "precision", "unit")
    _array_type = "q"
    byte_size = 8

    def __init__(self, type_def):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str
        self.scale = type_def.values[0]
        if self.scale not in (3, 6, 9):
            raise ProgrammingError(
                f"Unsupported Time64 scale {self.scale}; "
                "only 3, 6, or 9 are allowed for NumPy."
            )
        self.precision = 10**self.scale
        self.unit = np_date_types.get(self.scale)

    @property
    def np_type(self) -> str:
        return f"timedelta64{self.unit}"

    @property
    def max_ticks(self) -> int:
        return self.MAX_TIME_SECONDS * self.precision + (self.precision - 1)

    @property
    def min_ticks(self) -> int:
        return -self.max_ticks

    def _string_to_ticks(self, time_str: str) -> int:
        """Parse string format 'HHH:MM:SS[.fff]' to ticks with sub-second precision."""
        parts = self._parse_core(time_str)
        frac_ticks = int((parts.frac or "").ljust(self.scale, "0")[: self.scale])
        ticks = (
            parts.hours * 3600 + parts.minutes * 60 + parts.seconds
        ) * self.precision + frac_ticks
        if parts.is_negative:
            ticks = -ticks
        self._validate_range(ticks, time_str)

        return ticks

    def _ticks_to_string(self, ticks: int) -> str:
        """Format ticks as 'HHH:MM:SS[.fff]' string with sub-second precision."""
        sign = "-" if ticks < 0 else ""
        t = abs(ticks)
        sec_part, frac_part = divmod(t, self.precision)
        h, rem = divmod(sec_part, 3600)
        m, s = divmod(rem, 60)
        frac_str = f".{frac_part:0{self.scale}d}" if self.scale else ""

        return f"{sign}{h:03d}:{m:02d}:{s:02d}{frac_str}"

    def _timedelta_to_ticks(self, td: timedelta) -> int:
        """Convert timedelta to ticks with sub-second precision."""
        total_us = int(td.total_seconds()) * self._MICROS_PER_SECOND + td.microseconds
        ticks = (total_us * self.precision) // self._MICROS_PER_SECOND
        self._validate_range(ticks, td)

        return ticks

    def _ticks_to_timedelta(self, ticks: int) -> timedelta:
        """Convert ticks to timedelta with sub-second precision."""
        neg = ticks < 0
        t = abs(ticks)
        sec_part = t // self.precision
        frac_part = t - sec_part * self.precision
        micros = (frac_part * self._MICROS_PER_SECOND) // self.precision
        td = timedelta(seconds=sec_part, microseconds=micros)

        return -td if neg else td
