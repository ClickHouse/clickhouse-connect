use super::*;

#[derive(Clone, Copy)]
enum NumpyByteOrder {
    Little,
    Big,
}

#[derive(Clone, Copy)]
struct NumpyTimedeltaMeta {
    order: NumpyByteOrder,
    /// Duration of one stored unit as numerator/denominator seconds.
    numerator: i128,
    denominator: i128,
}

/// Parse a NumPy dtype object's `str` form without importing NumPy. Examples
/// are `<m8[ns]`, `>m8[ms]`, and `<m8[10us]`; the unitless `<m8` parses with
/// the generic flag set. Calendar-relative Y/M units are not accepted because
/// they do not have a fixed duration in seconds.
fn parse_timedelta_dtype(dtype: &Bound<'_, PyAny>) -> Option<(NumpyTimedeltaMeta, bool)> {
    let dtype_str = dtype.getattr(intern!(dtype.py(), "str")).ok()?;
    let dtype_str = dtype_str.downcast::<PyString>().ok()?.to_str().ok()?;
    let (order, rest) = match dtype_str.as_bytes().split_first()? {
        (b'<', rest) => (NumpyByteOrder::Little, rest),
        (b'>', rest) => (NumpyByteOrder::Big, rest),
        (b'=', rest) | (b'|', rest) => (
            if cfg!(target_endian = "little") {
                NumpyByteOrder::Little
            } else {
                NumpyByteOrder::Big
            },
            rest,
        ),
        _ => return None,
    };
    let rest = std::str::from_utf8(rest).ok()?;
    if rest == "m8" {
        let meta = NumpyTimedeltaMeta {
            order,
            numerator: 1,
            denominator: 1,
        };
        return Some((meta, true));
    }
    let unit = rest.strip_prefix("m8[")?.strip_suffix(']')?;
    let digit_count = unit.bytes().take_while(u8::is_ascii_digit).count();
    let multiplier = if digit_count == 0 {
        1i128
    } else {
        unit[..digit_count].parse::<i128>().ok()?
    };
    let base = &unit[digit_count..];
    let (unit_numerator, denominator) = match base {
        "W" => (604_800, 1),
        "D" => (86_400, 1),
        "h" => (3_600, 1),
        "m" => (60, 1),
        "s" => (1, 1),
        "ms" => (1, 1_000),
        "us" => (1, 1_000_000),
        "ns" => (1, 1_000_000_000),
        "ps" => (1, 1_000_000_000_000),
        "fs" => (1, 1_000_000_000_000_000),
        "as" => (1, 1_000_000_000_000_000_000),
        _ => return None,
    };
    Some((
        NumpyTimedeltaMeta {
            order,
            numerator: multiplier.checked_mul(unit_numerator)?,
            denominator,
        },
        false,
    ))
}

/// Non-generic timedelta meta read from a value's `dtype` attribute.
fn numpy_timedelta_meta(value: &Bound<'_, PyAny>) -> Option<NumpyTimedeltaMeta> {
    let dtype = value.getattr(intern!(value.py(), "dtype")).ok()?;
    match parse_timedelta_dtype(&dtype) {
        Some((meta, false)) => Some(meta),
        _ => None,
    }
}

fn numpy_i64(bytes: &[u8], order: NumpyByteOrder) -> Option<i64> {
    let raw: [u8; 8] = bytes.try_into().ok()?;
    Some(match order {
        NumpyByteOrder::Little => i64::from_le_bytes(raw),
        NumpyByteOrder::Big => i64::from_be_bytes(raw),
    })
}

fn numpy_timedelta_scalar_raw(
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<(i64, NumpyTimedeltaMeta)>> {
    let Ok(dtype) = value.getattr(intern!(value.py(), "dtype")) else {
        return Ok(None);
    };
    let Some((meta, generic)) = parse_timedelta_dtype(&dtype) else {
        return Ok(None);
    };
    let bytes = value.call_method0(intern!(value.py(), "tobytes"))?;
    let bytes = bytes
        .downcast::<PyBytes>()
        .map_err(|_| PyValueError::new_err("NumPy timedelta tobytes() did not return bytes"))?;
    let raw = numpy_i64(bytes.as_bytes(), meta.order)
        .ok_or_else(|| PyValueError::new_err("NumPy timedelta scalar is not 8 bytes"))?;
    if generic && raw != i64::MIN {
        return Ok(None);
    }
    Ok(Some((raw, meta)))
}

/// NumPy scalar probing requires Python `dtype`/`tobytes` lookups. Keep those
/// off the established Time input paths, especially the per-row timedelta hot
/// path. Exact timedelta/time objects are excluded; their subclasses
/// (pd.Timedelta, pandas NaT) go through the probe. These checks use only
/// CPython/PyO3 type predicates and run no Python.
#[inline]
fn should_probe_numpy_timedelta(value: &Bound<'_, PyAny>) -> bool {
    // SAFETY: value is a valid object pointer; PyLong_Check and
    // PyUnicode_Check are C-level type predicates that run no Python.
    if unsafe {
        ffi::PyLong_Check(value.as_ptr()) != 0 || ffi::PyUnicode_Check(value.as_ptr()) != 0
    } {
        return false;
    }
    if value.downcast::<PyFloat>().is_ok() {
        return false;
    }
    if value.downcast::<PyDelta>().is_ok() {
        // SAFETY: the successful downcast imported the datetime C-API.
        return unsafe { ffi::PyDelta_CheckExact(value.as_ptr()) } == 0;
    }
    if value.downcast::<PyTime>().is_ok() {
        // SAFETY: the successful downcast imported the datetime C-API.
        return unsafe { ffi::PyTime_CheckExact(value.as_ptr()) } == 0;
    }
    true
}

/// pandas NaT detection by C-level type name; runs no Python.
fn is_pandas_nat(value: &Bound<'_, PyAny>) -> bool {
    // SAFETY: Py_TYPE of a valid object; tp_name is a NUL-terminated string
    // owned by the type.
    let name = unsafe { std::ffi::CStr::from_ptr((*ffi::Py_TYPE(value.as_ptr())).tp_name) };
    let name = name.to_bytes();
    name == b"NaTType" || name.ends_with(b".NaTType")
}

/// Non-null outcome of probing one Time/Time64 cell.
pub(super) enum TimeProbe {
    Nat,
    Ticks(i64),
}

/// Per-column state for probing Time/Time64 cells that are not one of the
/// exact fast types. The parsed dtype and unit ratio are cached and reused
/// while subsequent cells carry an identical or equal dtype object.
pub(super) struct TimeScalarProbe {
    precision: u8,
    fractional: bool,
    type_name: &'static str,
    cache: Option<TimeDtypeCache>,
}

struct TimeDtypeCache {
    dtype: Py<PyAny>,
    order: NumpyByteOrder,
    generic: bool,
    ratio: NumpyTimeRatio,
}

impl TimeScalarProbe {
    pub(super) fn new(ch_type: &ChType) -> Option<Self> {
        match ch_type {
            ChType::Time => Some(Self {
                precision: 0,
                fractional: false,
                type_name: "Time",
                cache: None,
            }),
            ChType::Time64 { precision } => Some(Self {
                precision: *precision,
                fractional: true,
                type_name: "Time64",
                cache: None,
            }),
            _ => None,
        }
    }

    /// Probe one cell. `Ok(None)` means the value is neither a NumPy
    /// timedelta scalar nor pandas NaT; the caller converts it normally.
    pub(super) fn probe(
        &mut self,
        value: &Bound<'_, PyAny>,
        column: &str,
        row: usize,
    ) -> PyResult<Option<TimeProbe>> {
        if !should_probe_numpy_timedelta(value) {
            return Ok(None);
        }
        let py = value.py();
        let Ok(dtype) = value.getattr(intern!(py, "dtype")) else {
            return Ok(is_pandas_nat(value).then_some(TimeProbe::Nat));
        };
        let cached = self.cache.as_ref().and_then(|cached| {
            (cached.dtype.as_ptr() == dtype.as_ptr()
                || cached.dtype.bind(py).eq(&dtype).unwrap_or(false))
            .then_some((cached.order, cached.generic, cached.ratio))
        });
        let (order, generic, ratio) = match cached {
            Some(hit) => hit,
            None => {
                let Some((meta, generic)) = parse_timedelta_dtype(&dtype) else {
                    return Ok(None);
                };
                let ratio = numpy_time_ratio(meta, self.precision, column)?;
                self.cache = Some(TimeDtypeCache {
                    dtype: dtype.unbind(),
                    order: meta.order,
                    generic,
                    ratio,
                });
                (meta.order, generic, ratio)
            }
        };
        let bytes = value.call_method0(intern!(py, "tobytes"))?;
        let bytes = bytes.downcast::<PyBytes>().map_err(|_| {
            PyValueError::new_err(format!(
                "column {column:?} row {row} NumPy timedelta tobytes() did not return bytes"
            ))
        })?;
        let raw = numpy_i64(bytes.as_bytes(), order).ok_or_else(|| {
            PyValueError::new_err(format!(
                "column {column:?} row {row} NumPy timedelta scalar is not 8 bytes"
            ))
        })?;
        if raw == i64::MIN {
            return Ok(Some(TimeProbe::Nat));
        }
        if generic {
            return Ok(None);
        }
        let ticks = rescale_numpy_timedelta(
            raw,
            ratio,
            self.fractional,
            self.precision,
            column,
            row,
            self.type_name,
        )?;
        Ok(Some(TimeProbe::Ticks(ticks)))
    }
}

/// Build the Time/Time64 scalar for probed ticks.
pub(super) fn time_ticks_scalar(
    ch_type: &ChType,
    ticks: i64,
    column: &str,
    row: usize,
) -> PyResult<Scalar> {
    match ch_type {
        ChType::Time => {
            Ok(Scalar::Time(i32::try_from(ticks).map_err(|_| {
                time_range_error(column, row, "Time", ticks)
            })?))
        }
        ChType::Time64 { .. } => Ok(Scalar::Time64(ticks)),
        _ => Err(PyValueError::new_err("internal Time scalar type mismatch")),
    }
}

#[derive(Clone, Copy)]
enum NumpyTimeRatio {
    Identity,
    Multiply(i64),
    Divide(i64),
    General { numerator: i128, denominator: i128 },
}

fn gcd_i128(mut left: i128, mut right: i128) -> i128 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

/// Resolve source units -> target ticks once per column. Common NumPy units
/// become identity, checked i64 multiply, or signed i64 division; only unusual
/// dtype multipliers retain the general i128 path.
fn numpy_time_ratio(
    meta: NumpyTimedeltaMeta,
    precision: u8,
    column: &str,
) -> PyResult<NumpyTimeRatio> {
    let numerator = meta
        .numerator
        .checked_mul(i128::from(time64_scale(precision)))
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "column {column:?} NumPy timedelta unit scale overflows"
            ))
        })?;
    let common = gcd_i128(numerator, meta.denominator);
    let numerator = numerator / common;
    let denominator = meta.denominator / common;
    if numerator == 1 && denominator == 1 {
        Ok(NumpyTimeRatio::Identity)
    } else if denominator == 1 {
        Ok(match i64::try_from(numerator) {
            Ok(multiplier) => NumpyTimeRatio::Multiply(multiplier),
            Err(_) => NumpyTimeRatio::General {
                numerator,
                denominator,
            },
        })
    } else if numerator == 1 {
        Ok(match i64::try_from(denominator) {
            Ok(divisor) => NumpyTimeRatio::Divide(divisor),
            Err(_) => NumpyTimeRatio::General {
                numerator,
                denominator,
            },
        })
    } else {
        Ok(NumpyTimeRatio::General {
            numerator,
            denominator,
        })
    }
}

/// Convert one raw NumPy timedelta count using a precomputed unit ratio. Signed
/// division deliberately truncates toward zero for sub-tick negatives.
fn rescale_numpy_timedelta(
    raw: i64,
    ratio: NumpyTimeRatio,
    fractional: bool,
    precision: u8,
    column: &str,
    row: usize,
    type_name: &str,
) -> PyResult<i64> {
    let ticks = match ratio {
        NumpyTimeRatio::Identity => raw,
        NumpyTimeRatio::Multiply(multiplier) => raw
            .checked_mul(multiplier)
            .ok_or_else(|| time_range_error(column, row, type_name, raw))?,
        NumpyTimeRatio::Divide(divisor) => raw / divisor,
        NumpyTimeRatio::General {
            numerator,
            denominator,
        } => {
            let ticks = i128::from(raw)
                .checked_mul(numerator)
                .ok_or_else(|| time_range_error(column, row, type_name, raw))?
                / denominator;
            i64::try_from(ticks).map_err(|_| time_range_error(column, row, type_name, ticks))?
        }
    };
    let max = if fractional {
        max_time64_ticks(precision)
    } else {
        MAX_TIME_SECONDS
    };
    if ticks < -max || ticks > max {
        return Err(time_range_error(column, row, type_name, ticks));
    }
    Ok(ticks)
}

fn numpy_timedelta_source<'py>(
    py: Python<'py>,
    values: &Bound<'py, PyAny>,
) -> PyResult<Option<(Bound<'py, PyAny>, NumpyTimedeltaMeta)>> {
    let Some(meta) = numpy_timedelta_meta(values) else {
        return Ok(None);
    };
    if values.getattr("tobytes").is_ok() {
        return Ok(Some((values.clone(), meta)));
    }
    let Ok(to_numpy) = values.getattr("to_numpy") else {
        return Ok(None);
    };
    let kwargs = PyDict::new(py);
    kwargs.set_item("copy", false)?;
    let array = to_numpy.call((), Some(&kwargs))?;
    let Some(array_meta) = numpy_timedelta_meta(&array) else {
        return Ok(None);
    };
    Ok(Some((array, array_meta)))
}

/// Dependency-free ndarray/pandas fast path. `tobytes` performs one safe bulk
/// copy for arbitrary strides; conversion then walks the contiguous bytes in
/// Rust with no per-cell Python calls or temporary objects.
pub(super) fn try_numpy_timedelta_column(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Option<Column>> {
    let (precision, fractional, type_name) = match ch_type {
        ChType::Time => (0, false, "Time"),
        ChType::Time64 { precision } => (*precision, true, "Time64"),
        _ => return Ok(None),
    };
    let Some((source, meta)) = numpy_timedelta_source(py, values)? else {
        return Ok(None);
    };
    let ndim = source
        .getattr("ndim")
        .and_then(|value| value.extract::<usize>())
        .map_err(|_| {
            PyValueError::new_err(format!(
                "column {name:?} NumPy timedelta source has no valid ndim"
            ))
        })?;
    if ndim != 1 {
        return Err(PyValueError::new_err(format!(
            "column {name:?} NumPy timedelta source must be one-dimensional, got {ndim} dimensions"
        )));
    }
    let bytes = source.call_method0("tobytes")?;
    let bytes = bytes.downcast::<PyBytes>().map_err(|_| {
        PyValueError::new_err(format!(
            "column {name:?} NumPy timedelta tobytes() did not return bytes"
        ))
    })?;
    let expected_len = row_count
        .checked_mul(8)
        .ok_or_else(|| PyValueError::new_err(format!("column {name:?} byte size overflow")))?;
    let actual_len = bytes.as_bytes().len();
    if actual_len != expected_len {
        return Err(PyValueError::new_err(format!(
            "column {name:?} NumPy timedelta data is {} bytes, expected {expected_len}",
            actual_len
        )));
    }

    let ratio = numpy_time_ratio(meta, precision, name)?;
    let mut nulls = nullable.then(|| Vec::with_capacity(row_count));
    macro_rules! build_numpy_time_column {
        ($rust_type:ty, $column_variant:ident) => {{
            let mut out = Vec::<$rust_type>::with_capacity(row_count);
            for (row, raw_bytes) in bytes.as_bytes().chunks_exact(8).enumerate() {
                let raw = numpy_i64(raw_bytes, meta.order).ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "column {name:?} row {row} has invalid NumPy timedelta bytes"
                    ))
                })?;
                if raw == i64::MIN {
                    let Some(nulls) = &mut nulls else {
                        return Err(PyValueError::new_err(format!(
                            "column {name:?} row {row} is NaT but {ch_type} is not Nullable"
                        )));
                    };
                    nulls.push(1);
                    out.push(0);
                    continue;
                }
                if let Some(nulls) = &mut nulls {
                    nulls.push(0);
                }
                let ticks = rescale_numpy_timedelta(
                    raw, ratio, fractional, precision, name, row, type_name,
                )?;
                out.push(
                    <$rust_type>::try_from(ticks)
                        .map_err(|_| time_range_error(name, row, type_name, ticks))?,
                );
            }
            let validity = nulls.map(|map| Bitmap::from_ch_null_map(&map));
            Some(Column::$column_variant(match validity {
                Some(validity) => PrimitiveColumn::new_nullable(out, validity),
                None => PrimitiveColumn::new(out),
            }))
        }};
    }
    Ok(match ch_type {
        ChType::Time => build_numpy_time_column!(i32, Time),
        ChType::Time64 { .. } => build_numpy_time_column!(i64, Time64),
        _ => None,
    })
}

/// Narrowing i64 conversion for the fast paths. A generic helper so macro
/// expansions do not trip clippy's fallible-conversion lint when the target
/// is i64 itself.
#[inline]
fn narrow_i64<T: TryFrom<i64>>(value: i64) -> Result<T, ()> {
    T::try_from(value).map_err(|_| ())
}

#[inline]
pub(super) fn time64_scale(precision: u8) -> i64 {
    // parse_ch_type rejects precisions above 9.
    debug_assert!(precision <= 9);
    match precision {
        0 => 1,
        1 => 10,
        2 => 100,
        3 => 1_000,
        4 => 10_000,
        5 => 100_000,
        6 => 1_000_000,
        7 => 10_000_000,
        8 => 100_000_000,
        9 => 1_000_000_000,
        // parse_ch_type rejects this before the binding builds a column.
        _ => 0,
    }
}

#[inline]
pub(super) fn max_time64_ticks(precision: u8) -> i64 {
    let scale = time64_scale(precision);
    MAX_TIME_SECONDS * scale + (scale - 1)
}

/// Temporal wire values: the fast path accepts exact raw ints only (the same
/// values `convert_scalar` accepts without touching the object protocol);
/// date/datetime/str objects and out-of-range ints go through the fallback,
/// which carries each type's specific conversion and range errors.
macro_rules! impl_fast_temporal {
    ($name:ident, $prim:ty, $variant:ident) => {
        impl_fast_temporal!($name, $prim, $variant, |_value, _ch_type, _fast_limit| true);
    };
    ($name:ident, $prim:ty, $variant:ident, $validate:expr) => {
        #[derive(Clone, Copy)]
        #[repr(transparent)]
        pub(super) struct $name($prim);

        impl FastValue for $name {
            const DEFAULT: Self = $name(0);

            #[inline]
            unsafe fn from_exact(
                ptr: *mut ffi::PyObject,
                ch_type: &ChType,
                fast_limit: i64,
            ) -> Result<Self, ()> {
                let value = narrow_i64::<$prim>(exact_long_as_i64(ptr)?)?;
                if ($validate)(value, ch_type, fast_limit) {
                    Ok(Self(value))
                } else {
                    Err(())
                }
            }

            fn from_scalar(scalar: Scalar) -> PyResult<Self> {
                match scalar {
                    Scalar::$variant(value) => Ok(Self(value)),
                    _ => Err(PyValueError::new_err("internal scalar type mismatch")),
                }
            }

            fn into_column(values: Vec<Self>, validity: Option<Bitmap>) -> Column {
                // SAFETY: $name is #[repr(transparent)] over $prim.
                let values = unsafe { cast_vec::<Self, $prim>(values) };
                Column::$variant(match validity {
                    Some(validity) => PrimitiveColumn::new_nullable(values, validity),
                    None => PrimitiveColumn::new(values),
                })
            }
        }
    };
}

impl_fast_temporal!(DateVal, u16, Date);
impl_fast_temporal!(Date32Val, i32, Date32);
impl_fast_temporal!(DateTimeVal, u32, DateTime);
impl_fast_temporal!(DateTime64Val, i64, DateTime64);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(super) struct IntervalVal(i64);

impl FastValue for IntervalVal {
    const DEFAULT: Self = Self(0);

    #[inline]
    unsafe fn from_exact(
        ptr: *mut ffi::PyObject,
        _ch_type: &ChType,
        _fast_limit: i64,
    ) -> Result<Self, ()> {
        exact_long_as_i64(ptr).map(Self)
    }

    fn from_scalar(scalar: Scalar) -> PyResult<Self> {
        match scalar {
            Scalar::Interval(value) => Ok(Self(value)),
            _ => Err(PyValueError::new_err("internal scalar type mismatch")),
        }
    }

    fn from_buffer(
        py: Python<'_>,
        _name: &str,
        values: &Bound<'_, PyAny>,
        row_count: usize,
    ) -> PyResult<Option<Vec<Self>>> {
        // SAFETY: IntervalVal is #[repr(transparent)] over i64.
        Ok(buffer_values::<i64>(py, values, row_count)?
            .map(|values| unsafe { cast_vec::<i64, Self>(values) }))
    }

    fn into_column(values: Vec<Self>, validity: Option<Bitmap>) -> Column {
        // SAFETY: IntervalVal is #[repr(transparent)] over i64.
        let values = unsafe { cast_vec::<Self, i64>(values) };
        Column::Interval(match validity {
            Some(validity) => PrimitiveColumn::new_nullable(values, validity),
            None => PrimitiveColumn::new(values),
        })
    }
}

impl_fast_temporal!(
    TimeVal,
    i32,
    Time,
    |value: i32, _ch_type: &ChType, fast_limit: i64| {
        i64::from(value).unsigned_abs() <= fast_limit as u64
    }
);
impl_fast_temporal!(
    Time64Val,
    i64,
    Time64,
    |value: i64, _ch_type: &ChType, fast_limit: i64| { value.unsigned_abs() <= fast_limit as u64 }
);

pub(super) fn date_days(value: &Bound<'_, PyAny>, column: &str, row: usize) -> PyResult<i64> {
    // Precedence: int (incl. subclasses) -> date/datetime instance -> duck
    // tail (__index__, then toordinal). Cheap type checks avoid constructing
    // a TypeError per value; an object that is both a date instance and
    // int-convertible resolves as a date here. PyDate_Check also matches
    // datetime, a date subclass, like the tail's toordinal call.
    if unsafe { ffi::PyLong_Check(value.as_ptr()) } != 0 {
        // A wider-than-i64 int maps to the tail's conversion error without
        // re-running the failing extraction.
        return value
            .extract::<i64>()
            .map_err(|_| conversion_error(column, row, "Date"));
    }
    if value.is_instance_of::<PyDate>() {
        let ordinal = value
            .call_method0("toordinal")
            .and_then(|o| o.extract::<i64>())
            .map_err(|_| conversion_error(column, row, "Date"))?;
        return Ok(ordinal - EPOCH_DATE_ORDINAL);
    }
    // Duck-typed tail, e.g. numpy ints via __index__.
    if let Ok(days) = value.extract::<i64>() {
        return Ok(days);
    }
    let ordinal = value
        .call_method0("toordinal")
        .and_then(|o| o.extract::<i64>())
        .map_err(|_| conversion_error(column, row, "Date"))?;
    Ok(ordinal - EPOCH_DATE_ORDINAL)
}

pub(super) fn datetime_seconds(
    value: &Bound<'_, PyAny>,
    column: &str,
    row: usize,
) -> PyResult<i64> {
    // Precedence: int -> datetime instance -> duck tail; see date_days.
    if unsafe { ffi::PyLong_Check(value.as_ptr()) } != 0 {
        return value
            .extract::<i64>()
            .map_err(|_| conversion_error(column, row, "DateTime"));
    }
    if value.is_instance_of::<PyDateTime>() {
        let ts = value
            .call_method0("timestamp")
            .and_then(|o| o.extract::<f64>())
            .map_err(|_| conversion_error(column, row, "DateTime"))?;
        return finite_trunc_to_i64(ts, column, row, "DateTime");
    }
    // Duck-typed tail, e.g. numpy ints via __index__.
    if let Ok(secs) = value.extract::<i64>() {
        return Ok(secs);
    }
    let ts = value
        .call_method0("timestamp")
        .and_then(|o| o.extract::<f64>())
        .map_err(|_| conversion_error(column, row, "DateTime"))?;
    finite_trunc_to_i64(ts, column, row, "DateTime")
}

pub(super) fn datetime64_ticks(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    precision: u8,
    column: &str,
    row: usize,
) -> PyResult<i64> {
    // Precedence: int -> datetime instance -> duck tail; see date_days.
    if unsafe { ffi::PyLong_Check(value.as_ptr()) } != 0 {
        return value
            .extract::<i64>()
            .map_err(|_| conversion_error(column, row, "DateTime64"));
    }
    if value.is_instance_of::<PyDateTime>() {
        let secs = dt64_timestamp_secs(value, column, row)?;
        // is_instance_of imported the datetime C API, so the raw exact check
        // and struct accessor are safe. A subclass may override the
        // microsecond attribute, so only an exact datetime skips the getattr.
        let micros = if unsafe { ffi::PyDateTime_CheckExact(value.as_ptr()) } != 0 {
            i64::from(unsafe { ffi::PyDateTime_DATE_GET_MICROSECOND(value.as_ptr()) })
        } else {
            dt64_microsecond(value, column, row)?
        };
        return dt64_ticks_math(secs, micros, precision, column, row);
    }

    if let Ok(ticks) = value.extract::<i64>() {
        return Ok(ticks);
    }

    let value = if let Ok(s) = value.downcast::<PyString>() {
        py.import("datetime")?
            .getattr("datetime")?
            .call_method1("fromisoformat", (s.to_str()?,))?
    } else {
        value.clone()
    };

    let secs = dt64_timestamp_secs(&value, column, row)?;
    let micros = dt64_microsecond(&value, column, row)?;
    dt64_ticks_math(secs, micros, precision, column, row)
}

fn dt64_timestamp_secs(value: &Bound<'_, PyAny>, column: &str, row: usize) -> PyResult<i64> {
    let secs = value
        .call_method0("timestamp")
        .and_then(|o| o.extract::<f64>())
        .map_err(|_| conversion_error(column, row, "DateTime64"))?;
    finite_floor_to_i64(secs, column, row, "DateTime64")
}

fn dt64_microsecond(value: &Bound<'_, PyAny>, column: &str, row: usize) -> PyResult<i64> {
    value
        .getattr("microsecond")
        .and_then(|o| o.extract::<i64>())
        .map_err(|_| conversion_error(column, row, "DateTime64"))
}

fn dt64_ticks_math(
    secs: i64,
    micros: i64,
    precision: u8,
    column: &str,
    row: usize,
) -> PyResult<i64> {
    if !(0..1_000_000).contains(&micros) {
        return Err(PyValueError::new_err(format!(
            "column {column:?} row {row} DateTime64 microsecond {micros} is outside range"
        )));
    }
    let scale = 10i128.pow(u32::from(precision));
    let total_micros = i128::from(secs)
        .checked_mul(1_000_000)
        .and_then(|v| v.checked_add(i128::from(micros)))
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "column {column:?} row {row} DateTime64 value overflows"
            ))
        })?;
    let ticks = total_micros
        .checked_mul(scale)
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "column {column:?} row {row} DateTime64 value overflows"
            ))
        })?
        .div_euclid(1_000_000);
    i64::try_from(ticks).map_err(|_| {
        PyValueError::new_err(format!(
            "column {column:?} row {row} DateTime64 value {ticks} is outside Int64 range"
        ))
    })
}

pub(super) fn time_ticks(value: &Bound<'_, PyAny>, column: &str, row: usize) -> PyResult<i32> {
    let ticks = time_like_ticks(value, 0, false, column, row, "Time")?;
    i32::try_from(ticks).map_err(|_| time_range_error(column, row, "Time", ticks))
}

pub(super) fn time64_ticks(
    value: &Bound<'_, PyAny>,
    precision: u8,
    column: &str,
    row: usize,
) -> PyResult<i64> {
    time_like_ticks(value, precision, true, column, row, "Time64")
}

/// Convert the accepted Time/Time64 Python values to raw signed ticks. Exact
/// timedelta/time objects use PyO3's C-API accessors, so the common object path
/// performs no Python calls or attribute lookups. Integers are raw ticks;
/// floats follow TimeBase's `int(value)` truncation policy.
fn time_like_ticks(
    value: &Bound<'_, PyAny>,
    precision: u8,
    fractional: bool,
    column: &str,
    row: usize,
    type_name: &str,
) -> PyResult<i64> {
    let scale = time64_scale(precision);
    // SAFETY: value is a valid object pointer; PyLong_Check and PyFloat_Check
    // are C-level type predicates that run no Python.
    let (is_long, is_float) = unsafe {
        (
            ffi::PyLong_Check(value.as_ptr()) != 0,
            ffi::PyFloat_Check(value.as_ptr()) != 0,
        )
    };
    let ticks = if is_long {
        value
            .extract::<i64>()
            .map_err(|_| conversion_error(column, row, type_name))?
    } else if let Ok(delta) = value.downcast::<PyDelta>() {
        timedelta_ticks(delta, scale, precision, fractional, column, row, type_name)?
    } else if let Ok(time) = value.downcast::<PyTime>() {
        let total_micros = ((i128::from(time.get_hour()) * 3_600
            + i128::from(time.get_minute()) * 60
            + i128::from(time.get_second()))
            * 1_000_000)
            + i128::from(time.get_microsecond());
        let ticks = total_micros * i128::from(scale) / 1_000_000;
        i64::try_from(ticks).map_err(|_| time_range_error(column, row, type_name, ticks))?
    } else if let Ok(s) = value.downcast::<PyString>() {
        parse_time_literal(s.to_str()?, precision, fractional, column, row, type_name)?
    } else if is_float {
        // SAFETY: value is a float, so PyFloat_AsDouble cannot fail.
        let number = unsafe { ffi::PyFloat_AsDouble(value.as_ptr()) };
        finite_trunc_to_i64(number, column, row, type_name)?
    } else if let Some((raw, meta)) = numpy_timedelta_scalar_raw(value)? {
        if raw == i64::MIN {
            return Err(PyValueError::new_err(format!(
                "column {column:?} row {row} is NaT and cannot be converted to {type_name}"
            )));
        }
        let ratio = numpy_time_ratio(meta, precision, column)?;
        rescale_numpy_timedelta(raw, ratio, fractional, precision, column, row, type_name)?
    } else if is_pandas_nat(value) {
        return Err(PyValueError::new_err(format!(
            "column {column:?} row {row} is NaT and cannot be converted to {type_name}"
        )));
    } else {
        value
            .extract::<i64>()
            .map_err(|_| conversion_error(column, row, type_name))?
    };

    let max = if fractional {
        max_time64_ticks(precision)
    } else {
        MAX_TIME_SECONDS
    };
    if ticks < -max || ticks > max {
        return Err(time_range_error(column, row, type_name, ticks));
    }
    Ok(ticks)
}

/// Ticks from a datetime.timedelta. Exact objects read the C-struct fields;
/// subclasses (pd.Timedelta) first try the ns-resolution `asm8` scalar so
/// sub-microsecond values are not truncated.
fn timedelta_ticks(
    delta: &Bound<'_, PyDelta>,
    scale: i64,
    precision: u8,
    fractional: bool,
    column: &str,
    row: usize,
    type_name: &str,
) -> PyResult<i64> {
    // SAFETY: a successful PyDelta downcast produced `delta`, so the datetime
    // C-API is imported; the exact check runs no Python.
    if unsafe { ffi::PyDelta_CheckExact(delta.as_ptr()) } == 0 {
        if let Some(ticks) =
            timedelta_subclass_ticks(delta, precision, fractional, column, row, type_name)?
        {
            return Ok(ticks);
        }
    }
    const MICROS_PER_SECOND: i128 = 1_000_000;
    const MICROS_PER_DAY: i128 = 86_400 * MICROS_PER_SECOND;
    let total_micros = i128::from(delta.get_days()) * MICROS_PER_DAY
        + i128::from(delta.get_seconds()) * MICROS_PER_SECOND
        + i128::from(delta.get_microseconds());
    let scaled = total_micros * i128::from(scale);
    // Signed division truncates sub-tick values toward zero, consistent
    // with Time strings and the Python object decode policy.
    let ticks = scaled / MICROS_PER_SECOND;
    i64::try_from(ticks).map_err(|_| time_range_error(column, row, type_name, ticks))
}

/// ns-resolution ticks for timedelta subclasses exposing `asm8`
/// (pd.Timedelta). `Ok(None)` means the attribute or its dtype probe does not
/// apply and the caller falls back to the struct fields.
fn timedelta_subclass_ticks(
    value: &Bound<'_, PyAny>,
    precision: u8,
    fractional: bool,
    column: &str,
    row: usize,
    type_name: &str,
) -> PyResult<Option<i64>> {
    let Ok(asm8) = value.getattr(intern!(value.py(), "asm8")) else {
        return Ok(None);
    };
    let Some((raw, meta)) = numpy_timedelta_scalar_raw(&asm8)? else {
        return Ok(None);
    };
    if raw == i64::MIN {
        return Err(PyValueError::new_err(format!(
            "column {column:?} row {row} is NaT and cannot be converted to {type_name}"
        )));
    }
    let ratio = numpy_time_ratio(meta, precision, column)?;
    rescale_numpy_timedelta(raw, ratio, fractional, precision, column, row, type_name).map(Some)
}

fn parse_time_literal(
    value: &str,
    precision: u8,
    fractional: bool,
    column: &str,
    row: usize,
    type_name: &str,
) -> PyResult<i64> {
    let value = value.trim();
    let (negative, unsigned) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value),
    };
    let (hms, fraction) = match unsigned.split_once('.') {
        Some((hms, fraction))
            if !fraction.is_empty()
                && !fraction.contains('.')
                && fraction.bytes().all(|b| b.is_ascii_digit()) =>
        {
            (hms, Some(fraction))
        }
        Some(_) => return Err(time_literal_error(column, row, type_name, value)),
        None => (unsigned, None),
    };
    let mut parts = hms.split(':');
    let hours = parse_time_part(parts.next(), column, row, type_name, value)?;
    let minutes = parse_time_part(parts.next(), column, row, type_name, value)?;
    let seconds = parse_time_part(parts.next(), column, row, type_name, value)?;
    if parts.next().is_some() || hours > 999 || minutes > 59 || seconds > 59 {
        return Err(time_literal_error(column, row, type_name, value));
    }

    let scale = time64_scale(precision);
    let mut fraction_ticks = 0i64;
    if fractional {
        let mut digits = 0u8;
        for byte in fraction
            .unwrap_or_default()
            .bytes()
            .take(usize::from(precision))
        {
            fraction_ticks = fraction_ticks * 10 + i64::from(byte - b'0');
            digits += 1;
        }
        for _ in digits..precision {
            fraction_ticks *= 10;
        }
    }
    let ticks = (hours * 3_600 + minutes * 60 + seconds) * scale + fraction_ticks;
    Ok(if negative { -ticks } else { ticks })
}

fn parse_time_part(
    part: Option<&str>,
    column: &str,
    row: usize,
    type_name: &str,
    literal: &str,
) -> PyResult<i64> {
    let part = part
        .filter(|part| !part.is_empty())
        .ok_or_else(|| time_literal_error(column, row, type_name, literal))?;
    if !part.bytes().all(|b| b.is_ascii_digit()) {
        return Err(time_literal_error(column, row, type_name, literal));
    }
    part.parse::<i64>()
        .map_err(|_| time_literal_error(column, row, type_name, literal))
}

fn time_range_error<T: std::fmt::Display>(
    column: &str,
    row: usize,
    type_name: &str,
    value: T,
) -> PyErr {
    PyValueError::new_err(format!(
        "column {column:?} row {row} {type_name} value {value} is outside logical range"
    ))
}

fn time_literal_error(column: &str, row: usize, type_name: &str, value: &str) -> PyErr {
    PyValueError::new_err(format!(
        "column {column:?} row {row} invalid {type_name} literal {value:?}"
    ))
}

fn finite_trunc_to_i64(value: f64, column: &str, row: usize, type_name: &str) -> PyResult<i64> {
    if !value.is_finite() || value < i64::MIN as f64 || value > i64::MAX as f64 {
        return Err(PyValueError::new_err(format!(
            "column {column:?} row {row} {type_name} timestamp is outside Int64 range"
        )));
    }
    Ok(value.trunc() as i64)
}

fn finite_floor_to_i64(value: f64, column: &str, row: usize, type_name: &str) -> PyResult<i64> {
    if !value.is_finite() || value < i64::MIN as f64 || value > i64::MAX as f64 {
        return Err(PyValueError::new_err(format!(
            "column {column:?} row {row} {type_name} timestamp is outside Int64 range"
        )));
    }
    Ok(value.floor() as i64)
}
