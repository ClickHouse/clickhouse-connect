use super::*;

pub(super) fn build_plain_column(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    if let Some(column) = try_fast_column(py, name, ch_type, values, row_count, false)? {
        return Ok(column);
    }
    if wide_int_layout(ch_type).is_some() {
        return wide_column_from_rows(py, name, ch_type, &column_values, row_count, false);
    }
    plain_scalar_column(py, name, ch_type, &column_values, row_count)
}

pub(super) fn plain_scalar_column<'py, R: RowAccess<'py>>(
    py: Python<'py>,
    name: &str,
    ch_type: &ChType,
    rows: &R,
    row_count: usize,
) -> PyResult<Column> {
    let mut scalars = Vec::with_capacity(row_count);
    for row in 0..row_count {
        let value = rows.value(row)?;
        if value.is_none() {
            return Err(PyValueError::new_err(format!(
                "column {name:?} row {row} is None but {ch_type} is not Nullable"
            )));
        }
        scalars.push(convert_scalar(py, ch_type, &value, name, row)?);
    }
    column_from_scalars(ch_type, scalars, None)
}

pub(super) fn build_nullable_column(
    py: Python<'_>,
    name: &str,
    inner: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    // Expand a Nullable over a name-decoration alias by its delegate:
    // Nullable(Point) -> Nullable(Tuple), Nullable(SimpleAggregateFunction(T))
    // -> Nullable(T). The physical delegate governs the nullable value shape.
    if let Some(delegate) = inner.physical_delegate() {
        return build_nullable_column(py, name, &delegate, values, row_count);
    }
    if let ChType::Tuple(elements) = inner {
        return build_tuple_column(py, name, elements, values, row_count, true);
    }
    if matches!(inner, ChType::Nothing) {
        return build_nothing_column(name, values, row_count, true);
    }
    if matches!(inner, ChType::Json { .. }) {
        return build_json_text_column(py, name, values, row_count, true);
    }
    if let ChType::QBit {
        element_type,
        dimension,
    } = inner
    {
        return build_qbit_column(py, name, *element_type, *dimension, values, row_count, true);
    }
    if matches!(
        inner,
        ChType::Nullable(_) | ChType::LowCardinality(_) | ChType::Array(_) | ChType::Map(..)
    ) {
        return Err(PyNotImplementedError::new_err(format!(
            "unsupported Nullable inner type {inner} for column {name:?}"
        )));
    }

    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    if let Some(column) = try_fast_column(py, name, inner, values, row_count, true)? {
        return Ok(column);
    }
    if wide_int_layout(inner).is_some() {
        return wide_column_from_rows(py, name, inner, &column_values, row_count, true);
    }
    nullable_scalar_column(py, name, inner, &column_values, row_count)
}

/// Build a wide-integer column over generic row access with one checked data
/// allocation. Each Python value writes directly into its final row slice.
fn wide_column_from_rows<'py, R: RowAccess<'py>>(
    py: Python<'py>,
    name: &str,
    ch_type: &ChType,
    rows: &R,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let (width, signed, type_name) = wide_int_layout(ch_type)
        .ok_or_else(|| PyValueError::new_err("internal wide integer type mismatch"))?;
    let mut data = wide_data_buffer(name, width, row_count)?;
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));
    for row in 0..row_count {
        let value = rows.value(row)?;
        if value.is_none() {
            let Some(null_map) = &mut null_map else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but {ch_type} is not Nullable"
                )));
            };
            null_map.push(1);
            continue;
        }
        if let Some(null_map) = &mut null_map {
            null_map.push(0);
        }
        let start = row * width;
        wide_int_into(
            py,
            &value,
            &mut data[start..start + width],
            signed,
            name,
            row,
            type_name,
        )?;
    }
    let validity = null_map.map(|nulls| Bitmap::from_ch_null_map(&nulls));
    finish_wide_int_column(ch_type, data, validity)
}

pub(super) fn nullable_scalar_column<'py, R: RowAccess<'py>>(
    py: Python<'py>,
    name: &str,
    inner: &ChType,
    rows: &R,
    row_count: usize,
) -> PyResult<Column> {
    let mut null_map = Vec::with_capacity(row_count);
    let mut scalars = Vec::with_capacity(row_count);
    let mut time_probe = TimeScalarProbe::new(inner);
    for row in 0..row_count {
        let value = rows.value(row)?;
        if value.is_none() {
            null_map.push(1);
            scalars.push(default_scalar(inner)?);
            continue;
        }
        if let Some(probe) = time_probe.as_mut() {
            match probe.probe(&value, name, row)? {
                Some(TimeProbe::Nat) => {
                    null_map.push(1);
                    scalars.push(default_scalar(inner)?);
                    continue;
                }
                Some(TimeProbe::Ticks(ticks)) => {
                    null_map.push(0);
                    scalars.push(time_ticks_scalar(inner, ticks, name, row)?);
                    continue;
                }
                None => {}
            }
        }
        if is_enum_nan(inner, &value) {
            null_map.push(1);
            scalars.push(default_scalar(inner)?);
            continue;
        }
        null_map.push(0);
        scalars.push(convert_scalar(py, inner, &value, name, row)?);
    }
    column_from_scalars(inner, scalars, Some(Bitmap::from_ch_null_map(&null_map)))
}
pub(super) struct ColumnValues<'py> {
    values: Bound<'py, PyAny>,
    len: usize,
    name: String,
}

impl<'py> ColumnValues<'py> {
    pub(super) fn new(values: &Bound<'py, PyAny>, name: &str) -> PyResult<Self> {
        if is_string_or_bytes_like(values) {
            return Err(PyValueError::new_err(format!(
                "column {name:?} values must be an indexable column container, not bare str or bytes"
            )));
        }
        let len = values.len().map_err(|_| {
            PyValueError::new_err(format!(
                "column {name:?} values must be an indexable column container"
            ))
        })?;
        Ok(Self {
            values: positional_source(values),
            len,
            name: name.to_string(),
        })
    }

    fn len(&self) -> usize {
        self.len
    }

    pub(super) fn get_item(&self, row: usize) -> PyResult<Bound<'py, PyAny>> {
        self.values.get_item(row).map_err(|_| {
            PyValueError::new_err(format!(
                "column {:?} row {row} could not be read from column container",
                self.name
            ))
        })
    }
}

/// Positional row access for the generic scalar loops: a Python column
/// container or a flattened strong-reference run.
pub(super) trait RowAccess<'py> {
    fn value(&self, row: usize) -> PyResult<Bound<'py, PyAny>>;

    fn validate(&self) -> PyResult<()> {
        Ok(())
    }
}

impl<'py> RowAccess<'py> for ColumnValues<'py> {
    fn value(&self, row: usize) -> PyResult<Bound<'py, PyAny>> {
        self.get_item(row)
    }
}

/// Borrowed exact-list access for JSON serialization. The configured Python
/// serializer can execute arbitrary code, so the list size is revalidated
/// before the next unchecked item read.
pub(super) struct ListRows<'a, 'py> {
    pub(super) py: Python<'py>,
    pub(super) list: &'a Bound<'py, PyList>,
    pub(super) name: &'a str,
    pub(super) expected: usize,
}

impl<'py> RowAccess<'py> for ListRows<'_, 'py> {
    fn value(&self, row: usize) -> PyResult<Bound<'py, PyAny>> {
        // SAFETY: callers iterate below `expected`, which `validate` confirms
        // after every operation that may execute Python.
        Ok(unsafe {
            Bound::from_borrowed_ptr(
                self.py,
                ffi::PyList_GET_ITEM(self.list.as_ptr(), row as ffi::Py_ssize_t),
            )
        })
    }

    fn validate(&self) -> PyResult<()> {
        if self.list.len() == self.expected {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "column {:?} values changed size during JSON serialization",
                self.name
            )))
        }
    }
}

/// Borrowed exact-tuple access needs no resize guard because tuples are
/// immutable.
pub(super) struct TupleRows<'a, 'py> {
    pub(super) py: Python<'py>,
    pub(super) tuple: &'a Bound<'py, PyTuple>,
}

impl<'py> RowAccess<'py> for TupleRows<'_, 'py> {
    fn value(&self, row: usize) -> PyResult<Bound<'py, PyAny>> {
        // SAFETY: row_count was checked against the immutable tuple length.
        Ok(unsafe {
            Bound::from_borrowed_ptr(
                self.py,
                ffi::PyTuple_GET_ITEM(self.tuple.as_ptr(), row as ffi::Py_ssize_t),
            )
        })
    }
}

/// Row access over flattened Array element pointers, kept valid by the
/// `FlatRefs` strong references.
pub(super) struct PtrRows<'a, 'py> {
    pub(super) py: Python<'py>,
    pub(super) ptrs: &'a [*mut ffi::PyObject],
}

impl<'py> RowAccess<'py> for PtrRows<'_, 'py> {
    fn value(&self, row: usize) -> PyResult<Bound<'py, PyAny>> {
        // SAFETY: FlatRefs holds a strong reference for every pointer in the
        // run for the whole build.
        Ok(unsafe { Bound::from_borrowed_ptr(self.py, self.ptrs[row]) })
    }
}

pub(super) fn check_row_count(
    name: &str,
    values: &ColumnValues<'_>,
    row_count: usize,
) -> PyResult<()> {
    let len = values.len();
    if len != row_count {
        return Err(PyValueError::new_err(format!(
            "column {name:?} has {len} values but row_count is {row_count}"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Fast paths for primitive numeric columns. The ChType dispatch runs once per
// column; the per-value loop reads borrowed pointers from an exact list or
// tuple, or a matching buffer-protocol container copies straight into Vec<T>.
// Anything the exact-type checks reject falls back to `convert_scalar` per
// item, so accepted-type and error semantics match the generic path.
// ---------------------------------------------------------------------------

/// Per-value conversion for the primitive fast paths.
pub(super) trait FastValue: Copy {
    const DEFAULT: Self;

    /// Convert an exact-type Python object without running Python code.
    /// `Err(())` means "no fast conversion, use the generic fallback" and
    /// guarantees no Python exception is left pending.
    ///
    /// # Safety
    ///
    /// Requires the GIL; `ptr` must be a valid, non-null object pointer.
    unsafe fn from_exact(
        ptr: *mut ffi::PyObject,
        ch_type: &ChType,
        fast_limit: i64,
    ) -> Result<Self, ()>;

    /// Unwrap the `Scalar` produced by the `convert_scalar` fallback.
    fn from_scalar(scalar: Scalar) -> PyResult<Self>;

    /// Copy a matching buffer-protocol container. `Ok(None)` for types with
    /// no buffer representation or containers that do not match.
    fn from_buffer(
        py: Python<'_>,
        name: &str,
        values: &Bound<'_, PyAny>,
        row_count: usize,
    ) -> PyResult<Option<Vec<Self>>> {
        let _ = (py, name, values, row_count);
        Ok(None)
    }

    fn into_column(values: Vec<Self>, validity: Option<Bitmap>) -> Column;
}

/// Truncate a Float32 to ClickHouse's upper-16-bit BFloat16 representation.
/// NaN inputs get the quiet bit set so truncation cannot yield infinity.
#[inline]
fn f32_to_bfloat16(value: f32) -> [u8; 2] {
    let bits = value.to_bits() | if value.is_nan() { 0x0040_0000 } else { 0 };
    ((bits >> 16) as u16).to_le_bytes()
}

/// Narrow a Float64 without turning a finite out-of-range value into infinity.
#[inline]
pub(super) fn checked_f64_to_bfloat16(value: f64) -> Result<[u8; 2], ()> {
    let narrowed = value as f32;
    if value.is_finite() && narrowed.is_infinite() {
        return Err(());
    }
    Ok(f32_to_bfloat16(narrowed))
}

/// Read an exact `int` as i64. On overflow the pending exception is cleared
/// and `Err(())` sends the value through the generic fallback, which produces
/// the standard conversion error.
///
/// # Safety
///
/// Requires the GIL; `ptr` must be a valid, non-null object pointer.
#[inline]
pub(super) unsafe fn exact_long_as_i64(ptr: *mut ffi::PyObject) -> Result<i64, ()> {
    if ffi::PyLong_CheckExact(ptr) == 0 {
        return Err(());
    }
    let value = ffi::PyLong_AsLongLong(ptr);
    if value == -1 && !ffi::PyErr_Occurred().is_null() {
        ffi::PyErr_Clear();
        return Err(());
    }
    Ok(value)
}

macro_rules! impl_fast_prim_common {
    ($ty:ty, $variant:ident) => {
        fn from_scalar(scalar: Scalar) -> PyResult<Self> {
            match scalar {
                Scalar::$variant(value) => Ok(value),
                _ => Err(PyValueError::new_err("internal scalar type mismatch")),
            }
        }

        fn from_buffer(
            py: Python<'_>,
            _name: &str,
            values: &Bound<'_, PyAny>,
            row_count: usize,
        ) -> PyResult<Option<Vec<Self>>> {
            buffer_values::<$ty>(py, values, row_count)
        }

        fn into_column(values: Vec<Self>, validity: Option<Bitmap>) -> Column {
            Column::$variant(match validity {
                Some(validity) => PrimitiveColumn::new_nullable(values, validity),
                None => PrimitiveColumn::new(values),
            })
        }
    };
}

macro_rules! impl_fast_narrow_int {
    ($ty:ty, $variant:ident) => {
        impl FastValue for $ty {
            const DEFAULT: Self = 0;

            #[inline]
            unsafe fn from_exact(
                ptr: *mut ffi::PyObject,
                _ch_type: &ChType,
                _fast_limit: i64,
            ) -> Result<Self, ()> {
                <$ty>::try_from(exact_long_as_i64(ptr)?).map_err(|_| ())
            }

            impl_fast_prim_common!($ty, $variant);
        }
    };
}

impl_fast_narrow_int!(i8, Int8);
impl_fast_narrow_int!(i16, Int16);
impl_fast_narrow_int!(i32, Int32);
impl_fast_narrow_int!(u8, UInt8);
impl_fast_narrow_int!(u16, UInt16);
impl_fast_narrow_int!(u32, UInt32);

impl FastValue for i64 {
    const DEFAULT: Self = 0;

    #[inline]
    unsafe fn from_exact(
        ptr: *mut ffi::PyObject,
        _ch_type: &ChType,
        _fast_limit: i64,
    ) -> Result<Self, ()> {
        exact_long_as_i64(ptr)
    }

    impl_fast_prim_common!(i64, Int64);
}

impl FastValue for u64 {
    const DEFAULT: Self = 0;

    #[inline]
    unsafe fn from_exact(
        ptr: *mut ffi::PyObject,
        _ch_type: &ChType,
        _fast_limit: i64,
    ) -> Result<Self, ()> {
        if ffi::PyLong_CheckExact(ptr) == 0 {
            return Err(());
        }
        let value = ffi::PyLong_AsUnsignedLongLong(ptr);
        if value == u64::MAX && !ffi::PyErr_Occurred().is_null() {
            ffi::PyErr_Clear();
            return Err(());
        }
        Ok(value)
    }

    impl_fast_prim_common!(u64, UInt64);
}

impl FastValue for f64 {
    const DEFAULT: Self = 0.0;

    #[inline]
    unsafe fn from_exact(
        ptr: *mut ffi::PyObject,
        _ch_type: &ChType,
        _fast_limit: i64,
    ) -> Result<Self, ()> {
        if ffi::PyFloat_CheckExact(ptr) != 0 {
            return Ok(ffi::PyFloat_AS_DOUBLE(ptr));
        }
        // Exact int: same result as extract::<f64>'s PyFloat_AsDouble, which
        // reaches PyLong_AsDouble through int.__float__.
        if ffi::PyLong_CheckExact(ptr) != 0 {
            let value = ffi::PyLong_AsDouble(ptr);
            if value == -1.0 && !ffi::PyErr_Occurred().is_null() {
                ffi::PyErr_Clear();
                return Err(());
            }
            return Ok(value);
        }
        Err(())
    }

    impl_fast_prim_common!(f64, Float64);
}

impl FastValue for f32 {
    const DEFAULT: Self = 0.0;

    #[inline]
    unsafe fn from_exact(
        ptr: *mut ffi::PyObject,
        ch_type: &ChType,
        fast_limit: i64,
    ) -> Result<Self, ()> {
        // Matches extract::<f32>: extract as f64, then `as` cast.
        f64::from_exact(ptr, ch_type, fast_limit).map(|value| value as f32)
    }

    impl_fast_prim_common!(f32, Float32);
}

/// BFloat16 wire word; a distinct type so `[u8; 2]` can serve other 2-byte
/// wire types.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct Bf16Word([u8; 2]);

impl FastValue for Bf16Word {
    const DEFAULT: Self = Bf16Word([0; 2]);

    #[inline]
    unsafe fn from_exact(
        ptr: *mut ffi::PyObject,
        ch_type: &ChType,
        fast_limit: i64,
    ) -> Result<Self, ()> {
        f64::from_exact(ptr, ch_type, fast_limit)
            .and_then(checked_f64_to_bfloat16)
            .map(Self)
    }

    fn from_scalar(scalar: Scalar) -> PyResult<Self> {
        match scalar {
            Scalar::BFloat16(value) => Ok(Self(value)),
            _ => Err(PyValueError::new_err("internal scalar type mismatch")),
        }
    }

    fn from_buffer(
        py: Python<'_>,
        name: &str,
        values: &Bound<'_, PyAny>,
        row_count: usize,
    ) -> PyResult<Option<Vec<Self>>> {
        if let Some(values) =
            map_buffer_values::<f32, _, _>(py, values, row_count, |_row, value| {
                Ok(Self(f32_to_bfloat16(value)))
            })?
        {
            return Ok(Some(values));
        }
        map_buffer_values::<f64, _, _>(py, values, row_count, |row, value| {
            checked_f64_to_bfloat16(value)
                .map(Self)
                .map_err(|_| conversion_error(name, row, "BFloat16"))
        })
    }

    fn into_column(values: Vec<Self>, validity: Option<Bitmap>) -> Column {
        // SAFETY: Bf16Word is #[repr(transparent)] over [u8; 2].
        let values = unsafe { cast_vec::<Self, [u8; 2]>(values) };
        Column::BFloat16(match validity {
            Some(validity) => PrimitiveColumn::new_nullable(values, validity),
            None => PrimitiveColumn::new(values),
        })
    }
}

/// Bool wire byte (0/1); a distinct type so u8 can serve UInt8.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct WireBool(u8);

impl FastValue for WireBool {
    const DEFAULT: Self = WireBool(0);

    #[inline]
    unsafe fn from_exact(
        ptr: *mut ffi::PyObject,
        _ch_type: &ChType,
        _fast_limit: i64,
    ) -> Result<Self, ()> {
        if ptr == ffi::Py_True() {
            Ok(WireBool(1))
        } else if ptr == ffi::Py_False() {
            Ok(WireBool(0))
        } else {
            Err(())
        }
    }

    fn from_scalar(scalar: Scalar) -> PyResult<Self> {
        match scalar {
            Scalar::Bool(value) => Ok(WireBool(u8::from(value))),
            _ => Err(PyValueError::new_err("internal scalar type mismatch")),
        }
    }

    fn into_column(values: Vec<Self>, validity: Option<Bitmap>) -> Column {
        // SAFETY: WireBool is #[repr(transparent)] over u8, so the buffer can
        // be viewed as bytes directly.
        let bytes: &[u8] =
            unsafe { std::slice::from_raw_parts(values.as_ptr().cast(), values.len()) };
        Column::Bool(match validity {
            Some(validity) => BoolColumn::from_wire_bytes_nullable(bytes, validity),
            None => BoolColumn::from_wire_bytes(bytes),
        })
    }
}

/// Copy a one-dimensional buffer whose element type matches `T` exactly
/// (itemsize, signedness, and alignment are validated by `PyBuffer::get`).
/// Non-buffer containers and mismatched dtypes return `Ok(None)`.
///
/// The format string is revalidated here because `PyBuffer::get` in pyo3
/// 0.23 accepts b'>' as a matching byte order on little-endian targets and
/// maps format b'c' (char) to a 1-byte unsigned integer; both would change
/// the encoded values versus the generic per-item path. Any such buffer
/// falls through to the generic path instead.
fn matching_buffer<T: Element>(values: &Bound<'_, PyAny>, row_count: usize) -> Option<PyBuffer<T>> {
    matching_native_buffer(values, &[row_count])
}

pub(super) fn buffer_values<T: Element>(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Option<Vec<T>>> {
    let Some(buffer) = matching_buffer::<T>(values, row_count) else {
        return Ok(None);
    };
    buffer.to_vec(py).map(Some)
}

/// Map a matching one-dimensional Python buffer through a per-element
/// conversion. Contiguous buffers are read directly and allocate only the
/// destination; strided buffers use PyO3's safe gather before the same
/// Rust conversion.
fn map_buffer_values<T: Element, U, F: Fn(usize, T) -> PyResult<U>>(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    map: F,
) -> PyResult<Option<Vec<U>>> {
    let Some(buffer) = matching_buffer::<T>(values, row_count) else {
        return Ok(None);
    };
    if let Some(values) = buffer.as_slice(py) {
        return values
            .iter()
            .enumerate()
            .map(|(row, value)| map(row, value.get()))
            .collect::<PyResult<Vec<_>>>()
            .map(Some);
    }
    buffer
        .to_vec(py)?
        .into_iter()
        .enumerate()
        .map(|(row, value)| map(row, value))
        .collect::<PyResult<Vec<_>>>()
        .map(Some)
}

/// Borrowed positional access to an exact list or tuple.
pub(super) trait FastSeq {
    /// Whether Python code run from a conversion fallback can resize the
    /// container, invalidating later `get` calls.
    const MUTABLE: bool;

    /// # Safety
    ///
    /// Requires the GIL and `index < size()`. The returned pointer is
    /// borrowed and must be consumed before any Python code runs.
    unsafe fn get(&self, index: usize) -> *mut ffi::PyObject;

    fn size(&self) -> usize;
}

pub(super) struct ListSeq<'a, 'py>(pub(super) &'a Bound<'py, PyList>);

impl FastSeq for ListSeq<'_, '_> {
    const MUTABLE: bool = true;

    #[inline]
    unsafe fn get(&self, index: usize) -> *mut ffi::PyObject {
        ffi::PyList_GET_ITEM(self.0.as_ptr(), index as ffi::Py_ssize_t)
    }

    fn size(&self) -> usize {
        self.0.len()
    }
}

pub(super) struct TupleSeq<'a, 'py>(pub(super) &'a Bound<'py, PyTuple>);

impl FastSeq for TupleSeq<'_, '_> {
    const MUTABLE: bool = false;

    #[inline]
    unsafe fn get(&self, index: usize) -> *mut ffi::PyObject {
        ffi::PyTuple_GET_ITEM(self.0.as_ptr(), index as ffi::Py_ssize_t)
    }

    fn size(&self) -> usize {
        self.0.len()
    }
}

/// Flattened Array element run; the `FlatRefs` strong references keep every
/// pointer valid for the whole build and the slice can never be resized, so
/// fallbacks that run Python need no revalidation.
pub(super) struct PtrSeq<'a>(pub(super) &'a [*mut ffi::PyObject]);

impl FastSeq for PtrSeq<'_> {
    const MUTABLE: bool = false;

    #[inline]
    unsafe fn get(&self, index: usize) -> *mut ffi::PyObject {
        debug_assert!(index < self.0.len());
        // SAFETY: the trait contract requires index < size().
        *self.0.get_unchecked(index)
    }

    fn size(&self) -> usize {
        self.0.len()
    }
}

/// Convert `row_count` items from an exact list/tuple into a typed vector,
/// plus a validity bitmap when `nullable`. Items that fail the exact-type
/// check fall back to `convert_scalar`. The fallback can run arbitrary Python
/// (`__index__`, `__float__`, ...), so it holds a strong reference to its
/// item and the container size is revalidated afterwards, before the next
/// borrowed read could go out of bounds on a shrunk list.
fn seq_values<T: FastValue, S: FastSeq>(
    py: Python<'_>,
    seq: &S,
    ch_type: &ChType,
    name: &str,
    row_count: usize,
    nullable: bool,
) -> PyResult<(Vec<T>, Option<Bitmap>)> {
    let mut values = Vec::with_capacity(row_count);
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));
    let mut time_probe = TimeScalarProbe::new(ch_type);
    let fast_limit = match ch_type {
        ChType::Time => MAX_TIME_SECONDS,
        ChType::Time64 { precision } => max_time64_ticks(*precision),
        _ => 0,
    };
    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every fallback revalidates; the borrowed pointer is consumed before
        // any Python code can run.
        let ptr = unsafe { seq.get(row) };
        if ptr == unsafe { ffi::Py_None() } {
            let Some(null_map) = &mut null_map else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but {ch_type} is not Nullable"
                )));
            };
            null_map.push(1);
            values.push(T::DEFAULT);
            continue;
        }
        if let Some(null_map) = &mut null_map {
            null_map.push(0);
        }
        // SAFETY: GIL held; ptr is a valid borrowed item pointer.
        match unsafe { T::from_exact(ptr, ch_type, fast_limit) } {
            Ok(value) => values.push(value),
            Err(()) => {
                // SAFETY: ptr is valid here; taking a strong reference keeps
                // the item alive across any Python code the fallback runs.
                let obj = unsafe { Bound::from_borrowed_ptr(py, ptr) };
                if let Some(probe) = time_probe.as_mut() {
                    if let Some(hit) = probe.probe(&obj, name, row)? {
                        match hit {
                            TimeProbe::Nat => {
                                let Some(null_map) = &mut null_map else {
                                    return Err(PyValueError::new_err(format!(
                                        "column {name:?} row {row} is NaT but {ch_type} is not Nullable"
                                    )));
                                };
                                if let Some(entry) = null_map.last_mut() {
                                    *entry = 1;
                                }
                                values.push(T::DEFAULT);
                            }
                            TimeProbe::Ticks(ticks) => {
                                let scalar = time_ticks_scalar(ch_type, ticks, name, row)?;
                                values.push(T::from_scalar(scalar)?);
                            }
                        }
                        check_not_resized(seq, name, row_count)?;
                        continue;
                    }
                }
                let scalar = convert_scalar(py, ch_type, &obj, name, row)?;
                values.push(T::from_scalar(scalar)?);
                check_not_resized(seq, name, row_count)?;
            }
        }
    }
    Ok((
        values,
        null_map.map(|nulls| Bitmap::from_ch_null_map(&nulls)),
    ))
}

/// Wide-integer equivalent of `seq_values`: allocate the final byte buffer
/// once, then convert each borrowed list/tuple/container item directly into
/// its row slice. Exact ints within i64 convert in place without running
/// Python; any other conversion may execute Python (`__index__`, or `int()`
/// for a string), so the current item is held strongly and mutable sequence
/// length is revalidated before the next borrowed access.
fn wide_column_from_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    seq: &S,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let (width, signed, type_name) = wide_int_layout(ch_type)
        .ok_or_else(|| PyValueError::new_err("internal wide integer type mismatch"))?;
    let mut data = wide_data_buffer(name, width, row_count)?;
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));
    for row in 0..row_count {
        // SAFETY: row < row_count, the caller-validated size. A conversion
        // that executes Python is followed by the resize check below.
        let ptr = unsafe { seq.get(row) };
        if ptr == unsafe { ffi::Py_None() } {
            let Some(null_map) = &mut null_map else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but {ch_type} is not Nullable"
                )));
            };
            null_map.push(1);
            continue;
        }
        if let Some(null_map) = &mut null_map {
            null_map.push(0);
        }
        let start = row * width;
        let slice = &mut data[start..start + width];
        // SAFETY: the borrowed ptr stays valid without a strong ref only
        // because the GIL is held across this whole loop (the module keeps
        // pyo3's default gil_used = true) and the fast try never executes
        // Python, so nothing can mutate the container or drop the item under
        // it. A future free-threading (gil_used = false) opt-in invalidates
        // this and requires a strong ref before the fast try.
        let outcome = unsafe { wide_int_fast_into(ptr, slice, signed, name, row, type_name)? };
        if outcome == WideFast::Done {
            continue;
        }
        // SAFETY: taking a strong reference keeps the current item alive if
        // its conversion mutates the source list.
        let value = unsafe { Bound::from_borrowed_ptr(py, ptr) };
        wide_int_slow_into(
            py,
            &value,
            outcome == WideFast::WideInt,
            slice,
            signed,
            name,
            row,
            type_name,
        )?;
        check_not_resized(seq, name, row_count)?;
    }
    let validity = null_map.map(|nulls| Bitmap::from_ch_null_map(&nulls));
    finish_wide_int_column(ch_type, data, validity)
}

/// Fast column build for types with a per-value fast path. Returns `Ok(None)`
/// when the type or container has no fast path; the caller falls through to
/// the generic scalar loop.
fn try_fast_column(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Option<Column>> {
    if let Ok(list) = values.downcast_exact::<PyList>() {
        return try_fast_column_seq(py, name, ch_type, &ListSeq(list), row_count, nullable);
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return try_fast_column_seq(py, name, ch_type, &TupleSeq(tuple), row_count, nullable);
    }
    if let Some(column) =
        try_numpy_timedelta_column(py, name, ch_type, values, row_count, nullable)?
    {
        return Ok(Some(column));
    }
    try_buffer_column(py, name, ch_type, values, row_count, nullable)
}

/// Per-type dispatch over a borrowed-pointer run; runs once per column.
pub(super) fn try_fast_column_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    seq: &S,
    row_count: usize,
    nullable: bool,
) -> PyResult<Option<Column>> {
    fn prim<T: FastValue, S: FastSeq>(
        py: Python<'_>,
        name: &str,
        ch_type: &ChType,
        seq: &S,
        row_count: usize,
        nullable: bool,
    ) -> PyResult<Option<Column>> {
        let (values, validity) = seq_values::<T, S>(py, seq, ch_type, name, row_count, nullable)?;
        Ok(Some(T::into_column(values, validity)))
    }

    match ch_type {
        ChType::Bool => prim::<WireBool, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Int8 => prim::<i8, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Int16 => prim::<i16, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Int32 => prim::<i32, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Int64 => prim::<i64, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::UInt8 => prim::<u8, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::UInt16 => prim::<u16, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::UInt32 => prim::<u32, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::UInt64 => prim::<u64, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Int128 | ChType::UInt128 | ChType::Int256 | ChType::UInt256 => {
            wide_column_from_seq(py, name, ch_type, seq, row_count, nullable).map(Some)
        }
        ChType::Float32 => prim::<f32, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Float64 => prim::<f64, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::BFloat16 => prim::<Bf16Word, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Date => prim::<DateVal, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Date32 => prim::<Date32Val, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::DateTime { .. } => {
            prim::<DateTimeVal, S>(py, name, ch_type, seq, row_count, nullable)
        }
        ChType::DateTime64 { .. } => {
            prim::<DateTime64Val, S>(py, name, ch_type, seq, row_count, nullable)
        }
        ChType::Time => prim::<TimeVal, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Time64 { .. } => prim::<Time64Val, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Interval(_) => prim::<IntervalVal, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Uuid => uuid_seq(py, seq, ch_type, name, row_count, nullable).map(Some),
        ChType::Ipv4 => ipv4_seq(py, seq, ch_type, name, row_count, nullable).map(Some),
        ChType::Enum8 { variants } => {
            enum_seq(py, seq, ch_type, variants, name, row_count, nullable).map(Some)
        }
        ChType::Enum16 { variants } => {
            enum_seq(py, seq, ch_type, variants, name, row_count, nullable).map(Some)
        }
        _ => Ok(None),
    }
}

/// Copy from a buffer-protocol container whose element type matches exactly.
fn try_buffer_column(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Option<Column>> {
    fn buf<T: FastValue>(
        py: Python<'_>,
        name: &str,
        values: &Bound<'_, PyAny>,
        row_count: usize,
        nullable: bool,
    ) -> PyResult<Option<Column>> {
        Ok(T::from_buffer(py, name, values, row_count)?.map(|vals| {
            // A buffer holds no Python objects, so a nullable column is all-valid.
            let validity = nullable.then(|| Bitmap::all_valid(row_count));
            T::into_column(vals, validity)
        }))
    }

    match ch_type {
        ChType::Int8 => buf::<i8>(py, name, values, row_count, nullable),
        ChType::Int16 => buf::<i16>(py, name, values, row_count, nullable),
        ChType::Int32 => buf::<i32>(py, name, values, row_count, nullable),
        ChType::Int64 => buf::<i64>(py, name, values, row_count, nullable),
        ChType::UInt8 => buf::<u8>(py, name, values, row_count, nullable),
        ChType::UInt16 => buf::<u16>(py, name, values, row_count, nullable),
        ChType::UInt32 => buf::<u32>(py, name, values, row_count, nullable),
        ChType::UInt64 => buf::<u64>(py, name, values, row_count, nullable),
        ChType::Float32 => buf::<f32>(py, name, values, row_count, nullable),
        ChType::Float64 => buf::<f64>(py, name, values, row_count, nullable),
        ChType::BFloat16 => buf::<Bf16Word>(py, name, values, row_count, nullable),
        ChType::Interval(_) => buf::<IntervalVal>(py, name, values, row_count, nullable),
        _ => Ok(None),
    }
}

/// Reinterprets a `Vec` of a `#[repr(transparent)]` wrapper as its inner type,
/// or the reverse, without copying.
///
/// # Safety
/// Every `T` bit pattern must be a valid `U`.
pub(super) unsafe fn cast_vec<T, U>(values: Vec<T>) -> Vec<U> {
    const {
        assert!(std::mem::size_of::<T>() == std::mem::size_of::<U>());
        assert!(std::mem::align_of::<T>() == std::mem::align_of::<U>());
    }
    let mut values = std::mem::ManuallyDrop::new(values);
    Vec::from_raw_parts(
        values.as_mut_ptr().cast::<U>(),
        values.len(),
        values.capacity(),
    )
}

/// Multiplicative hasher for pointer-identity keys; object addresses are not
/// attacker-controlled hash-DoS inputs, so a fast mix beats SipHash here.
#[derive(Default)]
pub(super) struct PtrHasher(u64);

impl std::hash::Hasher for PtrHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        // The map's usize keys normally arrive via write_usize; fold byte
        // input the same way so a std Hash impl change cannot panic.
        for &byte in bytes {
            self.0 = (self.0 ^ u64::from(byte)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }

    fn write_usize(&mut self, value: usize) {
        // Fibonacci hashing: object addresses have aligned low zero bits, so
        // mix before the map takes the low bits of the hash.
        self.0 = (value as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

/// Cap on pointer-identity entries so a column of millions of distinct str
/// objects with few distinct contents cannot grow a cache without bound.
pub(super) const PTR_CACHE_CAP: usize = 1 << 16;

/// Error if a mutable container changed size after a conversion that may have
/// run Python code, before the next borrowed read could go out of bounds.
#[inline]
pub(super) fn check_not_resized<S: FastSeq>(seq: &S, name: &str, row_count: usize) -> PyResult<()> {
    if S::MUTABLE && seq.size() != row_count {
        return Err(PyValueError::new_err(format!(
            "column {name:?} was resized during encoding"
        )));
    }
    Ok(())
}
