use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};

use pyo3::buffer::{Element, PyBuffer};
use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyByteArray, PyBytes, PyList, PyString, PyStringMethods, PyTuple};

use ch_core_rs::batch::ColBatch as RustColBatch;
use ch_core_rs::bitmap::Bitmap;
use ch_core_rs::column::{
    BoolColumn, Column, DecimalColumn, DictionaryColumn, FixedBinaryColumn, PrimitiveColumn,
    Utf8Column,
};
use ch_core_rs::native::encode::{encode_block, EncodeError, EncodeOptions};
use ch_core_rs::schema::{ChType, Field, Schema};

use crate::decoder::buffer_to_vec;

const EPOCH_DATE_ORDINAL: i64 = 719_163;
const IPV4_V6_PREFIX: [u8; 12] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff];

#[pyfunction]
#[pyo3(signature = (column_names, column_type_names, column_data, row_count, prefix=None))]
pub(crate) fn encode_native_block(
    py: Python<'_>,
    column_names: Vec<String>,
    column_type_names: Vec<String>,
    column_data: &Bound<'_, PyAny>,
    row_count: usize,
    prefix: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<u8>> {
    let prefix = match prefix {
        Some(obj) if !obj.is_none() => buffer_to_vec(obj).map_err(|_| {
            PyValueError::new_err("prefix must be a bytes-like object when supplied")
        })?,
        _ => Vec::new(),
    };

    if column_names.len() != column_type_names.len() {
        return Err(PyValueError::new_err(format!(
            "column_names has {} entries but column_type_names has {}",
            column_names.len(),
            column_type_names.len()
        )));
    }

    let data_seq = Indexable::new(column_data, "column_data")?;
    if data_seq.len != column_names.len() {
        return Err(PyValueError::new_err(format!(
            "column_data has {} columns but column_names has {}",
            data_seq.len,
            column_names.len()
        )));
    }

    let mut fields = Vec::with_capacity(column_names.len());
    let mut columns = Vec::with_capacity(column_names.len());
    for (index, (name, type_name)) in column_names.iter().zip(&column_type_names).enumerate() {
        let ch_type = parse_ch_type(type_name).ok_or_else(|| {
            PyNotImplementedError::new_err(format!(
                "unsupported ClickHouse type {type_name:?} for column {name:?}"
            ))
        })?;
        let values = data_seq.get_item(index, "column_data")?;
        let column = build_column(py, name, &ch_type, &values, row_count)?;
        fields.push(Field {
            name: name.clone(),
            ch_type,
        });
        columns.push(column);
    }

    let batch = RustColBatch::new(Schema::new(fields), columns, row_count);
    let mut encoded = py
        .allow_threads(|| {
            encode_block(
                &batch,
                &EncodeOptions {
                    protocol_revision: 0,
                },
            )
        })
        .map_err(encode_err)?;

    if !prefix.is_empty() {
        let mut out = Vec::with_capacity(prefix.len() + encoded.len());
        out.extend_from_slice(&prefix);
        out.append(&mut encoded);
        encoded = out;
    }
    Ok(encoded)
}

fn encode_err(err: EncodeError) -> PyErr {
    match err {
        EncodeError::UnsupportedType { column, ch_type } => PyNotImplementedError::new_err(
            format!("unsupported ClickHouse type {ch_type} for column {column:?}"),
        ),
        EncodeError::InconsistentBatch { detail } => PyValueError::new_err(detail),
    }
}

struct Indexable<'py> {
    obj: Bound<'py, PyAny>,
    len: usize,
}

impl<'py> Indexable<'py> {
    fn new(obj: &Bound<'py, PyAny>, label: &str) -> PyResult<Self> {
        if is_string_or_bytes_like(obj) {
            return Err(PyValueError::new_err(format!(
                "{label} must be an indexable collection, not str or bytes"
            )));
        }
        let len = obj.len().map_err(|_| {
            PyValueError::new_err(format!("{label} must be an indexable collection"))
        })?;
        Ok(Self {
            obj: obj.clone(),
            len,
        })
    }

    fn get_item(&self, index: usize, label: &str) -> PyResult<Bound<'py, PyAny>> {
        self.obj.get_item(index).map_err(|_| {
            PyValueError::new_err(format!("{label} cannot be indexed at position {index}"))
        })
    }
}

fn positional_source<'py>(obj: &Bound<'py, PyAny>) -> Bound<'py, PyAny> {
    match obj.getattr("iloc") {
        Ok(iloc) if !iloc.is_none() => iloc,
        _ => obj.clone(),
    }
}

fn is_string_or_bytes_like(obj: &Bound<'_, PyAny>) -> bool {
    obj.downcast::<PyString>().is_ok()
        || obj.downcast::<PyBytes>().is_ok()
        || obj.downcast::<PyByteArray>().is_ok()
}

fn build_column(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    match ch_type {
        ChType::Nullable(inner) => build_nullable_column(py, name, inner, values, row_count),
        ChType::LowCardinality(inner) => {
            build_low_cardinality_column(py, name, inner, values, row_count)
        }
        _ => build_plain_column(py, name, ch_type, values, row_count),
    }
}

fn build_plain_column(
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
    let mut scalars = Vec::with_capacity(row_count);
    for row in 0..row_count {
        let value = column_values.get_item(row)?;
        if value.is_none() {
            return Err(PyValueError::new_err(format!(
                "column {name:?} row {row} is None but {ch_type} is not Nullable"
            )));
        }
        scalars.push(convert_scalar(py, ch_type, &value, name, row)?);
    }
    column_from_scalars(ch_type, scalars, None)
}

fn build_nullable_column(
    py: Python<'_>,
    name: &str,
    inner: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    if matches!(inner, ChType::Nullable(_) | ChType::LowCardinality(_)) {
        return Err(PyNotImplementedError::new_err(format!(
            "unsupported Nullable inner type {inner} for column {name:?}"
        )));
    }

    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    if let Some(column) = try_fast_column(py, name, inner, values, row_count, true)? {
        return Ok(column);
    }
    let mut null_map = Vec::with_capacity(row_count);
    let mut scalars = Vec::with_capacity(row_count);
    for row in 0..row_count {
        let value = column_values.get_item(row)?;
        if value.is_none() {
            null_map.push(1);
            scalars.push(default_scalar(inner)?);
        } else {
            null_map.push(0);
            scalars.push(convert_scalar(py, inner, &value, name, row)?);
        }
    }
    column_from_scalars(inner, scalars, Some(Bitmap::from_ch_null_map(&null_map)))
}

fn build_low_cardinality_column(
    py: Python<'_>,
    name: &str,
    inner: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    let (nullable, value_type) = match inner {
        ChType::Nullable(value_type) => (true, value_type.as_ref()),
        other => (false, other),
    };

    if !is_low_cardinality_inner(value_type) {
        return Err(PyNotImplementedError::new_err(format!(
            "unsupported LowCardinality inner type {value_type} for column {name:?}"
        )));
    }

    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;

    let mut indices = Vec::with_capacity(row_count);
    let mut dict_values = Vec::new();
    let mut slots = HashMap::<ScalarKey, i32>::new();
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));

    if nullable && row_count > 0 {
        dict_values.push(default_scalar(value_type)?);
    }

    for row in 0..row_count {
        let value = column_values.get_item(row)?;
        if value.is_none() {
            if !nullable {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but LowCardinality({value_type}) is not nullable"
                )));
            }
            indices.push(0);
            if let Some(nulls) = &mut null_map {
                nulls.push(1);
            }
            continue;
        }

        if let Some(nulls) = &mut null_map {
            nulls.push(0);
        }
        let scalar = convert_scalar(py, value_type, &value, name, row)?;
        let key = scalar.key();
        let slot = if let Some(slot) = slots.get(&key) {
            *slot
        } else {
            let slot = i32::try_from(dict_values.len()).map_err(|_| {
                PyValueError::new_err(format!(
                    "column {name:?} LowCardinality dictionary exceeds i32 index capacity"
                ))
            })?;
            dict_values.push(scalar);
            slots.insert(key, slot);
            slot
        };
        indices.push(slot);
    }

    let dict_column = column_from_scalars(value_type, dict_values, None)?;
    match null_map {
        Some(nulls) => Ok(Column::Dictionary(DictionaryColumn::new_nullable(
            indices,
            dict_column,
            Bitmap::from_ch_null_map(&nulls),
        ))),
        None => Ok(Column::Dictionary(DictionaryColumn::new(
            indices,
            dict_column,
        ))),
    }
}

struct ColumnValues<'py> {
    values: Bound<'py, PyAny>,
    len: usize,
    name: String,
}

impl<'py> ColumnValues<'py> {
    fn new(values: &Bound<'py, PyAny>, name: &str) -> PyResult<Self> {
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

    fn get_item(&self, row: usize) -> PyResult<Bound<'py, PyAny>> {
        self.values.get_item(row).map_err(|_| {
            PyValueError::new_err(format!(
                "column {:?} row {row} could not be read from column container",
                self.name
            ))
        })
    }
}

fn check_row_count(name: &str, values: &ColumnValues<'_>, row_count: usize) -> PyResult<()> {
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
trait FastValue: Copy {
    const DEFAULT: Self;

    /// Convert an exact-type Python object without running Python code.
    /// `Err(())` means "no fast conversion, use the generic fallback" and
    /// guarantees no Python exception is left pending.
    ///
    /// # Safety
    ///
    /// Requires the GIL; `ptr` must be a valid, non-null object pointer.
    unsafe fn from_exact(ptr: *mut ffi::PyObject) -> Result<Self, ()>;

    /// Unwrap the `Scalar` produced by the `convert_scalar` fallback.
    fn from_scalar(scalar: Scalar) -> PyResult<Self>;

    /// Copy a matching buffer-protocol container. `Ok(None)` for types with
    /// no buffer representation or containers that do not match.
    fn from_buffer(
        py: Python<'_>,
        values: &Bound<'_, PyAny>,
        row_count: usize,
    ) -> PyResult<Option<Vec<Self>>> {
        let _ = (py, values, row_count);
        Ok(None)
    }

    fn into_column(values: Vec<Self>, validity: Option<Bitmap>) -> Column;
}

/// Read an exact `int` as i64. On overflow the pending exception is cleared
/// and `Err(())` sends the value through the generic fallback, which produces
/// the standard conversion error.
///
/// # Safety
///
/// Requires the GIL; `ptr` must be a valid, non-null object pointer.
#[inline]
unsafe fn exact_long_as_i64(ptr: *mut ffi::PyObject) -> Result<i64, ()> {
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
            unsafe fn from_exact(ptr: *mut ffi::PyObject) -> Result<Self, ()> {
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
    unsafe fn from_exact(ptr: *mut ffi::PyObject) -> Result<Self, ()> {
        exact_long_as_i64(ptr)
    }

    impl_fast_prim_common!(i64, Int64);
}

impl FastValue for u64 {
    const DEFAULT: Self = 0;

    #[inline]
    unsafe fn from_exact(ptr: *mut ffi::PyObject) -> Result<Self, ()> {
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
    unsafe fn from_exact(ptr: *mut ffi::PyObject) -> Result<Self, ()> {
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
    unsafe fn from_exact(ptr: *mut ffi::PyObject) -> Result<Self, ()> {
        // Matches extract::<f32>: extract as f64, then `as` cast.
        f64::from_exact(ptr).map(|value| value as f32)
    }

    impl_fast_prim_common!(f32, Float32);
}

/// Bool wire byte (0/1); a distinct type so u8 can serve UInt8.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct WireBool(u8);

impl FastValue for WireBool {
    const DEFAULT: Self = WireBool(0);

    #[inline]
    unsafe fn from_exact(ptr: *mut ffi::PyObject) -> Result<Self, ()> {
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
fn buffer_values<T: Element>(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Option<Vec<T>>> {
    let Ok(buffer) = PyBuffer::<T>::get(values) else {
        return Ok(None);
    };
    let (order, code) = match *buffer.format().to_bytes() {
        [code] => (b'@', code),
        [order, code] => (order, code),
        _ => return Ok(None),
    };
    let native_order = match order {
        b'@' => true,
        b'<' | b'=' => cfg!(target_endian = "little"),
        b'>' | b'!' => cfg!(target_endian = "big"),
        _ => false,
    };
    if !native_order || code == b'c' {
        return Ok(None);
    }
    if buffer.dimensions() != 1 || buffer.item_count() != row_count {
        return Ok(None);
    }
    buffer.to_vec(py).map(Some)
}

/// Borrowed positional access to an exact list or tuple.
trait FastSeq {
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

struct ListSeq<'a, 'py>(&'a Bound<'py, PyList>);

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

struct TupleSeq<'a, 'py>(&'a Bound<'py, PyTuple>);

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
        match unsafe { T::from_exact(ptr) } {
            Ok(value) => values.push(value),
            Err(()) => {
                // SAFETY: ptr is valid here; taking a strong reference keeps
                // the item alive across any Python code the fallback runs.
                let obj = unsafe { Bound::from_borrowed_ptr(py, ptr) };
                let scalar = convert_scalar(py, ch_type, &obj, name, row)?;
                values.push(T::from_scalar(scalar)?);
                if S::MUTABLE && seq.size() != row_count {
                    return Err(PyValueError::new_err(format!(
                        "column {name:?} was resized during encoding"
                    )));
                }
            }
        }
    }
    Ok((
        values,
        null_map.map(|nulls| Bitmap::from_ch_null_map(&nulls)),
    ))
}

fn fast_column<T: FastValue>(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Option<Column>> {
    if let Ok(list) = values.downcast_exact::<PyList>() {
        let (vals, validity) =
            seq_values::<T, _>(py, &ListSeq(list), ch_type, name, row_count, nullable)?;
        return Ok(Some(T::into_column(vals, validity)));
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        let (vals, validity) =
            seq_values::<T, _>(py, &TupleSeq(tuple), ch_type, name, row_count, nullable)?;
        return Ok(Some(T::into_column(vals, validity)));
    }
    if let Some(vals) = T::from_buffer(py, values, row_count)? {
        // A buffer holds no Python objects, so a nullable column is all-valid.
        let validity = nullable.then(|| Bitmap::all_valid(row_count));
        return Ok(Some(T::into_column(vals, validity)));
    }
    Ok(None)
}

/// Fast column build for primitive numeric types. Returns `Ok(None)` when the
/// type or container has no fast path; the caller falls through to the
/// generic scalar loop.
fn try_fast_column(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Option<Column>> {
    match ch_type {
        ChType::Bool => fast_column::<WireBool>(py, name, ch_type, values, row_count, nullable),
        ChType::Int8 => fast_column::<i8>(py, name, ch_type, values, row_count, nullable),
        ChType::Int16 => fast_column::<i16>(py, name, ch_type, values, row_count, nullable),
        ChType::Int32 => fast_column::<i32>(py, name, ch_type, values, row_count, nullable),
        ChType::Int64 => fast_column::<i64>(py, name, ch_type, values, row_count, nullable),
        ChType::UInt8 => fast_column::<u8>(py, name, ch_type, values, row_count, nullable),
        ChType::UInt16 => fast_column::<u16>(py, name, ch_type, values, row_count, nullable),
        ChType::UInt32 => fast_column::<u32>(py, name, ch_type, values, row_count, nullable),
        ChType::UInt64 => fast_column::<u64>(py, name, ch_type, values, row_count, nullable),
        ChType::Float32 => fast_column::<f32>(py, name, ch_type, values, row_count, nullable),
        ChType::Float64 => fast_column::<f64>(py, name, ch_type, values, row_count, nullable),
        _ => Ok(None),
    }
}

#[derive(Debug)]
enum Scalar {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Date(u16),
    Date32(i32),
    DateTime(u32),
    DateTime64(i64),
    Bytes(Vec<u8>),
    Ipv4(u32),
    Enum8(i8),
    Enum16(i16),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ScalarKey {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(u32),
    Float64(u64),
    Date(u16),
    Date32(i32),
    DateTime(u32),
    DateTime64(i64),
    Bytes(Vec<u8>),
    Ipv4(u32),
    Enum8(i8),
    Enum16(i16),
}

impl Scalar {
    fn key(&self) -> ScalarKey {
        match self {
            Scalar::Bool(v) => ScalarKey::Bool(*v),
            Scalar::Int8(v) => ScalarKey::Int8(*v),
            Scalar::Int16(v) => ScalarKey::Int16(*v),
            Scalar::Int32(v) => ScalarKey::Int32(*v),
            Scalar::Int64(v) => ScalarKey::Int64(*v),
            Scalar::UInt8(v) => ScalarKey::UInt8(*v),
            Scalar::UInt16(v) => ScalarKey::UInt16(*v),
            Scalar::UInt32(v) => ScalarKey::UInt32(*v),
            Scalar::UInt64(v) => ScalarKey::UInt64(*v),
            Scalar::Float32(v) => ScalarKey::Float32(v.to_bits()),
            Scalar::Float64(v) => ScalarKey::Float64(v.to_bits()),
            Scalar::Date(v) => ScalarKey::Date(*v),
            Scalar::Date32(v) => ScalarKey::Date32(*v),
            Scalar::DateTime(v) => ScalarKey::DateTime(*v),
            Scalar::DateTime64(v) => ScalarKey::DateTime64(*v),
            Scalar::Bytes(v) => ScalarKey::Bytes(v.clone()),
            Scalar::Ipv4(v) => ScalarKey::Ipv4(*v),
            Scalar::Enum8(v) => ScalarKey::Enum8(*v),
            Scalar::Enum16(v) => ScalarKey::Enum16(*v),
        }
    }
}

macro_rules! primitive_column {
    ($scalars:expr, $validity:expr, $scalar_variant:ident, $column_variant:ident, $ty:ty) => {{
        let mut values = Vec::<$ty>::with_capacity($scalars.len());
        for scalar in $scalars {
            match scalar {
                Scalar::$scalar_variant(value) => values.push(value),
                _ => return Err(PyValueError::new_err("internal scalar type mismatch")),
            }
        }
        Ok(Column::$column_variant(match $validity {
            Some(validity) => PrimitiveColumn::new_nullable(values, validity),
            None => PrimitiveColumn::new(values),
        }))
    }};
}

fn column_from_scalars(
    ch_type: &ChType,
    scalars: Vec<Scalar>,
    validity: Option<Bitmap>,
) -> PyResult<Column> {
    match ch_type {
        ChType::Bool => {
            let mut bytes = Vec::with_capacity(scalars.len());
            for scalar in scalars {
                match scalar {
                    Scalar::Bool(value) => bytes.push(u8::from(value)),
                    _ => return Err(PyValueError::new_err("internal scalar type mismatch")),
                }
            }
            Ok(Column::Bool(match validity {
                Some(validity) => BoolColumn::from_wire_bytes_nullable(&bytes, validity),
                None => BoolColumn::from_wire_bytes(&bytes),
            }))
        }
        ChType::Int8 => primitive_column!(scalars, validity, Int8, Int8, i8),
        ChType::Int16 => primitive_column!(scalars, validity, Int16, Int16, i16),
        ChType::Int32 => primitive_column!(scalars, validity, Int32, Int32, i32),
        ChType::Int64 => primitive_column!(scalars, validity, Int64, Int64, i64),
        ChType::UInt8 => primitive_column!(scalars, validity, UInt8, UInt8, u8),
        ChType::UInt16 => primitive_column!(scalars, validity, UInt16, UInt16, u16),
        ChType::UInt32 => primitive_column!(scalars, validity, UInt32, UInt32, u32),
        ChType::UInt64 => primitive_column!(scalars, validity, UInt64, UInt64, u64),
        ChType::Float32 => primitive_column!(scalars, validity, Float32, Float32, f32),
        ChType::Float64 => primitive_column!(scalars, validity, Float64, Float64, f64),
        ChType::Date => primitive_column!(scalars, validity, Date, Date, u16),
        ChType::Date32 => primitive_column!(scalars, validity, Date32, Date32, i32),
        ChType::DateTime { .. } => {
            primitive_column!(scalars, validity, DateTime, DateTime, u32)
        }
        ChType::DateTime64 { .. } => {
            primitive_column!(scalars, validity, DateTime64, DateTime64, i64)
        }
        ChType::String => build_utf8_column(scalars, validity),
        ChType::FixedString(width) => build_fixed_binary_column(scalars, *width, validity),
        ChType::Uuid => build_uuid_column(scalars, validity),
        ChType::Ipv4 => primitive_column!(scalars, validity, Ipv4, Ipv4, u32),
        ChType::Ipv6 => build_ipv6_column(scalars, validity),
        ChType::Enum8 { .. } => primitive_column!(scalars, validity, Enum8, Enum8, i8),
        ChType::Enum16 { .. } => primitive_column!(scalars, validity, Enum16, Enum16, i16),
        ChType::Decimal {
            precision, scale, ..
        } => {
            let width = decimal_width(*precision)?;
            let mut data = Vec::with_capacity(width * scalars.len());
            for scalar in scalars {
                match scalar {
                    Scalar::Bytes(value) if value.len() == width => data.extend_from_slice(&value),
                    _ => return Err(PyValueError::new_err("internal scalar type mismatch")),
                }
            }
            Ok(Column::Decimal(match validity {
                Some(validity) => {
                    DecimalColumn::new_nullable(data, width, *precision, *scale, validity)
                }
                None => DecimalColumn::new(data, width, *precision, *scale),
            }))
        }
        ChType::Nullable(_) | ChType::LowCardinality(_) => Err(PyNotImplementedError::new_err(
            "nested wrapper conversion is not supported",
        )),
    }
}

fn build_utf8_column(scalars: Vec<Scalar>, validity: Option<Bitmap>) -> PyResult<Column> {
    let mut offsets = Vec::with_capacity(scalars.len() + 1);
    let mut data = Vec::new();
    offsets.push(0);
    for scalar in scalars {
        let value = match scalar {
            Scalar::Bytes(value) => value,
            _ => return Err(PyValueError::new_err("internal scalar type mismatch")),
        };
        data.extend_from_slice(&value);
        let offset = i32::try_from(data.len())
            .map_err(|_| PyValueError::new_err("String column data exceeds i32 offset capacity"))?;
        offsets.push(offset);
    }
    Ok(Column::Utf8(match validity {
        Some(validity) => Utf8Column::new_nullable(offsets, data, validity),
        None => Utf8Column::new(offsets, data),
    }))
}

fn build_fixed_binary_column(
    scalars: Vec<Scalar>,
    width: usize,
    validity: Option<Bitmap>,
) -> PyResult<Column> {
    let mut data = Vec::with_capacity(width * scalars.len());
    for scalar in scalars {
        match scalar {
            Scalar::Bytes(value) if value.len() == width => data.extend_from_slice(&value),
            _ => return Err(PyValueError::new_err("internal scalar type mismatch")),
        }
    }
    Ok(Column::FixedBinary(match validity {
        Some(validity) => FixedBinaryColumn::new_nullable(data, width, validity),
        None => FixedBinaryColumn::new(data, width),
    }))
}

fn build_uuid_column(scalars: Vec<Scalar>, validity: Option<Bitmap>) -> PyResult<Column> {
    let mut data = Vec::with_capacity(16 * scalars.len());
    for scalar in scalars {
        match scalar {
            Scalar::Bytes(value) if value.len() == 16 => data.extend_from_slice(&value),
            _ => return Err(PyValueError::new_err("internal scalar type mismatch")),
        }
    }
    Ok(Column::Uuid(match validity {
        Some(validity) => FixedBinaryColumn::new_nullable(data, 16, validity),
        None => FixedBinaryColumn::new(data, 16),
    }))
}

fn build_ipv6_column(scalars: Vec<Scalar>, validity: Option<Bitmap>) -> PyResult<Column> {
    let mut data = Vec::with_capacity(16 * scalars.len());
    for scalar in scalars {
        match scalar {
            Scalar::Bytes(value) if value.len() == 16 => data.extend_from_slice(&value),
            _ => return Err(PyValueError::new_err("internal scalar type mismatch")),
        }
    }
    Ok(Column::Ipv6(match validity {
        Some(validity) => FixedBinaryColumn::new_nullable(data, 16, validity),
        None => FixedBinaryColumn::new(data, 16),
    }))
}

fn convert_scalar(
    py: Python<'_>,
    ch_type: &ChType,
    value: &Bound<'_, PyAny>,
    column: &str,
    row: usize,
) -> PyResult<Scalar> {
    match ch_type {
        ChType::Bool => value
            .extract::<bool>()
            .map(Scalar::Bool)
            .map_err(|_| conversion_error(column, row, "Bool")),
        ChType::Int8 => value
            .extract::<i8>()
            .map(Scalar::Int8)
            .map_err(|_| conversion_error(column, row, "Int8")),
        ChType::Int16 => value
            .extract::<i16>()
            .map(Scalar::Int16)
            .map_err(|_| conversion_error(column, row, "Int16")),
        ChType::Int32 => value
            .extract::<i32>()
            .map(Scalar::Int32)
            .map_err(|_| conversion_error(column, row, "Int32")),
        ChType::Int64 => value
            .extract::<i64>()
            .map(Scalar::Int64)
            .map_err(|_| conversion_error(column, row, "Int64")),
        ChType::UInt8 => value
            .extract::<u8>()
            .map(Scalar::UInt8)
            .map_err(|_| conversion_error(column, row, "UInt8")),
        ChType::UInt16 => value
            .extract::<u16>()
            .map(Scalar::UInt16)
            .map_err(|_| conversion_error(column, row, "UInt16")),
        ChType::UInt32 => value
            .extract::<u32>()
            .map(Scalar::UInt32)
            .map_err(|_| conversion_error(column, row, "UInt32")),
        ChType::UInt64 => value
            .extract::<u64>()
            .map(Scalar::UInt64)
            .map_err(|_| conversion_error(column, row, "UInt64")),
        ChType::Float32 => value
            .extract::<f32>()
            .map(Scalar::Float32)
            .map_err(|_| conversion_error(column, row, "Float32")),
        ChType::Float64 => value
            .extract::<f64>()
            .map(Scalar::Float64)
            .map_err(|_| conversion_error(column, row, "Float64")),
        ChType::String => Ok(Scalar::Bytes(bytes_value(value, column, row, "String")?)),
        ChType::FixedString(width) => Ok(Scalar::Bytes(fixed_string_value(
            value, *width, column, row,
        )?)),
        ChType::Date => Ok(Scalar::Date(date_days(value, column, row).and_then(
            |days| {
                u16::try_from(days).map_err(|_| {
                    PyValueError::new_err(format!(
                        "column {column:?} row {row} Date value {days} is outside UInt16 range"
                    ))
                })
            },
        )?)),
        ChType::Date32 => Ok(Scalar::Date32(date_days(value, column, row).and_then(
            |days| {
                i32::try_from(days).map_err(|_| {
                    PyValueError::new_err(format!(
                        "column {column:?} row {row} Date32 value {days} is outside Int32 range"
                    ))
                })
            },
        )?)),
        ChType::DateTime { .. } => {
            let secs = datetime_seconds(value, column, row)?;
            Ok(Scalar::DateTime(u32::try_from(secs).map_err(|_| {
                PyValueError::new_err(format!(
                    "column {column:?} row {row} DateTime value {secs} is outside UInt32 range"
                ))
            })?))
        }
        ChType::DateTime64 { precision, .. } => Ok(Scalar::DateTime64(datetime64_ticks(
            py, value, *precision, column, row,
        )?)),
        ChType::Uuid => Ok(Scalar::Bytes(uuid_bytes(value, column, row)?)),
        ChType::Ipv4 => Ok(Scalar::Ipv4(ipv4_value(value, column, row)?)),
        ChType::Ipv6 => Ok(Scalar::Bytes(ipv6_bytes(value, column, row)?)),
        ChType::Enum8 { variants } => Ok(Scalar::Enum8(enum8_value(value, variants, column, row)?)),
        ChType::Enum16 { variants } => {
            Ok(Scalar::Enum16(enum16_value(value, variants, column, row)?))
        }
        ChType::Decimal {
            precision, scale, ..
        } => {
            let width = decimal_width(*precision)?;
            let text = decimal_text(value, column, row)?;
            Ok(Scalar::Bytes(decimal_to_le_bytes(
                &text, width, *precision, *scale, column, row,
            )?))
        }
        ChType::Nullable(_) | ChType::LowCardinality(_) => Err(PyNotImplementedError::new_err(
            "nested wrapper conversion is not supported",
        )),
    }
}

fn default_scalar(ch_type: &ChType) -> PyResult<Scalar> {
    match ch_type {
        ChType::Bool => Ok(Scalar::Bool(false)),
        ChType::Int8 => Ok(Scalar::Int8(0)),
        ChType::Int16 => Ok(Scalar::Int16(0)),
        ChType::Int32 => Ok(Scalar::Int32(0)),
        ChType::Int64 => Ok(Scalar::Int64(0)),
        ChType::UInt8 => Ok(Scalar::UInt8(0)),
        ChType::UInt16 => Ok(Scalar::UInt16(0)),
        ChType::UInt32 => Ok(Scalar::UInt32(0)),
        ChType::UInt64 => Ok(Scalar::UInt64(0)),
        ChType::Float32 => Ok(Scalar::Float32(0.0)),
        ChType::Float64 => Ok(Scalar::Float64(0.0)),
        ChType::String => Ok(Scalar::Bytes(Vec::new())),
        ChType::FixedString(width) => Ok(Scalar::Bytes(vec![0; *width])),
        ChType::Date => Ok(Scalar::Date(0)),
        ChType::Date32 => Ok(Scalar::Date32(0)),
        ChType::DateTime { .. } => Ok(Scalar::DateTime(0)),
        ChType::DateTime64 { .. } => Ok(Scalar::DateTime64(0)),
        ChType::Uuid => Ok(Scalar::Bytes(vec![0; 16])),
        ChType::Ipv4 => Ok(Scalar::Ipv4(0)),
        ChType::Ipv6 => Ok(Scalar::Bytes(vec![0; 16])),
        ChType::Enum8 { .. } => Ok(Scalar::Enum8(0)),
        ChType::Enum16 { .. } => Ok(Scalar::Enum16(0)),
        ChType::Decimal {
            precision,
            scale: _,
            ..
        } => Ok(Scalar::Bytes(vec![0; decimal_width(*precision)?])),
        ChType::Nullable(_) | ChType::LowCardinality(_) => Err(PyNotImplementedError::new_err(
            "nested wrapper conversion is not supported",
        )),
    }
}

fn conversion_error(column: &str, row: usize, type_name: &str) -> PyErr {
    PyValueError::new_err(format!(
        "column {column:?} row {row} cannot be converted to {type_name}"
    ))
}

fn bytes_value(
    value: &Bound<'_, PyAny>,
    column: &str,
    row: usize,
    type_name: &str,
) -> PyResult<Vec<u8>> {
    if let Ok(s) = value.downcast::<PyString>() {
        return Ok(s.to_str()?.as_bytes().to_vec());
    }
    buffer_to_vec(value).map_err(|_| {
        PyValueError::new_err(format!(
            "column {column:?} row {row} cannot be converted to {type_name} bytes"
        ))
    })
}

fn fixed_string_value(
    value: &Bound<'_, PyAny>,
    width: usize,
    column: &str,
    row: usize,
) -> PyResult<Vec<u8>> {
    if let Ok(s) = value.downcast::<PyString>() {
        let mut bytes = s.to_str()?.as_bytes().to_vec();
        if bytes.len() > width {
            return Err(PyValueError::new_err(format!(
                "column {column:?} row {row} UTF-8 encoded FixedString value is {} bytes, exceeding width {width}",
                bytes.len()
            )));
        }
        bytes.resize(width, 0);
        return Ok(bytes);
    }

    let bytes = buffer_to_vec(value).map_err(|_| {
        PyValueError::new_err(format!(
            "column {column:?} row {row} cannot be converted to FixedString bytes"
        ))
    })?;
    if bytes.len() != width {
        return Err(PyValueError::new_err(format!(
            "column {column:?} row {row} FixedString binary value is {} bytes, expected {width}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn date_days(value: &Bound<'_, PyAny>, column: &str, row: usize) -> PyResult<i64> {
    if let Ok(days) = value.extract::<i64>() {
        return Ok(days);
    }
    let ordinal = value
        .call_method0("toordinal")
        .and_then(|o| o.extract::<i64>())
        .map_err(|_| conversion_error(column, row, "Date"))?;
    Ok(ordinal - EPOCH_DATE_ORDINAL)
}

fn datetime_seconds(value: &Bound<'_, PyAny>, column: &str, row: usize) -> PyResult<i64> {
    if let Ok(secs) = value.extract::<i64>() {
        return Ok(secs);
    }
    let ts = value
        .call_method0("timestamp")
        .and_then(|o| o.extract::<f64>())
        .map_err(|_| conversion_error(column, row, "DateTime"))?;
    finite_trunc_to_i64(ts, column, row, "DateTime")
}

fn datetime64_ticks(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    precision: u8,
    column: &str,
    row: usize,
) -> PyResult<i64> {
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

    let secs = value
        .call_method0("timestamp")
        .and_then(|o| o.extract::<f64>())
        .map_err(|_| conversion_error(column, row, "DateTime64"))?;
    let secs = finite_floor_to_i64(secs, column, row, "DateTime64")?;
    let micros = value
        .getattr("microsecond")
        .and_then(|o| o.extract::<i64>())
        .map_err(|_| conversion_error(column, row, "DateTime64"))?;
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

fn uuid_bytes(value: &Bound<'_, PyAny>, column: &str, row: usize) -> PyResult<Vec<u8>> {
    if let Ok(s) = value.downcast::<PyString>() {
        return uuid_int_to_wire(parse_uuid_hex(s.to_str()?, column, row)?);
    }
    if let Ok(x) = value.extract::<u128>() {
        return uuid_int_to_wire(x);
    }
    if let Ok(int_attr) = value.getattr("int") {
        if let Ok(x) = int_attr.extract::<u128>() {
            return uuid_int_to_wire(x);
        }
    }
    let bytes = buffer_to_vec(value).map_err(|_| conversion_error(column, row, "UUID"))?;
    if bytes.len() != 16 {
        return Err(PyValueError::new_err(format!(
            "column {column:?} row {row} UUID bytes length is {}, expected 16",
            bytes.len()
        )));
    }
    let mut out = Vec::with_capacity(16);
    out.extend(bytes[..8].iter().rev());
    out.extend(bytes[8..].iter().rev());
    Ok(out)
}

fn parse_uuid_hex(value: &str, column: &str, row: usize) -> PyResult<u128> {
    let hex: String = value.chars().filter(|&ch| ch != '-').collect();
    if hex.len() != 32 {
        return Err(PyValueError::new_err(format!(
            "column {column:?} row {row} UUID string must contain 32 hex digits"
        )));
    }
    u128::from_str_radix(&hex, 16).map_err(|_| {
        PyValueError::new_err(format!(
            "column {column:?} row {row} UUID string contains non-hex characters"
        ))
    })
}

fn uuid_int_to_wire(value: u128) -> PyResult<Vec<u8>> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&((value >> 64) as u64).to_le_bytes());
    out.extend_from_slice(&(value as u64).to_le_bytes());
    Ok(out)
}

fn ipv4_value(value: &Bound<'_, PyAny>, column: &str, row: usize) -> PyResult<u32> {
    if let Ok(v) = value.extract::<u32>() {
        return Ok(v);
    }
    if let Ok(s) = value.downcast::<PyString>() {
        return s
            .to_str()?
            .parse::<Ipv4Addr>()
            .map(|addr| u32::from_be_bytes(addr.octets()))
            .map_err(|_| conversion_error(column, row, "IPv4"));
    }
    if let Ok(ip) = value.getattr("_ip").and_then(|o| o.extract::<u32>()) {
        return Ok(ip);
    }
    if let Ok(packed) = value.getattr("packed").and_then(|o| buffer_to_vec(&o)) {
        if packed.len() == 4 {
            return Ok(u32::from_be_bytes([
                packed[0], packed[1], packed[2], packed[3],
            ]));
        }
    }
    Err(conversion_error(column, row, "IPv4"))
}

fn ipv6_bytes(value: &Bound<'_, PyAny>, column: &str, row: usize) -> PyResult<Vec<u8>> {
    if let Ok(s) = value.downcast::<PyString>() {
        return ip_string_to_ipv6(s.to_str()?, column, row);
    }
    if let Ok(v) = value.extract::<u128>() {
        return Ok(v.to_be_bytes().to_vec());
    }
    if let Ok(packed) = value.getattr("packed").and_then(|o| buffer_to_vec(&o)) {
        return packed_to_ipv6(packed, true, column, row);
    }
    let bytes = buffer_to_vec(value).map_err(|_| conversion_error(column, row, "IPv6"))?;
    packed_to_ipv6(bytes, false, column, row)
}

fn ip_string_to_ipv6(value: &str, column: &str, row: usize) -> PyResult<Vec<u8>> {
    match value.parse::<IpAddr>() {
        Ok(IpAddr::V6(addr)) => Ok(addr.octets().to_vec()),
        Ok(IpAddr::V4(addr)) => {
            let mut out = Vec::with_capacity(16);
            out.extend_from_slice(&IPV4_V6_PREFIX);
            out.extend_from_slice(&addr.octets());
            Ok(out)
        }
        Err(_) => Err(conversion_error(column, row, "IPv6")),
    }
}

fn packed_to_ipv6(bytes: Vec<u8>, allow_ipv4: bool, column: &str, row: usize) -> PyResult<Vec<u8>> {
    if bytes.len() == 16 {
        return Ok(bytes);
    }
    if allow_ipv4 && bytes.len() == 4 {
        let mut out = Vec::with_capacity(16);
        out.extend_from_slice(&IPV4_V6_PREFIX);
        out.extend_from_slice(&bytes);
        return Ok(out);
    }
    Err(PyValueError::new_err(format!(
        "column {column:?} row {row} IPv6 bytes length is {}, expected 16",
        bytes.len()
    )))
}

fn enum8_value(
    value: &Bound<'_, PyAny>,
    variants: &[(String, i8)],
    column: &str,
    row: usize,
) -> PyResult<i8> {
    if let Ok(v) = value.extract::<i8>() {
        return Ok(v);
    }
    let name = value
        .downcast::<PyString>()
        .map_err(|_| conversion_error(column, row, "Enum8"))?
        .to_str()?;
    variants
        .iter()
        .find_map(|(variant, code)| (variant == name).then_some(*code))
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "column {column:?} row {row} Enum8 label {name:?} is not defined"
            ))
        })
}

fn enum16_value(
    value: &Bound<'_, PyAny>,
    variants: &[(String, i16)],
    column: &str,
    row: usize,
) -> PyResult<i16> {
    if let Ok(v) = value.extract::<i16>() {
        return Ok(v);
    }
    let name = value
        .downcast::<PyString>()
        .map_err(|_| conversion_error(column, row, "Enum16"))?
        .to_str()?;
    variants
        .iter()
        .find_map(|(variant, code)| (variant == name).then_some(*code))
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "column {column:?} row {row} Enum16 label {name:?} is not defined"
            ))
        })
}

fn decimal_width(precision: u8) -> PyResult<usize> {
    match precision {
        1..=9 => Ok(4),
        10..=18 => Ok(8),
        19..=38 => Ok(16),
        39..=76 => Ok(32),
        _ => Err(PyValueError::new_err(format!(
            "Decimal precision {precision} is outside 1..=76"
        ))),
    }
}

fn decimal_text(value: &Bound<'_, PyAny>, column: &str, row: usize) -> PyResult<String> {
    value
        .str()
        .and_then(|s| s.to_str().map(str::to_owned))
        .map_err(|_| {
            PyValueError::new_err(format!(
                "column {column:?} row {row} Decimal value cannot be stringified"
            ))
        })
}

fn decimal_to_le_bytes(
    text: &str,
    width: usize,
    precision: u8,
    scale: u8,
    column: &str,
    row: usize,
) -> PyResult<Vec<u8>> {
    let (negative, digits, exponent) = parse_decimal_text(text, column, row)?;
    let shift = exponent.checked_add(i32::from(scale)).ok_or_else(|| {
        PyValueError::new_err(format!(
            "column {column:?} row {row} Decimal value {text:?} has an unsupported exponent"
        ))
    })?;
    let first_non_zero = digits.iter().position(|&d| d != 0);
    let mut digits = match first_non_zero {
        Some(pos) => digits[pos..].to_vec(),
        None => Vec::new(),
    };

    if shift >= 0 {
        let precision = usize::from(precision);
        let shift = usize::try_from(shift).map_err(|_| conversion_error(column, row, "Decimal"))?;
        let new_len = digits.len().checked_add(shift).ok_or_else(|| {
            PyValueError::new_err(format!(
                "column {column:?} row {row} Decimal value {text:?} has an unsupported exponent"
            ))
        })?;
        if !digits.is_empty() && new_len > precision {
            return Err(PyValueError::new_err(format!(
                "column {column:?} row {row} Decimal value {text:?} exceeds precision {precision}"
            )));
        }
        if new_len > width * 4 {
            return Err(PyValueError::new_err(format!(
                "column {column:?} row {row} Decimal value {text:?} does not fit in {width} bytes"
            )));
        }
        digits.resize(new_len, 0);
    } else {
        let remove = usize::try_from(shift.unsigned_abs())
            .map_err(|_| conversion_error(column, row, "Decimal"))?;
        if remove >= digits.len() {
            digits.clear();
        } else {
            digits.truncate(digits.len() - remove);
        }
    }
    if digits.len() > usize::from(precision) {
        return Err(PyValueError::new_err(format!(
            "column {column:?} row {row} Decimal value {text:?} exceeds precision {precision}"
        )));
    }

    let mut mag = vec![0u8; width];
    for digit in digits {
        mul_add_decimal_digit(&mut mag, digit, column, row)?;
    }
    if !fits_signed_width(&mag, negative) {
        return Err(PyValueError::new_err(format!(
            "column {column:?} row {row} Decimal value {text:?} does not fit in {width} bytes"
        )));
    }
    if negative && mag.iter().any(|&b| b != 0) {
        twos_complement_in_place(&mut mag);
    }
    Ok(mag)
}

fn parse_decimal_text(text: &str, column: &str, row: usize) -> PyResult<(bool, Vec<u8>, i32)> {
    let text = text.trim();
    if text.is_empty() {
        return Err(conversion_error(column, row, "Decimal"));
    }

    let mut chars = text.chars().peekable();
    let negative = match chars.peek().copied() {
        Some('-') => {
            chars.next();
            true
        }
        Some('+') => {
            chars.next();
            false
        }
        _ => false,
    };

    let mut digits = Vec::new();
    let mut frac_digits = 0i32;
    let mut seen_dot = false;
    while let Some(ch) = chars.peek().copied() {
        match ch {
            '0'..='9' => {
                digits.push(ch as u8 - b'0');
                if seen_dot {
                    frac_digits += 1;
                }
                chars.next();
            }
            '.' if !seen_dot => {
                seen_dot = true;
                chars.next();
            }
            'e' | 'E' => break,
            _ => return Err(conversion_error(column, row, "Decimal")),
        }
    }

    if digits.is_empty() {
        return Err(conversion_error(column, row, "Decimal"));
    }

    let exponent = if matches!(chars.peek(), Some('e' | 'E')) {
        chars.next();
        let mut exp_sign = 1i32;
        match chars.peek().copied() {
            Some('-') => {
                exp_sign = -1;
                chars.next();
            }
            Some('+') => {
                chars.next();
            }
            _ => {}
        }
        let mut exp = 0i32;
        let mut seen_digit = false;
        for ch in chars {
            if !ch.is_ascii_digit() {
                return Err(conversion_error(column, row, "Decimal"));
            }
            seen_digit = true;
            exp = exp
                .checked_mul(10)
                .and_then(|v| v.checked_add((ch as u8 - b'0') as i32))
                .ok_or_else(|| conversion_error(column, row, "Decimal"))?;
        }
        if !seen_digit {
            return Err(conversion_error(column, row, "Decimal"));
        }
        exp_sign * exp
    } else {
        0
    };

    let exponent = exponent
        .checked_sub(frac_digits)
        .ok_or_else(|| conversion_error(column, row, "Decimal"))?;
    Ok((negative, digits, exponent))
}

fn mul_add_decimal_digit(bytes: &mut [u8], digit: u8, column: &str, row: usize) -> PyResult<()> {
    let mut carry = u16::from(digit);
    for byte in bytes {
        let next = u16::from(*byte) * 10 + carry;
        *byte = next as u8;
        carry = next >> 8;
    }
    if carry != 0 {
        return Err(PyValueError::new_err(format!(
            "column {column:?} row {row} Decimal value overflows target width"
        )));
    }
    Ok(())
}

fn fits_signed_width(bytes: &[u8], negative: bool) -> bool {
    if bytes.is_empty() {
        return true;
    }
    let last = bytes.len() - 1;
    if negative {
        bytes[last] < 0x80 || (bytes[last] == 0x80 && bytes[..last].iter().all(|&b| b == 0))
    } else {
        bytes[last] < 0x80
    }
}

fn twos_complement_in_place(bytes: &mut [u8]) {
    for byte in bytes.iter_mut() {
        *byte = !*byte;
    }
    let mut carry = 1u16;
    for byte in bytes {
        let next = u16::from(*byte) + carry;
        *byte = next as u8;
        carry = next >> 8;
        if carry == 0 {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Binding-local parser for the supported canonical ClickHouse type names.
// ---------------------------------------------------------------------------

fn parse_ch_type(type_name: &str) -> Option<ChType> {
    if let Some(inner) = type_name.strip_prefix("Nullable(") {
        if let Some(inner) = inner.strip_suffix(')') {
            let inner_type = parse_ch_type(inner)?;
            if matches!(inner_type, ChType::Nullable(_) | ChType::LowCardinality(_)) {
                return None;
            }
            return Some(ChType::Nullable(Box::new(inner_type)));
        }
    }

    if let Some(inner) = type_name.strip_prefix("LowCardinality(") {
        if let Some(inner) = inner.strip_suffix(')') {
            return parse_ch_type(inner).map(|t| ChType::LowCardinality(Box::new(t)));
        }
    }

    if let Some(n_str) = type_name.strip_prefix("FixedString(") {
        if let Some(n_str) = n_str.strip_suffix(')') {
            if let Ok(n) = n_str.trim().parse::<usize>() {
                if n > 0 {
                    return Some(ChType::FixedString(n));
                }
            }
        }
    }

    if let Some(inner) = type_name.strip_prefix("DateTime64(") {
        if let Some(inner) = inner.strip_suffix(')') {
            let (precision_str, timezone) = match inner.split_once(',') {
                Some((precision, timezone)) => (
                    precision.trim(),
                    Some(strip_quotes(timezone.trim()).to_string()),
                ),
                None => (inner.trim(), None),
            };
            return match precision_str.parse::<u8>() {
                Ok(precision) if precision <= 9 => Some(ChType::DateTime64 {
                    precision,
                    timezone,
                }),
                _ => None,
            };
        }
    }

    if let Some(inner) = type_name.strip_prefix("Enum8(") {
        if let Some(inner) = inner.strip_suffix(')') {
            return parse_enum_variants::<i8>(inner).map(|variants| ChType::Enum8 { variants });
        }
    }
    if let Some(inner) = type_name.strip_prefix("Enum16(") {
        if let Some(inner) = inner.strip_suffix(')') {
            return parse_enum_variants::<i16>(inner).map(|variants| ChType::Enum16 { variants });
        }
    }

    if let Some(inner) = type_name.strip_prefix("Decimal(") {
        if let Some(inner) = inner.strip_suffix(')') {
            let (precision, scale) = inner.split_once(',')?;
            let precision = precision.trim().parse::<u8>().ok()?;
            let scale = scale.trim().parse::<u8>().ok()?;
            if scale > precision {
                return None;
            }
            let bits = match precision {
                1..=9 => 32,
                10..=18 => 64,
                19..=38 => 128,
                39..=76 => 256,
                _ => return None,
            };
            return Some(ChType::Decimal {
                precision,
                scale,
                bits,
            });
        }
    }

    if let Some(inner) = type_name.strip_prefix("DateTime(") {
        if let Some(inner) = inner.strip_suffix(')') {
            return Some(ChType::DateTime {
                timezone: Some(strip_quotes(inner.trim()).to_string()),
            });
        }
    }

    match type_name {
        "Bool" | "Boolean" => Some(ChType::Bool),
        "Int8" => Some(ChType::Int8),
        "Int16" => Some(ChType::Int16),
        "Int32" => Some(ChType::Int32),
        "Int64" => Some(ChType::Int64),
        "UInt8" => Some(ChType::UInt8),
        "UInt16" => Some(ChType::UInt16),
        "UInt32" => Some(ChType::UInt32),
        "UInt64" => Some(ChType::UInt64),
        "Float32" => Some(ChType::Float32),
        "Float64" => Some(ChType::Float64),
        "String" => Some(ChType::String),
        "Date" => Some(ChType::Date),
        "Date32" => Some(ChType::Date32),
        "DateTime" => Some(ChType::DateTime { timezone: None }),
        "UUID" => Some(ChType::Uuid),
        "IPv4" => Some(ChType::Ipv4),
        "IPv6" => Some(ChType::Ipv6),
        _ => None,
    }
}

fn strip_quotes(value: &str) -> &str {
    value
        .strip_prefix('\'')
        .and_then(|v| v.strip_suffix('\''))
        .unwrap_or(value)
}

fn parse_enum_variants<V>(inner: &str) -> Option<Vec<(String, V)>>
where
    V: std::str::FromStr,
{
    let mut parser = EnumParser::new(inner);
    let mut variants = Vec::new();
    parser.skip_ws();
    if parser.is_eof() {
        return Some(variants);
    }

    loop {
        parser.skip_ws();
        let name = parser.parse_quoted()?;
        parser.skip_ws();
        parser.expect('=')?;
        parser.skip_ws();
        let value = parser.parse_value::<V>()?;
        variants.push((name, value));
        parser.skip_ws();
        if parser.is_eof() {
            return Some(variants);
        }
        parser.expect(',')?;
    }
}

struct EnumParser<'a> {
    s: &'a str,
    pos: usize,
}

impl<'a> EnumParser<'a> {
    fn new(s: &'a str) -> Self {
        Self { s, pos: 0 }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.s.len()
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.peek() {
            if !ch.is_whitespace() {
                break;
            }
            self.pos += ch.len_utf8();
        }
    }

    fn peek(&self) -> Option<char> {
        self.s[self.pos..].chars().next()
    }

    fn next(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn expect(&mut self, expected: char) -> Option<()> {
        (self.next()? == expected).then_some(())
    }

    fn parse_quoted(&mut self) -> Option<String> {
        self.expect('\'')?;
        let mut out = String::new();
        loop {
            let ch = self.next()?;
            match ch {
                '\'' => return Some(out),
                '\\' => {
                    let escaped = self.next()?;
                    out.push(match escaped {
                        '\\' => '\\',
                        '\'' => '\'',
                        'b' => '\u{08}',
                        'f' => '\u{0c}',
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '0' => '\0',
                        _ => return None,
                    });
                }
                other => out.push(other),
            }
        }
    }

    fn parse_value<V>(&mut self) -> Option<V>
    where
        V: std::str::FromStr,
    {
        let start = self.pos;
        if matches!(self.peek(), Some('-' | '+')) {
            self.next();
        }
        let mut saw_digit = false;
        while matches!(self.peek(), Some('0'..='9')) {
            saw_digit = true;
            self.next();
        }
        if !saw_digit {
            return None;
        }
        self.s[start..self.pos].parse::<V>().ok()
    }
}

fn is_low_cardinality_inner(ch_type: &ChType) -> bool {
    matches!(
        ch_type,
        ChType::Bool
            | ChType::Int8
            | ChType::Int16
            | ChType::Int32
            | ChType::Int64
            | ChType::UInt8
            | ChType::UInt16
            | ChType::UInt32
            | ChType::UInt64
            | ChType::Float32
            | ChType::Float64
            | ChType::String
            | ChType::FixedString(_)
            | ChType::Date
            | ChType::Date32
            | ChType::DateTime { .. }
            | ChType::Uuid
            | ChType::Ipv4
            | ChType::Ipv6
    )
}
