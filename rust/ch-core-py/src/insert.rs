use std::collections::HashMap;
use std::ffi::c_long;
use std::net::{IpAddr, Ipv4Addr};

use pyo3::buffer::{Element, PyBuffer};
use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{
    PyAnyMethods, PyByteArray, PyBytes, PyDate, PyDateTime, PyDict, PyFrozenSet, PyList, PySet,
    PyString, PyStringMethods, PyTuple,
};

use ch_core_rs::batch::ColBatch as RustColBatch;
use ch_core_rs::bitmap::Bitmap;
use ch_core_rs::column::{
    ArrayColumn, BoolColumn, Column, DecimalColumn, DictionaryColumn, FixedBinaryColumn,
    PrimitiveColumn, Utf8Column,
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
        ChType::Array(inner) => build_array_column(py, name, inner, values, row_count),
        _ => build_plain_column(py, name, ch_type, values, row_count),
    }
}

/// Build an `Array(T)` column: each row is a sequence of elements, flattened
/// into one strong-reference element run with an Arrow LargeList offsets run.
/// The element column is built once over the flat run, so elements hit the
/// same per-type fast paths as a plain column and nested Arrays compose
/// recursively. Arrays are never nullable at the array level, so a None row
/// is an error.
fn build_array_column(
    py: Python<'_>,
    name: &str,
    inner: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;

    if let Ok(list) = values.downcast_exact::<PyList>() {
        return array_column_from_seq(py, name, inner, &ListSeq(list), row_count);
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return array_column_from_seq(py, name, inner, &TupleSeq(tuple), row_count);
    }

    // Generic outer container: safe indexed reads, same per-row flatten.
    let mut offsets = Vec::with_capacity(row_count + 1);
    offsets.push(0i64);
    let mut flat = FlatRefs::default();
    for row in 0..row_count {
        let value = column_values.get_item(row)?;
        flatten_array_row(py, name, inner, &value, row, &mut flat)?;
        offsets.push(flat.end_offset(name)?);
    }
    let element_column = build_element_column(py, name, inner, &flat.ptrs)
        .map_err(|err| remap_element_err(py, name, &offsets, err))?;
    Ok(Column::Array(ArrayColumn::new(offsets, element_column)))
}

/// Array flatten over an exact list or tuple of rows: borrowed row reads, one
/// flat element run. A row that is not an exact list or tuple flattens through
/// `list.extend`, which can run Python, so the container size is revalidated
/// before the next borrowed read.
fn array_column_from_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    inner: &ChType,
    seq: &S,
    row_count: usize,
) -> PyResult<Column> {
    let mut offsets = Vec::with_capacity(row_count + 1);
    offsets.push(0i64);
    let mut flat = FlatRefs::default();
    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every fallback revalidates; the strong reference keeps the row
        // alive across any Python code the fallback runs.
        let value = unsafe { Bound::from_borrowed_ptr(py, seq.get(row)) };
        flatten_array_row(py, name, inner, &value, row, &mut flat)?;
        check_not_resized(seq, name, row_count)?;
        offsets.push(flat.end_offset(name)?);
    }
    let element_column = build_element_column(py, name, inner, &flat.ptrs)
        .map_err(|err| remap_element_err(py, name, &offsets, err))?;
    Ok(Column::Array(ArrayColumn::new(offsets, element_column)))
}

/// Rewrite an element-run error's flat index as the outer row and element
/// index, using the offsets built alongside the flat run. Nested Arrays remap
/// at each level, so the final message leads with the outermost row. An error
/// that is not a ValueError or whose text does not carry the `column "name"
/// row N` prefix passes through unchanged.
fn remap_element_err(py: Python<'_>, name: &str, offsets: &[i64], err: PyErr) -> PyErr {
    if !err.is_instance_of::<PyValueError>(py) {
        return err;
    }
    let Ok(text) = err.value(py).str() else {
        return err;
    };
    let Ok(text) = text.to_str() else {
        return err;
    };
    let prefix = format!("column {name:?} row ");
    let Some(rest) = text.strip_prefix(&prefix) else {
        return err;
    };
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    let Ok(flat) = rest[..digits].parse::<i64>() else {
        return err;
    };
    let row = offsets[1..].partition_point(|&end| end <= flat);
    if row + 1 >= offsets.len() {
        return err;
    }
    let element = flat - offsets[row];
    let tail = &rest[digits..];
    PyValueError::new_err(format!("{prefix}{row} element {element}{tail}"))
}

/// Append one Array row's elements to the flat run. Exact list/tuple rows
/// copy borrowed pointers without running Python; anything else keeps the
/// generic path's accepted containers and error messages via `list.extend`.
fn flatten_array_row(
    py: Python<'_>,
    name: &str,
    inner: &ChType,
    value: &Bound<'_, PyAny>,
    row: usize,
    flat: &mut FlatRefs,
) -> PyResult<()> {
    if let Ok(list) = value.downcast_exact::<PyList>() {
        // SAFETY: copying exact-list items runs no Python code.
        unsafe { flat.extend_from_seq(&ListSeq(list)) };
        return Ok(());
    }
    if let Ok(tuple) = value.downcast_exact::<PyTuple>() {
        // SAFETY: copying exact-tuple items runs no Python code.
        unsafe { flat.extend_from_seq(&TupleSeq(tuple)) };
        return Ok(());
    }
    if value.is_none() {
        return Err(PyValueError::new_err(format!(
            "column {name:?} row {row} is None but Array({inner}) is not Nullable"
        )));
    }
    if value.downcast::<PyString>().is_ok() {
        return Err(PyValueError::new_err(format!(
            "column {name:?} row {row} is a str, not an Array sequence"
        )));
    }
    // bytes-like rows flatten as int elements, matching the python codec's
    // `data.extend(row)` iteration semantics.
    if let Ok(bytes) = value.downcast::<PyBytes>() {
        return flat.extend_from_byte_run(py, bytes.as_bytes());
    }
    if let Ok(bytes) = value.downcast::<PyByteArray>() {
        return flat.extend_from_byte_run(py, &bytes.to_vec());
    }
    if value.downcast::<PySet>().is_ok()
        || value.downcast::<PyFrozenSet>().is_ok()
        || value.downcast::<PyDict>().is_ok()
    {
        return Err(PyValueError::new_err(format!(
            "column {name:?} row {row} is an unordered set/dict, which has no defined Array element order"
        )));
    }
    let items = PyList::empty(py);
    items
        .call_method1(intern!(py, "extend"), (value,))
        .map_err(|err| {
            let wrapped = PyValueError::new_err(format!(
                "column {name:?} row {row} is not a valid Array value"
            ));
            wrapped.set_cause(py, Some(err));
            wrapped
        })?;
    // SAFETY: items is an owned exact list; copying its items runs no Python
    // code and the strong references outlive the temporary list.
    unsafe { flat.extend_from_seq(&ListSeq(&items)) };
    Ok(())
}

/// Strong references to flattened Array elements. Holding a reference per
/// element keeps every pointer in the run valid across any Python code the
/// element conversion runs, so the run can be consumed with borrowed reads.
#[derive(Default)]
struct FlatRefs {
    ptrs: Vec<*mut ffi::PyObject>,
}

impl FlatRefs {
    /// Append every element of `seq` as a strong reference.
    ///
    /// # Safety
    ///
    /// Requires the GIL; `seq` must allow borrowed reads with no Python code
    /// running during the copy (an exact list or tuple).
    unsafe fn extend_from_seq<S: FastSeq>(&mut self, seq: &S) {
        let len = seq.size();
        self.ptrs.reserve(len);
        for index in 0..len {
            let item = seq.get(index);
            ffi::Py_INCREF(item);
            self.ptrs.push(item);
        }
    }

    /// Append each byte of `bytes` as an owned Python int element.
    fn extend_from_byte_run(&mut self, py: Python<'_>, bytes: &[u8]) -> PyResult<()> {
        self.ptrs.reserve(bytes.len());
        for &byte in bytes {
            // SAFETY: GIL held; PyLong_FromLong returns an owned reference
            // (a cached small int here) that Drop releases.
            let item = unsafe { ffi::PyLong_FromLong(c_long::from(byte)) };
            if item.is_null() {
                return Err(PyErr::fetch(py));
            }
            self.ptrs.push(item);
        }
        Ok(())
    }

    fn end_offset(&self, name: &str) -> PyResult<i64> {
        i64::try_from(self.ptrs.len()).map_err(|_| {
            PyValueError::new_err(format!(
                "column {name:?} Array element count exceeds i64 offset capacity"
            ))
        })
    }
}

impl Drop for FlatRefs {
    fn drop(&mut self) {
        // SAFETY: FlatRefs is only built and dropped inside a frame that holds
        // a `Python` token, so the GIL is held here.
        for &ptr in &self.ptrs {
            unsafe { ffi::Py_DECREF(ptr) };
        }
    }
}

/// Build the flattened Array element column. Mirrors `build_column`'s wrapper
/// dispatch with the flat run standing in for the Python container, feeding
/// the same seq fast paths and generic scalar loops.
fn build_element_column(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    ptrs: &[*mut ffi::PyObject],
) -> PyResult<Column> {
    let row_count = ptrs.len();
    let seq = PtrSeq(ptrs);
    match ch_type {
        ChType::Array(inner) => array_column_from_seq(py, name, inner, &seq, row_count),
        ChType::Nullable(inner) => {
            if matches!(
                inner.as_ref(),
                ChType::Nullable(_) | ChType::LowCardinality(_) | ChType::Array(_)
            ) {
                return Err(PyNotImplementedError::new_err(format!(
                    "unsupported Nullable inner type {inner} for column {name:?}"
                )));
            }
            if let Some(column) = try_fast_column_seq(py, name, inner, &seq, row_count, true)? {
                return Ok(column);
            }
            nullable_scalar_column(py, name, inner, &PtrRows { py, ptrs }, row_count)
        }
        ChType::LowCardinality(inner) => {
            let (nullable, value_type) = match inner.as_ref() {
                ChType::Nullable(value_type) => (true, value_type.as_ref()),
                other => (false, other),
            };
            if !is_low_cardinality_inner(value_type) {
                return Err(PyNotImplementedError::new_err(format!(
                    "unsupported LowCardinality inner type {value_type} for column {name:?}"
                )));
            }
            if matches!(value_type, ChType::String) {
                return lc_string_seq(py, name, value_type, &seq, row_count, nullable);
            }
            lc_scalar_column(
                py,
                name,
                value_type,
                &PtrRows { py, ptrs },
                row_count,
                nullable,
            )
        }
        _ => {
            if let Some(column) = try_fast_column_seq(py, name, ch_type, &seq, row_count, false)? {
                return Ok(column);
            }
            plain_scalar_column(py, name, ch_type, &PtrRows { py, ptrs }, row_count)
        }
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
    plain_scalar_column(py, name, ch_type, &column_values, row_count)
}

fn plain_scalar_column<'py, R: RowAccess<'py>>(
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

fn build_nullable_column(
    py: Python<'_>,
    name: &str,
    inner: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    if matches!(
        inner,
        ChType::Nullable(_) | ChType::LowCardinality(_) | ChType::Array(_)
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
    nullable_scalar_column(py, name, inner, &column_values, row_count)
}

fn nullable_scalar_column<'py, R: RowAccess<'py>>(
    py: Python<'py>,
    name: &str,
    inner: &ChType,
    rows: &R,
    row_count: usize,
) -> PyResult<Column> {
    let mut null_map = Vec::with_capacity(row_count);
    let mut scalars = Vec::with_capacity(row_count);
    for row in 0..row_count {
        let value = rows.value(row)?;
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
    if matches!(value_type, ChType::String) {
        if let Some(column) =
            lc_string_fast_column(py, name, value_type, values, row_count, nullable)?
        {
            return Ok(column);
        }
    }
    lc_scalar_column(py, name, value_type, &column_values, row_count, nullable)
}

fn lc_scalar_column<'py, R: RowAccess<'py>>(
    py: Python<'py>,
    name: &str,
    value_type: &ChType,
    rows: &R,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let mut indices = Vec::with_capacity(row_count);
    let mut dict_values = Vec::new();
    let mut slots = HashMap::<ScalarKey, i32>::new();
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));

    if nullable && row_count > 0 {
        dict_values.push(default_scalar(value_type)?);
    }

    for row in 0..row_count {
        let value = rows.value(row)?;
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

/// Positional row access for the generic scalar loops: a Python column
/// container or a flattened strong-reference run.
trait RowAccess<'py> {
    fn value(&self, row: usize) -> PyResult<Bound<'py, PyAny>>;
}

impl<'py> RowAccess<'py> for ColumnValues<'py> {
    fn value(&self, row: usize) -> PyResult<Bound<'py, PyAny>> {
        self.get_item(row)
    }
}

/// Row access over flattened Array element pointers, kept valid by the
/// `FlatRefs` strong references.
struct PtrRows<'a, 'py> {
    py: Python<'py>,
    ptrs: &'a [*mut ffi::PyObject],
}

impl<'py> RowAccess<'py> for PtrRows<'_, 'py> {
    fn value(&self, row: usize) -> PyResult<Bound<'py, PyAny>> {
        // SAFETY: FlatRefs holds a strong reference for every pointer in the
        // run for the whole build.
        Ok(unsafe { Bound::from_borrowed_ptr(self.py, self.ptrs[row]) })
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

/// Flattened Array element run; the `FlatRefs` strong references keep every
/// pointer valid for the whole build and the slice can never be resized, so
/// fallbacks that run Python need no revalidation.
struct PtrSeq<'a>(&'a [*mut ffi::PyObject]);

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
    try_buffer_column(py, ch_type, values, row_count, nullable)
}

/// Per-type dispatch over a borrowed-pointer run; runs once per column.
fn try_fast_column_seq<S: FastSeq>(
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
        ChType::Float32 => prim::<f32, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Float64 => prim::<f64, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Date => prim::<DateVal, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::Date32 => prim::<Date32Val, S>(py, name, ch_type, seq, row_count, nullable),
        ChType::DateTime { .. } => {
            prim::<DateTimeVal, S>(py, name, ch_type, seq, row_count, nullable)
        }
        ChType::DateTime64 { .. } => {
            prim::<DateTime64Val, S>(py, name, ch_type, seq, row_count, nullable)
        }
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
    ch_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Option<Column>> {
    fn buf<T: FastValue>(
        py: Python<'_>,
        values: &Bound<'_, PyAny>,
        row_count: usize,
        nullable: bool,
    ) -> PyResult<Option<Column>> {
        Ok(T::from_buffer(py, values, row_count)?.map(|vals| {
            // A buffer holds no Python objects, so a nullable column is all-valid.
            let validity = nullable.then(|| Bitmap::all_valid(row_count));
            T::into_column(vals, validity)
        }))
    }

    match ch_type {
        ChType::Int8 => buf::<i8>(py, values, row_count, nullable),
        ChType::Int16 => buf::<i16>(py, values, row_count, nullable),
        ChType::Int32 => buf::<i32>(py, values, row_count, nullable),
        ChType::Int64 => buf::<i64>(py, values, row_count, nullable),
        ChType::UInt8 => buf::<u8>(py, values, row_count, nullable),
        ChType::UInt16 => buf::<u16>(py, values, row_count, nullable),
        ChType::UInt32 => buf::<u32>(py, values, row_count, nullable),
        ChType::UInt64 => buf::<u64>(py, values, row_count, nullable),
        ChType::Float32 => buf::<f32>(py, values, row_count, nullable),
        ChType::Float64 => buf::<f64>(py, values, row_count, nullable),
        _ => Ok(None),
    }
}

/// Narrowing i64 conversion for the fast paths. A generic helper so macro
/// expansions do not trip clippy's fallible-conversion lint when the target
/// is i64 itself.
#[inline]
fn narrow_i64<T: TryFrom<i64>>(value: i64) -> Result<T, ()> {
    T::try_from(value).map_err(|_| ())
}

/// Temporal wire values: the fast path accepts exact raw ints only (the same
/// values `convert_scalar` accepts without touching the object protocol);
/// date/datetime/str objects and out-of-range ints go through the fallback,
/// which carries each type's specific conversion and range errors.
macro_rules! impl_fast_temporal {
    ($name:ident, $prim:ty, $variant:ident) => {
        #[derive(Clone, Copy)]
        #[repr(transparent)]
        struct $name($prim);

        impl FastValue for $name {
            const DEFAULT: Self = $name(0);

            #[inline]
            unsafe fn from_exact(ptr: *mut ffi::PyObject) -> Result<Self, ()> {
                narrow_i64::<$prim>(exact_long_as_i64(ptr)?).map(Self)
            }

            fn from_scalar(scalar: Scalar) -> PyResult<Self> {
                match scalar {
                    Scalar::$variant(value) => Ok(Self(value)),
                    _ => Err(PyValueError::new_err("internal scalar type mismatch")),
                }
            }

            fn into_column(values: Vec<Self>, validity: Option<Bitmap>) -> Column {
                // SAFETY: $name is #[repr(transparent)] over $prim, so the Vec
                // can be reinterpreted in place without copying.
                let values = unsafe {
                    let mut values = std::mem::ManuallyDrop::new(values);
                    Vec::from_raw_parts(
                        values.as_mut_ptr().cast::<$prim>(),
                        values.len(),
                        values.capacity(),
                    )
                };
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

/// Multiplicative hasher for pointer-identity keys; object addresses are not
/// attacker-controlled hash-DoS inputs, so a fast mix beats SipHash here.
#[derive(Default)]
struct PtrHasher(u64);

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
const PTR_CACHE_CAP: usize = 1 << 16;

/// Error if a mutable container changed size after a conversion that may have
/// run Python code, before the next borrowed read could go out of bounds.
#[inline]
fn check_not_resized<S: FastSeq>(seq: &S, name: &str, row_count: usize) -> PyResult<()> {
    if S::MUTABLE && seq.size() != row_count {
        return Err(PyValueError::new_err(format!(
            "column {name:?} was resized during encoding"
        )));
    }
    Ok(())
}

/// LowCardinality(String) fast path over an exact list or tuple. Returns
/// `Ok(None)` for other containers; the caller falls through to the generic
/// scalar loop.
fn lc_string_fast_column(
    py: Python<'_>,
    name: &str,
    value_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Option<Column>> {
    if let Ok(list) = values.downcast_exact::<PyList>() {
        return lc_string_seq(py, name, value_type, &ListSeq(list), row_count, nullable).map(Some);
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return lc_string_seq(py, name, value_type, &TupleSeq(tuple), row_count, nullable)
            .map(Some);
    }
    Ok(None)
}

/// Build a LowCardinality(String) dictionary column with two cache levels:
/// repeated str objects hit a pointer-identity map (no content read at all),
/// and new exact-str objects read their UTF-8 once for a content-keyed map,
/// allocating only when the content is genuinely new to the dictionary.
/// Non-str values fall back to `convert_scalar` for identical accepted-type
/// and error semantics; that fallback can run arbitrary Python (exotic buffer
/// types), so it holds a strong reference and revalidates the container size.
/// Pointer identity is only meaningful while no Python code has run since the
/// key was cached: a list can drop the last reference to an already-scanned
/// str mid-call, and the allocator can hand its address to a new str, so
/// every fallback return clears the pointer cache. Entries cached after the
/// clear are valid until the next fallback. A tuple keeps every item alive
/// for the whole call, so its cache never needs clearing.
fn lc_string_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    value_type: &ChType,
    seq: &S,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let mut indices = Vec::with_capacity(row_count);
    let mut dict_values: Vec<Scalar> = Vec::new();
    let mut ptr_slots: HashMap<usize, i32, std::hash::BuildHasherDefault<PtrHasher>> =
        HashMap::default();
    let mut content_slots: HashMap<Vec<u8>, i32> = HashMap::new();
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));

    if nullable && row_count > 0 {
        dict_values.push(default_scalar(value_type)?);
    }

    let dict_slot = |dict_values: &Vec<Scalar>| {
        i32::try_from(dict_values.len()).map_err(|_| {
            PyValueError::new_err(format!(
                "column {name:?} LowCardinality dictionary exceeds i32 index capacity"
            ))
        })
    };

    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every fallback revalidates; the borrowed pointer is consumed before
        // any Python code can run.
        let ptr = unsafe { seq.get(row) };
        if ptr == unsafe { ffi::Py_None() } {
            let Some(null_map) = &mut null_map else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but LowCardinality({value_type}) is not nullable"
                )));
            };
            indices.push(0);
            null_map.push(1);
            continue;
        }
        if let Some(null_map) = &mut null_map {
            null_map.push(0);
        }
        if let Some(&slot) = ptr_slots.get(&(ptr as usize)) {
            indices.push(slot);
            continue;
        }
        let slot = if unsafe { ffi::PyUnicode_CheckExact(ptr) } != 0 {
            // SAFETY: ptr is a valid borrowed reference, verified an exact
            // str; reading its UTF-8 runs no Python code.
            let obj =
                unsafe { Bound::from_borrowed_ptr(py, ptr).downcast_into_unchecked::<PyString>() };
            let bytes = obj.to_str()?.as_bytes();
            let slot = match content_slots.get(bytes) {
                Some(&slot) => slot,
                None => {
                    let slot = dict_slot(&dict_values)?;
                    content_slots.insert(bytes.to_vec(), slot);
                    dict_values.push(Scalar::Bytes(bytes.to_vec()));
                    slot
                }
            };
            // The container still holds this item (no Python ran since the
            // borrowed read), so its address is stable and unique among the
            // column's live values.
            if ptr_slots.len() < PTR_CACHE_CAP {
                ptr_slots.insert(ptr as usize, slot);
            }
            slot
        } else {
            // SAFETY: ptr is valid here; the strong reference keeps the item
            // alive across any Python code the fallback runs.
            let obj = unsafe { Bound::from_borrowed_ptr(py, ptr) };
            let scalar = convert_scalar(py, value_type, &obj, name, row)?;
            if S::MUTABLE {
                // The fallback may have run Python code: a same-size item
                // replacement can free a cached str whose address the
                // allocator may reuse, so drop all pointer-identity entries.
                check_not_resized(seq, name, row_count)?;
                ptr_slots.clear();
            }
            let Scalar::Bytes(bytes) = scalar else {
                return Err(PyValueError::new_err("internal scalar type mismatch"));
            };
            match content_slots.get(bytes.as_slice()) {
                Some(&slot) => slot,
                None => {
                    let slot = dict_slot(&dict_values)?;
                    content_slots.insert(bytes.clone(), slot);
                    dict_values.push(Scalar::Bytes(bytes));
                    slot
                }
            }
        };
        indices.push(slot);
    }

    let dict_column = column_from_scalars(value_type, dict_values, None)?;
    Ok(match null_map {
        Some(nulls) => Column::Dictionary(DictionaryColumn::new_nullable(
            indices,
            dict_column,
            Bitmap::from_ch_null_map(&nulls),
        )),
        None => Column::Dictionary(DictionaryColumn::new(indices, dict_column)),
    })
}

/// Build a UUID column: exact `uuid.UUID` items read the `int` attribute and
/// write the 16 wire bytes (hi u64 LE, then lo u64 LE) straight into the
/// column buffer; anything else (str, int, bytes) falls back to
/// `convert_scalar`. Both the attribute read (a patched class attribute) and
/// the fallback can run Python code, so the container size is revalidated
/// after each before the next borrowed read.
fn uuid_seq<S: FastSeq>(
    py: Python<'_>,
    seq: &S,
    ch_type: &ChType,
    name: &str,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let uuid_type = py
        .import(intern!(py, "uuid"))?
        .getattr(intern!(py, "UUID"))?;
    let uuid_type_ptr = uuid_type.as_ptr();
    let int_attr = intern!(py, "int");
    let mut data = Vec::with_capacity(16 * row_count);
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));

    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every conversion revalidates; the borrowed pointer is consumed
        // before any Python code can run.
        let ptr = unsafe { seq.get(row) };
        if ptr == unsafe { ffi::Py_None() } {
            let Some(null_map) = &mut null_map else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but {ch_type} is not Nullable"
                )));
            };
            null_map.push(1);
            data.extend_from_slice(&[0u8; 16]);
            continue;
        }
        if let Some(null_map) = &mut null_map {
            null_map.push(0);
        }
        // SAFETY: ptr is valid here; the strong reference keeps the item
        // alive across any Python code the conversion runs.
        let obj = unsafe { Bound::from_borrowed_ptr(py, ptr) };
        let fast = if unsafe { ffi::Py_TYPE(ptr) }.cast::<ffi::PyObject>() == uuid_type_ptr {
            obj.getattr(int_attr).and_then(|i| i.extract::<u128>()).ok()
        } else {
            None
        };
        match fast {
            Some(v) => {
                data.extend_from_slice(&((v >> 64) as u64).to_le_bytes());
                data.extend_from_slice(&(v as u64).to_le_bytes());
            }
            None => {
                let Scalar::Bytes(bytes) = convert_scalar(py, ch_type, &obj, name, row)? else {
                    return Err(PyValueError::new_err("internal scalar type mismatch"));
                };
                data.extend_from_slice(&bytes);
            }
        }
        check_not_resized(seq, name, row_count)?;
    }

    Ok(Column::Uuid(match null_map {
        Some(nulls) => FixedBinaryColumn::new_nullable(data, 16, Bitmap::from_ch_null_map(&nulls)),
        None => FixedBinaryColumn::new(data, 16),
    }))
}

/// Build an IPv4 column: exact `ipaddress.IPv4Address` items read the `_ip`
/// slot directly (or the attribute when the slot offset cannot be resolved),
/// exact ints and strs convert without constructing exceptions, and anything
/// else falls back to `convert_scalar`. The attribute read (a patched class
/// attribute) and the fallback can run Python code, so the container size is
/// revalidated after each before the next borrowed read.
fn ipv4_seq<S: FastSeq>(
    py: Python<'_>,
    seq: &S,
    ch_type: &ChType,
    name: &str,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let ipv4_type = py
        .import(intern!(py, "ipaddress"))?
        .getattr(intern!(py, "IPv4Address"))?;
    let ipv4_type_ptr = ipv4_type.as_ptr();
    let ip_attr = intern!(py, "_ip");
    // Re-resolved after any edge that runs Python, which can patch the class.
    let mut ip_slot = slot_object_offset(&ipv4_type, ip_attr);
    let mut values = Vec::<u32>::with_capacity(row_count);
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));

    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every conversion revalidates; the borrowed pointer is consumed
        // before any Python code can run.
        let ptr = unsafe { seq.get(row) };
        if ptr == unsafe { ffi::Py_None() } {
            let Some(null_map) = &mut null_map else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but {ch_type} is not Nullable"
                )));
            };
            null_map.push(1);
            values.push(0);
            continue;
        }
        if let Some(null_map) = &mut null_map {
            null_map.push(0);
        }
        if unsafe { ffi::Py_TYPE(ptr) }.cast::<ffi::PyObject>() == ipv4_type_ptr {
            if let Some(offset) = ip_slot {
                // SAFETY: ptr is an exact instance of the class the offset
                // was resolved from; the slot holds a strong reference (kept
                // alive by the instance) or NULL, and reading it runs no
                // Python code.
                let slot = unsafe { *ptr.cast::<u8>().offset(offset).cast::<*mut ffi::PyObject>() };
                if !slot.is_null() {
                    // SAFETY: GIL held; slot is a valid object pointer.
                    if let Ok(v) = unsafe { <u32 as FastValue>::from_exact(slot) } {
                        values.push(v);
                        continue;
                    }
                }
            }
            // SAFETY: ptr is valid here; the strong reference keeps the item
            // alive across any Python code the conversion runs.
            let obj = unsafe { Bound::from_borrowed_ptr(py, ptr) };
            match obj.getattr(ip_attr).and_then(|o| o.extract::<u32>()) {
                Ok(v) => values.push(v),
                Err(_) => values.push(ipv4_fallback(py, ch_type, &obj, name, row)?),
            }
            check_not_resized(seq, name, row_count)?;
            ip_slot = slot_object_offset(&ipv4_type, ip_attr);
            continue;
        }
        // SAFETY: GIL held; ptr is a valid borrowed item pointer.
        if let Ok(v) = unsafe { <u32 as FastValue>::from_exact(ptr) } {
            values.push(v);
            continue;
        }
        if unsafe { ffi::PyUnicode_CheckExact(ptr) } != 0 {
            // SAFETY: ptr is a valid borrowed reference, verified an exact
            // str; reading its UTF-8 runs no Python code.
            let obj =
                unsafe { Bound::from_borrowed_ptr(py, ptr).downcast_into_unchecked::<PyString>() };
            let v = obj
                .to_str()?
                .parse::<Ipv4Addr>()
                .map(|addr| u32::from_be_bytes(addr.octets()))
                .map_err(|_| conversion_error(name, row, "IPv4"))?;
            values.push(v);
            continue;
        }
        // SAFETY: ptr is valid here; the strong reference keeps the item
        // alive across any Python code the fallback runs.
        let obj = unsafe { Bound::from_borrowed_ptr(py, ptr) };
        values.push(ipv4_fallback(py, ch_type, &obj, name, row)?);
        check_not_resized(seq, name, row_count)?;
        ip_slot = slot_object_offset(&ipv4_type, ip_attr);
    }

    Ok(Column::Ipv4(match null_map {
        Some(nulls) => PrimitiveColumn::new_nullable(values, Bitmap::from_ch_null_map(&nulls)),
        None => PrimitiveColumn::new(values),
    }))
}

/// Instance offset of `attr` when it resolves to a plain object slot
/// (`__slots__` member descriptor, `Py_T_OBJECT_EX`, no flags) defined on
/// exactly `class`. Lets a loop over exact instances read the slot directly,
/// like CPython's LOAD_ATTR_SLOT specialization. `None` (patched or exotic
/// class attribute) means callers must use a normal attribute read.
fn slot_object_offset(class: &Bound<'_, PyAny>, attr: &Bound<'_, PyString>) -> Option<isize> {
    let descr = class.getattr(attr).ok()?;
    if unsafe { ffi::Py_TYPE(descr.as_ptr()) != std::ptr::addr_of_mut!(ffi::PyMemberDescr_Type) } {
        return None;
    }
    // SAFETY: descr is verified an exact member_descriptor, so its layout is
    // PyMemberDescrObject; d_member points at the defining PyMemberDef (the
    // ffi binding mistypes the field, hence the cast).
    unsafe {
        let descr = descr.as_ptr().cast::<ffi::PyMemberDescrObject>();
        if (*descr).d_common.d_type.cast::<ffi::PyObject>() != class.as_ptr() {
            return None;
        }
        let member = (*descr).d_member.cast::<ffi::PyMemberDef>();
        if member.is_null() || (*member).name.is_null() {
            return None;
        }
        if (*member).type_code != ffi::Py_T_OBJECT_EX || (*member).flags != 0 {
            return None;
        }
        let offset = (*member).offset;
        (offset > 0).then_some(offset)
    }
}

fn ipv4_fallback(
    py: Python<'_>,
    ch_type: &ChType,
    obj: &Bound<'_, PyAny>,
    name: &str,
    row: usize,
) -> PyResult<u32> {
    match convert_scalar(py, ch_type, obj, name, row)? {
        Scalar::Ipv4(v) => Ok(v),
        _ => Err(PyValueError::new_err("internal scalar type mismatch")),
    }
}

/// Enum code width plumbing for the enum sequence fast path.
trait EnumCode: Copy + Default {
    const TYPE_NAME: &'static str;

    fn from_enum_scalar(scalar: Scalar) -> PyResult<Self>;

    fn into_enum_column(values: Vec<Self>, validity: Option<Bitmap>) -> Column;
}

macro_rules! impl_enum_code {
    ($ty:ty, $variant:ident, $type_name:literal) => {
        impl EnumCode for $ty {
            const TYPE_NAME: &'static str = $type_name;

            fn from_enum_scalar(scalar: Scalar) -> PyResult<Self> {
                match scalar {
                    Scalar::$variant(value) => Ok(value),
                    _ => Err(PyValueError::new_err("internal scalar type mismatch")),
                }
            }

            fn into_enum_column(values: Vec<Self>, validity: Option<Bitmap>) -> Column {
                Column::$variant(match validity {
                    Some(validity) => PrimitiveColumn::new_nullable(values, validity),
                    None => PrimitiveColumn::new(values),
                })
            }
        }
    };
}

impl_enum_code!(i8, Enum8, "Enum8");
impl_enum_code!(i16, Enum16, "Enum16");

/// Build an Enum column with two lookup levels, following `lc_string_seq`:
/// repeated str objects hit a pointer-identity map, and new exact-str objects
/// read their UTF-8 once for a content lookup against the variant labels. An
/// exact str whose label is not defined is an error; anything else (raw int
/// codes, str subclasses) falls back to `convert_scalar`. The fallback can
/// run arbitrary Python, so it revalidates the container size and clears the
/// pointer-identity cache, whose entries are only valid while no Python code
/// has run since they were cached.
fn enum_seq<C: EnumCode, S: FastSeq>(
    py: Python<'_>,
    seq: &S,
    ch_type: &ChType,
    variants: &[(String, C)],
    name: &str,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let mut content_codes: HashMap<&[u8], C> = HashMap::with_capacity(variants.len());
    for (label, code) in variants {
        // First definition wins, matching enum8_value's linear scan.
        content_codes.entry(label.as_bytes()).or_insert(*code);
    }
    let mut ptr_codes: HashMap<usize, C, std::hash::BuildHasherDefault<PtrHasher>> =
        HashMap::default();
    let mut codes = Vec::with_capacity(row_count);
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
            codes.push(C::default());
            continue;
        }
        if let Some(null_map) = &mut null_map {
            null_map.push(0);
        }
        if let Some(&code) = ptr_codes.get(&(ptr as usize)) {
            codes.push(code);
            continue;
        }
        if unsafe { ffi::PyUnicode_CheckExact(ptr) } != 0 {
            // SAFETY: ptr is a valid borrowed reference, verified an exact
            // str; reading its UTF-8 runs no Python code.
            let obj =
                unsafe { Bound::from_borrowed_ptr(py, ptr).downcast_into_unchecked::<PyString>() };
            let label = obj.to_str()?;
            let Some(&code) = content_codes.get(label.as_bytes()) else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} {} label {label:?} is not defined",
                    C::TYPE_NAME
                )));
            };
            // The container still holds this item (no Python ran since the
            // borrowed read), so its address is stable and unique among the
            // column's live values.
            if ptr_codes.len() < PTR_CACHE_CAP {
                ptr_codes.insert(ptr as usize, code);
            }
            codes.push(code);
            continue;
        }
        // SAFETY: ptr is valid here; the strong reference keeps the item
        // alive across any Python code the fallback runs.
        let obj = unsafe { Bound::from_borrowed_ptr(py, ptr) };
        let scalar = convert_scalar(py, ch_type, &obj, name, row)?;
        codes.push(C::from_enum_scalar(scalar)?);
        if S::MUTABLE {
            check_not_resized(seq, name, row_count)?;
            // The fallback may have freed a cached str whose address the
            // allocator can reuse, so drop all pointer-identity entries.
            ptr_codes.clear();
        }
    }

    Ok(C::into_enum_column(
        codes,
        null_map.map(|nulls| Bitmap::from_ch_null_map(&nulls)),
    ))
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
        ChType::Array(_) => Err(PyNotImplementedError::new_err(
            "Array columns are built by build_array_column, not the scalar path",
        )),
        ChType::Tuple(_) | ChType::Map(..) => Err(PyNotImplementedError::new_err(
            "Tuple and Map columns are not supported for insert",
        )),
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
        ChType::Array(_) => Err(PyNotImplementedError::new_err(
            "Array columns are built by build_array_column, not the scalar path",
        )),
        ChType::Tuple(_) | ChType::Map(..) => Err(PyNotImplementedError::new_err(
            "Tuple and Map columns are not supported for insert",
        )),
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
        ChType::Array(_) => Err(PyNotImplementedError::new_err(
            "Array columns are built by build_array_column, not the scalar path",
        )),
        ChType::Tuple(_) | ChType::Map(..) => Err(PyNotImplementedError::new_err(
            "Tuple and Map columns are not supported for insert",
        )),
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

fn datetime_seconds(value: &Bound<'_, PyAny>, column: &str, row: usize) -> PyResult<i64> {
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

fn datetime64_ticks(
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
    // Precedence: str -> int (incl. subclasses) -> `int` attribute -> duck
    // int (__index__) -> 16 raw bytes. Type checks come before extract so
    // UUID inputs do not construct a failed extraction per value.
    if let Ok(s) = value.downcast::<PyString>() {
        return uuid_int_to_wire(parse_uuid_hex(s.to_str()?, column, row)?);
    }
    if unsafe { ffi::PyLong_Check(value.as_ptr()) } != 0 {
        let x = value
            .extract::<u128>()
            .map_err(|_| conversion_error(column, row, "UUID"))?;
        return uuid_int_to_wire(x);
    }
    if let Ok(x) = value
        .getattr(intern!(value.py(), "int"))
        .and_then(|i| i.extract::<u128>())
    {
        return uuid_int_to_wire(x);
    }
    if let Ok(x) = value.extract::<u128>() {
        return uuid_int_to_wire(x);
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
    // Precedence: int (incl. subclasses) -> str -> `_ip` attribute -> duck
    // int (__index__) -> `packed` attribute. Type checks come before extract
    // so IPv4Address inputs do not construct a failed extraction per value.
    if unsafe { ffi::PyLong_Check(value.as_ptr()) } != 0 {
        return value
            .extract::<u32>()
            .map_err(|_| conversion_error(column, row, "IPv4"));
    }
    if let Ok(s) = value.downcast::<PyString>() {
        return s
            .to_str()?
            .parse::<Ipv4Addr>()
            .map(|addr| u32::from_be_bytes(addr.octets()))
            .map_err(|_| conversion_error(column, row, "IPv4"));
    }
    if let Ok(ip) = value
        .getattr(intern!(value.py(), "_ip"))
        .and_then(|o| o.extract::<u32>())
    {
        return Ok(ip);
    }
    if let Ok(v) = value.extract::<u32>() {
        return Ok(v);
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

/// Wrapper/container nesting cap, mirroring the core decoder's parser. A crafted
/// deep type string would otherwise overflow the stack (an uncatchable crash)
/// before producing a ChType; bounding the parse also bounds every recursive
/// walk over the result, including `build_array_column`.
const MAX_TYPE_DEPTH: usize = 100;

fn parse_ch_type(type_name: &str) -> Option<ChType> {
    parse_ch_type_depth(type_name, 0)
}

fn parse_ch_type_depth(type_name: &str, depth: usize) -> Option<ChType> {
    if depth > MAX_TYPE_DEPTH {
        return None;
    }

    if let Some(inner) = type_name.strip_prefix("Nullable(") {
        if let Some(inner) = inner.strip_suffix(')') {
            let inner_type = parse_ch_type_depth(inner, depth + 1)?;
            if matches!(
                inner_type,
                ChType::Nullable(_) | ChType::LowCardinality(_) | ChType::Array(_)
            ) {
                return None;
            }
            return Some(ChType::Nullable(Box::new(inner_type)));
        }
    }

    if let Some(inner) = type_name.strip_prefix("LowCardinality(") {
        if let Some(inner) = inner.strip_suffix(')') {
            return parse_ch_type_depth(inner, depth + 1)
                .map(|t| ChType::LowCardinality(Box::new(t)));
        }
    }

    if let Some(inner) = type_name.strip_prefix("Array(") {
        if let Some(inner) = inner.strip_suffix(')') {
            return parse_ch_type_depth(inner, depth + 1).map(|t| ChType::Array(Box::new(t)));
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
