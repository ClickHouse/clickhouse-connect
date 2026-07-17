use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ffi::{c_int, c_long};
use std::io::Write as _;
use std::net::{IpAddr, Ipv4Addr};

use pyo3::buffer::{Element, PyBuffer};
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyValueError};
use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{
    PyAnyMethods, PyBool, PyByteArray, PyByteArrayMethods, PyBytes, PyDate, PyDateTime, PyDelta,
    PyDeltaAccess, PyDict, PyFloat, PyFrozenSet, PyList, PySet, PyString, PyStringMethods, PyTime,
    PyTimeAccess, PyTuple,
};

use ch_core_rs::batch::ColBatch as RustColBatch;
use ch_core_rs::bitmap::Bitmap;
use ch_core_rs::column::{
    AggregateStateColumn, ArrayColumn, BoolColumn, Column, DecimalColumn, DictionaryColumn,
    FixedBinaryColumn, JsonColumn, MapColumn, NothingColumn, PrimitiveColumn, TupleColumn,
    Utf8Column, VariantColumn,
};
use ch_core_rs::native::decode::{low_cardinality_dict_value_type, parse_ch_type};
use ch_core_rs::native::encode::{encode_block, EncodeError, EncodeOptions};
use ch_core_rs::schema::{ChType, Field, Schema};

use crate::decoder::buffer_to_vec;

const EPOCH_DATE_ORDINAL: i64 = 719_163;
const IPV4_V6_PREFIX: [u8; 12] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff];
const MAX_TIME_SECONDS: i64 = 999 * 3_600 + 59 * 60 + 59;

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
        // Dynamic inserts ship a String column the server casts back with
        // type inference, so the Native header carries the substituted type.
        let header_type = dynamic_insert_type(&ch_type).unwrap_or(ch_type);
        fields.push(Field {
            name: name.clone(),
            ch_type: header_type,
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
                    // V1/V2 Dynamic layout: pre-25.6 servers reject FLATTENED.
                    flattened_dynamic: false,
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
    // Expand SimpleAggregateFunction/geo/Nested aliases to the physical type
    // whose column the encoder actually builds; the block header still renders
    // the alias spelling from the Field's ChType.
    if let Some(delegate) = ch_type.physical_delegate() {
        return build_column(py, name, &delegate, values, row_count);
    }
    match ch_type {
        ChType::Nothing => build_nothing_column(name, values, row_count, false),
        ChType::AggregateFunction { .. } => {
            build_aggregate_state_column(py, name, ch_type, values, row_count)
        }
        ChType::Nullable(inner) => build_nullable_column(py, name, inner, values, row_count),
        ChType::LowCardinality(inner) => {
            build_low_cardinality_column(py, name, inner, values, row_count)
        }
        ChType::Array(inner) => build_array_column(py, name, inner, values, row_count),
        ChType::Tuple(elements) => build_tuple_column(py, name, elements, values, row_count, false),
        ChType::Map(key, value) => build_map_column(py, name, key, value, values, row_count),
        ChType::Variant(alternatives) => {
            build_variant_column(py, name, alternatives, values, row_count)
        }
        ChType::Dynamic { .. } => build_dynamic_string_column(name, values, row_count),
        ChType::Json { .. } => build_json_text_column(py, name, values, row_count, false),
        _ => build_plain_column(py, name, ch_type, values, row_count),
    }
}

/// The insert-header substitution for a type containing Dynamic: Dynamic
/// encodes as a String column the server casts back with
/// `cast_string_to_dynamic_use_inference`, recursing into Array/Tuple/Map
/// exactly like the python codec's `insert_name` chain. `None` when the type
/// contains no Dynamic.
fn dynamic_insert_type(ch_type: &ChType) -> Option<ChType> {
    // Expand name-decoration aliases (Nested, geo) to the physical type first,
    // matching `build_column`'s dispatch, so the substituted header describes
    // the column actually built.
    if let Some(delegate) = ch_type.physical_delegate() {
        return dynamic_insert_type(&delegate);
    }
    match ch_type {
        ChType::Dynamic { .. } => Some(ChType::String),
        ChType::Array(inner) => {
            dynamic_insert_type(inner).map(|inner| ChType::Array(Box::new(inner)))
        }
        ChType::Map(key, value) => {
            let new_key = dynamic_insert_type(key);
            let new_value = dynamic_insert_type(value);
            if new_key.is_none() && new_value.is_none() {
                return None;
            }
            Some(ChType::Map(
                Box::new(new_key.unwrap_or_else(|| (**key).clone())),
                Box::new(new_value.unwrap_or_else(|| (**value).clone())),
            ))
        }
        ChType::Tuple(elements) => {
            let substituted: Vec<Option<ChType>> = elements
                .iter()
                .map(|(_, element)| dynamic_insert_type(element))
                .collect();
            if substituted.iter().all(Option::is_none) {
                return None;
            }
            Some(ChType::Tuple(
                elements
                    .iter()
                    .zip(substituted)
                    .map(|((name, element), sub)| {
                        (name.clone(), sub.unwrap_or_else(|| element.clone()))
                    })
                    .collect(),
            ))
        }
        _ => None,
    }
}

/// Build a Dynamic column as its String insert form: each value stringifies
/// with `str()` and None becomes the literal "NULL", matching the python
/// codec's `write_str_values`.
fn build_dynamic_string_column(
    name: &str,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    dynamic_string_column(&column_values, row_count)
}

fn dynamic_string_column<'py, R: RowAccess<'py>>(rows: &R, row_count: usize) -> PyResult<Column> {
    let mut offsets = Vec::with_capacity(row_count + 1);
    offsets.push(0i32);
    let mut data = Vec::new();
    for row in 0..row_count {
        let value = rows.value(row)?;
        if value.is_none() {
            data.extend_from_slice(b"NULL");
        } else {
            let text = value.str()?;
            data.extend_from_slice(text.to_str()?.as_bytes());
        }
        let offset = i32::try_from(data.len())
            .map_err(|_| PyValueError::new_err("String column data exceeds i32 offset capacity"))?;
        offsets.push(offset);
    }
    Ok(Column::Utf8(Utf8Column::new(offsets, data)))
}

/// Build the core's STRING-mode JSON representation. The server performs its
/// normal JSON path/type inference from each document, while the Native header
/// remains the declared JSON type and the core emits structure word 1.
fn build_json_text_column(
    py: Python<'_>,
    name: &str,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let rows = ColumnValues::new(values, name)?;
    check_row_count(name, &rows, row_count)?;
    if let Ok(list) = values.downcast_exact::<PyList>() {
        return json_text_column_from_rows(
            py,
            name,
            &ListRows {
                py,
                list,
                name,
                expected: row_count,
            },
            row_count,
            nullable,
        );
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return json_text_column_from_rows(py, name, &TupleRows { py, tuple }, row_count, nullable);
    }
    json_text_column_from_rows(py, name, &rows, row_count, nullable)
}

fn json_text_column_from_rows<'py, R: RowAccess<'py>>(
    py: Python<'py>,
    name: &str,
    rows: &R,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    // Match the Python codec's `first_value` sniff: nullable scans for the
    // first non-null value, non-nullable inspects row 0 alone (None included).
    // A str there marks the whole column as pre-serialized JSON text.
    let mut direct_text = false;
    if nullable {
        for row in 0..row_count {
            let value = rows.value(row)?;
            if !value.is_none() {
                direct_text = value.downcast::<PyString>().is_ok();
                break;
            }
        }
    } else if row_count > 0 {
        direct_text = rows.value(0)?.downcast::<PyString>().is_ok();
    }

    // Resolved lazily on the first row the native writer cannot serialize.
    let mut serializer: Option<Bound<'py, PyAny>> = None;
    let mut offsets = Vec::with_capacity(row_count + 1);
    let mut data = Vec::with_capacity(row_count.saturating_mul(64));
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));
    offsets.push(0i32);
    for row in 0..row_count {
        let value = rows.value(row)?;
        if value.is_none() {
            if let Some(null_map) = &mut null_map {
                null_map.push(1);
            }
            data.extend_from_slice(b"null");
        } else {
            if let Some(null_map) = &mut null_map {
                null_map.push(0);
            }
            if direct_text {
                append_json_document(&value, name, row, &mut data, false)?;
            } else {
                let row_start = data.len();
                if write_json_value(value.as_ptr(), &mut data, 0).is_err() {
                    data.truncate(row_start);
                    let serializer = match &mut serializer {
                        Some(serializer) => &*serializer,
                        slot => {
                            // Import and attribute lookup run Python, so the
                            // source list must be revalidated before the next
                            // unchecked row read.
                            let resolved = py
                                .import("clickhouse_connect.datatypes.dynamic")?
                                .getattr("any_to_json")?;
                            let serializer = slot.insert(resolved);
                            rows.validate()?;
                            &*serializer
                        }
                    };
                    let encoded = serializer
                        .call1((&value,))
                        .map_err(|err| json_serialize_err(py, name, row, err))?;
                    rows.validate()?;
                    append_json_document(&encoded, name, row, &mut data, true)?;
                }
            }
        }
        offsets.push(i32::try_from(data.len()).map_err(|_| {
            PyValueError::new_err(format!(
                "column {name:?} JSON document data exceeds i32 offset capacity"
            ))
        })?);
    }
    let validity = null_map.map(|nulls| Bitmap::from_ch_null_map(&nulls));
    Ok(Column::Json(
        JsonColumn::text(Utf8Column::new(offsets, data)).with_validity(validity),
    ))
}

fn append_json_document(
    value: &Bound<'_, PyAny>,
    name: &str,
    row: usize,
    data: &mut Vec<u8>,
    serialized: bool,
) -> PyResult<()> {
    if let Ok(text) = value.downcast::<PyString>() {
        data.extend_from_slice(
            text.to_str()
                .map_err(|err| json_serialize_err(value.py(), name, row, err))?
                .as_bytes(),
        );
        return Ok(());
    }
    if let Ok(bytes) = value.downcast::<PyBytes>() {
        data.extend_from_slice(bytes.as_bytes());
        return Ok(());
    }
    if let Ok(bytes) = value.downcast::<PyByteArray>() {
        // SAFETY: the GIL is held and the complete bytearray is copied before
        // any Python API can run and resize it.
        data.extend_from_slice(unsafe { bytes.as_bytes() });
        return Ok(());
    }
    let type_name = python_type_name(value.as_ptr());
    Err(PyValueError::new_err(if serialized {
        format!(
            "column {name:?} row {row} JSON serializer returned {type_name}, expected str or bytes"
        )
    } else {
        format!(
            "column {name:?} row {row} is {type_name}, expected str because the first value marked \
             this column as pre-serialized JSON strings"
        )
    }))
}

fn json_serialize_err(py: Python<'_>, name: &str, row: usize, err: PyErr) -> PyErr {
    let wrapped = PyValueError::new_err(format!(
        "column {name:?} row {row} cannot be serialized as JSON"
    ));
    wrapped.set_cause(py, Some(err));
    wrapped
}

/// Marker: the native JSON writer cannot serialize this value; the caller
/// rewinds the row and uses the Python serializer.
struct JsonUnsupported;

/// Nesting depth past which the native writer defers to the Python serializer.
const JSON_NATIVE_MAX_DEPTH: usize = 128;

/// Write one Python value as JSON text using exact-type C-API fast paths.
/// Exact None/bool/int/float/str/dict/list/tuple traversal runs no user
/// Python, so the source column cannot mutate mid-row; anything else
/// (subclasses, ints past i64, non-finite floats, non-str dict keys, depth
/// past the cap) is `JsonUnsupported` and goes through the Python serializer.
fn write_json_value(
    value: *mut ffi::PyObject,
    data: &mut Vec<u8>,
    depth: usize,
) -> Result<(), JsonUnsupported> {
    if depth > JSON_NATIVE_MAX_DEPTH {
        return Err(JsonUnsupported);
    }
    // SAFETY: value is live, the GIL is held for the whole traversal, and the
    // borrowed container items read below stay valid because no user Python
    // runs on this path.
    unsafe {
        if value == ffi::Py_None() {
            data.extend_from_slice(b"null");
            return Ok(());
        }
        if value == ffi::Py_True() {
            data.extend_from_slice(b"true");
            return Ok(());
        }
        if value == ffi::Py_False() {
            data.extend_from_slice(b"false");
            return Ok(());
        }
        if ffi::PyLong_CheckExact(value) != 0 {
            let long = ffi::PyLong_AsLongLong(value);
            if long == -1 && !ffi::PyErr_Occurred().is_null() {
                ffi::PyErr_Clear();
                return Err(JsonUnsupported);
            }
            // io::Write into a Vec is infallible.
            let _ = write!(data, "{long}");
            return Ok(());
        }
        if ffi::PyFloat_CheckExact(value) != 0 {
            let float = ffi::PyFloat_AS_DOUBLE(value);
            if !float.is_finite() {
                return Err(JsonUnsupported);
            }
            let start = data.len();
            let _ = write!(data, "{float}");
            // Rust Display never uses exponents; a '.'-free rendering is a
            // whole float and keeps its float-ness with an explicit ".0".
            if !data[start..].contains(&b'.') {
                data.extend_from_slice(b".0");
            }
            return Ok(());
        }
        if ffi::PyUnicode_CheckExact(value) != 0 {
            write_json_string(json_utf8(value)?, data);
            return Ok(());
        }
        if ffi::PyDict_CheckExact(value) != 0 {
            data.push(b'{');
            let mut pos: ffi::Py_ssize_t = 0;
            let mut key: *mut ffi::PyObject = std::ptr::null_mut();
            let mut item: *mut ffi::PyObject = std::ptr::null_mut();
            let mut first = true;
            while ffi::PyDict_Next(value, &mut pos, &mut key, &mut item) != 0 {
                if ffi::PyUnicode_CheckExact(key) == 0 {
                    return Err(JsonUnsupported);
                }
                if !first {
                    data.push(b',');
                }
                first = false;
                write_json_string(json_utf8(key)?, data);
                data.push(b':');
                write_json_value(item, data, depth + 1)?;
            }
            data.push(b'}');
            return Ok(());
        }
        if ffi::PyList_CheckExact(value) != 0 {
            data.push(b'[');
            for index in 0..ffi::PyList_GET_SIZE(value) {
                if index > 0 {
                    data.push(b',');
                }
                write_json_value(ffi::PyList_GET_ITEM(value, index), data, depth + 1)?;
            }
            data.push(b']');
            return Ok(());
        }
        if ffi::PyTuple_CheckExact(value) != 0 {
            data.push(b'[');
            for index in 0..ffi::PyTuple_GET_SIZE(value) {
                if index > 0 {
                    data.push(b',');
                }
                write_json_value(ffi::PyTuple_GET_ITEM(value, index), data, depth + 1)?;
            }
            data.push(b']');
            return Ok(());
        }
    }
    Err(JsonUnsupported)
}

/// Borrow the UTF-8 bytes of an exact str; a lone surrogate cannot encode and
/// defers to the Python serializer.
///
/// # Safety
///
/// `value` must be a live exact `str` and the GIL must be held. The returned
/// slice borrows the object's cached UTF-8 buffer.
unsafe fn json_utf8<'a>(value: *mut ffi::PyObject) -> Result<&'a [u8], JsonUnsupported> {
    let mut size: ffi::Py_ssize_t = 0;
    let ptr = ffi::PyUnicode_AsUTF8AndSize(value, &mut size);
    if ptr.is_null() {
        ffi::PyErr_Clear();
        return Err(JsonUnsupported);
    }
    Ok(std::slice::from_raw_parts(ptr.cast::<u8>(), size as usize))
}

/// Emit a JSON string with mandatory-only escapes: `"`, `\`, and control
/// bytes below 0x20 (`\n`/`\r`/`\t` short forms, `\u00XX` otherwise).
/// Non-ASCII bytes pass through as raw UTF-8.
fn write_json_string(bytes: &[u8], data: &mut Vec<u8>) {
    data.push(b'"');
    let mut start = 0;
    for (index, &byte) in bytes.iter().enumerate() {
        let escape: &[u8] = match byte {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            b'\t' => b"\\t",
            0x00..=0x1f => b"",
            _ => continue,
        };
        data.extend_from_slice(&bytes[start..index]);
        if escape.is_empty() {
            let _ = write!(data, "\\u{byte:04x}");
        } else {
            data.extend_from_slice(escape);
        }
        start = index + 1;
    }
    data.extend_from_slice(&bytes[start..]);
    data.push(b'"');
}

/// Build Nothing from a Python column without constructing per-row scalar
/// values. Plain Nothing ignores every placeholder, matching the Python
/// codec. Nullable(Nothing) additionally retains which placeholders were
/// Python None so the core can emit the structural null map before the
/// canonical Nothing marker bytes.
fn build_nothing_column(
    name: &str,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;

    if let Ok(list) = values.downcast_exact::<PyList>() {
        return Ok(nothing_column_from_seq(&ListSeq(list), row_count, nullable));
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return Ok(nothing_column_from_seq(
            &TupleSeq(tuple),
            row_count,
            nullable,
        ));
    }

    let validity = if nullable {
        let mut null_map = Vec::with_capacity(row_count);
        for row in 0..row_count {
            null_map.push(u8::from(column_values.get_item(row)?.is_none()));
        }
        Some(Bitmap::from_ch_null_map(&null_map))
    } else {
        None
    };
    Ok(nothing_column(row_count, validity))
}

fn nothing_column_from_seq<S: FastSeq>(seq: &S, row_count: usize, nullable: bool) -> Column {
    // The unsafe seq reads below are in bounds only under this equality.
    assert_eq!(seq.size(), row_count);
    let validity = nullable.then(|| {
        let mut null_map = Vec::with_capacity(row_count);
        for row in 0..row_count {
            // SAFETY: row < row_count == seq.size(), the sequence is borrowed
            // while the GIL is held, and comparing to Py_None runs no Python.
            let value = unsafe { seq.get(row) };
            null_map.push(u8::from(value == unsafe { ffi::Py_None() }));
        }
        Bitmap::from_ch_null_map(&null_map)
    });
    nothing_column(row_count, validity)
}

fn nothing_column(row_count: usize, validity: Option<Bitmap>) -> Column {
    Column::Nothing(match validity {
        Some(validity) => NothingColumn::new_nullable(row_count, validity),
        None => NothingColumn::new(row_count),
    })
}

/// Build one AggregateFunction state column from exact serialized state bytes.
/// State framing and validation are function-specific, so the binding only
/// recovers Python buffer boundaries; the core encoder validates every slice
/// against the codec registered for the declared ChType.
fn build_aggregate_state_column(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    if let Ok(list) = values.downcast_exact::<PyList>() {
        return aggregate_state_column_from_seq(py, name, ch_type, &ListSeq(list), row_count);
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return aggregate_state_column_from_seq(py, name, ch_type, &TupleSeq(tuple), row_count);
    }
    aggregate_state_column_from_rows(name, ch_type, &column_values, row_count)
}

fn aggregate_state_column_from_rows<'py, R: RowAccess<'py>>(
    name: &str,
    ch_type: &ChType,
    rows: &R,
    row_count: usize,
) -> PyResult<Column> {
    let mut offsets = Vec::with_capacity(row_count + 1);
    let mut data = Vec::new();
    offsets.push(0);
    for row in 0..row_count {
        let value = rows.value(row)?;
        append_aggregate_state(&value, name, ch_type, row, &mut offsets, &mut data)?;
    }
    Ok(Column::AggregateState(AggregateStateColumn::new(
        offsets, data,
    )))
}

fn aggregate_state_column_from_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    seq: &S,
    row_count: usize,
) -> PyResult<Column> {
    let mut offsets = Vec::with_capacity(row_count + 1);
    let mut data = Vec::with_capacity(aggregate_state_size_hint(py, seq, row_count));
    offsets.push(0);
    for row in 0..row_count {
        // SAFETY: row is in bounds; the size is checked against row_count up
        // front and revalidated after any row that ran Python.
        // `from_borrowed_ptr` takes a strong reference, so container mutation
        // during generic buffer conversion cannot invalidate `value`.
        let value = unsafe { Bound::from_borrowed_ptr(py, seq.get(row)) };
        let ran_python =
            append_aggregate_state(&value, name, ch_type, row, &mut offsets, &mut data)?;
        if ran_python {
            // Dropping the strong reference can run __del__, which can resize
            // the container, so it must precede the size revalidation.
            drop(value);
            check_not_resized(seq, name, row_count)?;
        }
    }
    Ok(Column::AggregateState(AggregateStateColumn::new(
        offsets, data,
    )))
}

/// Exact data size when every row is exact bytes, read via `Py_SIZE` without
/// running Python; 0 (no reservation) otherwise.
fn aggregate_state_size_hint<S: FastSeq>(_py: Python<'_>, seq: &S, row_count: usize) -> usize {
    let mut total = 0usize;
    for row in 0..row_count {
        // SAFETY: the GIL is held, row is in bounds, and no Python runs in
        // this loop, so the borrowed pointer stays valid.
        let ptr = unsafe { seq.get(row) };
        if unsafe { ffi::PyBytes_CheckExact(ptr) } == 0 {
            return 0;
        }
        total = total.saturating_add(unsafe { ffi::Py_SIZE(ptr) } as usize);
    }
    total
}

fn agg_offset_overflow(name: &str) -> PyErr {
    PyValueError::new_err(format!(
        "column {name:?} AggregateFunction state data exceeds i64 offset capacity"
    ))
}

fn agg_state_convert_err(py: Python<'_>, name: &str, row: usize, err: PyErr) -> PyErr {
    let wrapped = PyValueError::new_err(format!(
        "column {name:?} row {row} cannot be converted to AggregateFunction state bytes"
    ));
    wrapped.set_cause(py, Some(err));
    wrapped
}

/// Append one Python bytes-like state directly into the column data run.
/// Returns true when generic buffer conversion may have executed Python.
fn append_aggregate_state(
    value: &Bound<'_, PyAny>,
    name: &str,
    ch_type: &ChType,
    row: usize,
    offsets: &mut Vec<i64>,
    data: &mut Vec<u8>,
) -> PyResult<bool> {
    if value.is_none() {
        return Err(PyValueError::new_err(format!(
            "column {name:?} row {row} is None but {ch_type} is not Nullable"
        )));
    }
    let ran_python = if let Ok(bytes) = value.downcast::<PyBytes>() {
        data.extend_from_slice(bytes.as_bytes());
        false
    } else if let Ok(bytes) = value.downcast::<PyByteArray>() {
        // SAFETY: the GIL is held and extend_from_slice copies the complete
        // buffer before any Python or PyO3 API can run and invalidate it.
        data.extend_from_slice(unsafe { bytes.as_bytes() });
        false
    } else {
        let buffer = PyBuffer::<u8>::get(value)
            .map_err(|err| agg_state_convert_err(value.py(), name, row, err))?;
        let start = data.len();
        let end = start
            .checked_add(buffer.item_count())
            .ok_or_else(|| agg_offset_overflow(name))?;
        data.resize(end, 0);
        if let Err(err) = buffer.copy_to_slice(value.py(), &mut data[start..end]) {
            data.truncate(start);
            return Err(agg_state_convert_err(value.py(), name, row, err));
        }
        true
    };
    offsets.push(i64::try_from(data.len()).map_err(|_| agg_offset_overflow(name))?);
    Ok(ran_python)
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
        offsets.push(flat.end_offset(name, "Array element")?);
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
        offsets.push(flat.end_offset(name, "Array element")?);
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
    remap_flat_err(py, name, offsets, "element", err)
}

/// Shared flat-run error rewrite: `unit` names what the flat index counts
/// ("element" for Array, "key"/"value" for Map).
fn remap_flat_err(py: Python<'_>, name: &str, offsets: &[i64], unit: &str, err: PyErr) -> PyErr {
    let Some((flat, prefix, tail)) = parse_row_err(py, name, &err) else {
        return err;
    };
    let Ok(flat) = i64::try_from(flat) else {
        return err;
    };
    let row = offsets[1..].partition_point(|&end| end <= flat);
    if row + 1 >= offsets.len() {
        return err;
    }
    let element = flat - offsets[row];
    PyValueError::new_err(format!("{prefix}{row} {unit} {element}{tail}"))
}

/// Parse a ValueError's `column {name:?} row N` prefix into the row index,
/// the prefix through "row ", and the message tail after the digits. `None`
/// for any error that does not carry the prefix.
fn parse_row_err(py: Python<'_>, name: &str, err: &PyErr) -> Option<(usize, String, String)> {
    if !err.is_instance_of::<PyValueError>(py) {
        return None;
    }
    let text = err.value(py).str().ok()?;
    let text = text.to_str().ok()?;
    let prefix = format!("column {name:?} row ");
    let rest = text.strip_prefix(&prefix)?;
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    let row = rest[..digits].parse::<usize>().ok()?;
    Some((row, prefix, rest[digits..].to_string()))
}

/// Rewrite a tuple field-run error: the flat index equals the outer row, so
/// only the element label (index, or name for a named tuple) is inserted.
fn remap_tuple_element_err(py: Python<'_>, name: &str, label: &str, err: PyErr) -> PyErr {
    let Some((row, prefix, tail)) = parse_row_err(py, name, &err) else {
        return err;
    };
    PyValueError::new_err(format!("{prefix}{row} element {label}{tail}"))
}

/// Rewrite an alternative's dense child row index to its logical Variant row.
fn remap_variant_child_err(
    py: Python<'_>,
    name: &str,
    discriminators: &[u8],
    alternative: usize,
    err: PyErr,
) -> PyErr {
    let Some((dense_row, prefix, tail)) = parse_row_err(py, name, &err) else {
        return err;
    };
    let Ok(discriminator) = u8::try_from(alternative) else {
        return err;
    };
    let Some(logical_row) = discriminators
        .iter()
        .enumerate()
        .filter_map(|(row, &disc)| (disc == discriminator).then_some(row))
        .nth(dense_row)
    else {
        return err;
    };
    PyValueError::new_err(format!("{prefix}{logical_row}{tail}"))
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

    /// Append a strong reference to `obj`.
    fn push_ref(&mut self, obj: &Bound<'_, PyAny>) {
        // SAFETY: GIL held (the Bound proves it); Drop releases the reference.
        unsafe { ffi::Py_INCREF(obj.as_ptr()) };
        self.ptrs.push(obj.as_ptr());
    }

    /// Append a strong reference to None.
    fn push_none(&mut self, _py: Python<'_>) {
        // SAFETY: GIL held; Py_None is a valid object pointer.
        unsafe {
            let none = ffi::Py_None();
            ffi::Py_INCREF(none);
            self.ptrs.push(none);
        }
    }

    /// Current run length as an i64 offset. `unit` names what the run counts
    /// ("Array element", "Map entry").
    fn end_offset(&self, name: &str, unit: &str) -> PyResult<i64> {
        i64::try_from(self.ptrs.len()).map_err(|_| {
            PyValueError::new_err(format!(
                "column {name:?} {unit} count exceeds i64 offset capacity"
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
    // Expand SimpleAggregateFunction/geo/Nested aliases before dispatch so a
    // nested alias (e.g. Array(Point)) builds its physical element column.
    if let Some(delegate) = ch_type.physical_delegate() {
        return build_element_column(py, name, &delegate, ptrs);
    }
    match ch_type {
        ChType::Nothing => Ok(nothing_column_from_seq(&seq, row_count, false)),
        ChType::AggregateFunction { .. } => {
            aggregate_state_column_from_seq(py, name, ch_type, &seq, row_count)
        }
        ChType::Array(inner) => array_column_from_seq(py, name, inner, &seq, row_count),
        ChType::Tuple(elements) => {
            tuple_column_from_seq(py, name, elements, &seq, row_count, false)
        }
        ChType::Map(key, value) => map_column_from_seq(py, name, key, value, &seq, row_count),
        ChType::Variant(alternatives) => {
            variant_column_from_seq(py, name, alternatives, &seq, row_count)
        }
        ChType::Dynamic { .. } => dynamic_string_column(&PtrRows { py, ptrs }, row_count),
        ChType::Json { .. } => {
            json_text_column_from_rows(py, name, &PtrRows { py, ptrs }, row_count, false)
        }
        ChType::Nullable(inner) => {
            // Nullable(Point) -> Nullable(Tuple), Nullable(SAF(T)) -> Nullable(T);
            // the physical delegate governs the nullable element shape.
            if let Some(delegate) = inner.physical_delegate() {
                return build_element_column(py, name, &ChType::Nullable(Box::new(delegate)), ptrs);
            }
            if let ChType::Tuple(elements) = inner.as_ref() {
                return tuple_column_from_seq(py, name, elements, &seq, row_count, true);
            }
            if matches!(inner.as_ref(), ChType::Nothing) {
                return Ok(nothing_column_from_seq(&seq, row_count, true));
            }
            if matches!(inner.as_ref(), ChType::Json { .. }) {
                return json_text_column_from_rows(
                    py,
                    name,
                    &PtrRows { py, ptrs },
                    row_count,
                    true,
                );
            }
            if matches!(
                inner.as_ref(),
                ChType::Nullable(_)
                    | ChType::LowCardinality(_)
                    | ChType::Array(_)
                    | ChType::Map(..)
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
            // Resolve the physical dict value type: strip the SAF chain, unwrap an
            // optional Nullable, strip again. So LowCardinality(SAF(anyLast, String))
            // and LowCardinality(SAF(anyLast, Nullable(String))) reach the String path.
            let (nullable, value_type) = low_cardinality_dict_value_type(inner);
            if !is_low_cardinality_inner(value_type) {
                return Err(PyNotImplementedError::new_err(format!(
                    "unsupported LowCardinality inner type {value_type} for column {name:?}"
                )));
            }
            if matches!(value_type, ChType::String) {
                return lc_string_seq(py, name, value_type, &seq, row_count, nullable);
            }
            if wide_int_layout(value_type).is_some() {
                return lc_wide_column(
                    py,
                    name,
                    value_type,
                    &PtrRows { py, ptrs },
                    row_count,
                    nullable,
                );
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

/// Build a `Tuple(T1, ...)` column: each row fans out into one strong-ref run
/// per element, then each element column is built once over its run, hitting
/// the same per-type fast paths as a plain column. `nullable` builds the
/// tuple-level validity of a `Nullable(Tuple)`; a None row keeps the children
/// full length with per-type default placeholders.
fn build_tuple_column(
    py: Python<'_>,
    name: &str,
    elements: &[(Option<String>, ChType)],
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    if let Ok(list) = values.downcast_exact::<PyList>() {
        return tuple_column_from_seq(py, name, elements, &ListSeq(list), row_count, nullable);
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return tuple_column_from_seq(py, name, elements, &TupleSeq(tuple), row_count, nullable);
    }
    let mut builder = TupleBuilder::new(py, name, elements, row_count, nullable);
    for row in 0..row_count {
        let value = column_values.get_item(row)?;
        builder.push_row(&value, row)?;
    }
    builder.finish()
}

/// Tuple rows over an exact list or tuple of rows, or a flattened element run.
fn tuple_column_from_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    elements: &[(Option<String>, ChType)],
    seq: &S,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let mut builder = TupleBuilder::new(py, name, elements, row_count, nullable);
    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every fallback revalidates; the strong reference keeps the row
        // alive across any Python code the row read runs.
        let value = unsafe { Bound::from_borrowed_ptr(py, seq.get(row)) };
        builder.push_row(&value, row)?;
        check_not_resized(seq, name, row_count)?;
    }
    builder.finish()
}

/// Streaming Tuple row builder: one strong-ref run per element, an optional
/// tuple-level null map, and a row-read mode decided by the first non-None
/// row (element-name dict reads for a fully named tuple whose first non-None
/// row is a dict, positional reads otherwise).
struct TupleBuilder<'a, 'py> {
    py: Python<'py>,
    name: &'a str,
    elements: &'a [(Option<String>, ChType)],
    flats: Vec<FlatRefs>,
    null_map: Option<Vec<u8>>,
    defaults: Option<Vec<Py<PyAny>>>,
    names: Option<Vec<Bound<'py, PyString>>>,
    dict_mode: Option<bool>,
    row_count: usize,
}

impl<'a, 'py> TupleBuilder<'a, 'py> {
    fn new(
        py: Python<'py>,
        name: &'a str,
        elements: &'a [(Option<String>, ChType)],
        row_count: usize,
        nullable: bool,
    ) -> Self {
        let names: Option<Vec<Bound<'py, PyString>>> =
            (!elements.is_empty() && elements.iter().all(|(n, _)| n.is_some())).then(|| {
                elements
                    .iter()
                    .map(|(n, _)| PyString::new(py, n.as_deref().unwrap_or_default()))
                    .collect()
            });
        Self {
            py,
            name,
            elements,
            flats: elements
                .iter()
                .map(|_| FlatRefs {
                    ptrs: Vec::with_capacity(row_count),
                })
                .collect(),
            null_map: nullable.then(|| Vec::with_capacity(row_count)),
            defaults: None,
            names,
            dict_mode: None,
            row_count,
        }
    }

    fn tuple_type(&self) -> ChType {
        ChType::Tuple(self.elements.to_vec())
    }

    fn push_row(&mut self, value: &Bound<'_, PyAny>, row: usize) -> PyResult<()> {
        if value.is_none() {
            if self.null_map.is_none() {
                return Err(PyValueError::new_err(format!(
                    "column {:?} row {row} is None but {} is not Nullable",
                    self.name,
                    self.tuple_type()
                )));
            }
            if self.defaults.is_none() {
                self.defaults = Some(
                    self.elements
                        .iter()
                        .map(|(_, element_type)| default_pyobject(self.py, element_type))
                        .collect::<PyResult<_>>()?,
                );
            }
            if let Some(null_map) = &mut self.null_map {
                null_map.push(1);
            }
            if let Some(defaults) = &self.defaults {
                for (flat, default) in self.flats.iter_mut().zip(defaults) {
                    flat.push_ref(default.bind(self.py));
                }
            }
            return Ok(());
        }
        if let Some(null_map) = &mut self.null_map {
            null_map.push(0);
        }
        let dict_mode = match self.dict_mode {
            Some(mode) => mode,
            None => {
                let mode = self.names.is_some() && value.is_instance_of::<PyDict>();
                self.dict_mode = Some(mode);
                mode
            }
        };
        if dict_mode {
            self.push_dict_row(value, row)
        } else {
            self.push_positional_row(value, row)
        }
    }

    /// Read one row by element name: missing keys become None, extra keys are
    /// ignored. Non-dict rows fall back to a `.get` method read.
    fn push_dict_row(&mut self, value: &Bound<'_, PyAny>, row: usize) -> PyResult<()> {
        let Some(names) = &self.names else {
            return Err(PyValueError::new_err("internal tuple row mode mismatch"));
        };
        if let Ok(dict) = value.downcast_exact::<PyDict>() {
            for (flat, key) in self.flats.iter_mut().zip(names) {
                match dict.get_item(key)? {
                    Some(item) => flat.push_ref(&item),
                    None => flat.push_none(self.py),
                }
            }
            return Ok(());
        }
        let get = value.getattr(intern!(self.py, "get")).map_err(|err| {
            let wrapped = PyValueError::new_err(format!(
                "column {:?} row {row} cannot be read as a dict for the named Tuple",
                self.name
            ));
            wrapped.set_cause(self.py, Some(err));
            wrapped
        })?;
        for (flat, key) in self.flats.iter_mut().zip(names) {
            let item = get.call1((key,)).map_err(|err| {
                let wrapped = PyValueError::new_err(format!(
                    "column {:?} row {row} element {key:?} cannot be read from the dict-like row",
                    self.name
                ));
                wrapped.set_cause(self.py, Some(err));
                wrapped
            })?;
            flat.push_ref(&item);
        }
        Ok(())
    }

    /// Read one row positionally. Exact list/tuple rows copy borrowed
    /// pointers; other iterables flatten through `list.extend`.
    fn push_positional_row(&mut self, value: &Bound<'_, PyAny>, row: usize) -> PyResult<()> {
        if let Ok(list) = value.downcast_exact::<PyList>() {
            // SAFETY: copying exact-list items runs no Python code.
            return unsafe { self.push_positional_seq(&ListSeq(list), row) };
        }
        if let Ok(tuple) = value.downcast_exact::<PyTuple>() {
            // SAFETY: copying exact-tuple items runs no Python code.
            return unsafe { self.push_positional_seq(&TupleSeq(tuple), row) };
        }
        if value.downcast::<PyString>().is_ok() {
            return Err(PyValueError::new_err(format!(
                "column {:?} row {row} is a str, not a Tuple row",
                self.name
            )));
        }
        if value.is_instance_of::<PyDict>() {
            return Err(PyValueError::new_err(format!(
                "column {:?} row {row} is a dict but Tuple rows are read positionally",
                self.name
            )));
        }
        if value.downcast::<PySet>().is_ok() || value.downcast::<PyFrozenSet>().is_ok() {
            return Err(PyValueError::new_err(format!(
                "column {:?} row {row} is an unordered set, which has no defined Tuple element order",
                self.name
            )));
        }
        let items = PyList::empty(self.py);
        items
            .call_method1(intern!(self.py, "extend"), (value,))
            .map_err(|err| {
                let wrapped = PyValueError::new_err(format!(
                    "column {:?} row {row} is not a valid Tuple row",
                    self.name
                ));
                wrapped.set_cause(self.py, Some(err));
                wrapped
            })?;
        // SAFETY: items is an owned exact list; copying runs no Python code.
        unsafe { self.push_positional_seq(&ListSeq(&items), row) }
    }

    /// Copy one arity-checked row into the element runs.
    ///
    /// # Safety
    ///
    /// Requires the GIL; `seq` must allow borrowed reads with no Python code
    /// running during the copy (an exact list or tuple).
    unsafe fn push_positional_seq<S: FastSeq>(&mut self, seq: &S, row: usize) -> PyResult<()> {
        let got = seq.size();
        if got != self.elements.len() {
            return Err(PyValueError::new_err(format!(
                "column {:?} row {row} has {got} elements but the Tuple declares {}",
                self.name,
                self.elements.len()
            )));
        }
        for (index, flat) in self.flats.iter_mut().enumerate() {
            let item = seq.get(index);
            ffi::Py_INCREF(item);
            flat.ptrs.push(item);
        }
        Ok(())
    }

    fn finish(self) -> PyResult<Column> {
        let mut fields = Vec::with_capacity(self.elements.len());
        for (index, ((element_name, element_type), flat)) in
            self.elements.iter().zip(&self.flats).enumerate()
        {
            let label = match element_name {
                Some(n) => format!("{n:?}"),
                None => index.to_string(),
            };
            let column = build_element_column(self.py, self.name, element_type, &flat.ptrs)
                .map_err(|err| remap_tuple_element_err(self.py, self.name, &label, err))?;
            fields.push(column);
        }
        Ok(Column::Tuple(match self.null_map {
            Some(nulls) => {
                TupleColumn::new_nullable(fields, self.row_count, Bitmap::from_ch_null_map(&nulls))
            }
            None => TupleColumn::new(fields, self.row_count),
        }))
    }
}

/// Build a `Map(K, V)` column: dict rows flatten into a key run and a value
/// run with an Arrow list offsets run; the two entry columns are built once
/// over their flat runs. Maps are never nullable at the map level, so a None
/// row is an error.
fn build_map_column(
    py: Python<'_>,
    name: &str,
    key_type: &ChType,
    value_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    if let Ok(list) = values.downcast_exact::<PyList>() {
        return map_column_from_seq(py, name, key_type, value_type, &ListSeq(list), row_count);
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return map_column_from_seq(py, name, key_type, value_type, &TupleSeq(tuple), row_count);
    }
    let mut builder = MapBuilder::new(py, name, key_type, value_type, row_count);
    for row in 0..row_count {
        let value = column_values.get_item(row)?;
        builder.push_row(&value, row)?;
    }
    builder.finish()
}

/// Map rows over an exact list or tuple of rows, or a flattened element run.
fn map_column_from_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    key_type: &ChType,
    value_type: &ChType,
    seq: &S,
    row_count: usize,
) -> PyResult<Column> {
    let mut builder = MapBuilder::new(py, name, key_type, value_type, row_count);
    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every fallback revalidates; the strong reference keeps the row
        // alive across any Python code the row read runs.
        let value = unsafe { Bound::from_borrowed_ptr(py, seq.get(row)) };
        builder.push_row(&value, row)?;
        check_not_resized(seq, name, row_count)?;
    }
    builder.finish()
}

/// Streaming Map row builder: an offsets run plus parallel key and value
/// strong-ref runs. Exact dict rows copy entries without running Python;
/// dict-like rows read through `.items()`.
struct MapBuilder<'a, 'py> {
    py: Python<'py>,
    name: &'a str,
    key_type: &'a ChType,
    value_type: &'a ChType,
    offsets: Vec<i64>,
    keys: FlatRefs,
    values: FlatRefs,
}

impl<'a, 'py> MapBuilder<'a, 'py> {
    fn new(
        py: Python<'py>,
        name: &'a str,
        key_type: &'a ChType,
        value_type: &'a ChType,
        row_count: usize,
    ) -> Self {
        let mut offsets = Vec::with_capacity(row_count + 1);
        offsets.push(0i64);
        Self {
            py,
            name,
            key_type,
            value_type,
            offsets,
            keys: FlatRefs::default(),
            values: FlatRefs::default(),
        }
    }

    fn push_row(&mut self, value: &Bound<'_, PyAny>, row: usize) -> PyResult<()> {
        if value.is_none() {
            return Err(PyValueError::new_err(format!(
                "column {:?} row {row} is None but Map({}, {}) is not Nullable",
                self.name, self.key_type, self.value_type
            )));
        }
        if let Ok(dict) = value.downcast_exact::<PyDict>() {
            for (key, val) in dict.iter() {
                self.keys.push_ref(&key);
                self.values.push_ref(&val);
            }
        } else {
            self.push_mapping_row(value, row)?;
        }
        self.offsets
            .push(self.keys.end_offset(self.name, "Map entry")?);
        Ok(())
    }

    /// Dict-like fallback: rows must expose `.items()`; anything else is not
    /// a Map row. Pair iterables are deliberately not accepted.
    fn push_mapping_row(&mut self, value: &Bound<'_, PyAny>, row: usize) -> PyResult<()> {
        let not_a_dict = |err: PyErr| {
            let wrapped = PyValueError::new_err(format!(
                "column {:?} row {row} is not a dict for Map({}, {})",
                self.name, self.key_type, self.value_type
            ));
            wrapped.set_cause(self.py, Some(err));
            wrapped
        };
        let items = value
            .call_method0(intern!(self.py, "items"))
            .map_err(not_a_dict)?;
        let entries = PyList::empty(self.py);
        entries
            .call_method1(intern!(self.py, "extend"), (items,))
            .map_err(not_a_dict)?;
        for index in 0..entries.len() {
            let entry = entries.get_item(index)?;
            let (key, val) = entry
                .extract::<(Bound<'_, PyAny>, Bound<'_, PyAny>)>()
                .map_err(|err| {
                    let wrapped = PyValueError::new_err(format!(
                        "column {:?} row {row} Map entry is not a key/value pair",
                        self.name
                    ));
                    wrapped.set_cause(self.py, Some(err));
                    wrapped
                })?;
            self.keys.push_ref(&key);
            self.values.push_ref(&val);
        }
        Ok(())
    }

    fn finish(self) -> PyResult<Column> {
        let total = self.keys.ptrs.len();
        let keys_column = build_element_column(self.py, self.name, self.key_type, &self.keys.ptrs)
            .map_err(|err| remap_flat_err(self.py, self.name, &self.offsets, "key", err))?;
        let values_column =
            build_element_column(self.py, self.name, self.value_type, &self.values.ptrs)
                .map_err(|err| remap_flat_err(self.py, self.name, &self.offsets, "value", err))?;
        let entries = Column::Tuple(TupleColumn::new(vec![keys_column, values_column], total));
        Ok(Column::Map(MapColumn::new(self.offsets, entries)))
    }
}

/// Build a Variant column in one row scan: one discriminator byte per logical
/// row and one strong-reference run per dense alternative. Each alternative is
/// then built once through the existing column fast paths.
fn build_variant_column(
    py: Python<'_>,
    name: &str,
    alternatives: &[ChType],
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    if let Ok(list) = values.downcast_exact::<PyList>() {
        return variant_column_from_seq(py, name, alternatives, &ListSeq(list), row_count);
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return variant_column_from_seq(py, name, alternatives, &TupleSeq(tuple), row_count);
    }

    let mut builder = VariantBuilder::new(py, name, alternatives, row_count)?;
    for row in 0..row_count {
        let value = column_values.get_item(row)?;
        builder.push_row(&value, row)?;
    }
    builder.finish()
}

/// Variant rows over an exact list/tuple or a flattened container element run.
fn variant_column_from_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    alternatives: &[ChType],
    seq: &S,
    row_count: usize,
) -> PyResult<Column> {
    let mut builder = VariantBuilder::new(py, name, alternatives, row_count)?;
    for row in 0..row_count {
        // SAFETY: row < row_count, which the caller checked against seq.size().
        // A strong reference protects the value while the builder retains its
        // selected payload in an alternative run.
        let value = unsafe { Bound::from_borrowed_ptr(py, seq.get(row)) };
        let ran_python = builder.push_row(&value, row)?;
        if ran_python {
            // Dispatch that ran Python (subclass attribute access or a
            // str-subclass name lookup) can resize the source. Drop the
            // temporary strong reference first so a finalizer cannot resize
            // it after this validation.
            drop(value);
            check_not_resized(seq, name, row_count)?;
        }
    }
    builder.finish()
}

/// The driver Variant instance's dispatch tables, resolved once per column.
struct VariantDispatch<'py> {
    /// Snapshot of `_python_map`: one strong type-object reference and its
    /// discriminator per unambiguous alternative.
    types: Vec<(Py<PyAny>, u8)>,
    name_index: Bound<'py, PyDict>,
    typed_variant_type: Bound<'py, PyAny>,
}

/// Per-column Variant dispatch state. The Python driver's Variant instance is
/// cached by canonical type name; snapshotting its `_python_map` and borrowing
/// its `_name_index` keeps this binding's exact-type inference and
/// `typed_variant` policy identical to the established Python encoder,
/// including collision removal.
struct VariantBuilder<'a, 'py> {
    py: Python<'py>,
    name: &'a str,
    alternatives: &'a [ChType],
    type_name: String,
    dispatch: VariantDispatch<'py>,
    discriminators: Vec<u8>,
    values: Vec<FlatRefs>,
    /// Explicit tag names already resolved through `_name_index`, with their
    /// discriminators. Rows reuse the same exact-str tag objects, so a pointer
    /// compare replaces the dict lookup. The strong references keep each
    /// cached name alive, so a cached pointer can never be a freed-and-reused
    /// address; exact-str-only entries mean the hit path runs no Python code.
    tag_cache: Vec<(Py<PyAny>, usize)>,
}

impl<'a, 'py> VariantBuilder<'a, 'py> {
    fn new(
        py: Python<'py>,
        name: &'a str,
        alternatives: &'a [ChType],
        row_count: usize,
    ) -> PyResult<Self> {
        let type_name = ChType::Variant(alternatives.to_vec()).to_string();
        // Any driver-interop failure becomes NotImplementedError so the
        // insert probe falls back to the Python codec.
        let dispatch =
            Self::driver_dispatch(py, &type_name, alternatives.len()).map_err(|err: PyErr| {
                PyNotImplementedError::new_err(format!(
                    "Variant type {type_name} is not supported by the native encoder: {err}"
                ))
            })?;
        Ok(Self {
            py,
            name,
            alternatives,
            type_name,
            dispatch,
            discriminators: Vec::with_capacity(row_count),
            values: alternatives.iter().map(|_| FlatRefs::default()).collect(),
            tag_cache: Vec::new(),
        })
    }

    /// Resolve the driver's cached Variant instance and snapshot its dispatch
    /// tables.
    fn driver_dispatch(
        py: Python<'py>,
        type_name: &str,
        alternative_count: usize,
    ) -> PyResult<VariantDispatch<'py>> {
        let py_variant = py
            .import("clickhouse_connect.datatypes.registry")?
            .getattr("get_from_name")?
            .call1((type_name,))?;
        let python_map = py_variant
            .getattr("_python_map")?
            .downcast_into::<PyDict>()?;
        let name_index = py_variant
            .getattr("_name_index")?
            .downcast_into::<PyDict>()?;
        let typed_variant_type = py
            .import("clickhouse_connect.datatypes.dynamic")?
            .getattr("TypedVariant")?;
        let mut types = Vec::with_capacity(python_map.len());
        for (key, value) in python_map.iter() {
            // SAFETY: value is a live dict value kept alive by its Bound.
            // PyLong_AsSize_t raises TypeError on a non-int without running
            // Python code.
            let index = unsafe { ffi::PyLong_AsSize_t(value.as_ptr()) };
            if index == usize::MAX && unsafe { !ffi::PyErr_Occurred().is_null() } {
                return Err(PyErr::fetch(py));
            }
            let discriminator = u8::try_from(index)
                .ok()
                .filter(|&d| usize::from(d) < alternative_count)
                .ok_or_else(|| PyValueError::new_err("Variant dispatch index is out of range"))?;
            types.push((key.unbind(), discriminator));
        }
        Ok(VariantDispatch {
            types,
            name_index,
            typed_variant_type,
        })
    }

    /// Select and retain one row. The boolean reports whether successful
    /// dispatch ran Python code, so borrowed-sequence callers only pay for a
    /// resize check when it did.
    fn push_row(&mut self, value: &Bound<'py, PyAny>, row: usize) -> PyResult<bool> {
        if value.is_none() {
            self.discriminators.push(u8::MAX);
            return Ok(false);
        }

        // SAFETY: value is a live Python object. Reading its type pointer and
        // comparing it against snapshot pointers cannot invoke user Python
        // code.
        let value_type = unsafe { ffi::Py_TYPE(value.as_ptr()) }.cast::<ffi::PyObject>();
        if value_type == self.dispatch.typed_variant_type.as_ptr()
            && unsafe { ffi::PyTuple_GET_SIZE(value.as_ptr()) } == 2
        {
            // Exact two-slot namedtuple layout: slots 0/1 are direct reads.
            // An exact instance with any other size falls through to the safe
            // attribute path below.
            let payload = unsafe {
                Bound::from_borrowed_ptr(self.py, ffi::PyTuple_GET_ITEM(value.as_ptr(), 0))
            };
            let explicit_name = unsafe {
                Bound::from_borrowed_ptr(self.py, ffi::PyTuple_GET_ITEM(value.as_ptr(), 1))
            };
            return self.push_explicit(&payload, &explicit_name, row);
        }

        if let Some(discriminator) =
            self.dispatch.types.iter().find_map(|(key, discriminator)| {
                (value_type == key.as_ptr()).then_some(*discriminator)
            })
        {
            self.push_selected(usize::from(discriminator), value)?;
            return Ok(false);
        }

        // A dispatch miss checks for a TypedVariant subtype before failing.
        let is_typed_variant = unsafe {
            ffi::PyObject_TypeCheck(
                value.as_ptr(),
                self.dispatch
                    .typed_variant_type
                    .as_ptr()
                    .cast::<ffi::PyTypeObject>(),
            ) != 0
        };
        if is_typed_variant {
            // A subclass (or a mis-sized exact instance) is not guaranteed to
            // have the base tuple's physical two-slot layout. Match Python's
            // isinstance policy through safe attribute reads; the outer
            // sequence path rechecks resize after this call because these
            // lookups may execute Python.
            let payload = value.getattr("value")?;
            let explicit_name = value.getattr("type_name")?;
            self.push_explicit(&payload, &explicit_name, row)?;
            return Ok(true);
        }

        Err(PyValueError::new_err(format!(
            "column {:?} row {row} cannot map Python type {} to any member of {}",
            self.name,
            python_type_name(value.as_ptr()),
            self.type_name
        )))
    }

    /// Push an explicitly-tagged row. The boolean reports whether the name
    /// lookup may have run Python code (a str-subclass name's `__hash__` and
    /// `__eq__` run during the dict lookup).
    fn push_explicit(
        &mut self,
        payload: &Bound<'py, PyAny>,
        explicit_name: &Bound<'py, PyAny>,
        row: usize,
    ) -> PyResult<bool> {
        if unsafe { ffi::PyUnicode_Check(explicit_name.as_ptr()) } == 0 {
            return Err(PyValueError::new_err(format!(
                "column {:?} row {row} typed Variant name must be a str",
                self.name
            )));
        }
        let ran_python = unsafe { ffi::PyUnicode_CheckExact(explicit_name.as_ptr()) } == 0;
        if !ran_python {
            if let Some(&(_, discriminator)) = self
                .tag_cache
                .iter()
                .find(|(cached, _)| cached.as_ptr() == explicit_name.as_ptr())
            {
                self.push_selected(discriminator, payload)?;
                return Ok(false);
            }
        }
        let discriminator =
            unsafe { dict_index(&self.dispatch.name_index, explicit_name.as_ptr())? }.ok_or_else(
                || {
                    let rendered = explicit_name
                        .extract::<String>()
                        .unwrap_or_else(|_| "<invalid>".to_string());
                    PyValueError::new_err(format!(
                        "column {:?} row {row} type {rendered:?} is not a member of {}",
                        self.name, self.type_name
                    ))
                },
            )?;
        // Bound so a column with many distinct exact-str tag objects cannot
        // grow an unbounded linear scan; misses fall back to the dict lookup.
        if !ran_python && self.tag_cache.len() < 16 {
            self.tag_cache
                .push((explicit_name.clone().unbind(), discriminator));
        }
        self.push_selected(discriminator, payload)?;
        Ok(ran_python)
    }

    fn push_selected(&mut self, discriminator: usize, payload: &Bound<'py, PyAny>) -> PyResult<()> {
        let discriminator = u8::try_from(discriminator).map_err(|_| {
            PyValueError::new_err("internal error: Variant discriminator exceeds UInt8 range")
        })?;
        if usize::from(discriminator) >= self.values.len() {
            return Err(PyValueError::new_err(
                "internal error: Variant dispatch index is out of range",
            ));
        }
        self.discriminators.push(discriminator);
        self.values[usize::from(discriminator)].push_ref(payload);
        Ok(())
    }

    fn finish(self) -> PyResult<Column> {
        let mut children = Vec::with_capacity(self.alternatives.len());
        for (alternative_index, (alternative, values)) in
            self.alternatives.iter().zip(&self.values).enumerate()
        {
            children.push(
                build_element_column(self.py, self.name, alternative, &values.ptrs).map_err(
                    |err| {
                        remap_variant_child_err(
                            self.py,
                            self.name,
                            &self.discriminators,
                            alternative_index,
                            err,
                        )
                    },
                )?,
            );
        }
        let variant = VariantColumn::try_new(&self.discriminators, children).map_err(|err| {
            PyValueError::new_err(format!(
                "column {:?} has an invalid Variant layout: {err}",
                self.name
            ))
        })?;
        Ok(Column::Variant(variant))
    }
}

/// Borrow a non-negative Python integer index from an exact dict lookup.
///
/// # Safety
///
/// `key` must be a live Python object and the GIL must be held.
unsafe fn dict_index(dict: &Bound<'_, PyDict>, key: *mut ffi::PyObject) -> PyResult<Option<usize>> {
    let value = ffi::PyDict_GetItemWithError(dict.as_ptr(), key);
    if value.is_null() {
        if ffi::PyErr_Occurred().is_null() {
            return Ok(None);
        }
        return Err(PyErr::fetch(dict.py()));
    }
    let index = ffi::PyLong_AsSize_t(value);
    if index == usize::MAX && !ffi::PyErr_Occurred().is_null() {
        return Err(PyErr::fetch(dict.py()));
    }
    Ok(Some(index))
}

/// Exact CPython type name without allocating or invoking Python code.
fn python_type_name(value: *mut ffi::PyObject) -> String {
    // SAFETY: value is live and tp_name is a NUL-terminated string owned by its
    // type for at least the lifetime of the object.
    unsafe { std::ffi::CStr::from_ptr((*ffi::Py_TYPE(value)).tp_name) }
        .to_string_lossy()
        .into_owned()
}

/// Default placeholder Python object for a null `Nullable(Tuple)` row's
/// element, matching the wire defaults the server writes for null rows.
fn default_pyobject(py: Python<'_>, ch_type: &ChType) -> PyResult<Py<PyAny>> {
    if let Some(delegate) = ch_type.physical_delegate() {
        return default_pyobject(py, &delegate);
    }
    Ok(match ch_type {
        // A Dynamic placeholder stays None; the String insert path renders it
        // as the literal "NULL".
        ChType::Nullable(_) | ChType::Variant(_) | ChType::Dynamic { .. } => py.None(),
        ChType::LowCardinality(inner) => default_pyobject(py, inner)?,
        ChType::Nothing => py.None(),
        ChType::Bool => PyBool::new(py, false).to_owned().into_any().unbind(),
        ChType::String | ChType::FixedString(_) => PyString::new(py, "").into_any().unbind(),
        ChType::Array(_) => PyList::empty(py).into_any().unbind(),
        ChType::Map(..) => PyDict::new(py).into_any().unbind(),
        ChType::Json { .. } => PyDict::new(py).into_any().unbind(),
        ChType::Tuple(elements) => {
            let defaults = elements
                .iter()
                .map(|(_, element_type)| default_pyobject(py, element_type))
                .collect::<PyResult<Vec<_>>>()?;
            PyTuple::new(py, defaults)?.into_any().unbind()
        }
        ChType::AggregateFunction { .. } => {
            return Err(PyNotImplementedError::new_err(
                "null Nullable(Tuple(...)) rows containing AggregateFunction are unsupported until the core provides a canonical placeholder state",
            ));
        }
        // Every remaining scalar accepts a raw 0 (epoch units, enum code,
        // integer UUID/IP forms, Decimal("0")).
        _ => 0i64.into_pyobject(py)?.into_any().unbind(),
    })
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
    if wide_int_layout(ch_type).is_some() {
        return wide_column_from_rows(py, name, ch_type, &column_values, row_count, false);
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

fn nullable_scalar_column<'py, R: RowAccess<'py>>(
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
        null_map.push(0);
        scalars.push(convert_scalar(py, inner, &value, name, row)?);
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
    // Resolve the physical dict value type: strip the SAF chain, unwrap an
    // optional Nullable, strip again. So LowCardinality(SAF(anyLast, String)) and
    // LowCardinality(SAF(anyLast, Nullable(String))) reach the String path.
    let (nullable, value_type) = low_cardinality_dict_value_type(inner);

    if !is_low_cardinality_inner(value_type) {
        return Err(PyNotImplementedError::new_err(format!(
            "unsupported LowCardinality inner type {value_type} for column {name:?}"
        )));
    }

    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    if wide_int_layout(value_type).is_some() {
        return lc_wide_column(py, name, value_type, &column_values, row_count, nullable);
    }
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
    // Time64 is rejected by is_low_cardinality_inner, so only Time probes.
    let mut time_probe = match value_type {
        ChType::Time => TimeScalarProbe::new(value_type),
        _ => None,
    };

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

        let time_hit = match time_probe.as_mut() {
            Some(probe) => probe.probe(&value, name, row)?,
            None => None,
        };
        if let Some(TimeProbe::Nat) = time_hit {
            if !nullable {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is NaT but LowCardinality({value_type}) is not nullable"
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
        let scalar = match time_hit {
            Some(TimeProbe::Ticks(ticks)) => time_ticks_scalar(value_type, ticks, name, row)?,
            _ => convert_scalar(py, value_type, &value, name, row)?,
        };
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

/// Build LowCardinality over a wide integer without a heap allocation per
/// input cell. Conversion writes into a fixed stack buffer; only distinct
/// dictionary entries are retained, and the final dictionary data is copied
/// once into the core's contiguous fixed-binary representation.
fn lc_wide_column<'py, R: RowAccess<'py>>(
    py: Python<'py>,
    name: &str,
    value_type: &ChType,
    rows: &R,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let (width, signed, type_name) = wide_int_layout(value_type)
        .ok_or_else(|| PyValueError::new_err("internal wide integer type mismatch"))?;
    let mut indices = Vec::with_capacity(row_count);
    let mut dict_values = Vec::<[u8; 32]>::new();
    let mut slots = HashMap::<[u8; 32], i32>::with_capacity(row_count.min(1024));
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));

    if nullable && row_count > 0 {
        dict_values.push([0u8; 32]);
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

        let mut bytes = [0u8; 32];
        wide_int_into(
            py,
            &value,
            &mut bytes[..width],
            signed,
            name,
            row,
            type_name,
        )?;
        let slot = match slots.entry(bytes) {
            Entry::Occupied(slot) => *slot.get(),
            Entry::Vacant(vacant) => {
                let slot = i32::try_from(dict_values.len()).map_err(|_| {
                    PyValueError::new_err(format!(
                        "column {name:?} LowCardinality dictionary exceeds i32 index capacity"
                    ))
                })?;
                dict_values.push(bytes);
                *vacant.insert(slot)
            }
        };
        indices.push(slot);
    }

    let byte_len = width.checked_mul(dict_values.len()).ok_or_else(|| {
        PyValueError::new_err(format!(
            "column {name:?} wide integer dictionary byte size exceeds usize capacity"
        ))
    })?;
    let mut data = Vec::with_capacity(byte_len);
    for value in dict_values {
        data.extend_from_slice(&value[..width]);
    }
    let dict_column = finish_wide_int_column(value_type, data, None)?;
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
struct ListRows<'a, 'py> {
    py: Python<'py>,
    list: &'a Bound<'py, PyList>,
    name: &'a str,
    expected: usize,
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
struct TupleRows<'a, 'py> {
    py: Python<'py>,
    tuple: &'a Bound<'py, PyTuple>,
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
fn checked_f64_to_bfloat16(value: f64) -> Result<[u8; 2], ()> {
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
    let Ok(buffer) = PyBuffer::<T>::get(values) else {
        return None;
    };
    let (order, code) = match *buffer.format().to_bytes() {
        [code] => (b'@', code),
        [order, code] => (order, code),
        _ => return None,
    };
    let native_order = match order {
        b'@' => true,
        b'<' | b'=' => cfg!(target_endian = "little"),
        b'>' | b'!' => cfg!(target_endian = "big"),
        _ => false,
    };
    if !native_order || code == b'c' {
        return None;
    }
    if buffer.dimensions() != 1 || buffer.item_count() != row_count {
        return None;
    }
    Some(buffer)
}

fn buffer_values<T: Element>(
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
enum TimeProbe {
    Nat,
    Ticks(i64),
}

/// Per-column state for probing Time/Time64 cells that are not one of the
/// exact fast types. The parsed dtype and unit ratio are cached and reused
/// while subsequent cells carry an identical or equal dtype object.
struct TimeScalarProbe {
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
    fn new(ch_type: &ChType) -> Option<Self> {
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
    fn probe(
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
fn time_ticks_scalar(ch_type: &ChType, ticks: i64, column: &str, row: usize) -> PyResult<Scalar> {
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
fn try_numpy_timedelta_column(
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

/// Narrowing i64 conversion for the fast paths. A generic helper so macro
/// expansions do not trip clippy's fallible-conversion lint when the target
/// is i64 itself.
#[inline]
fn narrow_i64<T: TryFrom<i64>>(value: i64) -> Result<T, ()> {
    T::try_from(value).map_err(|_| ())
}

#[inline]
fn time64_scale(precision: u8) -> i64 {
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
fn max_time64_ticks(precision: u8) -> i64 {
    let scale = time64_scale(precision);
    MAX_TIME_SECONDS * scale + (scale - 1)
}

/// Reinterprets a `Vec` of a `#[repr(transparent)]` wrapper as its inner type,
/// or the reverse, without copying.
///
/// # Safety
/// Every `T` bit pattern must be a valid `U`.
unsafe fn cast_vec<T, U>(values: Vec<T>) -> Vec<U> {
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
        struct $name($prim);

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
struct IntervalVal(i64);

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
                    if let Ok(v) = unsafe { <u32 as FastValue>::from_exact(slot, ch_type, 0) } {
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
        if let Ok(v) = unsafe { <u32 as FastValue>::from_exact(ptr, ch_type, 0) } {
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
    WideInt(Vec<u8>),
    Float32(f32),
    Float64(f64),
    BFloat16([u8; 2]),
    Date(u16),
    Date32(i32),
    DateTime(u32),
    DateTime64(i64),
    Time(i32),
    Time64(i64),
    Interval(i64),
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
    WideInt(Vec<u8>),
    Float32(u32),
    Float64(u64),
    BFloat16([u8; 2]),
    Date(u16),
    Date32(i32),
    DateTime(u32),
    DateTime64(i64),
    Time(i32),
    Time64(i64),
    Interval(i64),
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
            Scalar::WideInt(v) => ScalarKey::WideInt(v.clone()),
            Scalar::Float32(v) => ScalarKey::Float32(v.to_bits()),
            Scalar::Float64(v) => ScalarKey::Float64(v.to_bits()),
            Scalar::BFloat16(v) => ScalarKey::BFloat16(*v),
            Scalar::Date(v) => ScalarKey::Date(*v),
            Scalar::Date32(v) => ScalarKey::Date32(*v),
            Scalar::DateTime(v) => ScalarKey::DateTime(*v),
            Scalar::DateTime64(v) => ScalarKey::DateTime64(*v),
            Scalar::Time(v) => ScalarKey::Time(*v),
            Scalar::Time64(v) => ScalarKey::Time64(*v),
            Scalar::Interval(v) => ScalarKey::Interval(*v),
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
        ChType::Int128 | ChType::UInt128 | ChType::Int256 | ChType::UInt256 => {
            build_wide_int_column(ch_type, scalars, validity)
        }
        ChType::Float32 => primitive_column!(scalars, validity, Float32, Float32, f32),
        ChType::Float64 => primitive_column!(scalars, validity, Float64, Float64, f64),
        ChType::BFloat16 => {
            primitive_column!(scalars, validity, BFloat16, BFloat16, [u8; 2])
        }
        ChType::AggregateFunction { .. } => Err(PyNotImplementedError::new_err(
            "AggregateFunction insert conversion is not implemented",
        )),
        ChType::Nothing => Err(PyRuntimeError::new_err(
            "internal error: Nothing columns use the length-only builder",
        )),
        ChType::Date => primitive_column!(scalars, validity, Date, Date, u16),
        ChType::Date32 => primitive_column!(scalars, validity, Date32, Date32, i32),
        ChType::DateTime { .. } => {
            primitive_column!(scalars, validity, DateTime, DateTime, u32)
        }
        ChType::DateTime64 { .. } => {
            primitive_column!(scalars, validity, DateTime64, DateTime64, i64)
        }
        ChType::Time => primitive_column!(scalars, validity, Time, Time, i32),
        ChType::Time64 { .. } => primitive_column!(scalars, validity, Time64, Time64, i64),
        ChType::Interval(_) => primitive_column!(scalars, validity, Interval, Interval, i64),
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
        ChType::Tuple(_) | ChType::Map(..) | ChType::Variant(_) => {
            Err(PyNotImplementedError::new_err(
                "Tuple, Map, and Variant columns are built by their container paths, not the scalar path",
            ))
        }
        ChType::Nullable(_) | ChType::LowCardinality(_) => Err(PyNotImplementedError::new_err(
            "nested wrapper conversion is not supported",
        )),
        ChType::SimpleAggregateFunction { .. }
        | ChType::Geo(_)
        | ChType::Geometry
        | ChType::Nested(_) => Err(PyNotImplementedError::new_err(
            "name-decoration aliases are expanded to their physical type before the scalar path",
        )),
        ChType::Dynamic { .. } => Err(PyNotImplementedError::new_err(
            "Dynamic columns are built by the String insert path, not the scalar path",
        )),
        ChType::Json { .. } => Err(PyNotImplementedError::new_err(
            "JSON columns are built by the JSON text insert path, not the scalar path",
        )),
    }
}

fn build_wide_int_column(
    ch_type: &ChType,
    scalars: Vec<Scalar>,
    validity: Option<Bitmap>,
) -> PyResult<Column> {
    let (width, _, _) = wide_int_layout(ch_type)
        .ok_or_else(|| PyValueError::new_err("internal wide integer type mismatch"))?;
    let byte_len = width
        .checked_mul(scalars.len())
        .ok_or_else(|| PyValueError::new_err("wide integer column byte size overflow"))?;
    let mut data = Vec::with_capacity(byte_len);
    for scalar in scalars {
        match scalar {
            Scalar::WideInt(value) if value.len() == width => data.extend_from_slice(&value),
            _ => return Err(PyValueError::new_err("internal scalar type mismatch")),
        }
    }
    finish_wide_int_column(ch_type, data, validity)
}

fn wide_int_layout(ch_type: &ChType) -> Option<(usize, bool, &'static str)> {
    match ch_type {
        ChType::Int128 => Some((16, true, "Int128")),
        ChType::UInt128 => Some((16, false, "UInt128")),
        ChType::Int256 => Some((32, true, "Int256")),
        ChType::UInt256 => Some((32, false, "UInt256")),
        _ => None,
    }
}

fn wide_data_buffer(name: &str, width: usize, row_count: usize) -> PyResult<Vec<u8>> {
    let byte_len = width.checked_mul(row_count).ok_or_else(|| {
        PyValueError::new_err(format!(
            "column {name:?} wide integer byte size exceeds usize capacity"
        ))
    })?;
    Ok(vec![0u8; byte_len])
}

fn finish_wide_int_column(
    ch_type: &ChType,
    data: Vec<u8>,
    validity: Option<Bitmap>,
) -> PyResult<Column> {
    let (width, _, _) = wide_int_layout(ch_type)
        .ok_or_else(|| PyValueError::new_err("internal wide integer type mismatch"))?;
    let column = match validity {
        Some(validity) => FixedBinaryColumn::new_nullable(data, width, validity),
        None => FixedBinaryColumn::new(data, width),
    };
    Ok(match ch_type {
        ChType::Int128 => Column::Int128(column),
        ChType::UInt128 => Column::UInt128(column),
        ChType::Int256 => Column::Int256(column),
        ChType::UInt256 => Column::UInt256(column),
        _ => return Err(PyValueError::new_err("internal wide integer type mismatch")),
    })
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
        ChType::Int128 => {
            wide_int_bytes(py, value, 16, true, column, row, "Int128").map(Scalar::WideInt)
        }
        ChType::UInt128 => {
            wide_int_bytes(py, value, 16, false, column, row, "UInt128").map(Scalar::WideInt)
        }
        ChType::Int256 => {
            wide_int_bytes(py, value, 32, true, column, row, "Int256").map(Scalar::WideInt)
        }
        ChType::UInt256 => {
            wide_int_bytes(py, value, 32, false, column, row, "UInt256").map(Scalar::WideInt)
        }
        ChType::Float32 => value
            .extract::<f32>()
            .map(Scalar::Float32)
            .map_err(|_| conversion_error(column, row, "Float32")),
        ChType::Float64 => value
            .extract::<f64>()
            .map(Scalar::Float64)
            .map_err(|_| conversion_error(column, row, "Float64")),
        ChType::BFloat16 => value
            .extract::<f64>()
            .map_err(|_| conversion_error(column, row, "BFloat16"))
            .and_then(|value| {
                checked_f64_to_bfloat16(value)
                    .map(Scalar::BFloat16)
                    .map_err(|_| conversion_error(column, row, "BFloat16"))
            }),
        ChType::AggregateFunction { .. } => Err(PyNotImplementedError::new_err(
            "AggregateFunction insert conversion is not implemented",
        )),
        ChType::Nothing => Err(PyRuntimeError::new_err(
            "internal error: Nothing columns use the length-only builder",
        )),
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
        ChType::Time => Ok(Scalar::Time(time_ticks(value, column, row)?)),
        ChType::Time64 { precision } => Ok(Scalar::Time64(time64_ticks(
            value, *precision, column, row,
        )?)),
        ChType::Interval(_) => value
            .extract::<i64>()
            .map(Scalar::Interval)
            .map_err(|_| conversion_error(column, row, &ch_type.to_string())),
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
        ChType::Tuple(_) | ChType::Map(..) | ChType::Variant(_) => {
            Err(PyNotImplementedError::new_err(
                "Tuple, Map, and Variant columns are built by their container paths, not the scalar path",
            ))
        }
        ChType::Nullable(_) | ChType::LowCardinality(_) => Err(PyNotImplementedError::new_err(
            "nested wrapper conversion is not supported",
        )),
        ChType::SimpleAggregateFunction { .. }
        | ChType::Geo(_)
        | ChType::Geometry
        | ChType::Nested(_) => {
            Err(PyNotImplementedError::new_err(
                "name-decoration aliases are expanded to their physical type before the scalar path",
            ))
        }
        ChType::Dynamic { .. } => Err(PyNotImplementedError::new_err(
            "Dynamic columns are built by the String insert path, not the scalar path",
        )),
        ChType::Json { .. } => Err(PyNotImplementedError::new_err(
            "JSON columns are built by the JSON text insert path, not the scalar path",
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
        ChType::Int128 | ChType::UInt128 => Ok(Scalar::WideInt(vec![0; 16])),
        ChType::Int256 | ChType::UInt256 => Ok(Scalar::WideInt(vec![0; 32])),
        ChType::Float32 => Ok(Scalar::Float32(0.0)),
        ChType::Float64 => Ok(Scalar::Float64(0.0)),
        ChType::BFloat16 => Ok(Scalar::BFloat16([0; 2])),
        ChType::AggregateFunction { .. } => Err(PyNotImplementedError::new_err(
            "AggregateFunction has no generic default state; provide exact serialized state bytes",
        )),
        ChType::Nothing => Err(PyRuntimeError::new_err(
            "internal error: Nothing columns use the length-only builder",
        )),
        ChType::String => Ok(Scalar::Bytes(Vec::new())),
        ChType::FixedString(width) => Ok(Scalar::Bytes(vec![0; *width])),
        ChType::Date => Ok(Scalar::Date(0)),
        ChType::Date32 => Ok(Scalar::Date32(0)),
        ChType::DateTime { .. } => Ok(Scalar::DateTime(0)),
        ChType::DateTime64 { .. } => Ok(Scalar::DateTime64(0)),
        ChType::Time => Ok(Scalar::Time(0)),
        ChType::Time64 { .. } => Ok(Scalar::Time64(0)),
        ChType::Interval(_) => Ok(Scalar::Interval(0)),
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
        ChType::Tuple(_) | ChType::Map(..) | ChType::Variant(_) => {
            Err(PyNotImplementedError::new_err(
                "Tuple, Map, and Variant columns have no scalar placeholder",
            ))
        }
        ChType::Nullable(_) | ChType::LowCardinality(_) => Err(PyNotImplementedError::new_err(
            "nested wrapper conversion is not supported",
        )),
        ChType::SimpleAggregateFunction { .. }
        | ChType::Geo(_)
        | ChType::Geometry
        | ChType::Nested(_) => Err(PyNotImplementedError::new_err(
            "name-decoration aliases are expanded to their physical type before the scalar path",
        )),
        ChType::Dynamic { .. } => Err(PyNotImplementedError::new_err(
            "Dynamic columns are built by the String insert path, not the scalar path",
        )),
        ChType::Json { .. } => Err(PyNotImplementedError::new_err(
            "JSON columns are built by the JSON text insert path, not the scalar path",
        )),
    }
}

fn conversion_error(column: &str, row: usize, type_name: &str) -> PyErr {
    PyValueError::new_err(format!(
        "column {column:?} row {row} cannot be converted to {type_name}"
    ))
}

/// Convert a Python integer/index object or numeric string to an owned
/// fixed-width Native representation. LowCardinality retains these bytes per
/// distinct dictionary value; ordinary columns use `wide_int_into` directly.
fn wide_int_bytes(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    width: usize,
    signed: bool,
    column: &str,
    row: usize,
    type_name: &str,
) -> PyResult<Vec<u8>> {
    let mut bytes = vec![0u8; width];
    wide_int_into(py, value, &mut bytes, signed, column, row, type_name)?;
    Ok(bytes)
}

/// Outcome of the exact-int i64 probe.
#[derive(Clone, Copy, PartialEq, Eq)]
enum WideFast {
    /// Bytes written; conversion complete.
    Done,
    /// Exact int beyond i64; the slow path converts it directly.
    WideInt,
    /// Not an exact int; the slow path routes through index/`int()`.
    NotInt,
}

/// Try the exact-int fast path: an exact `int` within i64 writes its
/// sign-extended little-endian bytes directly (the target width is always 16
/// or 32, so any i64 fits). A miss touches nothing and reports which slow
/// path applies; a negative value for an unsigned target is the standard
/// conversion error, matching the full conversion.
///
/// # Safety
///
/// Requires the GIL; `ptr` must be a valid, non-null object pointer. Never
/// executes Python code.
#[inline]
unsafe fn wide_int_fast_into(
    ptr: *mut ffi::PyObject,
    bytes: &mut [u8],
    signed: bool,
    column: &str,
    row: usize,
    type_name: &str,
) -> PyResult<WideFast> {
    debug_assert!(bytes.len() >= 8);
    if ffi::PyLong_CheckExact(ptr) == 0 {
        return Ok(WideFast::NotInt);
    }
    let mut overflow: c_int = 0;
    let value = ffi::PyLong_AsLongLongAndOverflow(ptr, &mut overflow);
    if overflow != 0 {
        return Ok(WideFast::WideInt);
    }
    // An exact int should never error here; the no-error contract is
    // implementation behavior, so clear defensively and fall back.
    if value == -1 && !ffi::PyErr_Occurred().is_null() {
        ffi::PyErr_Clear();
        return Ok(WideFast::WideInt);
    }
    if !signed && value < 0 {
        return Err(conversion_error(column, row, type_name));
    }
    bytes[..8].copy_from_slice(&value.to_le_bytes());
    bytes[8..].fill(if value < 0 { 0xff } else { 0 });
    Ok(WideFast::Done)
}

/// Write one Python integer/index object or numeric string into its final
/// fixed-width little-endian slice. Non-string values use the index protocol,
/// so floats remain errors rather than being truncated; strings use Python's
/// `int` conversion to preserve the binding's existing BigInt behavior.
fn wide_int_into(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    bytes: &mut [u8],
    signed: bool,
    column: &str,
    row: usize,
    type_name: &str,
) -> PyResult<()> {
    // SAFETY: GIL held via `py`; `value` is a valid object.
    match unsafe { wide_int_fast_into(value.as_ptr(), bytes, signed, column, row, type_name)? } {
        WideFast::Done => Ok(()),
        outcome => wide_int_slow_into(
            py,
            value,
            outcome == WideFast::WideInt,
            bytes,
            signed,
            column,
            row,
            type_name,
        ),
    }
}

/// Slow-path completion after a fast-probe miss; `is_exact_int` carries the
/// probe's type check so it is not repeated. An exact int beyond i64 is
/// already its own index result; anything else routes through the index
/// protocol or `int()`.
#[allow(clippy::too_many_arguments)]
fn wide_int_slow_into(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    is_exact_int: bool,
    bytes: &mut [u8],
    signed: bool,
    column: &str,
    row: usize,
    type_name: &str,
) -> PyResult<()> {
    let owned;
    let integer_ptr = if is_exact_int {
        value.as_ptr()
    } else {
        let converted = unsafe {
            if value.downcast::<PyString>().is_ok() {
                ffi::PyNumber_Long(value.as_ptr())
            } else {
                ffi::PyNumber_Index(value.as_ptr())
            }
        };
        owned = unsafe { Bound::from_owned_ptr_or_err(py, converted) }
            .map_err(|_| conversion_error(column, row, type_name))?;
        owned.as_ptr()
    };
    #[cfg(not(Py_3_13))]
    let result = unsafe {
        ffi::_PyLong_AsByteArray(
            integer_ptr.cast(),
            bytes.as_mut_ptr(),
            bytes.len(),
            1,
            i32::from(signed),
        )
    };
    #[cfg(not(Py_3_13))]
    if result < 0 {
        // SAFETY: GIL held; discards the pending OverflowError.
        unsafe { ffi::PyErr_Clear() };
        return Err(conversion_error(column, row, type_name));
    }
    #[cfg(Py_3_13)]
    {
        let mut flags = ffi::Py_ASNATIVEBYTES_LITTLE_ENDIAN;
        if !signed {
            flags |= ffi::Py_ASNATIVEBYTES_UNSIGNED_BUFFER | ffi::Py_ASNATIVEBYTES_REJECT_NEGATIVE;
        }
        let required = unsafe {
            ffi::PyLong_AsNativeBytes(
                integer_ptr,
                bytes.as_mut_ptr().cast(),
                bytes.len() as ffi::Py_ssize_t,
                flags,
            )
        };
        if required < 0 {
            // SAFETY: GIL held; discards the pending ValueError.
            unsafe { ffi::PyErr_Clear() };
            return Err(conversion_error(column, row, type_name));
        }
        if required as usize > bytes.len() {
            return Err(conversion_error(column, row, type_name));
        }
    }
    Ok(())
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

fn time_ticks(value: &Bound<'_, PyAny>, column: &str, row: usize) -> PyResult<i32> {
    let ticks = time_like_ticks(value, 0, false, column, row, "Time")?;
    i32::try_from(ticks).map_err(|_| time_range_error(column, row, "Time", ticks))
}

fn time64_ticks(
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

// Insert-supported subset of core's LowCardinality inner-type allowlist, not
// a copy of it; extend only as insert support for a type lands.
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
            | ChType::Int128
            | ChType::UInt128
            | ChType::Int256
            | ChType::UInt256
            | ChType::Float32
            | ChType::Float64
            | ChType::BFloat16
            | ChType::String
            | ChType::FixedString(_)
            | ChType::Date
            | ChType::Date32
            | ChType::DateTime { .. }
            | ChType::Time
            | ChType::Interval(_)
            | ChType::Uuid
            | ChType::Ipv4
            | ChType::Ipv6
    )
}
