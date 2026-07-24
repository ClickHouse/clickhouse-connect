use super::*;

/// Build a Dynamic column as its String insert form: each value stringifies
/// with `str()` and None becomes the literal "NULL", matching the python
/// codec's `write_str_values`.
pub(super) fn build_dynamic_string_column(
    name: &str,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    dynamic_string_column(&column_values, row_count)
}

pub(super) fn dynamic_string_column<'py, R: RowAccess<'py>>(
    rows: &R,
    row_count: usize,
) -> PyResult<Column> {
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
pub(super) fn build_json_text_column(
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

pub(super) fn json_text_column_from_rows<'py, R: RowAccess<'py>>(
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
