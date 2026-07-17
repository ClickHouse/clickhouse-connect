use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ffi::{c_int, c_long};
use std::io::Write as _;
use std::net::{IpAddr, Ipv4Addr};

use pyo3::buffer::{Element, PyBuffer};
use pyo3::exceptions::{PyMemoryError, PyNotImplementedError, PyRuntimeError, PyValueError};
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
    FixedBinaryColumn, JsonColumn, MapColumn, NothingColumn, PrimitiveColumn, QBitColumn,
    TupleColumn, Utf8Column, VariantColumn,
};
use ch_core_rs::native::decode::{low_cardinality_dict_value_type, parse_ch_type};
use ch_core_rs::native::encode::{encode_block, EncodeError, EncodeOptions};
use ch_core_rs::schema::{ChType, Field, QBitElementType, Schema};

use crate::decoder::buffer_to_vec;

mod containers;
mod fastpath;
mod json;
mod qbit;
mod scalar;
mod special;
mod temporal;
mod variant;

use containers::*;
use fastpath::*;
use json::*;
use qbit::*;
use scalar::*;
use special::*;
use temporal::*;
use variant::*;

const EPOCH_DATE_ORDINAL: i64 = 719_163;
const IPV4_V6_PREFIX: [u8; 12] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff];
const MAX_TIME_SECONDS: i64 = 999 * 3_600 + 59 * 60 + 59;

/// Match a typed, native-endian PEP 3118 buffer with exactly the requested
/// shape. PyO3 0.23 accepts opposite-endian primitive formats and maps `c`
/// to a one-byte unsigned integer, so both cases need explicit rejection.
fn matching_native_buffer<T: Element>(
    value: &Bound<'_, PyAny>,
    expected_shape: &[usize],
) -> Option<PyBuffer<T>> {
    let Ok(buffer) = PyBuffer::<T>::get(value) else {
        return None;
    };
    let (order, code) = match *buffer.format().to_bytes() {
        [code] => (b'@', code),
        [order, code] => (order, code),
        _ => return None,
    };
    let native_order = match order {
        b'@' | b'=' => true,
        b'<' => cfg!(target_endian = "little"),
        b'>' | b'!' => cfg!(target_endian = "big"),
        _ => false,
    };
    if !native_order || code == b'c' {
        return None;
    }
    if buffer.dimensions() != expected_shape.len() || buffer.shape() != expected_shape {
        return None;
    }
    Some(buffer)
}

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
        ChType::QBit {
            element_type,
            dimension,
        } => build_qbit_column(
            py,
            name,
            *element_type,
            *dimension,
            values,
            row_count,
            false,
        ),
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
