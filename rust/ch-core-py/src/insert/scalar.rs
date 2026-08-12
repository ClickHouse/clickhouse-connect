use pyo3::sync::GILOnceCell;
use pyo3::types::PyType;

use super::*;

#[derive(Debug)]
pub(super) enum Scalar {
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
pub(super) enum ScalarKey {
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
    pub(super) fn key(&self) -> ScalarKey {
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

pub(super) fn column_from_scalars(
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
        ChType::QBit { .. } => Err(PyNotImplementedError::new_err(format!(
            "unsupported ClickHouse type {ch_type}"
        ))),
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

pub(super) fn wide_int_layout(ch_type: &ChType) -> Option<(usize, bool, &'static str)> {
    match ch_type {
        ChType::Int128 => Some((16, true, "Int128")),
        ChType::UInt128 => Some((16, false, "UInt128")),
        ChType::Int256 => Some((32, true, "Int256")),
        ChType::UInt256 => Some((32, false, "UInt256")),
        _ => None,
    }
}

pub(super) fn wide_data_buffer(name: &str, width: usize, row_count: usize) -> PyResult<Vec<u8>> {
    let byte_len = width.checked_mul(row_count).ok_or_else(|| {
        PyValueError::new_err(format!(
            "column {name:?} wide integer byte size exceeds usize capacity"
        ))
    })?;
    Ok(vec![0u8; byte_len])
}

pub(super) fn finish_wide_int_column(
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

pub(super) fn convert_scalar(
    py: Python<'_>,
    ch_type: &ChType,
    value: &Bound<'_, PyAny>,
    column: &str,
    row: usize,
) -> PyResult<Scalar> {
    macro_rules! integer_scalar {
        ($ty:ty, $variant:ident, $type_name:literal) => {{
            let integer = integer_object(
                py,
                value,
                column,
                row,
                $type_name,
                "pass an integer value",
                false,
            )?;
            integer
                .extract::<$ty>()
                .map(Scalar::$variant)
                .map_err(|_| integer_range_error(value, column, row, $type_name))
        }};
    }

    match ch_type {
        ChType::Bool => value
            .extract::<bool>()
            .map(Scalar::Bool)
            .map_err(|_| conversion_error(column, row, "Bool")),
        ChType::Int8 => integer_scalar!(i8, Int8, "Int8"),
        ChType::Int16 => integer_scalar!(i16, Int16, "Int16"),
        ChType::Int32 => integer_scalar!(i32, Int32, "Int32"),
        ChType::Int64 => integer_scalar!(i64, Int64, "Int64"),
        ChType::UInt8 => integer_scalar!(u8, UInt8, "UInt8"),
        ChType::UInt16 => integer_scalar!(u16, UInt16, "UInt16"),
        ChType::UInt32 => integer_scalar!(u32, UInt32, "UInt32"),
        ChType::UInt64 => integer_scalar!(u64, UInt64, "UInt64"),
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
            .map_err(|_| float_conversion_error(value, column, row, "Float32")),
        ChType::Float64 => value
            .extract::<f64>()
            .map(Scalar::Float64)
            .map_err(|_| float_conversion_error(value, column, row, "Float64")),
        ChType::BFloat16 => value
            .extract::<f64>()
            .map_err(|_| float_conversion_error(value, column, row, "BFloat16"))
            .and_then(|value| {
                checked_f64_to_bfloat16(value)
                    .map(Scalar::BFloat16)
                    .map_err(|_| conversion_error(column, row, "BFloat16"))
            }),
        ChType::QBit { .. } => Err(PyNotImplementedError::new_err(format!(
            "unsupported ClickHouse type {ch_type} for column {column:?}"
        ))),
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

pub(super) fn default_scalar(ch_type: &ChType) -> PyResult<Scalar> {
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
        ChType::QBit { .. } => Err(PyNotImplementedError::new_err(format!(
            "unsupported ClickHouse type {ch_type}"
        ))),
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
        ChType::Decimal { precision, .. } => Ok(Scalar::Bytes(vec![0; decimal_width(*precision)?])),
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

pub(super) fn conversion_error(column: &str, row: usize, type_name: &str) -> PyErr {
    PyValueError::new_err(format!(
        "column {column:?} row {row} cannot be converted to {type_name}"
    ))
}

const VALUE_REPR_MAX_CHARS: usize = 100;

fn value_repr(value: &Bound<'_, PyAny>) -> String {
    let repr = value
        .repr()
        .and_then(|repr| repr.to_str().map(str::to_owned))
        .unwrap_or_else(|_| {
            let type_name = value
                .get_type()
                .name()
                .ok()
                .and_then(|name| name.to_str().ok().map(str::to_owned))
                .unwrap_or_else(|| "value".to_string());
            format!("<{type_name}>")
        });
    match repr.char_indices().nth(VALUE_REPR_MAX_CHARS) {
        Some((cut, _)) => format!("{}...", &repr[..cut]),
        None => repr,
    }
}

fn conversion_error_detail(
    value: &Bound<'_, PyAny>,
    column: &str,
    row: usize,
    type_name: &str,
    detail: &str,
) -> PyErr {
    PyValueError::new_err(format!(
        "column {column:?} row {row} cannot be converted to {type_name}: {} {detail}",
        value_repr(value)
    ))
}

fn float_conversion_error(
    value: &Bound<'_, PyAny>,
    column: &str,
    row: usize,
    type_name: &str,
) -> PyErr {
    if value.downcast::<PyString>().is_ok() {
        return conversion_error_detail(
            value,
            column,
            row,
            type_name,
            "strings are not accepted; pass a float instead",
        );
    }
    conversion_error(column, row, type_name)
}

fn integer_range_error(
    value: &Bound<'_, PyAny>,
    column: &str,
    row: usize,
    type_name: &str,
) -> PyErr {
    conversion_error_detail(
        value,
        column,
        row,
        type_name,
        "is outside the target range; use a value within the column's range",
    )
}

static DECIMAL_TYPE: GILOnceCell<Py<PyType>> = GILOnceCell::new();

/// isinstance check against decimal.Decimal (imported once), so Decimal
/// subclasses are accepted like the python codec accepts them.
fn is_decimal(value: &Bound<'_, PyAny>) -> bool {
    let py = value.py();
    DECIMAL_TYPE
        .get_or_try_init(py, || {
            py.import("decimal")?
                .getattr("Decimal")?
                .downcast_into::<PyType>()
                .map(Bound::unbind)
                .map_err(PyErr::from)
        })
        .and_then(|ty| value.is_instance(ty.bind(py)))
        .unwrap_or(false)
}

fn is_numpy_float(value: &Bound<'_, PyAny>) -> bool {
    let value_type = value.get_type();
    let Ok(name) = value_type.name() else {
        return false;
    };
    let Ok(module) = value_type.module() else {
        return false;
    };
    // Only widths an f64 holds exactly. longdouble/float128 (x86 80/128-bit)
    // is excluded: extracting it through f64 can silently round a fraction
    // away and accept a value the exactness rules should reject.
    name.to_str()
        .is_ok_and(|name| matches!(name, "float16" | "float32" | "float64"))
        && module
            .to_str()
            .is_ok_and(|module| module == "numpy" || module.starts_with("numpy."))
}

/// Pandas ships missing values in a numeric enum column as float NaN; a
/// Nullable enum maps those to NULL. The non-nullable path keeps its code-0
/// sentinel via `nan_as_zero`.
pub(super) fn is_enum_nan(ch_type: &ChType, value: &Bound<'_, PyAny>) -> bool {
    if !matches!(ch_type, ChType::Enum8 { .. } | ChType::Enum16 { .. }) {
        return false;
    }
    if unsafe { ffi::PyFloat_Check(value.as_ptr()) } != 0 {
        // SAFETY: PyFloat_Check above guarantees PyFloat_AsDouble succeeds.
        return unsafe { ffi::PyFloat_AsDouble(value.as_ptr()) }.is_nan();
    }
    is_numpy_float(value) && value.extract::<f64>().is_ok_and(f64::is_nan)
}

/// Error context for the integer coercion helpers.
struct IntConvCtx<'a> {
    column: &'a str,
    row: usize,
    type_name: &'a str,
    guidance: &'a str,
}

impl IntConvCtx<'_> {
    fn detail_err(&self, value: &Bound<'_, PyAny>, detail: &str) -> PyErr {
        conversion_error_detail(value, self.column, self.row, self.type_name, detail)
    }

    fn fallback_err(&self, value: &Bound<'_, PyAny>) -> PyErr {
        self.detail_err(value, &format!("is not an integer; {}", self.guidance))
    }
}

fn long_from_float<'py>(
    py: Python<'py>,
    value: &Bound<'py, PyAny>,
    number: f64,
    ctx: &IntConvCtx<'_>,
    nan_as_zero: bool,
) -> PyResult<Bound<'py, PyAny>> {
    if number.is_nan() && nan_as_zero {
        // Pandas represents a numeric enum column with missing values as float64.
        // The Python Enum serializer uses zero as its non-nullable missing sentinel.
        return unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyLong_FromLong(0)) };
    }
    if !number.is_finite() {
        return Err(ctx.detail_err(value, &format!("is not finite; {}", ctx.guidance)));
    }
    if number.fract() != 0.0 {
        return Err(ctx.detail_err(
            value,
            &format!("would lose fractional data; {}", ctx.guidance),
        ));
    }
    unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyNumber_Long(value.as_ptr())) }
        .map_err(|_| ctx.fallback_err(value))
}

/// Return a Python integer while preserving exactness. Integral floats and
/// decimal.Decimal values are accepted; strings are deliberately rejected.
fn integer_object<'py>(
    py: Python<'py>,
    value: &Bound<'py, PyAny>,
    column: &str,
    row: usize,
    type_name: &str,
    guidance: &str,
    nan_as_zero: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let ctx = IntConvCtx {
        column,
        row,
        type_name,
        guidance,
    };
    if unsafe { ffi::PyLong_Check(value.as_ptr()) } != 0 {
        return Ok(value.clone());
    }
    if value.downcast::<PyString>().is_ok() {
        return Err(ctx.detail_err(value, "strings are not accepted; pass an int instead"));
    }
    if unsafe { ffi::PyFloat_Check(value.as_ptr()) } != 0 {
        // SAFETY: PyFloat_Check above guarantees PyFloat_AsDouble succeeds.
        let number = unsafe { ffi::PyFloat_AsDouble(value.as_ptr()) };
        return long_from_float(py, value, number, &ctx, nan_as_zero);
    }
    if is_numpy_float(value) {
        let number = value
            .extract::<f64>()
            .map_err(|_| ctx.fallback_err(value))?;
        return long_from_float(py, value, number, &ctx, nan_as_zero);
    }
    if is_decimal(value) {
        let finite = value
            .call_method0(intern!(py, "is_finite"))
            .and_then(|result| result.is_truthy())
            .map_err(|_| ctx.fallback_err(value))?;
        if !finite {
            return Err(ctx.detail_err(value, &format!("is not finite; {guidance}")));
        }
        let integral = value
            .call_method0(intern!(py, "to_integral_value"))
            .map_err(|_| ctx.fallback_err(value))?;
        if !integral.eq(value).map_err(|_| ctx.fallback_err(value))? {
            return Err(ctx.detail_err(value, &format!("would lose fractional data; {guidance}")));
        }
        return unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyNumber_Long(value.as_ptr())) }
            .map_err(|_| ctx.fallback_err(value));
    }
    unsafe { Bound::from_owned_ptr_or_err(py, ffi::PyNumber_Index(value.as_ptr())) }
        .map_err(|_| ctx.fallback_err(value))
}

/// Convert a Python integer/index object to an owned
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
pub(super) enum WideFast {
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
pub(super) unsafe fn wide_int_fast_into(
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

/// Write one Python integer-compatible object into its final fixed-width
/// little-endian slice, preserving the exactness rules used by narrow integers.
pub(super) fn wide_int_into(
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
/// already its own index result; anything else routes through the shared
/// exact integer coercion policy.
#[allow(clippy::too_many_arguments)]
pub(super) fn wide_int_slow_into(
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
        owned = integer_object(
            py,
            value,
            column,
            row,
            type_name,
            "pass an integer value",
            false,
        )?;
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
    if let Ok(name) = value.downcast::<PyString>() {
        let name = name.to_str()?;
        return variants
            .iter()
            .find_map(|(variant, code)| (variant == name).then_some(*code))
            .ok_or_else(|| {
                PyValueError::new_err(format!(
                    "column {column:?} row {row} Enum8 label {name:?} is not defined"
                ))
            });
    }
    let integer = integer_object(
        value.py(),
        value,
        column,
        row,
        "Enum8",
        "pass a valid enum label or integral code",
        true,
    )?;
    integer
        .extract::<i8>()
        .map_err(|_| integer_range_error(value, column, row, "Enum8"))
}

fn enum16_value(
    value: &Bound<'_, PyAny>,
    variants: &[(String, i16)],
    column: &str,
    row: usize,
) -> PyResult<i16> {
    if let Ok(name) = value.downcast::<PyString>() {
        let name = name.to_str()?;
        return variants
            .iter()
            .find_map(|(variant, code)| (variant == name).then_some(*code))
            .ok_or_else(|| {
                PyValueError::new_err(format!(
                    "column {column:?} row {row} Enum16 label {name:?} is not defined"
                ))
            });
    }
    let integer = integer_object(
        value.py(),
        value,
        column,
        row,
        "Enum16",
        "pass a valid enum label or integral code",
        true,
    )?;
    integer
        .extract::<i16>()
        .map_err(|_| integer_range_error(value, column, row, "Enum16"))
}

pub(super) fn decimal_width(precision: u8) -> PyResult<usize> {
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

pub(super) fn decimal_text(value: &Bound<'_, PyAny>, column: &str, row: usize) -> PyResult<String> {
    value
        .str()
        .and_then(|s| s.to_str().map(str::to_owned))
        .map_err(|_| {
            PyValueError::new_err(format!(
                "column {column:?} row {row} Decimal value cannot be stringified"
            ))
        })
}

/// Decimal text-to-wire failure classes; `decimal_err` renders the exact
/// user-facing message for each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DecimalError {
    Invalid,
    UnsupportedExponent,
    ExceedsPrecision,
    OverflowsWidth,
    DoesNotFit,
}

pub(super) fn decimal_err(
    err: DecimalError,
    text: &str,
    width: usize,
    precision: u8,
    column: &str,
    row: usize,
) -> PyErr {
    match err {
        DecimalError::Invalid => conversion_error(column, row, "Decimal"),
        DecimalError::UnsupportedExponent => PyValueError::new_err(format!(
            "column {column:?} row {row} Decimal value {text:?} has an unsupported exponent"
        )),
        DecimalError::ExceedsPrecision => PyValueError::new_err(format!(
            "column {column:?} row {row} Decimal value {text:?} exceeds precision {precision}"
        )),
        DecimalError::OverflowsWidth => PyValueError::new_err(format!(
            "column {column:?} row {row} Decimal value overflows target width"
        )),
        DecimalError::DoesNotFit => PyValueError::new_err(format!(
            "column {column:?} row {row} Decimal value {text:?} does not fit in {width} bytes"
        )),
    }
}

fn decimal_to_le_bytes(
    text: &str,
    width: usize,
    precision: u8,
    scale: u8,
    column: &str,
    row: usize,
) -> PyResult<Vec<u8>> {
    let mut bytes = vec![0u8; width];
    decimal_wire_into(text, &mut bytes, precision, scale)
        .map_err(|err| decimal_err(err, text, width, precision, column, row))?;
    Ok(bytes)
}

const POW10: [u64; 20] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
    10_000_000_000_000_000_000,
];

/// limbs = limbs * pow + add over little-endian u64 limbs; carry out of the
/// limb window is an overflow.
fn limbs_mul_add(limbs: &mut [u64], pow: u64, add: u64) -> Result<(), DecimalError> {
    let mut carry = u128::from(add);
    for limb in limbs.iter_mut() {
        let next = u128::from(*limb) * u128::from(pow) + carry;
        *limb = next as u64;
        carry = next >> 64;
    }
    if carry != 0 {
        return Err(DecimalError::OverflowsWidth);
    }
    Ok(())
}

/// Parse decimal text and write the scaled two's-complement little-endian
/// integer into `out` (4, 8, 16, or 32 bytes). Fractional digits beyond
/// `scale` are truncated toward zero before accumulating. Runs no Python.
pub(super) fn decimal_wire_into(
    text: &str,
    out: &mut [u8],
    precision: u8,
    scale: u8,
) -> Result<(), DecimalError> {
    let width = out.len();
    let bytes = text.trim().as_bytes();
    if bytes.is_empty() {
        return Err(DecimalError::Invalid);
    }

    let mut pos = 0;
    let negative = match bytes[0] {
        b'-' => {
            pos += 1;
            true
        }
        b'+' => {
            pos += 1;
            false
        }
        _ => false,
    };

    let int_start = pos;
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        pos += 1;
    }
    let int_run = &bytes[int_start..pos];
    let mut frac_run: &[u8] = &[];
    if pos < bytes.len() && bytes[pos] == b'.' {
        pos += 1;
        let frac_start = pos;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
        frac_run = &bytes[frac_start..pos];
    }
    if int_run.is_empty() && frac_run.is_empty() {
        return Err(DecimalError::Invalid);
    }

    let mut exp10 = 0i32;
    if pos < bytes.len() {
        if !matches!(bytes[pos], b'e' | b'E') {
            return Err(DecimalError::Invalid);
        }
        pos += 1;
        let mut exp_sign = 1i32;
        match bytes.get(pos) {
            Some(b'-') => {
                exp_sign = -1;
                pos += 1;
            }
            Some(b'+') => {
                pos += 1;
            }
            _ => {}
        }
        if pos == bytes.len() {
            return Err(DecimalError::Invalid);
        }
        let mut exp = 0i32;
        for &byte in &bytes[pos..] {
            if !byte.is_ascii_digit() {
                return Err(DecimalError::Invalid);
            }
            exp = exp
                .checked_mul(10)
                .and_then(|v| v.checked_add(i32::from(byte - b'0')))
                .ok_or(DecimalError::Invalid)?;
        }
        exp10 = exp_sign * exp;
    }

    let frac_count = i32::try_from(frac_run.len()).map_err(|_| DecimalError::Invalid)?;
    let exponent = exp10.checked_sub(frac_count).ok_or(DecimalError::Invalid)?;
    let shift = exponent
        .checked_add(i32::from(scale))
        .ok_or(DecimalError::UnsupportedExponent)?;

    // Significant digits: strip leading zeros across the combined runs.
    let mut sig_int = int_run;
    while let Some((b'0', rest)) = sig_int.split_first() {
        sig_int = rest;
    }
    let mut sig_frac = frac_run;
    if sig_int.is_empty() {
        while let Some((b'0', rest)) = sig_frac.split_first() {
            sig_frac = rest;
        }
    }
    let sig_len = sig_int.len() + sig_frac.len();

    // A non-negative shift pads with zeros; a negative shift drops fractional
    // digits beyond the scale (truncation toward zero).
    let precision = usize::from(precision);
    let (keep, zeros) = if shift >= 0 {
        let shift = usize::try_from(shift).map_err(|_| DecimalError::Invalid)?;
        let new_len = sig_len
            .checked_add(shift)
            .ok_or(DecimalError::UnsupportedExponent)?;
        if sig_len > 0 && new_len > precision {
            return Err(DecimalError::ExceedsPrecision);
        }
        if new_len > width * 4 {
            return Err(DecimalError::DoesNotFit);
        }
        (sig_len, shift)
    } else {
        let remove = usize::try_from(shift.unsigned_abs()).map_err(|_| DecimalError::Invalid)?;
        (sig_len.saturating_sub(remove), 0)
    };
    if keep + zeros > precision {
        return Err(DecimalError::ExceedsPrecision);
    }

    // Accumulate the kept digits in chunks of up to 19 (the largest power of
    // ten a u64 holds), then fold the zero padding in as pure multiplies.
    let limb_count = width.div_ceil(8).max(1);
    let mut limbs = [0u64; 4];
    let limbs = &mut limbs[..limb_count];
    let mut chunk = 0u64;
    let mut chunk_len = 0usize;
    for &digit in sig_int.iter().chain(sig_frac).take(keep) {
        chunk = chunk * 10 + u64::from(digit - b'0');
        chunk_len += 1;
        if chunk_len == 19 {
            limbs_mul_add(limbs, POW10[19], chunk)?;
            chunk = 0;
            chunk_len = 0;
        }
    }
    if chunk_len > 0 {
        limbs_mul_add(limbs, POW10[chunk_len], chunk)?;
    }
    let mut zeros = zeros;
    while zeros > 0 {
        let step = zeros.min(19);
        limbs_mul_add(limbs, POW10[step], 0)?;
        zeros -= step;
    }
    if width < 8 && limbs[0] >> (8 * width) != 0 {
        return Err(DecimalError::OverflowsWidth);
    }

    for (index, byte) in out.iter_mut().enumerate() {
        *byte = (limbs[index / 8] >> ((index % 8) * 8)) as u8;
    }
    if !fits_signed_width(out, negative) {
        return Err(DecimalError::DoesNotFit);
    }
    if negative && out.iter().any(|&b| b != 0) {
        twos_complement_in_place(out);
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

#[cfg(test)]
mod tests {
    use super::{decimal_wire_into, DecimalError};

    fn wire(text: &str, width: usize, precision: u8, scale: u8) -> Result<Vec<u8>, DecimalError> {
        let mut out = vec![0u8; width];
        decimal_wire_into(text, &mut out, precision, scale)?;
        Ok(out)
    }

    fn le(value: i128, width: usize) -> Vec<u8> {
        value.to_le_bytes()[..width].to_vec()
    }

    #[test]
    fn scaled_values_per_width() {
        assert_eq!(wire("1.5", 4, 9, 3), Ok(le(1_500, 4)));
        assert_eq!(wire("-1.5", 4, 9, 3), Ok(le(-1_500, 4)));
        assert_eq!(wire("+1.5", 4, 9, 3), Ok(le(1_500, 4)));
        assert_eq!(wire(" 12.5 ", 4, 9, 3), Ok(le(12_500, 4)));
        assert_eq!(wire("7", 8, 18, 6), Ok(le(7_000_000, 8)));
        assert_eq!(
            wire("123456789.123456", 8, 18, 6),
            Ok(le(123_456_789_123_456, 8))
        );
        assert_eq!(
            wire("-123456789012345678901234.567890123456", 16, 38, 12),
            Ok(le(-123_456_789_012_345_678_901_234_567_890_123_456, 16))
        );
    }

    #[test]
    fn boundary_values() {
        // +-(10^precision - 1) scaled
        assert_eq!(wire("999999.999", 4, 9, 3), Ok(le(999_999_999, 4)));
        assert_eq!(wire("-999999.999", 4, 9, 3), Ok(le(-999_999_999, 4)));
        assert_eq!(
            wire("999999999999.999999", 8, 18, 6),
            Ok(le(999_999_999_999_999_999, 8))
        );
        let top38 = 10i128.pow(38) - 1;
        assert_eq!(
            wire("99999999999999999999999999.999999999999", 16, 38, 12),
            Ok(le(top38, 16))
        );
        // the exact -2^(8w-1) two's-complement boundary fits; its positive
        // counterpart does not (width deliberately narrower than precision)
        assert_eq!(
            wire("-2147483648", 4, 10, 0),
            Ok(vec![0x00, 0x00, 0x00, 0x80])
        );
        assert_eq!(wire("2147483648", 4, 10, 0), Err(DecimalError::DoesNotFit));
    }

    #[test]
    fn zero_forms() {
        for text in ["0", "-0", "0.0", "-0.000", "0E-10", "0E-200"] {
            assert_eq!(wire(text, 4, 9, 3), Ok(vec![0; 4]), "{text}");
        }
        assert_eq!(wire("0E+10", 8, 18, 6), Ok(vec![0; 8]));
        // zero padded beyond the precision or width keeps the legacy errors
        assert_eq!(wire("0E+10", 4, 9, 3), Err(DecimalError::ExceedsPrecision));
        assert_eq!(wire("0E+200", 4, 9, 3), Err(DecimalError::DoesNotFit));
    }

    #[test]
    fn excess_fraction_truncates_toward_zero() {
        assert_eq!(wire("1.0005", 4, 9, 3), Ok(le(1_000, 4)));
        assert_eq!(wire("-1.0005", 4, 9, 3), Ok(le(-1_000, 4)));
        assert_eq!(
            wire("123456.789999999999999999999", 4, 9, 3),
            Ok(le(123_456_789, 4))
        );
        assert_eq!(wire("0.0001", 4, 9, 3), Ok(vec![0; 4]));
        assert_eq!(wire("-0.0001", 4, 9, 3), Ok(vec![0; 4]));
        // truncation happens before the precision check
        assert_eq!(
            wire("1234567890.1234", 4, 9, 3),
            Err(DecimalError::ExceedsPrecision)
        );
    }

    #[test]
    fn scientific_notation() {
        assert_eq!(wire("1E+2", 4, 9, 3), Ok(le(100_000, 4)));
        assert_eq!(wire("1E-7", 4, 9, 3), Ok(vec![0; 4]));
        assert_eq!(wire("-3E+1", 4, 9, 3), Ok(le(-30_000, 4)));
        assert_eq!(wire("5e2", 4, 9, 3), Ok(le(500_000, 4)));
        assert_eq!(wire("1.25E+3", 4, 9, 3), Ok(le(1_250_000, 4)));
    }

    #[test]
    fn scale_zero_and_max_scale() {
        assert_eq!(wire("123456789", 4, 9, 0), Ok(le(123_456_789, 4)));
        assert_eq!(wire("1.9", 4, 9, 0), Ok(le(1, 4)));
        assert_eq!(wire("-1.9", 4, 9, 0), Ok(le(-1, 4)));
        assert_eq!(wire("0.123456789", 4, 9, 9), Ok(le(123_456_789, 4)));
        assert_eq!(wire("1.0", 4, 9, 9), Err(DecimalError::ExceedsPrecision));
    }

    #[test]
    fn wide_widths_match_per_digit_reference() {
        // Reference accumulator: the per-digit byte-carry algorithm this
        // implementation replaced.
        fn reference(digits: &str, negative: bool, width: usize) -> Vec<u8> {
            let mut mag = vec![0u8; width];
            for digit in digits.bytes() {
                let mut carry = u16::from(digit - b'0');
                for byte in mag.iter_mut() {
                    let next = u16::from(*byte) * 10 + carry;
                    *byte = next as u8;
                    carry = next >> 8;
                }
                assert_eq!(carry, 0);
            }
            if negative && mag.iter().any(|&b| b != 0) {
                super::twos_complement_in_place(&mut mag);
            }
            mag
        }

        for digit_count in 1..=76usize {
            let digits: String = (0..digit_count)
                .map(|i| char::from(b'1' + (i % 9) as u8))
                .collect();
            let (int_part, frac_part) = digits.split_at(digit_count.min(46));
            let text = if frac_part.is_empty() {
                format!("{int_part}.0")
            } else {
                format!("{int_part}.{frac_part}")
            };
            let scaled: String = digits
                .chars()
                .chain(std::iter::repeat('0'))
                .take(int_part.len() + 30)
                .collect();
            for negative in [false, true] {
                let signed = if negative {
                    format!("-{text}")
                } else {
                    text.clone()
                };
                assert_eq!(
                    wire(&signed, 32, 76, 30),
                    Ok(reference(&scaled, negative, 32)),
                    "{signed}"
                );
            }
        }
    }

    #[test]
    fn invalid_text() {
        for text in [
            "",
            " ",
            "abc",
            "1.2.3",
            "1e",
            "1e+",
            "--5",
            "+",
            ".",
            "1x",
            "1e5x",
            "nan",
            "inf",
            "1e999999999999",
        ] {
            assert_eq!(wire(text, 4, 9, 3), Err(DecimalError::Invalid), "{text:?}");
        }
    }

    #[test]
    fn exponent_and_precision_errors() {
        assert_eq!(
            wire("1e2147483647", 4, 9, 3),
            Err(DecimalError::UnsupportedExponent)
        );
        assert_eq!(
            wire("1234567890", 4, 9, 3),
            Err(DecimalError::ExceedsPrecision)
        );
        assert_eq!(
            wire("1E+70", 32, 76, 30),
            Err(DecimalError::ExceedsPrecision)
        );
        // magnitude overflow of a width narrower than the claimed precision
        assert_eq!(
            wire("9999999999", 4, 76, 0),
            Err(DecimalError::OverflowsWidth)
        );
    }
}
