use super::*;

/// Error for a malformed Dynamic SharedVariant cell.
pub(super) fn shared_cell_err(err: &BinaryValueError) -> PyErr {
    PyValueError::new_err(format!(
        "Malformed payload: invalid Dynamic SharedVariant cell: {err}"
    ))
}

/// Error for a LowCardinality index outside its dictionary.
pub(super) fn lc_index_err() -> PyErr {
    PyValueError::new_err("Malformed payload: LowCardinality index out of dictionary range")
}

/// Error for a column whose ColumnCtx was prepared for a different type; an
/// internal invariant violation, not a payload condition.
pub(super) fn ctx_missing(what: &str) -> PyErr {
    PyValueError::new_err(format!("internal error: missing {what} column context"))
}

/// Error for a container column whose ColumnCtx carries a different number of
/// field contexts than the column has fields; an internal invariant violation.
pub(super) fn ctx_count_mismatch(what: &str) -> PyErr {
    PyValueError::new_err(format!(
        "internal error: {what} field context count mismatch"
    ))
}

/// Error for an Array column whose offsets are out of range for the element
/// buffer (out of order, negative, or an end past the element count).
pub(super) fn array_bounds_err() -> PyErr {
    PyValueError::new_err("Malformed payload: Array offsets are out of range")
}

/// Error for a Map column whose offsets are out of range for the entries
/// buffer (out of order, negative, or an end past the entry count).
pub(super) fn map_bounds_err() -> PyErr {
    PyValueError::new_err("Malformed payload: Map offsets are out of range")
}

/// Error for a Tuple column whose field columns are shorter than the row count.
pub(super) fn tuple_shape_err() -> PyErr {
    PyValueError::new_err("Malformed payload: Tuple field length mismatch")
}

/// Error for a Map column whose entries are not a two-field key/value tuple;
/// an internal invariant violation (the core always builds this shape).
pub(super) fn map_entries_err() -> PyErr {
    PyValueError::new_err("internal error: Map entries are not a two-field tuple")
}

/// Error for malformed Variant dense-union routing or child lengths.
pub(super) fn variant_shape_err() -> PyErr {
    PyValueError::new_err("Malformed payload: Variant routing or child length mismatch")
}

/// Error for malformed Dynamic dense-union routing or child lengths.
pub(super) fn dynamic_shape_err() -> PyErr {
    PyValueError::new_err("Malformed payload: Dynamic routing or child length mismatch")
}

pub(super) fn json_shape_err() -> PyErr {
    PyValueError::new_err("Malformed payload: JSON path or child layout mismatch")
}

/// Error for a UUID/IPv6 cell whose fixed width is not 16 bytes.
pub(super) fn fixed_width_err(what: &str) -> PyErr {
    PyValueError::new_err(format!("Malformed payload: {what} cell is not 16 bytes"))
}
