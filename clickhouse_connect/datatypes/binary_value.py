"""Recursive decoder for ClickHouse's single-value binary format.

Values stored in JSON shared data (and in a Dynamic column's shared variant) are
written by the server as::

    <binary type encoding><value in ISerialization::serializeBinary format>

See ColumnDynamic::serializeValueIntoSharedVariant.

Two things matter here:

1. The type encoding is recursive and variable length (``Array(T)`` is
   ``0x1E<nested>``), so it cannot be resolved with a single byte lookup.
2. ``serializeBinary`` is the *single value* format, which is NOT the same as
   the one-row column format for compound types.  ``Array`` writes a var_uint
   element count followed by each element's single-value encoding, whereas the
   native column format writes UInt64 offsets.  Scalars and String happen to be
   identical in both, which is why the existing scalar-only decoder works.
"""

from typing import Any

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.bytesource import ByteArraySource

# Binary type index -> ClickHouse type name, for types whose single-value binary
# encoding is identical to their one-row native column encoding.
SCALAR_TYPE_INDEXES = {
    0x00: "Nothing",
    0x01: "UInt8",
    0x02: "UInt16",
    0x03: "UInt32",
    0x04: "UInt64",
    0x05: "UInt128",
    0x06: "UInt256",
    0x07: "Int8",
    0x08: "Int16",
    0x09: "Int32",
    0x0A: "Int64",
    0x0B: "Int128",
    0x0C: "Int256",
    0x0D: "Float32",
    0x0E: "Float64",
    0x15: "String",
    0x2D: "Bool",
}

ARRAY = 0x1E
TUPLE = 0x1F
NAMED_TUPLE = 0x20
NULLABLE = 0x23
LOW_CARDINALITY = 0x26
MAP = 0x27
DYNAMIC = 0x2B
JSON = 0x30


class UnsupportedBinaryTypeError(Exception):
    """Raised when the encoded type contains something we cannot decode."""


class _Node:
    __slots__ = ("kind", "child", "children", "names", "ch_type")

    def __init__(self, kind, child=None, children=None, names=None, ch_type=None):
        self.kind = kind
        self.child = child
        self.children = children
        self.names = names
        self.ch_type = ch_type


def read_encoded_type(source: ByteArraySource) -> _Node:
    """Parse a binary type encoding, leaving the cursor at the start of the value."""
    idx = source.read_byte()

    name = SCALAR_TYPE_INDEXES.get(idx)
    if name is not None:
        return _Node("scalar", ch_type=get_from_name(name))

    if idx == ARRAY:
        return _Node("array", child=read_encoded_type(source))

    if idx == NULLABLE:
        return _Node("nullable", child=read_encoded_type(source))

    if idx == LOW_CARDINALITY:
        # LowCardinality delegates to the dictionary type's serializeBinary
        return read_encoded_type(source)

    if idx == MAP:
        key = read_encoded_type(source)
        value = read_encoded_type(source)
        return _Node("map", children=[key, value])

    if idx == TUPLE:
        count = source.read_leb128()
        return _Node("tuple", children=[read_encoded_type(source) for _ in range(count)])

    if idx == NAMED_TUPLE:
        count = source.read_leb128()
        names = []
        children = []
        for _ in range(count):
            names.append(source.read_leb128_str())
            children.append(read_encoded_type(source))
        return _Node("named_tuple", children=children, names=names)

    if idx == DYNAMIC:
        source.read_byte()  # uint8 max_types
        return _Node("dynamic")

    if idx == JSON:
        source.read_byte()  # uint8 serialization version
        source.read_leb128()  # max_dynamic_paths  (was: _read_var_int(source))
        source.read_byte()  # uint8 max_dynamic_types
        typed_paths = {}
        for _ in range(source.read_leb128()):
            path = source.read_leb128_str()
            typed_paths[path] = read_encoded_type(source)
        for _ in range(source.read_leb128()):  # SKIP paths
            source.read_leb128_str()
        for _ in range(source.read_leb128()):  # SKIP REGEXP
            source.read_leb128_str()
        return _Node("json", children=typed_paths)

    raise UnsupportedBinaryTypeError(f"Unhandled binary type index 0x{idx:02X}")


def read_binary_value(source: ByteArraySource, node: _Node, ctx) -> Any:
    """Read one value in ISerialization::serializeBinary format."""
    kind = node.kind

    if kind == "scalar":
        ch_type = node.ch_type
        if ch_type.name == "Nothing":
            return None
        read_state = ch_type.read_column_prefix(source, ctx)
        col_data = ch_type.read_column_data(source, 1, ctx, read_state)
        return col_data[0] if col_data else None

    if kind == "array":
        count = source.read_leb128()
        return [read_binary_value(source, node.child, ctx) for _ in range(count)]

    if kind == "nullable":
        if source.read_byte():
            return None
        return read_binary_value(source, node.child, ctx)

    if kind == "map":
        # Serialized as the nested Array(Tuple(K, V))
        count = source.read_leb128()
        key_node, value_node = node.children
        mapping = {}
        for _ in range(count):
            key = read_binary_value(source, key_node, ctx)
            mapping[key] = read_binary_value(source, value_node, ctx)
        return mapping

    if kind == "tuple":
        return tuple(read_binary_value(source, child, ctx) for child in node.children)

    if kind == "named_tuple":
        return {name: read_binary_value(source, child, ctx) for name, child in zip(node.names, node.children)}

    if kind == "dynamic":
        return read_binary_value(source, read_encoded_type(source), ctx)

    if kind == "json":
        from clickhouse_connect.datatypes.dynamic import _nest_value  # circular import

        typed_paths = node.children or {}
        obj: dict[str, Any] = {}
        for _ in range(source.read_leb128()):
            path = source.read_leb128_str()
            path_node = typed_paths.get(path)
            if path_node is None:
                # dynamic path or shared data: value is self describing
                path_node = read_encoded_type(source)
            value = read_binary_value(source, path_node, ctx)
            if value is not None:
                _nest_value(obj, path, value)
        return obj

    raise UnsupportedBinaryTypeError(f"Unhandled node kind {kind}")


def decode_binary_value(binary_data: bytes, ctx):
    """Decode <encoded type><value>.  Raises on anything unexpected."""
    source = ByteArraySource(binary_data)
    node = read_encoded_type(source)
    value = read_binary_value(source, node, ctx)
    if source.pos != len(binary_data):
        raise UnsupportedBinaryTypeError(f"Trailing bytes after decode: consumed {source.pos} of {len(binary_data)}")
    return value
