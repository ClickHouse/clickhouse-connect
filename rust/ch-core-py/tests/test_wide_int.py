from helpers import (
    _WIDE_TYPES,
    _ch_core,
    _NdarrayLikeColumn,
    build_native_block,
    pytest,
)

_WIDE_CASES = [
    ("Int128", [-(2**127), -1, 0, 2**127 - 1]),
    ("UInt128", [0, 13, 2**127, 2**128 - 1]),
    ("Int256", [-(2**255), -1, 0, 2**255 - 1]),
    ("UInt256", [0, 79, 2**255, 2**256 - 1]),
]

# Values straddling the i64/u64 fast-path boundary plus one true wide value;
# type min/max extremes are covered by _WIDE_CASES. Negative word boundaries
# exercise the decode word-scan where high words are all-ones.
_WIDE_FAST_SLOW_CASES = [
    (
        "Int128",
        [0, 1, -1, 2**63 - 1, -(2**63), -(2**63) - 1, 2**63, 2**64 - 1, 2**64,
         2**64 + 1, -(2**64), -(2**64) - 1, -(2**64) + 1, 2**100 + 13, -(2**100)],
    ),
    ("UInt128", [0, 1, 2**63 - 1, 2**63, 2**64 - 1, 2**64, 2**64 + 1, 2**100 + 13]),
    (
        "Int256",
        [0, 1, -1, 2**63 - 1, -(2**63), -(2**63) - 1, 2**63, 2**64 - 1, 2**64,
         2**64 + 1, -(2**64), -(2**64) - 1, -(2**64) + 1, -(2**128), -(2**192),
         -(2**192) - 1, 2**200 + 13, -(2**200)],
    ),
    ("UInt256", [0, 1, 2**63 - 1, 2**63, 2**64 - 1, 2**64, 2**64 + 1, 2**200 + 13]),
]


class _IndexValue:
    def __init__(self, value):
        self.value = value

    def __index__(self):
        return self.value


class _IntSubclass(int):
    pass


class TestWideIntegers:
    @pytest.mark.parametrize(("type_name", "values"), _WIDE_CASES)
    def test_scalar_boundaries_and_all_object_exits(self, type_name, values):
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        assert encoded == build_native_block([("v", type_name, values)])

        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == values
        assert list(batch.to_python_columns()[0]) == values
        assert [row[0] for row in batch.to_python_rows()] == values

    @pytest.mark.parametrize(("type_name", "values"), _WIDE_CASES)
    def test_index_values(self, type_name, values):
        indexed = [_IndexValue(value) for value in values]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [indexed], len(indexed))
        assert encoded == build_native_block([("v", type_name, values)])

    @pytest.mark.parametrize(("type_name", "values"), _WIDE_CASES)
    def test_generic_column_matches_direct_buffer_builder(self, type_name, values):
        generic = _NdarrayLikeColumn(values)
        encoded = _ch_core.encode_native_block(["v"], [type_name], [generic], len(values))
        assert encoded == build_native_block([("v", type_name, values)])

    @pytest.mark.parametrize("type_name", [type_name for type_name, _ in _WIDE_CASES])
    @pytest.mark.parametrize("wrapper", ["{}", "Nullable({})", "LowCardinality(Nullable({}))"])
    def test_numeric_strings_are_rejected(self, type_name, wrapper):
        wrapped = wrapper.format(type_name)
        values = ["13"] if wrapper == "{}" else [None, "13"]
        with pytest.raises(ValueError, match="strings are not accepted; pass an int instead"):
            _ch_core.encode_native_block(["v"], [wrapped], [values], len(values))

    @pytest.mark.parametrize("type_name", [type_name for type_name, _ in _WIDE_CASES])
    def test_integral_float_and_decimal_values(self, type_name):
        from decimal import Decimal

        values = [13.0, Decimal("79.000")]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        assert encoded == build_native_block([("v", type_name, [13, 79])])

    @pytest.mark.parametrize("type_name", [type_name for type_name, _ in _WIDE_CASES])
    @pytest.mark.parametrize("value", [13.5, pytest.param(float("nan"), id="nan")])
    def test_lossy_float_is_rejected(self, type_name, value):
        detail = "is not finite" if value != value else "would lose fractional data"
        with pytest.raises(ValueError, match=detail):
            _ch_core.encode_native_block(["v"], [type_name], [[value]], 1)

    @pytest.mark.parametrize(("type_name", "values"), _WIDE_CASES)
    @pytest.mark.parametrize("wrapper", ["Nullable({})", "LowCardinality(Nullable({}))"])
    def test_nullable_and_low_cardinality(self, type_name, values, wrapper):
        type_name = wrapper.format(type_name)
        nullable_values = [None, values[0], values[-1], values[0], None]
        encoded = _ch_core.encode_native_block(
            ["v"], [type_name], [nullable_values], len(nullable_values)
        )
        assert encoded == build_native_block([("v", type_name, nullable_values)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == nullable_values

    def test_nested_containers(self):
        columns = [
            (
                "a",
                "Array(Nullable(Int128))",
                [[-(2**127), None], [], [13, 2**127 - 1]],
            ),
            (
                "t",
                "Tuple(UInt128, Int256)",
                [(2**128 - 1, -(2**255)), (13, -1), (0, 2**255 - 1)],
            ),
            (
                "m",
                "Map(UInt128, UInt256)",
                [{13: 2**256 - 1}, {}, {2**128 - 1: 79}],
            ),
        ]
        names = [name for name, _, _ in columns]
        types = [type_name for _, type_name, _ in columns]
        values = [column for _, _, column in columns]
        encoded = _ch_core.encode_native_block(names, types, values, 3)
        assert encoded == build_native_block(columns)

        batch = _ch_core.ColBatch.decode_native(encoded)
        assert [list(batch.column_data(index)) for index in range(len(columns))] == values
        assert [list(column) for column in batch.to_python_columns()] == values
        assert list(batch.to_python_rows()) == list(zip(*values))

    @pytest.mark.parametrize(
        "type_name,rows",
        [
            ("Array(Int128)", [[13.0, "79"]]),
            ("Tuple(UInt128, Int256)", [(13.0, "79")]),
            ("Array(LowCardinality(Int256))", [[13.0, "79"]]),
        ],
    )
    def test_numeric_strings_in_containers_are_rejected(self, type_name, rows):
        with pytest.raises(ValueError, match="strings are not accepted; pass an int instead"):
            _ch_core.encode_native_block(["v"], [type_name], [rows], 1)

    @pytest.mark.parametrize("bad_index", ["raises", "non_int"])
    def test_bad_index_protocol_has_context(self, bad_index):
        class BadIndex:
            def __index__(self):
                if bad_index == "raises":
                    raise RuntimeError("index failed")
                return "13"

        with pytest.raises(
            ValueError,
            match=r'column "v" row 1 cannot be converted to Int256',
        ):
            _ch_core.encode_native_block(["v"], ["Int256"], [[13, BadIndex()]], 2)

    def test_index_mutating_source_list_is_rejected(self):
        values = [None, 13, 79]

        class MutatingIndex:
            def __index__(self):
                values.pop()
                return 5

        values[0] = MutatingIndex()
        with pytest.raises(ValueError, match=r'column "v" was resized during encoding'):
            _ch_core.encode_native_block(["v"], ["UInt256"], [values], 3)

    def test_empty_columns(self):
        names = [f"v{index}" for index in range(len(_WIDE_CASES))]
        types = [type_name for type_name, _ in _WIDE_CASES]
        values = [[] for _ in types]
        encoded = _ch_core.encode_native_block(names, types, values, 0)
        assert encoded == build_native_block(
            [(name, type_name, []) for name, type_name in zip(names, types)]
        )
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == types
        assert [list(column) for column in batch.to_python_columns()] == values
        assert list(batch.to_python_rows()) == []

    @pytest.mark.parametrize(
        ("type_name", "invalid"),
        [
            ("Int128", -(2**127) - 1),
            ("Int128", 2**127),
            ("UInt128", -1),
            ("UInt128", -(2**63)),
            ("UInt128", -(2**63) - 1),
            ("UInt128", 2**128),
            ("Int256", -(2**255) - 1),
            ("Int256", 2**255),
            ("UInt256", -1),
            ("UInt256", -(2**63)),
            ("UInt256", -(2**63) - 1),
            ("UInt256", 2**256),
            ("Int128", str(2**127)),
            ("UInt128", "-1"),
            ("Int256", str(2**255)),
            ("UInt256", str(2**256)),
            ("Int128", "not-an-integer"),
            ("Int128", 1.5),
            ("Int256", object()),
            ("UInt256", b"79"),
        ],
    )
    def test_range_and_type_errors(self, type_name, invalid):
        with pytest.raises(
            ValueError,
            match=rf'column "v" row 0 cannot be converted to {type_name}',
        ):
            _ch_core.encode_native_block(["v"], [type_name], [[invalid]], 1)

    @pytest.mark.parametrize(("type_name", "values"), _WIDE_FAST_SLOW_CASES)
    def test_fast_slow_boundary_round_trip(self, type_name, values):
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        assert encoded == build_native_block([("v", type_name, values)])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert list(batch.column_data(0)) == values
        assert list(batch.to_python_columns()[0]) == values
        assert [row[0] for row in batch.to_python_rows()] == values

    @pytest.mark.parametrize(("type_name", "values"), _WIDE_FAST_SLOW_CASES)
    def test_nullable_mixes_none_fast_and_slow_values(self, type_name, values):
        wrapped = f"Nullable({type_name})"
        mixed = [None, *values[:2], None, *values[2:], None]
        encoded = _ch_core.encode_native_block(["v"], [wrapped], [mixed], len(mixed))
        assert encoded == build_native_block([("v", wrapped, mixed)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == mixed

    @pytest.mark.parametrize("type_name", [type_name for type_name, _ in _WIDE_CASES])
    def test_int_subclass_encodes_via_index_protocol(self, type_name):
        # An int subclass is not an exact int; PyNumber_Index returns the
        # instance itself, so the slow path converts the subclass directly.
        values = [_IntSubclass(13), _IntSubclass(2**100)]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], 2)
        assert encoded == build_native_block([("v", type_name, [13, 2**100])])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [13, 2**100]

    @pytest.mark.parametrize("type_name", [type_name for type_name, _ in _WIDE_CASES])
    def test_bool_encodes_via_index_protocol(self, type_name):
        # bool is an int subclass, not an exact int, so it converts through
        # the index protocol to 1/0.
        encoded = _ch_core.encode_native_block(["v"], [type_name], [[True, False]], 2)
        assert encoded == build_native_block([("v", type_name, [1, 0])])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [1, 0]

    @pytest.mark.parametrize(("type_name", "values"), _WIDE_CASES)
    def test_arrow_fixed_binary_bytes(self, type_name, values):
        pa = pytest.importorskip("pyarrow")
        width, signed = _WIDE_TYPES[type_name]
        batch = _ch_core.ColBatch.decode_native(
            build_native_block([("v", type_name, values)])
        )
        column = pa.RecordBatchReader.from_stream(batch).read_all().column("v")
        assert column.type == pa.binary(width)
        assert column.to_pylist() == [
            value.to_bytes(width, "little", signed=signed) for value in values
        ]
