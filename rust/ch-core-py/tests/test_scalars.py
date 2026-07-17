from helpers import (
    _bfloat16_bytes,
    _bfloat16_value,
    _ch_core,
    _NdarrayLikeColumn,
    build_native_block,
    decimal,
    math,
    pytest,
    struct,
)

# ---------------------------------------------------------------------------
# Integer types
# ---------------------------------------------------------------------------


class TestDecodeInt8:
    def test_basic(self):
        data = build_native_block([("v", "Int8", [1, -1, 127])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 3
        assert batch.column_type_names == ["Int8"]
        assert list(batch.column_data(0)) == [1, -1, 127]


class TestDecodeInt16:
    def test_basic(self):
        data = build_native_block([("v", "Int16", [256, -256, 32767])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [256, -256, 32767]


class TestDecodeInt32:
    def test_basic(self):
        data = build_native_block([("v", "Int32", [70000, -70000])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [70000, -70000]


class TestDecodeInt64:
    def test_basic(self):
        data = build_native_block([("id", "Int64", [10, 20, 30])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 3
        assert batch.column_names == ["id"]
        assert batch.column_type_names == ["Int64"]
        assert list(batch.column_data(0)) == [10, 20, 30]

    def test_negative_values(self):
        data = build_native_block([("n", "Int64", [-1, 0, 9223372036854775807])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [-1, 0, 9223372036854775807]


class TestDecodeUInt8:
    def test_basic(self):
        data = build_native_block([("v", "UInt8", [0, 128, 255])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [0, 128, 255]


class TestDecodeUInt16:
    def test_basic(self):
        data = build_native_block([("v", "UInt16", [0, 65535])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [0, 65535]


class TestDecodeUInt32:
    def test_basic(self):
        data = build_native_block([("v", "UInt32", [0, 4_000_000_000])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [0, 4_000_000_000]


class TestDecodeUInt64:
    def test_basic(self):
        data = build_native_block([("v", "UInt64", [0, 2**64 - 1])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [0, 2**64 - 1]


# ---------------------------------------------------------------------------
# Float types
# ---------------------------------------------------------------------------


class TestDecodeFloat32:
    def test_basic(self):
        data = build_native_block([("v", "Float32", [1.5, -2.25])])
        batch = _ch_core.ColBatch.decode_native(data)
        vals = list(batch.column_data(0))
        assert vals[0] == pytest.approx(1.5)
        assert vals[1] == pytest.approx(-2.25)


class TestDecodeFloat64:
    def test_basic(self):
        data = build_native_block([("val", "Float64", [1.5, 2.7, -0.1])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 3
        vals = list(batch.column_data(0))
        assert vals[0] == pytest.approx(1.5)
        assert vals[1] == pytest.approx(2.7)
        assert vals[2] == pytest.approx(-0.1)


class TestBFloat16:
    def test_encode_decode_type_matrix_and_all_object_exits(self):
        columns = [
            ("scalar", "BFloat16", [1.1, -1.1, 13.0]),
            ("nullable", "Nullable(BFloat16)", [1.1, None, -1.1]),
            ("array", "Array(BFloat16)", [[1.1, -1.1], [], [13.0]]),
            (
                "tuple",
                "Tuple(BFloat16, String)",
                [(1.1, "user_1"), (-1.1, "user_2"), (13.0, "user_3")],
            ),
            (
                "array_tuple",
                "Array(Tuple(BFloat16, UInt8))",
                [[(1.1, 13), (-1.1, 79)], [], [(13.0, 5)]],
            ),
            (
                "map",
                "Map(BFloat16, String)",
                [{1.1: "user_1"}, {}, {-1.1: "user_2"}],
            ),
            (
                "low_cardinality",
                "LowCardinality(BFloat16)",
                [1.1, -1.1, 1.1],
            ),
            (
                "low_cardinality_nullable",
                "LowCardinality(Nullable(BFloat16))",
                [1.1, None, 1.1],
            ),
        ]
        expected = [
            [_bfloat16_value(1.1), _bfloat16_value(-1.1), 13.0],
            [_bfloat16_value(1.1), None, _bfloat16_value(-1.1)],
            [[_bfloat16_value(1.1), _bfloat16_value(-1.1)], [], [13.0]],
            [
                (_bfloat16_value(1.1), "user_1"),
                (_bfloat16_value(-1.1), "user_2"),
                (13.0, "user_3"),
            ],
            [
                [(_bfloat16_value(1.1), 13), (_bfloat16_value(-1.1), 79)],
                [],
                [(13.0, 5)],
            ],
            [{_bfloat16_value(1.1): "user_1"}, {}, {_bfloat16_value(-1.1): "user_2"}],
            [_bfloat16_value(1.1), _bfloat16_value(-1.1), _bfloat16_value(1.1)],
            [_bfloat16_value(1.1), None, _bfloat16_value(1.1)],
        ]

        encoded = _ch_core.encode_native_block(
            [name for name, _, _ in columns],
            [type_name for _, type_name, _ in columns],
            [values for _, _, values in columns],
            3,
        )

        assert encoded == build_native_block(columns)
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert list(batch.to_python_columns()) == expected
        assert [list(column) for column in zip(*batch.to_python_rows())] == expected
        assert [list(batch.column_data(index)) for index in range(len(columns))] == expected

    @pytest.mark.parametrize("make", [list, tuple, _NdarrayLikeColumn])
    def test_scalar_container_paths_match_golden_bytes(self, make):
        values = [3.141592, -2.71828, 13]
        encoded = _ch_core.encode_native_block(
            ["v"],
            ["BFloat16"],
            [make(values)],
            len(values),
        )
        assert encoded == build_native_block([("v", "BFloat16", values)])

    @pytest.mark.parametrize("dtype", ["float32", "float64"])
    def test_numpy_buffer_matches_list_path(self, dtype):
        np = pytest.importorskip("numpy")
        values = [3.141592, -2.71828, 13.0]
        array = np.array(values, dtype=dtype)
        from_buffer = _ch_core.encode_native_block(["v"], ["BFloat16"], [array], len(array))
        from_list = _ch_core.encode_native_block(["v"], ["BFloat16"], [list(array)], len(array))
        assert from_buffer == from_list == build_native_block([("v", "BFloat16", values)])

    def test_numpy_strided_buffer_matches_list_path(self):
        np = pytest.importorskip("numpy")
        array = np.array([1.1, 0.0, -1.1, 0.0, 13.0], dtype="float32")[::2]
        from_buffer = _ch_core.encode_native_block(["v"], ["BFloat16"], [array], len(array))
        from_list = _ch_core.encode_native_block(["v"], ["BFloat16"], [list(array)], len(array))
        assert from_buffer == from_list

    def test_float32_buffer_signaling_nan_encodes_nan_word(self):
        np = pytest.importorskip("numpy")
        # sNaN whose payload lives only in the low 16 mantissa bits; plain
        # truncation would produce 0x7F80 (+inf).
        array = np.array([0x7F800001], dtype=np.uint32).view(np.float32)
        encoded = _ch_core.encode_native_block(["v"], ["BFloat16"], [array], 1)
        word = struct.unpack("<H", encoded[-2:])[0]
        assert word & 0x7F80 == 0x7F80 and word & 0x007F, hex(word)
        assert word == 0x7FC0
        decoded = list(_ch_core.ColBatch.decode_native(encoded).column_data(0))
        assert math.isnan(decoded[0])

    def test_special_values_and_arrow_raw_words(self):
        pa = pytest.importorskip("pyarrow")
        values = [0.0, -0.0, float("inf"), float("-inf"), float("nan"), 2**-133]
        encoded = _ch_core.encode_native_block(["v"], ["BFloat16"], [values], len(values))
        assert encoded == build_native_block([("v", "BFloat16", values)])

        batch = _ch_core.ColBatch.decode_native(encoded)
        decoded = list(batch.column_data(0))
        assert decoded[:4] == [0.0, -0.0, float("inf"), float("-inf")]
        assert math.copysign(1.0, decoded[1]) == -1.0
        assert math.isnan(decoded[4])
        assert decoded[5] == 2**-133

        arrow_column = pa.RecordBatchReader.from_stream(batch).read_all().column("v")
        assert arrow_column.type == pa.binary(2)
        assert arrow_column.to_pylist() == [_bfloat16_bytes(value) for value in values]

    def test_nullable_all_null_and_zero_rows(self):
        values = [None, None, None]
        encoded = _ch_core.encode_native_block(
            ["v"], ["Nullable(BFloat16)"], [values], len(values)
        )
        assert encoded == build_native_block([("v", "Nullable(BFloat16)", values)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == values

        empty = _ch_core.encode_native_block(["v"], ["BFloat16"], [[]], 0)
        assert empty == build_native_block([("v", "BFloat16", [])])
        assert list(_ch_core.ColBatch.decode_native(empty).column_data(0)) == []

    @pytest.mark.parametrize(
        "type_name",
        ["bfloat16", "Bfloat16", "BFLOAT16", "BFloat16()", "BFloat32"],
    )
    def test_type_name_is_exact(self, type_name):
        with pytest.raises(NotImplementedError, match="unsupported ClickHouse type"):
            _ch_core.encode_native_block(["v"], [type_name], [[13.0]], 1)

    def test_invalid_value_names_row_and_type(self):
        with pytest.raises(ValueError, match='column "v" row 1 cannot be converted to BFloat16'):
            _ch_core.encode_native_block(["v"], ["BFloat16"], [[13.0, "invalid"]], 2)

    @pytest.mark.parametrize("make", [list, tuple, _NdarrayLikeColumn])
    def test_finite_float32_overflow_is_rejected(self, make):
        with pytest.raises(ValueError, match='column "v" row 1 cannot be converted to BFloat16'):
            _ch_core.encode_native_block(["v"], ["BFloat16"], [make([13.0, 1e300])], 2)

    def test_float64_buffer_finite_float32_overflow_is_rejected(self):
        np = pytest.importorskip("numpy")
        values = np.array([13.0, 1e300], dtype="float64")
        with pytest.raises(ValueError, match='column "v" row 1 cannot be converted to BFloat16'):
            _ch_core.encode_native_block(["v"], ["BFloat16"], [values], len(values))


# ---------------------------------------------------------------------------
# Bool
# ---------------------------------------------------------------------------

class TestDecodeBool:
    def test_basic(self):
        data = build_native_block([("b", "Bool", [1, 0, 1, 0, 1])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 5
        assert batch.column_type_names == ["Bool"]
        assert list(batch.column_data(0)) == [True, False, True, False, True]

    def test_nullable(self):
        data = build_native_block([("b", "Nullable(Bool)", [1, None, 0])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = list(batch.column_data(0))
        assert result == [True, None, False]


# ---------------------------------------------------------------------------
# Nullable
# ---------------------------------------------------------------------------


class TestDecodeNullable:
    def test_nullable_int64(self):
        data = build_native_block([("n", "Nullable(Int64)", [100, None, 300, None])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 4
        assert batch.column_type_names == ["Nullable(Int64)"]
        result = list(batch.column_data(0))
        assert result == [100, None, 300, None]

    def test_nullable_int32(self):
        data = build_native_block([("n", "Nullable(Int32)", [1, None, 3])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [1, None, 3]

    def test_nullable_float32(self):
        data = build_native_block([("f", "Nullable(Float32)", [None, 42.5])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = list(batch.column_data(0))
        assert result[0] is None
        assert result[1] == pytest.approx(42.5)

    def test_nullable_float64(self):
        data = build_native_block([("f", "Nullable(Float64)", [None, 42.5])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = list(batch.column_data(0))
        assert result[0] is None
        assert result[1] == pytest.approx(42.5)

    def test_nullable_string(self):
        data = build_native_block([("s", "Nullable(String)", ["abc", None, "xyz"])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == ["abc", None, "xyz"]


# ---------------------------------------------------------------------------
# Decimal
# ---------------------------------------------------------------------------


class TestDecodeDecimal:
    # (precision, scale, unscaled values); precision picks the wire width
    # (<=9 -> 32-bit, <=18 -> 64, <=38 -> 128, <=76 -> 256).
    _CASES = [
        (9, 4, [0, 5, -5, -12000, 999_999_999, -999_999_999]),
        (18, 6, [0, 7, -123456, 123456, 10**18 - 1, -(10**18 - 1)]),
        (38, 10, [0, -3, 10**9 + 1, 10**38 - 1, -(10**38 - 1)]),
        (76, 20, [0, 42, -42, 10**19, -(10**41 + 7), 10**76 - 1, -(10**76 - 1)]),
        (9, 0, [0, -13, 999_999_999]),
        (76, 0, [0, 10**76 - 1, -(10**76 - 1)]),
    ]

    @staticmethod
    def _reference(unscaled, precision, scale):
        with decimal.localcontext() as ctx:
            ctx.prec = precision
            return decimal.Decimal(unscaled).scaleb(-scale)

    @pytest.mark.parametrize("precision,scale,unscaled", _CASES)
    def test_matches_python_reference(self, precision, scale, unscaled):
        type_name = f"Decimal({precision}, {scale})"
        data = build_native_block([("d", type_name, unscaled)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == [type_name]
        got = list(batch.column_data(0))
        expected = [self._reference(u, precision, scale) for u in unscaled]
        assert got == expected
        assert [v.as_tuple() for v in got] == [e.as_tuple() for e in expected]

    def test_paths_agree(self):
        unscaled = [0, -3, 10**38 - 1]
        data = build_native_block([("d", "Decimal(38, 10)", unscaled)])
        batch = _ch_core.ColBatch.decode_native(data)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [row[0] for row in batch.to_python_rows()]
        expected = [self._reference(u, 38, 10) for u in unscaled]
        assert via_column_data == via_columns == via_rows == expected

    def test_nullable(self):
        vals = [12345, None, -12345, None]
        data = build_native_block([("d", "Nullable(Decimal(18, 4))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        expected = [None if v is None else self._reference(v, 18, 4) for v in vals]
        assert list(batch.column_data(0)) == expected
        assert [row[0] for row in batch.to_python_rows()] == expected

    def test_round_trip(self):
        vals = [decimal.Decimal("123.4567"), decimal.Decimal("-1.5")]
        encoded = _ch_core.encode_native_block(["d"], ["Decimal(20, 4)"], [vals], len(vals))
        got = list(_ch_core.ColBatch.decode_native(encoded).column_data(0))
        expected = [decimal.Decimal("123.4567"), decimal.Decimal("-1.5000")]
        assert got == expected
        assert [v.as_tuple() for v in got] == [e.as_tuple() for e in expected]
