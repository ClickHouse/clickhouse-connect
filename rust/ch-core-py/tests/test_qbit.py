from helpers import (
    _bfloat16_value,
    _ch_core,
    _NdarrayLikeColumn,
    build_native_block_from_bodies,
    math,
    pytest,
)


def _round_trip(type_name, values):
    encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
    return _ch_core.ColBatch.decode_native(encoded)


@pytest.mark.parametrize(
    ("type_name", "values", "expected"),
    [
        (
            "QBit(BFloat16, 3)",
            [[1.0, -2.5, 3.25], [0.1, float("inf"), float("-inf")]],
            [
                [_bfloat16_value(v) for v in [1.0, -2.5, 3.25]],
                [_bfloat16_value(v) for v in [0.1, float("inf"), float("-inf")]],
            ],
        ),
        (
            "QBit(Float32, 9)",
            [
                [float(v) / 3 for v in range(9)],
                [-0.0, 1.0, -2.0, 3.5, float("inf"), float("-inf"), 7.25, 8.5, 9.75],
            ],
            [
                [float(v) / 3 for v in range(9)],
                [-0.0, 1.0, -2.0, 3.5, float("inf"), float("-inf"), 7.25, 8.5, 9.75],
            ],
        ),
        (
            "QBit(Float64, 5)",
            [[-0.99105519, 1.28887844, -0.43526649, -0.98520696, 0.66154391]],
            [[-0.99105519, 1.28887844, -0.43526649, -0.98520696, 0.66154391]],
        ),
    ],
)
def test_python_exits_round_trip_all_element_widths(type_name, values, expected):
    batch = _round_trip(type_name, values)
    columns = list(batch.column_data(0))
    assert len(columns) == len(expected)
    for actual, wanted in zip(columns, expected):
        assert actual == pytest.approx(wanted, rel=1e-6, abs=0.0, nan_ok=True)
    assert list(batch.to_python_columns()[0]) == columns
    assert [row[0] for row in batch.to_python_rows()] == columns


def test_nullable_and_composite_shapes_round_trip():
    names = ["n", "a", "t", "at", "m", "v", "nt"]
    types = [
        "Nullable(QBit(Float32, 3))",
        "Array(QBit(Float64, 2))",
        "Tuple(QBit(Float32, 3), Nullable(QBit(BFloat16, 2)))",
        "Array(Tuple(QBit(Float32, 1), UInt8))",
        "Map(String, QBit(Float32, 2))",
        "Variant(QBit(Float32, 2), String)",
        "Nullable(Tuple(QBit(Float32, 2), UInt8))",
    ]
    columns = [
        [[1.0, 2.0, 3.0], None, [-1.0, -2.0, -3.0]],
        [[[1.0, 2.0]], [], [[3.0, 4.0], [5.0, 6.0]]],
        [
            ([1.0, 2.0, 3.0], [4.0, 5.0]),
            ([6.0, 7.0, 8.0], None),
            ([9.0, 10.0, 11.0], [12.0, 13.0]),
        ],
        [[([1.0], 13)], [], [([2.0], 79), ([3.0], 5)]],
        [{"a": [1.0, 2.0]}, {}, {"b": [3.0, 4.0]}],
        [[1.0, 2.0], "value", None],
        [([1.0, 2.0], 13), None, ([3.0, 4.0], 79)],
    ]
    encoded = _ch_core.encode_native_block(names, types, columns, 3)
    batch = _ch_core.ColBatch.decode_native(encoded)
    assert list(batch.to_python_rows()) == list(zip(*columns))


def test_exact_and_generic_outer_containers_encode_identically():
    values = [[1.0, 2.0, 3.0], [-4.0, 5.5, 6.25]]
    expected = _ch_core.encode_native_block(["v"], ["QBit(Float64, 3)"], [values], 2)
    assert _ch_core.encode_native_block(["v"], ["QBit(Float64, 3)"], [tuple(map(tuple, values))], 2) == expected
    assert _ch_core.encode_native_block(["v"], ["QBit(Float64, 3)"], [_NdarrayLikeColumn(values)], 2) == expected


@pytest.mark.parametrize("dimension", [1, 7, 8, 9, 15, 16, 17])
def test_float32_byte_boundary_dimensions(dimension):
    values = [[float(index) - 8.5 for index in range(dimension)]]
    result = list(_round_trip(f"QBit(Float32, {dimension})", values).column_data(0))
    assert result[0] == pytest.approx(values[0], rel=1e-6)


def test_dimension_nine_exact_native_byte_group_order():
    row_1 = [-0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -0.0, -0.0]
    row_2 = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -0.0]
    # Only plane zero, the sign bit, is nonzero. Native is plane-major and
    # row-major within the plane. The byte groups inside each row are reversed.
    body = b"\x01\x81\x01\x00" + bytes(31 * 2 * 2)
    native = build_native_block_from_bodies([("v", "QBit(Float32, 9)", body)], 2)

    assert _ch_core.encode_native_block(["v"], ["QBit(Float32, 9)"], [[row_1, row_2]], 2) == native
    decoded = list(_ch_core.ColBatch.decode_native(native).column_data(0))
    assert [math.copysign(1.0, value) for value in decoded[0]] == [
        -1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        -1.0,
        -1.0,
    ]
    assert [math.copysign(1.0, value) for value in decoded[1]] == [1.0] * 8 + [-1.0]


@pytest.mark.parametrize(
    ("type_name", "plane_count"),
    [("QBit(BFloat16, 9)", 16), ("QBit(Float64, 9)", 64)],
)
def test_dimension_nine_exact_native_bytes_other_widths(type_name, plane_count):
    row = [-0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -0.0]
    body = b"\x01\x01" + bytes((plane_count - 1) * 2)
    native = build_native_block_from_bodies([("v", type_name, body)], 1)

    encoded = _ch_core.encode_native_block(["v"], [type_name], [[row]], 1)
    assert encoded == native
    decoded = list(_ch_core.ColBatch.decode_native(native).column_data(0))[0]
    assert [math.copysign(1.0, value) for value in decoded] == [-1.0] + [1.0] * 7 + [-1.0]


@pytest.mark.parametrize(
    ("alias", "canonical"),
    [
        ("FLOAT", "Float32"),
        ("real", "Float32"),
        ("SINGLE", "Float32"),
        ("DOUBLE", "Float64"),
        ("double precision", "Float64"),
    ],
)
def test_float_aliases_resolve_to_canonical_qbit_type(alias, canonical):
    encoded = _ch_core.encode_native_block(["v"], [f"QBit({alias}, 2)"], [[[1.0, 2.0]]], 1)
    assert _ch_core.ColBatch.decode_native(encoded).column_type_names == [f"QBit({canonical}, 2)"]


@pytest.mark.parametrize(
    ("type_name", "dtype"),
    [
        ("QBit(BFloat16, 3)", "float32"),
        ("QBit(BFloat16, 3)", "float64"),
        ("QBit(Float32, 3)", "float32"),
        ("QBit(Float32, 3)", "float64"),
        ("QBit(Float64, 3)", "float32"),
        ("QBit(Float64, 3)", "float64"),
    ],
)
def test_numpy_matrix_fast_path(type_name, dtype):
    np = pytest.importorskip("numpy")
    values = np.array([[1.0, 2.0, 3.0], [-4.0, 5.5, 6.25]], dtype=dtype)
    expected_rows = list(_round_trip(type_name, values.tolist()).column_data(0))
    batch = _round_trip(type_name, values)
    for actual, expected in zip(batch.column_data(0), expected_rows):
        assert actual == pytest.approx(expected)


def test_nullable_numpy_matrix_fast_path_is_all_valid():
    np = pytest.importorskip("numpy")
    values = np.array([[1.0, 2.0, 3.0], [-4.0, 5.5, 6.25]], dtype="float32")
    batch = _round_trip("Nullable(QBit(Float32, 3))", values)
    assert list(batch.column_data(0)) == values.tolist()


def test_bfloat16_numpy_matrix_rejects_out_of_range_value():
    np = pytest.importorskip("numpy")
    values = np.array([[1e300]], dtype="float64")
    with pytest.raises(ValueError, match="row 0 element 0 cannot be converted to BFloat16"):
        _ch_core.encode_native_block(["v"], ["QBit(BFloat16, 1)"], [values], 1)


def test_zero_row_encode():
    batch = _round_trip("QBit(Float32, 9)", [])
    assert batch.num_rows == 0
    assert list(batch.column_data(0)) == []


def test_arrow_fixed_size_list_export():
    pa = pytest.importorskip("pyarrow")
    values = [[1.0, 2.0, 3.0], None, [-4.0, 5.5, 6.25]]
    batch = _round_trip("Nullable(QBit(Float32, 3))", values)
    table = pa.RecordBatchReader.from_stream(batch).read_all()
    assert pa.types.is_fixed_size_list(table.schema.field("v").type)
    assert table.schema.field("v").type.list_size == 3
    assert table.schema.field("v").type.value_type == pa.float32()
    assert table.column("v").to_pylist() == values


@pytest.mark.parametrize(
    ("type_name", "values", "message"),
    [
        ("QBit(Float32, 3)", [[1.0, 2.0]], "dimension mismatch"),
        ("QBit(Float32, 3)", [[1.0, 2.0, 3.0, 4.0]], "dimension mismatch"),
        ("QBit(Float32, 3)", [None], r"is None but QBit\(Float32, 3\) is not Nullable"),
        ("QBit(Float32, 3)", [[1.0, "bad", 3.0]], "element 1 cannot be converted"),
        ("QBit(Float32, 3)", [13], "is not a QBit vector"),
        ("QBit(BFloat16, 1)", [[1e300]], "element 0 cannot be converted to BFloat16"),
    ],
)
def test_insert_errors_identify_row_and_element(type_name, values, message):
    with pytest.raises(ValueError, match=message):
        _ch_core.encode_native_block(["v"], [type_name], [values], len(values))


def test_special_float_bits_survive_round_trip():
    values = [[float("nan"), float("inf"), float("-inf"), -0.0]]
    result = list(_round_trip("QBit(Float64, 4)", values).column_data(0))[0]
    assert math.isnan(result[0])
    assert result[1:] == [float("inf"), float("-inf"), -0.0]


def test_inner_list_finalizer_resize_is_rejected():
    row = [None, 13.0]

    class MutatingFloat:
        def __init__(self, container):
            self._container = container

        def __float__(self):
            self._container[0] = 5.0
            return 7.0

        def __del__(self):
            self._container.clear()

    row[0] = MutatingFloat(row)
    with pytest.raises(ValueError, match="resized during encoding"):
        _ch_core.encode_native_block(["v"], ["QBit(Float32, 2)"], [[row]], 1)


def test_outer_list_finalizer_resize_is_rejected():
    rows = [None, [13.0]]

    class MutatingVector:
        def __init__(self, container):
            self._container = container

        def __len__(self):
            return 1

        def __getitem__(self, index):
            self._container[0] = [5.0]
            return 7.0

        def __del__(self):
            self._container.clear()

    rows[0] = MutatingVector(rows)
    with pytest.raises(ValueError, match="resized during encoding"):
        _ch_core.encode_native_block(["v"], ["QBit(Float32, 1)"], [rows], 2)
