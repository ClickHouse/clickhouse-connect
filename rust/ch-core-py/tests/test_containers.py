from helpers import (
    ZoneInfo,
    _ch_core,
    _encode_varint,
    _encode_varint_string,
    _NdarrayLikeColumn,
    build_native_block,
    build_native_block_from_bodies,
    decimal,
    dt,
    ipaddress,
    pytest,
    struct,
    sys,
    uuid,
)

# ---------------------------------------------------------------------------
# Array
# ---------------------------------------------------------------------------

_ARR_KNOWN_UUID = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
_ARR_NY = ZoneInfo("America/New_York")

# DateTime element: tz-aware New York input round-trips to tz-aware New York.
_ARR_DT_TZ_ROWS = [
    [dt.datetime(2024, 1, 15, 7, 34, 56, tzinfo=_ARR_NY)],
    [],
    [
        dt.datetime(2000, 6, 15, 12, 0, 0, tzinfo=_ARR_NY),
        dt.datetime(1970, 1, 1, 0, 0, 0, tzinfo=_ARR_NY),
    ],
]

# DateTime64(3) has no timezone, so tz-aware UTC input decodes to the naive UTC
# wall clock; passing UTC keeps the expectation independent of the host locale.
_ARR_DT64_UTC_ROWS = [
    [dt.datetime(2024, 1, 15, 12, 34, 56, 789000, tzinfo=dt.timezone.utc)],
    [],
    [
        dt.datetime(1970, 1, 1, 0, 0, 0, 1000, tzinfo=dt.timezone.utc),
        dt.datetime(2000, 6, 15, 12, 0, 0, 500000, tzinfo=dt.timezone.utc),
    ],
]
_ARR_DT64_EXPECTED = [
    [dt.datetime(2024, 1, 15, 12, 34, 56, 789000)],
    [],
    [
        dt.datetime(1970, 1, 1, 0, 0, 0, 1000),
        dt.datetime(2000, 6, 15, 12, 0, 0, 500000),
    ],
]

# (type_name, py_rows, expected) with expected=None meaning it equals py_rows.
# Every case has multiple rows, at least one empty array, ragged lengths, and
# nullable element types mix None with values.
_ARR_ROUND_TRIP = [
    ("Array(Int32)", [[13, 79], [], [-1, 0, 2147483647], [7]], None),
    ("Array(Int64)", [[13], [], [9223372036854775807, -9223372036854775808], [5, 5]], None),
    ("Array(UInt64)", [[0, 18446744073709551615], [], [13, 79, 5]], None),
    ("Array(Float64)", [[1.5, -2.5], [], [0.0, 1e300]], None),
    ("Array(Bool)", [[True, False, True], [], [False]], None),
    ("Array(String)", [["user_1", "user_2"], [], ["", "sventon"]], None),
    ("Array(FixedString(3))", [[b"abc", b"xyz"], [], [b"a\x00\x00"]], None),
    ("Array(Date)", [[dt.date(2024, 1, 2), dt.date(1970, 1, 1)], [], [dt.date(2000, 6, 15)]], None),
    ("Array(DateTime('America/New_York'))", _ARR_DT_TZ_ROWS, None),
    ("Array(DateTime64(3))", _ARR_DT64_UTC_ROWS, _ARR_DT64_EXPECTED),
    ("Array(UUID)", [[uuid.UUID(int=0), _ARR_KNOWN_UUID], [], [uuid.UUID(int=79)]], None),
    (
        "Array(IPv4)",
        [
            [ipaddress.IPv4Address("0.0.0.0"), ipaddress.IPv4Address("1.2.3.4")],
            [],
            [ipaddress.IPv4Address("255.255.255.255")],
        ],
        None,
    ),
    (
        "Array(IPv6)",
        [
            [ipaddress.IPv6Address("::1")],
            [],
            [ipaddress.IPv6Address("2001:db8::1"), ipaddress.IPv6Address("::ffff:1.2.3.4")],
        ],
        None,
    ),
    (
        "Array(Decimal(9, 2))",
        [[decimal.Decimal("1.00"), decimal.Decimal("-3.50")], [], [decimal.Decimal("9999999.99")]],
        None,
    ),
    (
        "Array(Decimal(38, 10))",
        [[decimal.Decimal("1.5"), decimal.Decimal("-2.25")], [], [decimal.Decimal("0")]],
        None,
    ),
    ("Array(Enum8('a' = 1, 'b' = 2))", [["a", "b"], [], ["b", "a", "b"]], None),
    ("Array(Nullable(Int32))", [[13, None, 79], [], [None], [7]], None),
    ("Array(Nullable(String))", [["user_1", None], [], [None, "x"]], None),
    ("Array(LowCardinality(String))", [["red", "green", "red"], [], ["blue"]], None),
    ("Array(LowCardinality(Nullable(String)))", [["x", None, "x"], [], [None, "y"]], None),
    ("Array(Array(Int32))", [[[13, 79], [5]], [], [[7]], [[], [1, 2, 3]]], None),
]

# (type_name, py_rows, wire_rows, expected): wire_rows is the raw wire form the
# helper serializes, py_rows is what the encoder is given, expected is the decode.
# Excludes LowCardinality-in-array because the encoder sets the index word's
# NeedUpdateDictionary bit that the helper does not, so their bytes differ.
_ARR_GOLDEN = [
    ("Array(Int32)", [[13, 79], [], [7]], [[13, 79], [], [7]], [[13, 79], [], [7]]),
    ("Array(String)", [["user_1", "user_2"], [], ["x"]], [["user_1", "user_2"], [], ["x"]], [["user_1", "user_2"], [], ["x"]]),
    ("Array(FixedString(3))", [[b"abc"], [], [b"xyz", b"a\x00\x00"]], [[b"abc"], [], [b"xyz", b"a\x00\x00"]], [[b"abc"], [], [b"xyz", b"a\x00\x00"]]),
    ("Array(UInt64)", [[0, 18446744073709551615], [], [13]], [[0, 18446744073709551615], [], [13]], [[0, 18446744073709551615], [], [13]]),
    ("Array(Nullable(Int32))", [[13, None], [], [7]], [[13, None], [], [7]], [[13, None], [], [7]]),
    ("Array(Array(Int32))", [[[13, 79], [5]], [], [[7]]], [[[13, 79], [5]], [], [[7]]], [[[13, 79], [5]], [], [[7]]]),
    (
        "Array(UUID)",
        [[uuid.UUID(int=0), _ARR_KNOWN_UUID], [], [uuid.UUID(int=79)]],
        [[uuid.UUID(int=0), _ARR_KNOWN_UUID], [], [uuid.UUID(int=79)]],
        [[uuid.UUID(int=0), _ARR_KNOWN_UUID], [], [uuid.UUID(int=79)]],
    ),
    (
        "Array(IPv4)",
        [[ipaddress.IPv4Address("1.2.3.4")], [], [ipaddress.IPv4Address("0.0.0.0"), ipaddress.IPv4Address("255.255.255.255")]],
        [[ipaddress.IPv4Address("1.2.3.4")], [], [ipaddress.IPv4Address("0.0.0.0"), ipaddress.IPv4Address("255.255.255.255")]],
        [[ipaddress.IPv4Address("1.2.3.4")], [], [ipaddress.IPv4Address("0.0.0.0"), ipaddress.IPv4Address("255.255.255.255")]],
    ),
    (
        "Array(IPv6)",
        [[ipaddress.IPv6Address("::1")], [], [ipaddress.IPv6Address("2001:db8::1")]],
        [[ipaddress.IPv6Address("::1")], [], [ipaddress.IPv6Address("2001:db8::1")]],
        [[ipaddress.IPv6Address("::1")], [], [ipaddress.IPv6Address("2001:db8::1")]],
    ),
    (
        "Array(Date)",
        [[dt.date(2024, 1, 2)], [], [dt.date(1970, 1, 1), dt.date(2000, 6, 15)]],
        [
            [dt.date(2024, 1, 2).toordinal() - 719163],
            [],
            [0, dt.date(2000, 6, 15).toordinal() - 719163],
        ],
        [[dt.date(2024, 1, 2)], [], [dt.date(1970, 1, 1), dt.date(2000, 6, 15)]],
    ),
    (
        "Array(DateTime64(3))",
        [
            [dt.datetime(2024, 1, 15, 12, 34, 56, 789000, tzinfo=dt.timezone.utc)],
            [],
            [dt.datetime(1970, 1, 1, 0, 0, 0, 1000, tzinfo=dt.timezone.utc)],
        ],
        [[1705322096789], [], [1]],
        [
            [dt.datetime(2024, 1, 15, 12, 34, 56, 789000)],
            [],
            [dt.datetime(1970, 1, 1, 0, 0, 0, 1000)],
        ],
    ),
    (
        "Array(Decimal(9, 2))",
        [[decimal.Decimal("1.00"), decimal.Decimal("-3.50")], [], [decimal.Decimal("0.00")]],
        [[100, -350], [], [0]],
        [[decimal.Decimal("1.00"), decimal.Decimal("-3.50")], [], [decimal.Decimal("0.00")]],
    ),
    (
        "Array(Enum8('a' = 1, 'b' = 2))",
        [["a", "b"], [], ["a"]],
        [[1, 2], [], [1]],
        [["a", "b"], [], ["a"]],
    ),
]


class TestArray:
    @pytest.mark.parametrize("type_name,py_rows,expected", _ARR_ROUND_TRIP)
    def test_round_trip(self, type_name, py_rows, expected):
        if expected is None:
            expected = py_rows
        encoded = _ch_core.encode_native_block(["a"], [type_name], [py_rows], len(py_rows))
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == expected

    @pytest.mark.parametrize("type_name,py_rows,wire_rows,expected", _ARR_GOLDEN)
    def test_golden_bytes(self, type_name, py_rows, wire_rows, expected):
        encoded = _ch_core.encode_native_block(["a"], [type_name], [py_rows], len(py_rows))
        built = build_native_block([("a", type_name, wire_rows)])
        assert encoded == built
        assert list(_ch_core.ColBatch.decode_native(built).column_data(0)) == expected

    def test_golden_decode_low_cardinality_in_array(self):
        # encode == build is not asserted: the encoder sets the index word's
        # NeedUpdateDictionary bit while the helper does not, so their bytes
        # differ. Cover it by decode-of-helper-bytes plus an encode round-trip.
        rows = [["red", "green", "red"], [], ["blue"]]
        built = build_native_block([("c", "Array(LowCardinality(String))", rows)])
        assert list(_ch_core.ColBatch.decode_native(built).column_data(0)) == rows
        encoded = _ch_core.encode_native_block(
            ["c"], ["Array(LowCardinality(String))"], [rows], len(rows)
        )
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows

    def test_low_cardinality_in_array_all_empty(self):
        # Every array empty (total_elements == 0): the LowCardinality element
        # body is absent entirely, so only the hoisted key version and the zero
        # offsets remain. With no index word to differ, encode == build holds.
        rows = [[], [], []]
        built = build_native_block([("c", "Array(LowCardinality(String))", rows)])
        encoded = _ch_core.encode_native_block(
            ["c"], ["Array(LowCardinality(String))"], [rows], len(rows)
        )
        assert encoded == built
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows

    @pytest.mark.parametrize(
        "type_name,rows",
        [
            ("Array(Int32)", [[13, 79], [], [7, 5, 5]]),
            ("Array(Nullable(String))", [["user_1", None], [], [None, "x"]]),
            ("Array(Enum8('a' = 1, 'b' = 2))", [["a", "b"], [], ["b", "a", "b"]]),
            ("Array(DateTime('America/New_York'))", _ARR_DT_TZ_ROWS),
            ("Array(UUID)", [[uuid.UUID(int=0), _ARR_KNOWN_UUID], [], [uuid.UUID(int=79)]]),
            ("Array(LowCardinality(Nullable(String)))", [["x", None, "x"], [], [None, "y"]]),
            ("Array(Array(Int64))", [[[13, 79], [5]], [], [[7]], [[], [1, 2, 3]]]),
        ],
    )
    def test_all_exit_paths_agree(self, type_name, rows):
        encoded = _ch_core.encode_native_block(["a"], [type_name], [rows], len(rows))
        batch = _ch_core.ColBatch.decode_native(encoded)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == rows

    def test_multi_block_concatenation(self):
        rows_a = [[13, 79], [], [7]]
        rows_b = [[5], [1, 2], []]
        block_a = _ch_core.encode_native_block(["a"], ["Array(Int32)"], [rows_a], len(rows_a))
        block_b = _ch_core.encode_native_block(["a"], ["Array(Int32)"], [rows_b], len(rows_b))
        merged = _ch_core.ColBatch.from_batches(
            [
                _ch_core.ColBatch.decode_native(block_a),
                _ch_core.ColBatch.decode_native(block_b),
            ]
        )
        assert merged.num_chunks == 2
        assert list(merged.column_data(0)) == rows_a + rows_b
        concat = _ch_core.ColBatch.decode_native(block_a + block_b)
        assert list(concat.column_data(0)) == rows_a + rows_b

    def test_zero_rows(self):
        encoded = _ch_core.encode_native_block(["a"], ["Array(Int32)"], [[]], 0)
        assert encoded == build_native_block([("a", "Array(Int32)", [])])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.num_rows == 0
        assert batch.column_type_names == ["Array(Int32)"]
        assert list(batch.column_data(0)) == []

    def test_nullable_array_type_rejected(self):
        with pytest.raises(NotImplementedError, match="unsupported ClickHouse type"):
            _ch_core.encode_native_block(["a"], ["Nullable(Array(Int32))"], [[[13]]], 1)

    def test_none_row_rejected(self):
        with pytest.raises(ValueError, match="is None but Array"):
            _ch_core.encode_native_block(["a"], ["Array(Int32)"], [[None]], 1)

    def test_bare_str_row_rejected(self):
        with pytest.raises(ValueError, match="is a str, not an Array sequence"):
            _ch_core.encode_native_block(["a"], ["Array(Int32)"], [["abc"]], 1)

    def test_bytes_like_rows_flatten_as_int_elements(self):
        rows = [b"\x01\x02", bytearray(b"\x03"), memoryview(b"\x04\x05"), []]
        expected = _ch_core.encode_native_block(
            ["a"], ["Array(UInt8)"], [[[1, 2], [3], [4, 5], []]], 4
        )
        assert _ch_core.encode_native_block(["a"], ["Array(UInt8)"], [rows], 4) == expected

    def test_bytes_row_for_string_elements_raises_conversion_error(self):
        with pytest.raises(ValueError, match="row 0 element 0 cannot be converted to String"):
            _ch_core.encode_native_block(["a"], ["Array(String)"], [[b"ab"]], 1)

    def test_element_error_reports_outer_row_and_element(self):
        for outer in ([[1, 2], ["x"]], _NdarrayLikeColumn([[1, 2], ["x"]])):
            with pytest.raises(
                ValueError, match='column "v" row 1 element 0 cannot be converted to Int32'
            ):
                _ch_core.encode_native_block(["v"], ["Array(Int32)"], [outer], 2)
        with pytest.raises(
            ValueError, match='column "v" row 0 element 1 is None but Int32 is not Nullable'
        ):
            _ch_core.encode_native_block(["v"], ["Array(Int32)"], [[[1, None]]], 1)

    def test_nested_element_error_reports_outer_path(self):
        rows = [[[1]], [[2], ["x"]]]
        with pytest.raises(
            ValueError,
            match='column "v" row 1 element 1 element 0 cannot be converted to Int32',
        ):
            _ch_core.encode_native_block(["v"], ["Array(Array(Int32))"], [rows], 2)

    def test_array_lc_reuses_dictionary_objects(self):
        rows = [["red", "red", "green"], [], ["red", "green"]]
        encoded = _ch_core.encode_native_block(
            ["c"], ["Array(LowCardinality(String))"], [rows], len(rows)
        )
        batch = _ch_core.ColBatch.decode_native(encoded)
        col = batch.column_data(0)
        assert list(col) == rows
        assert col[0][0] is col[0][1]
        assert col[0][0] is col[2][0]
        cols = batch.to_python_columns()[0]
        assert cols[0][0] is cols[0][1] and cols[0][0] is cols[2][0]
        out_rows = batch.to_python_rows()
        assert out_rows[0][0][0] is out_rows[0][0][1]
        assert out_rows[0][0][0] is out_rows[2][0][0]

    def test_nested_array_lc_reuses_dictionary_objects(self):
        rows = [[["a", "a"], ["a"]], [], [["a"]]]
        encoded = _ch_core.encode_native_block(
            ["c"], ["Array(Array(LowCardinality(String)))"], [rows], len(rows)
        )
        batch = _ch_core.ColBatch.decode_native(encoded)
        col = batch.column_data(0)
        assert list(col) == rows
        assert col[0][0][0] is col[0][1][0]
        assert col[0][0][0] is col[2][0][0]

    def test_array_lc_nullable_reuse_and_values(self):
        rows = [["x", None, "x"], [], [None, "x"]]
        encoded = _ch_core.encode_native_block(
            ["c"], ["Array(LowCardinality(Nullable(String)))"], [rows], len(rows)
        )
        col = _ch_core.ColBatch.decode_native(encoded).column_data(0)
        assert list(col) == rows
        assert col[0][0] is col[0][2]
        assert col[0][0] is col[2][1]

    def test_json_element_round_trip(self):
        rows = [[{"a": 13}, {"b": "user_1"}], [], [{"c": [1, 2]}]]
        encoded = _ch_core.encode_native_block(["a"], ["Array(JSON)"], [rows], len(rows))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows

    def test_deeply_nested_type_rejected_not_crash(self):
        # Past the parser depth cap the type is rejected without a stack overflow.
        deep = "Array(" * 200 + "Int32" + ")" * 200
        with pytest.raises(NotImplementedError):
            _ch_core.encode_native_block(["c"], [deep], [[]], 0)

    def test_unordered_row_rejected(self):
        for bad in ({3, 1, 2}, {13: 0, 79: 0}):
            with pytest.raises(ValueError):
                _ch_core.encode_native_block(["v"], ["Array(Int32)"], [[bad]], 1)

    def test_malformed_offsets_decode_error(self):
        buf = bytearray()
        buf.extend(_encode_varint(1))
        buf.extend(_encode_varint(2))
        buf.extend(_encode_varint_string("a"))
        buf.extend(_encode_varint_string("Array(Int32)"))
        buf.extend(struct.pack("<Q", 2))  # row 0 end offset
        buf.extend(struct.pack("<Q", 1))  # row 1 end offset decreases -> invalid
        with pytest.raises(ValueError, match="Invalid Array layout"):
            _ch_core.ColBatch.decode_native(bytes(buf))


class TestArrayInsertFastPath:
    """Exact list/tuple row flattening into one flat element run."""

    def _encode(self, type_name, rows, row_count=None):
        n = len(rows) if row_count is None else row_count
        return _ch_core.encode_native_block(["v"], [type_name], [rows], n)

    def test_row_and_outer_container_kinds_agree(self):
        rows = [[13, 79], [], [7, 5, 5]]
        from_lists = self._encode("Array(Int64)", rows)
        assert from_lists == build_native_block([("v", "Array(Int64)", rows)])
        assert self._encode("Array(Int64)", tuple(rows), 3) == from_lists
        assert self._encode("Array(Int64)", [tuple(r) for r in rows]) == from_lists
        assert self._encode("Array(Int64)", _NdarrayLikeColumn(rows), 3) == from_lists

    def test_exotic_row_containers_match_list_rows(self):
        expected = self._encode("Array(Int64)", [[0, 1, 2], [7], [], [3, 4]])
        rows = [range(3), _NdarrayLikeColumn([7]), (x for x in []), (3, 4)]
        assert self._encode("Array(Int64)", rows, 4) == expected

    def test_mixed_rows_agree_across_outer_containers(self):
        rows = [[1, 2], (3,), range(2)]
        expected = self._encode("Array(Int64)", [[1, 2], [3], [0, 1]])
        assert self._encode("Array(Int64)", rows, 3) == expected
        assert self._encode("Array(Int64)", _NdarrayLikeColumn(rows), 3) == expected

    def test_nested_tuple_rows(self):
        rows = [((1, 2), [3]), [], [(7,)], [[], (1, 2, 3)]]
        expected_rows = [[[1, 2], [3]], [], [[7]], [[], [1, 2, 3]]]
        encoded = self._encode("Array(Array(Int64))", rows, 4)
        assert encoded == self._encode("Array(Array(Int64))", expected_rows)
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == expected_rows

    def test_nullable_element_tuple_rows(self):
        rows = [(13, None), (), (None, 7)]
        encoded = self._encode("Array(Nullable(Int64))", rows, 3)
        assert encoded == self._encode("Array(Nullable(Int64))", [list(r) for r in rows])
        expected = [[13, None], [], [None, 7]]
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == expected

    def test_string_and_lc_tuple_rows(self):
        rows = [("a", "bb"), (), ("a",)]
        for type_name in ("Array(String)", "Array(LowCardinality(String))"):
            fast = self._encode(type_name, rows, 3)
            assert fast == self._encode(type_name, [list(r) for r in rows])

    def test_lc_non_string_element_round_trip(self):
        rows = [[13, 79, 13], [], [79]]
        encoded = self._encode("Array(LowCardinality(UInt32))", rows)
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows

    def test_empty_rows_tuple_outer(self):
        assert self._encode("Array(Int64)", (), 0) == build_native_block(
            [("v", "Array(Int64)", [])]
        )
        assert self._encode("Array(Int64)", ((), (), ()), 3) == build_native_block(
            [("v", "Array(Int64)", [[], [], []])]
        )

    def test_invalid_row_in_fallback_reports_row(self):
        with pytest.raises(ValueError, match="row 1 is not a valid Array value"):
            self._encode("Array(Int64)", [[1], 5, [3]], 3)

    def test_row_fallback_preserves_cause(self):
        class Boom(Exception):
            pass

        class EvilRow:
            def __iter__(self):
                raise Boom("iteration exploded")

        with pytest.raises(ValueError, match="row 1 is not a valid Array value") as excinfo:
            self._encode("Array(Int64)", [[1], EvilRow()], 2)
        assert isinstance(excinfo.value.__cause__, Boom)

    def test_flat_refs_refcount_balance(self):
        shared = 1 << 40

        rows = [[shared, shared], [], [shared]]
        before = sys.getrefcount(shared)
        self._encode("Array(Int64)", rows, 3)
        assert sys.getrefcount(shared) == before

        rows = [[shared], ["x"]]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match="element 0 cannot be converted"):
            self._encode("Array(Int64)", rows, 2)
        assert sys.getrefcount(shared) == before

        rows = [[shared], 5]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match="not a valid Array value"):
            self._encode("Array(Int64)", rows, 2)
        assert sys.getrefcount(shared) == before

    def test_outer_list_resized_during_row_fallback_raises(self):
        rows = [[1], None, [3], [4]]

        class Evil:
            def __iter__(self):
                del rows[2:]
                return iter([7])

        rows[1] = Evil()
        with pytest.raises(ValueError, match="resized during encoding"):
            self._encode("Array(Int64)", rows, 4)

    def test_outer_list_resized_during_element_fallback_encodes_snapshot(self):
        # Element conversion runs Python that clears the outer list after the
        # flatten pass; the flat run holds strong references, so the original
        # rows still encode.
        rows = [[1], [], [3]]

        class Evil:
            def __index__(self):
                rows.clear()
                return 9

        rows[1] = [Evil()]
        expected = self._encode("Array(Int64)", [[1], [9], [3]])
        assert self._encode("Array(Int64)", rows, 3) == expected


class TestTupleInsert:
    """Tuple(T1, ...) encode: positional and named-dict rows."""

    def _encode(self, type_name, rows, row_count=None):
        n = len(rows) if row_count is None else row_count
        return _ch_core.encode_native_block(["t"], [type_name], [rows], n)

    def _decode(self, encoded):
        return list(_ch_core.ColBatch.decode_native(encoded).column_data(0))

    def test_unnamed_rows_match_golden_and_round_trip(self):
        rows = [(13, "a"), (-1, ""), (79, "sventon")]
        encoded = self._encode("Tuple(Int32, String)", rows)
        assert encoded == build_native_block([("t", "Tuple(Int32, String)", rows)])
        assert self._decode(encoded) == rows

    def test_row_and_outer_container_kinds_agree(self):
        rows = [(13, "a"), (79, "b")]
        expected = self._encode("Tuple(Int32, String)", rows)
        assert self._encode("Tuple(Int32, String)", [list(r) for r in rows]) == expected
        assert self._encode("Tuple(Int32, String)", tuple(rows), 2) == expected
        assert self._encode("Tuple(Int32, String)", _NdarrayLikeColumn(rows), 2) == expected

    def test_generic_iterable_rows(self):
        expected = self._encode("Tuple(Int64, Int64)", [(0, 1), (5, 6)])
        assert self._encode("Tuple(Int64, Int64)", [range(2), (5, 6)], 2) == expected

    def test_named_tuple_positional_rows(self):
        rows = [(13, "x"), (79, "y")]
        encoded = self._encode("Tuple(a Int32, b String)", rows)
        assert encoded == build_native_block([("t", "Tuple(a Int32, b String)", rows)])
        assert self._decode(encoded) == [{"a": 13, "b": "x"}, {"a": 79, "b": "y"}]

    def test_named_tuple_dict_rows(self):
        dict_rows = [{"a": 13, "b": "x"}, {"a": 79, "b": "y"}]
        expected = self._encode("Tuple(a Int32, b String)", [(13, "x"), (79, "y")])
        assert self._encode("Tuple(a Int32, b String)", dict_rows) == expected
        # Extra keys are ignored.
        dict_rows = [{"a": 13, "b": "x", "zz": 5}, {"b": "y", "a": 79}]
        assert self._encode("Tuple(a Int32, b String)", dict_rows) == expected

    def test_backtick_named_tuple_dict_rows(self):
        type_name = "Tuple(`a b` Int32, c String)"
        expected = self._encode(type_name, [(13, "x")])
        assert expected == build_native_block([("t", type_name, [(13, "x")])])
        assert self._encode(type_name, [{"a b": 13, "c": "x"}]) == expected

    def test_dict_row_missing_key_nullable_becomes_none(self):
        type_name = "Tuple(a Int32, b Nullable(String))"
        assert self._encode(type_name, [{"a": 13}]) == self._encode(type_name, [(13, None)])

    def test_dict_row_missing_key_non_nullable_raises(self):
        with pytest.raises(ValueError, match='column "t" row 0 element "b" is None'):
            self._encode("Tuple(a Int32, b String)", [{"a": 13}])

    def test_dict_subclass_rows_read_via_get(self):
        import collections

        rows = [collections.OrderedDict([("a", 13), ("b", "x")])]
        assert self._encode("Tuple(a Int32, b String)", rows) == self._encode(
            "Tuple(a Int32, b String)", [(13, "x")]
        )

    def test_non_dict_row_in_dict_mode_raises(self):
        rows = [{"a": 13, "b": "x"}, (79, "y")]
        with pytest.raises(ValueError, match="row 1 cannot be read as a dict"):
            self._encode("Tuple(a Int32, b String)", rows, 2)

    def test_dict_row_in_positional_mode_raises(self):
        rows = [(13, "x"), {"a": 79, "b": "y"}]
        with pytest.raises(ValueError, match="row 1 is a dict but Tuple rows are read"):
            self._encode("Tuple(a Int32, b String)", rows, 2)
        with pytest.raises(ValueError, match="row 0 is a dict but Tuple rows are read"):
            self._encode("Tuple(Int32, String)", [{"a": 1}], 1)

    def test_arity_mismatch_raises(self):
        with pytest.raises(
            ValueError, match='column "t" row 1 has 3 elements but the Tuple declares 2'
        ):
            self._encode("Tuple(Int32, String)", [(1, "a"), (1, "a", "extra")], 2)
        with pytest.raises(ValueError, match="row 0 has 1 elements but the Tuple declares 2"):
            self._encode("Tuple(Int32, String)", [[1]], 1)

    def test_str_row_rejected(self):
        with pytest.raises(ValueError, match="row 0 is a str, not a Tuple row"):
            self._encode("Tuple(String, String)", ["ab"], 1)

    def test_none_row_non_nullable_raises(self):
        with pytest.raises(ValueError, match="row 1 is None but Tuple"):
            self._encode("Tuple(Int32, String)", [(1, "a"), None], 2)

    def test_element_error_names_column_row_and_element(self):
        with pytest.raises(ValueError, match='column "t" row 1 element 0 cannot be converted'):
            self._encode("Tuple(Int64, String)", [(1, "a"), ("x", "b")], 2)
        with pytest.raises(ValueError, match='column "t" row 0 element "b" cannot be converted'):
            self._encode("Tuple(a Int32, b Int32)", [{"a": 1, "b": "x"}])

    def test_lc_string_element(self):
        # The core encoder's LC flags word differs from the test helper's
        # (both valid), so LC types round-trip instead of golden-comparing.
        type_name = "Tuple(LowCardinality(String), Int64)"
        rows = [("red", 1), ("red", 2), ("blue", 3)]
        encoded = self._encode(type_name, rows)
        assert self._decode(encoded) == rows

    def test_array_and_nullable_elements(self):
        type_name = "Tuple(Array(Int64), Nullable(String))"
        rows = [([13, 79], "a"), ([], None)]
        encoded = self._encode(type_name, rows)
        assert self._decode(encoded) == rows

    def test_nested_tuple_and_map_elements(self):
        rows = [(1, (2, "x")), (3, (4, "y"))]
        encoded = self._encode("Tuple(UInt8, Tuple(UInt8, String))", rows)
        assert self._decode(encoded) == rows
        rows = [({"k1": 1, "k2": 2}, 10), ({}, 20)]
        encoded = self._encode("Tuple(Map(String, UInt8), Int32)", rows)
        assert self._decode(encoded) == rows

    def test_nullable_tuple_none_rows_match_golden_and_round_trip(self):
        type_name = "Nullable(Tuple(Nullable(Int32), String))"
        rows = [(13, "a"), None, (None, "b"), None]
        encoded = self._encode(type_name, rows)
        assert encoded == build_native_block([("t", type_name, rows)])
        assert self._decode(encoded) == rows

    def test_nullable_tuple_default_placeholders_cover_scalar_types(self):
        type_name = "Nullable(Tuple(UUID, IPv4, Date, Decimal(9, 2), Bool, Float64))"
        rows = [None, (uuid.UUID(int=13), ipaddress.IPv4Address("1.2.3.4"), 79, 5, True, 1.5)]
        encoded = self._encode(type_name, rows, 2)
        decoded = self._decode(encoded)
        assert decoded[0] is None
        assert decoded[1] == (
            uuid.UUID(int=13),
            ipaddress.IPv4Address("1.2.3.4"),
            dt.date(1970, 3, 21),
            decimal.Decimal("5.00"),
            True,
            1.5,
        )

    def test_nullable_named_tuple_dict_rows_with_none(self):
        type_name = "Nullable(Tuple(a UInt64, b String))"
        rows = [{"a": 13, "b": "x"}, None, {"a": 5, "b": "y"}]
        encoded = self._encode(type_name, rows, 3)
        assert self._decode(encoded) == [{"a": 13, "b": "x"}, None, {"a": 5, "b": "y"}]

    def test_empty_tuple(self):
        rows = [(), (), ()]
        encoded = self._encode("Tuple()", rows)
        assert encoded == build_native_block([("t", "Tuple()", rows)])
        assert self._decode(encoded) == rows
        with pytest.raises(ValueError, match="has 1 elements but the Tuple declares 0"):
            self._encode("Tuple()", [(1,)], 1)

    def test_array_of_tuple(self):
        type_name = "Array(Tuple(Int64, String))"
        rows = [[(1, "a"), (2, "b")], [], [(3, "c")]]
        encoded = self._encode(type_name, rows)
        assert self._decode(encoded) == rows

    def test_zero_row_probe(self):
        encoded = _ch_core.encode_native_block(
            ["t", "m"], ["Tuple(a Int32, b String)", "Map(String, UInt8)"], [[], []], 0
        )
        assert encoded == build_native_block(
            [("t", "Tuple(a Int32, b String)", []), ("m", "Map(String, UInt8)", [])]
        )

    def test_lc_tuple_and_nullable_map_still_rejected(self):
        with pytest.raises(NotImplementedError, match="unsupported"):
            _ch_core.encode_native_block(
                ["v"], ["LowCardinality(Tuple(Int32, String))"], [[(1, "a")]], 1
            )
        with pytest.raises(NotImplementedError, match="unsupported"):
            _ch_core.encode_native_block(["v"], ["Nullable(Map(String, UInt8))"], [[{}]], 1)

    def test_flat_refs_refcount_balance(self):
        shared = 1 << 40

        rows = [(shared, "a"), (shared, "b")]
        before = sys.getrefcount(shared)
        self._encode("Tuple(Int64, String)", rows)
        assert sys.getrefcount(shared) == before

        rows = [(shared, "a"), (shared, 5)]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match="element 1"):
            self._encode("Tuple(Int64, String)", rows, 2)
        assert sys.getrefcount(shared) == before

        rows = [(shared, "a"), (shared,)]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match="declares 2"):
            self._encode("Tuple(Int64, String)", rows, 2)
        assert sys.getrefcount(shared) == before

        rows = [{"a": shared, "b": "x"}, {"a": shared}]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match='element "b" is None'):
            self._encode("Tuple(a Int64, b String)", rows, 2)
        assert sys.getrefcount(shared) == before


class TestMapInsert:
    """Map(K, V) encode: dict rows flattened into key and value runs."""

    def _encode(self, type_name, rows, row_count=None):
        n = len(rows) if row_count is None else row_count
        return _ch_core.encode_native_block(["m"], [type_name], [rows], n)

    def _decode(self, encoded):
        return list(_ch_core.ColBatch.decode_native(encoded).column_data(0))

    def test_dict_rows_match_golden_and_round_trip(self):
        rows = [{"k1": 1, "k2": 2}, {}, {"x": 255}]
        encoded = self._encode("Map(String, UInt8)", rows)
        assert encoded == build_native_block([("m", "Map(String, UInt8)", rows)])
        assert self._decode(encoded) == rows

    def test_outer_container_kinds_agree(self):
        rows = [{13: "a", 79: "b"}, {}, {5: "c"}]
        expected = self._encode("Map(UInt64, String)", rows)
        assert self._encode("Map(UInt64, String)", tuple(rows), 3) == expected
        assert self._encode("Map(UInt64, String)", _NdarrayLikeColumn(rows), 3) == expected

    def test_all_empty_dict_rows(self):
        rows = [{}, {}, {}]
        encoded = self._encode("Map(String, UInt8)", rows)
        assert encoded == build_native_block([("m", "Map(String, UInt8)", rows)])
        assert self._decode(encoded) == rows

    def test_dict_subclass_rows_via_items(self):
        import collections

        rows = [collections.OrderedDict([("a", 13), ("b", 79)]), {}]
        assert self._encode("Map(String, UInt8)", rows) == self._encode(
            "Map(String, UInt8)", [{"a": 13, "b": 79}, {}]
        )

    def test_non_dict_row_rejected(self):
        with pytest.raises(ValueError, match="row 1 is not a dict for Map"):
            self._encode("Map(String, UInt8)", [{"a": 1}, [("b", 2)]], 2)
        with pytest.raises(ValueError, match="row 0 is not a dict for Map"):
            self._encode("Map(String, UInt8)", ["ab"], 1)

    def test_none_row_rejected(self):
        with pytest.raises(ValueError, match="row 1 is None but Map"):
            self._encode("Map(String, UInt8)", [{"a": 1}, None], 2)

    def test_nullable_value_type(self):
        rows = [{"a": 13, "b": None}, {}, {"c": 79}]
        encoded = self._encode("Map(String, Nullable(Int32))", rows)
        assert encoded == build_native_block([("m", "Map(String, Nullable(Int32))", rows)])
        assert self._decode(encoded) == rows

    def test_array_value_type(self):
        rows = [{"k": [13, 79], "e": []}, {}, {"s": [5]}]
        encoded = self._encode("Map(String, Array(Int64))", rows)
        assert self._decode(encoded) == rows

    def test_low_cardinality_string_key(self):
        rows = [{"red": 1, "blue": 2}, {}, {"red": 3}]
        encoded = self._encode("Map(LowCardinality(String), UInt64)", rows)
        assert self._decode(encoded) == rows

    def test_map_of_map_and_tuple_values(self):
        rows = [{1: {2: "a"}}, {}, {3: {}}]
        encoded = self._encode("Map(UInt64, Map(UInt64, String))", rows)
        assert self._decode(encoded) == rows
        rows = [{"k": (1, "x")}, {}]
        encoded = self._encode("Map(String, Tuple(UInt8, String))", rows)
        assert self._decode(encoded) == rows

    def test_key_and_value_errors_name_row_and_entry(self):
        with pytest.raises(ValueError, match='column "m" row 1 key 0 cannot be converted'):
            self._encode("Map(UInt8, UInt8)", [{1: 1}, {"x": 2}], 2)
        with pytest.raises(ValueError, match='column "m" row 0 value 1 cannot be converted'):
            self._encode("Map(String, UInt8)", [{"a": 1, "b": "x"}], 1)

    def test_flat_refs_refcount_balance(self):
        shared = 1 << 40

        rows = [{shared: shared}, {}, {1: shared}]
        before = sys.getrefcount(shared)
        self._encode("Map(UInt64, Int64)", rows)
        assert sys.getrefcount(shared) == before

        rows = [{shared: shared}, {shared: "x"}]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match="value 0 cannot be converted"):
            self._encode("Map(UInt64, Int64)", rows, 2)
        assert sys.getrefcount(shared) == before

        rows = [{shared: shared}, "boom"]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match="is not a dict for Map"):
            self._encode("Map(UInt64, Int64)", rows, 2)
        assert sys.getrefcount(shared) == before


# ---------------------------------------------------------------------------
# Tuple
# ---------------------------------------------------------------------------

_TUP_KNOWN_UUID = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")

# (type_name, wire_rows, expected). Each wire row is a sequence of element
# values; expected None means the decoded column equals wire_rows (as tuples).
_TUPLE_UNNAMED = [
    ("Tuple(Int32, String)", [(13, "user_1"), (-1, ""), (79, "sventon")], None),
    ("Tuple(UInt64, Float64, Bool)",
     [(0, 1.5, True), (18446744073709551615, -2.5, False)], None),
    ("Tuple(Int32)", [(13,), (79,), (-2147483648,)], None),
    ("Tuple(Nullable(Int32), String)", [(13, "a"), (None, "b"), (79, "c")], None),
    ("Tuple(UUID, IPv4)",
     [(uuid.UUID(int=0), ipaddress.IPv4Address("1.2.3.4")),
      (_TUP_KNOWN_UUID, ipaddress.IPv4Address("255.255.255.255"))], None),
    ("Tuple(LowCardinality(String), Int32)",
     [("red", 1), ("red", 2), ("blue", 3)], None),
    ("Tuple(Array(Int64), String)",
     [([13, 79], "a"), ([], "b"), ([5, 5, 5], "c")], None),
    ("Tuple(UInt8, Tuple(UInt8, String))",
     [(1, (2, "x")), (3, (4, "y"))], None),
    ("Tuple(Map(String, UInt8), Int32)",
     [({"k1": 1, "k2": 2}, 10), ({}, 20)], None),
]

_TUPLE_NAMED = [
    ("Tuple(a Int32, b String)",
     [(13, "x"), (79, "y")], [{"a": 13, "b": "x"}, {"a": 79, "b": "y"}]),
    ("Tuple(id UInt64, val Nullable(Int32))",
     [(1, 13), (2, None)], [{"id": 1, "val": 13}, {"id": 2, "val": None}]),
    ("Tuple(`a b` Int32, c String)",
     [(13, "x")], [{"a b": 13, "c": "x"}]),
]


class TestTuple:
    @pytest.mark.parametrize("type_name,wire_rows,expected", _TUPLE_UNNAMED)
    def test_unnamed_to_tuple(self, type_name, wire_rows, expected):
        if expected is None:
            expected = [tuple(r) for r in wire_rows]
        built = build_native_block([("t", type_name, wire_rows)])
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == expected
        assert all(isinstance(v, tuple) for v in decoded)

    @pytest.mark.parametrize("type_name,wire_rows,expected", _TUPLE_NAMED)
    def test_named_to_dict(self, type_name, wire_rows, expected):
        built = build_native_block([("t", type_name, wire_rows)])
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == expected
        assert all(isinstance(v, dict) for v in decoded)

    @pytest.mark.parametrize(
        "type_name,wire_rows,expected",
        [
            ("Tuple(Int32, String)", [(13, "a"), (79, "b")], [(13, "a"), (79, "b")]),
            ("Tuple(x Int32, y String)", [(13, "a")], [{"x": 13, "y": "a"}]),
            ("Tuple(Array(Int64), Nullable(String))",
             [([13], "a"), ([], None)], [([13], "a"), ([], None)]),
        ],
    )
    def test_all_exit_paths_agree(self, type_name, wire_rows, expected):
        built = build_native_block([("t", type_name, wire_rows)])
        batch = _ch_core.ColBatch.decode_native(built)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == expected

    def test_nullable_tuple_null_rows_are_none(self):
        # Nullable(Tuple) decodes null rows to None and non-null rows to their
        # value, across all exit paths. A nullable field inside a non-null tuple
        # keeps its own None. (clickhouse-connect mis-decodes this rare type, so
        # the Rust path is the correct reference, not a parity target.)
        type_name = "Nullable(Tuple(Nullable(Int32), String))"
        wire_rows = [(13, "a"), None, (None, "b"), None]
        expected = [(13, "a"), None, (None, "b"), None]
        batch = _ch_core.ColBatch.decode_native(build_native_block([("t", type_name, wire_rows)]))
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == expected

    def test_nullable_named_tuple_null_rows_are_none(self):
        type_name = "Nullable(Tuple(a UInt64, b String))"
        wire_rows = [(13, "x"), None, (5, "y")]
        expected = [{"a": 13, "b": "x"}, None, {"a": 5, "b": "y"}]
        batch = _ch_core.ColBatch.decode_native(build_native_block([("t", type_name, wire_rows)]))
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == expected

    def test_empty_tuple_is_empty_tuple_per_row(self):
        # Tuple() has one placeholder byte per row and decodes to an empty tuple.
        built = build_native_block([("t", "Tuple()", [(), (), ()])])
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == [(), (), ()]

    def test_decimal_and_date_element_machinery(self):
        # Decimal/Date elements carry raw wire values in the helper (unscaled
        # integer, epoch days) and decode through the recursive per-field
        # context: the Decimal field builds its scaled decimal.Decimal.
        day = (dt.date(2024, 1, 2) - dt.date(1970, 1, 1)).days
        wire_rows = [(100, day), (-350, 0)]
        built = build_native_block([("t", "Tuple(Decimal(9, 2), Date)", wire_rows)])
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == [
            (decimal.Decimal("1.00"), dt.date(2024, 1, 2)),
            (decimal.Decimal("-3.50"), dt.date(1970, 1, 1)),
        ]

    def test_multi_block_concatenation(self):
        type_name = "Tuple(Int32, String)"
        rows_a = [(13, "a"), (79, "b")]
        rows_b = [(5, "c")]
        block_a = build_native_block([("t", type_name, rows_a)])
        block_b = build_native_block([("t", type_name, rows_b)])
        decoded = list(_ch_core.ColBatch.decode_native(block_a + block_b).column_data(0))
        assert decoded == [(13, "a"), (79, "b"), (5, "c")]

    def test_truncated_tuple_raises_eof(self):
        built = build_native_block([("t", "Tuple(Int32, Int32)", [(13, 79)])])
        with pytest.raises(EOFError):
            _ch_core.ColBatch.decode_native(built[:-2])


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

_MAP_DECODE = [
    ("Map(String, UInt8)", [{"k1": 1, "k2": 2}, {}, {"x": 255}], None),
    ("Map(UInt64, String)", [{13: "a", 79: "b"}, {}, {5: "c"}], None),
    ("Map(String, Nullable(Int32))", [{"a": 13, "b": None}, {}, {"c": 79}], None),
    ("Map(LowCardinality(String), UInt64)",
     [{"red": 1, "blue": 2}, {}, {"red": 3}], None),
    ("Map(UInt8, Array(Int32))", [{1: [13, 79], 2: []}, {}, {3: [5]}], None),
    ("Map(String, Tuple(UInt8, String))", [{"k": (1, "x")}, {}], None),
    ("Map(UInt64, Map(UInt64, String))", [{1: {2: "a"}}, {}, {3: {}}], None),
]


class TestMap:
    @pytest.mark.parametrize("type_name,wire_rows,expected", _MAP_DECODE)
    def test_round_trip(self, type_name, wire_rows, expected):
        if expected is None:
            expected = wire_rows
        built = build_native_block([("m", type_name, wire_rows)])
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == expected
        assert all(isinstance(v, dict) for v in decoded)

    @pytest.mark.parametrize(
        "type_name,wire_rows",
        [
            ("Map(String, UInt8)", [{"a": 13, "b": 79}, {}, {"c": 5}]),
            ("Map(UInt64, Nullable(String))", [{13: "x", 79: None}, {}]),
            ("Map(String, Array(Int32))", [{"k": [13, 79]}, {}]),
        ],
    )
    def test_all_exit_paths_agree(self, type_name, wire_rows):
        built = build_native_block([("m", type_name, wire_rows)])
        batch = _ch_core.ColBatch.decode_native(built)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == wire_rows

    def test_duplicate_keys_last_value_wins(self):
        # dict input cannot express duplicate keys, so build the entries by hand:
        # one row with keys [1, 1] and values [10, 20]. dict(zip(...)) keeps the
        # first position and the last value.
        body = struct.pack("<Q", 2) + b"\x01\x01" + b"\x0a\x14"
        block = build_native_block_from_bodies([("m", "Map(UInt8, UInt8)", body)], 1)
        decoded = list(_ch_core.ColBatch.decode_native(block).column_data(0))
        assert decoded == [{1: 20}]

    def test_decimal_value_machinery(self):
        # A Decimal value carries a raw unscaled integer in the helper and
        # decodes through the value field's context into a scaled decimal.Decimal.
        built = build_native_block(
            [("m", "Map(String, Decimal(9, 2))", [{"a": 100, "b": -350}, {}])]
        )
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == [{"a": decimal.Decimal("1.00"), "b": decimal.Decimal("-3.50")}, {}]

    def test_empty_maps(self):
        built = build_native_block([("m", "Map(String, UInt8)", [{}, {}, {}])])
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == [{}, {}, {}]

    def test_multi_block_concatenation(self):
        type_name = "Map(String, UInt8)"
        rows_a = [{"a": 13}, {}]
        rows_b = [{"b": 79, "c": 5}]
        block_a = build_native_block([("m", type_name, rows_a)])
        block_b = build_native_block([("m", type_name, rows_b)])
        decoded = list(_ch_core.ColBatch.decode_native(block_a + block_b).column_data(0))
        assert decoded == [{"a": 13}, {}, {"b": 79, "c": 5}]

    def test_non_monotonic_offsets_rejected(self):
        # A Map's offsets share the Array offset validation: a decreasing run is
        # rejected by the core decoder.
        body = struct.pack("<Q", 2) + struct.pack("<Q", 1) + b"\x01\x01" + b"\x0a\x14"
        block = build_native_block_from_bodies([("m", "Map(UInt8, UInt8)", body)], 2)
        with pytest.raises(ValueError, match="Invalid Array layout"):
            _ch_core.ColBatch.decode_native(block)


# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------


def _geometry_block():
    """Independent Native Geometry fixture with every alternative and NULL."""
    body = bytearray(struct.pack("<Q", 0))
    body.extend(bytes([0, 1, 2, 3, 4, 5, 255]))

    # LineString: two points.
    body.extend(struct.pack("<Q", 2))
    body.extend(struct.pack("<2d", 13.0, 14.0))
    body.extend(struct.pack("<2d", 23.0, 24.0))
    # MultiLineString: one line with two points.
    body.extend(struct.pack("<Q", 1))
    body.extend(struct.pack("<Q", 2))
    body.extend(struct.pack("<2d", 31.0, 32.0))
    body.extend(struct.pack("<2d", 41.0, 42.0))
    # MultiPolygon: one polygon containing one one-point ring.
    body.extend(struct.pack("<Q", 1))
    body.extend(struct.pack("<Q", 1))
    body.extend(struct.pack("<Q", 1))
    body.extend(struct.pack("<d", 51.0))
    body.extend(struct.pack("<d", 61.0))
    # Point.
    body.extend(struct.pack("<d", 71.0))
    body.extend(struct.pack("<d", 81.0))
    # Polygon: one two-point ring.
    body.extend(struct.pack("<Q", 1))
    body.extend(struct.pack("<Q", 2))
    body.extend(struct.pack("<2d", 91.0, 92.0))
    body.extend(struct.pack("<2d", 101.0, 102.0))
    # Ring: one point.
    body.extend(struct.pack("<Q", 1))
    body.extend(struct.pack("<d", 111.0))
    body.extend(struct.pack("<d", 121.0))
    return build_native_block_from_bodies([("g", "Geometry", bytes(body))], 7)


class TestGeometry:
    expected = [
        [(13.0, 23.0), (14.0, 24.0)],
        [[(31.0, 41.0), (32.0, 42.0)]],
        [[[(51.0, 61.0)]]],
        (71.0, 81.0),
        [[(91.0, 101.0), (92.0, 102.0)]],
        [(111.0, 121.0)],
        None,
    ]
    expected_arrow = [
        [{"1": 13.0, "2": 23.0}, {"1": 14.0, "2": 24.0}],
        [[{"1": 31.0, "2": 41.0}, {"1": 32.0, "2": 42.0}]],
        [[[{"1": 51.0, "2": 61.0}]]],
        {"1": 71.0, "2": 81.0},
        [[{"1": 91.0, "2": 101.0}, {"1": 92.0, "2": 102.0}]],
        [{"1": 111.0, "2": 121.0}],
        None,
    ]

    @staticmethod
    def tagged(values):
        from clickhouse_connect.datatypes.dynamic import typed_variant

        names = (
            "LineString",
            "MultiLineString",
            "MultiPolygon",
            "Point",
            "Polygon",
            "Ring",
        )
        return [typed_variant(value, name) for value, name in zip(values, names)] + [None]

    def test_golden_decode_all_object_exits_and_arrow(self):
        native = _geometry_block()
        batch = _ch_core.ColBatch.decode_native(native)

        assert batch.column_type_names == ["Geometry"]
        assert list(batch.column_data(0)) == self.expected
        assert list(batch.to_python_columns()[0]) == self.expected
        assert [row[0] for row in batch.to_python_rows()] == self.expected

        pa = pytest.importorskip("pyarrow")
        column = pa.RecordBatchReader.from_stream(batch).read_all().column("g")
        assert pa.types.is_union(column.type)
        assert column.to_pylist() == self.expected_arrow

    @pytest.mark.parametrize("type_name", ["Geometry", "GEOMETRY"])
    def test_encode_matches_golden_and_requires_explicit_geo_name(self, type_name):
        values = self.tagged(self.expected[:6])
        assert _ch_core.encode_native_block(["g"], [type_name], [values], 7) == _geometry_block()

        with pytest.raises(ValueError, match="cannot map Python type"):
            _ch_core.encode_native_block(["g"], [type_name], [[self.expected[3]]], 1)

    def test_bad_explicit_name_and_payload_report_the_logical_row(self):
        from clickhouse_connect.datatypes.dynamic import typed_variant

        with pytest.raises(ValueError, match=r'column "g" row 0 type "String" is not a member'):
            _ch_core.encode_native_block(
                ["g"], ["Geometry"], [[typed_variant("bad", "String")]], 1
            )
        with pytest.raises(ValueError, match=r'column "g" row 0'):
            _ch_core.encode_native_block(
                ["g"], ["Geometry"], [[typed_variant("bad", "Point")]], 1
            )

    @pytest.mark.parametrize(
        ("type_name", "tagged_rows", "expected_rows"),
        [
            (
                "Array(Geometry)",
                lambda tag: [[tag((13.0, 23.0), "Point"), None], [], [tag([(31.0, 41.0)], "Ring")]],
                [[(13.0, 23.0), None], [], [[(31.0, 41.0)]],],
            ),
            (
                "Tuple(Geometry, UInt8)",
                lambda tag: [(tag([(13.0, 23.0)], "LineString"), 1), (None, 2), (tag((31.0, 41.0), "Point"), 3)],
                [([(13.0, 23.0)], 1), (None, 2), ((31.0, 41.0), 3)],
            ),
            (
                "Array(Tuple(Geometry, UInt8))",
                lambda tag: [[(tag((13.0, 23.0), "Point"), 1)], [], [(tag([(31.0, 41.0)], "Ring"), 2)]],
                [[((13.0, 23.0), 1)], [], [([(31.0, 41.0)], 2)]],
            ),
            (
                "Map(String, Geometry)",
                lambda tag: [{"point": tag((13.0, 23.0), "Point")}, {}, {"ring": tag([(31.0, 41.0)], "Ring")}],
                [{"point": (13.0, 23.0)}, {}, {"ring": [(31.0, 41.0)]}],
            ),
        ],
    )
    def test_container_matrix(self, type_name, tagged_rows, expected_rows):
        from clickhouse_connect.datatypes.dynamic import typed_variant

        rows = tagged_rows(typed_variant)
        encoded = _ch_core.encode_native_block(["g"], [type_name], [rows], len(rows))
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert list(batch.column_data(0)) == expected_rows
        assert list(batch.to_python_columns()[0]) == expected_rows
        assert [row[0] for row in batch.to_python_rows()] == expected_rows

    @pytest.mark.parametrize(
        "type_name",
        ["geometry", "Nullable(Geometry)", "Variant(Geometry, String)", "LowCardinality(Geometry)"],
    )
    def test_invalid_type_shapes_are_rejected(self, type_name):
        with pytest.raises(NotImplementedError, match="unsupported"):
            _ch_core.encode_native_block(["g"], [type_name], [[]], 0)

    def test_zero_rows_and_multiple_blocks(self):
        empty = _ch_core.encode_native_block(["g"], ["Geometry"], [[]], 0)
        assert empty == build_native_block([("g", "Geometry", [])])
        batch = _ch_core.ColBatch.decode_native(empty + _geometry_block())
        assert list(batch.column_data(0)) == self.expected

    def test_invalid_discriminator_is_value_error(self):
        body = struct.pack("<Q", 0) + b"\x06"
        block = build_native_block_from_bodies([("g", "Geometry", body)], 1)
        with pytest.raises(ValueError, match="Invalid Variant layout"):
            _ch_core.ColBatch.decode_native(block)


# ---------------------------------------------------------------------------
# Geo aliases (Point/Ring/LineString/Polygon/MultiLineString/MultiPolygon)
# ---------------------------------------------------------------------------

# A Point decodes to an (x, y) tuple; each array-based kind wraps it in one more
# list level. Rows round-trip unchanged (tuples in, tuples out).
_GEO_DECODE = [
    ("Point", [(1.5, 2.5), (-3.25, 4.0), (0.0, 0.0)]),
    ("Ring", [[(1.0, 2.0), (3.0, 4.0)], [], [(5.5, 6.5)]]),
    ("LineString", [[(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)], [], [(7.5, 8.5)]]),
    ("Polygon", [[[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)], [(0.25, 0.25)]], []]),
    ("MultiLineString", [[[(0.0, 0.0), (1.0, 1.0)], [(2.0, 2.0)]], []]),
    ("MultiPolygon", [[[[(0.0, 0.0), (1.0, 0.0)]], [[(2.0, 2.0)], [(3.0, 3.0)]]], []]),
]


class TestGeo:
    @pytest.mark.parametrize("type_name,rows", _GEO_DECODE)
    def test_decode_shape_and_type_name(self, type_name, rows):
        batch = _ch_core.ColBatch.decode_native(build_native_block([("g", type_name, rows)]))
        assert batch.column_type_names == [type_name]
        # `==` distinguishes tuple from list, so equality enforces the Point ->
        # tuple, array-level -> list nesting exactly.
        assert list(batch.column_data(0)) == rows

    def test_point_leaves_are_tuples(self):
        decoded = list(_ch_core.ColBatch.decode_native(build_native_block([("g", "Point", [(1.5, 2.5), (-3.25, 4.0)])])).column_data(0))
        assert all(isinstance(v, tuple) and len(v) == 2 for v in decoded)

    @pytest.mark.parametrize("type_name,rows", _GEO_DECODE)
    def test_encode_round_trip_and_golden(self, type_name, rows):
        encoded = _ch_core.encode_native_block(["g"], [type_name], [rows], len(rows))
        assert encoded == build_native_block([("g", type_name, rows)])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == rows

    @pytest.mark.parametrize("type_name,rows", _GEO_DECODE)
    def test_all_exit_paths_agree(self, type_name, rows):
        batch = _ch_core.ColBatch.decode_native(build_native_block([("g", type_name, rows)]))
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == rows

    def test_nullable_point_null_rows_are_none(self):
        # Nullable(Point) parses as Nullable(Geo(Point)); the delegate is a Tuple,
        # so the decoded column carries tuple-level validity and null rows read
        # as None across all exit paths.
        rows = [(1.5, 2.5), None, (-3.25, 4.0), None]
        batch = _ch_core.ColBatch.decode_native(
            build_native_block([("g", "Nullable(Point)", rows)])
        )
        assert batch.column_type_names == ["Nullable(Point)"]
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == rows

    def test_nullable_point_encode_round_trip(self):
        rows = [(1.5, 2.5), None, (0.0, -1.0)]
        encoded = _ch_core.encode_native_block(["g"], ["Nullable(Point)"], [rows], len(rows))
        assert encoded == build_native_block([("g", "Nullable(Point)", rows)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows


# ---------------------------------------------------------------------------
# Nested (physical Array(Tuple(named ...)))
# ---------------------------------------------------------------------------

class TestNested:
    def test_decode_to_list_of_dicts(self):
        type_name = "Nested(a UInt32, b String)"
        wire_rows = [[(13, "north"), (79, "south")], [], [(5, "east")]]
        expected = [
            [{"a": 13, "b": "north"}, {"a": 79, "b": "south"}],
            [],
            [{"a": 5, "b": "east"}],
        ]
        batch = _ch_core.ColBatch.decode_native(build_native_block([("n", type_name, wire_rows)]))
        assert batch.column_type_names == [type_name]
        decoded = list(batch.column_data(0))
        assert decoded == expected
        assert all(isinstance(item, dict) for row in decoded for item in row)

    def test_all_exit_paths_agree(self):
        type_name = "Nested(a UInt32, b String)"
        wire_rows = [[(13, "north")], [], [(5, "east"), (7, "west")]]
        expected = [[{"a": 13, "b": "north"}], [], [{"a": 5, "b": "east"}, {"a": 7, "b": "west"}]]
        batch = _ch_core.ColBatch.decode_native(build_native_block([("n", type_name, wire_rows)]))
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == expected

    def test_encode_positional_golden_and_round_trip(self):
        type_name = "Nested(a UInt32, b String)"
        wire_rows = [[(13, "north"), (79, "south")], [], [(5, "east")]]
        encoded = _ch_core.encode_native_block(["n"], [type_name], [wire_rows], len(wire_rows))
        assert encoded == build_native_block([("n", type_name, wire_rows)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            [{"a": 13, "b": "north"}, {"a": 79, "b": "south"}],
            [],
            [{"a": 5, "b": "east"}],
        ]

    def test_encode_dict_rows_match_positional(self):
        type_name = "Nested(a UInt32, b String)"
        positional = [[(13, "north"), (79, "south")], [], [(5, "east")]]
        dict_rows = [
            [{"a": 13, "b": "north"}, {"a": 79, "b": "south"}],
            [],
            [{"a": 5, "b": "east"}],
        ]
        expected = _ch_core.encode_native_block(["n"], [type_name], [positional], 3)
        assert _ch_core.encode_native_block(["n"], [type_name], [dict_rows], 3) == expected

    def test_nested_low_cardinality_field_round_trip(self):
        # A LowCardinality Nested field resolves through the LC path; the core's
        # LC index word differs from the helper's, so this round-trips instead of
        # golden-comparing.
        type_name = "Nested(city LowCardinality(String), pop UInt32)"
        dict_rows = [
            [{"city": "harbor", "pop": 13}, {"city": "harbor", "pop": 79}],
            [],
            [{"city": "ridge", "pop": 5}],
        ]
        encoded = _ch_core.encode_native_block(["n"], [type_name], [dict_rows], 3)
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == dict_rows

    def test_nested_point_field_golden_and_round_trip(self):
        # A geo alias (Point) nested inside a non-alias container: Nested expands
        # to Array(Tuple(p Point, q UInt32)) and the Point field expands again.
        type_name = "Nested(p Point, q UInt32)"
        wire_rows = [[((1.5, 2.5), 13), ((3.0, 4.0), 79)], [], [((5.5, 6.5), 5)]]
        encoded = _ch_core.encode_native_block(["n"], [type_name], [wire_rows], len(wire_rows))
        assert encoded == build_native_block([("n", type_name, wire_rows)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            [{"p": (1.5, 2.5), "q": 13}, {"p": (3.0, 4.0), "q": 79}],
            [],
            [{"p": (5.5, 6.5), "q": 5}],
        ]


# ---------------------------------------------------------------------------
# Tuple/Map column-major fill
# ---------------------------------------------------------------------------

class TestTupleMapFill:
    """The buffered exits decode top-level Tuple/Map columns with a hoisted
    column-major fill; Tuple/Map nested inside Array stay on the per-cell path."""

    @staticmethod
    def _all_exits(batch):
        return (
            list(batch.column_data(0)),
            list(batch.to_python_columns()[0]),
            [r[0] for r in batch.to_python_rows()],
        )

    @pytest.mark.parametrize(
        "type_name,wire_rows,expected",
        [
            ("Tuple(LowCardinality(String), Int64)",
             [("x", 1), ("y", 2), ("x", 3)], [("x", 1), ("y", 2), ("x", 3)]),
            ("Tuple(a LowCardinality(String), b Int64)",
             [("x", 1), ("y", 2)], [{"a": "x", "b": 1}, {"a": "y", "b": 2}]),
            ("Map(String, Int64)", [{"a": 1, "b": 2}, {}, {"c": 3}], None),
            ("Map(String, Array(Int64))", [{"k": [1, 2], "e": []}, {}, {"z": [3]}], None),
            ("Nullable(Tuple(Int64, String))", [(1, "a"), None, (2, "b")], None),
            ("Tuple()", [(), (), ()], None),
            ("Map(String, UInt8)", [{}, {}, {}], None),
        ],
    )
    def test_all_exit_paths_agree(self, type_name, wire_rows, expected):
        if expected is None:
            expected = wire_rows
        built = build_native_block([("c", type_name, wire_rows)])
        batch = _ch_core.ColBatch.decode_native(built)
        via_column_data, via_columns, via_rows = self._all_exits(batch)
        assert via_column_data == via_columns == via_rows == expected

    def test_tuple_lc_field_identity_within_chunk(self):
        rows = [("red", 1), ("blue", 2), ("red", 3), ("red", 4)]
        built = build_native_block([("t", "Tuple(LowCardinality(String), Int64)", rows)])
        batch = _ch_core.ColBatch.decode_native(built)
        col = batch.column_data(0)
        assert list(col) == rows
        assert col[0][0] is col[2][0]
        assert col[0][0] is col[3][0]
        cols = batch.to_python_columns()[0]
        assert cols[0][0] is cols[2][0]
        out_rows = batch.to_python_rows()
        assert out_rows[0][0][0] is out_rows[2][0][0]

    def test_map_lc_value_identity_within_chunk(self):
        rows = [{"a": "red", "b": "blue"}, {}, {"c": "red"}]
        built = build_native_block([("m", "Map(String, LowCardinality(String))", rows)])
        batch = _ch_core.ColBatch.decode_native(built)
        col = batch.column_data(0)
        assert list(col) == rows
        assert col[0]["a"] is col[2]["c"]
        cols = batch.to_python_columns()[0]
        assert cols[0]["a"] is cols[2]["c"]
        out_rows = batch.to_python_rows()
        assert out_rows[0][0]["a"] is out_rows[2][0]["c"]

    def test_map_lc_key_identity_within_chunk(self):
        rows = [{"red": 1, "blue": 2}, {}, {"red": 3}]
        built = build_native_block([("m", "Map(LowCardinality(String), UInt64)", rows)])
        col = _ch_core.ColBatch.decode_native(built).column_data(0)
        assert list(col) == rows
        key0 = next(k for k in col[0] if k == "red")
        assert key0 is next(iter(col[2]))

    def test_nullable_tuple_lc_field_null_rows_and_identity(self):
        # Null rows discard their field values; valid rows still share the
        # dictionary object.
        rows = [("x", 1), None, ("x", 2), ("y", 3)]
        enc = _ch_core.encode_native_block(
            ["c"], ["Nullable(Tuple(LowCardinality(String), Int64))"], [rows], len(rows)
        )
        batch = _ch_core.ColBatch.decode_native(enc)
        via_column_data, via_columns, via_rows = self._all_exits(batch)
        assert via_column_data == via_columns == via_rows == rows
        col = batch.column_data(0)
        assert col[0][0] is col[2][0]

    def test_fill_path_matches_per_cell_array_element_path(self):
        # The same values decode through the column-major fill at top level and
        # through the per-cell path one Array level down; they must agree.
        tup_rows = [(1, "a"), (2, "b"), (3, "c")]
        map_rows = [{"a": 1, "b": 2}, {}, {"c": 3}]
        top = _ch_core.ColBatch.decode_native(build_native_block([
            ("t", "Tuple(Int64, String)", tup_rows),
            ("m", "Map(String, Int64)", map_rows),
        ]))
        wrapped = _ch_core.ColBatch.decode_native(build_native_block([
            ("t", "Array(Tuple(Int64, String))", [[r] for r in tup_rows]),
            ("m", "Array(Map(String, Int64))", [[r] for r in map_rows]),
        ]))
        assert [[v] for v in top.column_data(0)] == list(wrapped.column_data(0))
        assert [[v] for v in top.column_data(1)] == list(wrapped.column_data(1))

    def test_multi_chunk_named_tuple_and_map(self):
        tn = "Tuple(a Int64, b String)"
        mp = "Map(String, Int64)"
        block_a = build_native_block([("t", tn, [(1, "x")]), ("m", mp, [{"a": 1}])])
        block_b = build_native_block(
            [("t", tn, [(2, "y"), (3, "z")]), ("m", mp, [{}, {"b": 2}])]
        )
        batch = _ch_core.ColBatch.decode_native(block_a + block_b)
        assert list(batch.column_data(0)) == [
            {"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}
        ]
        assert list(batch.column_data(1)) == [{"a": 1}, {}, {"b": 2}]
        assert list(batch.to_python_rows()) == [
            ({"a": 1, "b": "x"}, {"a": 1}),
            ({"a": 2, "b": "y"}, {}),
            ({"a": 3, "b": "z"}, {"b": 2}),
        ]
