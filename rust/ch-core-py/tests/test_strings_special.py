from helpers import (
    _ch_core,
    _NdarrayLikeColumn,
    array,
    build_native_block,
    build_native_block_from_bodies,
    ipaddress,
    pytest,
    sys,
    uuid,
)


class TestNothing:
    @pytest.mark.parametrize(
        ("type_name", "values", "expected"),
        [
            ("Nothing", [13, None, "ignored"], [None, None, None]),
            ("Nullable(Nothing)", [13, None, "ignored"], [None, None, None]),
            (
                "Array(Nothing)",
                [[13, None], [], ["ignored"]],
                [[None, None], [], [None]],
            ),
            (
                "Array(Nullable(Nothing))",
                [[None, None], [], [None]],
                [[None, None], [], [None]],
            ),
            (
                "Tuple(Nothing, UInt8)",
                [(13, 13), (None, 79), ("ignored", 5)],
                [(None, 13), (None, 79), (None, 5)],
            ),
            (
                "Tuple(Nullable(Nothing), UInt8)",
                [(None, 13), (None, 79), (None, 5)],
                [(None, 13), (None, 79), (None, 5)],
            ),
            (
                "Array(Tuple(Nothing, UInt8))",
                [[(13, 13), (None, 79)], [], [("ignored", 5)]],
                [[(None, 13), (None, 79)], [], [(None, 5)]],
            ),
            (
                "Map(Nothing, UInt8)",
                [{13: 13}, {}, {"ignored": 79}],
                [{None: 13}, {}, {None: 79}],
            ),
            (
                "Map(UInt8, Nothing)",
                [{13: 13}, {}, {79: "ignored"}],
                [{13: None}, {}, {79: None}],
            ),
            (
                "Map(UInt8, Nullable(Nothing))",
                [{13: None}, {}, {79: None}],
                [{13: None}, {}, {79: None}],
            ),
        ],
    )
    def test_encode_decode_type_matrix_and_all_object_exits(self, type_name, values, expected):
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        assert encoded == build_native_block([("v", type_name, values)])

        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == expected
        assert list(batch.to_python_columns()[0]) == expected
        assert [row[0] for row in batch.to_python_rows()] == expected

    def test_noncanonical_wire_placeholder_bytes_are_ignored(self):
        data = build_native_block_from_bodies(
            [
                ("v", "Nothing", b"\x00\x7f\xff"),
                ("sentinel", "UInt8", bytes([13, 79, 5])),
            ],
            3,
        )
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [None, None, None]
        assert list(batch.column_data(1)) == [13, 79, 5]

    def test_nullable_nothing_with_structurally_valid_row_is_still_none(self):
        data = build_native_block_from_bodies([("v", "Nullable(Nothing)", b"\x00\x01" + b"01")], 2)
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [None, None]

    @pytest.mark.parametrize("type_name", ["Nothing", "Nullable(Nothing)", "Array(Nothing)"])
    def test_zero_rows(self, type_name):
        encoded = _ch_core.encode_native_block(["v"], [type_name], [[]], 0)
        assert encoded == build_native_block([("v", type_name, [])])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == []

    @pytest.mark.parametrize("container", [list, tuple, _NdarrayLikeColumn])
    def test_python_values_only_affect_nullable_structural_mask(self, container):
        values = container([13, None, "ignored"])
        plain = _ch_core.encode_native_block(["v"], ["Nothing"], [values], 3)
        nullable = _ch_core.encode_native_block(["v"], ["Nullable(Nothing)"], [values], 3)
        assert plain == build_native_block([("v", "Nothing", [13, None, "ignored"])])
        assert nullable == build_native_block([("v", "Nullable(Nothing)", [13, None, "ignored"])])

    def test_exact_bytes(self):
        # Pin the canonical 0x30 marker run independently of build_native_block.
        plain = _ch_core.encode_native_block(["v"], ["Nothing"], [[None, None, None]], 3)
        assert plain == build_native_block_from_bodies([("v", "Nothing", b"\x30\x30\x30")], 3)
        nullable = _ch_core.encode_native_block(["v"], ["Nullable(Nothing)"], [[None, 13, None]], 3)
        assert nullable == build_native_block_from_bodies(
            [("v", "Nullable(Nothing)", b"\x01\x00\x01" + b"\x30\x30\x30")], 3
        )

    def test_multi_entry_map_nothing_keys_collapse(self):
        # Dict representation: all-None keys collide, keeping the last value.
        data = build_native_block_from_bodies(
            [("v", "Map(Nothing, UInt8)", b"\x02\x00\x00\x00\x00\x00\x00\x00" + b"00" + bytes([13, 79]))],
            1,
        )
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [{None: 79}]

    @pytest.mark.parametrize("type_name", ["nothing", "NOTHING", "Nothing "])
    def test_type_name_is_case_sensitive(self, type_name):
        with pytest.raises(NotImplementedError, match="unsupported ClickHouse type"):
            _ch_core.encode_native_block(["v"], [type_name], [[]], 0)

    @pytest.mark.parametrize(
        "type_name",
        ["LowCardinality(Nothing)", "LowCardinality(Nullable(Nothing))"],
    )
    def test_low_cardinality_nothing_rejected(self, type_name):
        with pytest.raises(NotImplementedError, match="unsupported LowCardinality"):
            _ch_core.encode_native_block(["v"], [type_name], [[]], 0)

    def test_truncated_marker_run_rejected(self):
        data = build_native_block_from_bodies([("v", "Nothing", b"00")], 3)
        with pytest.raises(EOFError):
            _ch_core.ColBatch.decode_native(data)

    def test_arrow_null_type(self):
        pa = pytest.importorskip("pyarrow")
        encoded = _ch_core.encode_native_block(
            ["v", "n", "a"],
            ["Nothing", "Nullable(Nothing)", "Array(Nothing)"],
            [[None, None], [None, None], [[None], []]],
            2,
        )
        table = pa.RecordBatchReader.from_stream(_ch_core.ColBatch.decode_native(encoded)).read_all()
        assert table.schema.types == [pa.null(), pa.null(), pa.large_list(pa.null())]
        assert table.schema.field("v").nullable
        assert table.schema.field("n").nullable
        assert table.column("v").to_pylist() == [None, None]
        assert table.column("n").to_pylist() == [None, None]
        assert table.column("a").to_pylist() == [[None], []]


# ---------------------------------------------------------------------------
# String types
# ---------------------------------------------------------------------------


class TestDecodeString:
    def test_basic(self):
        data = build_native_block([("s", "String", ["hello", "", "world!"])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == ["hello", "", "world!"]

    def test_unicode(self):
        data = build_native_block([("s", "String", ["héllo", "日本語"])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == ["héllo", "日本語"]


class TestStringInvalidUtf8:
    def test_hex_fallback_on_all_paths(self):
        # Invalid UTF-8 renders as the lowercase hex of the raw bytes on every
        # materialization path, matching clickhouse-connect's String fallback.
        data = build_native_block([("s", "String", ["ok", b"\xff\xfe", b"\x80abc"])])
        batch = _ch_core.ColBatch.decode_native(data)
        expected = ["ok", "fffe", "80616263"]
        assert list(batch.column_data(0)) == expected
        assert list(batch.to_python_columns()[0]) == expected
        assert [row[0] for row in batch.to_python_rows()] == expected

    def test_hex_fallback_nullable(self):
        data = build_native_block([("s", "Nullable(String)", [b"\xc3\x28", None, "u1"])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == ["c328", None, "u1"]


class TestDecodeFixedString:
    def test_basic(self):
        data = build_native_block([("fs", "FixedString(3)", [b"abc", b"xyz"])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 2
        assert batch.column_type_names == ["FixedString(3)"]
        result = list(batch.column_data(0))
        assert result[0] == b"abc"
        assert result[1] == b"xyz"

    def test_null_padded(self):
        data = build_native_block([("fs", "FixedString(4)", [b"ab\x00\x00"])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = list(batch.column_data(0))
        assert result[0] == b"ab\x00\x00"


# ---------------------------------------------------------------------------
# Enum
# ---------------------------------------------------------------------------


class TestDecodeEnum:
    _E8 = "Enum8('red' = 1, 'green' = 2, 'blue' = 3)"
    _E16 = "Enum16('alpha' = -5, 'beta' = 0, 'gamma' = 1000)"

    def test_enum8_basic(self):
        # Wire carries the integer codes; the Python exit yields the labels.
        data = build_native_block([("c", self._E8, [1, 2, 3, 1, 2])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == [self._E8]
        assert list(batch.column_data(0)) == ["red", "green", "blue", "red", "green"]

    def test_enum16_negative_and_large(self):
        data = build_native_block([("c", self._E16, [-5, 0, 1000, 0, -5])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == [self._E16]
        assert list(batch.column_data(0)) == ["alpha", "beta", "gamma", "beta", "alpha"]

    def test_nullable_enum8(self):
        data = build_native_block([("c", f"Nullable({self._E8})", [1, None, 3, None])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == [f"Nullable({self._E8})"]
        assert list(batch.column_data(0)) == ["red", None, "blue", None]

    def test_unknown_code_is_none(self):
        # A code with no defined label materializes as None, matching
        # clickhouse-connect's int_map.get(code, None).
        data = build_native_block([("c", self._E8, [1, 99, 2])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == ["red", None, "green"]

    def test_paths_agree(self):
        data = build_native_block([("c", self._E16, [1000, -5, 0, 1000, -5])])
        batch = _ch_core.ColBatch.decode_native(data)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [row[0] for row in batch.to_python_rows()]
        expected = ["gamma", "alpha", "beta", "gamma", "alpha"]
        assert via_column_data == via_columns == via_rows == expected

    def test_arrow_exports_int_codes(self):
        # The core exports Enum as the raw signed-int codes (no per-cell label
        # remapping); the label policy lives only on the Python object exit.
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("c", self._E8, [1, 2, 3]), ("d", self._E16, [-5, 0, 1000])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = pa.RecordBatchReader.from_stream(batch).read_all()
        assert result.schema.field("c").type == pa.int8()
        assert result.schema.field("d").type == pa.int16()
        assert result.column("c").to_pylist() == [1, 2, 3]
        assert result.column("d").to_pylist() == [-5, 0, 1000]


# ---------------------------------------------------------------------------
# UUID
# ---------------------------------------------------------------------------

class TestDecodeUUID:
    _KNOWN = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
    _VALUES = [uuid.UUID(int=0), _KNOWN, uuid.UUID(int=(1 << 128) - 1)]

    def test_values(self):
        data = build_native_block([("u", "UUID", self._VALUES)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["UUID"]
        got = list(batch.column_data(0))
        assert got == self._VALUES
        assert all(type(v) is uuid.UUID for v in got)
        assert got[1] == uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
        assert all(v.is_safe is uuid.SafeUUID.unsafe for v in got)

    def test_paths_agree(self):
        data = build_native_block([("u", "UUID", self._VALUES)])
        batch = _ch_core.ColBatch.decode_native(data)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [row[0] for row in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == self._VALUES

    def test_nullable(self):
        vals = [self._KNOWN, None, uuid.UUID(int=0), None]
        data = build_native_block([("u", "Nullable(UUID)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals
        assert [row[0] for row in batch.to_python_rows()] == vals

    def test_low_cardinality(self):
        vals = [self._KNOWN, uuid.UUID(int=0), self._KNOWN, self._KNOWN]
        data = build_native_block([("u", "LowCardinality(UUID)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        got = list(batch.column_data(0))
        assert got == vals
        assert all(v.is_safe is uuid.SafeUUID.unsafe for v in got)

    def test_low_cardinality_nullable(self):
        vals = [self._KNOWN, None, self._KNOWN, None]
        data = build_native_block([("u", "LowCardinality(Nullable(UUID))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals
        assert [row[0] for row in batch.to_python_rows()] == vals

    def test_low_cardinality_all_null(self):
        vals = [None, None, None]
        data = build_native_block([("u", "LowCardinality(Nullable(UUID))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals

    def test_round_trip(self):
        vals = [self._KNOWN, uuid.UUID(int=79)]
        encoded = _ch_core.encode_native_block(["u"], ["UUID"], [vals], len(vals))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == vals


# ---------------------------------------------------------------------------
# IPv4
# ---------------------------------------------------------------------------

class TestDecodeIPv4:
    _VALUES = [
        ipaddress.IPv4Address("0.0.0.0"),
        ipaddress.IPv4Address("255.255.255.255"),
        ipaddress.IPv4Address("1.2.3.4"),
    ]

    def test_values(self):
        data = build_native_block([("v", "IPv4", self._VALUES)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["IPv4"]
        got = list(batch.column_data(0))
        assert got == self._VALUES
        assert all(type(v) is ipaddress.IPv4Address for v in got)
        assert got[2] == ipaddress.ip_address("1.2.3.4")

    def test_paths_agree(self):
        data = build_native_block([("v", "IPv4", self._VALUES)])
        batch = _ch_core.ColBatch.decode_native(data)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [row[0] for row in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == self._VALUES

    def test_nullable(self):
        vals = [ipaddress.IPv4Address("1.2.3.4"), None, ipaddress.IPv4Address("0.0.0.0")]
        data = build_native_block([("v", "Nullable(IPv4)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals
        assert [row[0] for row in batch.to_python_rows()] == vals

    def test_round_trip(self):
        vals = [ipaddress.IPv4Address("192.0.2.1"), ipaddress.IPv4Address("198.51.100.7")]
        encoded = _ch_core.encode_native_block(["v"], ["IPv4"], [vals], len(vals))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == vals


# ---------------------------------------------------------------------------
# IPv6
# ---------------------------------------------------------------------------

class TestDecodeIPv6:
    _VALUES = [
        ipaddress.IPv6Address("::"),
        ipaddress.IPv6Address("::1"),
        ipaddress.IPv6Address("2001:db8:85a3:8d3:1319:8a2e:370:7348"),
        ipaddress.IPv6Address("::ffff:1.2.3.4"),
    ]

    def test_values(self):
        data = build_native_block([("v", "IPv6", self._VALUES)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["IPv6"]
        got = list(batch.column_data(0))
        assert got == self._VALUES
        # Always IPv6Address, even for a v4-mapped value.
        assert all(type(v) is ipaddress.IPv6Address for v in got)
        # str() reads _scope_id, so it verifies the attribute was set.
        assert [str(v) for v in got] == [str(v) for v in self._VALUES]

    def test_paths_agree(self):
        data = build_native_block([("v", "IPv6", self._VALUES)])
        batch = _ch_core.ColBatch.decode_native(data)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [row[0] for row in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == self._VALUES

    def test_nullable(self):
        vals = [ipaddress.IPv6Address("::1"), None, ipaddress.IPv6Address("::ffff:1.2.3.4")]
        data = build_native_block([("v", "Nullable(IPv6)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals
        assert [row[0] for row in batch.to_python_rows()] == vals

    def test_round_trip(self):
        vals = [ipaddress.IPv6Address("2001:db8::1"), ipaddress.IPv6Address("::ffff:192.0.2.9")]
        encoded = _ch_core.encode_native_block(["v"], ["IPv6"], [vals], len(vals))
        got = list(_ch_core.ColBatch.decode_native(encoded).column_data(0))
        assert got == vals
        assert all(type(v) is ipaddress.IPv6Address for v in got)


# ---------------------------------------------------------------------------
# SimpleAggregateFunction (physical == the inner type)
# ---------------------------------------------------------------------------


class TestSimpleAggregateFunction:
    def test_sum_uint64_golden_and_round_trip(self):
        type_name = "SimpleAggregateFunction(sum, UInt64)"
        values = [13, 79, 0, 18446744073709551615]
        encoded = _ch_core.encode_native_block(["s"], [type_name], [values], len(values))
        assert encoded == build_native_block([("s", type_name, values)])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        decoded = list(batch.column_data(0))
        assert decoded == values
        assert all(isinstance(v, int) for v in decoded)

    def test_any_string_golden_and_round_trip(self):
        type_name = "SimpleAggregateFunction(any, String)"
        values = ["red", "", "green"]
        encoded = _ch_core.encode_native_block(["s"], [type_name], [values], len(values))
        assert encoded == build_native_block([("s", type_name, values)])
        decoded = list(_ch_core.ColBatch.decode_native(encoded).column_data(0))
        assert decoded == values
        assert all(isinstance(v, str) for v in decoded)

    def test_anylast_low_cardinality_string_round_trip(self):
        # The inner LowCardinality resolves through the LC path; the core's LC
        # index word differs from the helper's, so this round-trips.
        type_name = "SimpleAggregateFunction(anyLast, LowCardinality(String))"
        values = ["red", "blue", "red", "red", "green"]
        encoded = _ch_core.encode_native_block(["s"], [type_name], [values], len(values))
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == values

    def test_all_exit_paths_agree(self):
        type_name = "SimpleAggregateFunction(sum, UInt64)"
        values = [13, 79, 5]
        batch = _ch_core.ColBatch.decode_native(build_native_block([("s", type_name, values)]))
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == values

    def test_array_of_saf_golden_and_round_trip(self):
        # A SAF alias nested inside a non-alias Array container.
        type_name = "Array(SimpleAggregateFunction(sum, UInt64))"
        rows = [[13, 79], [], [5, 5, 18446744073709551615]]
        encoded = _ch_core.encode_native_block(["a"], [type_name], [rows], len(rows))
        assert encoded == build_native_block([("a", type_name, rows)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows

    def test_low_cardinality_string_round_trip(self):
        # LowCardinality(SAF(anyLast, String)): the SAF chain resolves through the
        # LC path. Server round-trips this live; the python codec cannot. The
        # core's LC index word differs from the helper's, so no golden compare.
        type_name = "LowCardinality(SimpleAggregateFunction(anyLast, String))"
        values = ["red", "blue", "red", "red", "green"]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == values
        built = build_native_block([("v", type_name, values)])
        assert list(_ch_core.ColBatch.decode_native(built).column_data(0)) == values

    def test_low_cardinality_nullable_string_round_trip(self):
        # LowCardinality(SAF(anyLast, Nullable(String))) is index-level nullable:
        # the SAF chain is stripped before the Nullable is detected.
        type_name = "LowCardinality(SimpleAggregateFunction(anyLast, Nullable(String)))"
        values = ["red", None, "red", None, "blue"]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == values
        built = build_native_block([("v", type_name, values)])
        assert list(_ch_core.ColBatch.decode_native(built).column_data(0)) == values


# ---------------------------------------------------------------------------
# AggregateFunction (opaque serialized state bytes)
# ---------------------------------------------------------------------------


class TestAggregateFunction:
    def test_count_decode_all_python_exits(self):
        type_name = "AggregateFunction(count)"
        states = [b"\x00", b"\x0d", b"\x80\x01"]
        native = build_native_block_from_bodies(
            [("c", type_name, b"".join(states))],
            len(states),
        )

        batch = _ch_core.ColBatch.decode_native(native)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == states
        assert list(batch.to_python_columns()[0]) == states
        assert batch.to_python_rows() == [(state,) for state in states]
        assert all(isinstance(state, bytes) for state in batch.column_data(0))

    def test_count_encode_accepts_bytes_like_and_round_trips(self):
        type_name = "AggregateFunction(count)"
        values = [b"\x00", bytearray(b"\x0d"), memoryview(b"\x80\xff\x01")[::2]]
        encoded = _ch_core.encode_native_block(["c"], [type_name], [values], len(values))
        expected = build_native_block_from_bodies(
            [("c", type_name, b"\x00\x0d\x80\x01")],
            len(values),
        )

        assert encoded == expected
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [bytes(value) for value in values]

        generic = _NdarrayLikeColumn([b"\x05", memoryview(b"\x0b")])
        generic_encoded = _ch_core.encode_native_block(
            ["c"],
            [type_name],
            [generic],
            len(generic),
        )
        assert list(_ch_core.ColBatch.decode_native(generic_encoded).column_data(0)) == [
            b"\x05",
            b"\x0b",
        ]

    @pytest.mark.parametrize(
        ("type_name", "width", "states"),
        [
            ("AggregateFunction(sum, UInt64)", 8, [13, 79]),
            ("AggregateFunction(sum, Int32)", 8, [-13, 79]),
            ("AggregateFunction(sum, UInt128)", 16, [13, 79]),
            ("AggregateFunction(sum, UInt256)", 32, [13, 79]),
            ("AggregateFunction(sum, Decimal(9, 2))", 16, [1300, 7900]),
        ],
    )
    def test_sum_fixed_width_states_round_trip(self, type_name, width, states):
        values = [value.to_bytes(width, "little", signed=value < 0) for value in states]
        encoded = _ch_core.encode_native_block(["s"], [type_name], [values], len(values))
        expected = build_native_block_from_bodies(
            [("s", type_name, b"".join(values))],
            len(values),
        )

        assert encoded == expected
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == values

    @pytest.mark.parametrize(
        ("type_name", "width", "value"),
        [
            ("AggregateFunction(sum, Nullable(UInt8))", 8, 13),
            ("AggregateFunction(sum, Nullable(Int32))", 8, -13),
            ("AggregateFunction(sum, Nullable(UInt128))", 16, 79),
            ("AggregateFunction(sum, Nullable(Decimal(9, 2)))", 16, 1300),
            ("AggregateFunction(sum, Nullable(UInt256))", 32, 79),
        ],
    )
    def test_nullable_sum_variable_width_states_round_trip(self, type_name, width, value):
        accumulator = value.to_bytes(width, "little", signed=value < 0)
        states = [b"\x00", b"\x01" + accumulator, b"\x80" + accumulator]
        encoded = _ch_core.encode_native_block(["s"], [type_name], [states], len(states))

        assert encoded == build_native_block_from_bodies(
            [("s", type_name, b"".join(states))],
            len(states),
        )
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == states

    def test_nothing_uint64_state_round_trip(self):
        type_name = "AggregateFunction(nothingUInt64, Nullable(Nothing))"
        states = [b"\x00", b"\x00", b"\x00"]
        encoded = _ch_core.encode_native_block(["s"], [type_name], [states], len(states))

        assert encoded == build_native_block_from_bodies(
            [("s", type_name, b"\x00\x00\x00")],
            len(states),
        )
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == states

    @pytest.mark.parametrize(
        ("type_name", "values"),
        [
            (
                "Array(AggregateFunction(count))",
                [[b"\x0d", b"\x4f"], [], [b"\x80\x01"]],
            ),
            (
                "Array(AggregateFunction(sum, Nullable(Int32)))",
                [
                    [b"\x00", b"\x01" + (-13).to_bytes(8, "little", signed=True)],
                    [],
                    [b"\x80" + (79).to_bytes(8, "little")],
                ],
            ),
            (
                "Tuple(AggregateFunction(sum, UInt64), UInt8)",
                [((13).to_bytes(8, "little"), 5), ((79).to_bytes(8, "little"), 11)],
            ),
            (
                "Tuple(state AggregateFunction(count), code UInt8)",
                [
                    {"state": b"\x0d", "code": 5},
                    {"state": b"\x4f", "code": 11},
                ],
            ),
            (
                "Array(Tuple(AggregateFunction(count), UInt8))",
                [[(b"\x0d", 5), (b"\x4f", 11)], [], [(b"\x80\x01", 17)]],
            ),
            (
                "Array(Tuple(AggregateFunction(sum, Nullable(UInt64)), UInt8))",
                [
                    [
                        (b"\x00", 5),
                        (b"\x01" + (13).to_bytes(8, "little"), 11),
                    ],
                    [],
                    [(b"\x80" + (79).to_bytes(8, "little"), 17)],
                ],
            ),
            (
                "Nullable(Tuple(AggregateFunction(count)))",
                [(b"\x0d",), (b"\x4f",)],
            ),
            (
                "Map(String, AggregateFunction(count))",
                [{"first": b"\x0d", "second": b"\x4f"}, {}, {"third": b"\x80\x01"}],
            ),
        ],
    )
    def test_container_shapes_round_trip(self, type_name, values):
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == values

    @pytest.mark.parametrize(
        ("type_name", "states"),
        [
            ("AggregateFunction(count)", [b"\x0d", b"\x80\x01", b"\x05"]),
            (
                "AggregateFunction(sum, Nullable(UInt64))",
                [b"\x00", b"\x01" + (13).to_bytes(8, "little"), b"\x00"],
            ),
        ],
    )
    def test_arrow_export_is_large_binary(self, type_name, states):
        pa = pytest.importorskip("pyarrow")
        native = build_native_block_from_bodies(
            [("s", type_name, b"".join(states))],
            len(states),
        )
        batch = _ch_core.ColBatch.decode_native(native)

        table = pa.RecordBatchReader.from_stream(batch).read_all()
        assert table.schema.field("s").type == pa.large_binary()
        assert not table.schema.field("s").nullable
        assert table.column("s").to_pylist() == states

    def test_stream_decoder_preserves_variable_state_boundaries_across_chunks(self):
        type_name = "AggregateFunction(count)"
        first_states = [b"\x0d", b"\x80\x01"]
        second_states = [b"\x05", b"\xff\x01"]
        native = b"".join(
            [
                build_native_block_from_bodies(
                    [("c", type_name, b"".join(first_states))],
                    len(first_states),
                ),
                build_native_block_from_bodies(
                    [("c", type_name, b"".join(second_states))],
                    len(second_states),
                ),
            ]
        )
        decoder = _ch_core.StreamDecoder()
        batches = []
        for byte in native:
            batches.extend(decoder.feed(bytes([byte])))
        batches.extend(decoder.finish())

        assert len(batches) == 2
        combined = _ch_core.ColBatch.from_batches(batches)
        assert combined.num_chunks == 2
        assert list(combined.column_data(0)) == first_states + second_states

    def test_nullable_sum_stream_decoder_preserves_conditional_boundaries(self):
        type_name = "AggregateFunction(sum, Nullable(UInt64))"
        first_states = [b"\x00", b"\x01" + (13).to_bytes(8, "little")]
        second_states = [b"\x80" + (79).to_bytes(8, "little"), b"\x00"]
        native = b"".join(
            build_native_block_from_bodies(
                [("s", type_name, b"".join(states))],
                len(states),
            )
            for states in (first_states, second_states)
        )
        decoder = _ch_core.StreamDecoder()
        batches = []
        for byte in native:
            batches.extend(decoder.feed(bytes([byte])))
        batches.extend(decoder.finish())

        combined = _ch_core.ColBatch.from_batches(batches)
        expected = first_states + second_states
        assert combined.num_chunks == 2
        assert list(combined.column_data(0)) == expected
        assert list(combined.to_python_columns()[0]) == expected
        assert combined.to_python_rows() == [(state,) for state in expected]

    @pytest.mark.parametrize("state", [b"", b"\x01", b"\x01" + b"\x0d" * 7])
    def test_nullable_sum_truncation_is_eof(self, state):
        native = build_native_block_from_bodies(
            [("s", "AggregateFunction(sum, Nullable(UInt64))", state)],
            1,
        )

        with pytest.raises(EOFError, match="Truncated Native data"):
            _ch_core.ColBatch.decode_native(native)

    @pytest.mark.parametrize(
        "type_name",
        [
            "AggregateFunction(sum, UInt64)",
            "AggregateFunction(sum, Nullable(UInt64))",
        ],
    )
    def test_zero_rows_preserves_schema(self, type_name):
        native = build_native_block_from_bodies([("s", type_name, b"")], 0)
        batch = _ch_core.ColBatch.decode_native(native)

        assert batch.num_rows == 0
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == []
        assert _ch_core.encode_native_block(["s"], [type_name], [[]], 0) == native

    @pytest.mark.parametrize(
        ("type_name", "value", "error"),
        [
            ("AggregateFunction(count)", "not-bytes", "AggregateFunction state bytes"),
            (
                "AggregateFunction(count)",
                array.array("b", [13]),
                "AggregateFunction state bytes",
            ),
            ("AggregateFunction(count)", None, "is None but AggregateFunction"),
            ("AggregateFunction(count)", b"", "not exactly one valid serialized"),
            ("AggregateFunction(count)", b"\x80", "not exactly one valid serialized"),
            (
                "AggregateFunction(nothingUInt64, Nullable(Nothing))",
                b"\x01",
                "not exactly one valid serialized",
            ),
            (
                "AggregateFunction(sum, UInt64)",
                b"\x00" * 7,
                "not exactly one valid serialized",
            ),
            (
                "AggregateFunction(sum, Nullable(UInt64))",
                b"",
                "not exactly one valid serialized",
            ),
            (
                "AggregateFunction(sum, Nullable(UInt64))",
                b"\x00\x0d",
                "not exactly one valid serialized",
            ),
            (
                "AggregateFunction(sum, Nullable(UInt64))",
                b"\x01" + b"\x0d" * 7,
                "not exactly one valid serialized",
            ),
            (
                "AggregateFunction(sum, Nullable(UInt64))",
                b"\x01" + b"\x0d" * 9,
                "not exactly one valid serialized",
            ),
        ],
    )
    def test_insert_rejects_invalid_state(self, type_name, value, error):
        with pytest.raises(ValueError, match=error):
            _ch_core.encode_native_block(["s"], [type_name], [[value]], 1)

    @pytest.mark.skipif(sys.version_info < (3, 12), reason="pure-python __buffer__")
    def test_resize_during_buffer_conversion_is_rejected(self):
        class SlotSwappingState:
            def __init__(self, container):
                self._container = container

            def __buffer__(self, flags):
                self._container[0] = b"\x0d"
                return memoryview(b"\x0d")

            def __del__(self):
                self._container.pop()

        values = [None, b"\x4f", b"\x05"]
        values[0] = SlotSwappingState(values)
        with pytest.raises(ValueError, match="resized during encoding"):
            _ch_core.encode_native_block(
                ["s"], ["AggregateFunction(count)"], [values], 3
            )

    @pytest.mark.parametrize(
        "type_name",
        [
            "Nullable(AggregateFunction(count))",
            "LowCardinality(AggregateFunction(count))",
            "Aggregatefunction(count)",
            "AggregateFunction(sum, Nullable(Nothing))",
            "AggregateFunction(sum, Nullable(String))",
        ],
    )
    def test_illegal_or_misspelled_type_is_rejected(self, type_name):
        with pytest.raises(NotImplementedError, match="unsupported"):
            _ch_core.encode_native_block(["s"], [type_name], [[b"\x00"]], 1)

    def test_null_nullable_tuple_requires_core_placeholder_state(self):
        type_name = "Nullable(Tuple(AggregateFunction(count)))"

        with pytest.raises(NotImplementedError, match="canonical placeholder state"):
            _ch_core.encode_native_block(["s"], [type_name], [[None]], 1)


class TestAliasRejections:
    @pytest.mark.parametrize(
        "type_name",
        ["Nullable(Ring)", "Nullable(Nested(a UInt32, b String))"],
    )
    def test_server_illegal_nullable_alias_rejected(self, type_name):
        # Ring and Nested expand to an Array, which is not nullable-able, so the
        # core parser rejects the type and encode raises at the header stage.
        with pytest.raises(NotImplementedError, match="unsupported ClickHouse type"):
            _ch_core.encode_native_block(["x"], [type_name], [[None]], 1)
