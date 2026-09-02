from helpers import (
    _INTERVAL_TYPES,
    _ch_core,
    _encode_varint,
    _encode_varint_string,
    _NdarrayLikeColumn,
    _SeriesLikeColumn,
    build_native_block,
    build_native_block_from_bodies,
    decimal,
    dt,
    ipaddress,
    os,
    pytest,
    subprocess,
    sys,
    textwrap,
    uuid,
)


class TestEncodeNativeBlock:
    def test_indexable_non_sequence_columns_match_native_helper(self):
        names = ["i", "s"]
        type_names = ["Int32", "String"]
        columns = [
            _NdarrayLikeColumn([13, 79]),
            _SeriesLikeColumn(["user_1", "user_2"]),
        ]

        encoded = _ch_core.encode_native_block(names, type_names, columns, 2)
        expected = build_native_block(
            [
                ("i", "Int32", [13, 79]),
                ("s", "String", ["user_1", "user_2"]),
            ]
        )
        assert encoded == expected

    @pytest.mark.parametrize("bad_values", ["xy", b"xy", bytearray(b"xy")])
    def test_bare_string_or_bytes_column_rejected(self, bad_values):
        with pytest.raises(ValueError, match="bare str or bytes"):
            _ch_core.encode_native_block(["s"], ["String"], [bad_values], len(bad_values))

    def test_common_types_match_native_helper(self):
        enum_type = "Enum8('red' = 1, 'green' = 2)"
        names = ["i", "s", "n", "fs", "d", "ts", "e"]
        type_names = [
            "Int32",
            "String",
            "Nullable(UInt16)",
            "FixedString(4)",
            "Date",
            "DateTime64(3)",
            enum_type,
        ]
        aware_ts = dt.datetime(2024, 1, 15, 12, 34, 56, 789000, tzinfo=dt.timezone.utc)
        columns = [
            [13, 79],
            ["user_1", b"\xff"],
            [13, None],
            ["ab", b"xy\x00\x00"],
            [dt.date(1970, 1, 2), 19737],
            [aware_ts, 0],
            ["red", 2],
        ]
        encoded = _ch_core.encode_native_block(names, type_names, columns, 2)
        expected = build_native_block(
            [
                ("i", "Int32", [13, 79]),
                ("s", "String", ["user_1", b"\xff"]),
                ("n", "Nullable(UInt16)", [13, None]),
                ("fs", "FixedString(4)", [b"ab", b"xy"]),
                ("d", "Date", [1, 19737]),
                ("ts", "DateTime64(3)", [1705322096789, 0]),
                ("e", enum_type, [1, 2]),
            ]
        )
        assert encoded == expected

    def test_datetime64_pre_epoch_fractional_encode(self):
        values = [
            dt.datetime(1969, 12, 31, 23, 59, 59, 500000, tzinfo=dt.timezone.utc),
            dt.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=dt.timezone.utc),
            dt.datetime(1970, 1, 1, 0, 0, 0, 999000, tzinfo=dt.timezone.utc),
        ]
        encoded = _ch_core.encode_native_block(["ts"], ["DateTime64(3)"], [values], len(values))
        expected = build_native_block([("ts", "DateTime64(3)", [-500, -1, 999])])
        assert encoded == expected
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            dt.datetime(1969, 12, 31, 23, 59, 59, 500000),
            dt.datetime(1969, 12, 31, 23, 59, 59, 999000),
            dt.datetime(1970, 1, 1, 0, 0, 0, 999000),
        ]

    @pytest.mark.parametrize("type_name", _INTERVAL_TYPES)
    def test_interval_round_trip_preserves_kind_and_signed_i64(self, type_name):
        values = [-(2**63), -79, 0, 13, 2**63 - 1]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))

        assert encoded == build_native_block([("v", type_name, values)])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == values
        assert list(batch.to_python_columns()[0]) == values
        assert batch.to_python_rows() == [(value,) for value in values]

    def test_interval_wrappers_and_containers_round_trip(self):
        columns = [
            ("scalar", "IntervalDay", [-13, 0, 79]),
            ("nullable", "Nullable(IntervalHour)", [-13, None, 79]),
            ("array", "Array(IntervalMinute)", [[-13, 79], [], [0]]),
            ("tuple", "Tuple(IntervalSecond, String)", [(-13, "x"), (0, "y"), (79, "z")]),
            (
                "array_tuple",
                "Array(Tuple(IntervalMillisecond, IntervalMonth))",
                [[(-13, 1), (79, -2)], [], [(0, 3)]],
            ),
            ("map", "Map(IntervalDay, String)", [{-13: "x"}, {}, {79: "z"}]),
            ("low_cardinality", "LowCardinality(IntervalHour)", [13, 79, 13]),
        ]
        encoded = _ch_core.encode_native_block(
            [name for name, _, _ in columns],
            [type_name for _, type_name, _ in columns],
            [values for _, _, values in columns],
            3,
        )

        assert encoded == build_native_block(columns)
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name for _, type_name, _ in columns]
        assert batch.to_python_columns() == [values for _, _, values in columns]

    @pytest.mark.parametrize(
        "type_name",
        ["intervalDay", "Intervalday", "INTERVALDAY", "IntervalDays", "IntervalDay()"],
    )
    def test_interval_type_names_are_exact(self, type_name):
        with pytest.raises(NotImplementedError, match="unsupported ClickHouse type"):
            _ch_core.encode_native_block(["v"], [type_name], [[13]], 1)

    def test_prefix_is_prepended(self):
        payload = _ch_core.encode_native_block(["v"], ["Int8"], [[13]], 1)
        prefix = b"INSERT INTO t FORMAT Native\n"
        encoded = _ch_core.encode_native_block(["v"], ["Int8"], [[13]], 1, prefix=prefix)
        assert encoded == prefix + payload

    def test_low_cardinality_round_trip(self):
        vals = ["x", None, "y", "x", None, ""]
        encoded = _ch_core.encode_native_block(["c"], ["LowCardinality(Nullable(String))"], [vals], len(vals))
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == ["LowCardinality(Nullable(String))"]
        assert list(batch.column_data(0)) == vals

    def test_uuid_bytes_match_python_serializer_order(self):
        value_uuid = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
        encoded = _ch_core.encode_native_block(["u"], ["UUID"], [[value_uuid.bytes]], 1)
        expected_body = bytearray()
        expected_body.extend(bytes(reversed(value_uuid.bytes[:8])))
        expected_body.extend(bytes(reversed(value_uuid.bytes[8:])))
        expected = build_native_block_from_bodies([("u", "UUID", expected_body)], 1)
        assert encoded == expected

    def test_special_binary_types_exact_bytes(self):
        value_uuid = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
        ipv4_values = ["192.0.2.1", ipaddress.IPv4Address("198.51.100.7")]
        ipv6_values = [ipaddress.IPv6Address("2001:db8::1"), "192.0.2.9"]
        decimals = [decimal.Decimal("123.4567"), "-1.5"]

        encoded = _ch_core.encode_native_block(
            ["u", "v4", "v6", "dec"],
            ["UUID", "IPv4", "IPv6", "Decimal(20, 4)"],
            [[value_uuid, 0], ipv4_values, ipv6_values, decimals],
            2,
        )

        uuid_body = bytearray()
        uuid_int = value_uuid.int
        uuid_body.extend((uuid_int >> 64).to_bytes(8, "little"))
        uuid_body.extend((uuid_int & 0xFFFFFFFFFFFFFFFF).to_bytes(8, "little"))
        uuid_body.extend(bytes(16))

        ipv4_body = bytearray()
        for value in ipv4_values:
            ipv4_body.extend(int(ipaddress.IPv4Address(value)).to_bytes(4, "little"))

        ipv6_body = bytearray()
        ipv6_body.extend(ipv6_values[0].packed)
        ipv6_body.extend(b"\x00" * 10 + b"\xff\xff" + ipaddress.IPv4Address(ipv6_values[1]).packed)

        decimal_body = bytearray()
        decimal_body.extend((1234567).to_bytes(16, "little", signed=True))
        decimal_body.extend((-15000).to_bytes(16, "little", signed=True))

        expected = build_native_block_from_bodies(
            [
                ("u", "UUID", uuid_body),
                ("v4", "IPv4", ipv4_body),
                ("v6", "IPv6", ipv6_body),
                ("dec", "Decimal(20, 4)", decimal_body),
            ],
            2,
        )
        assert encoded == expected

    def test_decimal_precision_boundary_and_overflow(self):
        encoded = _ch_core.encode_native_block(
            ["d"],
            ["Decimal(3, 1)"],
            [[decimal.Decimal("99.9"), decimal.Decimal("-99.9")]],
            2,
        )
        body = bytearray()
        body.extend((999).to_bytes(4, "little", signed=True))
        body.extend((-999).to_bytes(4, "little", signed=True))
        assert encoded == build_native_block_from_bodies([("d", "Decimal(3, 1)", body)], 2)

        for value in (decimal.Decimal("100.0"), decimal.Decimal("-100.0"), "999"):
            with pytest.raises(ValueError, match="exceeds precision 3"):
                _ch_core.encode_native_block(["d"], ["Decimal(3, 1)"], [[value]], 1)

    def test_decimal_str_failure_is_value_error_with_context(self):
        class BadDecimalStr:
            def __str__(self):
                raise RuntimeError("cannot render")

        with pytest.raises(ValueError, match='column "d" row 0 Decimal value cannot be stringified'):
            _ch_core.encode_native_block(["d"], ["Decimal(9, 2)"], [[BadDecimalStr()]], 1)

    def test_encode_errors_are_specific(self):
        with pytest.raises(ValueError, match="row_count"):
            _ch_core.encode_native_block(["v"], ["Int8"], [[13, 79]], 1)
        with pytest.raises(ValueError, match="not Nullable"):
            _ch_core.encode_native_block(["v"], ["Int8"], [[None]], 1)
        with pytest.raises(ValueError, match="FixedString binary value"):
            _ch_core.encode_native_block(["fs"], ["FixedString(4)"], [[b"xy"]], 1)
        with pytest.raises(NotImplementedError, match="unsupported ClickHouse type"):
            _ch_core.encode_native_block(["v"], ["Object('json')"], [[{"a": 1}]], 1)
        with pytest.raises(ValueError, match="label"):
            _ch_core.encode_native_block(["e"], ["Enum8('ok' = 1)"], [["missing"]], 1)


# ---------------------------------------------------------------------------
# Multi-column
# ---------------------------------------------------------------------------

class TestMultiColumn:
    def test_mixed_types(self):
        data = build_native_block([
            ("i", "Int32", [1, 2]),
            ("f", "Float64", [1.1, 2.2]),
            ("s", "String", ["a", "bb"]),
            ("b", "Bool", [1, 0]),
        ])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 2
        assert batch.num_columns == 4
        assert batch.column_names == ["i", "f", "s", "b"]
        assert list(batch.column_data(0)) == [1, 2]
        assert list(batch.column_data(2)) == ["a", "bb"]
        assert list(batch.column_data(3)) == [True, False]


# ---------------------------------------------------------------------------
# Python access
# ---------------------------------------------------------------------------

class TestPythonAccess:
    def test_to_python_rows(self):
        data = build_native_block([
            ("a", "Int32", [10, 20]),
            ("b", "String", ["x", "y"]),
        ])
        batch = _ch_core.ColBatch.decode_native(data)
        rows = list(batch.to_python_rows())
        assert rows == [(10, "x"), (20, "y")]

    def test_to_python_columns(self):
        data = build_native_block([
            ("a", "Int64", [1, 2, 3]),
            ("b", "Float64", [1.0, 2.0, 3.0]),
        ])
        batch = _ch_core.ColBatch.decode_native(data)
        cols = list(batch.to_python_columns())
        assert list(cols[0]) == [1, 2, 3]

    def test_column_data_out_of_range(self):
        data = build_native_block([("a", "Int64", [1])])
        batch = _ch_core.ColBatch.decode_native(data)
        with pytest.raises(ValueError, match="out of range"):
            batch.column_data(5)


# ---------------------------------------------------------------------------
# Path equivalence
# ---------------------------------------------------------------------------

class TestPathEquivalence:
    _COLUMNS = [
        ("b", "Bool", [1, 0, 1]),
        ("i8", "Int8", [-5, 0, 127]),
        ("i16", "Int16", [-300, 0, 32767]),
        ("i32", "Int32", [-70000, 0, 70000]),
        ("i64", "Int64", [-1, 0, 2**62]),
        ("u8", "UInt8", [0, 128, 255]),
        ("u16", "UInt16", [0, 13, 65535]),
        ("u32", "UInt32", [0, 79, 4_000_000_000]),
        ("u64", "UInt64", [0, 1, 2**64 - 1]),
        ("f32", "Float32", [1.5, -2.25, 0.0]),
        ("f64", "Float64", [1.5, -0.1, 1e300]),
        ("s", "String", ["u1", "", b"\xff"]),
        ("fs", "FixedString(2)", [b"ab", b"cd", b"ef"]),
        ("d", "Date", [0, 100, 19737]),
        ("d32", "Date32", [-25567, 0, 19737]),
        ("dt", "DateTime", [0, 961056000, 1705322096]),
        ("dtz", "DateTime('America/New_York')", [0, 961056000, 1705322096]),
        ("ts", "DateTime64(3)", [0, 13, 1705322096789]),
        ("tsz", "DateTime64(6, 'Asia/Istanbul')", [0, 79, 1705322096789012]),
        ("nb", "Nullable(Bool)", [1, None, 0]),
        ("ni", "Nullable(Int64)", [13, None, 79]),
        ("nf", "Nullable(Float64)", [1.5, None, -2.5]),
        ("ns", "Nullable(String)", ["x", None, b"\x80"]),
        ("nts", "Nullable(DateTime64(3))", [1705322096789, None, 0]),
    ]

    def _assert_paths_agree(self, batch):
        via_columns = [list(c) for c in batch.to_python_columns()]
        via_column_data = [list(batch.column_data(i)) for i in range(batch.num_columns)]
        via_rows = [list(col) for col in zip(*batch.to_python_rows())]
        assert via_columns == via_column_data
        assert via_columns == via_rows

    def test_all_types_agree_across_paths(self):
        data = build_native_block(self._COLUMNS)
        self._assert_paths_agree(_ch_core.ColBatch.decode_native(data))

    def test_mid_column_error_raises_on_all_paths(self):
        # A timestamp beyond datetime.MAXYEAR fails partway through the
        # column; every path must surface a clean ValueError, not crash or
        # return a partial result.
        data = build_native_block([("ts", "DateTime64(0)", [0, 300_000_000_000])])
        batch = _ch_core.ColBatch.decode_native(data)
        with pytest.raises(ValueError):
            batch.column_data(0)
        with pytest.raises(ValueError):
            batch.to_python_columns()
        with pytest.raises(ValueError):
            batch.to_python_rows()

    def test_paths_agree_across_chunks(self):
        # Two Native blocks in one buffer decode as two chunks; every path
        # must concatenate them identically.
        first = build_native_block(self._COLUMNS)
        second = build_native_block(
            [(name, type_name, values[::-1]) for name, type_name, values in self._COLUMNS]
        )
        batch = _ch_core.ColBatch.decode_native(first + second)
        assert batch.num_chunks == 2
        assert batch.num_rows == 6
        self._assert_paths_agree(batch)


# ---------------------------------------------------------------------------
# Arrow export
# ---------------------------------------------------------------------------

class TestArrowExport:
    def test_arrow_c_stream(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([
            ("i", "Int32", [10, 20, 30]),
            ("f", "Float64", [1.5, 2.5, 3.5]),
            ("s", "String", ["a", "b", "c"]),
        ])
        batch = _ch_core.ColBatch.decode_native(data)
        reader = pa.RecordBatchReader.from_stream(batch)
        result = reader.read_all()

        assert result.num_rows == 3
        assert result.schema.field("i").type == pa.int32()
        assert result.schema.field("f").type == pa.float64()
        assert result.schema.field("s").type == pa.utf8()
        assert result.column("i").to_pylist() == [10, 20, 30]

    def test_arrow_bool(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("b", "Bool", [1, 0, 1])])
        batch = _ch_core.ColBatch.decode_native(data)
        reader = pa.RecordBatchReader.from_stream(batch)
        result = reader.read_all()
        assert result.schema.field("b").type == pa.bool_()
        assert result.column("b").to_pylist() == [True, False, True]

    def test_arrow_int_widths(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([
            ("a", "Int8", [1]),
            ("b", "Int16", [2]),
            ("c", "UInt32", [3]),
            ("d", "UInt64", [4]),
        ])
        batch = _ch_core.ColBatch.decode_native(data)
        reader = pa.RecordBatchReader.from_stream(batch)
        result = reader.read_all()
        assert result.schema.field("a").type == pa.int8()
        assert result.schema.field("b").type == pa.int16()
        assert result.schema.field("c").type == pa.uint32()
        assert result.schema.field("d").type == pa.uint64()

    def test_arrow_float32(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("f", "Float32", [1.5])])
        batch = _ch_core.ColBatch.decode_native(data)
        reader = pa.RecordBatchReader.from_stream(batch)
        result = reader.read_all()
        assert result.schema.field("f").type == pa.float32()

    def test_arrow_fixed_binary(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("fs", "FixedString(3)", [b"abc", b"xyz"])])
        batch = _ch_core.ColBatch.decode_native(data)
        reader = pa.RecordBatchReader.from_stream(batch)
        result = reader.read_all()
        assert result.schema.field("fs").type == pa.binary(3)
        assert result.column("fs").to_pylist() == [b"abc", b"xyz"]

    def test_arrow_nullable(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("n", "Nullable(Int32)", [1, None, 3])])
        batch = _ch_core.ColBatch.decode_native(data)
        reader = pa.RecordBatchReader.from_stream(batch)
        result = reader.read_all()
        col = result.column("n")
        assert col.null_count == 1
        assert col.to_pylist() == [1, None, 3]

    def test_arrow_tuple(self):
        # Tuple exports as an Arrow struct; a named tuple keeps its element names,
        # an unnamed one uses the core's positional field names.
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("t", "Tuple(a Int32, b String)", [(13, "x"), (79, "y")])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = pa.RecordBatchReader.from_stream(batch).read_all()
        assert pa.types.is_struct(result.schema.field("t").type)
        assert result.column("t").to_pylist() == [{"a": 13, "b": "x"}, {"a": 79, "b": "y"}]

    def test_arrow_map(self):
        # Map exports as an Arrow large_list of a key/value struct.
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("m", "Map(String, UInt8)", [{"k1": 1, "k2": 2}, {}])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = pa.RecordBatchReader.from_stream(batch).read_all()
        assert result.column("m").to_pylist() == [
            [{"key": "k1", "value": 1}, {"key": "k2", "value": 2}],
            [],
        ]


# ---------------------------------------------------------------------------
# PipeDecoder fd ownership
# ---------------------------------------------------------------------------

@pytest.mark.skipif(sys.platform == "win32", reason="PipeDecoder is unix-only")
class TestPipeDecoderFd:
    def test_invalid_fd_raises(self):
        with pytest.raises(OSError):
            _ch_core.PipeDecoder(999999)

    def test_negative_fd_raises(self):
        with pytest.raises(OSError):
            _ch_core.PipeDecoder(-1)

    def test_caller_closing_fd_is_safe(self):
        # The decoder reads from its own duplicate, so the caller closing both
        # pipe ends must not abort the process when the decoder is dropped.
        read_fd, write_fd = os.pipe()
        decoder = _ch_core.PipeDecoder(read_fd)
        os.close(write_fd)
        os.close(read_fd)
        assert list(decoder) == []
        del decoder

    def test_streams_blocks_from_pipe(self):
        read_fd, write_fd = os.pipe()
        os.write(write_fd, build_native_block([("v", "Int64", [13, 79])]))
        os.close(write_fd)
        decoder = _ch_core.PipeDecoder(read_fd)
        batches = [list(b.column_data(0)) for b in decoder]
        os.close(read_fd)
        assert batches == [[13, 79]]


# ---------------------------------------------------------------------------
# Arrow capsule lifecycle
# ---------------------------------------------------------------------------

class TestArrowCapsuleLifecycle:
    def test_unconsumed_capsule_does_not_leak(self):
        # Run in a subprocess so ru_maxrss reflects only this workload and not
        # the pytest process high-water mark.
        script = textwrap.dedent("""
            import resource
            import sys

            import _ch_core

            data = sys.stdin.buffer.read()

            def rss_kb():
                usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                # ru_maxrss is bytes on macOS, kilobytes on Linux.
                return usage // 1024 if sys.platform == "darwin" else usage

            for _ in range(1000):
                _ch_core.ColBatch.decode_native(data).__arrow_c_stream__()
            base = rss_kb()
            for _ in range(50000):
                _ch_core.ColBatch.decode_native(data).__arrow_c_stream__()
            print(rss_kb() - base)
        """)
        data = build_native_block([("v", "Int64", [13, 79, 5])])
        result = subprocess.run(
            [sys.executable, "-c", script],
            input=data,
            capture_output=True,
            check=True,
        )
        delta_kb = int(result.stdout)
        # The leak this guards against grew tens of MB over this loop.
        assert delta_kb < 16 * 1024

    def test_consumed_capsule_no_double_free(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("v", "Int64", [13, 79])])
        for _ in range(10000):
            table = pa.RecordBatchReader.from_stream(
                _ch_core.ColBatch.decode_native(data)
            ).read_all()
            assert table.num_rows == 2


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestUnsupportedType:
    def test_raises_value_error(self):
        # A server type the core does not implement surfaces as a clean
        # UnsupportedType error before body decoding.
        buf = bytearray()
        buf.extend(_encode_varint(1))
        buf.extend(_encode_varint(1))
        buf.extend(_encode_varint_string("id"))
        buf.extend(_encode_varint_string("QBit(UInt8, 8)"))
        with pytest.raises(NotImplementedError, match="Unsupported ClickHouse type 'QBit"):
            _ch_core.ColBatch.decode_native(bytes(buf))


class TestBufferInputs:
    def test_decode_native_accepts_buffer_types(self):
        data = build_native_block([("v", "Int64", [13, 79])])
        for view in (data, bytearray(data), memoryview(data)):
            batch = _ch_core.ColBatch.decode_native(view)
            assert list(batch.column_data(0)) == [13, 79]

    def test_feed_accepts_buffer_types(self):
        data = build_native_block([("v", "Int64", [13, 79])])
        for view in (data, bytearray(data), memoryview(data)):
            decoder = _ch_core.StreamDecoder()
            batches = list(decoder.feed(view)) + list(decoder.finish())
            assert [list(b.column_data(0)) for b in batches] == [[13, 79]]


class TestMismatchedBlocks:
    def test_mismatched_second_block_rejected_at_decode(self):
        # Every block of a result shares one schema; the core rejects a
        # mismatched later block at decode time. The binding also guards its
        # raw list and tuple fills as defense in depth.
        wide = build_native_block([("a", "Int64", [13]), ("b", "Int64", [79])])
        narrow = build_native_block([("a", "Int64", [5])])
        with pytest.raises(ValueError, match="schema differs"):
            _ch_core.ColBatch.decode_native(wide + narrow)

    def test_mismatched_type_rejected_at_decode(self):
        first = build_native_block([("a", "Int64", [13])])
        second = build_native_block([("a", "String", ["u1"])])
        with pytest.raises(ValueError, match="schema differs"):
            _ch_core.ColBatch.decode_native(first + second)

    def test_stream_decoder_rejects_mismatch(self):
        first = build_native_block([("a", "Int64", [13])])
        second = build_native_block([("a", "String", ["u1"])])
        decoder = _ch_core.StreamDecoder()
        assert len(decoder.feed(first)) == 1
        with pytest.raises(ValueError, match="schema differs"):
            decoder.feed(second)

    def test_block_decoder_rejects_mismatch(self):
        first = build_native_block([("a", "Int64", [13])])
        second = build_native_block([("a", "String", ["u1"])])
        decoder = _ch_core.BlockDecoder(first + second)
        batch = next(decoder)
        assert list(batch.column_data(0)) == [13]
        with pytest.raises(ValueError, match="schema differs"):
            next(decoder)

    @pytest.mark.skipif(sys.platform == "win32", reason="PipeDecoder is unix-only")
    def test_pipe_decoder_rejects_mismatch(self):
        read_fd, write_fd = os.pipe()
        os.write(write_fd, build_native_block([("a", "Int64", [13])]))
        os.write(write_fd, build_native_block([("a", "String", ["u1"])]))
        os.close(write_fd)
        decoder = _ch_core.PipeDecoder(read_fd)
        batches = []
        with pytest.raises(ValueError, match="schema differs"):
            for batch in decoder:
                batches.append(list(batch.column_data(0)))
        os.close(read_fd)
        # Whether the first good block is yielded depends on how the pipe
        # reads chunk the feed; only the rejection is guaranteed.
        assert batches in ([], [[13]])


class TestTruncatedData:
    def test_decode_native_truncated_raises_eof(self):
        data = build_native_block([("v", "Int64", [13, 79])])
        with pytest.raises(EOFError):
            _ch_core.ColBatch.decode_native(data[:-3])

    def test_stream_decoder_truncated_finish_raises_eof(self):
        data = build_native_block([("v", "Int64", [13, 79])])
        decoder = _ch_core.StreamDecoder()
        assert list(decoder.feed(data[:-3])) == []
        with pytest.raises(EOFError):
            decoder.finish()


class TestEmptyBatch:
    def test_zero_rows(self):
        data = build_native_block([("a", "Int32", []), ("b", "String", [])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 0
        assert batch.num_columns == 2
        assert list(batch.to_python_rows()) == []


class TestFromBatches:
    _COLUMNS_A = [("id", "Int64", [13, 79]), ("name", "String", ["user_1", "user_2"])]
    _COLUMNS_B = [("id", "Int64", [5]), ("name", "String", ["user_3"])]

    def test_merge_matches_decode_native(self):
        first = build_native_block(self._COLUMNS_A)
        second = build_native_block(self._COLUMNS_B)
        parts = list(_ch_core.BlockDecoder(first)) + list(_ch_core.BlockDecoder(second))
        merged = _ch_core.ColBatch.from_batches(parts)
        reference = _ch_core.ColBatch.decode_native(first + second)
        assert merged.num_rows == 3
        assert merged.num_chunks == 2
        assert list(merged.to_python_rows()) == list(reference.to_python_rows())
        pa = pytest.importorskip("pyarrow")
        merged_table = pa.RecordBatchReader.from_stream(merged).read_all()
        reference_table = pa.RecordBatchReader.from_stream(reference).read_all()
        assert merged_table == reference_table

    def test_mismatched_schema_raises(self):
        first = _ch_core.ColBatch.decode_native(
            build_native_block([("id", "Int64", [13])])
        )
        wrong_type = _ch_core.ColBatch.decode_native(
            build_native_block([("id", "String", ["user_1"])])
        )
        wrong_name = _ch_core.ColBatch.decode_native(
            build_native_block([("other", "Int64", [79])])
        )
        with pytest.raises(ValueError, match="Batch 1 schema differs"):
            _ch_core.ColBatch.from_batches([first, wrong_type])
        with pytest.raises(ValueError, match="Batch 2 schema differs"):
            _ch_core.ColBatch.from_batches([first, first, wrong_name])

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="at least one batch"):
            _ch_core.ColBatch.from_batches([])

    def test_zero_row_trailer_dropped(self):
        rows = _ch_core.ColBatch.decode_native(build_native_block(self._COLUMNS_A))
        # BlockDecoder keeps a zero-row block as a chunk, so the trailer batch
        # carries one zero-row chunk into the merge.
        (trailer,) = _ch_core.BlockDecoder(
            build_native_block([("id", "Int64", []), ("name", "String", [])])
        )
        assert trailer.num_chunks == 1
        merged = _ch_core.ColBatch.from_batches([rows, trailer])
        assert merged.num_rows == 2
        assert merged.num_chunks == 1
        assert list(merged.to_python_rows()) == [(13, "user_1"), (79, "user_2")]

    def test_all_zero_rows(self):
        empty_block = build_native_block([("id", "Int64", []), ("name", "String", [])])
        reference = _ch_core.ColBatch.decode_native(empty_block)
        # Each part holds one zero-row chunk; the merge must drop them all.
        (part,) = _ch_core.BlockDecoder(empty_block)
        merged = _ch_core.ColBatch.from_batches([part, part])
        assert merged.num_rows == 0
        assert merged.num_chunks == 0
        assert merged.num_chunks == reference.num_chunks
        assert list(merged.to_python_rows()) == list(reference.to_python_rows()) == []
        merged_cols = [list(c) for c in merged.to_python_columns()]
        reference_cols = [list(c) for c in reference.to_python_columns()]
        assert merged_cols == reference_cols == [[], []]
        pa = pytest.importorskip("pyarrow")
        table = pa.RecordBatchReader.from_stream(merged).read_all()
        assert table.schema.names == ["id", "name"]
        assert table.schema == pa.RecordBatchReader.from_stream(reference).read_all().schema
        assert table.num_rows == 0


class TestMaterializeFastPath:
    """Edge values and multi-chunk slot accounting for the typed fill loops."""

    _EDGE_COLUMNS = [
        ("i8", "Int8", [-128, -1, 127]),
        ("i64", "Int64", [-(2**63), 0, 2**63 - 1]),
        ("u32", "UInt32", [0, 2**31, 2**32 - 1]),
        ("u64", "UInt64", [0, 2**63, 2**64 - 1]),
        ("f64", "Float64", [-0.5, 0.0, 1.5]),
        ("b", "Bool", [True, False, True]),
        ("n", "Nullable(Int64)", [-(2**63), None, 2**63 - 1]),
    ]

    def test_edge_values_rows_columns_and_column_data(self):
        batch = _ch_core.ColBatch.decode_native(build_native_block(self._EDGE_COLUMNS))
        expected_cols = [values for _, _, values in self._EDGE_COLUMNS]
        assert [list(c) for c in batch.to_python_columns()] == expected_cols
        assert list(batch.to_python_rows()) == list(zip(*expected_cols))
        for idx, col in enumerate(expected_cols):
            assert list(batch.column_data(idx)) == col

    def test_multi_chunk_mixed_fast_and_fallback_columns(self):
        block = build_native_block(
            [
                ("v", "Int64", [-(2**63), 2**63 - 1]),
                ("s", "String", ["user_1", "user_2"]),
                ("u", "UInt32", [2**32 - 1, 7]),
            ]
        )
        # The same batch three times stresses the per-chunk row-offset
        # bookkeeping with duplicate Arc chunks.
        (part,) = _ch_core.BlockDecoder(block)
        merged = _ch_core.ColBatch.from_batches([part, part, part])
        assert merged.num_chunks == 3
        expected_rows = [(-(2**63), "user_1", 2**32 - 1), (2**63 - 1, "user_2", 7)] * 3
        assert list(merged.to_python_rows()) == expected_rows
        assert [list(c) for c in merged.to_python_columns()] == [
            [-(2**63), 2**63 - 1] * 3,
            ["user_1", "user_2"] * 3,
            [2**32 - 1, 7] * 3,
        ]


class TestBlockInfo:
    def test_with_block_info(self):
        data = build_native_block(
            [("v", "Int64", [77, 88])],
            block_info=True,
        )
        batch = _ch_core.ColBatch.decode_native(data, has_block_info=True)
        assert batch.num_rows == 2
        assert list(batch.column_data(0)) == [77, 88]
