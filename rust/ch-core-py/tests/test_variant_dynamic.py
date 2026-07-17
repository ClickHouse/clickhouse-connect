from helpers import (
    ZoneInfo,
    _build_low_cardinality_body_no_prefix,
    _ch_core,
    _element_state_prefix,
    _encode_plain_body,
    _encode_varint,
    _encode_varint_string,
    _NdarrayLikeColumn,
    _uuid_wire_bytes,
    build_native_block,
    build_native_block_from_bodies,
    dt,
    pytest,
    struct,
    uuid,
)

# ---------------------------------------------------------------------------
# Variant
# ---------------------------------------------------------------------------


def _basic_variant_block(discriminators, string_values, uint_values):
    """Independent Native Variant(String, UInt64) fixture.

    BASIC mode is UInt64 zero, followed by one discriminator byte per logical
    row, then the String and UInt64 dense child bodies in canonical order.
    """
    body = bytearray(struct.pack("<Q", 0))
    body.extend(discriminators)
    body.extend(_encode_plain_body("String", string_values))
    body.extend(_encode_plain_body("UInt64", uint_values))
    return build_native_block_from_bodies(
        [("v", "Variant(String, UInt64)", bytes(body))], len(discriminators)
    )


class TestVariant:
    def test_golden_decode_all_object_exits_and_arrow(self):
        expected = [None, "user_1", 13, "user_2", 79, None]
        native = _basic_variant_block([255, 0, 1, 0, 1, 255], ["user_1", "user_2"], [13, 79])
        batch = _ch_core.ColBatch.decode_native(native)

        assert batch.column_type_names == ["Variant(String, UInt64)"]
        assert list(batch.column_data(0)) == expected
        assert list(batch.to_python_columns()[0]) == expected
        assert [row[0] for row in batch.to_python_rows()] == expected

        pa = pytest.importorskip("pyarrow")
        column = pa.RecordBatchReader.from_stream(batch).read_all().column("v")
        assert pa.types.is_union(column.type)
        assert column.to_pylist() == expected

    def test_encode_matches_golden_and_canonicalizes_alternatives(self):
        values = [None, "user_1", 13, "user_2", 79, None]
        encoded = _ch_core.encode_native_block(["v"], ["Variant(UInt64, String)"], [values], len(values))
        assert encoded == _basic_variant_block([255, 0, 1, 0, 1, 255], ["user_1", "user_2"], [13, 79])
        assert encoded == _ch_core.encode_native_block(["v"], ["Variant(UInt64, String)"], [tuple(values)], len(values))
        assert encoded == _ch_core.encode_native_block(["v"], ["Variant(UInt64, String)"], [_NdarrayLikeColumn(values)], len(values))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == values

        spaced = _ch_core.encode_native_block(["v"], ["Variant( UInt64 , String )"], [values], len(values))
        assert spaced == encoded

    def test_typed_variant_resolves_ambiguous_python_types(self):
        from clickhouse_connect.datatypes.dynamic import TypedVariant, typed_variant

        type_name = "Variant(Array(String), Array(UInt32))"
        values = [
            typed_variant([13, 79], "Array(UInt32)"),
            typed_variant(["a", "b"], "Array(String)"),
            typed_variant([], "Array(UInt32)"),
            None,
        ]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            [13, 79],
            ["a", "b"],
            [],
            None,
        ]

        with pytest.raises(ValueError, match="cannot map Python type list"):
            _ch_core.encode_native_block(["v"], [type_name], [[[13, 79]]], 1)

        class TypedVariantSubclass(TypedVariant):
            pass

        subclass_values = [TypedVariantSubclass([79, 13], "Array(UInt32)")]
        subclass_encoded = _ch_core.encode_native_block(["v"], [type_name], [subclass_values], 1)
        assert list(_ch_core.ColBatch.decode_native(subclass_encoded).column_data(0)) == [[79, 13]]

        resizing_values = [None, typed_variant([], "Array(UInt32)")]

        class ResizingTypedVariant(TypedVariant):
            @property
            def value(self):
                resizing_values.pop()
                return tuple.__getitem__(self, 0)

        resizing_values[0] = ResizingTypedVariant([13], "Array(UInt32)")
        with pytest.raises(ValueError, match="resized during encoding"):
            _ch_core.encode_native_block(["v"], [type_name], [resizing_values], len(resizing_values))

    def test_dense_child_error_reports_logical_row(self):
        from clickhouse_connect.datatypes.dynamic import typed_variant

        values = ["ok", typed_variant("not-an-int", "UInt64")]
        with pytest.raises(ValueError, match=r'column "v" row 1 .*UInt64'):
            _ch_core.encode_native_block(["v"], ["Variant(String, UInt64)"], [values], len(values))

    def test_exact_type_dispatch_keeps_bool_distinct_from_int(self):
        values = [True, 13, False, -79]
        encoded = _ch_core.encode_native_block(["v"], ["Variant(Int32, Bool)"], [values], len(values))
        decoded = list(_ch_core.ColBatch.decode_native(encoded).column_data(0))
        assert decoded == values
        assert [type(value) for value in decoded] == [bool, int, bool, int]

    def test_low_cardinality_alternative_prefix_and_identity(self):
        type_name = "Variant(LowCardinality(String), UInt64)"
        discriminators = [0, 1, 0, 255]
        body = bytearray(struct.pack("<Q", 0))
        body.extend(_element_state_prefix("LowCardinality(String)"))
        body.extend(discriminators)
        body.extend(_build_low_cardinality_body_no_prefix("LowCardinality(String)", ["user_1", "user_1"]))
        body.extend(_encode_plain_body("UInt64", [13]))
        native = build_native_block_from_bodies([("v", type_name, bytes(body))], len(discriminators))

        batch = _ch_core.ColBatch.decode_native(native)
        decoded = list(batch.column_data(0))
        assert decoded == ["user_1", 13, "user_1", None]
        assert decoded[0] is decoded[2]
        assert _ch_core.encode_native_block(["v"], [type_name], [decoded], len(decoded)) == native

    def test_nested_container_matrix_and_map_order(self):
        columns = [
            (
                "a",
                "Array(Variant(String, UInt64))",
                [["a", 13, None], [], [79, "b"]],
            ),
            (
                "t",
                "Tuple(Variant(Bool, String), Variant(Int32, String))",
                [(True, 13), (False, "x"), ("s", 79)],
            ),
            (
                "m",
                "Map(String, Variant(String, UInt64))",
                [{"a": "x", "b": 13, "c": "y"}, {}, {"d": 79, "e": "z"}],
            ),
            (
                "at",
                "Array(Tuple(Variant(Int32, String), UInt8))",
                [[(13, 1), ("a", 2)], [], [(79, 3), ("b", 4)]],
            ),
        ]
        encoded = _ch_core.encode_native_block(
            [name for name, _, _ in columns],
            [type_name for _, type_name, _ in columns],
            [values for _, _, values in columns],
            3,
        )
        batch = _ch_core.ColBatch.decode_native(encoded)
        expected = [values for _, _, values in columns]
        assert [list(batch.column_data(index)) for index in range(4)] == expected
        assert [list(column) for column in batch.to_python_columns()] == expected
        assert list(batch.to_python_rows()) == list(zip(*expected))

    def test_composite_alternatives_preserve_outer_null(self):
        type_name = "Variant(Array(Nullable(String)), Map(String, UInt64))"
        values = [None, ["x", None], {"a": 13}, [], {}]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == values

    def test_variant_default_inside_nullable_tuple(self):
        type_name = "Nullable(Tuple(Variant(Int32, String), String))"
        values = [None, (13, "a"), ("x", "b")]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == values

    def test_zero_rows_and_multiple_blocks(self):
        empty = _ch_core.encode_native_block(["v"], ["Variant(String, UInt64)"], [[]], 0)
        assert empty == build_native_block([("v", "Variant(String, UInt64)", [])])
        all_null = _ch_core.encode_native_block(
            ["v"], ["Variant(String, UInt64)"], [[None, None, None]], 3
        )
        assert all_null == _basic_variant_block([255, 255, 255], [], [])
        assert list(_ch_core.ColBatch.decode_native(all_null).column_data(0)) == [None, None, None]
        first = _basic_variant_block([0, 1], ["a"], [13])
        second = _basic_variant_block([255, 1, 0], ["b"], [79])
        batch = _ch_core.ColBatch.decode_native(first + second)
        assert list(batch.column_data(0)) == ["a", 13, None, 79, "b"]

    @pytest.mark.parametrize(
        "body",
        [
            struct.pack("<Q", 2),
            struct.pack("<Q", 0) + b"\x02",
        ],
        ids=["invalid_mode", "invalid_discriminator"],
    )
    def test_invalid_layout_is_value_error(self, body):
        block = build_native_block_from_bodies([("v", "Variant(String, UInt64)", body)], 1)
        with pytest.raises(ValueError, match="Invalid Variant layout"):
            _ch_core.ColBatch.decode_native(block)

    @pytest.mark.parametrize("slots", [(), ("x",)], ids=["empty", "one_slot"])
    def test_undersized_exact_typed_variant_raises_cleanly(self, slots):
        # tuple.__new__ builds an exact TypedVariant without the two-slot layout;
        # the encoder must fall back to attribute access, not read raw slots.
        from clickhouse_connect.datatypes.dynamic import TypedVariant

        values = [tuple.__new__(TypedVariant, slots)]
        with pytest.raises(IndexError):
            _ch_core.encode_native_block(["v"], ["Variant(String, UInt64)"], [values], 1)

    def test_metaclass_hash_never_runs_during_dispatch(self):
        # Dispatch is a pointer-identity scan, so a metaclass __hash__/__eq__
        # that mutates the source list never runs and the list stays intact.
        values = []

        class MutatingMeta(type):
            def __hash__(cls):
                values.clear()
                return 13

            def __eq__(cls, other):
                values.clear()
                return NotImplemented

        class UserValue(metaclass=MutatingMeta):
            pass

        values.extend([UserValue(), 79])
        with pytest.raises(ValueError, match="cannot map Python type"):
            _ch_core.encode_native_block(["v"], ["Variant(String, UInt64)"], [values], 2)
        assert len(values) == 2

    def test_str_subclass_type_name_matches_exact(self):
        from clickhouse_connect.datatypes.dynamic import TypedVariant

        class TypeName(str):
            pass

        type_names = ["Variant(String, UInt64)"]
        exact = [TypedVariant(13, "UInt64"), TypedVariant("user_1", "String")]
        subclassed = [TypedVariant(13, TypeName("UInt64")), TypedVariant("user_1", TypeName("String"))]
        expected = _ch_core.encode_native_block(["v"], type_names, [exact], 2)
        assert _ch_core.encode_native_block(["v"], type_names, [subclassed], 2) == expected

    def test_variant_cells_ignore_raw_time_ticks(self):
        from clickhouse_connect.datatypes.dynamic import typed_variant

        type_name = "Tuple(Time64(3), Variant(String, Time64(3)))"
        leaf = dt.timedelta(milliseconds=13)
        cell = dt.timedelta(milliseconds=79)
        rows = [(leaf, typed_variant(cell, "Time64(3)")), (leaf, "user_1")]
        encoded = _ch_core.encode_native_block(["t"], [type_name], [rows], 2)
        decoded = list(_ch_core.ColBatch.decode_native(encoded).column_data(0, raw_time_ticks=True))
        assert decoded == [(13, cell), (13, "user_1")]
        assert isinstance(decoded[0][0], int)
        assert isinstance(decoded[0][1], dt.timedelta)

    def test_zero_selected_low_cardinality_alternative(self):
        # No LC rows selected: the golden body is the mode word, the hoisted LC
        # key version, the discriminators, no LC body, then the UInt64 body.
        type_name = "Variant(LowCardinality(String), UInt64)"
        values = [13, 79]
        body = bytearray(struct.pack("<Q", 0))
        body.extend(_element_state_prefix("LowCardinality(String)"))
        body.extend(bytes([1, 1]))
        body.extend(_encode_plain_body("UInt64", values))
        expected = build_native_block_from_bodies([("v", type_name, bytes(body))], 2)
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], 2)
        assert encoded == expected
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == values


# ---------------------------------------------------------------------------
# Dynamic
# ---------------------------------------------------------------------------


def _flattened_dynamic_block(type_names, type_ids, child_bodies, type_name="Dynamic"):
    """Independent FLATTENED Dynamic fixture with block-local typed children."""
    # Widen to 4-byte indexes before growing a fixture past the u16 range.
    assert len(type_names) <= 65535
    body = bytearray(struct.pack("<Q", 3))
    body.extend(_encode_varint(len(type_names)))
    for child_type in type_names:
        body.extend(_encode_varint_string(child_type))
    if len(type_names) <= 255:
        body.extend(type_ids)
    else:
        body.extend(struct.pack(f"<{len(type_ids)}H", *type_ids))
    for child_body in child_bodies:
        body.extend(child_body)
    return build_native_block_from_bodies([("v", type_name, bytes(body))], len(type_ids))


def _shared_dynamic_block(cells, type_name="Dynamic"):
    """V2 Dynamic fixture whose only child is SharedVariant; None rows are NULL."""
    body = bytearray(struct.pack("<Q", 2))
    body.extend(_encode_varint(0))
    body.extend(struct.pack("<Q", 0))
    for cell in cells:
        body.append(255 if cell is None else 0)
    for cell in cells:
        if cell is not None:
            body.extend(_encode_varint(len(cell)))
            body.extend(cell)
    return build_native_block_from_bodies([("v", type_name, bytes(body))], len(cells))


_DYNAMIC_DIRECT_AND_SHARED = bytes.fromhex(
    """
    020101640744796e616d69630100000000000000030305496e7433320653
    7472696e670655496e7436340000000000000000ff01731444796e616d69
    63286d61785f74797065733d302901000000000000000000000000000000
    0000ff020101640744796e616d69630100000000000000030305496e7433
    3206537472696e670655496e74363400000000000000000206757365725f
    3101731444796e616d6963286d61785f74797065733d3029010000000000
    00000000000000000000000000030f0c4d020101640744796e616d696301
    00000000000000030305496e74333206537472696e670655496e74363400
    00000000000000034f0000000000000001731444796e616d6963286d6178
    5f74797065733d302901000000000000000000000000000000000000061e
    0103010203020101640744796e616d69630100000000000000030305496e
    74333206537472696e670655496e743634000000000000000000f3ffffff
    01731444796e616d6963286d61785f74797065733d302901000000000000
    00000000000000000000000007150568656c6c6f
    """
)

_DYNAMIC_NESTED = bytes.fromhex(
    """
    040101610e41727261792844796e616d6963290100000000000000020206
    537472696e670655496e7436340000000000000000030000000000000001
    02ff06757365725f320d000000000000000174155475706c652844796e61
    6d69632c2055496e7438290100000000000000010105496e743332000000
    000000000000b1ffffff05016d144d617028537472696e672c2044796e61
    6d6963290100000000000000020206537472696e670655496e7436340000
    0000000000000200000000000000017801790201017a4f00000000000000
    0261741c4172726179285475706c652844796e616d69632c2055496e7438
    29290100000000000000020205496e74333206537472696e670000000000
    000000020000000000000000020d00000001610102
    """
)

_DYNAMIC_TIME = bytes.fromhex(
    """
    01010174195475706c652844796e616d69632c2054696d65363428332929
    010000000000000001010954696d6536342833290000000000000000014f
    000000000000000d00000000000000
    """
)


class TestDynamic:
    def test_direct_shared_multi_block_all_object_exits_and_arrow(self):
        direct = [None, "user_1", 79, -13]
        shared_cells = [
            None,
            b"\x0f\x0cM",  # Date 19724
            b"\x1e\x01\x03\x01\x02\x03",  # Array(UInt8) [1, 2, 3]
            b"\x15\x05hello",  # String "hello"
        ]
        # Object exits decode SharedVariant cells to typed values.
        shared = [None, dt.date(2024, 1, 2), [1, 2, 3], "hello"]
        batch = _ch_core.ColBatch.decode_native(_DYNAMIC_DIRECT_AND_SHARED)

        assert batch.column_type_names == ["Dynamic", "Dynamic(max_types=0)"]
        assert batch.num_chunks == 4
        assert list(batch.column_data(0)) == direct
        assert list(batch.column_data(1)) == shared
        assert [list(column) for column in batch.to_python_columns()] == [direct, shared]
        assert list(batch.to_python_rows()) == list(zip(direct, shared))

        # The Arrow C Stream export keeps SharedVariant cells as raw bytes for
        # schema stability.
        pa = pytest.importorskip("pyarrow")
        table = pa.RecordBatchReader.from_stream(batch).read_all()
        assert table.column("d").to_pylist() == direct
        assert table.column("s").to_pylist() == shared_cells
        assert pa.types.is_union(table.schema.field("d").type)
        assert pa.types.is_union(table.schema.field("s").type)

    def test_shared_cells_decode_typed_value_matrix(self):
        value = uuid.UUID("12345678-1234-5678-1234-567812345678")
        cells = [
            b"\x1d" + _uuid_wire_bytes(value),
            b"\x14\x03\x03UTC" + struct.pack("<q", 1_500),  # DateTime64(3, 'UTC')
            b"\x27\x15\x0a" + b"\x01\x01a" + struct.pack("<q", -7),  # Map(String, Int64)
            # Array(Nullable(Int32)) [1, None]
            b"\x1e\x23\x09\x02" + b"\x00" + struct.pack("<i", 1) + b"\x01",
            # Named Tuple(a Bool, b String)
            b"\x20\x02\x01a\x2d\x01b\x15" + b"\x01" + b"\x01x",
            b"\x26\x15\x02lc",  # LowCardinality(String), no framing
            None,
        ]
        expected = [
            value,
            dt.datetime(1970, 1, 1, 0, 0, 1, 500_000),
            {"a": -7},
            [1, None],
            {"a": True, "b": "x"},
            "lc",
            None,
        ]
        batch = _ch_core.ColBatch.decode_native(_shared_dynamic_block(cells))
        assert list(batch.column_data(0)) == expected

        # The per-cell container route decodes the same cells: an
        # Array(Dynamic) whose only child is SharedVariant.
        present = [cell for cell in cells if cell is not None]
        body = bytearray(struct.pack("<Q", 2))
        body.extend(_encode_varint(0))
        body.extend(struct.pack("<Q", 0))
        body.extend(struct.pack("<Q", len(cells)))  # one array row
        body.extend(b"".join(b"\xff" if cell is None else b"\x00" for cell in cells))
        for cell in present:
            body.extend(_encode_varint(len(cell)))
            body.extend(cell)
        native = build_native_block_from_bodies([("v", "Array(Dynamic)", bytes(body))], 1)
        nested = _ch_core.ColBatch.decode_native(native)
        assert list(nested.column_data(0)) == [expected]

    def test_shared_aggregate_function_and_unsupported_stay_bytes(self):
        # Cells are varint-length framed, so the raw bytes stay recoverable
        # whenever the descriptor is unsupported or does not parse (an unknown
        # tag is a future server type, not corruption).
        agg_cell = bytes([0x25, 0x00, 0x03]) + b"sum" + bytes([0x00, 0x01, 0x04]) + struct.pack("<Q", 79)
        json_cell = b"\x30\x01\x02\x03"  # JSON: unsupported descriptor
        variant_cell = b"\x2a\x02\x15\x04" + b"\x00\x01x"  # Variant value encoding is undefined here
        # Variant(DateTime('Bad/Zone'), String): the bytes fallback must not
        # run context preparation, which would raise for the unknown zone.
        bad_tz_cell = b"\x2a\x02\x12\x08Bad/Zone\x15" + b"\x00"
        unknown_tag_cell = b"\xfe"  # tag past every descriptor ClickHouse defines
        empty_cell = b""
        cells = [agg_cell, json_cell, variant_cell, bad_tz_cell, unknown_tag_cell, empty_cell]
        batch = _ch_core.ColBatch.decode_native(_shared_dynamic_block(cells))
        assert list(batch.column_data(0)) == cells

    @pytest.mark.parametrize(
        "cell",
        [
            b"\x09\x01\x02",  # Int32 truncated to 2 bytes
            b"\x09\x01\x02\x03\x04\x05",  # trailing byte after the Int32
            b"\x1e\x01\x05\x01",  # Array(UInt8) count 5, one element present
        ],
    )
    def test_malformed_shared_cell_is_value_error(self, cell):
        # The descriptor parsed, so a payload that fails to decode is real
        # corruption, not a future server type.
        batch = _ch_core.ColBatch.decode_native(_shared_dynamic_block([cell]))
        with pytest.raises(ValueError, match="SharedVariant cell"):
            batch.column_data(0)

    def test_array_dynamic_with_uuid_and_tz_children(self):
        # Exercises the per-cell Array chain context cache: block-local
        # children DateTime('America/New_York'), SharedVariant, UUID in
        # canonical order.
        first = uuid.UUID("12345678-1234-5678-1234-567812345678")
        second = uuid.UUID("87654321-4321-8765-4321-876543218765")
        seconds = 1_704_207_600
        body = bytearray(struct.pack("<Q", 2))
        body.extend(_encode_varint(2))
        body.extend(_encode_varint_string("DateTime('America/New_York')"))
        body.extend(_encode_varint_string("UUID"))
        body.extend(struct.pack("<Q", 0))
        body.extend(struct.pack("<QQ", 2, 3))  # array offsets: rows [2, 1]
        body.extend(bytes([2, 0, 2]))  # element discriminators
        body.extend(struct.pack("<I", seconds))
        body.extend(_uuid_wire_bytes(first))
        body.extend(_uuid_wire_bytes(second))
        native = build_native_block_from_bodies([("v", "Array(Dynamic)", bytes(body))], 2)

        tz_value = dt.datetime.fromtimestamp(seconds, ZoneInfo("America/New_York"))
        batch = _ch_core.ColBatch.decode_native(native)
        assert list(batch.column_data(0)) == [[first, tz_value], [second]]

    def test_nested_container_matrix(self):
        expected = [
            [["user_2", 13, None]],
            [(-79, 5)],
            [{"x": 79, "y": "z"}],
            [[(13, 1), ("a", 2)]],
        ]
        batch = _ch_core.ColBatch.decode_native(_DYNAMIC_NESTED)

        assert [list(batch.column_data(index)) for index in range(4)] == expected
        assert [list(column) for column in batch.to_python_columns()] == expected
        assert list(batch.to_python_rows()) == list(zip(*expected))

    def test_block_local_children_unify_for_arrow(self):
        string_block = _flattened_dynamic_block(["String"], [0], [_encode_plain_body("String", ["user_1"])])
        uint_block = _flattened_dynamic_block(["UInt64"], [0], [_encode_plain_body("UInt64", [79])])
        first, second = (
            list(_ch_core.BlockDecoder(string_block))[0],
            list(_ch_core.BlockDecoder(uint_block))[0],
        )
        batch = _ch_core.ColBatch.from_batches([first, second])
        assert list(batch.column_data(0)) == ["user_1", 79]

        pa = pytest.importorskip("pyarrow")
        table = pa.RecordBatchReader.from_stream(batch).read_all()
        assert table.column("v").to_pylist() == ["user_1", 79]
        child_names = [field.name for field in table.schema.field("v").type]
        assert child_names == ["String", "UInt64", "NULL"]

    def test_dynamic_cells_ignore_raw_time_ticks(self):
        batch = _ch_core.ColBatch.decode_native(_DYNAMIC_TIME)
        (dynamic_value, plain_value) = list(batch.column_data(0, raw_time_ticks=True))[0]
        assert dynamic_value == dt.timedelta(milliseconds=79)
        assert plain_value == 13
        assert isinstance(dynamic_value, dt.timedelta)
        assert isinstance(plain_value, int)

    def test_invalid_layout_is_column_named_value_error(self):
        malformed = bytearray(_DYNAMIC_DIRECT_AND_SHARED)
        prefix = malformed.index(b"\x07Dynamic") + len(b"\x07Dynamic")
        malformed[prefix : prefix + 8] = struct.pack("<Q", 4)
        with pytest.raises(ValueError, match="Invalid Dynamic layout for column 'd'"):
            _ch_core.ColBatch.decode_native(malformed)

    def test_arrow_reports_result_wide_child_limit(self):
        pa = pytest.importorskip("pyarrow")
        child_count = 16_257
        type_names = [f"FixedString({width})" for width in range(1, child_count + 1)]
        child_bodies = [b"x"] + [b""] * (child_count - 1)
        native = _flattened_dynamic_block(type_names, [0], child_bodies)
        batch = _ch_core.ColBatch.decode_native(native)

        with pytest.raises(pa.ArrowInvalid, match=r"16257.*16256|16256.*16257"):
            pa.RecordBatchReader.from_stream(batch)

    def test_all_null_dynamic_with_zero_children(self):
        native = _flattened_dynamic_block([], [0, 0, 0], [])
        batch = _ch_core.ColBatch.decode_native(native)
        assert list(batch.column_data(0)) == [None, None, None]
        assert list(batch.to_python_rows()) == [(None,), (None,), (None,)]

    def test_stream_decoder_split_mid_prefix(self):
        # Byte-at-a-time feed splits every Dynamic prefix mid-way.
        decoder = _ch_core.StreamDecoder()
        batches = []
        for index in range(len(_DYNAMIC_DIRECT_AND_SHARED)):
            batches.extend(decoder.feed(_DYNAMIC_DIRECT_AND_SHARED[index : index + 1]))
        batches.extend(decoder.finish())
        batch = _ch_core.ColBatch.from_batches(batches)
        assert list(batch.column_data(0)) == [None, "user_1", 79, -13]
        assert list(batch.column_data(1)) == [None, dt.date(2024, 1, 2), [1, 2, 3], "hello"]

    @pytest.mark.parametrize("type_name", ["Dynamic", "Dynamic()", "Dynamic(max_types=0)"])
    def test_insert_encodes_string_column_with_str_parity(self, type_name):
        # The wire bytes are identical to a hand-built String column: header
        # says String, values are str(v), None is the literal "NULL".
        values = [True, 7, 1.5, "s", None]
        strings = ["True", "7", "1.5", "s", "NULL"]
        expected = _ch_core.encode_native_block(["v"], ["String"], [strings], 5)
        assert _ch_core.encode_native_block(["v"], [type_name], [values], 5) == expected

    def test_insert_nested_dynamic_substitutes_recursively(self):
        rows = [[1, None], [{"k": 2}]]
        strings = [["1", "NULL"], ["{'k': 2}"]]
        expected = _ch_core.encode_native_block(["v"], ["Array(String)"], [strings], 2)
        assert _ch_core.encode_native_block(["v"], ["Array(Dynamic)"], [rows], 2) == expected

        rows = [(3, 1), (None, 2)]
        strings = [("3", 1), ("NULL", 2)]
        expected = _ch_core.encode_native_block(["v"], ["Tuple(String, UInt8)"], [strings], 2)
        assert _ch_core.encode_native_block(["v"], ["Tuple(Dynamic, UInt8)"], [rows], 2) == expected

        rows = [{"a": 5}, {"b": None}]
        strings = [{"a": "5"}, {"b": "NULL"}]
        expected = _ch_core.encode_native_block(["v"], ["Map(String, String)"], [strings], 2)
        assert _ch_core.encode_native_block(["v"], ["Map(String, Dynamic)"], [rows], 2) == expected

    def test_insert_nested_alias_dynamic_substitutes(self):
        # Nested(a Dynamic) expands to Array(Tuple(a Dynamic)) before the
        # substitution, so all three spellings produce identical wire bytes.
        rows = [[(1,), (None,)], [({"k": 2},)]]
        strings = [[("1",), ("NULL",)], [("{'k': 2}",)]]
        expected = _ch_core.encode_native_block(["v"], ["Array(Tuple(a String))"], [strings], 2)
        assert _ch_core.encode_native_block(["v"], ["Nested(a Dynamic)"], [rows], 2) == expected
        assert _ch_core.encode_native_block(["v"], ["Array(Tuple(a Dynamic))"], [rows], 2) == expected

    def test_insert_zero_row_block(self):
        # The driver's empty-block probe: no NotImplementedError, and the
        # header carries the substituted String type.
        expected = _ch_core.encode_native_block(["v"], ["String"], [[]], 0)
        assert _ch_core.encode_native_block(["v"], ["Dynamic"], [[]], 0) == expected
