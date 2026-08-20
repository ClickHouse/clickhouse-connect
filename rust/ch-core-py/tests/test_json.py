from helpers import (
    _ch_core,
    _encode_plain_body,
    _encode_varint,
    _encode_varint_string,
    build_native_block_from_bodies,
    decimal,
    dt,
    pytest,
    struct,
    sys,
)


class _DictSubclass(dict):
    pass


class _StrSubclass(str):
    pass


def _deep_list(depth):
    doc = "x"
    for _ in range(depth):
        doc = [doc]
    return doc


def _json_v2_prefix():
    """JSON V2 state prefix: one dynamic path nested.value whose Dynamic has
    one String alternative plus the implicit SharedVariant, in BASIC mode."""
    prefix = bytearray(struct.pack("<Q", 2))
    prefix.extend(_encode_varint(1))
    prefix.extend(_encode_varint_string("nested.value"))
    prefix.extend(struct.pack("<Q", 2))
    prefix.extend(_encode_varint(1))
    prefix.extend(_encode_varint_string("String"))
    prefix.extend(struct.pack("<Q", 0))
    return bytes(prefix)


def _json_v2_rows(typed_vals, dyn_vals, shared_vals, typed_nullable=False):
    """JSON V2 row bodies matching _json_v2_prefix: the `a%2Eb` Int64 typed
    path, the nested.value String dynamic path (None = NULL), and a
    shared%2Ekey.inner Int64 shared pair (None = no pair in that row)."""
    rows = bytearray()
    if typed_nullable:
        rows.extend(bytes(1 if v is None else 0 for v in typed_vals))
    rows.extend(struct.pack(f"<{len(typed_vals)}q", *(v or 0 for v in typed_vals)))
    # "SharedVariant" < "String", so String is global discriminator 1.
    rows.extend(bytes(255 if v is None else 1 for v in dyn_vals))
    rows.extend(
        _encode_plain_body("String", [v for v in dyn_vals if v is not None])
    )
    ends = []
    total = 0
    for v in shared_vals:
        total += v is not None
        ends.append(total)
    rows.extend(struct.pack(f"<{len(shared_vals)}Q", *ends))
    for v in shared_vals:
        if v is not None:
            rows.extend(_encode_varint_string("shared%2Ekey.inner"))
    for v in shared_vals:
        if v is not None:
            cell = b"\x0a" + struct.pack("<q", v)
            rows.extend(_encode_varint(len(cell)))
            rows.extend(cell)
    return bytes(rows)


def _structured_json_block(nullable=False):
    """Independent V2 JSON fixture with typed, dynamic, and shared paths."""
    type_name = "JSON(`a%2Eb` Int64)"
    body = bytearray(_json_v2_prefix())
    if nullable:
        body.extend(b"\x00\x01")
    body.extend(_json_v2_rows([13, 79], ["user_1", None], [5, None]))
    if nullable:
        type_name = f"Nullable({type_name})"
    return build_native_block_from_bodies([("j", type_name, bytes(body))], 2)


def _array_structured_json_block():
    """Array(JSON) fixture: 2 rows over 3 structured elements with typed,
    dynamic, and shared paths. Element 1 has a NULL typed value."""
    type_name = "Array(JSON(`a%2Eb` Nullable(Int64)))"
    body = bytearray(_json_v2_prefix())
    body.extend(struct.pack("<QQ", 2, 3))
    body.extend(
        _json_v2_rows(
            [13, None, 79],
            ["user_1", None, None],
            [5, None, None],
            typed_nullable=True,
        )
    )
    return build_native_block_from_bodies([("j", type_name, bytes(body))], 2)


def _tuple_structured_json_block():
    """Tuple(JSON, UInt8) fixture: 2 rows with typed, dynamic, and shared
    paths in the JSON element. Row 1 has a NULL typed value."""
    type_name = "Tuple(JSON(`a%2Eb` Nullable(Int64)), UInt8)"
    body = bytearray(_json_v2_prefix())
    body.extend(
        _json_v2_rows([13, None], ["user_1", None], [5, None], typed_nullable=True)
    )
    body.extend(bytes([1, 2]))
    return build_native_block_from_bodies([("j", type_name, bytes(body))], 2)


class TestJson:
    def test_structured_typed_dynamic_shared_all_object_exits(self):
        expected = [
            {
                "a.b": 13,
                "nested": {"value": "user_1"},
                "shared.key": {"inner": 5},
            },
            {"a.b": 79},
        ]
        batch = _ch_core.ColBatch.decode_native(_structured_json_block())
        assert list(batch.column_data(0)) == expected
        assert [list(column) for column in batch.to_python_columns()] == [expected]
        assert list(batch.to_python_rows()) == [(expected[0],), (expected[1],)]

        pa = pytest.importorskip("pyarrow")
        table = pa.RecordBatchReader.from_stream(batch).read_all()
        assert table.num_rows == 2
        assert pa.types.is_struct(table.schema.field("j").type)

    def test_nullable_structured_json(self):
        batch = _ch_core.ColBatch.decode_native(_structured_json_block(nullable=True))
        assert list(batch.column_data(0)) == [
            {
                "a.b": 13,
                "nested": {"value": "user_1"},
                "shared.key": {"inner": 5},
            },
            None,
        ]

    def test_array_structured_json_all_object_exits(self):
        # Null policy inside a container: a NULL typed value keeps its key as
        # None while NULL dynamic values and absent shared pairs drop the key.
        expected = [
            [
                {
                    "a.b": 13,
                    "nested": {"value": "user_1"},
                    "shared.key": {"inner": 5},
                },
                {"a.b": None},
            ],
            [{"a.b": 79}],
        ]
        batch = _ch_core.ColBatch.decode_native(_array_structured_json_block())
        assert list(batch.column_data(0)) == expected
        assert [list(column) for column in batch.to_python_columns()] == [expected]
        assert list(batch.to_python_rows()) == [(expected[0],), (expected[1],)]

    def test_tuple_structured_json_all_object_exits(self):
        expected = [
            (
                {
                    "a.b": 13,
                    "nested": {"value": "user_1"},
                    "shared.key": {"inner": 5},
                },
                1,
            ),
            ({"a.b": None}, 2),
        ]
        batch = _ch_core.ColBatch.decode_native(_tuple_structured_json_block())
        assert list(batch.column_data(0)) == expected
        assert [list(column) for column in batch.to_python_columns()] == [expected]
        assert list(batch.to_python_rows()) == [(expected[0],), (expected[1],)]

    @pytest.mark.parametrize(
        ("type_name", "rows"),
        [
            ("JSON", [{"a": 13}, {"b": [1, 2]}]),
            ("Nullable(JSON)", [{"a": 13}, None]),
            ("Array(JSON)", [[{"a": 13}, {"b": "user_1"}], []]),
            ("Tuple(JSON, UInt8)", [({"a": 13}, 1), ({"b": "user_1"}, 2)]),
            (
                "Array(Tuple(JSON, UInt8))",
                [[({"a": 13}, 1)], [({"b": "user_1"}, 2)]],
            ),
            (
                "Map(String, JSON)",
                [{"first": {"a": 13}}, {"second": {"b": "user_1"}}],
            ),
        ],
    )
    def test_text_insert_container_matrix(self, type_name, rows):
        encoded = _ch_core.encode_native_block(["j"], [type_name], [rows], len(rows))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows

    def test_text_insert_variant(self):
        from clickhouse_connect.datatypes.dynamic import typed_variant

        rows = [typed_variant({"a": 13}, "JSON"), "user_1"]
        encoded = _ch_core.encode_native_block(
            ["j"], ["Variant(JSON, String)"], [rows], len(rows)
        )
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            {"a": 13},
            "user_1",
        ]

    def test_text_insert_preserves_column_wide_string_mode(self):
        rows = ['{"a":13}', '{"b":"user_1"}']
        encoded = _ch_core.encode_native_block(["j"], ["JSON"], [rows], len(rows))
        # The structure word immediately after the Native header is STRING=1.
        header = 2 + 2 + 5
        assert encoded[header : header + 8] == struct.pack("<Q", 1)
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            {"a": 13},
            {"b": "user_1"},
        ]

    def test_text_insert_serializes_each_fallback_value_once(self, monkeypatch):
        from clickhouse_connect.datatypes import dynamic

        calls = []

        def serialize(value):
            calls.append(value)
            return f'{{"id":{value}}}'

        monkeypatch.setattr(dynamic, "any_to_json", serialize)
        rows = [None, decimal.Decimal("13"), decimal.Decimal("79")]
        encoded = _ch_core.encode_native_block(
            ["j"], ["Nullable(JSON)"], [rows], len(rows)
        )
        assert calls == rows[1:]
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            None,
            {"id": 13},
            {"id": 79},
        ]

    def test_text_insert_rejects_list_resize_during_serializer(self, monkeypatch):
        from clickhouse_connect.datatypes import dynamic

        rows = [decimal.Decimal("13"), decimal.Decimal("79")]

        def serialize(value):
            rows.pop()
            return f'{{"id":{value}}}'

        monkeypatch.setattr(dynamic, "any_to_json", serialize)
        with pytest.raises(ValueError, match="changed size during JSON serialization"):
            _ch_core.encode_native_block(["j"], ["JSON"], [rows], len(rows))

    def test_text_insert_rejects_list_resize_during_serializer_resolution(
        self, monkeypatch
    ):
        import types

        rows = [decimal.Decimal("13"), {"id": 79}]

        fake = types.ModuleType("clickhouse_connect.datatypes.dynamic")

        def module_getattr(name):
            if name == "any_to_json":
                rows.clear()
                return lambda value: b"null"
            raise AttributeError(name)

        fake.__getattr__ = module_getattr
        monkeypatch.setitem(
            sys.modules, "clickhouse_connect.datatypes.dynamic", fake
        )
        with pytest.raises(ValueError, match="changed size during JSON serialization"):
            _ch_core.encode_native_block(["j"], ["JSON"], [rows], 2)

    def test_non_nullable_first_row_none_serializes_column(self):
        # first_value parity: non-nullable inspects row 0 alone, so a leading
        # None routes the later string through serialization as a JSON string
        # value, not an object.
        rows = [None, '{"a":1}']
        encoded = _ch_core.encode_native_block(["j"], ["JSON"], [rows], len(rows))
        expected = build_native_block_from_bodies(
            [
                (
                    "j",
                    "JSON",
                    struct.pack("<Q", 1)
                    + _encode_plain_body("String", ["null", '"{\\"a\\":1}"']),
                )
            ],
            2,
        )
        assert encoded == expected
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            None,
            '{"a":1}',
        ]

    def test_non_nullable_first_row_str_is_direct_text(self):
        rows = ['{"a":1}', None]
        encoded = _ch_core.encode_native_block(["j"], ["JSON"], [rows], len(rows))
        expected = build_native_block_from_bodies(
            [
                (
                    "j",
                    "JSON",
                    struct.pack("<Q", 1)
                    + _encode_plain_body("String", ['{"a":1}', "null"]),
                )
            ],
            2,
        )
        assert encoded == expected
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            {"a": 1},
            None,
        ]

    def test_nullable_first_non_null_str_is_direct_text(self):
        # Nullable first_value skips leading Nones, so the column stays text.
        rows = [None, '{"a":1}']
        encoded = _ch_core.encode_native_block(
            ["j"], ["Nullable(JSON)"], [rows], len(rows)
        )
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            None,
            {"a": 1},
        ]

    def test_direct_text_non_str_row_reports_inference(self):
        with pytest.raises(ValueError, match="pre-serialized JSON strings"):
            _ch_core.encode_native_block(
                ["j"], ["JSON"], [['{"a":1}', {"b": 2}]], 2
            )

    def test_json_named_arguments_with_spaces_parse(self):
        rows = [{"a": 13}]
        encoded = _ch_core.encode_native_block(
            ["j"],
            ["JSON(max_dynamic_paths = 8, max_dynamic_types = 4)"],
            [rows],
            1,
        )
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows

    def test_enum_name_containing_json_argument_text(self):
        type_name = "Enum8('max_dynamic_paths = ' = 1, 'plain' = 2)"
        rows = ["max_dynamic_paths = ", "plain"]
        encoded = _ch_core.encode_native_block(["e"], [type_name], [rows], len(rows))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows

    @pytest.mark.parametrize(
        "doc",
        [
            {},
            {"a": 1, "b": [1, 2, {"c": None}], "d": {"e": {"f": "g"}}},
            {"i": -(2**40), "zero": 0, "neg": -1, "max": 2**63 - 1, "min": -(2**63)},
            {"flag": True, "off": False, "none": None},
            {"text": "naïve ütf8 ✓", "quote": 'say "hi"', "back": "a\\b"},
            {"ctl": "line\nreturn\rtab\tunit\x1fbell\x07"},
            {"f": 1.5, "whole": 3.0, "neg": -2.25, "big": 1e16, "tiny": 1e-10},
            {"deep": [[[["x"]]]], "tup": (1, "two", None), "empty": []},
        ],
    )
    def test_native_writer_value_parity(self, doc):
        import json

        from clickhouse_connect.datatypes.dynamic import any_to_json

        encoded = _ch_core.encode_native_block(["j"], ["JSON"], [[doc]], 1)
        decoded = list(_ch_core.ColBatch.decode_native(encoded).column_data(0))[0]
        try:
            expected = json.loads(any_to_json(doc))
        except TypeError:
            # any_to_json cannot handle tuples; stdlib json defines the value.
            expected = json.loads(json.dumps(doc))
        assert decoded == expected

    def test_native_writer_skips_python_serializer(self, monkeypatch):
        from clickhouse_connect.datatypes import dynamic

        def forbidden(value):
            raise AssertionError("serializer must not run for native rows")

        monkeypatch.setattr(dynamic, "any_to_json", forbidden)
        rows = [{"id": 13}, {"n": [1, 2.5, None, True, "x"], "t": (1, 2)}]
        encoded = _ch_core.encode_native_block(["j"], ["JSON"], [rows], len(rows))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            {"id": 13},
            {"n": [1, 2.5, None, True, "x"], "t": [1, 2]},
        ]

    def test_native_writer_escaping_pinned(self):
        import json

        doc = {'k"\\\n\r\t\x01\x1f': "v\x00é✓"}
        encoded = _ch_core.encode_native_block(["j"], ["JSON"], [[doc]], 1)
        expected_doc = '{"k\\"\\\\\\n\\r\\t\\u0001\\u001f":"v\\u0000é✓"}'.encode()
        expected = build_native_block_from_bodies(
            [
                (
                    "j",
                    "JSON",
                    struct.pack("<Q", 1) + _encode_plain_body("String", [expected_doc]),
                )
            ],
            1,
        )
        assert encoded == expected
        assert json.loads(expected_doc) == doc

    @pytest.mark.parametrize(
        "doc",
        [
            {"big": 2**70},
            {"nested_big": [-(2**70)]},
            {"inf": float("inf")},
            {"nan": float("nan")},
            {"when": dt.datetime(2024, 1, 2, 3, 4, 5)},
            {"dec": decimal.Decimal("1.25")},
            _DictSubclass(a=1),
            {"s": _StrSubclass("x")},
            {"key_subclass_value": 1, _StrSubclass("k"): 2},
            _deep_list(150),
        ],
    )
    def test_native_writer_falls_back_per_row(self, doc, monkeypatch):
        from clickhouse_connect.datatypes import dynamic

        calls = []
        real = dynamic.any_to_json

        def recorder(value):
            calls.append(value)
            return real(value)

        monkeypatch.setattr(dynamic, "any_to_json", recorder)
        try:
            payload = real(doc)
        except (TypeError, ValueError):
            payload = None
        if payload is None:
            with pytest.raises(ValueError, match="cannot be serialized as JSON"):
                _ch_core.encode_native_block(["j"], ["JSON"], [[doc]], 1)
        else:
            encoded = _ch_core.encode_native_block(["j"], ["JSON"], [[doc]], 1)
            assert payload in encoded
        assert calls == [doc]

    def test_native_writer_fallback_rewinds_partial_row(self, monkeypatch):
        from clickhouse_connect.datatypes import dynamic

        # The dict prefix is natively serializable; the Decimal aborts the row
        # mid-document and the fallback output replaces it completely.
        monkeypatch.setattr(dynamic, "any_to_json", lambda value: b'{"fixed":1}')
        rows = [{"a": 1, "d": decimal.Decimal("1.5")}, {"b": 2}]
        encoded = _ch_core.encode_native_block(["j"], ["JSON"], [rows], len(rows))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            {"fixed": 1},
            {"b": 2},
        ]

    def test_invalid_structure_maps_to_value_error(self):
        malformed = build_native_block_from_bodies(
            [("j", "JSON", struct.pack("<Q", 99))], 1
        )
        with pytest.raises(ValueError, match="Invalid JSON layout"):
            _ch_core.ColBatch.decode_native(malformed)
