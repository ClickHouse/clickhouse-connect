from helpers import (
    _ch_core,
    _NdarrayLikeColumn,
    build_native_block,
    dt,
    ipaddress,
    pytest,
    uuid,
)


class TestEncodeFastPaths:
    """Exact list/tuple and buffer-protocol fast paths for primitive columns."""

    def _encode(self, type_name, values, row_count=None):
        n = len(values) if row_count is None else row_count
        return _ch_core.encode_native_block(["v"], [type_name], [values], n)

    @pytest.mark.parametrize(
        "type_name,values",
        [
            ("Int8", [-128, -1, 0, 127]),
            ("Int16", [-32768, 0, 32767]),
            ("Int32", [-(2**31), 0, 2**31 - 1]),
            ("Int64", [-(2**63), -1, 0, 2**63 - 1]),
            ("UInt8", [0, 255]),
            ("UInt16", [0, 65535]),
            ("UInt32", [0, 2**32 - 1]),
            ("UInt64", [0, 1, 2**64 - 1]),
            ("Float32", [0.0, -1.5, 3.25]),
            ("Float64", [0.0, -1.5, 1e300]),
            ("Bool", [True, False, True]),
        ],
    )
    def test_edge_values_match_helper_and_container_kinds_agree(self, type_name, values):
        from_list = self._encode(type_name, list(values))
        assert from_list == build_native_block([("v", type_name, values)])
        assert self._encode(type_name, tuple(values), len(values)) == from_list
        # Non-list, non-buffer container takes the generic path; same bytes.
        assert self._encode(type_name, _NdarrayLikeColumn(values), len(values)) == from_list

    @pytest.mark.parametrize(
        "type_name,values",
        [
            ("Int8", [0, 128]),
            ("Int8", [-129]),
            ("Int64", [2**63]),
            ("Int64", [-(2**63) - 1]),
            ("UInt8", [256]),
            ("UInt64", [-1]),
            ("UInt64", [2**64]),
            ("Float64", [10**400]),
            ("IntervalDay", [2**63]),
            ("IntervalDay", [-(2**63) - 1]),
        ],
    )
    def test_out_of_range_raises_conversion_error(self, type_name, values):
        with pytest.raises(ValueError, match=f"row {len(values) - 1} cannot be converted to {type_name}"):
            self._encode(type_name, values)

    @pytest.mark.parametrize("value", [1.5, "5", dt.timedelta(days=5)])
    def test_interval_rejects_non_int_values(self, value):
        with pytest.raises(ValueError, match="row 0 cannot be converted to IntervalDay"):
            self._encode("IntervalDay", [value])

    def test_none_in_non_nullable_raises(self):
        with pytest.raises(ValueError, match='column "v" row 1 is None but Int64 is not Nullable'):
            self._encode("Int64", [3, None, 5])

    @pytest.mark.parametrize("make", [list, tuple])
    def test_nullable_list_and_tuple(self, make):
        values = [3, None, -(2**63), None, 2**63 - 1]
        encoded = self._encode("Nullable(Int64)", make(values), len(values))
        assert encoded == build_native_block([("v", "Nullable(Int64)", values)])

    def test_fallback_types_still_accepted(self):
        from decimal import Decimal
        import enum

        class IntLike(enum.IntEnum):
            SEVEN = 7

        values = [True, IntLike.SEVEN, 13.0, Decimal("79.000")]
        assert self._encode("Int64", values) == build_native_block([("v", "Int64", [1, 7, 13, 79])])
        # Exact ints take the fast path into floats; bool goes through the fallback.
        assert self._encode("Float64", [1, True, 2.5]) == build_native_block(
            [("v", "Float64", [1.0, 1.0, 2.5])]
        )

    @pytest.mark.parametrize(
        "value,detail",
        [
            (13.5, "would lose fractional data; pass an integer value"),
            (float("nan"), "is not finite; pass an integer value"),
            (float("inf"), "is not finite; pass an integer value"),
        ],
    )
    def test_float_to_integer_rejection_is_actionable(self, value, detail):
        with pytest.raises(ValueError, match=detail):
            self._encode("Int32", [value])

    @pytest.mark.parametrize("value", ["13", "-79"])
    def test_numeric_string_to_integer_rejection_is_actionable(self, value):
        with pytest.raises(ValueError, match="strings are not accepted; pass an int instead"):
            self._encode("Int32", [value])

    def test_decimal_to_integer_rejection_is_actionable(self):
        from decimal import Decimal

        with pytest.raises(ValueError, match="would lose fractional data; pass an integer value"):
            self._encode("Int32", [Decimal("13.5")])
        with pytest.raises(ValueError, match="is not finite; pass an integer value"):
            self._encode("Int32", [Decimal("NaN")])

    def test_float32_overflow_becomes_inf(self):
        encoded = self._encode("Float32", [1e300])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_data(0)[0] == float("inf")

    def test_list_resized_during_fallback_raises(self):
        values = [1, None, 3, 4]

        class Evil:
            def __index__(self):
                del values[2:]
                return 7

        values[1] = Evil()
        with pytest.raises(ValueError, match="resized during encoding"):
            self._encode("Int64", values, 4)

    @pytest.mark.parametrize(
        "type_name,dtype,values",
        [
            ("Int8", "int8", [-128, 0, 127]),
            ("Int64", "int64", [-(2**63), 0, 2**63 - 1]),
            ("IntervalDay", "int64", [-(2**63), 0, 2**63 - 1]),
            ("UInt64", "uint64", [0, 2**64 - 1]),
            ("Float32", "float32", [0.0, -1.5, 3.25]),
            ("Float64", "float64", [0.0, -1.5, 1e300]),
        ],
    )
    def test_numpy_buffer_matches_list_path(self, type_name, dtype, values):
        np = pytest.importorskip("numpy")
        arr = np.array(values, dtype=dtype)
        assert self._encode(type_name, arr, len(values)) == self._encode(type_name, list(values))

    def test_numpy_strided_view_matches_list_path(self):
        np = pytest.importorskip("numpy")
        arr = np.arange(10, dtype="int64")[::2]
        assert self._encode("Int64", arr, 5) == self._encode("Int64", list(arr))

    def test_numpy_mismatched_dtype_falls_back(self):
        np = pytest.importorskip("numpy")
        arr = np.array([1, 2, 3], dtype="int32")
        assert self._encode("Int64", arr, 3) == self._encode("Int64", [1, 2, 3])

    def test_numpy_nullable_is_all_valid(self):
        np = pytest.importorskip("numpy")
        arr = np.array([1.5, 2.5], dtype="float64")
        expected = build_native_block([("v", "Nullable(Float64)", [1.5, 2.5])])
        assert self._encode("Nullable(Float64)", arr, 2) == expected

    @pytest.mark.parametrize(
        "type_name,dtype,values",
        [
            ("Int64", ">i8", [1, 2, 3]),
            ("Float64", ">f8", [1.5, -2.5, 1e300]),
        ],
    )
    def test_numpy_non_native_byte_order_matches_list_path(self, type_name, dtype, values):
        np = pytest.importorskip("numpy")
        arr = np.array(values, dtype=dtype)
        assert self._encode(type_name, arr, len(values)) == self._encode(type_name, list(values))

    def test_char_format_buffer_still_raises_for_uint8(self):
        view = memoryview(b"AB").cast("c")
        with pytest.raises(ValueError, match="cannot be converted to UInt8"):
            self._encode("UInt8", view, 2)

    def test_tuple_fallback_types_still_accepted(self):
        import enum

        class IntLike(enum.IntEnum):
            SEVEN = 7

        values = (True, IntLike.SEVEN, 3)
        assert self._encode("Int64", values, 3) == build_native_block([("v", "Int64", [1, 7, 3])])
        with pytest.raises(ValueError, match="row 1 cannot be converted to Int64"):
            self._encode("Int64", (1, "x", 3), 3)


class TestScalarObjectInsertFastPath:
    """UUID, IPv4, and Enum8/16 fast paths over exact lists and tuples."""

    _E8 = "Enum8('alpha' = 1, 'beta' = 2, 'gamma' = 3)"
    _E16 = "Enum16('alpha' = -5, 'beta' = 0, 'gamma' = 1000)"

    def _encode(self, type_name, vals, n=None):
        n = len(vals) if n is None else n
        return _ch_core.encode_native_block(["v"], [type_name], [vals], n)

    def _decode(self, encoded):
        return list(_ch_core.ColBatch.decode_native(encoded).column_data(0))

    def test_uuid_containers_agree_and_round_trip(self):
        vals = [
            uuid.UUID(int=0),
            uuid.UUID("00112233-4455-6677-8899-aabbccddeeff"),
            uuid.UUID(int=(1 << 128) - 1),
        ]
        fast = self._encode("UUID", vals)
        assert fast == self._encode("UUID", tuple(vals), len(vals))
        assert fast == self._encode("UUID", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == vals

    def test_uuid_mixed_types_use_fallback(self):
        class SubUUID(uuid.UUID):
            pass

        known = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
        vals = [known, str(known), known.int, known.bytes, SubUUID(int=known.int)]
        fast = self._encode("UUID", vals)
        assert fast == self._encode("UUID", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == [known] * 5

    def test_nullable_uuid(self):
        vals = [uuid.UUID(int=9), None, uuid.UUID(int=0), None]
        fast = self._encode("Nullable(UUID)", vals)
        assert fast == self._encode("Nullable(UUID)", tuple(vals), len(vals))
        assert fast == self._encode("Nullable(UUID)", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == vals

    def test_uuid_errors_unchanged(self):
        with pytest.raises(ValueError, match="row 1 cannot be converted to UUID"):
            self._encode("UUID", [uuid.UUID(int=1), 1.5])
        with pytest.raises(ValueError, match="row 1 is None but UUID is not Nullable"):
            self._encode("UUID", [uuid.UUID(int=1), None])

    def test_ipv4_containers_agree_and_round_trip(self):
        vals = [
            ipaddress.IPv4Address("0.0.0.0"),
            ipaddress.IPv4Address("255.255.255.255"),
            ipaddress.IPv4Address("192.0.2.1"),
        ]
        fast = self._encode("IPv4", vals)
        assert fast == self._encode("IPv4", tuple(vals), len(vals))
        assert fast == self._encode("IPv4", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == vals

    def test_ipv4_mixed_types_without_object_wrappers(self):
        import enum

        class IntLike(enum.IntEnum):
            ADDR = 16909060

        vals = [ipaddress.IPv4Address("1.2.3.4"), "1.2.3.4", 16909060, IntLike.ADDR]
        fast = self._encode("IPv4", vals)
        assert fast == self._encode("IPv4", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == [ipaddress.IPv4Address("1.2.3.4")] * 4

    def test_nullable_ipv4(self):
        vals = [ipaddress.IPv4Address("1.2.3.4"), None, ipaddress.IPv4Address("0.0.0.0")]
        fast = self._encode("Nullable(IPv4)", vals)
        assert fast == self._encode("Nullable(IPv4)", tuple(vals), len(vals))
        assert fast == self._encode("Nullable(IPv4)", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == vals

    def test_ipv4_errors_unchanged(self):
        with pytest.raises(ValueError, match="row 0 cannot be converted to IPv4"):
            self._encode("IPv4", ["1.2.3.999"])
        with pytest.raises(ValueError, match="row 0 cannot be converted to IPv4"):
            self._encode("IPv4", [_NdarrayLikeColumn([1])])
        with pytest.raises(ValueError, match="row 1 cannot be converted to IPv4"):
            self._encode("IPv4", [1, 2**32])

    def test_enum8_labels_and_codes_round_trip(self):
        vals = ["alpha", "beta", 3, "alpha", 99]
        fast = self._encode(self._E8, vals)
        assert fast == self._encode(self._E8, tuple(vals), len(vals))
        assert fast == self._encode(self._E8, _NdarrayLikeColumn(vals), len(vals))
        # Unknown raw codes pass through and read back as None.
        assert self._decode(fast) == ["alpha", "beta", "gamma", "alpha", None]

    def test_enum16_labels_and_codes_round_trip(self):
        vals = ["gamma", -5, "beta", "gamma"]
        fast = self._encode(self._E16, vals)
        assert fast == self._encode(self._E16, _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == ["gamma", "alpha", "beta", "gamma"]

    def test_nullable_enum8(self):
        vals = ["alpha", None, "gamma", None]
        fast = self._encode(f"Nullable({self._E8})", vals)
        assert fast == self._encode(f"Nullable({self._E8})", tuple(vals), len(vals))
        assert fast == self._encode(f"Nullable({self._E8})", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == vals

    @pytest.mark.parametrize("type_name", [_E8, _E16])
    def test_enum_accepts_integral_float_and_pandas_nan(self, type_name):
        vals = [1.0, float("nan"), 2.0]
        encoded = self._encode(type_name, vals)
        assert encoded == build_native_block([("v", type_name, [1, 0, 2])])

    def test_enum_rejects_lossy_float_with_actionable_error(self):
        with pytest.raises(ValueError, match="would lose fractional data; pass a valid enum label or integral code"):
            self._encode(self._E8, [1.5])

    @pytest.mark.parametrize("type_name,enum_name", [(_E8, "Enum8"), (_E16, "Enum16")])
    def test_enum_unknown_label_raises(self, type_name, enum_name):
        expected = f'row 1 {enum_name} label "delta" is not defined'
        with pytest.raises(ValueError, match=expected):
            self._encode(type_name, ["alpha", "delta"])
        with pytest.raises(ValueError, match=expected):
            self._encode(type_name, _NdarrayLikeColumn(["alpha", "delta"]), 2)

    def test_enum_str_subclass_uses_fallback(self):
        class S(str):
            pass

        vals = [S("alpha"), "beta", S("gamma")]
        fast = self._encode(self._E8, vals)
        assert fast == self._encode(self._E8, _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == ["alpha", "beta", "gamma"]

    @pytest.mark.parametrize("size", [4, 8, 16, 32, 64])
    def test_enum_item_replacement_during_fallback_invalidates_ptr_cache(self, size):
        # A fallback __index__ drops the last ref to an already-scanned label;
        # the allocator can hand its address to a new same-size str, which
        # must not false-hit the pointer-identity cache.
        a, b = "A" * size, "B" * size
        tname = f"Enum8('{a}' = 1, '{b}' = 2, 'EV' = 3)"
        vals = ["A" * size, None, "C" * size]

        class Evil:
            def __index__(self):
                vals[0] = "x"  # drop the sole ref to the scanned label
                vals[2] = "B" * size  # same size class, may reuse its address
                return 3

        vals[1] = Evil()
        assert self._decode(self._encode(tname, vals, 3)) == [a, "EV", b]

    def test_enum_list_resized_during_fallback_raises(self):
        vals = ["alpha", None, "beta", "gamma"]

        class Evil:
            def __index__(self):
                del vals[2:]
                return 2

        vals[1] = Evil()
        with pytest.raises(ValueError, match="resized during encoding"):
            self._encode(self._E8, vals, 4)
