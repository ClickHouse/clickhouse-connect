from helpers import _ch_core, _NdarrayLikeColumn, build_native_block, pytest


class TestNumpyDatetimeScalar:
    def _encode(self, type_name, values, row_count=None):
        row_count = len(values) if row_count is None else row_count
        return _ch_core.encode_native_block(["t"], [type_name], [values], row_count)

    @pytest.mark.parametrize("type_name", ["DateTime", "DateTime64(0)", "DateTime64(9)"])
    def test_numpy_datetime_calendar_overflow(self, type_name):
        np = pytest.importorskip("numpy")
        with pytest.raises(ValueError, match="column.*row 0.*outside range"):
            self._encode(type_name, [np.datetime64(-1111120336821728400, "Y")])

    @pytest.mark.parametrize("type_name,ticks", [("DateTime", 13), ("DateTime64(3)", 13_000)])
    @pytest.mark.parametrize("dtype_kind", ["application", "datetime"])
    def test_timestamp_fallback_with_dtype(self, type_name, ticks, dtype_kind):
        np = pytest.importorskip("numpy")

        class TimestampValue:
            dtype = "application-specific" if dtype_kind == "application" else np.dtype("datetime64[s]")
            microsecond = 0

            def timestamp(self):
                return 13.0

        values = [TimestampValue()]
        expected = build_native_block([("t", type_name, [ticks])])
        assert self._encode(type_name, values) == expected
        assert self._encode(type_name, _NdarrayLikeColumn(values), 1) == expected

    @pytest.mark.parametrize("type_name", ["DateTime", "DateTime64(3)"])
    @pytest.mark.parametrize("dtype", [">M8[s]", "<M8[s]"])
    @pytest.mark.parametrize("shape", [(), (1,)])
    def test_datetime_numpy_array_cell_is_not_scalar(self, type_name, dtype, shape):
        np = pytest.importorskip("numpy")
        value = np.full(shape, 13, dtype=dtype)
        with pytest.raises(ValueError, match="column.*row 0 cannot be converted to DateTime"):
            self._encode(type_name, [value])

    @pytest.mark.parametrize(
        "unit,raw,numerator,denominator",
        [
            ("Y", 13, 410_227_200, 1),
            ("M", 13, 34_214_400, 1),
            ("M", -1, -2_678_400, 1),
            ("Y", -70, -2_208_988_800, 1),
            ("Y", 130, 4_102_444_800, 1),
            ("M", 1562, 4_107_542_400, 1),
            ("3M", 13, 102_470_400, 1),
            ("W", 13, 7_862_400, 1),
            ("D", 13, 1_123_200, 1),
            ("h", 13, 46_800, 1),
            ("m", 13, 780, 1),
            ("s", 13, 13, 1),
            ("3s", 13, 39, 1),
            ("ms", 13, 13, 1_000),
            ("us", 13, 13, 1_000_000),
            ("ns", 13, 13, 1_000_000_000),
            ("ns", -13, -13, 1_000_000_000),
            ("ps", 13, 13, 10**12),
            ("fs", 13, 13, 10**15),
            ("as", 13, 13, 10**18),
            ("us", 16_725_225_600_000_000, 16_725_225_600, 1),
        ],
    )
    @pytest.mark.parametrize("precision", [None, 0, 2, 3, 6, 9])
    @pytest.mark.parametrize("nullable", [False, True])
    def test_numpy_datetime_scalar_units(self, unit, raw, numerator, denominator, precision, nullable):
        np = pytest.importorskip("numpy")
        type_name = "DateTime" if precision is None else f"DateTime64({precision})"
        ticks = numerator * 10 ** (precision or 0) // denominator
        valid = 0 <= ticks < 2**32 if precision is None else -(2**63) <= ticks < 2**63
        if nullable:
            type_name = f"Nullable({type_name})"
        values = [np.datetime64(raw, unit)]
        if not valid:
            with pytest.raises(ValueError, match="outside.*range"):
                self._encode(type_name, values)
            return
        expected = build_native_block([("t", type_name, [ticks])])
        assert self._encode(type_name, values) == expected
        assert self._encode(type_name, _NdarrayLikeColumn(values), 1) == expected

    @pytest.mark.parametrize("unit", ["generic", "Y", "M", "W", "D", "h", "m", "s", "ms", "us", "ns", "ps", "fs", "as"])
    @pytest.mark.parametrize("type_name", ["DateTime", "DateTime64(0)", "DateTime64(2)", "DateTime64(3)", "DateTime64(6)", "DateTime64(9)"])
    @pytest.mark.parametrize("nullable", [False, True])
    def test_numpy_datetime_nat_rejected(self, unit, type_name, nullable):
        np = pytest.importorskip("numpy")
        value = np.datetime64("NaT") if unit == "generic" else np.datetime64("NaT", unit)
        if nullable:
            type_name = f"Nullable({type_name})"
            expected = build_native_block([("t", type_name, [None, None])])
            assert self._encode(type_name, [value, value]) == expected
            assert self._encode(type_name, _NdarrayLikeColumn([value, value]), 2) == expected
            assert self._encode(type_name, np.array([value, value], dtype=object)) == expected
            return
        with pytest.raises(ValueError, match="NaT"):
            self._encode(type_name, [value])

    @pytest.mark.parametrize(
        "type_name",
        ["DateTime", "DateTime64(0)", "DateTime64(2)", "DateTime64(3)", "DateTime64(6)", "DateTime64(9)"],
    )
    @pytest.mark.parametrize(
        "template,wrap",
        [
            ("Nullable({})", lambda values: values),
            ("Array(Nullable({}))", lambda values: [values]),
            ("Tuple(Nullable({}))", lambda values: [(value,) for value in values]),
            ("Array(Tuple(Nullable({})))", lambda values: [[(value,) for value in values]]),
            ("Map(String, Nullable({}))", lambda values: [{str(index): value for index, value in enumerate(values)}]),
            ("SimpleAggregateFunction(anyLast, Nullable({}))", lambda values: values),
        ],
    )
    def test_numpy_datetime_nat_nullable_positions(self, type_name, template, wrap):
        np = pytest.importorskip("numpy")
        values = wrap([None, 13, np.datetime64("NaT"), 79, np.datetime64("NaT", "ns"), None])
        type_name = template.format(type_name)
        expected = build_native_block([("t", type_name, wrap([None, 13, None, 79, None, None]))])
        assert self._encode(type_name, values) == expected
        assert self._encode(type_name, _NdarrayLikeColumn(values), len(values)) == expected

    def test_numpy_datetime_nat_low_cardinality_nullable(self):
        np = pytest.importorskip("numpy")
        type_name = "LowCardinality(Nullable(DateTime))"
        values = [None, np.datetime64("NaT"), np.datetime64(13, "s"), np.datetime64("NaT", "ns")]
        expected = build_native_block([("t", type_name, [None, None, 13, None])])
        assert self._encode(type_name, values) == expected
        assert self._encode(type_name, _NdarrayLikeColumn(values), len(values)) == expected
