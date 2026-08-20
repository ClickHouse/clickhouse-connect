from helpers import (
    _EPOCH_DATE,
    _EPOCH_NAIVE,
    ZoneInfo,
    _ch_core,
    _encode_plain_body,
    _NdarrayLikeColumn,
    build_native_block,
    build_native_block_from_bodies,
    dt,
    pytest,
    struct,
)


class TestTemporalInsertFastPath:
    def _encode(self, type_name, vals, n=None):
        n = len(vals) if n is None else n
        return _ch_core.encode_native_block(["t"], [type_name], [vals], n)

    @pytest.mark.parametrize("type_name", ["DateTime", "DateTime64(3)", "DateTime64(6)"])
    def test_datetime_inputs_match_generic_container(self, type_name):
        class SubDT(dt.datetime):
            pass

        vals = [
            dt.datetime(2024, 5, 4, 3, 2, 1),
            dt.datetime(2024, 1, 15, 12, 34, 56, 789000, tzinfo=dt.timezone.utc),
            SubDT(2024, 5, 4, 3, 2, 1, 123456),
            1700000000,
        ]
        fast = self._encode(type_name, vals)
        assert fast == self._encode(type_name, tuple(vals), len(vals))
        assert fast == self._encode(type_name, _NdarrayLikeColumn(vals), len(vals))

    def test_datetime64_string_fallback_matches_generic(self):
        vals = ["2024-01-15T12:34:56.789000+00:00", 5]
        fast = self._encode("DateTime64(3)", vals)
        assert fast == self._encode("DateTime64(3)", _NdarrayLikeColumn(vals), 2)

    def test_date_inputs_match_generic_and_helper(self):
        vals = [dt.date(2024, 1, 2), 19737, dt.date(1970, 1, 1)]
        fast = self._encode("Date", vals)
        assert fast == self._encode("Date", _NdarrayLikeColumn(vals), 3)
        expected_days = [dt.date(2024, 1, 2).toordinal() - 719163, 19737, 0]
        assert fast == build_native_block([("t", "Date", expected_days)])

    def test_nullable_datetime_with_none(self):
        vals = [dt.datetime(2024, 1, 15, 12, 0, 0, tzinfo=dt.timezone.utc), None, 5]
        fast = self._encode("Nullable(DateTime)", vals)
        assert fast == self._encode("Nullable(DateTime)", _NdarrayLikeColumn(vals), 3)

    def test_out_of_range_errors_unchanged(self):
        with pytest.raises(ValueError, match="outside UInt32 range"):
            self._encode("DateTime", [2**32])
        with pytest.raises(ValueError, match="outside UInt16 range"):
            self._encode("Date", [65536])
        with pytest.raises(ValueError, match="row 0 is None but DateTime is not Nullable"):
            self._encode("DateTime", [None])

    def test_negative_ints(self):
        # Negative is out of range for DateTime (falls back to the specific
        # range error) but a valid pre-epoch value for Date32.
        with pytest.raises(ValueError, match="outside UInt32 range"):
            self._encode("DateTime", [-1])
        fast = self._encode("Date32", [-100, 0, 100])
        assert fast == self._encode("Date32", _NdarrayLikeColumn([-100, 0, 100]), 3)
        decoded = list(_ch_core.ColBatch.decode_native(fast).column_data(0))
        assert decoded == [dt.date(1969, 9, 23), dt.date(1970, 1, 1), dt.date(1970, 4, 11)]


class TestTimeInsert:
    def _encode(self, type_name, vals, n=None):
        n = len(vals) if n is None else n
        return _ch_core.encode_native_block(["t"], [type_name], [vals], n)

    def _decode(self, encoded):
        return list(_ch_core.ColBatch.decode_native(encoded).column_data(0))

    def test_time_accepted_values_and_fast_raw_ints(self):
        values = [
            13,
            dt.timedelta(seconds=-1, microseconds=-500_000),
            dt.time(1, 2, 3, 999_999),
            "002:03:04.999",
            79.9,
        ]
        ticks = [13, -1, 3_723, 7_384, 79]
        encoded = self._encode("Time", values)
        assert encoded == build_native_block([("t", "Time", ticks)])
        assert encoded == self._encode("Time", tuple(values), len(values))
        assert encoded == self._encode(
            "Time", _NdarrayLikeColumn(values), len(values)
        )
        assert self._decode(encoded) == [dt.timedelta(seconds=v) for v in ticks]

    def test_established_inputs_skip_numpy_scalar_probe(self):
        probes = {"count": 0}

        def dtype_probe():
            probes["count"] += 1
            return "not-a-numpy-dtype"

        class StringValue(str):
            @property
            def dtype(self):
                return dtype_probe()

        class FloatValue(float):
            @property
            def dtype(self):
                return dtype_probe()

        class IntValue(int):
            @property
            def dtype(self):
                return dtype_probe()

        values = [
            dt.timedelta(seconds=1, microseconds=234_567),
            dt.time(0, 0, 1, 234_567),
            StringValue("000:00:01.234567"),
            FloatValue(79),
            IntValue(-13),
        ]
        expected_ticks = [1_234_567, 1_234_567, 1_234_567, 79, -13]
        direct = self._encode("Nullable(Time64(6))", [*values, None])
        generic = self._encode(
            "Nullable(Time64(6))", _NdarrayLikeColumn([*values, None]), 6
        )
        assert direct == generic == build_native_block(
            [
                (
                    "t",
                    "Nullable(Time64(6))",
                    [*expected_ticks, None],
                )
            ]
        )
        self._encode("Array(Time64(6))", [values])
        self._encode(
            "LowCardinality(Time)",
            [values[0], values[1], values[2], values[3], values[4]],
        )
        assert probes["count"] == 0

    def test_delta_and_time_subclasses_probe_then_fall_back(self):
        # Subclasses go through the numpy scalar probe (pd.Timedelta needs it);
        # a non-dtype attribute falls back to the struct-field conversion.
        class DeltaValue(dt.timedelta):
            @property
            def dtype(self):
                return "not-a-numpy-dtype"

        class TimeValue(dt.time):
            @property
            def dtype(self):
                return "not-a-numpy-dtype"

        values = [
            DeltaValue(seconds=1, microseconds=234_567),
            TimeValue(0, 0, 1, 234_567),
        ]
        encoded = self._encode("Nullable(Time64(6))", values)
        assert encoded == build_native_block(
            [("t", "Nullable(Time64(6))", [1_234_567, 1_234_567])]
        )

    @pytest.mark.parametrize(
        "precision,values,ticks",
        [
            (
                0,
                [
                    dt.timedelta(seconds=13, microseconds=999_999),
                    dt.time(1, 2, 3, 999_999),
                    "-002:03:04.9",
                ],
                [13, 3_723, -7_384],
            ),
            (
                3,
                [
                    dt.timedelta(seconds=1, microseconds=234_567),
                    dt.time(1, 2, 3, 456_789),
                    "-002:03:04.56789",
                ],
                [1_234, 3_723_456, -7_384_567],
            ),
            (
                6,
                [
                    dt.timedelta(microseconds=79),
                    dt.time(0, 0, 1, 234_567),
                    "000:00:01.2",
                ],
                [79, 1_234_567, 1_200_000],
            ),
            (
                9,
                [
                    dt.timedelta(microseconds=79),
                    dt.time(0, 0, 1, 234_567),
                    "000:00:01.000000079",
                ],
                [79_000, 1_234_567_000, 1_000_000_079],
            ),
        ],
    )
    def test_time64_precisions(self, precision, values, ticks):
        type_name = f"Time64({precision})"
        encoded = self._encode(type_name, values)
        assert encoded == build_native_block([("t", type_name, ticks)])
        assert self._decode(encoded) == [
            dt.timedelta(
                microseconds=(abs(v) * 1_000_000 // (10**precision))
                * (-1 if v < 0 else 1)
            )
            for v in ticks
        ]

    @pytest.mark.parametrize(
        "type_name,value",
        [
            ("Time64(0)", dt.timedelta(milliseconds=-999)),
            ("Time64(3)", dt.timedelta(microseconds=-999)),
        ],
    )
    def test_negative_timedelta_sub_tick_truncates_toward_zero(
        self, type_name, value
    ):
        encoded = self._encode(type_name, [value])
        assert encoded == build_native_block([("t", type_name, [0])])

    @pytest.mark.parametrize(
        "type_name,value",
        [
            ("Time64(0)", ("timedelta64[ms]", -999)),
            ("Time64(3)", ("timedelta64[us]", -999)),
            ("Time64(6)", ("timedelta64[ns]", -999)),
            ("Time64(9)", ("timedelta64[ps]", -999)),
        ],
    )
    def test_numpy_scalar_negative_sub_tick_truncates_toward_zero(
        self, type_name, value
    ):
        np = pytest.importorskip("numpy")
        dtype, raw = value
        scalar = np.array(raw, dtype=dtype)[()]
        encoded = self._encode(type_name, [scalar])
        assert encoded == build_native_block([("t", type_name, [0])])

    @pytest.mark.parametrize(
        "type_name,expected_ticks",
        [
            ("Time", [1, -1, 0]),
            ("Time64(3)", [1_234, -1_234, 0]),
            ("Time64(6)", [1_234_567, -1_234_567, 0]),
            ("Time64(9)", [1_234_567_890, -1_234_567_890, 0]),
        ],
    )
    def test_numpy_timedelta64_ndarray_bulk_path(self, type_name, expected_ticks):
        np = pytest.importorskip("numpy")
        values = np.array(
            [1_234_567_890, -1_234_567_890, 0], dtype="timedelta64[ns]"
        )
        encoded = self._encode(type_name, values)
        assert encoded == build_native_block([("t", type_name, expected_ticks)])

    def test_numpy_timedelta64_two_dimensional_array_rejected(self):
        np = pytest.importorskip("numpy")
        values = np.array([13, 79], dtype="timedelta64[ns]").reshape(2, 1)
        with pytest.raises(ValueError, match="must be one-dimensional"):
            self._encode("Time64(9)", values)

    def test_numpy_timedelta64_byte_order_multiplier_and_general_ratios(self):
        np = pytest.importorskip("numpy")

        big_endian = np.array([1_234, -1_234], dtype=">m8[ms]")
        assert self._encode("Time64(3)", big_endian) == build_native_block(
            [("t", "Time64(3)", [1_234, -1_234])]
        )

        multiplied = np.array([13, -13], dtype="timedelta64[10us]")
        assert self._encode("Time64(9)", multiplied) == build_native_block(
            [("t", "Time64(9)", [130_000, -130_000])]
        )

        general = np.array([334, -334], dtype="timedelta64[3ps]")
        assert self._encode("Time64(9)", general) == build_native_block(
            [("t", "Time64(9)", [1, -1])]
        )

    def test_numpy_timedelta64_nullable_nat_array_and_scalars(self):
        np = pytest.importorskip("numpy")
        values = np.array([1_234_567, "NaT", -1_234_567], dtype="timedelta64[us]")
        encoded = self._encode("Nullable(Time64(6))", values)
        assert encoded == build_native_block(
            [("t", "Nullable(Time64(6))", [1_234_567, None, -1_234_567])]
        )
        assert self._decode(encoded) == [
            dt.timedelta(microseconds=1_234_567),
            None,
            dt.timedelta(microseconds=-1_234_567),
        ]

        scalar_values = [np.timedelta64(13, "ms"), np.timedelta64("NaT")]
        scalar_encoded = self._encode("Nullable(Time64(3))", scalar_values)
        assert scalar_encoded == build_native_block(
            [("t", "Nullable(Time64(3))", [13, None])]
        )

    def test_pandas_timedelta_series_bulk_path(self):
        pd = pytest.importorskip("pandas")
        values = pd.Series(
            pd.to_timedelta(["1.234567890s", None, "-1.234567890s"])
        )
        encoded = self._encode("Nullable(Time64(9))", values)
        assert encoded == build_native_block(
            [
                (
                    "t",
                    "Nullable(Time64(9))",
                    [1_234_567_890, None, -1_234_567_890],
                )
            ]
        )

    def test_pandas_timedelta_scalar_ns_precision_in_list(self):
        pd = pytest.importorskip("pandas")
        values = [pd.Timedelta("1s 123456789ns"), pd.Timedelta("-1s")]
        encoded = self._encode("Time64(9)", values)
        assert encoded == build_native_block(
            [("t", "Time64(9)", [1_123_456_789, -1_000_000_000])]
        )
        sub_tick = self._encode("Time64(0)", [pd.Timedelta("-999ms")])
        assert sub_tick == build_native_block([("t", "Time64(0)", [0])])

    def test_pandas_nat_nullable_and_non_nullable(self):
        pd = pytest.importorskip("pandas")
        encoded = self._encode("Nullable(Time64(9))", [pd.Timedelta("1us"), pd.NaT])
        assert encoded == build_native_block(
            [("t", "Nullable(Time64(9))", [1_000, None])]
        )
        with pytest.raises(ValueError, match="row 0 is NaT but Time is not Nullable"):
            self._encode("Time", [pd.NaT])
        with pytest.raises(ValueError, match="row 1 is NaT"):
            self._encode("Time64(3)", _NdarrayLikeColumn([13, pd.NaT]), 2)

    def test_numpy_nat_non_nullable_and_range_errors(self):
        np = pytest.importorskip("numpy")
        with pytest.raises(ValueError, match="row 0 is NaT.*not Nullable"):
            self._encode("Time", np.array(["NaT"], dtype="timedelta64[ns]"))
        with pytest.raises(ValueError, match="outside logical range"):
            self._encode("Time", np.array([1_000], dtype="timedelta64[h]"))

    def test_late_day_time64_nested_value_avoids_i64_overflow(self):
        value = dt.time(23, 59, 59, 999_999)
        type_name = "Array(Tuple(Time64(9)))"
        encoded = self._encode(type_name, [[(value,)]])
        expected_ticks = 86_399_999_999_000
        assert encoded == build_native_block(
            [("t", type_name, [[(expected_ticks,)]])]
        )
        assert self._decode(encoded) == [
            [(dt.timedelta(seconds=86_399, microseconds=999_999),)]
        ]

    def test_nullable_and_recursive_shapes(self):
        nullable = [
            dt.timedelta(seconds=1, microseconds=250_000),
            None,
            "-000:00:00.001",
        ]
        encoded = self._encode("Nullable(Time64(3))", nullable)
        assert self._decode(encoded) == [
            dt.timedelta(milliseconds=1_250),
            None,
            dt.timedelta(milliseconds=-1),
        ]

        array_rows = [[dt.timedelta(milliseconds=13), "000:00:00.079"], [], [-1]]
        encoded = self._encode("Array(Time64(3))", array_rows)
        assert self._decode(encoded) == [
            [dt.timedelta(milliseconds=13), dt.timedelta(milliseconds=79)],
            [],
            [dt.timedelta(milliseconds=-1)],
        ]

        tuple_rows = [
            (dt.time(0, 0, 13), dt.timedelta(microseconds=79)),
            (-13, None),
        ]
        encoded = self._encode("Tuple(Time, Nullable(Time64(6)))", tuple_rows)
        assert self._decode(encoded) == [
            (dt.timedelta(seconds=13), dt.timedelta(microseconds=79)),
            (dt.timedelta(seconds=-13), None),
        ]

        array_tuple_rows = [
            [("000:00:13", 79_000)],
            [],
            [(dt.time(0, 1, 19), -1_000)],
        ]
        encoded = self._encode(
            "Array(Tuple(Time, Time64(6)))", array_tuple_rows
        )
        assert self._decode(encoded) == [
            [(dt.timedelta(seconds=13), dt.timedelta(microseconds=79_000))],
            [],
            [(dt.timedelta(seconds=79), dt.timedelta(microseconds=-1_000))],
        ]

    def test_low_cardinality_time_and_time64_rejection(self):
        encoded = self._encode("LowCardinality(Time)", [13, 79, 13, -1])
        assert self._decode(encoded) == [
            dt.timedelta(seconds=13),
            dt.timedelta(seconds=79),
            dt.timedelta(seconds=13),
            dt.timedelta(seconds=-1),
        ]
        with pytest.raises(
            NotImplementedError, match="unsupported LowCardinality inner type"
        ):
            self._encode("LowCardinality(Time64(3))", [13])

    @pytest.mark.parametrize(
        "type_name,value",
        [
            ("Time", 3_600_000),
            ("Time", -3_600_000),
            ("Time64(3)", 3_600_000_000),
            ("Time64(3)", -3_600_000_000),
            ("Time", "1000:00:00"),
            ("Time64(6)", "001:60:00"),
            ("Time64(6)", float("inf")),
        ],
    )
    def test_invalid_values(self, type_name, value):
        with pytest.raises(ValueError, match="column.*row 0.*Time"):
            self._encode(type_name, [value])

    def test_none_requires_nullable(self):
        with pytest.raises(ValueError, match="row 0 is None but Time is not Nullable"):
            self._encode("Time", [None])


# ---------------------------------------------------------------------------
# Temporal types
# ---------------------------------------------------------------------------


class TestDecodeDate:
    def test_date(self):
        days = [0, 19737, 100, 65535]
        data = build_native_block([("d", "Date", days)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["Date"]
        expected = [_EPOCH_DATE + dt.timedelta(days=x) for x in days]
        assert list(batch.column_data(0)) == expected

    def test_date32_pre_epoch(self):
        days = [-25567, -1, 0, 19737]  # -25567 ~= 1900-01-01, signed days
        data = build_native_block([("d", "Date32", days)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["Date32"]
        expected = [_EPOCH_DATE + dt.timedelta(days=x) for x in days]
        assert list(batch.column_data(0)) == expected


class TestDecodeDateTime:
    def test_datetime_naive(self):
        secs = [0, 1705322096, 961056000]
        data = build_native_block([("dt", "DateTime", secs)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["DateTime"]
        expected = [_EPOCH_NAIVE + dt.timedelta(seconds=s) for s in secs]
        result = list(batch.column_data(0))
        assert result == expected
        assert all(v.tzinfo is None for v in result)

    def test_datetime_utc_is_naive(self):
        # A UTC-equivalent timezone renders naive, matching clickhouse-connect.
        secs = [1705322096]
        data = build_native_block([("dt", "DateTime('UTC')", secs)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["DateTime('UTC')"]
        v = list(batch.column_data(0))[0]
        assert v == _EPOCH_NAIVE + dt.timedelta(seconds=secs[0])
        assert v.tzinfo is None

    def test_datetime_named_zone_is_aware(self):
        secs = [1705322096]  # 2024-01-15 12:34:56 UTC
        data = build_native_block([("dt", "DateTime('America/New_York')", secs)])
        batch = _ch_core.ColBatch.decode_native(data)
        v = list(batch.column_data(0))[0]
        assert v.tzinfo == ZoneInfo("America/New_York")
        # Same instant as the source UTC seconds, expressed in New York time.
        assert v == dt.datetime(2024, 1, 15, 12, 34, 56, tzinfo=dt.timezone.utc)
        assert (v.hour, v.minute, v.second) == (7, 34, 56)


class TestDateTimeTzSubSecond:
    _NY = ZoneInfo("America/New_York")

    def _decode_one(self, type_name, tick):
        data = build_native_block([("ts", type_name, [tick])])
        return list(_ch_core.ColBatch.decode_native(data).column_data(0))[0]

    def test_dst_fall_back_fold(self):
        # America/New_York 2020-11-01: 1:00:00.5 wall time exists twice. The
        # epoch values pin each side of the fold; sub-second micros must not
        # disturb the UTC offset.
        for epoch_secs, offset_hours in ((1604206800, -4), (1604210400, -5)):
            v = self._decode_one(
                "DateTime64(6, 'America/New_York')", epoch_secs * 1_000_000 + 500_000
            )
            assert v.microsecond == 500_000
            assert v.utcoffset() == dt.timedelta(hours=offset_hours)
            assert (v.hour, v.minute, v.second) == (1, 0, 0)

    def test_microsecond_exactness(self):
        # Sweep awkward microsecond values at a recent epoch and assert exact
        # round-trips through the tz-aware path.
        for secs in (1705322096, -1, -86_400):
            for micros in (1, 3, 333_333, 499_999, 500_000, 500_001, 999_999):
                v = self._decode_one(
                    "DateTime64(6, 'America/New_York')", secs * 1_000_000 + micros
                )
                expected = dt.datetime.fromtimestamp(secs, self._NY).replace(
                    microsecond=micros
                )
                assert v == expected
                assert v.microsecond == micros

    def test_far_future_exactness(self):
        # Year 2200 is beyond f64 sub-microsecond precision for epoch seconds;
        # the exact path must still produce the precise microsecond.
        secs = 7_258_204_800
        micros = 123_457
        v = self._decode_one("DateTime64(6, 'America/New_York')", secs * 1_000_000 + micros)
        expected = dt.datetime.fromtimestamp(secs, self._NY).replace(microsecond=micros)
        assert v == expected
        assert v.microsecond == micros


class TestDecodeDateTime64:
    def test_dt64_millis(self):
        ticks = [0, 1705322096789]  # milliseconds
        data = build_native_block([("ts", "DateTime64(3)", ticks)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["DateTime64(3)"]
        expected = [_EPOCH_NAIVE + dt.timedelta(milliseconds=t) for t in ticks]
        assert list(batch.column_data(0)) == expected

    def test_dt64_nanos_truncate_to_micros(self):
        # Precision 9 (ns); Python datetime resolves to microseconds, so the
        # sub-microsecond digits are truncated.
        ticks = [1705322096789012345]
        data = build_native_block([("ts", "DateTime64(9)", ticks)])
        batch = _ch_core.ColBatch.decode_native(data)
        v = list(batch.column_data(0))[0]
        assert v == dt.datetime(2024, 1, 15, 12, 34, 56, 789012)

    def test_dt64_nullable(self):
        ticks = [1705322096789, None, 0]
        data = build_native_block([("ts", "Nullable(DateTime64(3))", ticks)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["Nullable(DateTime64(3))"]
        result = list(batch.column_data(0))
        assert result[1] is None
        assert result[0] == _EPOCH_NAIVE + dt.timedelta(milliseconds=ticks[0])

    def test_temporal_rows_and_columns(self):
        days = [0, 19737]
        ticks = [0, 1705322096789]
        data = build_native_block([("d", "Date", days), ("ts", "DateTime64(3)", ticks)])
        batch = _ch_core.ColBatch.decode_native(data)
        rows = list(batch.to_python_rows())
        assert rows[1] == (
            _EPOCH_DATE + dt.timedelta(days=days[1]),
            _EPOCH_NAIVE + dt.timedelta(milliseconds=ticks[1]),
        )
        cols = list(batch.to_python_columns())
        assert list(cols[0])[0] == _EPOCH_DATE


class TestDecodeTime:
    @pytest.mark.parametrize(
        "type_name,ticks,expected",
        [
            (
                "Time",
                [-3_599_999, -13, 0, 3_599_999],
                [
                    dt.timedelta(seconds=v)
                    for v in [-3_599_999, -13, 0, 3_599_999]
                ],
            ),
            (
                "Time64(0)",
                [-13, 0, 79],
                [dt.timedelta(seconds=v) for v in [-13, 0, 79]],
            ),
            (
                "Time64(3)",
                [-1_500, -1, 0, 79_999],
                [
                    dt.timedelta(milliseconds=-1_500),
                    dt.timedelta(milliseconds=-1),
                    dt.timedelta(0),
                    dt.timedelta(milliseconds=79_999),
                ],
            ),
            (
                "Time64(6)",
                [-1_500_001, -1, 0, 79_999_999],
                [
                    dt.timedelta(microseconds=-1_500_001),
                    dt.timedelta(microseconds=-1),
                    dt.timedelta(0),
                    dt.timedelta(microseconds=79_999_999),
                ],
            ),
            (
                "Time64(9)",
                [-1_999, -1_001, -999, 1_999],
                [
                    dt.timedelta(microseconds=-1),
                    dt.timedelta(microseconds=-1),
                    dt.timedelta(0),
                    dt.timedelta(microseconds=1),
                ],
            ),
        ],
    )
    def test_plain_precisions_and_truncation(self, type_name, ticks, expected):
        batch = _ch_core.ColBatch.decode_native(
            build_native_block([("t", type_name, ticks)])
        )
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == expected

    def test_nullable_and_all_object_exits(self):
        ticks = [-1_001, None, 1_999]
        expected = [
            dt.timedelta(microseconds=-1),
            None,
            dt.timedelta(microseconds=1),
        ]
        batch = _ch_core.ColBatch.decode_native(
            build_native_block([("t", "Nullable(Time64(9))", ticks)])
        )
        assert batch.column_type_names == ["Nullable(Time64(9))"]
        assert list(batch.column_data(0)) == expected
        assert list(batch.to_python_columns()[0]) == expected
        assert [row[0] for row in batch.to_python_rows()] == expected

    def test_raw_time_ticks_scalar_nullable_and_low_cardinality(self):
        data = build_native_block(
            [
                ("t", "Time", [-13, 0, 79]),
                ("t64", "Nullable(Time64(9))", [-1_001, None, 1_999]),
                ("lc", "LowCardinality(Time)", [13, 79, 13]),
            ]
        )
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0, raw_time_ticks=True)) == [-13, 0, 79]
        assert list(batch.column_data(1, raw_time_ticks=True)) == [
            -1_001,
            None,
            1_999,
        ]
        assert list(batch.column_data(2, raw_time_ticks=True)) == [13, 79, 13]
        assert list(batch.column_data(0)) == [
            dt.timedelta(seconds=-13),
            dt.timedelta(0),
            dt.timedelta(seconds=79),
        ]

    def test_raw_time_ticks_recursive_array_tuple_and_low_cardinality(self):
        type_name = (
            "Array(Tuple(Time, Nullable(Time64(9)), "
            "Array(LowCardinality(Time))))"
        )
        rows = [
            [(13, -1_001, [13, 13, 79]), (-79, None, [])],
            [],
            [(0, 1_999, [-1])],
        ]
        batch = _ch_core.ColBatch.decode_native(
            build_native_block([("v", type_name, rows)])
        )
        assert list(batch.column_data(0, True)) == rows
        assert list(batch.column_data(0)) == [
            [
                (
                    dt.timedelta(seconds=13),
                    dt.timedelta(microseconds=-1),
                    [dt.timedelta(seconds=13)] * 2 + [dt.timedelta(seconds=79)],
                ),
                (dt.timedelta(seconds=-79), None, []),
            ],
            [],
            [
                (
                    dt.timedelta(0),
                    dt.timedelta(microseconds=1),
                    [dt.timedelta(seconds=-1)],
                )
            ],
        ]

    def test_raw_time_ticks_map_duplicate_key_last_value_wins(self):
        type_name = "Map(Time, String)"
        body = bytearray(struct.pack("<Q", 3))
        body.extend(struct.pack("<iii", 13, 13, 79))
        body.extend(_encode_plain_body("String", ["first", "second", "third"]))
        data = build_native_block_from_bodies([("m", type_name, bytes(body))], 1)
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0, raw_time_ticks=True)) == [
            {13: "second", 79: "third"}
        ]
        assert list(batch.column_data(0)) == [
            {
                dt.timedelta(seconds=13): "second",
                dt.timedelta(seconds=79): "third",
            }
        ]

    @pytest.mark.parametrize("type_name", ["time", "time64(3)", "Time64"])
    def test_noncanonical_direct_headers_rejected(self, type_name):
        payload = build_native_block([("t", type_name, [])])
        with pytest.raises(NotImplementedError, match="Unsupported ClickHouse type"):
            _ch_core.ColBatch.decode_native(payload)


class TestArrowTemporal:
    def test_arrow_temporal_types(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([
            ("d", "Date", [0, 19737]),
            ("d32", "Date32", [-25567, 19737]),
            ("dt", "DateTime", [0, 1705322096]),
            ("ts", "DateTime64(3)", [0, 1705322096789]),
        ])
        batch = _ch_core.ColBatch.decode_native(data)
        result = pa.RecordBatchReader.from_stream(batch).read_all()
        # Zero-copy export keeps native widths: Date is raw uint16 days,
        # DateTime is raw uint32 seconds; Date32 and DateTime64(3) map to real
        # Arrow temporal types.
        assert result.schema.field("d").type == pa.uint16()
        assert result.schema.field("d32").type == pa.date32()
        assert result.schema.field("dt").type == pa.uint32()
        assert result.schema.field("ts").type == pa.timestamp("ms")
        assert result.column("d32").to_pylist() == [
            _EPOCH_DATE + dt.timedelta(days=-25567),
            _EPOCH_DATE + dt.timedelta(days=19737),
        ]
