from helpers import (
    ZoneInfo,
    _ch_core,
    _NdarrayLikeColumn,
    build_native_block,
    dt,
    pytest,
)


class TestLowCardinalityDictCache:
    """LC read exits materialize each dictionary value once and reuse it."""

    def _batch(self, type_name, vals):
        encoded = _ch_core.encode_native_block(["lc"], [type_name], [vals], len(vals))
        return _ch_core.ColBatch.decode_native(encoded)

    def test_column_data_reuses_dictionary_objects(self):
        col = self._batch("LowCardinality(String)", ["a", "b", "a", "b", "a"]).column_data(0)
        assert list(col) == ["a", "b", "a", "b", "a"]
        assert col[0] is col[2] and col[2] is col[4]
        assert col[1] is col[3]

    def test_to_python_columns_and_rows_reuse(self):
        batch = self._batch("LowCardinality(String)", ["a", "b", "a", "b", "a"])
        col = batch.to_python_columns()[0]
        assert col[0] is col[2]
        rows = batch.to_python_rows()
        assert [r[0] for r in rows] == ["a", "b", "a", "b", "a"]
        assert rows[0][0] is rows[2][0]

    def test_nullable_lc_read_values_and_reuse(self):
        vals = ["x", None, "y", "x", None]
        col = self._batch("LowCardinality(Nullable(String))", vals).column_data(0)
        assert list(col) == vals
        assert col[0] is col[3]

    def test_lc_non_string_inner(self):
        vals = [7, 9, 7, 9, 7]
        col = self._batch("LowCardinality(Int64)", vals).column_data(0)
        assert list(col) == vals

    def test_all_null_lc_never_materializes_dictionary(self):
        # Null rows never reference the dictionary, so its slots stay
        # unmaterialized on every exit.
        batch = self._batch("LowCardinality(Nullable(UUID))", [None, None])
        assert list(batch.column_data(0)) == [None, None]
        assert list(batch.to_python_columns()[0]) == [None, None]
        assert [r[0] for r in batch.to_python_rows()] == [None, None]


class TestLowCardinalityInsertFastPath:
    def _encode(self, type_name, vals, n=None):
        n = len(vals) if n is None else n
        return _ch_core.encode_native_block(["lc"], [type_name], [vals], n)

    def test_containers_agree_and_round_trip(self):
        uniq = [f"tag_{i}" for i in range(5)]
        vals = [uniq[i % 5] for i in range(20)] + ["solo"]
        # Runtime-built distinct objects with equal content must dedupe by content.
        vals += ["tag_%d" % (i % 3) for i in range(6)]
        from_list = self._encode("LowCardinality(String)", vals)
        assert from_list == self._encode("LowCardinality(String)", tuple(vals), len(vals))
        assert from_list == self._encode("LowCardinality(String)", _NdarrayLikeColumn(vals), len(vals))
        assert list(_ch_core.ColBatch.decode_native(from_list).column_data(0)) == vals

    def test_nullable_none_and_empty_string(self):
        vals = ["x", None, "", "x", None, ""]
        fast = self._encode("LowCardinality(Nullable(String))", vals)
        generic = self._encode("LowCardinality(Nullable(String))", _NdarrayLikeColumn(vals), len(vals))
        assert fast == generic
        batch = _ch_core.ColBatch.decode_native(fast)
        assert list(batch.column_data(0)) == vals

    def test_bytes_values_use_fallback(self):
        vals = [b"\xff", "x", b"\xff", "x"]
        fast = self._encode("LowCardinality(String)", vals)
        generic = self._encode("LowCardinality(String)", _NdarrayLikeColumn(vals), len(vals))
        assert fast == generic

    def test_none_in_non_nullable_lc_raises(self):
        with pytest.raises(ValueError, match="row 1 is None but LowCardinality\\(String\\) is not nullable"):
            self._encode("LowCardinality(String)", ["a", None])

    def test_bad_value_reports_row(self):
        with pytest.raises(ValueError, match="row 2 cannot be converted to String"):
            self._encode("LowCardinality(String)", ["a", "b", 13])

    @pytest.mark.parametrize("size", [4, 8, 16, 32, 64])
    def test_item_replacement_during_fallback_invalidates_ptr_cache(self, size):
        # A fallback __buffer__ drops the last ref to an already-scanned str;
        # the allocator can hand its address to a new same-size str, which
        # must not false-hit the pointer-identity cache.
        vals = ["A" * size, None, "C" * size]

        class EvilBuf:
            def __buffer__(self, flags):
                vals[0] = "x"  # drop the sole ref to the scanned "A"*size
                vals[2] = "B" * size  # same size class, may reuse its address
                return memoryview(b"EV")

            def __release_buffer__(self, view):
                pass

        vals[1] = EvilBuf()
        encoded = self._encode("LowCardinality(String)", vals, 3)
        decoded = list(_ch_core.ColBatch.decode_native(encoded).column_data(0))
        assert decoded == ["A" * size, "EV", "B" * size]

    def test_distinct_values_beyond_ptr_cache_cap(self):
        vals = [f"v_{i}" for i in range(70_000)]
        encoded = self._encode("LowCardinality(String)", vals)
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == vals

    def test_str_subclass_uses_fallback(self):
        class S(str):
            pass

        vals = [S("a"), "b", S("a"), "b"]
        fast = self._encode("LowCardinality(String)", vals)
        generic = self._encode("LowCardinality(String)", _NdarrayLikeColumn(vals), len(vals))
        assert fast == generic

    def test_lone_surrogate_raises_like_generic(self):
        vals = ["ok", "\ud800"]
        with pytest.raises(UnicodeEncodeError):
            self._encode("LowCardinality(String)", vals)
        with pytest.raises(UnicodeEncodeError):
            self._encode("LowCardinality(String)", _NdarrayLikeColumn(vals), len(vals))


# ---------------------------------------------------------------------------
# LowCardinality
# ---------------------------------------------------------------------------


class TestDecodeLowCardinality:
    def test_string_basic(self):
        vals = ["red", "green", "red", "blue", "green", "red"]
        data = build_native_block([("c", "LowCardinality(String)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["LowCardinality(String)"]
        assert list(batch.column_data(0)) == vals

    def test_nullable_string(self):
        vals = ["x", None, "y", "x", None, "y"]
        data = build_native_block([("c", "LowCardinality(Nullable(String))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["LowCardinality(Nullable(String))"]
        assert list(batch.column_data(0)) == vals

    def test_empty_string_distinct_from_null(self):
        # A real empty string is its own dictionary entry, not the null sentinel.
        vals = ["", None, "", "a"]
        data = build_native_block([("c", "LowCardinality(Nullable(String))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals

    def test_uint32_inner(self):
        # A non-String inner type exercises the dictionary -> primitive recursion.
        vals = [100, 200, 100, 4_000_000_000, 200]
        data = build_native_block([("c", "LowCardinality(UInt32)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals

    def test_datetime_named_zone(self):
        # LowCardinality(DateTime(tz)) must still apply the timezone policy, which
        # means prepare_temporal has to see through the LowCardinality wrapper.
        secs = 1705322096  # 2024-01-15 12:34:56 UTC
        data = build_native_block(
            [("c", "LowCardinality(DateTime('America/New_York'))", [secs, secs])]
        )
        batch = _ch_core.ColBatch.decode_native(data)
        v = list(batch.column_data(0))[0]
        assert v.tzinfo == ZoneInfo("America/New_York")
        assert v == dt.datetime(2024, 1, 15, 12, 34, 56, tzinfo=dt.timezone.utc)

    def test_invalid_utf8_hex_fallback(self):
        vals = ["ok", b"\xff\xfe", "ok"]
        data = build_native_block([("c", "LowCardinality(String)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == ["ok", "fffe", "ok"]

    def test_paths_agree(self):
        vals = ["a", "a", "b", "c", "b", "a", None]
        data = build_native_block([("c", "LowCardinality(Nullable(String))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [row[0] for row in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == vals

    def test_arrow_dictionary(self):
        pa = pytest.importorskip("pyarrow")
        vals = ["red", "green", "red", "blue"]
        data = build_native_block([("c", "LowCardinality(String)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        result = pa.RecordBatchReader.from_stream(batch).read_all()
        assert pa.types.is_dictionary(result.schema.field("c").type)
        assert result.column("c").to_pylist() == vals

    def test_arrow_nullable_dictionary(self):
        pa = pytest.importorskip("pyarrow")
        vals = ["x", None, "y", "x", None]
        data = build_native_block([("c", "LowCardinality(Nullable(String))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        col = pa.RecordBatchReader.from_stream(batch).read_all().column("c")
        assert pa.types.is_dictionary(col.type)
        assert col.to_pylist() == vals
        assert col.null_count == 2

    def test_across_chunks(self):
        # Each Native block carries its own dictionary; the two chunks must
        # concatenate correctly even with different per-block dictionaries.
        first = build_native_block([("c", "LowCardinality(String)", ["a", "b", "a"])])
        second = build_native_block([("c", "LowCardinality(String)", ["c", "c", "d"])])
        batch = _ch_core.ColBatch.decode_native(first + second)
        assert batch.num_chunks == 2
        assert list(batch.column_data(0)) == ["a", "b", "a", "c", "c", "d"]

    def test_with_block_info(self):
        # The shape clickhouse-connect actually receives: client_protocol_version
        # 54405 emits a BlockInfo preamble and, being below 54454, no per-column
        # custom-serialization marker. The LowCardinality state prefix follows.
        vals = ["red", "green", "red", None, "green"]
        data = build_native_block(
            [("c", "LowCardinality(Nullable(String))", vals)], block_info=True
        )
        batch = _ch_core.ColBatch.decode_native(data, has_block_info=True)
        assert list(batch.column_data(0)) == vals
