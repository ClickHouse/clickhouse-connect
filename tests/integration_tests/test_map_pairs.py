import pytest

from clickhouse_connect.datatypes.format import clear_read_format, set_read_format
from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.options import np, pd

_MAP = "map('k', 13, 'k', 79)"
_PAIRS = [("k", 13), ("k", 79)]
_PATTERNS = ["Map", "map", "MAP", "Ma*"]


@pytest.mark.parametrize(
    "expression,expected",
    [
        (_MAP, _PAIRS),
        ("CAST(map(), 'Map(String, UInt8)')", []),
        (f"[{_MAP}]", [_PAIRS]),
        (f"tuple({_MAP}, 13)", (_PAIRS, 13)),
        (f"[tuple({_MAP}, 13)]", [(_PAIRS, 13)]),
        (f"CAST(tuple({_MAP}), 'Tuple(value Map(String, UInt8))')", {"value": _PAIRS}),
        (f"map('outer', {_MAP}, 'outer', {_MAP})", [("outer", _PAIRS), ("outer", _PAIRS)]),
        ("map('k', CAST(NULL, 'Nullable(UInt8)'), 'k', 79)", [("k", None), ("k", 79)]),
        (f"CAST({_MAP}, 'Map(LowCardinality(String), UInt8)')", _PAIRS),
        ("map(13, 'user_1', 13, 'user_2')", [(13, "user_1"), (13, "user_2")]),
        ("map('k', tuple(13, 'user_1'), 'k', tuple(79, 'user_2'))", [("k", (13, "user_1")), ("k", (79, "user_2"))]),
        ("map('k', [13], 'k', [79, 13])", [("k", [13]), ("k", [79, 13])]),
        (f"CAST({_MAP}, 'Dynamic')", _PAIRS),
        (f"CAST({_MAP}, 'Variant(Map(String, UInt8), String)')", _PAIRS),
        (f"CAST({_MAP}, 'SimpleAggregateFunction(anyLast, Map(String, UInt8))')", _PAIRS),
        (f"CAST([{_MAP}], 'SimpleAggregateFunction(anyLast, Array(Map(String, UInt8)))')", [_PAIRS]),
        ("""[CAST('{"m":{"k":13,"k":79}}', 'JSON(m Map(String, UInt8))')]""", [{"m": _PAIRS}]),
    ],
)
@pytest.mark.parametrize("method", ["query", "query_np", "query_df"])
def test_map_pairs_shapes(param_client, call, expression, expected, method):
    if method == "query_np" and np is None or method == "query_df" and pd is None:
        pytest.skip("Optional dataframe dependency not installed")
    result = call(getattr(param_client, method), f"SELECT {expression} AS m FROM numbers(3)", query_formats={"Map": "pairs"})
    if method == "query":
        assert result.result_rows == [(expected,)] * 3
    elif method == "query_np":
        assert result.shape == (3, 1)
        assert result.dtype == np.dtype("O")
        assert result[:, 0].tolist() == [expected] * 3
    else:
        assert result.shape == (3, 1)
        if "Nullable" in expression:
            expected = [("k", pd.NA), ("k", 79)]
        assert result["m"].tolist() == [expected] * 3


@pytest.mark.parametrize(
    "scope,pattern",
    [("column", "Map")] + [(scope, pattern) for scope in ("query", "column_type", "global") for pattern in _PATTERNS],
)
def test_map_pairs_format_scopes(param_client, call, scope, pattern):
    kwargs = {}
    if scope == "query":
        kwargs["query_formats"] = {pattern: "pairs"}
    elif scope == "column":
        kwargs["column_formats"] = {"m": "pairs"}
    elif scope == "column_type":
        kwargs["column_formats"] = {"m": {pattern: "pairs"}}
    else:
        set_read_format(pattern, "pairs")
    try:
        assert call(param_client.query, f"SELECT [{_MAP}] AS m", **kwargs).first_row == ([_PAIRS],)
    finally:
        clear_read_format(pattern)


def test_map_pairs_format_precedence(param_client, call):
    set_read_format("Map", "pairs")
    try:
        query = f"SELECT {_MAP} AS m, {_MAP} AS n"
        result = call(param_client.query, query, query_formats={"Map": "dict"}, column_formats={"m": {"Map": "pairs"}})
        assert result.first_row == (_PAIRS, {"k": 79})
        result = call(param_client.query, query, query_formats={"Map": "pairs"}, column_formats={"m": "native"})
        assert result.first_row == ({"k": 79}, _PAIRS)
    finally:
        clear_read_format("Map")
    # The existing tuple format applies to named tuples inside a Map, without changing the Map to pairs.
    expression = "map('k', CAST(tuple(13, 'user_1'), 'Tuple(n UInt8, s String)'))"
    assert call(param_client.query, f"SELECT {expression} AS m", column_formats={"m": "tuple"}).first_row == ({"k": (13, "user_1")},)


@pytest.mark.parametrize("fmt", [None, "native", "dict", "unknown"])
def test_map_default_and_insert_unchanged(param_client, call, table_context, fmt):
    formats = {"Map": fmt} if fmt else None
    assert call(param_client.query, f"SELECT {_MAP}", query_formats=formats).first_row == ({"k": 79},)
    with table_context("map_dict_insert", ["m Map(String, UInt8)"]):
        set_read_format("Map", "pairs")
        try:
            call(param_client.insert, "map_dict_insert", [[{"k": 13}]])
            assert call(param_client.query, "SELECT m FROM map_dict_insert").first_row == ([("k", 13)],)
        finally:
            clear_read_format("Map")


def test_map_pairs_insert_error(param_client, call, table_context):
    pairs = call(param_client.query, f"SELECT {_MAP}", query_formats={"Map": "pairs"}).first_row[0]
    with table_context("map_pairs_insert", ["m Map(String, UInt8)"]):
        with pytest.raises(DataError, match="must be dictionaries"):
            call(param_client.insert, "map_pairs_insert", [[{}], [pairs]])


@pytest.mark.parametrize("scope", ["query", "column", "global"])
def test_map_pairs_json(param_client, call, scope):
    query = """SELECT CAST('{"m":{"k":13,"k":79},"plain":13}',
                           'JSON(m Map(String, UInt64), max_dynamic_paths=0)') AS j,
                      CAST('{"plain":79}', 'JSON') AS d"""
    kwargs = {}
    if scope == "query":
        kwargs["query_formats"] = {"Map": "pairs"}
    elif scope == "column":
        kwargs["column_formats"] = {"j": "pairs"}
    else:
        set_read_format("Map", "pairs")
    try:
        assert call(param_client.query, query, **kwargs).first_row == ({"m": _PAIRS, "plain": 13}, {"plain": 79})
    finally:
        clear_read_format("Map")


@pytest.mark.skipif(np is None, reason="NumPy not installed")
@pytest.mark.parametrize("extra_column", ["", ", toUInt16(13) AS n", ", 'user_1' AS s"])
def test_map_pairs_numpy_mixed_and_empty(param_client, call, extra_column):
    query = f"SELECT {_MAP} AS m{extra_column} FROM numbers(3)"
    result = call(param_client.query_np, query, query_formats={"Map": "pairs"})
    if "UInt16" in extra_column:
        assert result.shape == (3,)
        assert result.dtype["m"] == np.dtype("O")
        assert result["m"].tolist() == [_PAIRS] * 3
        assert result.dtype["n"] == np.dtype("uint16")
    else:
        assert result.shape == (3, 2 if extra_column else 1)
        assert result[:, 0].tolist() == [_PAIRS] * 3
    result = call(param_client.query_np, query + " WHERE number < 0", query_formats={"Map": "pairs"})
    assert result.shape[0] == 0


@pytest.mark.skipif(np is None, reason="NumPy not installed")
def test_map_pairs_numpy_json_empty_first_row(param_client, call):
    query = """SELECT if(number = 0, [], [CAST('{"m":{"k":13,"k":79}}', 'JSON(m Map(String, UInt8))')]) AS m
               FROM numbers(3)"""
    result = call(param_client.query_np, query, query_formats={"Map": "pairs"}, settings={"max_block_size": 1, "max_threads": 1})
    assert result.shape == (3, 1)
    assert result[:, 0].tolist() == [[], [{"m": _PAIRS}], [{"m": _PAIRS}]]


@pytest.mark.parametrize(
    "method", ["query_rows_stream", "query_row_block_stream", "query_column_block_stream", "query_np_stream", "query_df_stream"]
)
@pytest.mark.parametrize("wrapper", ["{map}", "if(number = 0, [], [{map}])", "if(number = 0, CAST(13, 'Dynamic'), CAST({map}, 'Dynamic'))"])
def test_map_pairs_streams(param_client, call, consume_stream, method, wrapper):
    if method == "query_np_stream" and np is None or method == "query_df_stream" and pd is None:
        pytest.skip("Optional dataframe dependency not installed")
    expression = wrapper.format(map="map('k', number, 'k', number + 1)")
    result = []
    block_count = 0

    def consume(block):
        nonlocal block_count
        block_count += 1
        if method == "query_rows_stream":
            result.append(block[0])
        elif method == "query_row_block_stream":
            result.extend(row[0] for row in block)
        elif method == "query_column_block_stream":
            result.extend(block[0])
        elif method == "query_np_stream":
            assert block.ndim == 2 and block.shape[1] == 1
            result.extend(block[:, 0].tolist())
        else:
            result.extend(block["m"].tolist())

    stream = call(
        getattr(param_client, method),
        f"SELECT {expression} AS m FROM numbers(5)",
        query_formats={"Map": "pairs"},
        settings={"max_block_size": 1, "max_threads": 1},
    )
    consume_stream(stream, consume)
    assert block_count > 1
    expected = [[("k", n), ("k", n + 1)] for n in range(5)]
    if "[]" in wrapper:
        expected = [[]] + [[pairs] for pairs in expected[1:]]
    elif "Dynamic" in wrapper:
        expected[0] = 13
    assert result == expected
