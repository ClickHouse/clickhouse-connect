import pytest

from clickhouse_connect.cc_sqlalchemy.inspector import _parse_dictionary_column


@pytest.mark.parametrize(
    "definition,key,expression,comment",
    [
        ("`value` String DEFAULT 'n/a'", "server_default", "'n/a'", None),
        ("doubled UInt64 MATERIALIZED id * 2", "clickhouse_materialized", "id * 2", None),
        ("label String ALIAS concat(name, '!') COMMENT 'shown'", "clickhouse_alias", "concat(name, '!')", "shown"),
    ],
)
def test_dictionary_column_expressions_are_strings(definition, key, expression, comment):
    column = _parse_dictionary_column(definition)
    assert isinstance(column[key], str)
    assert column[key] == expression
    assert column.get("comment") == comment
