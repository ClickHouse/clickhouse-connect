import pytest

from clickhouse_connect.cc_sqlalchemy.inspector import _parse_dictionary_column, get_dictionary_columns


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


@pytest.mark.parametrize(
    "attribute,type_name,server_default",
    [
        ("`parent_id` UInt64 DEFAULT 0 HIERARCHICAL", "UInt64", "0"),
        ("`parent_id` UInt64 HIERARCHICAL BIDIRECTIONAL", "UInt64", None),
        ("`parent_id` UInt64 IS_OBJECT_ID", "UInt64", None),
        ("`label` Nullable(String) INJECTIVE", "Nullable(String)", None),
        ("`label` Nullable(String) DEFAULT NULL INJECTIVE", "Nullable(String)", "NULL"),
        ("`label` Nullable(String) EXPRESSION toNullable(name)", "Nullable(String)", None),
        ("`label` String DEFAULT 'user_1' EXPRESSION upper(name) INJECTIVE", "String", "'user_1'"),
        ("`total` UInt64 EXPRESSION parent_id + injective", "UInt64", None),
        ("`label` String DEFAULT ' INJECTIVE'", "String", "' INJECTIVE'"),
        ("`tags` Array(String) DEFAULT ['user_1', 'user_2']", "Array(String)", "['user_1', 'user_2']"),
        ("`tags` Array(Array(UInt64)) DEFAULT [[13, 79], [13]]", "Array(Array(UInt64))", "[[13, 79], [13]]"),
        ("`tags` Array(String) EXPRESSION [name, upper(name)]", "Array(String)", None),
        ("`tags` Array(String) EXPRESSION array(name, upper(name))", "Array(String)", None),
        ("`label` String DEFAULT '[user_1, user_2'", "String", "'[user_1, user_2'"),
        ("`label` String DEFAULT '\u00df\u00df' INJECTIVE", "String", "'\u00df\u00df'"),
    ],
)
def test_dictionary_column_attribute_options(monkeypatch, attribute, type_name, server_default):
    create_sql = (
        f"CREATE DICTIONARY default.lookup\n(\n    `id` UInt64,\n    {attribute}\n)\nPRIMARY KEY id\n"
        "SOURCE(CLICKHOUSE(TABLE 'source'))\nLIFETIME(MIN 0 MAX 0)\nLAYOUT(HASHED())"
    )
    monkeypatch.setattr("clickhouse_connect.cc_sqlalchemy.inspector.get_dictionary_create_sql", lambda *_args: create_sql)
    id_column, column = get_dictionary_columns(None, "lookup")
    assert (id_column["name"], id_column["type"].name) == ("id", "UInt64")
    assert (column["type"].name, column.get("server_default")) == (type_name, server_default)
