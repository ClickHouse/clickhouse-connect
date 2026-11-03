from clickhouse_connect.cc_sqlalchemy.inspector import _parse_dictionary_column


def test_dictionary_column_expressions_are_strings():
    default = _parse_dictionary_column("`value` String DEFAULT 'n/a'")
    assert default["server_default"] == "'n/a'"

    materialized = _parse_dictionary_column("doubled UInt64 MATERIALIZED id * 2")
    assert materialized["clickhouse_materialized"] == "id * 2"

    alias = _parse_dictionary_column("label String ALIAS concat(name, '!') COMMENT 'shown'")
    assert alias["clickhouse_alias"] == "concat(name, '!')"
    assert alias["comment"] == "shown"
