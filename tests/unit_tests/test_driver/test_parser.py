from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.parser import parse_callable, parse_enum
from clickhouse_connect.driver.query import remove_sql_comments


def test_parse_callable():
    assert parse_callable('CALLABLE(1, 5)') == ('CALLABLE', (1, 5), '')
    assert parse_callable("Enum4('v1' = 5) other stuff") == ('Enum4', ("'v1'= 5",), 'other stuff')
    assert parse_callable('BareThing') == ('BareThing', (), '')
    assert parse_callable('Tuple(Tuple (String), Int32)') == ('Tuple', ('Tuple(String)', 'Int32'), '')
    assert parse_callable("ReplicatedMergeTree('/clickhouse/tables/test', '{replica'}) PARTITION BY key")\
           == ('ReplicatedMergeTree', ("'/clickhouse/tables/test'", "'{replica'}"), 'PARTITION BY key')


def test_parse_enum():
    assert parse_enum("Enum8('one' = 1)") == (('one',), (1,))
    assert parse_enum("Enum16('**\\'5' = 5, '578' = 7)") == (("**'5", '578'), (5, 7))


def test_map_type():
    ch_type = get_from_name('Map(String, Decimal(5, 5))')
    assert ch_type.name == 'Map(String, Decimal(5, 5))'


def test_variant_type():
    ch_type = get_from_name('Variant(UInt64, String, Array(UInt64))')
    assert ch_type.name == 'Variant(UInt64, String, Array(UInt64))'


def test_json_type():
    names = ['JSON',
             'JSON(max_dynamic_paths=100, a.b UInt32, SKIP `a.e`)',
             "JSON(max_dynamic_types = 55, SKIP REGEXP 'a[efg]')",
             'JSON(max_dynamic_types = 33, `a.b` UInt64, b.c String)']
    parsed = ['JSON',
               'JSON(max_dynamic_paths = 100, `a.b` UInt32, SKIP `a.e`)',
               "JSON(max_dynamic_types = 55, SKIP REGEXP 'a[efg]')",
               'JSON(max_dynamic_types = 33, `a.b` UInt64, `b.c` String)'
               ]
    for name, x in zip(names, parsed):
        ch_type = get_from_name(name)
        assert x == ch_type.name


def test_remove_comments():
    sql = """SELECT -- 6dcd92a04feb50f14bbcf07c661680ba
* FROM benchmark_results /*With an inline comment */ WHERE result = 'True'
/*  A single line */
LIMIT
/*  A multiline comment
   
*/
2
-- 6dcd92a04feb50f14bbcf07c661680ba
"""
    assert remove_sql_comments(sql) == "SELECT \n* FROM benchmark_results  WHERE result = 'True'\n\nLIMIT\n\n2\n\n"
