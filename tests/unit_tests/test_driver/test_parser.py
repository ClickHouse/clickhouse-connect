from clickhouse_connect.driver.parser import parse_callable, parse_enum


def test_parse_callable():
    assert parse_callable('CALLABLE(1, 5)') == ('CALLABLE', (1, 5), '')
    assert parse_callable("Enum4('v1' = 5) other stuff") == ('Enum4', ("'v1'=5",), 'other stuff')
    assert parse_callable('BareThing') == ('BareThing', (), '')
    assert parse_callable('Tuple(Tuple (String), Int32)') == ('Tuple', ('Tuple(String)', 'Int32'), '')
    assert parse_callable("ReplicatedMergeTree('/clickhouse/tables/test', '{replica'}) PARTITION BY key")\
           == ('ReplicatedMergeTree', ("'/clickhouse/tables/test'", "'{replica'}"), 'PARTITION BY key')


def test_parse_enum():
    assert parse_enum("Enum8('one' = 1)") == (('one',), (1,))
    assert parse_enum("Enum16('**\\'5' = 5, '578' = 7)") == (("**'5", '578'), (5, 7))
