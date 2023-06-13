import clickhouse_connect

client: clickhouse_connect.driver.Client


def inserted_nested_flat():
    client.command('DROP TABLE IF EXISTS test_nested_flat')
    client.command('SET flatten_nested = 1')
    client.command(
"""
CREATE TABLE test_nested_flat
(
    `key` UInt32,
    `value` Nested(str String, int32 Int32)
)
ENGINE = MergeTree
ORDER BY key
""")
    result = client.query('DESCRIBE TABLE test_nested_flat')
    print(result.column_names[0:2])
    print(result.result_columns[0:2])

    # Note the Nested 'value' column is inserted as two parallel arrays of values
    # into their own columns of the form `col_name.key_name` with Array data types
    data = [[1, ['string_1', 'string_2'], [20, 30]],
            [2, ['string_3', 'string_4'], [40, 50]]
            ]
    client.insert('test_nested_flat', data,
                  column_names=['key', 'value.str', 'value.int32'],
                  column_type_names=['UInt32', 'Array(String)', 'Array(Int32)'])

    result = client.query('SELECT * FROM test_nested_flat')
    print(result.column_names)
    print(result.result_columns)
    client.command('DROP TABLE test_nested_flat')


def insert_nested_not_flat():
    client.command('DROP TABLE IF EXISTS test_nested_not_flat')
    client.command('SET flatten_nested = 0')
    client.command(
"""
CREATE TABLE test_nested_not_flat
(
    `key` UInt32,
    `value` Nested(str String, int32 Int32)
)
ENGINE = MergeTree
ORDER BY key
""")
    result = client.query('DESCRIBE TABLE test_nested_not_flat')
    print (result.column_names[0:2])
    print (result.result_columns[0:2])

    # Note the Nested 'value' column is inserted as a list of dictionaries for each row
    data = [[1, [{'str': 'nested_string_1', 'int32': 20},
                {'str': 'nested_string_2', 'int32': 30}]],
            [2, [{'str': 'nested_string_3', 'int32': 40},
                {'str': 'nested_string_4', 'int32': 50}]]
            ]
    client.insert('test_nested_not_flat', data,
                  column_names=['key', 'value'],
                  column_type_names=['UInt32', 'Nested(str String, int32 Int32)'])

    result = client.query('SELECT * FROM test_nested_not_flat')
    print(result.column_names)
    print(result.result_columns)
    client.command('DROP TABLE test_nested_not_flat')


def main():
    global client
    client = clickhouse_connect.get_client()
    print ('Nested example flatten_nested = 1 (Default)')
    inserted_nested_flat()
    print('\n\nNested example flatten_nested = 0')
    insert_nested_not_flat()


if __name__ == '__main__':
    main()