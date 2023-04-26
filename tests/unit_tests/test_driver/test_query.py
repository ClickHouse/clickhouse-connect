from clickhouse_connect.driver.query import QueryContext


def test_copy_context():
    settings = {'max_bytes_for_external_group_by': 1024 * 1024 * 100,
                'read_overflow_mode': 'throw'}
    parameters = {'user_id': 'user_1'}
    query_formats = {'IPv*': 'string'}
    context = QueryContext('SELECT source_ip FROM table WHERE user_id = %(user_id)s',
                           settings=settings,
                           parameters=parameters,
                           query_formats=query_formats,
                           use_none=True)
    assert context.use_none is True
    assert context.final_query == "SELECT source_ip FROM table WHERE user_id = 'user_1'"
    assert context.query_formats['IPv*'] == 'string'
    assert context.settings['max_bytes_for_external_group_by'] == 104857600

    context_copy = context.updated_copy(
        settings={'max_bytes_for_external_group_by': 1024 * 1024 * 24, 'max_execution_time': 120},
        parameters={'user_id': 'user_2'}
    )
    assert context_copy.settings['read_overflow_mode'] == 'throw'
    assert context_copy.settings['max_execution_time'] == 120
    assert context_copy.settings['max_bytes_for_external_group_by'] == 25165824
    assert context_copy.final_query == "SELECT source_ip FROM table WHERE user_id = 'user_2'"
