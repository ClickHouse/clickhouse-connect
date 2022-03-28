def to_bytes(hex_str):
    return memoryview(bytes.fromhex(hex_str))


def add_test_entries():
    import pkg_resources
    dist = pkg_resources.Distribution('clickhouse-connect')
    ep = pkg_resources.EntryPoint.parse('clickhousedb.connect = clickhouse_connect.sqlalchemy.dialect:ClickHouseDialect', dist=dist)
    entry_map = dist.get_entry_map()
    map_entry = entry_map.get('sqlalchemy.dialects')
    if map_entry is None:
        entry_map['sqlalchemy.dialects'] = {'clickhousedb': ep}
        pkg_resources.working_set.add(dist)
        print('test ep added to distribution')
    else:
        print('test ep already added')


