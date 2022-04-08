import pkg_resources

def to_bytes(hex_str):
    return memoryview(bytes.fromhex(hex_str))


def add_test_entries():
    dist = pkg_resources.Distribution('clickhouse-connect')
    ep1 = pkg_resources.EntryPoint.parse(
        'clickhousedb.connect = clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect', dist=dist)
    ep2 = pkg_resources.EntryPoint.parse(
        'clickhousedb = clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect', dist=dist)
    entry_map = dist.get_entry_map()
    entry_map['sqlalchemy.dialects'] = {'clickhousedb.connect': ep1, 'clickhousedb': ep2}
    pkg_resources.working_set.add(dist)
    print('test eps added to distribution')
