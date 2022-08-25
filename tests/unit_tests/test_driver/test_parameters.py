from datetime import datetime

from clickhouse_connect.driver.query import finalize_query


def test_finalize():
    hash_id = '0x772'
    parameters = {'hash_id': hash_id, 'dt': datetime.fromtimestamp(1661447719)}
    query = finalize_query('SELECT hash_id FROM db.mytable WHERE hash_id = %(hash_id)s AND dt = %(dt)s', parameters)
    assert query == "SELECT hash_id FROM db.mytable WHERE hash_id = '0x772' AND dt = '2022-08-25 17:15:19'"
