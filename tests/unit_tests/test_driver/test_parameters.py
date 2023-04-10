from datetime import datetime

import pytest

from clickhouse_connect.driver.query import finalize_query, format_bind_value


def test_finalize():
    hash_id = '0x772'
    timestamp = datetime.fromtimestamp(1661447719)
    parameters = {'hash_id': hash_id, 'dt': timestamp}
    expected = "SELECT hash_id FROM db.mytable WHERE hash_id = '0x772' AND dt = '2022-08-25 17:15:19'"
    query = finalize_query('SELECT hash_id FROM db.mytable WHERE hash_id = %(hash_id)s AND dt = %(dt)s', parameters)
    assert query == expected

    parameters = [hash_id, timestamp]
    query = finalize_query('SELECT hash_id FROM db.mytable WHERE hash_id = %s AND dt = %s', parameters)
    assert query == expected


# pylint: disable=inconsistent-quotes
@pytest.mark.parametrize('value, expected', [
    ("a", "a"),
    ("a'", r"a\'"),
    ("'a'", r"\'a\'"),
    ("''a'", r"\'\'a\'"),
    ([], "[]"),
    ([1], "[1]"),
    (["a"], "['a']"),
    (["a'"], r"['a\'']"),
    ([["a"]], "[['a']]"),

])
def test_format_bind_value(value, expected):
    assert format_bind_value(value) == expected
