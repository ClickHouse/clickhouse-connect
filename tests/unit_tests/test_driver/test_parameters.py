from datetime import datetime, date

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
    (date(2023, 6, 1), '2023-06-01'),
    (datetime(2023, 6, 1, 20, 4, 5), '2023-06-01 20:04:05'),
    ([date(2023, 6, 1), date(2023, 8, 5)], "['2023-06-01', '2023-08-05']")

])
def test_format_bind_value(value, expected):
    assert format_bind_value(value) == expected
