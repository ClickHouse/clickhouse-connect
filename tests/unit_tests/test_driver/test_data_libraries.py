import math
from datetime import datetime, date

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.insert import from_pandas_df
from clickhouse_connect.driver.options import pd

str_type = get_from_name('String')
int32_type = get_from_name('Int32')
dt_type = get_from_name('DateTime')
float_type = get_from_name('Float32')


def test_pandas():
    if not pd:
        pytest.skip('Pandas package not available')
    nulls_data = [['key1', 1000, 77.3, 'value1', datetime(2022, 10, 15, 10, 3, 2)],
                  ['key2', 2000, 882.00, None, None],
                  ['key3', None, math.nan, 'value3', date(2022, 7, 4)]]
    df = pd.DataFrame(nulls_data, columns=['key', 'num', 'flt', 'str', 'date'])
    # print(df)
    data_python = from_pandas_df(df, [str_type, int32_type, float_type, str_type, dt_type])
    print (data_python)
