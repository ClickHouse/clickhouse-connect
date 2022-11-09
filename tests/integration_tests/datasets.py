import math
from datetime import datetime, date

null_ds = [['key1', 1000, 77.3, 'value1', datetime(2022, 10, 15, 10, 3, 2), None],
           ['key2', 2000, 882.00, None, None, date(1976, 5, 5)],
           ['key3', None, math.nan, 'value3', date(2022, 7, 4), date(1999, 12, 31)]]
null_ds_columns = ['key', 'num', 'flt', 'str', 'dt', 'd']
null_ds_types = ['String', 'Nullable(Int32)', 'Nullable(Float32)', 'Nullable(String)', 'Nullable(DateTime)',
                 'Nullable(Date)']

basic_ds = [('key1', 1000, 50.3, 'value1'),
            ('key2', 2000, -532.43, 'value2'),
            ('key3', -2503, 300.00, 'value3')]
basic_ds_columns = ['key', 'num', 'flt', 'str']
basic_ds_types = ['String', 'Int32', 'Float64', 'String']
