from datetime import datetime, date

null_ds = [('key1', 1000, 77.3, 'value1', datetime(2022, 10, 15, 10, 3, 2), None),
           ('key2', 2000, 882.00, None, None, date(1976, 5, 5)),
           ('key3', None, float('nan'), 'value3', datetime(2022, 7, 4), date(1999, 12, 31)),
           ('key4', 3000, None, 'value4', None, None)]
null_ds_columns = ['key', 'num', 'flt', 'str', 'dt', 'd']
null_ds_types = ['String', 'Nullable(Int32)', 'Nullable(Float64)', 'Nullable(String)', 'Nullable(DateTime)',
                 'Nullable(Date)']

basic_ds = [('key1', 1000, 50.3, 'value1', datetime.now(), 'lc_1'),
            ('key2', 2000, -532.43, 'value2', datetime(1976, 7, 4, 12, 12, 11), 'lc_2'),
            ('key3', -2503, 300.00, 'value3', date(2022, 10, 15), 'lc_99')]
basic_ds_columns = ['key', 'num', 'flt', 'str', 'dt', 'lc_string']
basic_ds_types = ['String', 'Int32', 'Float64', 'String', 'DateTime64(9)', 'LowCardinality(String)']
basic_ds_types_ver19 = ['String', 'Int32', 'Float64', 'String', 'DateTime', 'LowCardinality(String)']

dt_ds = [datetime(2020, 10, 10),
         datetime(2021, 11, 11)]
dt_ds_columns = ['timestamp']
dt_ds_types = ['DateTime']
