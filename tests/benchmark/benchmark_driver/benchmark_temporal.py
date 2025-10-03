from datetime import datetime
from random import random

import pytest
import pytz
from clickhouse_connect.datatypes.base import TypeDef
from clickhouse_connect.datatypes.temporal import DateTime64
from clickhouse_connect.driver import tzutil


class TestBenchmarkDateTime64:
    def test_benchmark_read_binary_tz(self, benchmark):
        dt = DateTime64(TypeDef(values=(9,)))
        columns = [int(random() * 1000000000) for _ in range(1)]
        benchmark(dt._read_binary_tz, column = columns, tz_info=pytz.UTC)

    def test_benchmark_read_binary_naive(self, benchmark):
        dt = DateTime64(TypeDef(values=(9,)))
        columns = [int(random() * 1000000000) for _ in range(1)]
        benchmark(dt._read_binary_naive, column = columns)

class TestBenchmarkDateTimeFromTimestampParsing:

    @pytest.fixture()
    def ts(self):
        prec = 10 ** 9
        return int(random() * 1000000000) // prec

    def test_benchmark_datetime_fromtimestamp(self, benchmark, ts):
        benchmark(datetime.fromtimestamp, ts)

    def test_benchmark_tzutil_utcfromtimestamp(self, benchmark, ts):
        benchmark(tzutil.utcfromtimestamp, ts)



