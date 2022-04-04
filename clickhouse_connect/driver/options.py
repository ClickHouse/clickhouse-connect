from clickhouse_connect.driver.exceptions import NotSupportedError

has_numpy = True
has_pandas = True

try:
    import numpy as np
except ImportError:
    has_numpy = False

try:
    import pandas as pa
except ImportError:
    has_pandas = False


def check_numpy():
    if not has_numpy:
        raise NotSupportedError("Numpy package is not installed")


def check_pandas():
    if not has_pandas:
        raise NotSupportedError("Pandas package is not installed")
