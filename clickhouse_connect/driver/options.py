# pylint: disable=unused-import
from clickhouse_connect.driver.exceptions import NotSupportedError

HAS_NUMPY = True
HAS_PANDAS = True


try:
    import numpy as np
except ImportError:
    HAS_NUMPY = False

try:
    import pandas as pa
except ImportError:
    HAS_PANDAS = False


def check_numpy():
    if not HAS_NUMPY:
        raise NotSupportedError("Numpy package is not installed")


def check_pandas():
    if not HAS_PANDAS:
        raise NotSupportedError("Pandas package is not installed")
