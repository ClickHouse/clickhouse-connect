from clickhouse_connect.driver.exceptions import NotSupportedError

try:
    import numpy as np
except ImportError:
    np = None

try:
    import pandas as pd
    pd_extended_dtypes = not pd.__version__.startswith('0')
except ImportError:
    pd = None
    pd_extended_dtypes = False

try:
    import pyarrow as arrow
except ImportError:
    arrow = None


def check_numpy():
    if np:
        return np
    raise NotSupportedError('Numpy package is not installed')


def check_pandas():
    if pd:
        return pd
    raise NotSupportedError('Pandas package is not installed')


def check_arrow():
    if arrow:
        return arrow
    raise NotSupportedError('PyArrow package is not installed')
