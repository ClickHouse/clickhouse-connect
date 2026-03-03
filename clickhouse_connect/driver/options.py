from clickhouse_connect.driver.exceptions import NotSupportedError

pd_time_test = None

try:
    import numpy as np
except ImportError:
    np = None

try:
    import pandas as pd

    _pandas_version = tuple(int(x) for x in pd.__version__.split(".")[:2])
    if _pandas_version < (2, 0):
        pd = None

    else:
        def pd_time_test(arr_or_dtype):
            kind = getattr(getattr(arr_or_dtype, "dtype", arr_or_dtype), "kind", None)
            return kind in ("M", "m")

except ImportError:
    pd = None

try:
    import pyarrow as arrow
except ImportError:
    arrow = None

try:
    import polars as pl
except ImportError:
    pl = None

def check_numpy():
    if np:
        return np
    raise NotSupportedError('Numpy package is not installed')


def check_pandas():
    if pd:
        return pd
    try:
        import pandas as _pd
        raise NotSupportedError(
            f"pandas >= 2.0 is required, found {_pd.__version__}. "
            "Please upgrade: pip install 'pandas>=2'"
        )
    except ImportError:
        raise NotSupportedError("Pandas package is not installed")


def check_arrow():
    if arrow:
        return arrow
    raise NotSupportedError('PyArrow package is not installed')


def check_polars():
    if pl:
        return pl
    raise NotSupportedError("Polars package is not installed")
